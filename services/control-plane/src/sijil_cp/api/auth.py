"""Signup, login, logout, the current user and their sessions."""

import asyncio
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from redis.asyncio import Redis
from sqlalchemy import delete, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import ReturningUpdate

from sijil_core.crypto import Envelope, LocalKeyService, open_envelope, seal
from sijil_core.db.models import Digits, Locale, RecoveryCode, User, UserSession
from sijil_cp.auth import passwords, ratelimit, sessions, totp
from sijil_cp.deps import (
    current_session,
    current_user,
    get_db,
    get_keys,
    get_now,
    get_redis,
    has_recent_totp,
    require_recent_totp,
    user_in_totp_org,
)
from sijil_cp.errors import ApiError

router = APIRouter(tags=["auth"])

_EMAIL_UNIQUE = "uq_user_account_email"
# Bounds request size before argon2 sees it; the real rule (12–128) is checked separately.
_PASSWORD_CAP = 4096

Db = Annotated[AsyncSession, Depends(get_db)]
Now = Annotated[datetime, Depends(get_now)]
Rds = Annotated[Redis, Depends(get_redis)]
Keys = Annotated[LocalKeyService, Depends(get_keys)]
# Long enough for any typo, short enough to bound work; wrong lengths simply fail to verify.
_CODE_CAP = 64


def _normalise_email(value: str) -> str:
    value = value.strip().lower()
    local, _, domain = value.partition("@")
    if not local or not domain or "@" in domain or any(c.isspace() for c in value):
        raise ValueError("invalid email")
    return value


class SignupIn(BaseModel):
    email: str = Field(max_length=320)
    password: str = Field(max_length=_PASSWORD_CAP)
    name: str = Field(min_length=1, max_length=200)
    locale: Locale = Locale.fr

    @field_validator("email")
    @classmethod
    def _email(cls, value: str) -> str:
        return _normalise_email(value)

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("empty name")
        return value


class LoginIn(BaseModel):
    email: str = Field(max_length=320)
    password: str = Field(max_length=_PASSWORD_CAP)

    @field_validator("email")
    @classmethod
    def _email(cls, value: str) -> str:
        return _normalise_email(value)


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    email: str
    name: str
    locale: Locale
    digits: Digits
    totp_enabled: bool


class SignupOut(BaseModel):
    user: UserOut


class LoginOut(BaseModel):
    status: Literal["ok", "totp_required"]


class TotpCodeIn(BaseModel):
    code: str = Field(max_length=_CODE_CAP)


class EnrolIn(BaseModel):
    # Required while the user has no TOTP yet (a session alone is not enough to add a factor).
    password: str | None = Field(default=None, max_length=_PASSWORD_CAP)


class TotpVerifyIn(BaseModel):
    code: str | None = Field(default=None, max_length=_CODE_CAP)
    recovery_code: str | None = Field(default=None, max_length=_CODE_CAP)

    @model_validator(mode="after")
    def _exactly_one(self) -> "TotpVerifyIn":
        if (self.code is None) == (self.recovery_code is None):
            raise ValueError("send exactly one of code and recovery_code")
        return self


class EnrolOut(BaseModel):
    otpauth_uri: str


class RecoveryCodesOut(BaseModel):
    recovery_codes: list[str]


class SessionOut(BaseModel):
    id: UUID
    created_at: datetime
    last_seen_at: datetime
    ip: str | None
    user_agent: str | None
    current: bool


def _client(request: Request) -> tuple[str, str]:
    ip = request.client.host if request.client else ""
    return ip, request.headers.get("user-agent", "")


def _set_session_cookie(request: Request, response: Response, raw: str) -> None:
    response.set_cookie(
        sessions.COOKIE,
        raw,
        max_age=int(sessions.ABSOLUTE_TTL.total_seconds()),
        httponly=True,
        secure=request.app.state.settings.cookie_secure,
        samesite="lax",
        path="/",
    )


@router.post("/auth/signup", status_code=status.HTTP_201_CREATED)
async def signup(
    body: SignupIn, request: Request, response: Response, db: Db, redis: Rds, now: Now
) -> SignupOut:
    ip, user_agent = _client(request)
    key = ratelimit.ip_key("signup", ip)
    if not await ratelimit.hit(redis, key, ratelimit.SIGNUP_LIMIT, ratelimit.SIGNUP_WINDOW_S):
        raise ApiError("auth.rate_limited", 429)
    if not passwords.acceptable(body.password):
        raise ApiError("auth.weak_password", 422)
    password_hash = await asyncio.to_thread(passwords.hash_password, body.password)
    user = User(email=body.email, name=body.name, locale=body.locale, password_hash=password_hash)
    db.add(user)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        if _EMAIL_UNIQUE in str(exc.orig):
            raise ApiError("user.email_taken", 409) from None
        raise
    await sessions.revoke_token(db, request.cookies.get(sessions.COOKIE), now)
    _, raw = await sessions.create_session(
        db, user, ip=ip, user_agent=user_agent, pending_totp=False, now=now
    )
    _set_session_cookie(request, response, raw)
    return SignupOut(user=UserOut.model_validate(user))


@router.post("/auth/login")
async def login(
    body: LoginIn,
    request: Request,
    response: Response,
    db: Db,
    redis: Rds,
    now: Now,
) -> LoginOut:
    ip, user_agent = _client(request)
    key = ratelimit.login_key(body.email, ip)
    # Counted before verifying, atomically, so parallel requests can't share one read of the count.
    if not await ratelimit.hit(redis, key, ratelimit.LOGIN_LIMIT, ratelimit.LOGIN_WINDOW_S):
        raise ApiError("auth.rate_limited", 429)
    user = await db.scalar(select(User).where(User.email == body.email))
    # Exactly one argon2 verify whether or not the account exists.
    stored = user.password_hash if user is not None else None
    ok = await asyncio.to_thread(passwords.verify_password, stored, body.password)
    if user is None or not ok:
        raise ApiError("auth.invalid_credentials", 401)
    # Success forgives earlier failures; the attempt itself was counted above.
    await ratelimit.clear(redis, key)
    await sessions.revoke_token(db, request.cookies.get(sessions.COOKIE), now)
    _, raw = await sessions.create_session(
        db, user, ip=ip, user_agent=user_agent, pending_totp=user.totp_enabled, now=now
    )
    _set_session_cookie(request, response, raw)
    return LoginOut(status="totp_required" if user.totp_enabled else "ok")


@router.post("/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(request: Request, db: Db, now: Now) -> Response:
    await sessions.revoke_token(db, request.cookies.get(sessions.COOKIE), now)
    response = Response(status_code=status.HTTP_204_NO_CONTENT)
    response.delete_cookie(
        sessions.COOKIE,
        httponly=True,
        secure=request.app.state.settings.cookie_secure,
        samesite="lax",
        path="/",
    )
    return response


@router.get("/me")
async def me(user: Annotated[User, Depends(current_user)]) -> UserOut:
    return UserOut.model_validate(user)


@router.get("/auth/sessions")
async def list_sessions(
    session: Annotated[UserSession, Depends(current_session())], db: Db, now: Now
) -> list[SessionOut]:
    rows = await db.scalars(
        select(UserSession)
        .where(
            UserSession.user_id == session.user_id,
            UserSession.revoked_at.is_(None),
            UserSession.expires_at > now,
            UserSession.last_seen_at > now - sessions.IDLE_TTL,
        )
        .order_by(UserSession.created_at.desc())
    )
    return [
        SessionOut(
            id=row.id,
            created_at=row.created_at,
            last_seen_at=row.last_seen_at,
            ip=row.ip,
            user_agent=row.user_agent,
            current=row.id == session.id,
        )
        for row in rows
    ]


@router.delete("/auth/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_session(
    session_id: UUID,
    session: Annotated[UserSession, Depends(current_session())],
    db: Db,
    now: Now,
) -> Response:
    owner = await db.scalar(select(UserSession.user_id).where(UserSession.id == session_id))
    if owner != session.user_id:
        raise ApiError("not_found", 404)
    await sessions.revoke(db, session_id, now=now)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- TOTP -------------------------------------------------------------------------------------
# The secret is sealed with key_id "platform" and context "user:<id>:totp", both while pending
# and once confirmed, so confirming only moves the envelope between columns.

_TOTP_KEY_ID = "platform"


def _totp_context(user: User) -> str:
    return f"user:{user.id}:totp"


def _open_secret(keys: LocalKeyService, user: User, sealed: bytes) -> str:
    return open_envelope(Envelope.from_bytes(sealed), keys, _totp_context(user)).decode()


async def _count_totp_attempt(redis: Redis, user: User) -> str:
    """Count one attempt for this user before checking it; 429 once over the limit."""
    key = ratelimit.totp_key(user.id)
    if not await ratelimit.hit(redis, key, ratelimit.TOTP_LIMIT, ratelimit.TOTP_WINDOW_S):
        raise ApiError("auth.rate_limited", 429)
    return key


def _spend_step(user_id: UUID, step: int) -> ReturningUpdate[tuple[UUID]]:
    """Record `step` as used, only if it is newer than the last one: replay-safe under races."""
    return (
        update(User)
        .where(
            User.id == user_id,
            or_(User.totp_last_step.is_(None), User.totp_last_step < step),
        )
        .values(totp_last_step=step)
        .returning(User.id)
    )


@router.post("/auth/totp/enrol")
async def totp_enrol(
    session: Annotated[UserSession, Depends(current_session())],
    user: Annotated[User, Depends(current_user)],
    db: Db,
    redis: Rds,
    keys: Keys,
    now: Now,
    body: EnrolIn | None = None,
) -> EnrolOut:
    if user.totp_enabled:
        # Replacing a working second factor is as sensitive as removing it.
        if not has_recent_totp(session, user, now):
            raise ApiError("auth.step_up_required", 403)
    else:
        # Adding the first factor proves the password again, so a stolen session cannot turn
        # itself into a permanent takeover. Counted per user, like TOTP codes.
        key = await _count_totp_attempt(redis, user)
        password = body.password if body is not None else None
        ok = password is not None and await asyncio.to_thread(
            passwords.verify_password, user.password_hash, password
        )
        if not ok:
            raise ApiError("auth.invalid_credentials", 401)
        await ratelimit.clear(redis, key)
    secret = totp.new_secret()
    user.totp_pending = seal(secret.encode(), keys, _TOTP_KEY_ID, _totp_context(user)).to_bytes()
    await db.commit()
    return EnrolOut(otpauth_uri=totp.provisioning_uri(secret, user.email))


@router.post("/auth/totp/confirm")
async def totp_confirm(
    body: TotpCodeIn,
    session: Annotated[UserSession, Depends(current_session())],
    user: Annotated[User, Depends(current_user)],
    db: Db,
    redis: Rds,
    keys: Keys,
    now: Now,
) -> RecoveryCodesOut:
    key = await _count_totp_attempt(redis, user)
    pending = user.totp_pending
    if pending is None:
        raise ApiError("auth.totp_invalid", 401)
    step = totp.verify(_open_secret(keys, user, pending), body.code, user.totp_last_step, now)
    if step is None:
        raise ApiError("auth.totp_invalid", 401)
    # Swap only the secret that was verified, and only with a step nobody has spent.
    swapped = await db.scalar(
        _spend_step(user.id, step)
        .where(User.totp_pending == pending)
        .values(totp_secret=pending, totp_pending=None, totp_enabled=True)
    )
    if swapped is None:
        await db.rollback()
        raise ApiError("auth.totp_invalid", 401)
    codes = totp.new_recovery_codes()
    await db.execute(delete(RecoveryCode).where(RecoveryCode.user_id == user.id))
    db.add_all(
        RecoveryCode(user_id=user.id, code_hmac=totp.recovery_hmac(keys, code)) for code in codes
    )
    await db.commit()
    await ratelimit.clear(redis, key)
    # The second factor changed: every other session (perhaps a thief's) must log in again.
    await sessions.revoke_others(db, user.id, keep=session.id, now=now)
    return RecoveryCodesOut(recovery_codes=codes)


@router.post("/auth/totp/verify")
async def totp_verify(
    body: TotpVerifyIn,
    request: Request,
    response: Response,
    session: Annotated[UserSession, Depends(current_session(allow_pending=True))],
    db: Db,
    redis: Rds,
    keys: Keys,
    now: Now,
) -> LoginOut:
    """Complete a pending login, or refresh step-up on a full session. Rotates the token."""
    user = await db.get(User, session.user_id)
    if user is None:
        raise ApiError("auth.unauthenticated", 401)
    key = await _count_totp_attempt(redis, user)
    if not user.totp_enabled or user.totp_secret is None:
        raise ApiError("auth.totp_invalid", 401)
    if body.code is not None:
        secret = _open_secret(keys, user, user.totp_secret)
        step = totp.verify(secret, body.code, user.totp_last_step, now)
        ok = step is not None and await db.scalar(_spend_step(user.id, step)) is not None
    else:
        recovery = body.recovery_code or ""
        # Single use under races: the UPDATE claims the code or matches nothing.
        ok = (
            await db.scalar(
                update(RecoveryCode)
                .where(
                    RecoveryCode.user_id == user.id,
                    RecoveryCode.code_hmac == totp.recovery_hmac(keys, recovery),
                    RecoveryCode.used_at.is_(None),
                )
                .values(used_at=now)
                .returning(RecoveryCode.id)
            )
            is not None
        )
    if not ok:
        await db.rollback()
        raise ApiError("auth.totp_invalid", 401)
    await db.commit()
    await ratelimit.clear(redis, key)

    # A pending login gets a full session's lifetime; a step-up keeps the current expiry.
    keep_expiry = None if session.pending_totp else session.expires_at
    await sessions.revoke_token(db, request.cookies.get(sessions.COOKIE), now)
    ip, user_agent = _client(request)
    _, raw = await sessions.create_session(
        db,
        user,
        ip=ip,
        user_agent=user_agent,
        pending_totp=False,
        now=now,
        totp_verified_at=now,
        expires_at=keep_expiry,
    )
    _set_session_cookie(request, response, raw)
    return LoginOut(status="ok")


@router.post("/auth/totp/disable", status_code=status.HTTP_204_NO_CONTENT)
async def totp_disable(
    session: Annotated[UserSession, Depends(require_recent_totp())],
    user: Annotated[User, Depends(current_user)],
    db: Db,
    now: Now,
) -> Response:
    if await user_in_totp_org(db, user.id):
        raise ApiError("auth.totp_enforced", 409)
    user.totp_enabled = False
    user.totp_secret = None
    user.totp_pending = None
    await db.execute(delete(RecoveryCode).where(RecoveryCode.user_id == user.id))
    await db.commit()
    await sessions.revoke_others(db, user.id, keep=session.id, now=now)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
