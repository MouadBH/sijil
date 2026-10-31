"""Signup, login, logout, the current user and their sessions."""

import asyncio
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from redis.asyncio import Redis
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import Digits, Locale, User, UserSession
from sijil_cp.auth import passwords, ratelimit, sessions
from sijil_cp.deps import current_session, current_user, get_db, get_now, get_redis
from sijil_cp.errors import ApiError

router = APIRouter(tags=["auth"])

_EMAIL_UNIQUE = "uq_user_account_email"
# Bounds request size before argon2 sees it; the real rule (12–128) is checked separately.
_PASSWORD_CAP = 4096

Db = Annotated[AsyncSession, Depends(get_db)]
Now = Annotated[datetime, Depends(get_now)]


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


async def _retire_presented_token(db: AsyncSession, request: Request, now: datetime) -> None:
    """Revoke whatever session token the client arrived with: no session fixation."""
    raw = request.cookies.get(sessions.COOKIE)
    if not raw:
        return
    await db.execute(
        update(UserSession)
        .where(UserSession.token_hash == sessions.token_hash(raw), UserSession.revoked_at.is_(None))
        .values(revoked_at=now)
    )
    await db.commit()


@router.post("/auth/signup", status_code=status.HTTP_201_CREATED)
async def signup(
    body: SignupIn, request: Request, response: Response, db: Db, now: Now
) -> SignupOut:
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
    await _retire_presented_token(db, request, now)
    ip, user_agent = _client(request)
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
    redis: Annotated[Redis, Depends(get_redis)],
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
    await _retire_presented_token(db, request, now)
    _, raw = await sessions.create_session(
        db, user, ip=ip, user_agent=user_agent, pending_totp=user.totp_enabled, now=now
    )
    _set_session_cookie(request, response, raw)
    return LoginOut(status="totp_required" if user.totp_enabled else "ok")


@router.post("/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(request: Request, db: Db, now: Now) -> Response:
    await _retire_presented_token(db, request, now)
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
