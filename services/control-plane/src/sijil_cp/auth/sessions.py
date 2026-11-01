"""Server-side sessions. The client holds a random token; the database holds only its SHA-256."""

import base64
import hashlib
import secrets
from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.base import utcnow
from sijil_core.db.models import User, UserSession

COOKIE = "sijil_session"
ABSOLUTE_TTL = timedelta(hours=12)
IDLE_TTL = timedelta(hours=2)
# A session that has passed the password but not yet TOTP lives only this long.
PENDING_TTL = timedelta(minutes=10)
# last_seen_at is written at most this often.
TOUCH_EVERY = timedelta(minutes=1)
_TOKEN_BYTES = 32
_UA_MAX = 512


def new_token() -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(_TOKEN_BYTES)).rstrip(b"=").decode()


def token_hash(raw_token: str) -> bytes:
    return hashlib.sha256(raw_token.encode()).digest()


async def create_session(
    db: AsyncSession,
    user: User,
    *,
    ip: str,
    user_agent: str,
    pending_totp: bool,
    now: datetime | None = None,
    totp_verified_at: datetime | None = None,
    expires_at: datetime | None = None,
) -> tuple[UserSession, str]:
    """A new session and its raw token.

    `expires_at` lets a rotated session keep the absolute expiry of the one it replaces;
    it can only shorten the default, never extend it.
    """
    now = now or utcnow()
    raw = new_token()
    ceiling = now + (PENDING_TTL if pending_totp else ABSOLUTE_TTL)
    row = UserSession(
        user_id=user.id,
        token_hash=token_hash(raw),
        created_at=now,
        expires_at=min(expires_at, ceiling) if expires_at is not None else ceiling,
        last_seen_at=now,
        pending_totp=pending_totp,
        totp_verified_at=totp_verified_at,
        ip=ip,
        user_agent=user_agent[:_UA_MAX],
    )
    db.add(row)
    await db.commit()
    return row, raw


async def resolve(
    db: AsyncSession, raw_token: str, now: datetime, *, allow_pending: bool = False
) -> UserSession | None:
    row = await db.scalar(
        select(UserSession).where(UserSession.token_hash == token_hash(raw_token))
    )
    if row is None or row.revoked_at is not None:
        return None
    if now >= row.expires_at or now >= row.last_seen_at + IDLE_TTL:
        return None
    if row.pending_totp and not allow_pending:
        return None
    if now - row.last_seen_at >= TOUCH_EVERY:
        row.last_seen_at = now
        await db.commit()
    return row


async def revoke(db: AsyncSession, session_id: UUID, *, now: datetime | None = None) -> None:
    await db.execute(
        update(UserSession)
        .where(UserSession.id == session_id, UserSession.revoked_at.is_(None))
        .values(revoked_at=now or utcnow())
    )
    await db.commit()


async def revoke_others(db: AsyncSession, user_id: UUID, keep: UUID, now: datetime) -> None:
    """Revoke every live session of `user_id` except `keep` (after a second-factor change)."""
    await db.execute(
        update(UserSession)
        .where(
            UserSession.user_id == user_id,
            UserSession.id != keep,
            UserSession.revoked_at.is_(None),
        )
        .values(revoked_at=now)
    )
    await db.commit()


async def revoke_token(db: AsyncSession, raw_token: str | None, now: datetime) -> None:
    """Revoke the session a raw token belongs to, if any (no session fixation on rotation)."""
    if not raw_token:
        return
    await db.execute(
        update(UserSession)
        .where(UserSession.token_hash == token_hash(raw_token), UserSession.revoked_at.is_(None))
        .values(revoked_at=now)
    )
    await db.commit()
