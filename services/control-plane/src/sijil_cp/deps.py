from collections.abc import AsyncIterator, Callable, Coroutine
from datetime import datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, Request
from redis.asyncio import Redis
from sqlalchemy import exists, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.crypto import LocalKeyService
from sijil_core.db.base import utcnow
from sijil_core.db.models import Membership, Organisation, User, UserSession, Workspace
from sijil_cp.auth import sessions
from sijil_cp.errors import ApiError


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessionmaker() as session:
        yield session


def get_redis(request: Request) -> Redis:
    redis: Redis = request.app.state.redis
    return redis


def get_keys(request: Request) -> LocalKeyService:
    keys: LocalKeyService = request.app.state.keys
    return keys


def get_now() -> datetime:
    """The request's clock. Overridden in tests to move time."""
    return utcnow()


STEP_UP_WINDOW = timedelta(minutes=10)

SessionDep = Callable[..., Coroutine[Any, Any, UserSession]]


def _session_dependency(allow_pending: bool) -> SessionDep:
    async def dependency(
        request: Request,
        db: Annotated[AsyncSession, Depends(get_db)],
        now: Annotated[datetime, Depends(get_now)],
    ) -> UserSession:
        raw = request.cookies.get(sessions.COOKIE)
        row = await sessions.resolve(db, raw, now, allow_pending=True) if raw else None
        if row is None:
            raise ApiError("auth.unauthenticated", 401)
        if row.pending_totp and not allow_pending:
            # A live session that still owes its second factor.
            raise ApiError("auth.totp_required", 401)
        return row

    return dependency


# One callable per variant, so FastAPI caches the result within a request.
_SESSION_DEPS = {False: _session_dependency(False), True: _session_dependency(True)}


def current_session(allow_pending: bool = False) -> SessionDep:
    """A dependency resolving the request's session; pending-TOTP sessions only if allowed."""
    return _SESSION_DEPS[allow_pending]


async def current_user(
    session: Annotated[UserSession, Depends(current_session())],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> User:
    user = await db.get(User, session.user_id)
    if user is None:
        raise ApiError("auth.unauthenticated", 401)
    return user


async def user_in_totp_org(db: AsyncSession, user_id: UUID) -> bool:
    """Whether any organisation the user belongs to (directly or via a workspace) requires TOTP."""
    via_workspace = (
        select(Workspace.org_id)
        .join(Membership, Membership.workspace_id == Workspace.id)
        .where(Membership.user_id == user_id)
    )
    direct = select(Membership.org_id).where(
        Membership.user_id == user_id, Membership.org_id.is_not(None)
    )
    query = select(
        exists().where(
            Organisation.require_totp.is_(True),
            or_(Organisation.id.in_(direct), Organisation.id.in_(via_workspace)),
        )
    )
    return bool(await db.scalar(query))


async def require_totp_compliance(
    user: Annotated[User, Depends(current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> User:
    """403 `auth.totp_enrolment_required` while an organisation requires TOTP the user lacks."""
    if not user.totp_enabled and await user_in_totp_org(db, user.id):
        raise ApiError("auth.totp_enrolment_required", 403)
    return user


def has_recent_totp(
    session: UserSession, user: User, now: datetime, window: timedelta = STEP_UP_WINDOW
) -> bool:
    """Step-up: this session verified a TOTP (or recovery) code within `window`."""
    verified = session.totp_verified_at
    return user.totp_enabled and verified is not None and now - window <= verified <= now


def require_recent_totp(minutes: int = 10) -> SessionDep:
    """A dependency demanding a TOTP verification on this session within `minutes`.

    Users without TOTP can never satisfy it: they get `auth.step_up_required` too.
    """
    window = timedelta(minutes=minutes)

    async def dependency(
        session: Annotated[UserSession, Depends(current_session())],
        user: Annotated[User, Depends(current_user)],
        now: Annotated[datetime, Depends(get_now)],
    ) -> UserSession:
        if not has_recent_totp(session, user, now, window):
            raise ApiError("auth.step_up_required", 403)
        return session

    return dependency
