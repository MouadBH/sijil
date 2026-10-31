from collections.abc import AsyncIterator, Callable, Coroutine
from datetime import datetime
from typing import Annotated, Any

from fastapi import Depends, Request
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.crypto import LocalKeyService
from sijil_core.db.base import utcnow
from sijil_core.db.models import User, UserSession
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


SessionDep = Callable[..., Coroutine[Any, Any, UserSession]]


def _session_dependency(allow_pending: bool) -> SessionDep:
    async def dependency(
        request: Request,
        db: Annotated[AsyncSession, Depends(get_db)],
        now: Annotated[datetime, Depends(get_now)],
    ) -> UserSession:
        raw = request.cookies.get(sessions.COOKIE)
        row = await sessions.resolve(db, raw, now, allow_pending=allow_pending) if raw else None
        if row is None:
            raise ApiError("auth.unauthenticated", 401)
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
