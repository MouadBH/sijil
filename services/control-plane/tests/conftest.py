import base64
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID

import httpx
import pyotp
import pytest
from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.base import utcnow
from sijil_core.db.models import JobKind
from sijil_cp import deps
from sijil_cp.main import create_app
from sijil_cp.settings import CpSettings

# Ports nothing listens on: readiness must report these as down.
CLOSED_DB = "postgresql+asyncpg://sijil:sijil@127.0.0.1:1/sijil_cp"
CLOSED_REDIS = "redis://127.0.0.1:1/0"
# Deterministic test-only master key (32 bytes).
TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()
UNSAFE = {"POST", "PUT", "PATCH", "DELETE"}


def make_settings(**overrides: str) -> CpSettings:
    values = {
        "region": "ma-cas",
        "environment": "test",
        "database_url": CLOSED_DB,
        "redis_url": CLOSED_REDIS,
        "master_key": TEST_MASTER_KEY,
        "cookie_secure": "false",
    } | overrides
    return CpSettings.model_validate(values)


class CsrfClient(httpx.AsyncClient):
    """Fetches a CSRF token once and sends it on every unsafe request."""

    _csrf: str | None = None

    async def request(self, method: str, url: Any, **kwargs: Any) -> httpx.Response:
        if method.upper() in UNSAFE:
            if self._csrf is None:
                self._csrf = (await super().request("GET", "/v1/auth/csrf")).json()["csrf"]
            headers = dict(kwargs.pop("headers", None) or {})
            headers.setdefault("X-CSRF-Token", self._csrf)
            kwargs["headers"] = headers
        return await super().request(method, url, **kwargs)


@pytest.fixture
def make_app() -> Callable[..., FastAPI]:
    """Build an app on closed ports; override any setting by keyword."""

    def _make(**overrides: str) -> FastAPI:
        return create_app(make_settings(**overrides))

    return _make


@pytest.fixture
def client_for() -> Callable[[FastAPI], httpx.AsyncClient]:
    def _make(app: FastAPI) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://cp")

    return _make


@pytest.fixture
async def unreachable_client(
    client_for: Callable[[FastAPI], httpx.AsyncClient], make_app: Callable[..., FastAPI]
) -> AsyncIterator[httpx.AsyncClient]:
    """App wired to closed ports, for health/readiness tests."""
    async with client_for(make_app()) as c:
        yield c


@pytest.fixture
def make_db_app(migrated_db: Any, redis: Redis) -> Callable[..., FastAPI]:
    """Build an app on the migrated test DB and test Redis; override any setting by keyword."""

    def _make(**overrides: str) -> FastAPI:
        settings = make_settings(
            database_url=migrated_db.app_url,
            redis_url=os.environ["SIJIL_TEST_REDIS_URL"],
            **overrides,
        )
        return create_app(settings)

    return _make


@pytest.fixture
def csrf_client_for() -> Callable[..., CsrfClient]:
    def _make(app: FastAPI, base_url: str = "http://cp") -> CsrfClient:
        return CsrfClient(transport=httpx.ASGITransport(app=app), base_url=base_url)

    return _make


@pytest.fixture
async def client(
    make_db_app: Callable[..., FastAPI], csrf_client_for: Callable[..., CsrfClient]
) -> AsyncIterator[CsrfClient]:
    """App on the migrated test DB and test Redis; unsafe requests carry a CSRF token."""
    async with csrf_client_for(make_db_app()) as c:
        yield c


class RecordingEnqueuer:
    """Stands in for Celery: remembers what would have been sent."""

    def __init__(self) -> None:
        self.calls: list[tuple[JobKind, UUID, UUID]] = []

    def send(self, kind: JobKind, job_id: UUID, workspace_id: UUID) -> None:
        self.calls.append((kind, job_id, workspace_id))


@pytest.fixture
def enqueuer() -> RecordingEnqueuer:
    return RecordingEnqueuer()


class Clock:
    """A frozen request clock that tests move by hand."""

    def __init__(self) -> None:
        now = utcnow()
        # Five seconds into a TOTP step, so ±1 step is unambiguous.
        self.now = datetime.fromtimestamp((int(now.timestamp()) // 30) * 30 + 5, tz=now.tzinfo)

    def advance(self, delta: timedelta) -> datetime:
        self.now += delta
        return self.now


@pytest.fixture
def clock() -> Clock:
    return Clock()


PASSWORD = "correct horse battery"


class Actor:
    """A signed-in user with their own cookie jar."""

    def __init__(self, client: CsrfClient, email: str, user_id: UUID, clock: Clock) -> None:
        self.client = client
        self.email = email
        self.user_id = user_id
        self.clock = clock
        self.secret: str | None = None

    async def enable_totp(self) -> None:
        r = await self.client.post("/v1/auth/totp/enrol", json={"password": PASSWORD})
        assert r.status_code == 200
        self.secret = parse_qs(urlparse(str(r.json()["otpauth_uri"])).query)["secret"][0]
        code = pyotp.TOTP(self.secret).at(self.clock.now)
        assert (await self.client.post("/v1/auth/totp/confirm", json={"code": code})).is_success
        self.clock.advance(timedelta(seconds=30))

    async def step_up(self) -> None:
        """Log in again and verify a fresh TOTP code (needs enable_totp first)."""
        assert self.secret is not None
        r = await self.client.post(
            "/v1/auth/login", json={"email": self.email, "password": PASSWORD}
        )
        assert r.status_code == 200
        code = pyotp.TOTP(self.secret).at(self.clock.now)
        assert (await self.client.post("/v1/auth/totp/verify", json={"code": code})).is_success
        self.clock.advance(timedelta(seconds=30))


@pytest.fixture
def cp_app(
    make_db_app: Callable[..., FastAPI], enqueuer: RecordingEnqueuer, clock: Clock
) -> FastAPI:
    app = make_db_app()
    app.state.enqueuer = enqueuer
    app.dependency_overrides[deps.get_now] = lambda: clock.now
    return app


@pytest.fixture
async def make_actor(
    cp_app: FastAPI, csrf_client_for: Callable[..., CsrfClient], clock: Clock, db: AsyncSession
) -> AsyncIterator[Callable[[str], Awaitable[Actor]]]:
    clients: list[CsrfClient] = []

    async def _make(email: str) -> Actor:
        c = csrf_client_for(cp_app)
        clients.append(c)
        r = await c.post(
            "/v1/auth/signup",
            json={"email": email, "password": PASSWORD, "name": email, "locale": "fr"},
        )
        assert r.status_code == 201
        return Actor(c, email, UUID(r.json()["user"]["id"]), clock)

    yield _make
    for c in clients:
        await c.aclose()
