import base64
import os
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from redis.asyncio import Redis

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
