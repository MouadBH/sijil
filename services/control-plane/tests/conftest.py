from collections.abc import AsyncIterator, Callable

import httpx
import pytest
from fastapi import FastAPI

from sijil_cp.main import create_app
from sijil_cp.settings import CpSettings

# Ports nothing listens on: readiness must report these as down.
CLOSED_DB = "postgresql+asyncpg://sijil:sijil@127.0.0.1:1/sijil_cp"
CLOSED_REDIS = "redis://127.0.0.1:1/0"


def make_settings(**overrides: str) -> CpSettings:
    values = {
        "region": "ma-cas",
        "environment": "test",
        "database_url": CLOSED_DB,
        "redis_url": CLOSED_REDIS,
    } | overrides
    return CpSettings.model_validate(values)


@pytest.fixture
def client_for() -> Callable[[FastAPI], httpx.AsyncClient]:
    def _make(app: FastAPI) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://cp")

    return _make


@pytest.fixture
async def client(
    client_for: Callable[[FastAPI], httpx.AsyncClient],
) -> AsyncIterator[httpx.AsyncClient]:
    async with client_for(create_app(make_settings())) as c:
        yield c
