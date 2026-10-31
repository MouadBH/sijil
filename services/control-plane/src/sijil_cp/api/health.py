from typing import Literal

import redis.asyncio as aioredis
from fastapi import APIRouter, Request, Response, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from sijil_cp import __version__
from sijil_cp.settings import CpSettings

router = APIRouter(prefix="/health", tags=["health"])

CheckState = Literal["up", "down"]


class Health(BaseModel):
    status: Literal["ok"]
    region: str
    version: str


class Readiness(BaseModel):
    status: Literal["ok", "unavailable"]
    checks: dict[str, CheckState]


@router.get("")
async def health(request: Request) -> Health:
    settings: CpSettings = request.app.state.settings
    return Health(status="ok", region=settings.region, version=__version__)


async def _database_up(engine: AsyncEngine) -> bool:
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:
        return False
    return True


async def _redis_up(url: str) -> bool:
    client = aioredis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
    try:
        return bool(await client.ping())
    except Exception:
        return False
    finally:
        await client.aclose()


@router.get("/ready", responses={503: {"model": Readiness}})
async def ready(request: Request, response: Response) -> Readiness:
    settings: CpSettings = request.app.state.settings
    checks: dict[str, CheckState] = {
        "database": "up" if await _database_up(request.app.state.engine) else "down",
        "redis": "up" if await _redis_up(str(settings.redis_url)) else "down",
    }
    if all(state == "up" for state in checks.values()):
        return Readiness(status="ok", checks=checks)
    response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return Readiness(status="unavailable", checks=checks)
