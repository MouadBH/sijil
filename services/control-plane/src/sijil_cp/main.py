from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI
from redis.asyncio import Redis

from sijil_core.crypto import LocalKeyService
from sijil_cp import __version__
from sijil_cp.api import auth, health
from sijil_cp.auth import csrf
from sijil_cp.auth.csrf import CsrfMiddleware
from sijil_cp.db import make_engine, make_sessionmaker
from sijil_cp.errors import register_error_handlers
from sijil_cp.settings import CpSettings


def create_app(settings: CpSettings) -> FastAPI:
    engine = make_engine(settings)
    redis = Redis.from_url(str(settings.redis_url))

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        await redis.aclose()
        await engine.dispose()

    app = FastAPI(
        title="Sijil control plane",
        version=__version__,
        openapi_url="/v1/openapi.json",
        docs_url="/v1/docs",
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.engine = engine
    app.state.sessionmaker = make_sessionmaker(engine)
    app.state.redis = redis
    app.state.keys = LocalKeyService(settings.master_key_bytes())
    register_error_handlers(app)
    app.add_middleware(CsrfMiddleware)

    v1 = APIRouter(prefix="/v1")
    v1.include_router(health.router)
    v1.include_router(csrf.router)
    v1.include_router(auth.router)
    app.include_router(v1)
    return app


def run() -> None:
    import uvicorn

    settings = CpSettings()
    uvicorn.run(create_app(settings), host="0.0.0.0", port=8000, proxy_headers=True)  # noqa: S104
