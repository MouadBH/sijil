from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI

from sijil_cp import __version__
from sijil_cp.api import health
from sijil_cp.db import make_engine
from sijil_cp.settings import CpSettings


def create_app(settings: CpSettings) -> FastAPI:
    engine = make_engine(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
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

    v1 = APIRouter(prefix="/v1")
    v1.include_router(health.router)
    app.include_router(v1)
    return app


def run() -> None:
    import uvicorn

    settings = CpSettings()
    uvicorn.run(create_app(settings), host="0.0.0.0", port=8000, proxy_headers=True)  # noqa: S104
