from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI
from redis.asyncio import Redis

from sijil_core.crypto import LocalKeyService
from sijil_core.index import IndexEngines
from sijil_core.storage import S3ObjectStore
from sijil_cp import __version__
from sijil_cp.api import auth, documents, health, jobs, members, orgs, review, workspaces
from sijil_cp.auth import csrf
from sijil_cp.auth.csrf import CsrfMiddleware
from sijil_cp.db import make_engine, make_sessionmaker
from sijil_cp.enqueue import CeleryEnqueuer
from sijil_cp.errors import register_error_handlers
from sijil_cp.settings import CpSettings


def create_app(settings: CpSettings) -> FastAPI:
    engine = make_engine(settings)
    redis = Redis.from_url(str(settings.redis_url))
    sessionmaker = make_sessionmaker(engine)
    keys = LocalKeyService(settings.master_key_bytes())

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            try:
                index: IndexEngines = app.state.index
                await index.aclose()
            finally:
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
    app.state.sessionmaker = sessionmaker
    app.state.redis = redis
    app.state.keys = keys
    # Engines to the workspaces' index databases (field values); built per workspace on use.
    app.state.index = IndexEngines(settings, keys, sessionmaker)
    # Tests replace this with a recording stand-in; production sends tasks to the worker.
    app.state.enqueuer = CeleryEnqueuer(str(settings.redis_url))
    # The original store (object lock); tests swap in an in-memory double.
    app.state.store = S3ObjectStore(settings)
    # The same bucket through the sealing key (retention only): the seal path alone uses it.
    app.state.seal_store = S3ObjectStore(settings.seal_storage())
    register_error_handlers(app)
    app.add_middleware(CsrfMiddleware)

    v1 = APIRouter(prefix="/v1")
    v1.include_router(health.router)
    v1.include_router(csrf.router)
    v1.include_router(auth.router)
    v1.include_router(orgs.router)
    v1.include_router(workspaces.router)
    v1.include_router(members.router)
    v1.include_router(jobs.router)
    v1.include_router(documents.router)
    v1.include_router(review.router)
    app.include_router(v1)
    return app


def run() -> None:
    import uvicorn

    settings = CpSettings()
    uvicorn.run(create_app(settings), host="0.0.0.0", port=8000, proxy_headers=True)  # noqa: S104
