import asyncio
import importlib
from typing import Any
from uuid import UUID

from celery import current_app, shared_task

from sijil_core.crypto import LocalKeyService
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.paperless import PaperlessClient, PaperlessUnavailable
from sijil_worker.provisioning import provision_workspace
from sijil_worker.runtime.base import TenantRuntime
from sijil_worker.settings import WorkerSettings


@shared_task(name="sijil.ping")
def ping() -> dict[str, Any]:
    return {"pong": True, "region": current_app.conf.sijil_region}


def _make_runtime(settings: WorkerSettings) -> TenantRuntime:
    # The Docker implementation arrives with the runtime task; imported lazily so the worker
    # boots (and every other task runs) without it.
    module = importlib.import_module("sijil_worker.runtime.docker")
    runtime: TenantRuntime = module.DockerRuntime(settings)
    return runtime


async def _provision(job_id: str, workspace_id: str) -> None:
    settings = WorkerSettings()
    engine = make_engine(settings.database_url)
    try:
        await provision_workspace(
            UUID(workspace_id),
            UUID(job_id),
            sessions=make_sessionmaker(engine),
            keys=LocalKeyService(settings.master_key_bytes()),
            runtime=_make_runtime(settings),
            paperless=PaperlessClient,
        )
    finally:
        await engine.dispose()


@shared_task(
    name="sijil.provision_workspace",
    autoretry_for=(PaperlessUnavailable,),
    max_retries=3,
    retry_backoff=True,
)
def provision_workspace_task(job_id: str, workspace_id: str) -> None:
    asyncio.run(_provision(job_id, workspace_id))
