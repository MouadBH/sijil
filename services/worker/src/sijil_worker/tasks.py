import asyncio
from typing import Any
from uuid import UUID

import docker
from celery import current_app, shared_task

from sijil_core.crypto import LocalKeyService
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.paperless import PaperlessClient, PaperlessUnavailable
from sijil_worker.provisioning import provision_workspace
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import WorkerSettings


@shared_task(name="sijil.ping")
def ping() -> dict[str, Any]:
    return {"pong": True, "region": current_app.conf.sijil_region}


async def _provision(job_id: str, workspace_id: str) -> None:
    settings = WorkerSettings()
    engine = make_engine(settings.database_url)
    client = docker.from_env()
    try:
        await provision_workspace(
            UUID(workspace_id),
            UUID(job_id),
            sessions=make_sessionmaker(engine),
            keys=LocalKeyService(settings.master_key_bytes()),
            runtime=DockerRuntime(client, settings),
            paperless=PaperlessClient,
        )
    finally:
        client.close()
        await engine.dispose()


@shared_task(
    name="sijil.provision_workspace",
    queue="provisioning",
    autoretry_for=(PaperlessUnavailable,),
    max_retries=3,
    retry_backoff=True,
)
def provision_workspace_task(job_id: str, workspace_id: str) -> None:
    asyncio.run(_provision(job_id, workspace_id))
