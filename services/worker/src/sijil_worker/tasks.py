import asyncio
from collections.abc import Callable
from typing import Any
from uuid import UUID

import asyncpg
import docker
from botocore.exceptions import BotoCoreError, ClientError
from celery import Task, current_app, shared_task
from celery.utils.time import get_exponential_backoff_interval
from docker.errors import APIError
from sqlalchemy.exc import OperationalError

from sijil_core.crypto import LocalKeyService
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.paperless import (
    PaperlessClient,
    PaperlessError,
    PaperlessUnavailable,
)
from sijil_core.storage import S3ObjectStore, StorageSettings
from sijil_worker.ingest import (
    ConsumeFailed,
    WorkspaceNotActive,
    ingest_document,
    requeue_stuck_ingest,
)
from sijil_worker.provisioning import provision_workspace, record_failure
from sijil_worker.reconcile import reconcile_tenants
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import WorkerSettings

PROVISION_MAX_RETRIES = 3
RETRY_BACKOFF_MAX_S = 600
# Ingest outlasts a Paperless that is down for a while: 30 s, 1 min, 2 min ... capped at 15 min
# (full jitter), eight times. After that the document is `failed` and audited.
INGEST_MAX_RETRIES = 8
INGEST_BACKOFF_FACTOR_S = 30
INGEST_BACKOFF_MAX_S = 900

# Failures worth running the (idempotent) provisioning again for. Everything else is final.
_TRANSIENT: tuple[type[BaseException], ...] = (
    PaperlessUnavailable,
    OSError,  # connection refused/reset, timeouts, the Docker socket's transport errors
    asyncpg.PostgresConnectionError,
    asyncpg.CannotConnectNowError,  # Postgres starting up or shutting down
    OperationalError,  # SQLAlchemy's wrapper for a lost or refused database connection
)


def is_transient(exc: BaseException) -> bool:
    # APIError is an OSError (via requests): only the Engine's own 5xx are transient.
    if isinstance(exc, APIError):
        return exc.is_server_error()
    return isinstance(exc, _TRANSIENT)


def is_ingest_transient(exc: BaseException) -> bool:
    """Failures worth running ingest again for. Everything else settles the document."""
    if isinstance(exc, FileNotFoundError):
        return False  # the original is gone: retrying cannot bring it back
    if isinstance(exc, PaperlessError):
        return isinstance(exc, PaperlessUnavailable)  # 400/401/403/404 are final
    if isinstance(exc, WorkspaceNotActive | ConsumeFailed | ClientError | BotoCoreError):
        return True
    return is_transient(exc)


@shared_task(name="sijil.ping")
def ping() -> dict[str, Any]:
    return {"pong": True, "region": current_app.conf.sijil_region}


async def _provision(
    job_id: str, workspace_id: str, will_retry: Callable[[Exception], bool]
) -> None:
    settings = WorkerSettings()
    engine = make_engine(settings.database_url)
    sessions = make_sessionmaker(engine)
    client: docker.DockerClient | None = None
    try:
        try:
            client = docker.from_env()
        except Exception as exc:
            await record_failure(
                sessions,
                UUID(workspace_id),
                UUID(job_id),
                "docker_client",
                final=not will_retry(exc),
            )
            raise
        await provision_workspace(
            UUID(workspace_id),
            UUID(job_id),
            sessions=sessions,
            keys=LocalKeyService(settings.master_key_bytes()),
            runtime=DockerRuntime(client, settings),
            paperless=PaperlessClient,
            settings=settings,
            will_retry=will_retry,
        )
    finally:
        if client is not None:
            client.close()
        await engine.dispose()


@shared_task(
    bind=True,
    name="sijil.provision_workspace",
    queue="provisioning",
    max_retries=PROVISION_MAX_RETRIES,
)
def provision_workspace_task(self: "Task[Any, Any]", job_id: str, workspace_id: str) -> None:
    retries = int(self.request.retries or 0)

    def will_retry(exc: Exception) -> bool:
        return is_transient(exc) and retries < PROVISION_MAX_RETRIES

    try:
        asyncio.run(_provision(job_id, workspace_id, will_retry))
    except Exception as exc:
        if not will_retry(exc):
            raise
        countdown = get_exponential_backoff_interval(
            factor=1, retries=retries, maximum=RETRY_BACKOFF_MAX_S, full_jitter=True
        )
        raise self.retry(exc=exc, countdown=countdown) from exc


@shared_task(name="sijil.reconcile_tenants")
def reconcile_tenants_task() -> list[str]:
    """Re-attach compose services to tenant networks (also run once when the worker starts)."""
    client = docker.from_env()
    try:
        return reconcile_tenants(client, WorkerSettings())
    finally:
        client.close()


async def _ingest(
    ledger_id: str,
    workspace_id: str,
    will_retry: Callable[[Exception], bool],
    retry_after_s: float,
) -> None:
    settings = WorkerSettings()
    engine = make_engine(settings.database_url)
    try:
        await ingest_document(
            UUID(ledger_id),
            sessions=make_sessionmaker(engine),
            keys=LocalKeyService(settings.master_key_bytes()),
            store=S3ObjectStore(StorageSettings()),
            paperless=PaperlessClient,
            settings=settings,
            workspace_id=UUID(workspace_id),
            will_retry=will_retry,
            retry_after_s=retry_after_s,
        )
    finally:
        await engine.dispose()


@shared_task(
    bind=True,
    name="sijil.ingest_document",
    queue="ingest",
    max_retries=INGEST_MAX_RETRIES,
)
def ingest_document_task(self: "Task[Any, Any]", ledger_id: str, workspace_id: str) -> None:
    retries = int(self.request.retries or 0)

    def will_retry(exc: Exception) -> bool:
        return is_ingest_transient(exc) and retries < INGEST_MAX_RETRIES

    # Chosen up front so the ledger records exactly when Celery will retry, and the requeue
    # sweep leaves the row alone until then.
    countdown = get_exponential_backoff_interval(
        factor=INGEST_BACKOFF_FACTOR_S,
        retries=retries,
        maximum=INGEST_BACKOFF_MAX_S,
        full_jitter=True,
    )
    try:
        asyncio.run(_ingest(ledger_id, workspace_id, will_retry, float(countdown)))
    except Exception as exc:
        if not will_retry(exc):
            raise
        raise self.retry(exc=exc, countdown=countdown) from exc


async def _requeue() -> int:
    settings = WorkerSettings()
    engine = make_engine(settings.database_url)

    async def enqueue(ledger_id: UUID, workspace_id: UUID) -> None:
        current_app.send_task(
            "sijil.ingest_document",
            kwargs={"ledger_id": str(ledger_id), "workspace_id": str(workspace_id)},
            queue="ingest",
        )

    try:
        return await requeue_stuck_ingest(make_sessionmaker(engine), enqueue)
    finally:
        await engine.dispose()


@shared_task(name="sijil.requeue_stuck_ingest")
def requeue_stuck_ingest_task() -> int:
    """Re-send ingest for rows still `queued` after 10 minutes (lost message, failed enqueue)."""
    return asyncio.run(_requeue())
