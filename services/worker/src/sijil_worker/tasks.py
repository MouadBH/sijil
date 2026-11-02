import asyncio
import logging
import random
from collections.abc import Callable
from typing import Any
from uuid import UUID

import asyncpg
import docker
from botocore.exceptions import BotoCoreError, ClientError
from celery import Task, current_app, shared_task
from celery.utils.time import get_exponential_backoff_interval
from docker.errors import APIError
from redis.asyncio import Redis
from sqlalchemy.exc import OperationalError

from sijil_core.crypto import LocalKeyService
from sijil_core.db.base import utcnow
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.index import IndexEngines
from sijil_core.ollama import OllamaClient, OllamaError, OllamaUnavailable
from sijil_core.paperless import (
    PaperlessClient,
    PaperlessError,
    PaperlessUnavailable,
)
from sijil_core.region import RegionMismatch
from sijil_core.storage import S3ObjectStore, StorageSettings
from sijil_worker.drift import (
    ENGINE_UNREACHABLE,
    drift_check,
    enqueue_drift_checks,
    record_check_failed,
)
from sijil_worker.extract import (
    extract_document,
    record_extraction_failed,
    requeue_unextracted,
)
from sijil_worker.ingest import (
    INGEST_MAX_RETRIES,
    ConsumeFailed,
    WorkspaceNotActive,
    ingest_document,
    is_transient_cause,
    requeue_stuck_ingest,
)
from sijil_worker.lock import lock_sealed, requeue_unlocked
from sijil_worker.mirror import mirror_fields
from sijil_worker.pending import PendingExtractions, PendingOp
from sijil_worker.provisioning import provision_workspace, record_failure
from sijil_worker.reconcile import reconcile_tenants
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import ExtractionSettings, SealSettings, WorkerSettings

log = logging.getLogger(__name__)

PROVISION_MAX_RETRIES = 3
RETRY_BACKOFF_MAX_S = 600
# Ingest outlasts a Paperless that is down for a while: 30 s, 1 min, 2 min ... capped at 15 min
# (full jitter), INGEST_MAX_RETRIES times. After that a transient cause waits hours and is sent
# again by the requeue sweep (see sijil_worker.ingest); any other cause is `failed`.
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
    """Failures worth a Celery retry. Paperless 400/401/403/404 and a missing original are not.

    A consume failure is retried within the cycle (the next run resumes or re-uploads), but it
    is not a transient cause: once the retries are spent, the document is `failed`.
    """
    return isinstance(exc, ConsumeFailed) or is_transient_cause(exc)


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


EXTRACT_TASK = ("sijil.extract_document", "extraction")


async def _send_raw_extract(ledger_id: UUID, workspace_id: UUID) -> None:
    name, queue = EXTRACT_TASK
    current_app.send_task(
        name,
        kwargs={"ledger_id": str(ledger_id), "workspace_id": str(workspace_id)},
        queue=queue,
    )


async def _with_pending(op: PendingOp) -> None:
    """Runs `op` on the broker Redis's set of extractions in flight."""
    redis = Redis.from_url(str(ExtractionSettings().redis_url))
    try:
        await op(PendingExtractions(redis))
    finally:
        await redis.aclose()


def _pending_quietly(op: PendingOp) -> None:
    """Best effort: a key that is not cleared or refreshed only expires (1 h)."""
    try:
        asyncio.run(_with_pending(op))
    except Exception as exc:
        log.warning("extraction pending key update failed: %s", type(exc).__name__)


async def _send_extract(ledger_id: UUID, workspace_id: UUID) -> None:
    """Sends the extraction unless one is already in flight for the document."""

    async def op(pending: PendingExtractions) -> None:
        await pending.send(_send_raw_extract, ledger_id, workspace_id)

    await _with_pending(op)


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
            enqueue_extract=_send_extract,
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
        return await requeue_stuck_ingest(
            make_sessionmaker(engine), enqueue, region=settings.region
        )
    finally:
        await engine.dispose()


@shared_task(name="sijil.requeue_stuck_ingest")
def requeue_stuck_ingest_task() -> int:
    """Re-send ingest for rows nothing is working on (lost message, delay over, dead worker)."""
    return asyncio.run(_requeue())


# Extraction outlasts an Ollama or Paperless that is down for a while: 30 s, 1 min, 2 min ...
# capped at 15 min, 8 times, with equal jitter (half fixed, half random), so the retries always
# span at least ~30 min. After that the document is flagged `extraction_failed` (audited) so that
# it goes to review instead of waiting for ever.
EXTRACT_MAX_RETRIES = 8
# A retry's pending key lives for its countdown plus this, so it outlasts the wait.
PENDING_MARGIN_S = 900
EXTRACT_BACKOFF_FACTOR_S = 30
EXTRACT_BACKOFF_MAX_S = 900


def extract_countdown(retries: int, rand: Callable[[], float] = random.random) -> float:  # noqa: S311
    """Seconds before retry number `retries + 1`: in [base / 2, base], base = 30 s * 2^retries
    capped at 900 s. Unlike full jitter it never picks a near-zero wait."""
    base = float(min(EXTRACT_BACKOFF_FACTOR_S * 2 ** min(retries, 16), EXTRACT_BACKOFF_MAX_S))
    return base / 2 + rand() * base / 2


def is_extract_transient(exc: BaseException) -> bool:
    """Ollama or Paperless down, slow or 5xx, the workspace paused, the database or the object
    store (extending the lock at filing) unreachable."""
    if isinstance(exc, OllamaError):
        return isinstance(exc, OllamaUnavailable)
    if isinstance(exc, ClientError):
        return _store_error_transient(exc)
    if isinstance(exc, BotoCoreError):
        return True  # connection, timeout, endpoint: the store is unreachable
    return isinstance(exc, WorkspaceNotActive) or is_transient(exc)


_STORE_TRANSIENT_CODES = frozenset(
    {"SlowDown", "RequestTimeout", "InternalError", "ServiceUnavailable"}
)


def _store_error_transient(exc: ClientError) -> bool:
    """Throttling and server errors pass; a refusal (NoSuchKey, AccessDenied, InvalidRequest
    ...) does not, so the document goes to review instead of retrying for ever."""
    code = str(exc.response.get("Error", {}).get("Code", ""))
    status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
    return code in _STORE_TRANSIENT_CODES or code.startswith("Throttling") or status >= 500


def extraction_error_code(exc: BaseException) -> str:
    """The machine code audited with `document.extraction_failed`."""
    if isinstance(exc, OllamaUnavailable):
        return "extraction.ai_unavailable"
    if isinstance(exc, OllamaError):
        return "extraction.ai_error"
    if isinstance(exc, PaperlessUnavailable):
        return "extraction.paperless_unavailable"
    if isinstance(exc, PaperlessError):
        return "extraction.paperless_error"
    if isinstance(exc, WorkspaceNotActive):
        return "extraction.workspace_not_active"
    if isinstance(exc, RegionMismatch):
        return "extraction.region_mismatch"
    return "extraction.error"


async def _extract(ledger_id: str, workspace_id: str | None) -> None:
    # Runs in the extraction worker, which has extraction settings only.
    settings = ExtractionSettings()
    engine = make_engine(settings.database_url)
    keys = LocalKeyService(settings.master_key_bytes())
    sessions = make_sessionmaker(engine)
    engines = IndexEngines(settings, keys, sessions)
    try:
        async with OllamaClient(settings.ollama_url, settings.ai_allowed_hosts) as ollama:
            await extract_document(
                UUID(ledger_id),
                sessions=sessions,
                keys=keys,
                index=engines,
                paperless=PaperlessClient,
                ollama=ollama,
                store=S3ObjectStore(StorageSettings()),
                settings=settings,
                now=utcnow(),
                workspace_id=UUID(workspace_id) if workspace_id else None,
            )
    finally:
        await engines.aclose()
        await engine.dispose()


async def _extract_failed(ledger_id: str, workspace_id: str | None, code: str) -> None:
    settings = ExtractionSettings()
    engine = make_engine(settings.database_url)
    sessions = make_sessionmaker(engine)
    engines = IndexEngines(settings, LocalKeyService(settings.master_key_bytes()), sessions)
    try:
        await record_extraction_failed(
            UUID(ledger_id),
            code,
            sessions=sessions,
            index=engines,
            store=S3ObjectStore(StorageSettings()),
            settings=settings,
            now=utcnow(),
            workspace_id=UUID(workspace_id) if workspace_id else None,
        )
    finally:
        await engines.aclose()
        await engine.dispose()


@shared_task(
    bind=True,
    name="sijil.extract_document",
    queue="extraction",
    max_retries=EXTRACT_MAX_RETRIES,
)
def extract_document_task(
    self: "Task[Any, Any]", ledger_id: str, workspace_id: str | None = None
) -> None:
    """Fields of one consumed document by the in-region model (see sijil_worker.extract)."""
    retries = int(self.request.retries or 0)
    lid = UUID(ledger_id)

    async def clear(pending: PendingExtractions) -> None:
        await pending.clear(lid)

    try:
        asyncio.run(_extract(ledger_id, workspace_id))
    except Exception as exc:
        if is_extract_transient(exc) and retries < EXTRACT_MAX_RETRIES:
            countdown = extract_countdown(retries)

            # The retry is still this document's message in flight: keep (and stretch) the key.
            async def refresh(pending: PendingExtractions) -> None:
                await pending.refresh(lid, int(countdown) + PENDING_MARGIN_S)

            _pending_quietly(refresh)
            raise self.retry(exc=exc, countdown=countdown) from exc
        # Retries ran out, or a final error: the document must reach review, never wait for
        # ever in `received`.
        try:
            asyncio.run(_extract_failed(ledger_id, workspace_id, extraction_error_code(exc)))
        finally:
            _pending_quietly(clear)
        raise
    _pending_quietly(clear)  # routed, or nothing to do


async def _requeue_extract() -> int:
    settings = ExtractionSettings()
    engine = make_engine(settings.database_url)
    redis = Redis.from_url(str(settings.redis_url))
    try:
        return await requeue_unextracted(
            make_sessionmaker(engine),
            _send_raw_extract,
            region=settings.region,
            pending=PendingExtractions(redis),
        )
    finally:
        await redis.aclose()
        await engine.dispose()


@shared_task(name="sijil.requeue_unextracted")
def requeue_unextracted_task() -> int:
    """Re-send extraction for consumed rows still `received` (a lost message)."""
    return asyncio.run(_requeue_extract())


DRIFT_MAX_RETRIES = 5
DRIFT_BACKOFF_FACTOR_S = 300
DRIFT_BACKOFF_MAX_S = 3600


def is_drift_transient(exc: BaseException) -> bool:
    """Paperless or the object store unreachable: retry later, flag nothing."""
    if isinstance(exc, PaperlessError):
        return isinstance(exc, PaperlessUnavailable)
    if isinstance(exc, ClientError | BotoCoreError):
        return True
    return is_transient(exc)


async def _drift(workspace_id: str) -> None:
    settings = WorkerSettings()
    engine = make_engine(settings.database_url)
    try:
        await drift_check(
            UUID(workspace_id),
            sessions=make_sessionmaker(engine),
            keys=LocalKeyService(settings.master_key_bytes()),
            store=S3ObjectStore(StorageSettings()),
            paperless=PaperlessClient,
            settings=settings,
            now=utcnow(),
        )
    finally:
        await engine.dispose()


async def _drift_failed(workspace_id: str, code: str) -> None:
    settings = WorkerSettings()
    engine = make_engine(settings.database_url)
    try:
        await record_check_failed(make_sessionmaker(engine), UUID(workspace_id), code, now=utcnow())
    finally:
        await engine.dispose()


@shared_task(bind=True, name="sijil.drift_check", max_retries=DRIFT_MAX_RETRIES)
def drift_check_task(self: "Task[Any, Any]", workspace_id: str) -> None:
    """Integrity drift check of one workspace; retried later when a dependency is down."""
    try:
        asyncio.run(_drift(workspace_id))
    except Exception as exc:
        if not is_drift_transient(exc):
            raise
        if int(self.request.retries or 0) >= DRIFT_MAX_RETRIES:
            # Retries ran out: a check that never runs must not be silent.
            asyncio.run(_drift_failed(workspace_id, ENGINE_UNREACHABLE))
            raise
        countdown = get_exponential_backoff_interval(
            factor=DRIFT_BACKOFF_FACTOR_S,
            retries=int(self.request.retries or 0),
            maximum=DRIFT_BACKOFF_MAX_S,
            full_jitter=True,
        )
        raise self.retry(exc=exc, countdown=countdown) from exc


async def _drift_all() -> int:
    settings = WorkerSettings()
    engine = make_engine(settings.database_url)

    async def enqueue(workspace_id: UUID) -> None:
        current_app.send_task("sijil.drift_check", kwargs={"workspace_id": str(workspace_id)})

    try:
        return await enqueue_drift_checks(make_sessionmaker(engine), settings, enqueue)
    finally:
        await engine.dispose()


@shared_task(name="sijil.drift_check_all")
def drift_check_all_task() -> int:
    """Nightly: fan out one drift check per active workspace of this region."""
    return asyncio.run(_drift_all())


# Mirror of a sealed document's fields into its Paperless: 30 s, 1 min ... capped at 15 min,
# MIRROR_MAX_RETRIES times. Paperless keeps a copy only; the sealed record is ours already.
MIRROR_MAX_RETRIES = 8
MIRROR_BACKOFF_FACTOR_S = 30
MIRROR_BACKOFF_MAX_S = 900


def is_mirror_transient(exc: BaseException) -> bool:
    """Paperless or a database down or slow, or the workspace paused."""
    return isinstance(exc, WorkspaceNotActive) or is_transient(exc)


async def _mirror(ledger_id: str, workspace_id: str) -> None:
    # The main worker: its settings carry the index database's host and the master key.
    settings = WorkerSettings()
    engine = make_engine(settings.database_url)
    keys = LocalKeyService(settings.master_key_bytes())
    sessions = make_sessionmaker(engine)
    engines = IndexEngines(settings, keys, sessions)
    try:
        await mirror_fields(
            UUID(ledger_id),
            sessions=sessions,
            keys=keys,
            index=engines,
            paperless=PaperlessClient,
            settings=settings,
            now=utcnow(),
            workspace_id=UUID(workspace_id),
        )
    finally:
        await engines.aclose()
        await engine.dispose()


@shared_task(bind=True, name="sijil.mirror_fields", queue="default", max_retries=MIRROR_MAX_RETRIES)
def mirror_fields_task(self: "Task[Any, Any]", ledger_id: str, workspace_id: str) -> None:
    """A sealed document's fields as custom fields in its Paperless (see sijil_worker.mirror)."""
    try:
        asyncio.run(_mirror(ledger_id, workspace_id))
    except Exception as exc:
        retries = int(self.request.retries or 0)
        if not is_mirror_transient(exc) or retries >= MIRROR_MAX_RETRIES:
            raise
        countdown = get_exponential_backoff_interval(
            factor=MIRROR_BACKOFF_FACTOR_S,
            retries=retries,
            maximum=MIRROR_BACKOFF_MAX_S,
            full_jitter=True,
        )
        raise self.retry(exc=exc, countdown=countdown) from exc


# The compliance lock (seal worker): 30 s, 1 min ... capped at 15 min, LOCK_MAX_RETRIES times.
# A permanent refusal is logged by its code and left to the sweep and the drift check: the
# original stays GOVERNANCE-locked meanwhile, so nothing is lost by waiting.
LOCK_MAX_RETRIES = 8
LOCK_BACKOFF_FACTOR_S = 30
LOCK_BACKOFF_MAX_S = 900


def is_lock_transient(exc: BaseException) -> bool:
    """The object store or the database unreachable, throttling or failing (5xx)."""
    if isinstance(exc, ClientError):
        return _store_error_transient(exc)
    if isinstance(exc, BotoCoreError):
        return True
    return is_transient(exc)


def _store_code(exc: BaseException) -> str:
    if isinstance(exc, ClientError):
        return str(exc.response.get("Error", {}).get("Code", "")) or "ClientError"
    return type(exc).__name__


async def _with_lock_pending(op: PendingOp) -> None:
    redis = Redis.from_url(str(SealSettings().redis_url))
    try:
        await op(PendingExtractions(redis, prefix="lock"))
    finally:
        await redis.aclose()


def _pending_lock_quietly(op: PendingOp) -> None:
    """Best effort: a key that is not cleared or refreshed only expires (1 h)."""
    try:
        asyncio.run(_with_lock_pending(op))
    except Exception as exc:
        log.warning("lock pending key update failed: %s", type(exc).__name__)


async def _lock_sealed(ledger_id: str, workspace_id: str) -> None:
    settings = SealSettings()
    engine = make_engine(settings.database_url)
    try:
        await lock_sealed(
            UUID(ledger_id),
            sessions=make_sessionmaker(engine),
            store=S3ObjectStore(settings),
            settings=settings,
            now=utcnow(),
            workspace_id=UUID(workspace_id),
        )
    finally:
        await engine.dispose()


@shared_task(bind=True, name="sijil.lock_sealed", queue="seal", max_retries=LOCK_MAX_RETRIES)
def lock_sealed_task(self: "Task[Any, Any]", ledger_id: str, workspace_id: str) -> None:
    """A sealed original switched to COMPLIANCE at its committed retention (sijil_worker.lock)."""
    lid = UUID(ledger_id)

    async def clear(pending: PendingExtractions) -> None:
        await pending.clear(lid)

    try:
        asyncio.run(_lock_sealed(ledger_id, workspace_id))
    except Exception as exc:
        retries = int(self.request.retries or 0)
        if is_lock_transient(exc) and retries < LOCK_MAX_RETRIES:
            countdown = get_exponential_backoff_interval(
                factor=LOCK_BACKOFF_FACTOR_S,
                retries=retries,
                maximum=LOCK_BACKOFF_MAX_S,
                full_jitter=True,
            )

            async def refresh(pending: PendingExtractions) -> None:
                await pending.refresh(lid, int(countdown) + PENDING_MARGIN_S)

            _pending_lock_quietly(refresh)
            raise self.retry(exc=exc, countdown=countdown) from exc
        _pending_lock_quietly(clear)
        if is_lock_transient(exc):
            raise  # retries ran out: the sweep sends it again later
        # Permanent (e.g. AccessDenied): logged by its code only, never retried in a loop.
        log.error("compliance lock of document %s refused: %s", ledger_id, _store_code(exc))
        return
    _pending_lock_quietly(clear)


async def _requeue_unlocked() -> int:
    settings = WorkerSettings()
    engine = make_engine(settings.database_url)
    redis = Redis.from_url(str(settings.redis_url))

    async def send(ledger_id: UUID, workspace_id: UUID) -> None:
        current_app.send_task(
            "sijil.lock_sealed",
            kwargs={"ledger_id": str(ledger_id), "workspace_id": str(workspace_id)},
            queue="seal",
        )

    try:
        return await requeue_unlocked(
            make_sessionmaker(engine),
            send,
            region=settings.region,
            pending=PendingExtractions(redis, prefix="lock"),
        )
    finally:
        await redis.aclose()
        await engine.dispose()


@shared_task(name="sijil.requeue_unlocked")
def requeue_unlocked_task() -> int:
    """Re-send the compliance lock for sealed rows still unlocked (a lost message, a store
    that was down). Runs on the main worker: it only reads the ledger and sends tasks."""
    return asyncio.run(_requeue_unlocked())
