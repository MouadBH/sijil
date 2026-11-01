"""Pushes an accepted original from the object store into the workspace's own Paperless.

The ledger row is the state machine: `queued -> sending -> consumed | unsupported | failed`.
Each step uses its own short database session, so no transaction is held open while waiting on
the object store or Paperless. A transient failure puts the row back to `queued` with an error
code and re-raises, so the caller can retry. When the caller's retries run out:
- a transient cause (Paperless or the workspace unavailable, a consume task that does not finish
  in time, the store or the database unreachable) is never final: the row goes back to `queued`
  with a long delay (1 h, doubling per exhausted cycle up to 24 h), audited once per cycle as
  `document.ingest_delayed`, and the requeue sweep sends it again when the delay is over;
- any other cause makes the row `failed`, audited as `document.ingest_failed`.
`consumed`, `unsupported` and `failed` are final: a redelivery of the task is a no-op.

Paperless 3.4.1 does not refuse duplicates (it only flags them), so before uploading, ingest
looks the file's SHA-256 up in Paperless and adopts the existing document. That makes a retry
after a lost response, or the same bytes arriving by two routes, harmless.

The filename sent to Paperless is derived from the hash and the sniffed kind: the user's own
filename is not kept anywhere in our databases. Errors record only a machine code: never a
token, a Paperless message or a response body.
"""

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core.crypto import Envelope, KeyService, open_envelope
from sijil_core.db.base import utcnow
from sijil_core.db.models import (
    ActorType,
    CredentialKind,
    DocumentLedger,
    IngestState,
    Organisation,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.filetypes import FileKind
from sijil_core.ingest import Enqueue
from sijil_core.paperless import (
    PaperlessClient,
    PaperlessError,
    PaperlessForbidden,
    PaperlessTask,
    PaperlessUnavailable,
    PaperlessUnsupportedType,
)
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.retention import region_local_date
from sijil_core.settings import CoreSettings
from sijil_core.storage import ObjectRef, ObjectStore

log = logging.getLogger(__name__)

POLL_TIMEOUT_S = 300.0
POLL_INTERVAL_S = 2.0
REQUEUE_AFTER = timedelta(minutes=10)
REQUEUE_BATCH = 500
# Celery retries a run this many times (the first run plus these make one cycle of attempts).
INGEST_MAX_RETRIES = 8
ATTEMPTS_PER_CYCLE = INGEST_MAX_RETRIES + 1
# After a cycle runs out on a transient cause, the row waits this long, doubling per cycle.
DELAY_FIRST = timedelta(hours=1)
DELAY_MAX = timedelta(hours=24)
# A claim older than this is abandoned (the worker died or hung): another run may take over.
CLAIM_LEASE = timedelta(seconds=POLL_TIMEOUT_S) + timedelta(minutes=5)

UNSUPPORTED = "ingest.unsupported_format"
_EXTENSION = {
    FileKind.pdf: "pdf",
    FileKind.jpeg: "jpg",
    FileKind.png: "png",
    FileKind.tiff: "tiff",
    FileKind.heic: "heic",
    FileKind.docx: "docx",
    FileKind.xlsx: "xlsx",
    FileKind.xml: "xml",
    FileKind.eml: "eml",
}
# Stable substrings of what Paperless 3.4.1 says when it cannot consume a type (observed, see
# tests/integration/test_ingest_formats.py): an upload refusal `File type text/xml not
# supported`, or a consume failure `cannot identify image file` for an undecodable image.
_UNSUPPORTED_MARKERS = ("not supported", "unsupported", "cannot identify image file")
_DUPLICATE_MARKER = "duplicate"
_ABSENT_CODES = {"404", "NoSuchKey", "NoSuchVersion", "NotFound"}
_TASK_DONE = {"SUCCESS", "FAILURE", "REVOKED"}


class WorkspaceNotActive(Exception):  # noqa: N818
    """The workspace is not `active` (yet, or any more): try again later."""


class MissingCredential(LookupError):
    """The workspace has no service token."""


class ConsumeFailed(Exception):  # noqa: N818
    """Paperless failed to consume the file for a reason that is not 'unsupported'."""


class _Unsupported(Exception):  # noqa: N818
    """Paperless cannot consume this file type. Final, never retried."""


def _never_retried(_exc: Exception) -> bool:
    return False


def is_transient_cause(exc: BaseException) -> bool:
    """A cause that goes away by itself: ingest never gives up on the document for it."""
    if isinstance(exc, FileNotFoundError):
        return False  # the original is gone from the store: waiting cannot bring it back
    return isinstance(
        exc,
        PaperlessUnavailable  # down, 5xx, or a consume task not finished in time
        | WorkspaceNotActive
        | ClientError  # the object store (an absent object is FileNotFoundError, above)
        | BotoCoreError
        | OSError  # connection refused/reset, timeouts
        | asyncpg.PostgresConnectionError
        | asyncpg.CannotConnectNowError
        | OperationalError,
    )


def delay_after(attempts: int, per_cycle: int = ATTEMPTS_PER_CYCLE) -> timedelta:
    """The wait after the cycle that `attempts` (as counted on the row) ran out in."""
    cycle = max(1, -(-attempts // per_cycle))
    factor: int = 2 ** min(cycle - 1, 16)
    return min(DELAY_FIRST * factor, DELAY_MAX)


def error_code(exc: BaseException) -> str:
    """The machine code stored on the ledger row for a failure."""
    if isinstance(exc, RegionMismatch):
        return "ingest.region_mismatch"
    if isinstance(exc, WorkspaceNotActive):
        return "ingest.workspace_not_active"
    if isinstance(exc, MissingCredential):
        return "ingest.no_credential"
    if isinstance(exc, ConsumeFailed):
        return "ingest.consume_failed"
    if isinstance(exc, FileNotFoundError):
        return "ingest.original_missing"
    if isinstance(exc, PaperlessUnavailable):
        return "ingest.paperless_unavailable"
    if isinstance(exc, PaperlessForbidden):
        return "ingest.paperless_forbidden"
    if isinstance(exc, PaperlessError):
        return "ingest.paperless_error"
    if isinstance(exc, ClientError | BotoCoreError):
        return "ingest.storage_unavailable"
    if isinstance(exc, LookupError):
        return "ingest.not_found"
    return "ingest.error"


@dataclass(frozen=True)
class _Claim:
    """What a run learned when it won the row: the lease stamp is its proof of ownership."""

    claimed_at: datetime
    workspace_id: UUID
    sha256: str
    kind: FileKind
    ref: ObjectRef
    ingested_at: datetime
    task_id: str | None


@dataclass(frozen=True)
class _Job:
    claim: _Claim
    paperless_url: str
    token: str
    title: str


async def _claim(
    sessions: async_sessionmaker[AsyncSession],
    ledger_id: UUID,
    expected_workspace: UUID | None,
    lease: timedelta,
) -> _Claim | None:
    """Compare-and-set `queued` (or `sending` with an expired lease) to `sending`.

    None when another run holds the row or it is settled (`consumed`, `unsupported`, `failed`
    are terminal for every automatic path). Raises LookupError when the row does not exist.
    """
    now = utcnow()
    scope = [DocumentLedger.id == ledger_id]
    if expected_workspace is not None:
        scope.append(DocumentLedger.workspace_id == expected_workspace)
    async with sessions() as session:
        row = (
            await session.execute(
                update(DocumentLedger)
                .where(
                    *scope,
                    or_(
                        DocumentLedger.ingest_state == IngestState.queued,
                        and_(
                            DocumentLedger.ingest_state == IngestState.sending,
                            DocumentLedger.ingest_claimed_at < now - lease,
                        ),
                    ),
                )
                .values(
                    ingest_state=IngestState.sending,
                    ingest_claimed_at=now,
                    ingest_attempts=DocumentLedger.ingest_attempts + 1,
                )
                .returning(
                    DocumentLedger.workspace_id,
                    DocumentLedger.sha256,
                    DocumentLedger.content_kind,
                    DocumentLedger.object_key,
                    DocumentLedger.object_version_id,
                    DocumentLedger.ingested_at,
                    DocumentLedger.paperless_task_id,
                )
                .execution_options(synchronize_session=False)
            )
        ).first()
        if row is None:
            exists = await session.scalar(select(DocumentLedger.id).where(*scope))
            if exists is None:
                raise LookupError("document not found")
            return None
        await session.commit()
    return _Claim(
        claimed_at=now,
        workspace_id=row.workspace_id,
        sha256=row.sha256,
        kind=FileKind(row.content_kind),
        ref=ObjectRef(row.object_key, row.object_version_id),
        ingested_at=row.ingested_at,
        task_id=row.paperless_task_id,
    )


async def _prepare(
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    settings: CoreSettings,
    claim: _Claim,
) -> _Job:
    async with sessions() as session:
        workspace = await session.get(Workspace, claim.workspace_id)
        org = None if workspace is None else await session.get(Organisation, workspace.org_id)
        if workspace is None or org is None:
            raise LookupError("workspace not found")
        # Before anything leaves the database: this worker only serves its own region.
        assert_region(org.region, settings)
        if workspace.state is not WorkspaceState.active or not workspace.paperless_url:
            raise WorkspaceNotActive
        credential = await session.scalar(
            select(TenantCredential).where(
                TenantCredential.workspace_id == workspace.id,
                TenantCredential.kind == CredentialKind.service_token,
            )
        )
        if credential is None:
            raise MissingCredential("no service token")
        token = open_envelope(
            Envelope.from_bytes(credential.envelope),
            keys,
            f"{workspace.id}:{CredentialKind.service_token}",
        ).decode()
        day = region_local_date(claim.ingested_at, org.region).isoformat()
        return _Job(
            claim=claim,
            paperless_url=workspace.paperless_url,
            token=token,
            # Until M3 extraction names it: no filename is kept, so date and kind it is.
            title=f"{day} {claim.kind.value}",
        )


async def _read_original(store: ObjectStore, ref: ObjectRef) -> bytes:
    """Stream the original from the store (chunk by chunk) into one buffer for the upload."""
    buffer = bytearray()
    try:
        async for chunk in store.open(ref):
            buffer += chunk
    except ClientError as exc:
        if str(exc.response.get("Error", {}).get("Code", "")) in _ABSENT_CODES:
            raise FileNotFoundError(ref.key) from None
        raise
    return bytes(buffer)


async def _await_task(
    client: PaperlessClient, task_id: str, timeout_s: float, interval_s: float, *, resume: bool
) -> PaperlessTask:
    deadline = time.monotonic() + timeout_s
    while True:
        task = await client.task(task_id)
        if task.status in _TASK_DONE or (resume and not task.known):
            return task
        if time.monotonic() >= deadline:
            raise PaperlessUnavailable("consume task not finished in time")
        await asyncio.sleep(interval_s)


async def _set_task_id(
    sessions: async_sessionmaker[AsyncSession], ledger_id: UUID, claim: _Claim, task_id: str | None
) -> None:
    async with sessions() as session:
        ledger = await session.get(DocumentLedger, ledger_id, with_for_update=True)
        if ledger is not None and ledger.ingest_claimed_at == claim.claimed_at:
            ledger.paperless_task_id = task_id
            await session.commit()


async def _resolve(
    client: PaperlessClient,
    sha256: str,
    task_id: str,
    poll_timeout_s: float,
    poll_interval_s: float,
    *,
    resume: bool,
) -> int | None:
    """The document id a consume task produced. None (resume only) means: upload afresh."""
    task = await _await_task(client, task_id, poll_timeout_s, poll_interval_s, resume=resume)
    if resume and not task.known:
        return None
    if task.status == "SUCCESS":
        doc_id = task.related_document
        if doc_id is None:
            doc_id = await client.find_document_by_checksum(sha256)
        if doc_id is not None:
            return doc_id
    else:
        message = (task.error or "").lower()
        if _DUPLICATE_MARKER in message:
            # A Paperless configured to refuse duplicates: the earlier copy is the document.
            doc_id = await client.find_document_by_checksum(sha256)
            if doc_id is not None:
                return doc_id
        elif any(marker in message for marker in _UNSUPPORTED_MARKERS):
            raise _Unsupported
    if resume:
        return None
    raise ConsumeFailed("consume task failed")


async def _push(
    job: _Job,
    ledger_id: UUID,
    client: PaperlessClient,
    sessions: async_sessionmaker[AsyncSession],
    store: ObjectStore,
    poll_timeout_s: float,
    poll_interval_s: float,
) -> int:
    """The Paperless document id of the original; uploads at most once per run.

    Order: the document already in Paperless (by SHA-256), then the task a previous run started
    (resumed, so a lost response or a crash mid-poll never uploads twice), then a fresh upload.
    """
    claim = job.claim
    existing = await client.find_document_by_checksum(claim.sha256)
    if existing is not None:
        return existing
    if claim.task_id:
        resumed = await _resolve(
            client, claim.sha256, claim.task_id, poll_timeout_s, poll_interval_s, resume=True
        )
        if resumed is not None:
            return resumed
        await _set_task_id(sessions, ledger_id, claim, None)  # unknown or failed: start over
    content = await _read_original(store, claim.ref)
    filename = f"{claim.sha256[:12]}.{_EXTENSION[claim.kind]}"
    try:
        task_id = await client.upload(content, filename, title=job.title)
    except PaperlessUnsupportedType:
        raise _Unsupported from None
    await _set_task_id(sessions, ledger_id, claim, task_id)
    doc_id = await _resolve(
        client, claim.sha256, task_id, poll_timeout_s, poll_interval_s, resume=False
    )
    if doc_id is None:  # unreachable: only a resumed task may answer None
        raise ConsumeFailed("consume task failed")
    return doc_id


async def _settle(
    sessions: async_sessionmaker[AsyncSession],
    ledger_id: UUID,
    claim: _Claim,
    *,
    doc_id: int | None = None,
    unsupported: bool = False,
    code: str | None = None,
    final: bool = True,
    delayed: bool = False,
    retry_after_s: float = 0.0,
) -> None:
    """Record the outcome (and audit it) in one transaction.

    Only the run that still owns the lease may write: a row another run took over, or one that
    is already settled, is left alone (so `failed` is never reopened).
    """
    now = utcnow()
    async with sessions() as session:
        ledger = await session.get(DocumentLedger, ledger_id, with_for_update=True)
        if (
            ledger is None
            or ledger.ingest_state is not IngestState.sending
            or ledger.ingest_claimed_at != claim.claimed_at
        ):
            return
        action: str | None
        data: dict[str, str | int | None]
        ledger.ingest_claimed_at = None
        if doc_id is not None:
            ledger.ingest_state = IngestState.consumed
            ledger.paperless_doc_id = doc_id
            ledger.ingest_error_code = None
            ledger.ingest_next_attempt_at = None
            action = "document.ingested"
            data = {"paperless_doc_id": doc_id, "sha256": claim.sha256}
        elif unsupported:
            ledger.ingest_state = IngestState.unsupported
            ledger.ingest_error_code = UNSUPPORTED
            ledger.ingest_next_attempt_at = None
            action, data = "document.ingest_failed", {"code": UNSUPPORTED, "sha256": claim.sha256}
        elif delayed:
            # Retries ran out on a transient cause: keep the document in the pipeline.
            next_at = now + delay_after(ledger.ingest_attempts)
            ledger.ingest_state = IngestState.queued
            ledger.ingest_error_code = code
            ledger.ingest_next_attempt_at = next_at
            action = "document.ingest_delayed"
            data = {
                "code": code,
                "next_attempt_at": next_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            }
        elif final:
            ledger.ingest_state = IngestState.failed
            ledger.ingest_error_code = code
            ledger.ingest_next_attempt_at = None
            action, data = "document.ingest_failed", {"code": code, "sha256": claim.sha256}
        else:
            ledger.ingest_state = IngestState.queued
            ledger.ingest_error_code = code
            # Celery retries at the same moment, so the sweep leaves the row alone until then.
            ledger.ingest_next_attempt_at = now + timedelta(seconds=retry_after_s)
            action, data = None, {}
        if action is not None:
            await audit.append(
                session,
                workspace_id=ledger.workspace_id,
                action=action,
                actor_type=ActorType.system,
                actor_id=None,
                target_type="document",
                target_id=str(ledger_id),
                data=data,
                now=now,
            )
        await session.commit()


async def ingest_document(
    ledger_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    store: ObjectStore,
    paperless: Callable[..., PaperlessClient],
    settings: CoreSettings,
    poll_timeout_s: float = POLL_TIMEOUT_S,
    poll_interval_s: float = POLL_INTERVAL_S,
    workspace_id: UUID | None = None,
    will_retry: Callable[[Exception], bool] = _never_retried,
    retry_after_s: float = 0.0,
) -> None:
    """`will_retry(exc)` says whether the caller will run this again after `exc`, `retry_after_s`
    seconds from now (the Celery countdown, recorded so the requeue sweep agrees with it)."""
    claim = await _claim(
        sessions, ledger_id, workspace_id, timedelta(seconds=poll_timeout_s) + timedelta(minutes=5)
    )
    if claim is None:
        return  # settled, or another run holds it: a redelivery changes nothing
    try:
        job = await _prepare(sessions, keys, settings, claim)
        async with paperless(job.paperless_url, token=job.token) as client:
            doc_id = await _push(
                job, ledger_id, client, sessions, store, poll_timeout_s, poll_interval_s
            )
        await _settle(sessions, ledger_id, claim, doc_id=doc_id)
    except _Unsupported:
        await _settle(sessions, ledger_id, claim, unsupported=True)
    except Exception as exc:
        # Only a machine code is recorded; the exception carries no secrets by contract.
        code = error_code(exc)
        log.error("ingest of document %s failed: %s", ledger_id, code)
        retrying = not isinstance(exc, RegionMismatch) and will_retry(exc)
        await _settle(
            sessions,
            ledger_id,
            claim,
            code=code,
            final=not retrying,
            delayed=not retrying and is_transient_cause(exc),
            retry_after_s=retry_after_s,
        )
        raise


async def requeue_stuck_ingest(
    sessions: async_sessionmaker[AsyncSession],
    enqueue: Enqueue,
    *,
    now: datetime | None = None,
    older_than: timedelta = REQUEUE_AFTER,
    limit: int = REQUEUE_BATCH,
) -> int:
    """Send `ingest_document` again for rows that nothing is working on.

    Those are: `queued` for over `older_than` with no next attempt recorded (a lost broker
    message or a failed enqueue); `queued` whose next attempt is over `older_than` past (a
    Celery retry that never came, or the long delay after exhausted retries being over); and
    `sending` whose lease expired (a dead worker). Rows waiting on a Celery countdown or a
    delay are skipped. Safe to repeat: the claim admits one run per row. Returns how many were
    sent.
    """
    at = now or utcnow()
    cutoff = at - older_than
    async with sessions() as session:
        rows = (
            await session.execute(
                select(DocumentLedger.id, DocumentLedger.workspace_id)
                .where(
                    or_(
                        and_(
                            DocumentLedger.ingest_state == IngestState.queued,
                            or_(
                                and_(
                                    DocumentLedger.ingest_next_attempt_at.is_(None),
                                    DocumentLedger.ingested_at < cutoff,
                                ),
                                DocumentLedger.ingest_next_attempt_at < cutoff,
                            ),
                        ),
                        and_(
                            DocumentLedger.ingest_state == IngestState.sending,
                            DocumentLedger.ingest_claimed_at < at - CLAIM_LEASE,
                        ),
                    )
                )
                .order_by(DocumentLedger.ingested_at)
                .limit(limit)
            )
        ).all()
    sent = 0
    for ledger_id, workspace_id in rows:
        try:
            await enqueue(ledger_id, workspace_id)
        except Exception:
            log.exception("requeue of ingest for document %s failed", ledger_id)
            continue
        sent += 1
    return sent
