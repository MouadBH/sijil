"""Pushes an accepted original from the object store into the workspace's own Paperless.

The ledger row is the state machine: `queued -> sending -> consumed | unsupported | failed`.
Each step uses its own short database session, so no transaction is held open while waiting on
the object store or Paperless. A transient failure puts the row back to `queued` with an error
code and re-raises, so the caller can retry; once the caller will not retry, the row becomes
`failed` and the failure is audited. `consumed` and `unsupported` are final: a redelivery of
the task is a no-op.

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
from datetime import datetime, timedelta
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import select
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
class _Job:
    workspace_id: UUID
    sha256: str
    kind: FileKind
    ref: ObjectRef
    paperless_url: str
    token: str
    title: str


async def _load(
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    settings: CoreSettings,
    ledger_id: UUID,
    expected_workspace: UUID | None,
) -> _Job | None:
    """Validate the row and claim it (`sending`). None when there is nothing to do."""
    async with sessions() as session:
        ledger = await session.get(DocumentLedger, ledger_id, with_for_update=True)
        if ledger is None or (expected_workspace and ledger.workspace_id != expected_workspace):
            raise LookupError("document not found")
        if ledger.ingest_state in (IngestState.consumed, IngestState.unsupported):
            return None  # a redelivery (acks_late) or a duplicate message: already settled
        workspace = await session.get(Workspace, ledger.workspace_id)
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
        kind = FileKind(ledger.content_kind)
        ledger.ingest_state = IngestState.sending
        job = _Job(
            workspace_id=workspace.id,
            sha256=ledger.sha256,
            kind=kind,
            ref=ObjectRef(ledger.object_key, ledger.object_version_id),
            paperless_url=workspace.paperless_url,
            token=token,
            # Until M3 extraction names it: no filename is kept, so date and kind it is.
            title=f"{region_local_date(ledger.ingested_at, org.region).isoformat()} {kind.value}",
        )
        await session.commit()
        return job


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
    client: PaperlessClient, task_id: str, timeout_s: float, interval_s: float
) -> PaperlessTask:
    deadline = time.monotonic() + timeout_s
    while True:
        task = await client.task(task_id)
        if task.status in _TASK_DONE:
            return task
        if time.monotonic() >= deadline:
            raise PaperlessUnavailable("consume task not finished in time")
        await asyncio.sleep(interval_s)


async def _store_task_id(
    sessions: async_sessionmaker[AsyncSession], ledger_id: UUID, task_id: str
) -> None:
    async with sessions() as session:
        ledger = await session.get(DocumentLedger, ledger_id)
        if ledger is not None:
            ledger.paperless_task_id = task_id
            await session.commit()


async def _push(
    job: _Job,
    ledger_id: UUID,
    client: PaperlessClient,
    sessions: async_sessionmaker[AsyncSession],
    store: ObjectStore,
    poll_timeout_s: float,
    poll_interval_s: float,
) -> int:
    """The Paperless document id of the original, uploading it unless Paperless already has it."""
    existing = await client.find_document_by_checksum(job.sha256)
    if existing is not None:
        return existing
    content = await _read_original(store, job.ref)
    filename = f"{job.sha256[:12]}.{_EXTENSION[job.kind]}"
    try:
        task_id = await client.upload(content, filename, title=job.title)
    except PaperlessUnsupportedType:
        raise _Unsupported from None
    await _store_task_id(sessions, ledger_id, task_id)
    task = await _await_task(client, task_id, poll_timeout_s, poll_interval_s)
    if task.status == "SUCCESS":
        doc_id = task.related_document
        if doc_id is None:
            doc_id = await client.find_document_by_checksum(job.sha256)
        if doc_id is None:
            raise ConsumeFailed("consumed but no document found")
        return doc_id
    message = (task.error or "").lower()
    if _DUPLICATE_MARKER in message:
        # A Paperless configured to refuse duplicates: the earlier copy is the document.
        doc_id = await client.find_document_by_checksum(job.sha256)
        if doc_id is not None:
            return doc_id
    elif any(marker in message for marker in _UNSUPPORTED_MARKERS):
        raise _Unsupported
    raise ConsumeFailed("consume task failed")


async def _settle(
    sessions: async_sessionmaker[AsyncSession],
    ledger_id: UUID,
    sha256: str | None,
    *,
    doc_id: int | None = None,
    unsupported: bool = False,
    code: str | None = None,
    final: bool = True,
) -> None:
    """Record the outcome (and audit it) in one transaction. Settled rows are never changed."""
    async with sessions() as session:
        ledger = await session.get(DocumentLedger, ledger_id, with_for_update=True)
        if ledger is None or ledger.ingest_state in (IngestState.consumed, IngestState.unsupported):
            return
        action: str | None
        data: dict[str, str | int | None]
        if doc_id is not None:
            ledger.ingest_state = IngestState.consumed
            ledger.paperless_doc_id = doc_id
            ledger.ingest_error_code = None
            action, data = "document.ingested", {"paperless_doc_id": doc_id, "sha256": sha256}
        elif unsupported:
            ledger.ingest_state = IngestState.unsupported
            ledger.ingest_error_code = UNSUPPORTED
            action, data = "document.ingest_failed", {"code": UNSUPPORTED, "sha256": sha256}
        elif final:
            ledger.ingest_state = IngestState.failed
            ledger.ingest_error_code = code
            action, data = "document.ingest_failed", {"code": code, "sha256": sha256}
        else:
            ledger.ingest_state = IngestState.queued
            ledger.ingest_error_code = code
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
                now=utcnow(),
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
) -> None:
    """`will_retry(exc)` says whether the caller will run this again after `exc`."""
    sha256: str | None = None
    try:
        job = await _load(sessions, keys, settings, ledger_id, workspace_id)
        if job is None:
            return
        sha256 = job.sha256
        async with paperless(job.paperless_url, token=job.token) as client:
            doc_id = await _push(
                job, ledger_id, client, sessions, store, poll_timeout_s, poll_interval_s
            )
        await _settle(sessions, ledger_id, sha256, doc_id=doc_id)
    except _Unsupported:
        await _settle(sessions, ledger_id, sha256, unsupported=True)
    except Exception as exc:
        # Only a machine code is recorded; the exception carries no secrets by contract.
        code = error_code(exc)
        log.error("ingest of document %s failed: %s", ledger_id, code)
        final = isinstance(exc, RegionMismatch) or not will_retry(exc)
        await _settle(sessions, ledger_id, sha256, code=code, final=final)
        raise


async def requeue_stuck_ingest(
    sessions: async_sessionmaker[AsyncSession],
    enqueue: Enqueue,
    *,
    now: datetime | None = None,
    older_than: timedelta = REQUEUE_AFTER,
    limit: int = REQUEUE_BATCH,
) -> int:
    """Send `ingest_document` again for rows still `queued` after `older_than`.

    Covers a lost broker message or a failed enqueue at acceptance. Safe to repeat: ingest is
    a no-op on settled rows and adopts what Paperless already holds. Returns how many were sent.
    """
    cutoff = (now or utcnow()) - older_than
    async with sessions() as session:
        rows = (
            await session.execute(
                select(DocumentLedger.id, DocumentLedger.workspace_id)
                .where(
                    DocumentLedger.ingest_state == IngestState.queued,
                    DocumentLedger.ingested_at < cutoff,
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
