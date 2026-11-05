"""A removed or deleted document's Paperless copies and index rows, purged once its original is
gone.

The removal worker sends `sijil.purge_document` (queue `default`, this main worker: it holds the
master key and the index database settings) after it recorded `original_removed_at`; ingest
sends it again when it stops on a removed row after an upload. `purge_document`:

1. asserts the region before anything else is read or sent;
2. proceeds only for a ledger row in `removed` or `deleted` with `original_removed_at` set: any
   other row, or a forged or early message, is logged and left alone;
3. works in Paperless with the tenant admin's credential (a token obtained with its password):
   the service user has no delete right and keeps none;
4. when the ledger recorded no Paperless id and an upload may still be consuming, stops with
   ConsumeInFlight, before deleting or recording anything: a copy could appear after a purge
   that found none. "May still be consuming" is a recorded consume task Paperless still runs,
   or, with no task known, an ingest claim or a pending retry within CONSUME_WINDOW (an upload
   whose task id was never recorded: ingest stopped on the removal, or failed, right after
   it). The task sends the purge again later without spending a retry;
5. finds every copy: the ledger's id, the document a finished consume task produced, every live
   document with this SHA-256 (all pages: 3.4.1 keeps duplicates), and every trashed document
   with ingest's file name `{sha256[:12]}.{ext}` (the trash has no checksum filter);
6. deletes the live ones (a 404 counts as deleted) and empties each from the trash (400: not in
   it, i.e. emptied already), then lists the trash again: a copy still there raises
   PurgeIncomplete (transient), so `document.purged` is recorded only once Paperless keeps none;
7. purges the index rows (`index.purge_document`, the sealed row too only for `deleted`);
8. audits `document.purged` `{paperless}`: whether Paperless held a copy this run removed.

Every step is idempotent, so a retry or a second message repeats it harmlessly. A workspace that
is not active (paused, hibernated, closing) raises WorkspaceNotActive, retried later; Paperless
down raises PaperlessUnavailable likewise. Nothing here touches the object store.
`requeue_unpurged` re-sends the purge for rows that never recorded `document.purged`.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Final
from uuid import UUID

from sqlalchemy import String, cast, exists, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core import index as index_db
from sijil_core.crypto import Envelope, KeyService, open_envelope
from sijil_core.db.models import (
    ActorType,
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    DocumentState,
    IngestState,
    Organisation,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.filetypes import FileKind
from sijil_core.index import IndexEngines, credential_context
from sijil_core.ingest import Enqueue
from sijil_core.paperless import PaperlessClient, PaperlessError
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.settings import CoreSettings
from sijil_worker.ingest import (
    CLAIM_LEASE,
    MissingCredential,
    WorkspaceNotActive,
    upload_filename,
)
from sijil_worker.pending import PendingExtractions
from sijil_worker.runtime.base import ADMIN_USERNAME

log = logging.getLogger(__name__)

PURGED: Final = "document.purged"
REQUEUE_BATCH: Final = 50
# How long after an ingest claim (or a failure) its upload may still be consuming: ingest's own
# lease, its poll timeout plus margin.
CONSUME_WINDOW: Final = CLAIM_LEASE
_PURGEABLE = (DocumentState.removed, DocumentState.deleted)
_TASK_DONE = frozenset({"SUCCESS", "FAILURE", "REVOKED"})


class ConsumeInFlight(Exception):  # noqa: N818 - a state, not an error
    """An upload of this document may still be consuming in Paperless: purge later."""


class PurgeIncomplete(Exception):  # noqa: N818
    """A copy is still in Paperless's trash after it was emptied. Transient: purge again."""


@dataclass(frozen=True)
class _Target:
    workspace: Workspace
    paperless_url: str
    admin_password: str
    paperless_doc_id: int | None
    sha256: str
    filename: str
    sealed: bool
    ingest_state: IngestState
    task_id: str | None
    claimed_at: datetime | None
    next_attempt_at: datetime | None


async def _target(
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    settings: CoreSettings,
    ledger_id: UUID,
    workspace_id: UUID,
) -> _Target | None:
    async with sessions() as session:
        found = (
            await session.execute(
                select(DocumentLedger, Workspace, Organisation.region)
                .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(DocumentLedger.id == ledger_id)
            )
        ).first()
        if found is None or found[1].id != workspace_id:
            raise LookupError("document not found")
        ledger, workspace, region = found
        try:
            assert_region(region, settings)
        except RegionMismatch:
            log.warning("purge of document %s skipped: region %s not served", ledger_id, region)
            return None
        if ledger.state not in _PURGEABLE or ledger.original_removed_at is None:
            log.info(
                "purge of document %s skipped: state %s, original removed at %s",
                ledger_id, ledger.state.value, ledger.original_removed_at,
            )  # fmt: skip
            return None
        if workspace.state is not WorkspaceState.active or not workspace.paperless_url:
            raise WorkspaceNotActive  # transient: paused, hibernated or closing, retried later
        kind = CredentialKind.admin_password
        credential = await session.scalar(
            select(TenantCredential).where(
                TenantCredential.workspace_id == workspace.id, TenantCredential.kind == kind
            )
        )
        if credential is None:
            raise MissingCredential("no admin password")
        password = open_envelope(
            Envelope.from_bytes(credential.envelope),
            keys,
            credential_context(workspace.id, kind),
        ).decode()
        return _Target(
            workspace=workspace,
            paperless_url=workspace.paperless_url,
            admin_password=password,
            paperless_doc_id=ledger.paperless_doc_id,
            sha256=ledger.sha256,
            filename=upload_filename(ledger.sha256, FileKind(ledger.content_kind)),
            sealed=ledger.state is DocumentState.deleted,
            ingest_state=ledger.ingest_state,
            task_id=ledger.paperless_task_id,
            claimed_at=ledger.ingest_claimed_at,
            next_attempt_at=ledger.ingest_next_attempt_at,
        )


def _recent_upload(target: _Target, now: datetime) -> bool:
    """An ingest claim, or a failure after which a retry is pending, within CONSUME_WINDOW: an
    upload sent then may still be consuming. A retry is recorded `now + countdown`, so a failure
    within the window has its next attempt after `now - CONSUME_WINDOW`."""
    if target.ingest_state not in (IngestState.queued, IngestState.sending):
        return False
    since = now - CONSUME_WINDOW
    return (target.claimed_at is not None and target.claimed_at > since) or (
        target.next_attempt_at is not None and target.next_attempt_at > since
    )


async def _copies(client: PaperlessClient, target: _Target, now: datetime) -> set[int]:
    """Every live copy Paperless holds (see the module docstring, steps 4 and 5)."""
    ids: set[int] = set()
    if target.paperless_doc_id is not None:
        ids.add(target.paperless_doc_id)
    else:
        task = await client.task(target.task_id) if target.task_id else None
        if task is not None and task.known and task.status not in _TASK_DONE:
            raise ConsumeInFlight
        if task is not None and task.status == "SUCCESS" and task.related_document is not None:
            ids.add(task.related_document)
        elif _recent_upload(target, now):
            raise ConsumeInFlight
    ids.update(await client.documents_with_checksum(target.sha256))
    return ids


async def _trashed(client: PaperlessClient, filename: str) -> set[int]:
    return {d.id for d in await client.trashed_documents() if d.original_file_name == filename}


async def _purge_paperless(
    target: _Target, paperless: Callable[..., PaperlessClient], now: datetime
) -> bool:
    """True when Paperless held a copy (live or in its trash) that this run removed."""
    async with paperless(target.paperless_url) as anonymous:
        token = await anonymous.obtain_token(ADMIN_USERNAME, target.admin_password)
    async with paperless(target.paperless_url, token=token) as client:
        live = await _copies(client, target, now)
        deleted: set[int] = set()
        for doc_id in sorted(live):
            try:
                await client.delete_document(doc_id)
            except PaperlessError as exc:
                if exc.status != 404:
                    raise
                continue  # absent: deleted already, perhaps still in the trash (found below)
            deleted.add(doc_id)
        emptied: set[int] = set()
        for doc_id in sorted(deleted | await _trashed(client, target.filename)):
            try:
                await client.empty_trash([doc_id])
            except PaperlessError as exc:
                if exc.status != 400:
                    raise
                continue  # not in the trash: emptied already
            emptied.add(doc_id)
        ours = deleted | emptied
        if any(
            d.id in ours or d.original_file_name == target.filename
            for d in await client.trashed_documents()
        ):
            raise PurgeIncomplete
        return bool(ours)


async def purge_document(
    ledger_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    index: IndexEngines,
    paperless: Callable[..., PaperlessClient],
    settings: CoreSettings,
    now: datetime,
    workspace_id: UUID,
) -> bool:
    """See the module docstring. True when this run purged; False when there was nothing to do
    here. ConsumeInFlight, Paperless, index and database errors propagate for the caller."""
    target = await _target(sessions, keys, settings, ledger_id, workspace_id)
    if target is None:
        return False
    in_paperless = await _purge_paperless(target, paperless, now)
    # After Paperless: a failure there leaves everything for the retry to find again.
    async with (await index.engine(target.workspace)).begin() as conn:
        await index_db.purge_document(conn, ledger_id, sealed=target.sealed)
    async with sessions() as session:
        await audit.append(
            session,
            workspace_id=workspace_id,
            action=PURGED,
            actor_type=ActorType.system,
            actor_id=None,
            target_type="document",
            target_id=str(ledger_id),
            data={"paperless": in_paperless},
            now=now,
        )
        await session.commit()
    return True


async def requeue_unpurged(
    sessions: async_sessionmaker[AsyncSession],
    send: Enqueue,
    *,
    region: str,
    pending: PendingExtractions,
    limit: int = REQUEUE_BATCH,
) -> int:
    """Re-send `purge_document` for this region's `removed` and `deleted` rows whose original
    is removed but that never recorded `document.purged` (a lost message, Paperless down for
    longer than the retries), in active workspaces only (any other would only retry), oldest
    removal first, at most `limit`, skipping those in flight (`purge:pending:<id>`). A failed
    send is logged and the sweep goes on. Returns how many were sent."""
    purged = exists().where(
        AuditEvent.workspace_id == DocumentLedger.workspace_id,
        AuditEvent.action == PURGED,
        AuditEvent.target_type == "document",
        AuditEvent.target_id == cast(DocumentLedger.id, String),
    )
    async with sessions() as session:
        rows = (
            await session.execute(
                select(DocumentLedger.id, DocumentLedger.workspace_id)
                .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(
                    Organisation.region == region,
                    Workspace.state == WorkspaceState.active,
                    DocumentLedger.state.in_(_PURGEABLE),
                    DocumentLedger.original_removed_at.is_not(None),
                    ~purged,
                )
                .order_by(DocumentLedger.original_removed_at, DocumentLedger.id)
                .limit(limit * 10)
            )
        ).all()
    sent = 0
    for ledger_id, ws_id in rows:
        if sent >= limit:
            break
        try:
            if await pending.send(send, ledger_id, ws_id):
                sent += 1
        except Exception as exc:
            log.warning("purge re-send of document %s failed: %s", ledger_id, type(exc).__name__)
    return sent
