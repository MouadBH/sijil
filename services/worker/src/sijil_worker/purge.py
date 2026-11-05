"""A removed or deleted document's Paperless copy and index rows, purged once its original is gone.

The removal worker sends `sijil.purge_document` (queue `default`, this main worker: it holds the
master key and the index database settings) after it recorded `original_removed_at`.
`purge_document`:

1. asserts the region before anything leaves the databases;
2. proceeds only for a ledger row in `removed` or `deleted` with `original_removed_at` set: any
   other row, or a forged or early message, is logged and left alone;
3. deletes the Paperless document with the tenant admin's credential (a token obtained with its
   password): the service user has no delete right and keeps none. The document is the one the
   ledger recorded or, when ingest stopped on the removal before recording it, the one Paperless
   holds with this SHA-256. A document already absent (404) counts as deleted;
4. empties it from Paperless's trash, so Paperless keeps no copy (400: not in the trash, i.e.
   already emptied);
5. purges the index rows (`index.purge_document`, the sealed row too for `deleted`);
6. audits `document.purged` `{paperless}`: whether Paperless held a copy this run removed.

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
    Organisation,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.index import IndexEngines, credential_context
from sijil_core.ingest import Enqueue
from sijil_core.paperless import PaperlessClient, PaperlessError
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.settings import CoreSettings
from sijil_worker.ingest import MissingCredential, WorkspaceNotActive
from sijil_worker.pending import PendingExtractions
from sijil_worker.runtime.base import ADMIN_USERNAME

log = logging.getLogger(__name__)

PURGED: Final = "document.purged"
REQUEUE_BATCH: Final = 50
_PURGEABLE = (DocumentState.removed, DocumentState.deleted)


@dataclass(frozen=True)
class _Target:
    workspace: Workspace
    paperless_url: str
    admin_password: str
    paperless_doc_id: int | None
    sha256: str
    sealed: bool


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
            sealed=ledger.state is DocumentState.deleted,
        )


async def _delete_in_paperless(target: _Target, paperless: Callable[..., PaperlessClient]) -> bool:
    """True when Paperless held a copy (live or in its trash) that this run removed."""
    async with paperless(target.paperless_url) as anonymous:
        token = await anonymous.obtain_token(ADMIN_USERNAME, target.admin_password)
    async with paperless(target.paperless_url, token=token) as client:
        doc_id = target.paperless_doc_id
        if doc_id is None:
            doc_id = await client.find_document_by_checksum(target.sha256)
        if doc_id is None:
            return False
        removed = True
        try:
            await client.delete_document(doc_id)
        except PaperlessError as exc:
            if exc.status != 404:
                raise
            removed = False  # absent: deleted already, perhaps still in the trash
        try:
            await client.empty_trash([doc_id])
        except PaperlessError as exc:
            if exc.status != 400:
                raise
            return removed  # not in the trash: emptied already
        return True


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
    here. Paperless, index and database errors propagate for the caller to classify."""
    target = await _target(sessions, keys, settings, ledger_id, workspace_id)
    if target is None:
        return False
    in_paperless = await _delete_in_paperless(target, paperless)
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
