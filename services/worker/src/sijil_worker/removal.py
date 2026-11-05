"""The removal worker: an original deleted from the write-once store, after the ledger says so.

Product-owner ruling (2026-11-03): one single-purpose worker, `worker-removal`, on the internal
`core` network only, holds the one key that can delete an original (policy `sijil-removal`:
read a version's lock, delete a version, with the GOVERNANCE bypass; nothing else). The control
plane commits the decision (the Owner's pre-seal removal, `removed`; or the Owner's approved
deletion after retention, `deleting`) and sends `sijil.remove_original`. `remove_original`:

1. asserts the region, holding the ledger row FOR UPDATE (before the audit chain's lock);
2. proceeds only from `removed` (GOVERNANCE bypass) or `deleting` (no bypass: the lock must
   have expired) with `original_removed_at` still NULL. Any other state, or a removal already
   recorded, is logged and left alone: a forged or stale message never deletes;
3. `store.remove_original(ref, governance=...)`, which refuses (`RetentionActive`, no call) a
   version COMPLIANCE-locked until today or later; the store refuses it too. An absent version
   counts as removed, so a crash between the delete and the commit only repeats this step;
4. sets `original_removed_at` (and, from `deleting`, `state = deleted` and `deleted_at`) and
   audits `document.original_removed` `{mode: "governance" | "expired"}`, then commits;
5. sends `sijil.purge_document` (the Paperless copy and the index rows) to the main worker.

A store error propagates for the task to classify (sijil_worker.tasks): transient ones are
retried; a final one is audited `document.removal_failed` `{code}` by `record_removal_failed`
and the state stays as it is. `requeue_unremoved` re-sends the removal for rows still waiting.
"""

import logging
from datetime import datetime
from typing import Final, Protocol
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core.db.models import (
    ActorType,
    DocumentLedger,
    DocumentState,
    Organisation,
    Workspace,
)
from sijil_core.ingest import Enqueue
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.settings import CoreSettings
from sijil_core.storage import ObjectRef
from sijil_worker.pending import PendingExtractions

log = logging.getLogger(__name__)

REQUEUE_BATCH: Final = 50
_WAITING = (DocumentState.removed, DocumentState.deleting)


class RemovalStore(Protocol):
    """What removal needs of the object store (`ObjectStore` satisfies it)."""

    async def remove_original(self, ref: ObjectRef, *, governance: bool) -> None: ...


async def _locked_row(
    session: AsyncSession, ledger_id: UUID, workspace_id: UUID, settings: CoreSettings
) -> DocumentLedger | None:
    """The ledger row FOR UPDATE, or None when its region is not served here. LookupError
    when the document is unknown or belongs to another workspace."""
    found = (
        await session.execute(
            select(DocumentLedger, Organisation.region)
            .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
            .join(Organisation, Organisation.id == Workspace.org_id)
            .where(DocumentLedger.id == ledger_id)
            .with_for_update(of=DocumentLedger)
        )
    ).first()
    if found is None or found[0].workspace_id != workspace_id:
        raise LookupError("document not found")
    ledger, region = found
    try:
        assert_region(region, settings)
    except RegionMismatch:
        log.warning("removal of document %s skipped: region %s not served", ledger_id, region)
        return None
    return ledger


async def remove_original(
    ledger_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    store: RemovalStore,
    settings: CoreSettings,
    now: datetime,
    workspace_id: UUID,
    purge: Enqueue,
) -> bool:
    """See the module docstring. True when this run removed the original; False when there
    was nothing to do here. Store errors propagate for the caller to classify and retry."""
    async with sessions() as session:
        ledger = await _locked_row(session, ledger_id, workspace_id, settings)
        if ledger is None:
            return False
        if ledger.state not in _WAITING or ledger.original_removed_at is not None:
            log.info(
                "removal of document %s skipped: state %s, original removed at %s",
                ledger_id, ledger.state.value, ledger.original_removed_at,
            )  # fmt: skip
            return False
        governance = ledger.state is DocumentState.removed
        await store.remove_original(
            ObjectRef(ledger.object_key, ledger.object_version_id), governance=governance
        )
        ledger.original_removed_at = now
        if ledger.state is DocumentState.deleting:
            ledger.state = DocumentState.deleted
            ledger.deleted_at = now
        await audit.append(
            session,
            workspace_id=workspace_id,
            action="document.original_removed",
            actor_type=ActorType.system,
            actor_id=None,
            target_type="document",
            target_id=str(ledger_id),
            data={"mode": "governance" if governance else "expired"},
            now=now,
        )
        await session.commit()
    try:
        await purge(ledger_id, workspace_id)
    except Exception as exc:
        # Recorded already: a failure here must not read as a failed removal.
        log.error("purge of document %s not sent: %s", ledger_id, type(exc).__name__)
    return True


async def record_removal_failed(
    ledger_id: UUID,
    workspace_id: UUID,
    code: str,
    *,
    sessions: async_sessionmaker[AsyncSession],
    settings: CoreSettings,
    now: datetime,
) -> bool:
    """Audit `document.removal_failed` `{code}`; the state is left as it is. False (nothing
    recorded) for an unknown document, another workspace's, or another region's."""
    async with sessions() as session:
        try:
            ledger = await _locked_row(session, ledger_id, workspace_id, settings)
        except LookupError:
            return False
        if ledger is None:
            return False
        await audit.append(
            session,
            workspace_id=workspace_id,
            action="document.removal_failed",
            actor_type=ActorType.system,
            actor_id=None,
            target_type="document",
            target_id=str(ledger_id),
            data={"code": code},
            now=now,
        )
        await session.commit()
    return True


async def requeue_unremoved(
    sessions: async_sessionmaker[AsyncSession],
    send: Enqueue,
    *,
    region: str,
    pending: PendingExtractions,
    limit: int = REQUEUE_BATCH,
) -> int:
    """Re-send `remove_original` for this region's `removed` and `deleting` rows whose original
    is not yet recorded removed (a lost message, a store that was down), in any workspace state
    (a closing workspace's removal still completes), oldest decision first, at most `limit`,
    skipping those in flight (`remove:pending:<id>`). A failed send is logged and the sweep goes
    on. Returns how many were sent."""
    decided = func.coalesce(DocumentLedger.deletion_approved_at, DocumentLedger.removed_at)
    async with sessions() as session:
        rows = (
            await session.execute(
                select(DocumentLedger.id, DocumentLedger.workspace_id)
                .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(
                    Organisation.region == region,
                    DocumentLedger.state.in_(_WAITING),
                    DocumentLedger.original_removed_at.is_(None),
                )
                .order_by(decided, DocumentLedger.id)
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
            log.warning("removal re-send of document %s failed: %s", ledger_id, type(exc).__name__)
    return sent
