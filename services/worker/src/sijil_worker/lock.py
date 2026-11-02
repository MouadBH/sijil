"""The seal worker: a sealed original switched to COMPLIANCE, after the ledger seal commits.

Product-owner ruling: the internet-facing control plane holds no key that can replace an object
lock. It commits the seal (ledger and index) and sends `sijil.lock_sealed` to this worker, the
only runtime service holding the sealing key, on its own queue (`seal`) and with no tenant or
AI network. `lock_sealed`:
1. asserts the region and reads the ledger row; anything not sealed, or already locked
   (`object_locked_at` set), is a no-op;
2. `store.seal(ref, ledger.retention_until)`: the committed retention, never recomputed here;
   idempotent at the same date, and a COMPLIANCE lock is only ever extended;
3. records `object_locked_at` and audits `document.object_locked` `{retention_until}`, in one
   transaction holding the ledger row FOR UPDATE (before the audit chain's lock).

A crash between 2 and 3 only repeats step 2 on the next run. Until 3, the original stays under
its GOVERNANCE lock at the provisional date (no running service's key can delete or shorten
it); `requeue_unlocked` re-sends the lock for sealed rows still unlocked after two minutes, and
the drift check flags them after an hour (`object_lock_pending`).
"""

import logging
from datetime import date, datetime, timedelta
from typing import Final, Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core.db.base import utcnow
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

REQUEUE_AFTER: Final = timedelta(minutes=2)
REQUEUE_BATCH: Final = 50


class SealStore(Protocol):
    """What locking needs of the object store (`ObjectStore` satisfies it)."""

    async def seal(self, ref: ObjectRef, retain_until: date) -> None: ...


async def lock_sealed(
    ledger_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    store: SealStore,
    settings: CoreSettings,
    now: datetime,
    workspace_id: UUID | None = None,
) -> bool:
    """See the module docstring. True when this run locked the original; False when there
    was nothing to do here. Store errors propagate for the caller to classify and retry."""
    async with sessions() as session:
        found = (
            await session.execute(
                select(DocumentLedger, Organisation.region)
                .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(DocumentLedger.id == ledger_id)
            )
        ).first()
    if found is None:
        raise LookupError("document not found")
    ledger, region = found
    if workspace_id is not None and ledger.workspace_id != workspace_id:
        raise LookupError("document not found")
    try:
        assert_region(region, settings)
    except RegionMismatch:
        log.warning("lock of document %s skipped: region %s not served", ledger_id, region)
        return False
    if ledger.state is not DocumentState.sealed or ledger.object_locked_at is not None:
        return False
    retain_until = ledger.retention_until  # committed with the seal; frozen by the trigger
    await store.seal(ObjectRef(ledger.object_key, ledger.object_version_id), retain_until)
    async with sessions() as session:
        locked = await session.scalar(
            select(DocumentLedger)
            .where(DocumentLedger.id == ledger_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if locked is None or locked.object_locked_at is not None:
            return False  # a concurrent run recorded it first
        locked.object_locked_at = now
        await audit.append(
            session,
            workspace_id=locked.workspace_id,
            action="document.object_locked",
            actor_type=ActorType.system,
            actor_id=None,
            target_type="document",
            target_id=str(ledger_id),
            data={"retention_until": retain_until.isoformat()},
            now=now,
        )
        await session.commit()
    return True


async def requeue_unlocked(
    sessions: async_sessionmaker[AsyncSession],
    send: Enqueue,
    *,
    region: str,
    pending: PendingExtractions,
    now: datetime | None = None,
    older_than: timedelta = REQUEUE_AFTER,
    limit: int = REQUEUE_BATCH,
) -> int:
    """Re-send `lock_sealed` for this region's sealed rows still without `object_locked_at`
    whose seal is over `older_than` old (a lost message, a store that was down), oldest first,
    at most `limit`, skipping those with a lock in flight (`lock:pending:<id>`). A failed send
    is logged and the sweep goes on. Returns how many were sent."""
    cutoff = (now or utcnow()) - older_than
    async with sessions() as session:
        rows = (
            await session.execute(
                select(DocumentLedger.id, DocumentLedger.workspace_id)
                .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(
                    Organisation.region == region,
                    DocumentLedger.state == DocumentState.sealed,
                    DocumentLedger.object_locked_at.is_(None),
                    DocumentLedger.sealed_at < cutoff,
                )
                .order_by(DocumentLedger.sealed_at, DocumentLedger.id)
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
            log.warning("lock re-send of document %s failed: %s", ledger_id, type(exc).__name__)
    return sent
