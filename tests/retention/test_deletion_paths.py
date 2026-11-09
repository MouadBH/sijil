"""The Owner's approved deletion after retention is a path that deletes a document (CLAUDE.md
rule 4): from the API (layer 1) through the removal worker (layer 4), with the store's own lock
having the last word.

The ledger guard (layer 3: `deleting` only once `retention_until` is before today in the
region) is tests/retention/test_terminal_states.py::test_deleting_refused_before_retention_ends;
the RustFS refusal of the removal key on a COMPLIANCE version is
tests/integration/test_removal_key.py.
"""

import hashlib
import os
import uuid
from collections.abc import Awaitable, Callable
from datetime import date, timedelta
from typing import Any

import pytest
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    Membership,
    Role,
    Workspace,
)
from sijil_core.storage import ObjectRef
from tests.support.memory_store import MemoryObjectStore, StoredObject
from tests.support.worker_tasks import WorkerTasks, worker_tasks

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db

PAST = date(2020, 12, 31)
FAR = date(2036, 12, 31)


@pytest.fixture
async def owner(make_actor: MakeActor, db: AsyncSession, workspace: Workspace) -> Actor:
    actor = await make_actor("deletion-owner@example.com")
    db.add(Membership(user_id=actor.user_id, workspace_id=workspace.id, role=Role.owner))
    await db.commit()
    await actor.enable_totp()
    await actor.step_up()
    return actor


@pytest.fixture
def worker(
    cp_app: FastAPI, migrated_db: Any, memory_store: MemoryObjectStore,
    monkeypatch: pytest.MonkeyPatch,
) -> WorkerTasks:  # fmt: skip
    return worker_tasks(
        monkeypatch,
        database_url=migrated_db.app_url,
        redis_url=os.environ["SIJIL_TEST_REDIS_URL"],
        store=memory_store,
    )


async def _sealed(
    db: AsyncSession, store: MemoryObjectStore, ws: Workspace, owner: Actor, *,
    retention: date, lock_until: date,
) -> DocumentLedger:  # fmt: skip
    """A sealed row whose original the seal worker switched to COMPLIANCE until `lock_until`."""
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    sealed_at = owner.clock.now - timedelta(days=1)
    row = DocumentLedger(
        workspace_id=ws.id, sha256=sha, md5="0" * 32, size_bytes=6, content_kind="pdf",
        source=DocumentSource.upload, object_key=f"k/{sha}", object_version_id="v1",
        state=DocumentState.sealed, ingest_state=IngestState.consumed, paperless_doc_id=7,
        retention_until=retention, ingested_at=sealed_at, fields_sha256="f" * 64,
        sealed_at=sealed_at, sealed_by=owner.user_id, object_locked_at=sealed_at,
    )  # fmt: skip
    db.add(row)
    await db.commit()
    store.objects[ObjectRef(row.object_key, "v1")] = StoredObject(
        data=b"SAMPLE", mode="COMPLIANCE", retain_until=lock_until
    )
    return row


async def _fresh(db: AsyncSession, row: DocumentLedger) -> DocumentLedger:
    found = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == row.id)
        .execution_options(populate_existing=True)
    )
    assert found is not None
    return found


async def _events(db: AsyncSession, ws: Workspace) -> list[tuple[str, object]]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id, AuditEvent.target_type == "document")
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return [(e.action, e.data) for e in rows]


async def _approve(owner: Actor, ws: Workspace, row: DocumentLedger) -> None:
    r = await owner.client.post(
        f"/v1/workspaces/{ws.id}/retention/approve-deletion", json={"ids": [str(row.id)]}
    )
    assert (r.status_code, r.json()) == (202, {"approved": 1}), r.text


async def test_approve_deletion_of_sealed_after_retention(
    owner: Actor, workspace: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    enqueuer: Any, worker: WorkerTasks,
) -> None:  # fmt: skip
    """A sealed document whose retention and COMPLIANCE lock are both over: the approval is
    allowed, the removal worker deletes the original without any bypass, the row ends
    `deleted` and its Paperless copy and index rows are handed to the purge."""
    row = await _sealed(db, memory_store, workspace, owner, retention=PAST, lock_until=PAST)
    await _approve(owner, workspace, row)
    fresh = await _fresh(db, row)
    assert fresh.state is DocumentState.deleting
    assert fresh.sealed_at is not None  # the seal stays on the record
    assert enqueuer.removals == [(row.id, workspace.id)]

    [(ledger_id, ws_id)] = enqueuer.removals
    result = await worker.run(
        "sijil.remove_original", ledger_id=str(ledger_id), workspace_id=str(ws_id)
    )
    assert result.successful(), result.result
    assert ObjectRef(row.object_key, "v1") not in memory_store.objects
    fresh = await _fresh(db, row)
    assert fresh.state is DocumentState.deleted
    assert fresh.deleted_at is not None
    assert fresh.original_removed_at == fresh.deleted_at
    assert (fresh.sha256, fresh.object_key) == (row.sha256, row.object_key)  # the tombstone
    assert await _events(db, workspace) == [
        ("document.deletion_approved", {}),
        ("document.original_removed", {"mode": "expired"}),
    ]
    assert [(s.name, s.kwargs) for s in worker.sent] == [
        ("sijil.purge_document", {"ledger_id": str(row.id), "workspace_id": str(workspace.id)})
    ]


async def test_unexpired_compliance_stays_deleting(
    owner: Actor, workspace: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    enqueuer: Any, worker: WorkerTasks,
) -> None:  # fmt: skip
    """The row's date forged into the past while the store's COMPLIANCE lock still runs: the
    API (which trusts the ledger) approves, but the removal worker refuses before any delete,
    audits `document.removal_failed` and the row stays `deleting`; nothing is purged."""
    row = await _sealed(db, memory_store, workspace, owner, retention=PAST, lock_until=FAR)
    await _approve(owner, workspace, row)
    result = await worker.run(
        "sijil.remove_original", ledger_id=str(row.id), workspace_id=str(workspace.id)
    )
    assert result.successful(), result.result  # final: recorded, not retried
    stored = memory_store.objects[ObjectRef(row.object_key, "v1")]
    assert (stored.mode, stored.retain_until) == ("COMPLIANCE", FAR)
    assert memory_store.removed == []
    fresh = await _fresh(db, row)
    assert fresh.state is DocumentState.deleting
    assert (fresh.original_removed_at, fresh.deleted_at) == (None, None)
    assert await _events(db, workspace) == [
        ("document.deletion_approved", {}),
        ("document.removal_failed", {"code": "retention.locked"}),
    ]
    assert worker.sent == []


async def test_approve_deletion_refuses_a_sealed_document_under_retention(
    owner: Actor, workspace: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    enqueuer: Any,
) -> None:  # fmt: skip
    """Layer 1: a sealed document whose retention is not over is never approved."""
    row = await _sealed(db, memory_store, workspace, owner, retention=FAR, lock_until=FAR)
    r = await owner.client.post(
        f"/v1/workspaces/{workspace.id}/retention/approve-deletion", json={"ids": [str(row.id)]}
    )
    assert (r.status_code, r.json()["error"]["code"]) == (409, "retention.not_eligible")
    fresh = await _fresh(db, row)
    assert (fresh.state, fresh.deletion_approved_at) == (DocumentState.sealed, None)
    assert enqueuer.removals == []
    assert ObjectRef(row.object_key, "v1") in memory_store.objects
