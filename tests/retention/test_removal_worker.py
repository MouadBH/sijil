"""The removal worker's path (retention lock, layers 3 and 4): `sijil.remove_original` is the
one running code that deletes an original, and it never deletes a sealed one before its date.

Its RustFS variant (the removal key refused on a COMPLIANCE version) is
tests/integration/test_removal_key.py::test_removal_key_cannot_delete_compliance_version.
"""

import hashlib
import uuid
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    User,
    Workspace,
)
from sijil_core.storage import ObjectRef, RetentionActive
from sijil_worker.removal import remove_original
from sijil_worker.settings import WorkerSettings
from tests.support.memory_store import MemoryObjectStore, StoredObject

pytestmark = pytest.mark.db

NOW = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")


async def _sealed(
    db: AsyncSession, store: MemoryObjectStore, ws: Workspace, user: User, retention: date
) -> DocumentLedger:
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    row = DocumentLedger(
        workspace_id=ws.id, sha256=sha, md5="0" * 32, size_bytes=6, content_kind="pdf",
        source=DocumentSource.upload, object_key=f"k/{sha}", object_version_id="v1",
        state=DocumentState.sealed, ingest_state=IngestState.consumed,
        retention_until=retention, ingested_at=NOW - timedelta(days=1),
        fields_sha256="f" * 64, sealed_at=NOW - timedelta(days=1), sealed_by=user.id,
    )  # fmt: skip
    db.add(row)
    await db.commit()
    store.objects[ObjectRef(row.object_key, "v1")] = StoredObject(
        data=b"SAMPLE", mode="COMPLIANCE", retain_until=date(2036, 12, 31)
    )
    return row


async def _run(row: DocumentLedger, sessions: object, store: MemoryObjectStore) -> bool:
    async def purge(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        raise AssertionError("no purge for a document still under retention")

    return await remove_original(
        row.id, sessions=sessions, store=store, settings=SETTINGS, now=NOW,  # type: ignore[arg-type]
        workspace_id=row.workspace_id, purge=purge,
    )  # fmt: skip


async def test_removal_worker_never_deletes_a_sealed_original(
    db: AsyncSession, workspace: Workspace, user: User, memory_store: MemoryObjectStore
) -> None:
    """A message for a sealed document (forged, or stale) deletes nothing: the state gate."""
    row = await _sealed(db, memory_store, workspace, user, date(2036, 12, 31))
    sessions = async_sessionmaker(db.bind, expire_on_commit=False)
    assert not await _run(row, sessions, memory_store)
    assert ObjectRef(row.object_key, "v1") in memory_store.objects
    assert (await db.scalars(select(AuditEvent).where(AuditEvent.workspace_id == workspace.id))
            ).all() == []  # fmt: skip


async def test_an_approved_deletion_never_beats_an_unexpired_compliance_lock(
    db: AsyncSession, workspace: Workspace, user: User, memory_store: MemoryObjectStore
) -> None:
    """The ledger says retention is over (approved, `deleting`) while the store's COMPLIANCE
    lock still runs: the worker refuses before any delete and the row stays `deleting`."""
    row = await _sealed(db, memory_store, workspace, user, date(2020, 12, 31))
    await db.execute(
        text(
            "UPDATE document_ledger SET state = 'deleting', deletion_approved_at = now(),"
            " deletion_approved_by = :u WHERE id = :id"
        ),
        {"id": row.id, "u": user.id},
    )
    await db.commit()
    sessions = async_sessionmaker(db.bind, expire_on_commit=False)
    with pytest.raises(RetentionActive):
        await _run(row, sessions, memory_store)
    assert memory_store.objects[ObjectRef(row.object_key, "v1")].mode == "COMPLIANCE"
    state = await db.scalar(
        select(DocumentLedger.state)
        .where(DocumentLedger.id == row.id)
        .execution_options(populate_existing=True)
    )
    assert state is DocumentState.deleting
