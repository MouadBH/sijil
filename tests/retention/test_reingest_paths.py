"""Re-ingesting a removed or deleted document's bytes (0013) is a path that meets a tombstone
(CLAUDE.md rule 4): it adds a new ledger row and a new object version, and never changes the
tombstone, its row or its version.

Through `sijil_core.ingest.accept_file`, the one way a file enters (web upload and email alike).
"""

import hashlib
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from sijil_core import ingest
from sijil_core.db.models import (
    ActorType,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    User,
    Workspace,
)
from sijil_core.retention import region_local_date
from sijil_core.storage import ObjectRef
from sijil_core.tenancy import tenant_ref
from tests.support.memory_store import MemoryObjectStore, StoredObject

pytestmark = pytest.mark.db

PDF = (Path(__file__).parents[2] / "fixtures" / "formats" / "sample.pdf").read_bytes()
SHA = hashlib.sha256(PDF).hexdigest()
REASON = "Doublon déposé par erreur (SAMPLE)"
STEPS = {
    DocumentState.removed: [
        "state = 'removed', removed_at = now(), removed_by = :u, removal_reason = :reason"
    ],
    DocumentState.deleted: [
        "state = 'sealed', fields_sha256 = repeat('f', 64), sealed_at = now(), sealed_by = :u",
        "state = 'deleting', deletion_approved_at = now(), deletion_approved_by = :u",
        "original_removed_at = now()",
        "state = 'deleted', deleted_at = now()",
    ],
}


def _key(ws: Workspace) -> str:
    return f"{tenant_ref(ws.id)}/{SHA}"


async def _tombstone(
    db: AsyncSession, store: MemoryObjectStore, ws: Workspace, user: User, state: DocumentState
) -> uuid.UUID:
    """A row of PDF's bytes brought to `state` as 0011 allows; a removed one keeps its version
    in the store (its removal worker has not run yet), a deleted one has none."""
    yesterday = region_local_date(datetime.now(UTC), "ma-cas") - timedelta(days=1)
    row = DocumentLedger(
        workspace_id=ws.id, sha256=SHA, md5="0" * 32, size_bytes=len(PDF), content_kind="pdf",
        source=DocumentSource.upload, object_key=_key(ws), object_version_id="v-tomb",
        state=DocumentState.filed, ingest_state=IngestState.consumed,
        retention_until=yesterday if state is DocumentState.deleted else date(2036, 12, 31),
        paperless_doc_id=7,
    )  # fmt: skip
    db.add(row)
    await db.commit()
    row_id = row.id
    for assignment in STEPS[state]:
        await db.execute(
            text(f"UPDATE document_ledger SET {assignment} WHERE id = :id"),  # noqa: S608
            {"id": row_id, "u": user.id, "reason": REASON},
        )
        await db.commit()
    if state is DocumentState.removed:
        store.objects[ObjectRef(_key(ws), "v-tomb")] = StoredObject(
            data=PDF, mode="GOVERNANCE", retain_until=date(2036, 12, 31)
        )
    return row_id


async def _row(conn: AsyncConnection, row_id: uuid.UUID) -> Any:
    return await conn.scalar(
        text("SELECT to_jsonb(d) FROM document_ledger d WHERE id = :id"), {"id": row_id}
    )


async def _chunks() -> AsyncIterator[bytes]:
    yield PDF


@pytest.mark.parametrize("state", [DocumentState.removed, DocumentState.deleted])
async def test_a_reingest_never_changes_the_tombstone(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, state: DocumentState,
) -> None:  # fmt: skip
    tomb = await _tombstone(db, memory_store, workspace, user, state)
    before = await _row(owner_conn, tomb)
    stored_before = {ref: replace(o) for ref, o in memory_store.objects.items()}
    sent: list[tuple[uuid.UUID, uuid.UUID]] = []

    async def enqueue(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        sent.append((ledger_id, workspace_id))

    result = await ingest.accept_file(
        sessions=async_sessionmaker(db.bind, expire_on_commit=False),
        store=memory_store, enqueue=enqueue, workspace=workspace, region="ma-cas",
        chunks=_chunks(), source=DocumentSource.upload, audit_action="document.uploaded",
        actor_type=ActorType.user, actor_id=str(user.id), ip=None, now=datetime.now(UTC),
    )  # fmt: skip
    assert isinstance(result, ingest.Accepted)
    assert result.document_id != tomb
    assert sent == [(result.document_id, workspace.id)]
    # The tombstone's row is exactly as it was, and so is everything the store held.
    assert await _row(owner_conn, tomb) == before
    for ref, stored in stored_before.items():
        assert memory_store.objects[ref] == stored
    new = await _row(owner_conn, result.document_id)
    assert (new["sha256"], new["object_key"], new["state"]) == (SHA, _key(workspace), "received")
    assert new["object_version_id"] != before["object_version_id"]
