"""The Owner's pre-seal removal is a path that can change a document (CLAUDE.md rule 4): it never
touches a sealed one, nor one whose seal is half done, and it keeps the tombstone's hash.

Layer 1 (the API) here; layer 3 (the ledger trigger refusing `sealed -> removed`) is
tests/retention/test_terminal_states.py; layer 4 (the removal worker's gate) is
tests/retention/test_removal_worker.py.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any

import pytest
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core import index
from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    DocumentState,
    Membership,
    Role,
    Workspace,
)
from sijil_core.fields import fields_digest
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import (
    ARRIVAL,
    INVOICE,
    add_document,
    add_index_credential,
    engines_on,
    stored,
)

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db

REASON = "Relevé déposé dans le mauvais espace (SAMPLE)"


@pytest.fixture(scope="module")
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
async def index_conn(index_db_url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(index_db_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as conn:
        await index.ensure_schema(conn)
        yield conn
    await engine.dispose()


@pytest.fixture
async def ws(
    db: AsyncSession, workspace: Workspace, cp_app: FastAPI, index_db_url: str
) -> AsyncIterator[Workspace]:
    await add_index_credential(db, cp_app, workspace)
    engines = engines_on(cp_app, index_db_url)
    cp_app.state.index = engines
    yield workspace
    await engines.aclose()


@pytest.fixture
async def owner(make_actor: MakeActor, db: AsyncSession, ws: Workspace) -> Actor:
    actor = await make_actor("removal-owner@example.com")
    db.add(Membership(user_id=actor.user_id, workspace_id=ws.id, role=Role.owner))
    await db.commit()
    await actor.enable_totp()
    await actor.step_up()
    return actor


def _url(ws: Workspace, doc_id: uuid.UUID, action: str) -> str:
    return f"/v1/workspaces/{ws.id}/documents/{doc_id}/{action}"


async def _ledger(db: AsyncSession, doc_id: uuid.UUID) -> DocumentLedger:
    found = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == doc_id)
        .execution_options(populate_existing=True)
    )
    assert found is not None
    return found


async def _actions(db: AsyncSession, ws: Workspace) -> list[tuple[str, object]]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id, AuditEvent.action.like("document.remov%"))
        .order_by(AuditEvent.seq)
    )
    return [(e.action, e.data) for e in rows]


async def test_sealed_cannot_be_removed(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    assert (await owner.client.post(_url(ws, row.id, "seal"))).status_code == 200
    sealed = await _ledger(db, row.id)
    r = await owner.client.post(_url(ws, row.id, "remove"), json={"reason": REASON})
    assert (r.status_code, r.json()["error"]["code"]) == (409, "document.sealed_cannot_be_removed")
    after = await _ledger(db, row.id)
    assert (after.state, after.removed_at, after.sealed_at) == (
        DocumentState.sealed, None, sealed.sealed_at
    )  # fmt: skip
    assert await _actions(db, ws) == [
        ("document.removal_refused", {"code": "document.sealed_cannot_be_removed"})
    ]
    assert enqueuer.removals == []
    assert stored(memory_store, row).data == b"SAMPLE"


async def test_half_sealed_cannot_be_removed(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    """The index row is frozen but the ledger is not (`seal_incomplete`): a person confirmed
    these values, so the document is treated as sealed and never removed."""
    row = await add_document(db, memory_store, index_conn, ws)
    await index.seal_fields(
        index_conn, row.id, fields_sha256=fields_digest(INVOICE), sealed_at=ARRIVAL
    )
    r = await owner.client.post(_url(ws, row.id, "remove"), json={"reason": REASON})
    assert (r.status_code, r.json()["error"]["code"]) == (409, "document.sealed_cannot_be_removed")
    assert (await _ledger(db, row.id)).state is DocumentState.filed
    assert await _actions(db, ws) == [
        ("document.removal_refused", {"code": "document.sealed_cannot_be_removed"})
    ]
    assert enqueuer.removals == []
    # Sealing again still completes the half-done seal.
    assert (await owner.client.post(_url(ws, row.id, "seal"))).status_code == 200


async def test_tombstone_keeps_hash(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    before = await _ledger(db, row.id)
    identity = (before.sha256, before.object_key, before.object_version_id)
    r = await owner.client.post(_url(ws, row.id, "remove"), json={"reason": REASON})
    assert r.status_code == 202, r.text
    after = await _ledger(db, row.id)
    assert (after.sha256, after.object_key, after.object_version_id) == identity
    body = (await owner.client.get(f"/v1/workspaces/{ws.id}/documents/{row.id}")).json()
    assert (body["state"], body["sha256"]) == ("removed", before.sha256)


async def test_remove_races_seal_one_wins(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    """Both take the ledger row FOR UPDATE: whichever is second sees the first's commit."""
    for _ in range(4):
        row = await add_document(db, memory_store, index_conn, ws)
        seal, remove = await asyncio.gather(
            owner.client.post(_url(ws, row.id, "seal")),
            owner.client.post(_url(ws, row.id, "remove"), json={"reason": REASON}),
        )
        outcome = {seal.status_code, remove.status_code}
        assert outcome in ({200, 409}, {202, 409}), (seal.text, remove.text)
        ledger = await _ledger(db, row.id)
        fields = await index.read_fields(index_conn, row.id)
        assert fields is not None
        if seal.status_code == 200:
            assert remove.json()["error"]["code"] == "document.sealed_cannot_be_removed"
            assert ledger.state is DocumentState.sealed
            assert (ledger.removed_at, ledger.removal_reason) == (None, None)
            assert fields.sealed_at is not None
            assert (row.id, ws.id) not in enqueuer.removals
        else:
            assert seal.json()["error"]["code"] == "document.not_reviewable"
            assert ledger.state is DocumentState.removed
            assert (ledger.sealed_at, ledger.fields_sha256) == (None, None)
            assert fields.sealed_at is None
            assert (row.id, ws.id) in enqueuer.removals
