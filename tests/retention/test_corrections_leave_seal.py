"""A correction note is a path that writes next to a sealed document: it must leave the sealed
index row, its hash and the ledger exactly as they were, and the notes are append-only."""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi import FastAPI
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core import index
from sijil_core.db.models import DocumentLedger, Membership, Role, Workspace
from sijil_core.fields import FIELD_NAMES, fields_digest
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import INVOICE, add_document, add_index_credential, engines_on

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db


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
async def member(make_actor: MakeActor, db: AsyncSession, ws: Workspace) -> Actor:
    actor = await make_actor("corr-member@example.com")
    db.add(Membership(user_id=actor.user_id, workspace_id=ws.id, role=Role.member))
    await db.commit()
    return actor


async def test_correction_never_changes_sealed_index_row(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    base = f"/v1/workspaces/{ws.id}/documents/{row.id}"
    assert (await member.client.post(f"{base}/seal")).status_code == 200
    sealed_index = await index.read_fields(index_conn, row.id)
    sealed_ledger = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == row.id)
        .execution_options(populate_existing=True)
    )
    assert sealed_index is not None and sealed_ledger is not None
    snapshot = (sealed_ledger.state, sealed_ledger.fields_sha256, sealed_ledger.retention_until)

    for name in FIELD_NAMES:
        r = await member.client.post(
            f"{base}/corrections",
            json={"field": name, "value": None, "reason": "A correction for the audit."},
        )
        assert r.status_code == 201, (name, r.text)

    after = await index.read_fields(index_conn, row.id)
    assert after == sealed_index
    assert after is not None
    assert dict(after.values) == INVOICE
    assert fields_digest(after.values) == sealed_index.fields_sha256 == snapshot[1]
    ledger = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == row.id)
        .execution_options(populate_existing=True)
    )
    assert ledger is not None
    assert (ledger.state, ledger.fields_sha256, ledger.retention_until) == snapshot
    # The sealed row still refuses every write, and the notes are append-only.
    with pytest.raises(index.RetentionLocked):
        await index.update_fields(
            index_conn, row.id, {"total_ttc": "1.00"}, source="user", updated_by=None,
            now=datetime.now(UTC),
        )  # fmt: skip
    for sql in (
        "UPDATE correction_note SET reason = 'x' WHERE ledger_id = :id",
        "DELETE FROM correction_note WHERE ledger_id = :id",
    ):
        with pytest.raises(DBAPIError, match="index.append_only"):
            await index_conn.execute(text(sql), {"id": row.id})
    assert len(await index.read_corrections(index_conn, row.id)) == len(FIELD_NAMES)
