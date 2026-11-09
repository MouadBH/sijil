"""Removing a tag is a path that deletes index rows (CLAUDE.md rule 4).

A tag is an annotation (ruling, M5 Task 2): since index schema v4 `document_tag` allows a plain
DELETE, which `search_index.set_tags` uses to replace a document's tags. It never touches the
sealed `document_fields` row or its `fields_sha256`, it changes `search_doc.tags` in the same
index transaction (both or neither), and `index.purge_document` still deletes the tags left.
"""

import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from sijil_core import index
from sijil_core.db.models import DocumentState
from sijil_core.fields import FIELD_NAMES, fields_digest
from sijil_core.index import FieldsRow
from sijil_core.search_index import (
    SearchLedgerFacts,
    mark_search_state,
    refresh_search_doc,
    set_tags,
)
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

NOW = datetime(2026, 11, 9, 10, 0, tzinfo=UTC)
ARRIVAL = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
# SAMPLE values (fictitious supplier and ICE).
VALUES: dict[str, str | None] = dict.fromkeys(FIELD_NAMES) | {
    "document_type": "invoice",
    "supplier_name": "Atlas Télécom SAMPLE SARL",
    "supplier_ice": "001905837000041",
    "date": "2026-09-30",
    "number": "F-2026-01001",
    "total_ttc": "1500.00",
    "currency": "MAD",
}


@pytest.fixture(scope="module")
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
async def engine(index_db_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(index_db_url)
    async with engine.begin() as conn:
        await index.ensure_schema(conn)
    yield engine
    await engine.dispose()


async def _sealed(engine: AsyncEngine) -> uuid.UUID:
    """A sealed document, its search row and tags `bureau`, `urgent`."""
    ledger_id = uuid.uuid4()
    async with engine.begin() as conn:
        await index.write_extraction(
            conn,
            FieldsRow(
                ledger_id=ledger_id, document_type="invoice", values=VALUES,
                source=dict.fromkeys(VALUES, "extraction"), updated_at=ARRIVAL,
            ),
        )  # fmt: skip
        await index.seal_fields(conn, ledger_id, fields_sha256=fields_digest(VALUES), sealed_at=NOW)
        facts = SearchLedgerFacts(
            ledger_id=ledger_id, state=DocumentState.sealed, ingested_at=ARRIVAL, region="ma-cas",
            now=NOW,
        )  # fmt: skip
        await refresh_search_doc(conn, facts, "Facture SAMPLE")
        await set_tags(conn, ledger_id, ["urgent", "bureau"], by=uuid.uuid4(), now=NOW)
    return ledger_id


async def _tags(conn: AsyncConnection, ledger_id: uuid.UUID) -> tuple[list[str], list[str]]:
    """(`document_tag` rows, `search_doc.tags`)."""
    rows = await conn.scalars(
        text("SELECT tag FROM document_tag WHERE ledger_id = :id ORDER BY tag"), {"id": ledger_id}
    )
    denormalised = await conn.scalar(
        text("SELECT tags FROM search_doc WHERE ledger_id = :id"), {"id": ledger_id}
    )
    return list(rows), list(denormalised or [])


async def test_removing_a_tag_never_touches_sealed_fields(engine: AsyncEngine) -> None:
    ledger_id = await _sealed(engine)
    async with engine.connect() as conn:
        before = await index.read_fields(conn, ledger_id)
    assert before is not None and before.fields_sha256 == fields_digest(VALUES)
    async with engine.begin() as conn:
        await set_tags(conn, ledger_id, ["bureau"], by=uuid.uuid4(), now=NOW)
    async with engine.connect() as conn:
        assert await _tags(conn, ledger_id) == (["bureau"], ["bureau"])
        after = await index.read_fields(conn, ledger_id)
    assert after == before
    assert after is not None and fields_digest(after.values) == after.fields_sha256


async def test_tag_removal_and_search_doc_tags_commit_together(engine: AsyncEngine) -> None:
    ledger_id = await _sealed(engine)
    with pytest.raises(ConnectionResetError):
        async with engine.begin() as conn:
            await set_tags(conn, ledger_id, ["audit"], by=uuid.uuid4(), now=NOW)
            # Inside the transaction both changed...
            assert await _tags(conn, ledger_id) == (["audit"], ["audit"])
            raise ConnectionResetError("index connection lost before the commit")
    async with engine.connect() as conn:
        # ...and neither did once it rolled back.
        assert await _tags(conn, ledger_id) == (["bureau", "urgent"], ["bureau", "urgent"])


async def test_purge_still_deletes_the_remaining_tags(engine: AsyncEngine) -> None:
    ledger_id = await _sealed(engine)
    other = await _sealed(engine)
    async with engine.begin() as conn:
        await set_tags(conn, ledger_id, ["bureau"], by=uuid.uuid4(), now=NOW)
        await mark_search_state(conn, ledger_id, DocumentState.deleting)
        await index.purge_document(conn, ledger_id, sealed=True)
    async with engine.connect() as conn:
        assert await _tags(conn, ledger_id) == ([], [])
        assert await _tags(conn, other) == (["bureau", "urgent"], ["bureau", "urgent"])


async def test_tags_of_a_terminal_row_stay_out_of_search_doc(engine: AsyncEngine) -> None:
    """A row marked `removed` keeps its tags until the purge; `set_tags` never revives it."""
    ledger_id = await _sealed(engine)
    async with engine.begin() as conn:
        await mark_search_state(conn, ledger_id, DocumentState.deleting)
        await set_tags(conn, ledger_id, ["late"], by=uuid.uuid4(), now=NOW)
        state = await conn.scalar(
            text("SELECT state FROM search_doc WHERE ledger_id = :id"), {"id": ledger_id}
        )
    assert state == "deleting"
