"""Ask records are a path that can keep document data (CLAUDE.md rule 4).

An answer (`ask_query`, index schema v6) quotes documents: its figures count them, its citations
name them and its text may repeat them. When a document is purged (`index.purge_document`),
every answer that cites it or counted it in a figure goes too: only the removed document's
tombstone may remain. An answer is stored only while every document it cites or counts is live
(`ask_store.store_answer`, which holds their search rows FOR SHARE until it commits), so a
removal marked first is never quoted afterwards. Storing an answer never touches the sealed
`document_fields` row, its `fields_sha256` or the search row.
"""

import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from sijil_core import ask_store, index
from sijil_core.db.models import DocumentState
from sijil_core.fields import FIELD_NAMES, fields_digest
from sijil_core.index import FieldsRow
from sijil_core.search_index import SearchLedgerFacts, mark_search_state, refresh_search_doc
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
    "vat": "250.00",
    "total_ttc": "1500.00",
    "currency": "MAD",
}


@pytest.fixture(scope="module")
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
async def conn(index_db_url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(index_db_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as connection:
        await index.ensure_schema(connection)
        yield connection
    await engine.dispose()


async def _document(conn: AsyncConnection, *, sealed: bool) -> uuid.UUID:
    ledger_id = uuid.uuid4()
    await index.write_extraction(
        conn,
        FieldsRow(
            ledger_id=ledger_id, document_type="invoice", values=VALUES,
            source=dict.fromkeys(VALUES, "extraction"), updated_at=ARRIVAL,
        ),
    )  # fmt: skip
    if sealed:
        await index.seal_fields(conn, ledger_id, fields_sha256=fields_digest(VALUES), sealed_at=NOW)
    state = DocumentState.sealed if sealed else DocumentState.filed
    facts = SearchLedgerFacts(
        ledger_id=ledger_id, state=state, ingested_at=ARRIVAL, region="ma-cas", now=NOW
    )
    await refresh_search_doc(conn, facts, "Facture SAMPLE Atlas Télécom")
    return ledger_id


async def _asked(conn: AsyncConnection) -> uuid.UUID:
    ask_id = uuid.uuid4()
    await ask_store.create_ask(
        conn, ask_id, question="TVA payée à Atlas en 2026 ?", locale="fr",
        created_by=uuid.uuid4(), now=NOW,
    )  # fmt: skip
    return ask_id


async def _answer(
    conn: AsyncConnection, ask_id: uuid.UUID, citations: list[uuid.UUID],
    counted: list[uuid.UUID],
) -> bool:  # fmt: skip
    return await ask_store.store_answer(
        conn, ask_id,
        ask_store.Answer(
            intent={"kind": "sum"}, answer_text=None, answer_code="ask.answer.sum",
            figures=[{"measure": "vat", "value": "250.00", "currency": "MAD",
                      "document_count": len(counted)}],
            citations=citations, counted=counted, excluded={"needs_review": 0},
        ),
        now=NOW,
    )  # fmt: skip


async def _asks(conn: AsyncConnection, *ids: uuid.UUID) -> set[uuid.UUID]:
    found = await conn.scalars(
        text("SELECT id FROM ask_query WHERE id = ANY(:ids)"), {"ids": list(ids)}
    )
    return set(found)


@pytest.mark.parametrize("sealed", [False, True])
async def test_purge_deletes_the_answers_that_cite_or_count_the_document(
    conn: AsyncConnection, sealed: bool
) -> None:
    gone = await _document(conn, sealed=sealed)
    kept = await _document(conn, sealed=sealed)
    cited = await _asked(conn)
    counted_only = await _asked(conn)
    other = await _asked(conn)
    pending = await _asked(conn)
    assert await _answer(conn, cited, [gone, kept], [gone, kept])
    # Counted in the figure but beyond the citations shown.
    assert await _answer(conn, counted_only, [kept], [kept, gone])
    assert await _answer(conn, other, [kept], [kept])
    await mark_search_state(conn, gone, DocumentState.deleting if sealed else DocumentState.removed)
    await index.purge_document(conn, gone, sealed=sealed)
    assert await _asks(conn, cited, counted_only, other, pending) == {other, pending}
    await index.purge_document(conn, gone, sealed=sealed)  # idempotent


@pytest.mark.parametrize("state", [DocumentState.removed, DocumentState.deleting])
async def test_an_answer_quoting_a_terminal_or_purged_document_is_not_stored(
    conn: AsyncConnection, state: DocumentState
) -> None:
    doc = await _document(conn, sealed=state is DocumentState.deleting)
    live = await _document(conn, sealed=False)
    await mark_search_state(conn, doc, state)
    ask_id = await _asked(conn)
    assert not await _answer(conn, ask_id, [live], [live, doc])
    assert not await _answer(conn, ask_id, [doc], [])
    await index.purge_document(conn, doc, sealed=state is DocumentState.deleting)
    assert not await _answer(conn, ask_id, [doc], [doc])
    record = await ask_store.read_ask(conn, ask_id)
    assert record is not None and record.state == "queued" and record.citations == []
    assert await _answer(conn, ask_id, [live], [live])


async def test_storing_an_answer_leaves_the_sealed_document_untouched(
    conn: AsyncConnection,
) -> None:
    doc = await _document(conn, sealed=True)
    before = await index.read_fields(conn, doc)
    search_before = (
        await conn.execute(text("SELECT * FROM search_doc WHERE ledger_id = :id"), {"id": doc})
    ).one()
    ask_id = await _asked(conn)
    assert await _answer(conn, ask_id, [doc], [doc])
    after = await index.read_fields(conn, doc)
    assert after == before and after is not None
    assert after.fields_sha256 == fields_digest(VALUES)
    search_after = (
        await conn.execute(text("SELECT * FROM search_doc WHERE ledger_id = :id"), {"id": doc})
    ).one()
    assert search_after == search_before
