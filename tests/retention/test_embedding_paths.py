"""Embeddings are a path that writes and deletes document data (CLAUDE.md rule 4).

`doc_chunk` rows (index schema v5) are never updated. They leave only through a gate opened for
one ledger id: the purge's (`index.purge_document`, `sijil.purge`) or a re-embedding's
(`index.replace_chunks`, `sijil.reembed`, which replaces a live document's chunks as a whole). A
plain DELETE, UPDATE or TRUNCATE is refused, and a gate opened for another document does not
open this one. Embedding never touches the sealed `document_fields` row or its
`fields_sha256`, and never writes the chunks of a document whose search row is terminal or gone.
"""

import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from sijil_core import index
from sijil_core.db.models import DocumentState
from sijil_core.fields import FIELD_NAMES, fields_digest
from sijil_core.index import EMBEDDING_DIM, FieldsRow
from sijil_core.search_index import SearchLedgerFacts, mark_search_state, refresh_search_doc
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

NOW = datetime(2026, 11, 9, 10, 0, tzinfo=UTC)
ARRIVAL = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
MODEL = "bge-m3"
# SAMPLE values (fictitious supplier and ICE).
VALUES: dict[str, str | None] = dict.fromkeys(FIELD_NAMES) | {
    "document_type": "invoice",
    "supplier_name": "Atlas Télécom SAMPLE SARL",
    "supplier_ice": "001905837000041",
    "number": "F-2026-01001",
    "total_ttc": "1500.00",
    "currency": "MAD",
}


def _vector(seed: int) -> list[float]:
    return [((seed + i) % 7) / 7 for i in range(EMBEDDING_DIM)]


SOURCE = "a" * 64  # the digest of the chunk input (sijil_worker.embed.source_digest)
CHUNKS = [("invoice | Atlas Télécom SAMPLE SARL", _vector(1)), ("Facture SAMPLE", _vector(2))]


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
    """A fields row (sealed or not), its live search row and two chunks."""
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
    await refresh_search_doc(conn, facts, "Facture SAMPLE Télécom")
    assert await index.replace_chunks(conn, ledger_id, MODEL, CHUNKS, source_sha256=SOURCE)
    return ledger_id


async def _chunks(conn: AsyncConnection, ledger_id: uuid.UUID) -> int:
    found = await conn.scalar(
        text("SELECT count(*) FROM doc_chunk WHERE ledger_id = :id"), {"id": ledger_id}
    )
    return int(found or 0)


@pytest.mark.parametrize("sealed", [False, True])
async def test_purge_deletes_chunks(conn: AsyncConnection, sealed: bool) -> None:
    ledger_id = await _document(conn, sealed=sealed)
    other = await _document(conn, sealed=sealed)
    await mark_search_state(
        conn, ledger_id, DocumentState.deleting if sealed else DocumentState.removed
    )
    await index.purge_document(conn, ledger_id, sealed=sealed)
    assert await _chunks(conn, ledger_id) == 0
    assert await _chunks(conn, other) == 2, "another document's chunks were touched"
    await index.purge_document(conn, ledger_id, sealed=sealed)  # idempotent


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM doc_chunk WHERE ledger_id = :id",
        "UPDATE doc_chunk SET text = 'x' WHERE ledger_id = :id",
        "UPDATE doc_chunk SET n = n + 10 WHERE ledger_id = :id",
    ],
)
async def test_plain_delete_of_chunks_refused(conn: AsyncConnection, sql: str) -> None:
    ledger_id = await _document(conn, sealed=False)
    with pytest.raises(DBAPIError, match="retention.locked"):
        await conn.execute(text(sql), {"id": ledger_id})
    with pytest.raises(DBAPIError, match="retention.locked"):
        await conn.execute(text("TRUNCATE doc_chunk"))
    assert await _chunks(conn, ledger_id) == 2


async def test_a_gate_for_another_document_does_not_open_this_one(conn: AsyncConnection) -> None:
    ledger_id = await _document(conn, sealed=False)
    other = uuid.uuid4()
    for gate in ("sijil.reembed", "sijil.purge"):
        # Session-wide (the connection is AUTOCOMMIT), reset after.
        await conn.execute(text(f"SELECT set_config('{gate}', :v, false)"), {"v": str(other)})
        try:
            with pytest.raises(DBAPIError, match="retention.locked"):
                await conn.execute(
                    text("DELETE FROM doc_chunk WHERE ledger_id = :id"), {"id": ledger_id}
                )
        finally:
            await conn.execute(text(f"SELECT set_config('{gate}', '', false)"))
    assert await _chunks(conn, ledger_id) == 2


async def test_reembed_leaves_the_sealed_fields_untouched(conn: AsyncConnection) -> None:
    ledger_id = await _document(conn, sealed=True)
    before = await index.read_fields(conn, ledger_id)
    replaced = [("Facture SAMPLE corrigée", _vector(3))]
    assert await index.replace_chunks(conn, ledger_id, MODEL, replaced, source_sha256="b" * 64)
    after = await index.read_fields(conn, ledger_id)
    assert after == before
    assert after is not None and after.fields_sha256 == fields_digest(VALUES)
    assert await _chunks(conn, ledger_id) == 1
    assert await index.chunks_source(conn, ledger_id) == "b" * 64
    assert await index.chunks_source(conn, uuid.uuid4()) is None


@pytest.mark.parametrize("state", [DocumentState.removed, DocumentState.deleting])
async def test_reembed_of_a_terminal_or_purged_document_writes_nothing(
    conn: AsyncConnection, state: DocumentState
) -> None:
    """A removal marks the search row before the purge: from then on no chunk is written (and
    the old ones wait for the purge); after the purge there is no search row to write for."""
    ledger_id = await _document(conn, sealed=state is DocumentState.deleting)
    await mark_search_state(conn, ledger_id, state)
    assert not await index.replace_chunks(
        conn, ledger_id, MODEL, [("x", _vector(4))], source_sha256=SOURCE
    )
    assert await _chunks(conn, ledger_id) == 2
    await index.purge_document(conn, ledger_id, sealed=state is DocumentState.deleting)
    assert not await index.replace_chunks(
        conn, ledger_id, MODEL, [("x", _vector(4))], source_sha256=SOURCE
    )
    assert await _chunks(conn, ledger_id) == 0
