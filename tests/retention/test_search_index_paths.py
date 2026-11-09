"""The search index is a path that writes and deletes document data (CLAUDE.md rule 4).

`search_doc` and `document_tag` rows leave only through `index.purge_document` (the
`sijil.purge` gate, opened for one ledger id): a plain DELETE or TRUNCATE is refused like on
every other index table. Writing search rows (upsert, state marks, tags) never touches the
sealed `document_fields` row, its values or its `fields_sha256`.
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
from sijil_core.index import FieldsRow
from sijil_core.search_index import (
    SearchLedgerFacts,
    mark_search_state,
    refresh_search_doc,
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
SEARCH_TABLES = ("search_doc", "document_tag")


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
    """A fields row (sealed or not), its search row and two tags."""
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
    await _tag(conn, ledger_id, "bureau")
    await _tag(conn, ledger_id, "urgent")
    state = DocumentState.sealed if sealed else DocumentState.filed
    await refresh_search_doc(conn, _facts(ledger_id, state), "Facture SAMPLE Télécom")
    return ledger_id


def _facts(ledger_id: uuid.UUID, state: str) -> SearchLedgerFacts:
    return SearchLedgerFacts(
        ledger_id=ledger_id, state=state, ingested_at=ARRIVAL, region="ma-cas", now=NOW
    )


async def _tag(conn: AsyncConnection, ledger_id: uuid.UUID, tag: str) -> None:
    await conn.execute(
        text(
            "INSERT INTO document_tag (ledger_id, tag, created_by, created_at) "
            "VALUES (:id, :tag, :by, :at)"
        ),
        {"id": ledger_id, "tag": tag, "by": uuid.uuid4(), "at": NOW},
    )


async def _count(conn: AsyncConnection, table: str, ledger_id: uuid.UUID) -> int:
    found = await conn.scalar(
        text(f"SELECT count(*) FROM {table} WHERE ledger_id = :id"),  # noqa: S608
        {"id": ledger_id},
    )
    return int(found or 0)


@pytest.mark.parametrize("sealed", [False, True])
async def test_purge_deletes_search_doc_and_tags(conn: AsyncConnection, sealed: bool) -> None:
    ledger_id = await _document(conn, sealed=sealed)
    other = await _document(conn, sealed=sealed)
    if sealed:
        await mark_search_state(conn, ledger_id, DocumentState.deleting)
    else:
        await mark_search_state(conn, ledger_id, DocumentState.removed)
    await index.purge_document(conn, ledger_id, sealed=sealed)
    for table in (*SEARCH_TABLES, "document_fields"):
        assert await _count(conn, table, ledger_id) == 0
        assert await _count(conn, table, other) > 0, "another document's rows were touched"
    await index.purge_document(conn, ledger_id, sealed=sealed)  # idempotent


@pytest.mark.parametrize("table", SEARCH_TABLES)
async def test_plain_delete_of_search_doc_refused(conn: AsyncConnection, table: str) -> None:
    ledger_id = await _document(conn, sealed=False)
    with pytest.raises(DBAPIError, match="retention.locked"):
        await conn.execute(
            text(f"DELETE FROM {table} WHERE ledger_id = :id"),  # noqa: S608
            {"id": ledger_id},
        )
    with pytest.raises(DBAPIError, match="retention.locked"):
        await conn.execute(text(f"TRUNCATE {table}"))
    # A gate opened for another document does not open this one.
    await conn.execute(
        text("SELECT set_config('sijil.purge', :other, false)"), {"other": str(uuid.uuid4())}
    )
    try:
        with pytest.raises(DBAPIError, match="retention.locked"):
            await conn.execute(
                text(f"DELETE FROM {table} WHERE ledger_id = :id"),  # noqa: S608
                {"id": ledger_id},
            )
    finally:
        await conn.execute(text("SELECT set_config('sijil.purge', '', false)"))
    assert await _count(conn, table, ledger_id) > 0


async def test_search_doc_never_changes_sealed_fields(conn: AsyncConnection) -> None:
    ledger_id = await _document(conn, sealed=True)
    before = await index.read_fields(conn, ledger_id)
    assert before is not None and before.sealed_at is not None
    await index.add_correction(
        conn, ledger_id, "total_ttc", "1600.00", "Avoir SAMPLE reçu après le scellement",
        created_by=uuid.uuid4(), now=NOW,
    )  # fmt: skip
    await refresh_search_doc(conn, _facts(ledger_id, DocumentState.sealed), None)
    await _tag(conn, ledger_id, "audit")
    await refresh_search_doc(conn, _facts(ledger_id, DocumentState.sealed), "")
    await mark_search_state(conn, ledger_id, DocumentState.deleting)
    after = await index.read_fields(conn, ledger_id)
    assert after == before
    assert after is not None and fields_digest(after.values) == before.fields_sha256
    total = await conn.scalar(
        text("SELECT total_ttc FROM search_doc WHERE ledger_id = :id"), {"id": ledger_id}
    )
    assert str(total) == "1600.00"  # the search row shows the correction; the seal is intact
