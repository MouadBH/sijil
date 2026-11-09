"""Index schema v3: the per-workspace search index (`search_doc`, `document_tag`) and its upsert.

The row holds a document's effective values (corrections win), its retention basis date and its
OCR text; `tsv` is generated from `search_normalise`d text, so an Arabic or French query
normalised the same way matches regardless of hamza, tatweel, harakat, accents or digits.
"""

import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from sijil_core import index
from sijil_core.db.models import DocumentState
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow
from sijil_core.search_index import (
    SearchLedgerFacts,
    mark_search_state,
    refresh_search_doc,
    upsert_search_doc,
)
from sijil_core.text import search_normalise
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
    "total_ht": "1250.00",
    "vat": "250.00",
    "total_ttc": "1500.00",
    "currency": "MAD",
}
CONTENT = "SAMPLE ف\u0640اتورة رقم ٢٠٢٦ — شركة أطلس للاتصالات\nFacture Télécom"


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


def _facts(ledger_id: uuid.UUID, state: str = DocumentState.filed) -> SearchLedgerFacts:
    return SearchLedgerFacts(
        ledger_id=ledger_id, state=state, ingested_at=ARRIVAL, region="ma-cas", now=NOW
    )


def _fields(ledger_id: uuid.UUID, values: dict[str, str | None] = VALUES) -> FieldsRow:
    return FieldsRow(
        ledger_id=ledger_id,
        document_type=values.get("document_type"),
        values=values,
        source=dict.fromkeys(values, "extraction"),
        updated_at=ARRIVAL,
    )


async def _row(conn: AsyncConnection, ledger_id: uuid.UUID) -> dict[str, object]:
    found = await conn.execute(
        text("SELECT * FROM search_doc WHERE ledger_id = :id"), {"id": ledger_id}
    )
    return dict(found.mappings().one())


async def test_schema_v3_has_the_extensions_and_tables(conn: AsyncConnection) -> None:
    assert index.SCHEMA_VERSION == 3
    assert await conn.scalar(text("SELECT max(version) FROM index_schema")) == 3
    extensions = set(await conn.scalars(text("SELECT extname FROM pg_extension")))
    assert {"pg_trgm", "unaccent", "vector"} <= extensions
    tables = set(
        await conn.scalars(text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'"))
    )
    assert {"search_doc", "document_tag"} <= tables


@pytest.fixture
def fresh_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


async def test_v2_database_upgrades_to_v3(
    fresh_db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An existing workspace's index (v2, rows present) gains v3 on its next `ensure_schema`,
    and its purge then covers the search rows too."""
    engine = create_async_engine(fresh_db_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as c:
            with monkeypatch.context() as m:
                m.setattr(index, "_DDL", {v: index._DDL[v] for v in (1, 2)})
                await index.ensure_schema(c)
            ledger_id = uuid.uuid4()
            await index.write_extraction(c, _fields(ledger_id))
            await index.ensure_schema(c)
            versions = (await c.execute(text("SELECT version FROM index_schema"))).scalars()
            assert sorted(versions) == [1, 2, 3]
            await refresh_search_doc(c, _facts(ledger_id), CONTENT)
            await index.purge_document(c, ledger_id)
            for table in ("search_doc", "document_fields"):
                count = await c.scalar(
                    text(f"SELECT count(*) FROM {table} WHERE ledger_id = :id"),  # noqa: S608
                    {"id": ledger_id},
                )
                assert count == 0
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "raw",
    [
        "Atlas Télécom SAMPLE SARL",
        "ÉCHANTILLON — Façade, Œuvre & Garçon, Straße",
        "ف\u0640\u0640\u0640اتورة رقم ٢٠٢٦ / ۲۰۲۶",
        "ف\u064eات\u064fور\u064eة\u064c أحمد إسماعيل آمنة ٱلدار مصطفى",
        "ﻓﺎﺗﻮﺭﺓ\u00a0\u00a0\u200fF-2026-01001\tTotal TTC 1 500,00 MAD",
        "شركة أطلس للاتصالات ش.م.م — ICE 001905837000041",
    ],
)
async def test_sql_search_normalise_agrees_with_python(conn: AsyncConnection, raw: str) -> None:
    assert await conn.scalar(text("SELECT search_normalise(:s)"), {"s": raw}) == (
        search_normalise(raw)
    )


async def test_upsert_uses_effective_values_and_basis_date(conn: AsyncConnection) -> None:
    ledger_id = uuid.uuid4()
    note = index.Correction(
        id=uuid.uuid4(), ledger_id=ledger_id, field="total_ttc", value="1600.00",
        reason="Avoir SAMPLE reçu après le scellement", created_by=uuid.uuid4(), created_at=NOW,
    )  # fmt: skip
    await upsert_search_doc(conn, _facts(ledger_id), _fields(ledger_id), [note], CONTENT)
    row = await _row(conn, ledger_id)
    assert row["state"] == "filed"
    assert row["ingested_at"] == ARRIVAL
    assert row["basis_date"] == date(2026, 9, 30)
    assert row["document_type"] == "invoice"
    assert row["supplier_name"] == "Atlas Télécom SAMPLE SARL"
    assert row["supplier_ice"] == "001905837000041"
    assert row["number"] == "F-2026-01001"
    assert (row["total_ht"], row["vat"]) == (Decimal("1250.00"), Decimal("250.00"))
    assert row["total_ttc"] == Decimal("1600.00")  # the correction, not the sealed value
    assert row["currency"] == "MAD"
    assert row["corrected"] == ["total_ttc"]
    assert row["tags"] == []
    assert row["content"] == CONTENT
    assert row["updated_at"] == NOW


async def test_basis_date_falls_back_to_arrival(conn: AsyncConnection) -> None:
    ledger_id = uuid.uuid4()
    undated = VALUES | {"date": "2031-01-01"}  # implausible: after today
    await upsert_search_doc(conn, _facts(ledger_id), _fields(ledger_id, undated), [], None)
    row = await _row(conn, ledger_id)
    assert row["basis_date"] == date(2026, 10, 1)
    assert row["content"] == ""


async def test_upsert_without_fields_keeps_the_ledger_facts(conn: AsyncConnection) -> None:
    ledger_id = uuid.uuid4()
    await upsert_search_doc(conn, _facts(ledger_id, DocumentState.received), None, [], "")
    row = await _row(conn, ledger_id)
    assert (row["state"], row["document_type"], row["total_ttc"]) == ("received", None, None)
    assert row["basis_date"] == date(2026, 10, 1)


async def test_upsert_refuses_terminal_state(conn: AsyncConnection) -> None:
    ledger_id = uuid.uuid4()
    for state in (DocumentState.removed, DocumentState.deleting, DocumentState.deleted):
        with pytest.raises(ValueError, match="terminal"):
            await upsert_search_doc(conn, _facts(ledger_id, state), _fields(ledger_id), [], "")
    count = await conn.scalar(
        text("SELECT count(*) FROM search_doc WHERE ledger_id = :id"), {"id": ledger_id}
    )
    assert count == 0


async def test_content_none_keeps_the_stored_content(conn: AsyncConnection) -> None:
    ledger_id = uuid.uuid4()
    await upsert_search_doc(conn, _facts(ledger_id), _fields(ledger_id), [], CONTENT)
    sealed = _facts(ledger_id, DocumentState.sealed)
    await upsert_search_doc(conn, sealed, _fields(ledger_id), [], None)
    row = await _row(conn, ledger_id)
    assert (row["state"], row["content"]) == ("sealed", CONTENT)


async def test_a_terminal_row_is_never_revived(conn: AsyncConnection) -> None:
    ledger_id = uuid.uuid4()
    await upsert_search_doc(conn, _facts(ledger_id), _fields(ledger_id), [], CONTENT)
    await mark_search_state(conn, ledger_id, DocumentState.removed)
    # A late upsert (an extraction that routed just before the removal) changes nothing.
    await upsert_search_doc(conn, _facts(ledger_id), _fields(ledger_id), [], "")
    row = await _row(conn, ledger_id)
    assert (row["state"], row["content"]) == ("removed", CONTENT)


async def test_mark_search_state_refuses_a_live_state(conn: AsyncConnection) -> None:
    with pytest.raises(ValueError, match="terminal"):
        await mark_search_state(conn, uuid.uuid4(), DocumentState.filed)


async def test_tags_are_denormalised(conn: AsyncConnection) -> None:
    ledger_id = uuid.uuid4()
    for tag in ("urgent", "bureau"):
        await conn.execute(
            text(
                "INSERT INTO document_tag (ledger_id, tag, created_by, created_at) "
                "VALUES (:id, :tag, :by, :at)"
            ),
            {"id": ledger_id, "tag": tag, "by": uuid.uuid4(), "at": NOW},
        )
    await upsert_search_doc(conn, _facts(ledger_id), _fields(ledger_id), [], "")
    assert (await _row(conn, ledger_id))["tags"] == ["bureau", "urgent"]


async def test_tag_length_is_checked(conn: AsyncConnection) -> None:
    from sqlalchemy.exc import IntegrityError

    for tag in ("", "x" * 51):
        with pytest.raises(IntegrityError):
            await conn.execute(
                text(
                    "INSERT INTO document_tag (ledger_id, tag, created_by, created_at) "
                    "VALUES (:id, :tag, :by, :at)"
                ),
                {"id": uuid.uuid4(), "tag": tag, "by": uuid.uuid4(), "at": NOW},
            )


async def test_refresh_reads_fields_and_corrections(conn: AsyncConnection) -> None:
    ledger_id = uuid.uuid4()
    await index.write_extraction(conn, _fields(ledger_id))
    await index.add_correction(
        conn, ledger_id, "supplier_name", "Atlas Télécom SAMPLE SA",
        "Raison sociale SAMPLE corrigée", created_by=uuid.uuid4(), now=NOW,
    )  # fmt: skip
    await refresh_search_doc(conn, _facts(ledger_id), CONTENT)
    row = await _row(conn, ledger_id)
    assert row["supplier_name"] == "Atlas Télécom SAMPLE SA"
    assert row["corrected"] == ["supplier_name"]
    assert row["content"] == CONTENT


@pytest.mark.parametrize(
    "query",
    ["فاتوره", "فاتورة", "أطلس", "اطلس", "2026", "telecom", "TÉLÉCOM", "f-2026-01001"],
)
async def test_tsv_matches_queries_normalised_the_same_way(
    conn: AsyncConnection, query: str
) -> None:
    ledger_id = uuid.uuid4()
    await upsert_search_doc(conn, _facts(ledger_id), _fields(ledger_id), [], CONTENT)
    hit = await conn.scalar(
        text("SELECT tsv @@ plainto_tsquery('simple', :q) FROM search_doc WHERE ledger_id = :id"),
        {"q": search_normalise(query), "id": ledger_id},
    )
    assert hit is True


async def test_normalised_columns_are_written_by_python(conn: AsyncConnection) -> None:
    ledger_id = uuid.uuid4()
    await upsert_search_doc(conn, _facts(ledger_id), _fields(ledger_id), [], CONTENT)
    row = await _row(conn, ledger_id)
    assert row["supplier_norm"] == search_normalise(str(VALUES["supplier_name"]))
    assert row["content_norm"] == search_normalise(CONTENT)
    # `content` None keeps both the text and its normalised form.
    await upsert_search_doc(conn, _facts(ledger_id), _fields(ledger_id), [], None)
    row = await _row(conn, ledger_id)
    assert (row["content"], row["content_norm"]) == (CONTENT, search_normalise(CONTENT))


@pytest.mark.parametrize(
    ("column", "operator", "query"),
    [
        ("supplier_norm", "%", "atlas telecom sample sarl"),  # no accents, lower case
        ("supplier_norm", "%", "ATLAS TÉLÉCOM SAMPLE"),
        ("content_norm", "<%", "فاتوره"),  # teh marbuta written as heh
        ("content_norm", "<%", "اطلس"),  # alef without hamza
        ("content_norm", "<%", "telecom"),
    ],
)
async def test_trigram_path_matches_normalised_queries(
    conn: AsyncConnection, column: str, operator: str, query: str
) -> None:
    """The trigram indexes are on the normalised columns, so a query normalised the same way
    finds accented and Arabic-variant text through them (an index scan, not a table scan)."""
    ledger_id = uuid.uuid4()
    await upsert_search_doc(conn, _facts(ledger_id), _fields(ledger_id), [], CONTENT)
    q = search_normalise(query)
    if operator == "%":
        condition = f"{column} % :q"
    else:
        condition = f":q <% {column}"
    hit = await conn.scalar(
        text(f"SELECT count(*) FROM search_doc WHERE ledger_id = :id AND {condition}"),  # noqa: S608
        {"q": q, "id": ledger_id},
    )
    assert hit == 1
    await conn.execute(text("SET enable_seqscan = off"))
    try:
        plan = "\n".join(
            await conn.scalars(
                text(f"EXPLAIN SELECT ledger_id FROM search_doc WHERE {condition}"),  # noqa: S608
                {"q": q},
            )
        )
    finally:
        await conn.execute(text("RESET enable_seqscan"))
    assert f"search_doc_{column.removesuffix('_norm')}_trgm" in plan
