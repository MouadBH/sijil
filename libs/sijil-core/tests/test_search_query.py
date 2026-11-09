"""The search query builder (`sijil_core.search_query`) against a real index database.

Each test gets its own empty index database: `search_doc` rows cannot be deleted, so sharing
one would let another test's rows into the results.
"""

import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from sijil_core import index
from sijil_core.db.models import DocumentState
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow
from sijil_core.search_index import SearchLedgerFacts, mark_search_state, upsert_search_doc
from sijil_core.search_query import QueryInvalid, SearchFilters, filter_names, run_search
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

NOW = datetime(2026, 11, 9, 10, 0, tzinfo=UTC)
ARRIVAL = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
ICE = "001905837000041"  # SAMPLE (fictitious)


@pytest.fixture
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
async def conn(index_db_url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(index_db_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as connection:
        await index.ensure_schema(connection)
        yield connection
    await engine.dispose()


async def _doc(
    conn: AsyncConnection,
    *,
    state: str = DocumentState.filed,
    content: str = "Facture SAMPLE",
    ingested_at: datetime = ARRIVAL,
    **values: str | None,
) -> uuid.UUID:
    ledger_id = uuid.uuid4()
    full: dict[str, str | None] = dict.fromkeys(FIELD_NAMES) | {
        "document_type": "invoice",
        "supplier_name": "Atlas Télécom SAMPLE SARL",
        "supplier_ice": ICE,
        "date": "2026-09-30",
        "number": f"F-{uuid.uuid4().hex[:6]}",
        "total_ttc": "1500.00",
        "currency": "MAD",
    }
    full |= values
    fields = FieldsRow(
        ledger_id=ledger_id,
        document_type=full["document_type"],
        values=full,
        source=dict.fromkeys(full, "extraction"),
        updated_at=ingested_at,
    )
    facts = SearchLedgerFacts(
        ledger_id=ledger_id, state=state, ingested_at=ingested_at, region="ma-cas", now=NOW
    )
    await upsert_search_doc(conn, facts, fields, [], content)
    return ledger_id


async def _ids(conn: AsyncConnection, sort: str = "date_desc", **filters: object) -> set[uuid.UUID]:
    page = await run_search(conn, SearchFilters.model_validate(filters), sort, None, 100)
    return {item.id for item in page.items}


async def test_filter_amount_range(conn: AsyncConnection) -> None:
    low = await _doc(conn, total_ttc="99.99")
    at_min = await _doc(conn, total_ttc="100.00")
    at_max = await _doc(conn, total_ttc="250.50")
    high = await _doc(conn, total_ttc="250.51")
    none = await _doc(conn, total_ttc=None)
    found = await _ids(conn, amount_min="100.00", amount_max="250.50")
    assert found == {at_min, at_max}
    assert not {low, high, none} & found
    assert await _ids(conn, amount_min="250.51") == {high}
    page = await run_search(conn, SearchFilters(amount_max=Decimal("99.99")), "date_desc", None, 10)
    assert [(i.id, i.total_ttc) for i in page.items] == [(low, Decimal("99.99"))]


async def test_filter_period_uses_basis_date(conn: AsyncConnection) -> None:
    # Arrived in October 2026, dated March 2025: the period is the document's date.
    march = await _doc(conn, date="2025-03-10")
    # No plausible date: the basis is the arrival date.
    undated = await _doc(conn, date=None, ingested_at=datetime(2025, 3, 31, 12, tzinfo=UTC))
    october = await _doc(conn, date="2026-10-02")
    found = await _ids(conn, period_from="2025-03-01", period_to="2025-03-31")
    assert found == {march, undated}
    assert await _ids(conn, period_from="2026-10-02", period_to="2026-10-02") == {october}
    page = await run_search(conn, SearchFilters(period_to=date(2025, 3, 10)), "date_asc", None, 5)
    assert [(i.id, i.basis_date) for i in page.items] == [(march, date(2025, 3, 10))]


async def test_filter_status_never_returns_terminal(conn: AsyncConnection) -> None:
    live = {
        state: await _doc(conn, state=state, content="Facture SAMPLE fantôme")
        for state in ("received", "needs_review", "filed", "sealed")
    }
    for state in ("removed", "deleting", "deleted"):
        doc = await _doc(conn, content="Facture SAMPLE fantôme")
        await mark_search_state(conn, doc, state)
    assert await _ids(conn) == set(live.values())
    assert await _ids(conn, q="fantome") == set(live.values())
    assert await _ids(conn, status=["sealed", "filed"]) == {live["sealed"], live["filed"]}
    for terminal in ("removed", "deleting", "deleted"):
        with pytest.raises(ValidationError):
            SearchFilters.model_validate({"status": [terminal]})


async def test_query_ice_exact_and_number_exact(conn: AsyncConnection) -> None:
    other_ice = "002345678000099"
    wanted = await _doc(conn, number="F-2026-01001", supplier_ice=ICE)
    near = await _doc(conn, number="F-2026-010011", supplier_ice=other_ice)
    elsewhere = await _doc(conn, number="B/77", supplier_ice="001905837000042")
    assert await _ids(conn, q=ICE) == {wanted}
    assert await _ids(conn, q="F-2026-01001") == {wanted}
    assert await _ids(conn, q="B/77") == {elsewhere}
    # Arabic-Indic digits fold to the same ICE.
    assert await _ids(conn, q="٠٠١٩٠٥٨٣٧٠٠٠٠٤١") == {wanted}
    # The supplier filter: an ICE exactly, else part of the name.
    assert await _ids(conn, supplier=other_ice) == {near}
    assert await _ids(conn, supplier="00190583700004") == set()
    assert await _ids(conn, supplier="telecom sample") == {wanted, near, elsewhere}


async def test_query_arabic_variants_match(conn: AsyncConnection) -> None:
    doc = await _doc(
        conn,
        supplier_name="شركة أطلس للاتصالات",
        content="فاتورة رقم ٢٠٢٦ — إتصالات المغرب، مبلغ الفاتورة الإجمالي",
    )
    await _doc(conn, supplier_name="Rabat Papeterie SAMPLE", content="Facture papeterie")
    # Tatweel, alef without hamza, teh marbuta as heh, harakat, ASCII digits.
    for q in ("فـاتورة", "اتصالات", "الفاتوره", "فَاتُورَة", "2026", "اطلس"):
        assert await _ids(conn, q=q) == {doc}, q
    page = await run_search(conn, SearchFilters(q="فاتورة"), "relevance", None, 10)
    snippet = page.items[0].snippet
    assert snippet is not None and "«" in snippet and "»" in snippet and "<" not in snippet


async def test_query_french_accents_match(conn: AsyncConnection) -> None:
    doc = await _doc(
        conn,
        supplier_name="Société Générale Équipement SAMPLE",
        content="FACTURE n° 12 — Prestations de télécommunication réglées",
    )
    await _doc(conn, supplier_name="Rabat Papeterie SAMPLE", content="Facture papeterie")
    for q in ("societe", "SOCIÉTÉ", "equipement", "telecommunication", "réglées", "reglees"):
        assert await _ids(conn, q=q) == {doc}, q
    # A misspelt full supplier name still matches by trigram similarity.
    assert await _ids(conn, q="Societe Generale Equipment SAMPLE") == {doc}


async def test_keyset_paging_stable_with_insert_between_pages(conn: AsyncConnection) -> None:
    # Ties on the sort key: the ledger id breaks them.
    docs = [await _doc(conn, date="2026-05-01", total_ttc="10.00") for _ in range(5)]
    docs += [await _doc(conn, date="2026-06-01", total_ttc="20.00") for _ in range(4)]
    docs.append(await _doc(conn, date="2026-04-01", total_ttc=None))
    for sort in ("date_desc", "date_asc", "amount_desc", "amount_asc", "relevance"):
        filters = SearchFilters(q="facture" if sort == "relevance" else None)
        seen: list[uuid.UUID] = []
        cursor: str | None = None
        first = True
        while True:
            page = await run_search(conn, filters, sort, cursor, 3)
            seen += [item.id for item in page.items]
            if first:
                # Arrives between the first and second pages, at every position of the order.
                for when, amount in (("2026-07-01", "30.00"), ("2026-05-01", "10.00")):
                    docs.append(await _doc(conn, date=when, total_ttc=amount))
                first = False
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        assert len(seen) == len(set(seen)), f"{sort}: a row repeated"
        assert set(docs[:10]) <= set(seen), f"{sort}: a row present before paging was skipped"
    with pytest.raises(QueryInvalid):
        await run_search(conn, SearchFilters(), "date_desc", "not-a-cursor", 3)
    page = await run_search(conn, SearchFilters(), "amount_asc", None, 1)
    assert page.next_cursor is not None
    with pytest.raises(QueryInvalid):  # a cursor of another sort
        await run_search(conn, SearchFilters(), "date_desc", page.next_cursor, 3)


def test_filters_reject_unknown_and_long_queries() -> None:
    with pytest.raises(ValidationError):
        SearchFilters.model_validate({"text": "x"})
    with pytest.raises(ValidationError):
        SearchFilters(q="x" * 201)
    assert filter_names(SearchFilters(q="x", tags=["a"], amount_min=Decimal(1))) == (
        "q,amount_min,tags"
    )
    assert filter_names(SearchFilters(q=" ", tags=[])) == ""
