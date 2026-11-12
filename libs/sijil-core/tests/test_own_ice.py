"""The interim guard against the workspace's own ICE (final review I2, until the organisation's
ICE is a setting): an ICE printed on at least half of the workspace's live documents, with at
least 5 live documents, is the workspace's own (`suppliers.own_ices`). Only its own company is
printed on most of what a company archives (as the client on purchases, the seller on sales).
"""

import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from sijil_core import index, suppliers
from sijil_core.db.models import DocumentState
from sijil_core.search_index import SearchLedgerFacts, mark_search_state, upsert_search_doc
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

NOW = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
OWN = "002999888000011"  # SAMPLE: the workspace's own company (fictitious)
OTHER = "001905837000041"  # SAMPLE: a supplier (fictitious)


@pytest.fixture
def index_db_url() -> Iterator[str]:
    """One per test: the guard counts every live row of the index."""
    yield from throwaway_index_db()


@pytest.fixture
async def conn(index_db_url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(index_db_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as connection:
        await index.ensure_schema(connection)
        yield connection
    await engine.dispose()


async def _documents(conn: AsyncConnection, total: int, carrying: int) -> list[uuid.UUID]:
    """`total` live search rows, the first `carrying` of which print OWN (SAMPLE text)."""
    ids = []
    for n in range(total):
        ledger_id = uuid.uuid4()
        facts = SearchLedgerFacts(
            ledger_id=ledger_id, state=DocumentState.filed, ingested_at=NOW, region="ma-cas",
            now=NOW,
        )  # fmt: skip
        client = f"Client : Entreprise Exemple SARL, ICE {OWN}\n" if n < carrying else ""
        content = f"Facture SAMPLE F-{n}\nFournisseur ICE {OTHER}\n{client}"
        await upsert_search_doc(conn, facts, None, [], content)
        ids.append(ledger_id)
    return ids


@pytest.mark.parametrize(
    ("total", "carrying", "own"),
    [
        (4, 4, False),  # under 5 live documents: never
        (5, 3, True),  # at least half of 5
        (5, 2, False),
        (6, 3, True),  # exactly half
    ],
)
async def test_own_ice_threshold(
    conn: AsyncConnection, total: int, carrying: int, own: bool
) -> None:
    await _documents(conn, total, carrying)
    found = await suppliers.own_ices(conn, [OWN, OTHER, "123"])
    # OTHER is printed on every document: with 5 or more, it counts too (the heuristic's cost).
    assert (OWN in found) is own
    assert (OTHER in found) is (total >= suppliers.OWN_ICE_MIN_DOCUMENTS)
    assert "123" not in found


async def test_terminal_documents_do_not_count(conn: AsyncConnection) -> None:
    ids = await _documents(conn, 6, 3)
    await mark_search_state(conn, ids[0], DocumentState.removed)  # 5 live, 2 carrying
    assert OWN not in await suppliers.own_ices(conn, [OWN])


async def test_no_candidate_no_query(conn: AsyncConnection) -> None:
    await _documents(conn, 5, 5)
    assert await suppliers.own_ices(conn, []) == frozenset()


async def test_the_count_uses_the_trigram_index(conn: AsyncConnection) -> None:
    """One indexed query: the text is matched through `search_doc_content_trgm`."""
    await _documents(conn, 5, 3)
    await conn.execute(text("SET enable_seqscan = off"))
    plan = "\n".join(
        (
            await conn.execute(
                text(f"EXPLAIN {suppliers._OWN_ICES.text}"),  # noqa: S608, SLF001
                suppliers._own_params([OWN]),  # noqa: SLF001
            )
        ).scalars()
    )
    await conn.execute(text("RESET enable_seqscan"))
    assert "search_doc_content_trgm" in plan, plan
