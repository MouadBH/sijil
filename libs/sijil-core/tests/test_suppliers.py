"""Index schema v7: the workspace's supplier records (`supplier`), learnt from reviewers (REC-5).

A record is keyed by ICE. A newly confirmed name replaces the record's name and the previous one
is kept as an alias (deduplicated after `search_normalise`, at most 10). Without an ICE nothing
is learnt. Records are workspace knowledge, not one document's data: the purge leaves them.
"""

import random
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from sijil_core import index, suppliers
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

NOW = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
REVIEWER = uuid.UUID("00000000-0000-4000-8000-0000000000aa")


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


def _ice() -> str:
    """A fresh SAMPLE ICE (fictitious): the module shares one database."""
    return "".join(random.choice("0123456789") for _ in range(15))  # noqa: S311


def test_schema_v7_is_the_supplier_table() -> None:
    assert index.SCHEMA_VERSION == 7
    assert any("CREATE TABLE supplier" in sql for sql in index._DDL[7])  # noqa: SLF001


async def test_learn_upserts_and_keeps_aliases(conn: AsyncConnection) -> None:
    ice = _ice()
    first = await suppliers.learn(
        conn, ice=ice, name="Atlas Telecom SAMPLE", source="review", actor=REVIEWER, now=NOW
    )
    assert (first.new, first.changed) == (True, True)
    assert first.supplier is not None
    assert (first.supplier.name, first.supplier.aliases) == ("Atlas Telecom SAMPLE", ())
    assert (first.supplier.source, first.supplier.updated_by) == ("review", REVIEWER)

    again = await suppliers.learn(
        conn, ice=ice, name="Atlas Telecom SAMPLE", source="seal", actor=REVIEWER, now=NOW
    )
    assert (again.new, again.changed) == (False, False)

    renamed = await suppliers.learn(
        conn, ice=ice, name="Atlas Télécom Services SAMPLE SARL", source="seal",
        actor=REVIEWER, now=NOW,
    )  # fmt: skip
    assert (renamed.new, renamed.changed) == (False, True)
    stored = await suppliers.get(conn, ice)
    assert stored is not None
    assert stored.name == "Atlas Télécom Services SAMPLE SARL"
    assert stored.aliases == ("Atlas Telecom SAMPLE",)
    assert stored.source == "seal"

    # The same name again, as an alias spelled differently: never a duplicate alias, and the
    # name that comes back leaves the aliases.
    await suppliers.learn(
        conn, ice=ice, name="ATLAS TÉLÉCOM SAMPLE", source="correction", actor=REVIEWER, now=NOW
    )
    stored = await suppliers.get(conn, ice)
    assert stored is not None
    assert stored.name == "ATLAS TÉLÉCOM SAMPLE"
    assert stored.aliases == ("Atlas Télécom Services SAMPLE SARL",)


async def test_aliases_keep_the_ten_latest(conn: AsyncConnection) -> None:
    ice = _ice()
    for n in range(13):
        await suppliers.learn(
            conn, ice=ice, name=f"SAMPLE Name {n}", source="review", actor=None, now=NOW
        )
    stored = await suppliers.get(conn, ice)
    assert stored is not None
    assert stored.name == "SAMPLE Name 12"
    assert stored.aliases == tuple(f"SAMPLE Name {n}" for n in range(2, 12))


@pytest.mark.parametrize("ice", [None, "", "12345", "00190583700004X", "0019058370000411"])
async def test_learn_ignores_missing_ice(conn: AsyncConnection, ice: str | None) -> None:
    before = await conn.scalar(text("SELECT count(*) FROM supplier"))
    change = await suppliers.learn(
        conn, ice=ice, name="SAMPLE Sans ICE", source="review", actor=REVIEWER, now=NOW
    )
    assert (change.supplier, change.new, change.changed) == (None, False, False)
    assert await conn.scalar(text("SELECT count(*) FROM supplier")) == before


async def test_learn_ignores_a_missing_name(conn: AsyncConnection) -> None:
    ice = _ice()
    change = await suppliers.learn(conn, ice=ice, name=None, source="seal", actor=REVIEWER, now=NOW)
    assert change.supplier is None
    assert await suppliers.get(conn, ice) is None


async def test_find_by_ices_and_correspondent_id(conn: AsyncConnection) -> None:
    a, b = _ice(), _ice()
    await suppliers.learn(conn, ice=a, name="SAMPLE A", source="seal", actor=None, now=NOW)
    await suppliers.learn(conn, ice=b, name="SAMPLE B", source="seal", actor=None, now=NOW)
    found = await suppliers.find(conn, [b, a, _ice()])
    assert sorted(s.name for s in found) == ["SAMPLE A", "SAMPLE B"]
    await suppliers.set_correspondent_id(conn, a, 42)
    stored = await suppliers.get(conn, a)
    assert stored is not None and stored.paperless_correspondent_id == 42


async def test_the_table_refuses_a_malformed_record(conn: AsyncConnection) -> None:
    with pytest.raises(DBAPIError):
        await conn.execute(
            text(
                "INSERT INTO supplier (ice, name, source, updated_at) "
                "VALUES ('123', 'SAMPLE', 'review', now())"
            )
        )
    with pytest.raises(DBAPIError):
        await conn.execute(
            text(
                "INSERT INTO supplier (ice, name, source, updated_at) "
                "VALUES (:ice, 'SAMPLE', 'model', now())"
            ),
            {"ice": _ice()},
        )


async def test_the_purge_leaves_supplier_records(conn: AsyncConnection) -> None:
    """Workspace knowledge, not the document's data: purging the document it was learnt from
    keeps the record."""
    ice = _ice()
    ledger_id = uuid.uuid4()
    values = dict.fromkeys(FIELD_NAMES) | {"supplier_name": "SAMPLE Purge", "supplier_ice": ice}
    await index.write_extraction(
        conn,
        FieldsRow(ledger_id=ledger_id, values=values, updated_at=NOW),
    )
    await suppliers.learn(conn, ice=ice, name="SAMPLE Purge", source="review", actor=None, now=NOW)
    await index.purge_document(conn, ledger_id)
    assert await index.read_fields(conn, ledger_id) is None
    assert await suppliers.get(conn, ice) is not None
