"""Index schema v2: correction notes (append-only) and the gated purge of one document's rows,
the only DELETE the index triggers allow (removal before seal, approved deletion after retention).
"""

import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from sijil_core import index
from sijil_core.fields import fields_digest
from sijil_core.index import FieldsRow, RetentionLocked
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

NOW = datetime(2027, 3, 1, 10, 0, tzinfo=UTC)
TABLES = ("document_fields", "extraction_run", "correction_note")


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


async def _document(conn: AsyncConnection, *, sealed: bool = False) -> uuid.UUID:
    """A document's fields row, two model runs and a correction note (SAMPLE values)."""
    ledger_id = uuid.uuid4()
    values: dict[str, str | None] = {"number": "F-SAMPLE-1", "total_ttc": "1500.00"}
    await index.write_extraction(
        conn,
        FieldsRow(
            ledger_id=ledger_id, document_type="invoice", values=values,
            source=dict.fromkeys(values, "extraction"), updated_at=NOW,
        ),
    )  # fmt: skip
    for run in ("a", "b"):
        await index.append_run(conn, ledger_id, run, "m", {"number": "F-SAMPLE-1"}, NOW)
    await _note(conn, ledger_id)
    if sealed:
        await index.seal_fields(conn, ledger_id, fields_sha256=fields_digest(values), sealed_at=NOW)
    return ledger_id


async def _note(conn: AsyncConnection, ledger_id: uuid.UUID) -> None:
    await conn.execute(
        text(
            "INSERT INTO correction_note (id, ledger_id, field, value, reason, created_by, "
            "created_at) VALUES (:id, :ledger_id, 'total_ttc', '1600.00', "
            "'Avoir SAMPLE reçu après le scellement', :by, :at)"
        ),
        {"id": uuid.uuid4(), "ledger_id": ledger_id, "by": uuid.uuid4(), "at": NOW},
    )


async def _counts(conn: AsyncConnection, ledger_id: uuid.UUID) -> dict[str, int]:
    counts = {}
    for table in TABLES:
        found = await conn.scalar(
            text(f"SELECT count(*) FROM {table} WHERE ledger_id = :id"),  # noqa: S608
            {"id": ledger_id},
        )
        counts[table] = int(found or 0)
    return counts


def _gated_delete(table: str, *, gate: uuid.UUID, target: uuid.UUID) -> str:
    """The purge gate opened by hand, then a DELETE: one statement, so one transaction even on
    the AUTOCOMMIT test connection. The ids are generated uuids, safe to inline."""
    delete = f"DELETE FROM {table} WHERE ledger_id = '{target}'"  # noqa: S608
    return f"DO $$ BEGIN PERFORM set_config('sijil.purge', '{gate}', true); {delete}; END $$"


FULL = {"document_fields": 1, "extraction_run": 2, "correction_note": 1}
EMPTY = dict.fromkeys(TABLES, 0)


async def test_purge_removes_unsealed_rows(conn: AsyncConnection) -> None:
    target, other = await _document(conn), await _document(conn)
    await index.purge_document(conn, target)
    assert await _counts(conn, target) == EMPTY
    assert await _counts(conn, other) == FULL
    await index.purge_document(conn, target)  # nothing left: a retry is a no-op


async def test_plain_delete_still_refused(conn: AsyncConnection) -> None:
    ledger_id, other = await _document(conn), await _document(conn)
    codes = {"document_fields": "SJ001", "extraction_run": "SJ002", "correction_note": "SJ002"}
    for table, code in codes.items():
        for sql in (f"DELETE FROM {table} WHERE ledger_id = :id", f"TRUNCATE {table}"):  # noqa: S608
            with pytest.raises(DBAPIError) as exc:
                await conn.execute(text(sql), {"id": ledger_id})
            assert exc.value.orig.sqlstate == code, sql  # type: ignore[union-attr]
    # The gate names one document: set for another, the delete is still refused.
    for table in TABLES:
        with pytest.raises(DBAPIError, match="retention.locked|index.append_only"):
            await conn.execute(text(_gated_delete(table, gate=other, target=ledger_id)))
    # Control: naming the right document opens it (so the refusals above are the gate's).
    control = await _document(conn)
    for table in TABLES:
        await conn.execute(text(_gated_delete(table, gate=control, target=control)))
    assert await _counts(conn, control) == EMPTY
    # SET LOCAL ends with the purge's transaction: nothing leaks to the next statement.
    await index.purge_document(conn, other)
    with pytest.raises(DBAPIError, match="retention.locked"):
        await conn.execute(
            text("DELETE FROM document_fields WHERE ledger_id = :id"), {"id": ledger_id}
        )
    assert await _counts(conn, ledger_id) == FULL


async def test_purge_of_sealed_needs_sealed_flag(conn: AsyncConnection) -> None:
    ledger_id = await _document(conn, sealed=True)
    with pytest.raises(RetentionLocked):
        await index.purge_document(conn, ledger_id)
    # One transaction: the runs and the note are not gone either.
    assert await _counts(conn, ledger_id) == FULL
    # The purge GUC alone does not open a sealed row.
    with pytest.raises(DBAPIError, match="retention.locked"):
        await conn.execute(text(_gated_delete("document_fields", gate=ledger_id, target=ledger_id)))
    await index.purge_document(conn, ledger_id, sealed=True)
    assert await _counts(conn, ledger_id) == EMPTY


async def test_purge_joins_the_callers_transaction(index_db_url: str) -> None:
    engine = create_async_engine(index_db_url)
    try:
        async with engine.connect() as c:
            await index.ensure_schema(c)
            await c.commit()
            ledger_id = await _document(c)
            await c.commit()
            trans = await c.begin()
            await index.purge_document(c, ledger_id)
            await trans.rollback()
            assert await _counts(c, ledger_id) == FULL
    finally:
        await engine.dispose()


async def test_correction_note_append_only(conn: AsyncConnection) -> None:
    ledger_id = await _document(conn)
    for sql in (
        "UPDATE correction_note SET reason = 'autre' WHERE ledger_id = :id",
        "UPDATE correction_note SET value = NULL WHERE ledger_id = :id",
        "DELETE FROM correction_note WHERE ledger_id = :id",
        "TRUNCATE correction_note",
    ):
        with pytest.raises(DBAPIError, match="index.append_only") as exc:
            await conn.execute(text(sql), {"id": ledger_id})
        assert exc.value.orig.sqlstate == "SJ002", sql  # type: ignore[union-attr]
    # A note on a sealed document is still accepted: that is what notes are for.
    sealed = await _document(conn, sealed=True)
    await _note(conn, sealed)
    assert (await _counts(conn, sealed))["correction_note"] == 2


@pytest.fixture
def fresh_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


async def test_v1_database_upgrades_to_v2(
    fresh_db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An existing workspace's index (v1, rows present) gains v2 on its next `ensure_schema`."""
    engine = create_async_engine(fresh_db_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as c:
            with monkeypatch.context() as m:
                m.setattr(index, "_DDL", {1: index._DDL[1]})
                await index.ensure_schema(c)
            ledger_id = uuid.uuid4()
            await index.append_run(c, ledger_id, "a", "m", {}, NOW)
            await index.ensure_schema(c)
            versions = (await c.execute(text("SELECT version FROM index_schema"))).scalars()
            assert sorted(versions) == list(range(1, index.SCHEMA_VERSION + 1))
            await _note(c, ledger_id)
            await index.purge_document(c, ledger_id)
            assert await _counts(c, ledger_id) == EMPTY
    finally:
        await engine.dispose()
