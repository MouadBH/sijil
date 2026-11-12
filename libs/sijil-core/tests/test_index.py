import asyncio
import uuid
from collections.abc import AsyncIterator, Iterator, Sequence
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from sijil_core import index
from sijil_core.crypto import LocalKeyService, seal
from sijil_core.db.models import CredentialKind, TenantCredential, Workspace
from sijil_core.fields import fields_digest
from sijil_core.index import FieldsChanged, FieldsRow, IndexEngines, RetentionLocked
from sijil_core.settings import CoreSettings
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

NOW = datetime(2027, 3, 1, 10, 0, tzinfo=UTC)
SHA = "a" * 64


@pytest.fixture(scope="module")
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
def fresh_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
async def conn(index_db_url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(index_db_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as connection:
        await index.ensure_schema(connection)
        # Triggers forbid DELETE; TRUNCATE is forbidden too, so each test uses fresh ledger ids.
        yield connection
    await engine.dispose()


def row(ledger_id: uuid.UUID | None = None, /, **values: str | None) -> FieldsRow:
    return FieldsRow(
        ledger_id=ledger_id or uuid.uuid4(),
        document_type="invoice",
        values=values,
        confidence=dict.fromkeys(values, 0.9),
        signals={name: {"found": True} for name in values},
        flags=("low_confidence",),
        source=dict.fromkeys(values, "extraction"),
        model="m",
        extracted_at=NOW,
        updated_at=NOW,
    )


def sha(fields: FieldsRow) -> str:
    """The digest a seal of this row must carry."""
    return fields_digest(fields.values)


async def refused(conn: AsyncConnection, sql: str, **params: Any) -> str:
    with pytest.raises(DBAPIError) as exc:
        await conn.execute(text(sql), params)
    return str(exc.value)


async def test_ensure_schema_idempotent(conn: AsyncConnection) -> None:
    await index.ensure_schema(conn)
    await index.ensure_schema(conn)
    versions = (await conn.execute(text("SELECT version FROM index_schema"))).scalars().all()
    assert versions == list(range(1, index.SCHEMA_VERSION + 1))


def test_schema_version_is_the_latest_ddl() -> None:
    assert max(index._DDL) == index.SCHEMA_VERSION == 8


async def test_concurrent_first_runs_apply_once(fresh_db_url: str) -> None:
    """Nothing exists yet: four first runs race on the advisory lock, one applies the DDL."""
    engine = create_async_engine(fresh_db_url)
    try:
        results = await asyncio.gather(
            *(_run_ensure(engine) for _ in range(4)), return_exceptions=True
        )
        assert results == [None] * 4
        async with engine.connect() as c:
            versions = (await c.execute(text("SELECT version FROM index_schema"))).scalars().all()
        assert versions == list(range(1, index.SCHEMA_VERSION + 1))
    finally:
        await engine.dispose()


async def test_later_versions_apply_in_order(
    fresh_db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = create_async_engine(fresh_db_url)
    try:
        await _run_ensure(engine)
        later = index.SCHEMA_VERSION + 1
        monkeypatch.setattr(index, "SCHEMA_VERSION", later + 1)
        monkeypatch.setattr(
            index,
            "_DDL",
            {
                **index._DDL,
                later: ("CREATE TABLE later (id int PRIMARY KEY)",),
                later + 1: ("CREATE TABLE latest (id int REFERENCES later (id))",),
            },
        )
        await _run_ensure(engine)
        await _run_ensure(engine)
        async with engine.connect() as c:
            versions = (
                (await c.execute(text("SELECT version FROM index_schema ORDER BY version")))
                .scalars()
                .all()
            )
        assert versions == list(range(1, later + 2))
    finally:
        await engine.dispose()


async def test_triggers_raise_dedicated_sqlstate(conn: AsyncConnection) -> None:
    fields = row(number="F-1")
    await index.write_extraction(conn, fields)
    with pytest.raises(DBAPIError) as exc:
        await conn.execute(
            text("DELETE FROM document_fields WHERE ledger_id = :id"), {"id": fields.ledger_id}
        )
    assert exc.value.orig.sqlstate == "SJ001"  # type: ignore[union-attr]


async def test_concurrent_seals_are_race_safe(index_db_url: str) -> None:
    e1 = create_async_engine(index_db_url)
    e2 = create_async_engine(index_db_url)
    try:
        async with e1.begin() as c:
            await index.ensure_schema(c)
            fields = row(number="F-1")
            await index.write_extraction(c, fields)
        same, other = sha(fields), "b" * 64

        async def seal(engine: AsyncEngine, sha: str) -> FieldsRow:
            async with engine.connect() as c:
                result = await index.seal_fields(
                    c, fields.ledger_id, fields_sha256=sha, sealed_at=NOW
                )
                await c.commit()
                return result

        # Same hash from two connections: both succeed, one row.
        r1, r2 = await asyncio.gather(seal(e1, same), seal(e2, same))
        assert r1.fields_sha256 == r2.fields_sha256 == same
        # A different hash loses.
        with pytest.raises(RetentionLocked):
            await seal(e2, other)

        racing = row(number="F-2")
        async with e1.begin() as c:
            await index.write_extraction(c, racing)

        async def seal_racing(engine: AsyncEngine, sha: str) -> FieldsRow:
            async with engine.connect() as c:
                result = await index.seal_fields(
                    c, racing.ledger_id, fields_sha256=sha, sealed_at=NOW
                )
                await c.commit()
                return result

        outcomes = await asyncio.gather(
            seal_racing(e1, sha(racing)), seal_racing(e2, other), return_exceptions=True
        )
        winners = [o for o in outcomes if isinstance(o, FieldsRow)]
        # Another hash never wins: refused as changed values, or as locked once sealed.
        losers = [o for o in outcomes if isinstance(o, RetentionLocked | FieldsChanged)]
        assert len(winners) == 1
        assert len(losers) == 1
        assert winners[0].fields_sha256 == sha(racing)
    finally:
        await e1.dispose()
        await e2.dispose()


async def _run_ensure(engine: AsyncEngine) -> None:
    async with engine.begin() as c:
        await index.ensure_schema(c)


async def test_write_and_read_roundtrip(conn: AsyncConnection) -> None:
    original = row(number="F-1", total_ttc="120.00")
    await index.write_extraction(conn, original)
    assert await index.read_fields(conn, original.ledger_id) == original
    assert await index.read_fields(conn, uuid.uuid4()) is None


async def test_write_extraction_upserts(conn: AsyncConnection) -> None:
    first = row(number="F-1")
    await index.write_extraction(conn, first)
    second = row(first.ledger_id, number="F-2")
    await index.write_extraction(conn, second)
    assert await index.read_fields(conn, first.ledger_id) == second


async def test_sealed_row_update_refused(conn: AsyncConnection) -> None:
    fields = row(number="F-1")
    await index.write_extraction(conn, fields)
    await index.seal_fields(conn, fields.ledger_id, fields_sha256=sha(fields), sealed_at=NOW)
    message = await refused(
        conn,
        "UPDATE document_fields SET \"values\" = '{}'::jsonb WHERE ledger_id = :id",
        id=fields.ledger_id,
    )
    assert "retention.locked" in message
    with pytest.raises(RetentionLocked):
        await index.write_extraction(conn, row(fields.ledger_id, number="F-2"))
    with pytest.raises(RetentionLocked):
        await index.update_fields(
            conn, fields.ledger_id, {"number": "X"}, source="user", updated_by=None, now=NOW
        )


async def test_sealed_columns_only_set_together(conn: AsyncConnection) -> None:
    fields = row(number="F-1")
    await index.write_extraction(conn, fields)
    message = await refused(
        conn,
        "UPDATE document_fields SET sealed_at = now() WHERE ledger_id = :id",
        id=fields.ledger_id,
    )
    assert "document_fields_sealed_together" in message


async def test_fields_row_delete_refused(conn: AsyncConnection) -> None:
    fields = row(number="F-1")
    await index.write_extraction(conn, fields)
    assert "retention.locked" in await refused(
        conn, "DELETE FROM document_fields WHERE ledger_id = :id", id=fields.ledger_id
    )
    assert "retention.locked" in await refused(conn, "TRUNCATE document_fields")


async def test_extraction_run_append_only(conn: AsyncConnection) -> None:
    ledger_id = uuid.uuid4()
    await conn.execute(
        text(
            "INSERT INTO extraction_run (ledger_id, run, model, raw) "
            "VALUES (:id, 'a', 'm', CAST('{}' AS jsonb))"
        ),
        {"id": ledger_id},
    )
    assert "index.append_only" in await refused(conn, "UPDATE extraction_run SET model = 'x'")
    assert "index.append_only" in await refused(conn, "DELETE FROM extraction_run")
    assert "index.append_only" in await refused(conn, "TRUNCATE extraction_run")


async def test_append_run_records_each_run(conn: AsyncConnection) -> None:
    ledger_id = uuid.uuid4()
    await index.append_run(conn, ledger_id, "a", "qwen3:4b", {"number": "F-1"}, NOW)
    await index.append_run(conn, ledger_id, "b", "qwen3:4b", {"error": "x"}, NOW)
    rows = (
        await conn.execute(
            text(
                "SELECT run, model, raw, at FROM extraction_run WHERE ledger_id = :id ORDER BY id"
            ),
            {"id": ledger_id},
        )
    ).all()
    assert [(r.run, r.model, r.raw, r.at) for r in rows] == [
        ("a", "qwen3:4b", {"number": "F-1"}, NOW),
        ("b", "qwen3:4b", {"error": "x"}, NOW),
    ]


async def test_update_fields_merges_and_marks_user(conn: AsyncConnection) -> None:
    fields = row(number="F-1", total_ttc="120.00")
    await index.write_extraction(conn, fields)
    later = datetime(2027, 3, 2, tzinfo=UTC)
    editor = uuid.uuid4()
    updated = await index.update_fields(
        conn, fields.ledger_id, {"number": "F-9", "supplier_ice": None},
        source="user", updated_by=editor, now=later,
    )  # fmt: skip
    assert updated.values == {"number": "F-9", "total_ttc": "120.00", "supplier_ice": None}
    assert updated.source == {"number": "user", "total_ttc": "extraction", "supplier_ice": "user"}
    assert updated.confidence["number"] == 1.0
    assert updated.confidence["supplier_ice"] == 1.0
    assert updated.confidence["total_ttc"] == 0.9
    assert (updated.updated_at, updated.updated_by) == (later, editor)
    assert await index.read_fields(conn, fields.ledger_id) == updated


async def test_update_fields_document_type_moves_the_column_too(conn: AsyncConnection) -> None:
    fields = row(document_type="invoice", number="F-1")
    await index.write_extraction(conn, fields)
    updated = await index.update_fields(
        conn, fields.ledger_id, {"document_type": "receipt"}, source="user", updated_by=None,
        now=NOW,
    )  # fmt: skip
    assert (updated.document_type, updated.values["document_type"]) == ("receipt", "receipt")
    kept = await index.update_fields(
        conn, fields.ledger_id, {"number": "F-2"}, source="user", updated_by=None, now=NOW
    )
    assert kept.document_type == "receipt"
    cleared = await index.update_fields(
        conn, fields.ledger_id, {"document_type": None}, source="user", updated_by=None, now=NOW
    )
    assert cleared.document_type is None


async def test_update_fields_extraction_source_keeps_confidence(conn: AsyncConnection) -> None:
    fields = row(number="F-1")
    await index.write_extraction(conn, fields)
    updated = await index.update_fields(
        conn, fields.ledger_id, {"number": "F-2"}, source="extraction", updated_by=None, now=NOW
    )
    assert updated.confidence == {"number": 0.9}
    assert updated.source == {"number": "extraction"}


async def test_update_fields_unknown_ledger(conn: AsyncConnection) -> None:
    with pytest.raises(LookupError):
        await index.update_fields(
            conn, uuid.uuid4(), {"a": "b"}, source="user", updated_by=None, now=NOW
        )


async def test_seal_fields_idempotent_same_hash_different_refused(conn: AsyncConnection) -> None:
    fields = row(number="F-1")
    await index.write_extraction(conn, fields)
    digest = sha(fields)
    sealed = await index.seal_fields(conn, fields.ledger_id, fields_sha256=digest, sealed_at=NOW)
    assert (sealed.sealed_at, sealed.fields_sha256) == (NOW, digest)
    later = datetime(2027, 4, 1, tzinfo=UTC)
    again = await index.seal_fields(conn, fields.ledger_id, fields_sha256=digest, sealed_at=later)
    assert again == sealed
    with pytest.raises(RetentionLocked):
        await index.seal_fields(conn, fields.ledger_id, fields_sha256="b" * 64, sealed_at=later)
    with pytest.raises(LookupError):
        await index.seal_fields(conn, uuid.uuid4(), fields_sha256=SHA, sealed_at=NOW)


async def test_seal_fields_refuses_values_that_changed_since_they_were_hashed(
    conn: AsyncConnection,
) -> None:
    """The digest is recomputed from the row locked FOR UPDATE: a caller that hashed values an
    edit has since replaced gets FieldsChanged, and nothing is sealed."""
    fields = row(number="F-1", total_ttc="120.00")
    await index.write_extraction(conn, fields)
    stale = sha(fields)
    await index.update_fields(
        conn, fields.ledger_id, {"total_ttc": "130.00"}, source="user", updated_by=None, now=NOW
    )
    with pytest.raises(FieldsChanged):
        await index.seal_fields(conn, fields.ledger_id, fields_sha256=stale, sealed_at=NOW)
    current = await index.read_fields(conn, fields.ledger_id)
    assert current is not None and current.sealed_at is None
    sealed = await index.seal_fields(
        conn, fields.ledger_id, fields_sha256=fields_digest(current.values), sealed_at=NOW
    )
    assert sealed.fields_sha256 == fields_digest(sealed.values)


async def test_set_scores_replaces_confidence_and_flags_only(conn: AsyncConnection) -> None:
    fields = row(number="F-1", total_ttc="120.00")
    await index.write_extraction(conn, fields)
    await index.set_scores(
        conn, fields.ledger_id, {"number": 0.95, "total_ttc": 0.5}, ("possible_duplicate",)
    )
    stored = await index.read_fields(conn, fields.ledger_id)
    assert stored is not None
    assert stored.confidence == {"number": 0.95, "total_ttc": 0.5}
    assert stored.flags == ("possible_duplicate",)
    assert (stored.values, stored.signals, stored.updated_at) == (
        fields.values,
        fields.signals,
        fields.updated_at,
    )
    with pytest.raises(LookupError):
        await index.set_scores(conn, uuid.uuid4(), {}, ())


async def test_set_scores_refused_once_sealed(conn: AsyncConnection) -> None:
    fields = row(number="F-1")
    await index.write_extraction(conn, fields)
    await index.seal_fields(conn, fields.ledger_id, fields_sha256=sha(fields), sealed_at=NOW)
    with pytest.raises(RetentionLocked):
        await index.set_scores(conn, fields.ledger_id, {"number": 1.0}, ())


async def test_find_possible_duplicates_matches_number_ice_ttc(conn: AsyncConnection) -> None:
    ice = f"{uuid.uuid4().int % 10**15:015d}"
    base = {"number": "F-77", "supplier_ice": ice, "total_ttc": "120.00"}
    me, twin = row(**base), row(**base)
    other_number = row(**{**base, "number": "F-78"})
    other_total = row(**{**base, "total_ttc": "121.00"})
    # Another first digit (a bare "0" + ice[1:] is the same ICE one time in ten).
    other_ice = row(**{**base, "supplier_ice": ("1" if ice[0] == "0" else "0") + ice[1:]})
    for r in (me, twin, other_number, other_total, other_ice):
        await index.write_extraction(conn, r)
    found = await index.find_possible_duplicates(conn, me.ledger_id, "F-77", ice, "120.00")
    assert found == [twin.ledger_id]


async def test_known_supplier_is_name_on_latest_sealed_row(conn: AsyncConnection) -> None:
    ice = f"{uuid.uuid4().int % 10**15:015d}"
    old, new, unsealed = (
        row(supplier_ice=ice, supplier_name=n) for n in ("Old SARL", "New SARL", "Draft SARL")
    )
    for r in (old, new, unsealed):
        await index.write_extraction(conn, r)
    await index.seal_fields(conn, old.ledger_id, fields_sha256=sha(old), sealed_at=NOW)
    await index.seal_fields(
        conn, new.ledger_id, fields_sha256=sha(new), sealed_at=datetime(2027, 5, 1, tzinfo=UTC)
    )
    assert await index.known_supplier(conn, ice) == "New SARL"
    assert await index.known_supplier(conn, "999999999999999") is None


# Engines ------------------------------------------------------------------------------------


class FakeEngine:
    def __init__(self, url: URL) -> None:
        self.url = url
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


class FakeSessions:
    """Stands in for the control-plane sessionmaker: one credential row."""

    def __init__(self, credential: TenantCredential | None) -> None:
        self._credential = credential

    def __call__(self) -> "FakeSessions":
        return self

    async def __aenter__(self) -> "FakeSessions":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def scalar(self, _statement: object) -> TenantCredential | None:
        return self._credential


def workspace_named(slug: str) -> Workspace:
    return Workspace(
        id=uuid.uuid4(),
        name="A",
        slug=slug,
        db_name=f"paperless_{slug}",
        index_db_name=f"index_{slug}",
        kms_key_id=f"kek-{slug}",
    )


def credential_for(ws: Workspace, keys: LocalKeyService, password: str) -> TenantCredential:
    kind = CredentialKind.index_db_password
    envelope = seal(password.encode(), keys, ws.kms_key_id, f"{ws.id}:{kind}")
    return TenantCredential(workspace_id=ws.id, kind=kind, envelope=envelope.to_bytes())


SETTINGS = CoreSettings.model_construct(
    region="ma-cas", environment="test", index_db_host="pg.internal", index_db_port=6543
)


def engines_for(
    ws: Workspace, keys: LocalKeyService, created: list[FakeEngine], ensured: list[Any], **kw: Any
) -> IndexEngines:
    def factory(url: URL) -> Any:
        engine = FakeEngine(url)
        created.append(engine)
        return engine

    async def ensure(engine: Any) -> None:
        ensured.append(engine)

    return IndexEngines(
        SETTINGS,
        keys,
        FakeSessions(credential_for(ws, keys, "s3cret/pw:@")),  # type: ignore[arg-type]
        engine_factory=factory,
        ensure=ensure,
        **kw,
    )


async def test_index_db_password_from_credential() -> None:
    keys = LocalKeyService(b"k" * 32)
    ws = workspace_named("a")
    created: list[FakeEngine] = []
    ensured: list[Any] = []
    engines = engines_for(ws, keys, created, ensured)
    engine = await engines.engine(ws)
    assert engine is cast(Any, created[0])
    url = created[0].url
    assert (url.drivername, url.username, url.password) == (
        "postgresql+asyncpg", "index_a", "s3cret/pw:@",
    )  # fmt: skip
    assert (url.host, url.port, url.database) == ("pg.internal", 6543, "index_a")
    assert await engines.engine(ws) is engine
    assert len(created) == 1
    assert ensured == [engine]  # schema ensured once per engine


async def test_credential_bound_to_workspace_context() -> None:
    keys = LocalKeyService(b"k" * 32)
    ws, other = workspace_named("a"), workspace_named("b")
    created: list[FakeEngine] = []
    engines = engines_for(ws, keys, created, [])
    from sijil_core.crypto import DecryptionError

    with pytest.raises(DecryptionError):
        await engines.engine(other)
    assert created == []


async def test_engines_lru_disposes_evicted() -> None:
    keys = LocalKeyService(b"k" * 32)
    created: list[FakeEngine] = []
    ws = [workspace_named(f"w{i}") for i in range(3)]
    # The helper's single credential only fits ws[0]: every workspace gets a fixed password below.
    engines = engines_for(ws[0], keys, created, [], max_engines=2)

    async def password(w: Workspace) -> str:
        return "pw"

    engines._password = password  # type: ignore[method-assign,assignment]
    e0 = await engines.engine(ws[0])
    e1 = await engines.engine(ws[1])
    await engines.engine(ws[0])  # w0 is now most recently used
    await engines.engine(ws[2])  # evicts w1
    assert [e.disposed for e in created] == [False, True, False]
    assert cast(Any, created[1]) is e1
    assert e0 is cast(Any, created[0])
    await engines.aclose()
    assert all(e.disposed for e in created)


def plain_engines(ensure: Any, max_engines: int = 32) -> tuple[IndexEngines, list[FakeEngine]]:
    created: list[FakeEngine] = []

    def factory(url: URL) -> Any:
        engine = FakeEngine(url)
        created.append(engine)
        return engine

    engines = IndexEngines(
        SETTINGS,
        LocalKeyService(b"k" * 32),
        FakeSessions(None),  # type: ignore[arg-type]
        engine_factory=factory,
        ensure=ensure,
        max_engines=max_engines,
    )

    async def password(_w: Workspace) -> str:
        return "pw"

    engines._password = password  # type: ignore[method-assign,assignment]
    return engines, created


async def test_slow_workspace_does_not_block_cached_ones() -> None:
    gate = asyncio.Event()

    async def ensure(engine: Any) -> None:
        if engine.url.database == "index_slow":
            await gate.wait()

    engines, _ = plain_engines(ensure)
    fast, slow = workspace_named("fast"), workspace_named("slow")
    cached = await engines.engine(fast)
    pending = asyncio.create_task(engines.engine(slow))
    await asyncio.sleep(0.05)
    assert not pending.done()
    assert await asyncio.wait_for(engines.engine(fast), 0.5) is cached
    # A different new workspace is not held up by the slow one either.
    other = await asyncio.wait_for(engines.engine(workspace_named("other")), 0.5)
    assert other is not None
    gate.set()
    await pending


async def test_failed_build_then_concurrent_callers_cache_one_engine() -> None:
    attempts = 0

    async def ensure(_engine: Any) -> None:
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(0.05)
        if attempts == 1:
            raise ConnectionError("down")

    engines, created = plain_engines(ensure)
    ws = workspace_named("flaky")
    first = asyncio.create_task(engines.engine(ws))
    second = asyncio.create_task(engines.engine(ws))  # waits behind the failing build
    with pytest.raises(ConnectionError):
        await first
    await asyncio.sleep(0.02)  # second is now building
    third = asyncio.create_task(engines.engine(ws))  # arrives while it builds
    cached = await second
    assert await third is cached
    assert [e for e in created if not e.disposed] == [cast(Any, cached)]
    assert len(engines._engines) == 1


async def test_dispose_failure_is_best_effort() -> None:
    async def ensure(_engine: Any) -> None:
        return None

    engines, created = plain_engines(ensure, max_engines=1)

    async def bad() -> None:
        raise OSError("boom")

    a, b, c = (workspace_named(n) for n in "abc")
    await engines.engine(a)
    created[0].dispose = bad  # type: ignore[method-assign]
    await engines.engine(b)  # evicts a: its dispose fails, the caller is unaffected
    await engines.engine(c)  # evicts b
    assert created[1].disposed
    created[2].dispose = bad  # type: ignore[method-assign]
    await engines.aclose()  # does not raise


async def test_seal_loser_after_stale_read_is_retention_locked(
    index_db_url: str, conn: AsyncConnection
) -> None:
    """The row looks unsealed at SELECT, is sealed before the UPDATE: the trigger refuses."""
    fields = row(number="F-1")
    await index.write_extraction(conn, fields)
    other = create_async_engine(index_db_url, isolation_level="AUTOCOMMIT")

    class Snapshot:
        def __init__(self, rows: Sequence[Any]) -> None:
            self._rows = rows

        def mappings(self) -> "Snapshot":
            return self

        def first(self) -> Any:
            return self._rows[0] if self._rows else None

    class Stale:
        async def execute(self, statement: Any, params: Any = None) -> Any:
            result = await conn.execute(statement, params)
            if "FOR UPDATE" not in str(statement):
                return result
            unsealed = result.mappings().all()
            async with other.connect() as c2:  # the winner seals in between
                await index.seal_fields(
                    c2, fields.ledger_id, fields_sha256=sha(fields), sealed_at=NOW
                )
            return Snapshot(unsealed)

    try:
        with pytest.raises(RetentionLocked):
            await index._seal(Stale(), fields.ledger_id, sha(fields), NOW)  # type: ignore[arg-type]
    finally:
        await other.dispose()


async def test_concurrent_first_calls_build_one_engine() -> None:
    calls = 0

    async def ensure(_engine: Any) -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)

    engines, created = plain_engines(ensure)
    ws = workspace_named("one")
    results = await asyncio.gather(*(engines.engine(ws) for _ in range(5)))
    assert len(created) == 1
    assert calls == 1
    assert all(r is results[0] for r in results)
