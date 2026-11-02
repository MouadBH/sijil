"""The per-workspace index database: extracted field values, kept out of the shared database.

Document content (field values) lives only in `index_<ref>`, one database per workspace, owned
by a role of the same name. The control plane and the ledger never hold values, only hashes.

A row of `document_fields` is frozen once sealed, at the database level (trigger
`document_fields_frozen`): the error message carries the machine code `retention.locked`, which
this module raises as `RetentionLocked` so callers can answer 409. `extraction_run` is evidence
of what each model run said and is append-only.

The DDL is plain SQL, versioned by `SCHEMA_VERSION` and recorded in `index_schema`.
"""

import asyncio
import json
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Literal
from uuid import UUID

from sqlalchemy import URL, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sijil_core.crypto import Envelope, KeyService, open_envelope
from sijil_core.db.models import CredentialKind, TenantCredential, Workspace
from sijil_core.settings import CoreSettings

log = logging.getLogger(__name__)

SCHEMA_VERSION: Final = 1
MAX_ENGINES: Final = 32
LOCKED_CODE: Final = "retention.locked"
# SQLSTATE the triggers raise it with (custom class "SJ": Sijil).
LOCKED_SQLSTATE: Final = "SJ001"
# Serialises concurrent first runs of the DDL on one database.
_SCHEMA_LOCK_KEY: Final = 0x53696A696C  # "Sijil"

_DDL: Final[dict[int, tuple[str, ...]]] = {
    1: (
        """CREATE TABLE document_fields (
        ledger_id uuid PRIMARY KEY,
        document_type text,
        "values" jsonb NOT NULL,
        confidence jsonb NOT NULL,
        signals jsonb NOT NULL,
        flags text[] NOT NULL DEFAULT '{}',
        source jsonb NOT NULL,
        model text,
        extracted_at timestamptz,
        updated_at timestamptz NOT NULL,
        updated_by uuid,
        sealed_at timestamptz,
        fields_sha256 text,
        CONSTRAINT document_fields_sealed_together
            CHECK ((sealed_at IS NULL) = (fields_sha256 IS NULL))
    )""",
        "CREATE INDEX document_fields_ice ON document_fields ((\"values\" ->> 'supplier_ice'))",
        """CREATE TABLE extraction_run (
        id bigserial PRIMARY KEY,
        ledger_id uuid NOT NULL,
        run text NOT NULL CHECK (run IN ('a', 'b')),
        model text,
        raw jsonb NOT NULL,
        at timestamptz NOT NULL DEFAULT now()
    )""",
        "CREATE INDEX extraction_run_ledger ON extraction_run (ledger_id)",
        """CREATE FUNCTION document_fields_frozen() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
            RAISE EXCEPTION 'retention.locked' USING ERRCODE = 'SJ001';
        END IF;
        IF OLD.sealed_at IS NOT NULL THEN
            RAISE EXCEPTION 'retention.locked' USING ERRCODE = 'SJ001';
        END IF;
        RETURN NEW;
    END $$""",
        """CREATE TRIGGER document_fields_frozen BEFORE UPDATE OR DELETE ON document_fields
        FOR EACH ROW EXECUTE FUNCTION document_fields_frozen()""",
        """CREATE TRIGGER document_fields_no_truncate BEFORE TRUNCATE ON document_fields
        FOR EACH STATEMENT EXECUTE FUNCTION document_fields_frozen()""",
        """CREATE FUNCTION extraction_run_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        RAISE EXCEPTION 'index.append_only' USING ERRCODE = 'SJ002';
    END $$""",
        """CREATE TRIGGER extraction_run_append_only BEFORE UPDATE OR DELETE ON extraction_run
        FOR EACH ROW EXECUTE FUNCTION extraction_run_append_only()""",
        """CREATE TRIGGER extraction_run_no_truncate BEFORE TRUNCATE ON extraction_run
        FOR EACH STATEMENT EXECUTE FUNCTION extraction_run_append_only()""",
    ),
}


class RetentionLocked(Exception):  # noqa: N818 - named for the machine code it maps to
    """The row is sealed: the database refused the change (`retention.locked`)."""


@dataclass(frozen=True, kw_only=True)
class FieldsRow:
    """One `document_fields` row, column for column."""

    ledger_id: UUID
    document_type: str | None = None
    values: Mapping[str, str | None] = field(default_factory=dict)
    confidence: Mapping[str, float] = field(default_factory=dict)
    signals: Mapping[str, Mapping[str, bool | None]] = field(default_factory=dict)
    flags: tuple[str, ...] = ()
    source: Mapping[str, str] = field(default_factory=dict)
    model: str | None = None
    extracted_at: datetime | None = None
    updated_at: datetime
    updated_by: UUID | None = None
    sealed_at: datetime | None = None
    fields_sha256: str | None = None


# Schema -------------------------------------------------------------------------------------


async def ensure_schema(conn: AsyncConnection) -> None:
    """Applies the DDL once, in one transaction. Idempotent and safe to run concurrently."""
    if conn.in_transaction():
        await _ensure_schema(conn)
    else:
        async with conn.begin():
            await _ensure_schema(conn)


async def _ensure_schema(conn: AsyncConnection) -> None:
    await conn.exec_driver_sql(f"SELECT pg_advisory_xact_lock({_SCHEMA_LOCK_KEY})")
    await conn.exec_driver_sql("CREATE TABLE IF NOT EXISTS index_schema (version int PRIMARY KEY)")
    applied = (await conn.exec_driver_sql("SELECT max(version) FROM index_schema")).scalar() or 0
    for version in sorted(v for v in _DDL if v > applied):
        for statement in _DDL[version]:
            await conn.exec_driver_sql(statement)
        await conn.execute(text("INSERT INTO index_schema (version) VALUES (:v)"), {"v": version})


# Access -------------------------------------------------------------------------------------

_COLUMNS: Final = (
    'ledger_id, document_type, "values", confidence, signals, flags, source, model, '
    "extracted_at, updated_at, updated_by, sealed_at, fields_sha256"
)


def _locked(exc: DBAPIError) -> bool:
    if getattr(exc.orig, "sqlstate", None) == LOCKED_SQLSTATE:
        return True
    return LOCKED_CODE in str(exc.orig)


def _loads(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _row(mapping: Mapping[Any, Any]) -> FieldsRow:
    data = dict(mapping)
    for name in ("values", "confidence", "signals", "source"):
        data[name] = _loads(data[name])
    data["flags"] = tuple(data["flags"] or ())
    return FieldsRow(**data)


async def read_fields(conn: AsyncConnection, ledger_id: UUID) -> FieldsRow | None:
    result = await conn.execute(
        text(f"SELECT {_COLUMNS} FROM document_fields WHERE ledger_id = :id"),  # noqa: S608
        {"id": ledger_id},
    )
    found = result.mappings().first()
    return None if found is None else _row(found)


async def write_extraction(conn: AsyncConnection, row: FieldsRow) -> None:
    """Upsert of an extraction result. Raises `RetentionLocked` once the row is sealed.

    It replaces the whole unsealed row, user edits included, so it must only run before review
    (ledger state `received`).
    """
    try:
        await conn.execute(
            text(
                """
                INSERT INTO document_fields (ledger_id, document_type, "values", confidence,
                    signals, flags, source, model, extracted_at, updated_at, updated_by)
                VALUES (:ledger_id, :document_type, CAST(:values AS jsonb),
                    CAST(:confidence AS jsonb), CAST(:signals AS jsonb),
                    CAST(:flags AS text[]), CAST(:source AS jsonb), :model, :extracted_at,
                    :updated_at, :updated_by)
                ON CONFLICT (ledger_id) DO UPDATE SET
                    document_type = EXCLUDED.document_type, "values" = EXCLUDED."values",
                    confidence = EXCLUDED.confidence, signals = EXCLUDED.signals,
                    flags = EXCLUDED.flags, source = EXCLUDED.source, model = EXCLUDED.model,
                    extracted_at = EXCLUDED.extracted_at, updated_at = EXCLUDED.updated_at,
                    updated_by = EXCLUDED.updated_by
                """
            ),
            {
                "ledger_id": row.ledger_id,
                "document_type": row.document_type,
                "values": json.dumps(row.values),
                "confidence": json.dumps(row.confidence),
                "signals": json.dumps(row.signals),
                "flags": list(row.flags),
                "source": json.dumps(row.source),
                "model": row.model,
                "extracted_at": row.extracted_at,
                "updated_at": row.updated_at,
                "updated_by": row.updated_by,
            },
        )
    except DBAPIError as exc:
        if _locked(exc):
            raise RetentionLocked from exc
        raise


async def append_run(
    conn: AsyncConnection,
    ledger_id: UUID,
    run: Literal["a", "b"],
    model: str | None,
    raw: Mapping[str, Any],
    at: datetime,
) -> None:
    """Records what one model run said (evidence; the table is append-only)."""
    await conn.execute(
        text(
            "INSERT INTO extraction_run (ledger_id, run, model, raw, at) "
            "VALUES (:ledger_id, :run, :model, CAST(:raw AS jsonb), :at)"
        ),
        {"ledger_id": ledger_id, "run": run, "model": model, "raw": json.dumps(raw), "at": at},
    )


async def update_fields(
    conn: AsyncConnection,
    ledger_id: UUID,
    values: Mapping[str, str | None],
    *,
    source: Literal["extraction", "user"],
    updated_by: UUID | None,
    now: datetime,
) -> FieldsRow:
    """Merges only the given names. A user edit pins its confidence at 1.0."""
    names = list(values)
    confidence = dict.fromkeys(names, 1.0) if source == "user" else {}
    try:
        result = await conn.execute(
            text(
                f"""
                UPDATE document_fields SET
                    "values" = "values" || CAST(:values AS jsonb),
                    source = source || CAST(:source AS jsonb),
                    confidence = confidence || CAST(:confidence AS jsonb),
                    updated_at = :now, updated_by = :by
                WHERE ledger_id = :id RETURNING {_COLUMNS}
                """  # noqa: S608
            ),
            {
                "values": json.dumps(values),
                "source": json.dumps(dict.fromkeys(names, source)),
                "confidence": json.dumps(confidence),
                "now": now,
                "by": updated_by,
                "id": ledger_id,
            },
        )
    except DBAPIError as exc:
        if _locked(exc):
            raise RetentionLocked from exc
        raise
    found = result.mappings().first()
    if found is None:
        raise LookupError("no fields for this document")
    return _row(found)


async def set_scores(
    conn: AsyncConnection,
    ledger_id: UUID,
    confidence: Mapping[str, float],
    flags: tuple[str, ...],
) -> None:
    """Replaces the row's confidence and flags (scoring at routing), nothing else. Raises
    `RetentionLocked` once sealed and LookupError for an unknown row."""
    try:
        result = await conn.execute(
            text(
                "UPDATE document_fields SET confidence = CAST(:confidence AS jsonb), "
                "flags = CAST(:flags AS text[]) WHERE ledger_id = :id RETURNING ledger_id"
            ),
            {"confidence": json.dumps(confidence), "flags": list(flags), "id": ledger_id},
        )
    except DBAPIError as exc:
        if _locked(exc):
            raise RetentionLocked from exc
        raise
    if result.first() is None:
        raise LookupError("no fields for this document")


async def seal_fields(
    conn: AsyncConnection, ledger_id: UUID, *, fields_sha256: str, sealed_at: datetime
) -> FieldsRow:
    """Seals the row once. Same hash again is a no-op; a different one is `RetentionLocked`.

    Race-safe: the row is locked `FOR UPDATE` inside a transaction (one is opened if none is
    active), so of two concurrent seals the loser sees the winner's row. That idempotency (a
    same-hash loser returning the row) needs a transactional connection. On an AUTOCOMMIT one the
    trigger still refuses the loser with `RetentionLocked`, which is safe.
    """
    if conn.in_transaction():
        return await _seal(conn, ledger_id, fields_sha256, sealed_at)
    async with conn.begin():
        return await _seal(conn, ledger_id, fields_sha256, sealed_at)


async def _seal(
    conn: AsyncConnection, ledger_id: UUID, fields_sha256: str, sealed_at: datetime
) -> FieldsRow:
    locked = await conn.execute(
        text(f"SELECT {_COLUMNS} FROM document_fields WHERE ledger_id = :id FOR UPDATE"),  # noqa: S608
        {"id": ledger_id},
    )
    current = locked.mappings().first()
    if current is None:
        raise LookupError("no fields for this document")
    existing = _row(current)
    if existing.sealed_at is not None:
        if existing.fields_sha256 == fields_sha256:
            return existing
        raise RetentionLocked
    try:
        result = await conn.execute(
            text(
                "UPDATE document_fields SET sealed_at = :at, fields_sha256 = :sha "  # noqa: S608
                f"WHERE ledger_id = :id RETURNING {_COLUMNS}"
            ),
            {"at": sealed_at, "sha": fields_sha256, "id": ledger_id},
        )
    except DBAPIError as exc:
        if _locked(exc):
            raise RetentionLocked from exc
        raise
    return _row(result.mappings().one())


async def find_possible_duplicates(
    conn: AsyncConnection, ledger_id: UUID, number: str, supplier_ice: str, total_ttc: str
) -> list[UUID]:
    """Other documents with the same invoice number, supplier ICE and total (canonical strings)."""
    result = await conn.execute(
        text(
            """
            SELECT ledger_id FROM document_fields
            WHERE ledger_id <> :id AND "values" ->> 'number' = :number
              AND "values" ->> 'supplier_ice' = :ice AND "values" ->> 'total_ttc' = :total
            ORDER BY updated_at, ledger_id
            """
        ),
        {"id": ledger_id, "number": number, "ice": supplier_ice, "total": total_ttc},
    )
    return [r[0] for r in result]


async def known_supplier(conn: AsyncConnection, supplier_ice: str) -> str | None:
    """The supplier name on the most recently sealed row with this ICE."""
    result = await conn.execute(
        text(
            """
            SELECT "values" ->> 'supplier_name' FROM document_fields
            WHERE sealed_at IS NOT NULL AND "values" ->> 'supplier_ice' = :ice
              AND "values" ->> 'supplier_name' IS NOT NULL
            ORDER BY sealed_at DESC LIMIT 1
            """
        ),
        {"ice": supplier_ice},
    )
    return result.scalar()


# Engines ------------------------------------------------------------------------------------


def credential_context(workspace_id: UUID, kind: CredentialKind) -> str:
    """The envelope context every tenant credential is bound to."""
    return f"{workspace_id}:{kind}"


def index_url(settings: CoreSettings, name: str, password: str) -> URL:
    """The role and the database share the workspace's `index_db_name`."""
    return URL.create(
        "postgresql+asyncpg",
        username=name,
        password=password,
        host=settings.index_db_host,
        port=settings.index_db_port,
        database=name,
    )


def _make_engine(url: URL) -> AsyncEngine:
    return create_async_engine(url, hide_parameters=True, pool_size=2, pool_pre_ping=True)


async def _ensure_on(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await ensure_schema(conn)


async def apply_schema(settings: CoreSettings, name: str, password: str) -> None:
    """Connects as the index role and applies the schema (provisioning)."""
    engine = _make_engine(index_url(settings, name, password))
    try:
        await _ensure_on(engine)
    finally:
        await engine.dispose()


async def _dispose(engine: AsyncEngine) -> None:
    """Best effort: one engine failing to close must not leak the rest or surprise a caller."""
    try:
        await engine.dispose()
    except Exception:
        log.warning("disposing an index engine failed", exc_info=False)


class IndexEngines:
    """Engines to the workspaces' index databases: an LRU, schema ensured once per engine."""

    def __init__(
        self,
        settings: CoreSettings,
        keys: KeyService,
        sessions_cp: async_sessionmaker[AsyncSession],
        *,
        engine_factory: Callable[[URL], AsyncEngine] = _make_engine,
        ensure: Callable[[AsyncEngine], Awaitable[None]] = _ensure_on,
        max_engines: int = MAX_ENGINES,
    ) -> None:
        self._settings = settings
        self._keys = keys
        self._sessions = sessions_cp
        self._factory = engine_factory
        self._ensure = ensure
        self._max = max_engines
        self._engines: OrderedDict[UUID, AsyncEngine] = OrderedDict()
        self._building: dict[UUID, asyncio.Lock] = {}

    async def _password(self, workspace: Workspace) -> str:
        kind = CredentialKind.index_db_password
        async with self._sessions() as session:
            credential = await session.scalar(
                select(TenantCredential).where(
                    TenantCredential.workspace_id == workspace.id, TenantCredential.kind == kind
                )
            )
        if credential is None:
            raise LookupError("no index database credential")
        return open_envelope(
            Envelope.from_bytes(credential.envelope),
            self._keys,
            credential_context(workspace.id, kind),
        ).decode()

    async def engine(self, workspace: Workspace) -> AsyncEngine:
        """The workspace's engine, built (credential read, schema ensured) on first use.

        Callers must `assert_region` first, and must not keep the engine beyond one unit of
        work: it can be evicted and disposed once 32 other workspaces have been used.
        A cache hit awaits nothing; a build holds only that workspace's lock, never a global one.
        """
        cached = self._engines.get(workspace.id)
        if cached is not None:
            self._engines.move_to_end(workspace.id)
            return cached
        # One lock per workspace, kept for good: popping it would let a waiter and a new caller
        # build under different locks.
        lock = self._building.setdefault(workspace.id, asyncio.Lock())
        evicted: list[AsyncEngine] = []
        async with lock:
            cached = self._engines.get(workspace.id)
            if cached is not None:
                return cached
            password = await self._password(workspace)
            engine = self._factory(index_url(self._settings, workspace.index_db_name, password))
            try:
                await self._ensure(engine)
            except BaseException:
                await _dispose(engine)
                raise
            self._engines[workspace.id] = engine
            while len(self._engines) > self._max:
                evicted.append(self._engines.popitem(last=False)[1])
        for old in evicted:
            await _dispose(old)
        return engine

    async def aclose(self) -> None:
        engines, self._engines = list(self._engines.values()), OrderedDict()
        for engine in engines:
            await _dispose(engine)
