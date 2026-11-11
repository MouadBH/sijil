"""The per-workspace index database: extracted field values, kept out of the shared database.

Document content (field values) lives only in `index_<ref>`, one database per workspace, owned
by a role of the same name. The control plane and the ledger never hold values, only hashes.

A row of `document_fields` is frozen once sealed, at the database level (trigger
`document_fields_frozen`): the error message carries the machine code `retention.locked`, which
this module raises as `RetentionLocked` so callers can answer 409. `extraction_run` is evidence
of what each model run said and is append-only, like `correction_note` (v2). The search index
(v3, `sijil_core.search_index`: `search_doc`, `document_tag`) may change but never be deleted
from. The one DELETE the triggers allow is `purge_document`'s, but for removing a tag (v4: a tag
is an annotation). Saved views (v4, `saved_view`) are the workspace's, not a document's. A
document's chunks and their embeddings (v5, `doc_chunk`) are replaced as a whole by
`replace_chunks` (gate `sijil.reembed`) and deleted by the purge; never updated. Embeddings,
like every value here, stay in this database.

The DDL is plain SQL, versioned by `SCHEMA_VERSION` and recorded in `index_schema`.
"""

import asyncio
import json
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Literal
from uuid import UUID, uuid4

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
from sijil_core.fields import fields_digest
from sijil_core.settings import CoreSettings

log = logging.getLogger(__name__)

SCHEMA_VERSION: Final = 5
# bge-m3's dimensions (SIJIL_EMBEDDING_MODEL): `doc_chunk.embedding` is vector(1024).
EMBEDDING_DIM: Final = 1024
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
    # v2: correction notes, and the one DELETE allowed: `purge_document`'s, gated per ledger id.
    2: (
        """CREATE TABLE correction_note (
        id uuid PRIMARY KEY,
        ledger_id uuid NOT NULL,
        field text NOT NULL,
        value text,
        reason text NOT NULL,
        created_by uuid NOT NULL,
        created_at timestamptz NOT NULL
    )""",
        "CREATE INDEX correction_note_ledger ON correction_note (ledger_id)",
        """CREATE OR REPLACE FUNCTION document_fields_frozen() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'retention.locked' USING ERRCODE = 'SJ001';
        END IF;
        IF TG_OP = 'DELETE' THEN
            IF current_setting('sijil.purge', true) IS DISTINCT FROM OLD.ledger_id::text
               OR (OLD.sealed_at IS NOT NULL
                   AND current_setting('sijil.purge_sealed', true)
                       IS DISTINCT FROM OLD.ledger_id::text) THEN
                RAISE EXCEPTION 'retention.locked' USING ERRCODE = 'SJ001';
            END IF;
            RETURN OLD;
        END IF;
        IF OLD.sealed_at IS NOT NULL THEN
            RAISE EXCEPTION 'retention.locked' USING ERRCODE = 'SJ001';
        END IF;
        RETURN NEW;
    END $$""",
        """CREATE FUNCTION index_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF TG_OP = 'DELETE' THEN
            IF current_setting('sijil.purge', true) IS NOT DISTINCT FROM OLD.ledger_id::text THEN
                RETURN OLD;
            END IF;
        END IF;
        RAISE EXCEPTION 'index.append_only' USING ERRCODE = 'SJ002';
    END $$""",
        "DROP TRIGGER extraction_run_append_only ON extraction_run",
        "DROP TRIGGER extraction_run_no_truncate ON extraction_run",
        "DROP FUNCTION extraction_run_append_only()",
        *(
            sql
            for table in ("extraction_run", "correction_note")
            for sql in (
                f"""CREATE TRIGGER {table}_append_only BEFORE UPDATE OR DELETE ON {table}
        FOR EACH ROW EXECUTE FUNCTION index_append_only()""",
                f"""CREATE TRIGGER {table}_no_truncate BEFORE TRUNCATE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION index_append_only()""",
            )
        ),
        # One statement, so one transaction even on an AUTOCOMMIT connection; the gate is
        # opened for this ledger id only and closed again before it returns.
        """CREATE FUNCTION purge_document(target uuid, sealed boolean) RETURNS void
    LANGUAGE plpgsql AS $$
    BEGIN
        PERFORM set_config('sijil.purge', target::text, true);
        IF sealed THEN
            PERFORM set_config('sijil.purge_sealed', target::text, true);
        END IF;
        DELETE FROM correction_note WHERE ledger_id = target;
        DELETE FROM extraction_run WHERE ledger_id = target;
        DELETE FROM document_fields WHERE ledger_id = target;
        PERFORM set_config('sijil.purge', '', true);
        PERFORM set_config('sijil.purge_sealed', '', true);
    END $$""",
    ),
    # v3: the search index (sijil_core.search_index). pg_trgm and unaccent are trusted, so the
    # index role (the database's owner) creates them here; vector is not, and is already in
    # every index database (infra/compose/postgres/init.sh, `template_index`), so IF NOT EXISTS
    # skips it without asking for privileges.
    3: (
        "CREATE EXTENSION IF NOT EXISTS pg_trgm",
        "CREATE EXTENSION IF NOT EXISTS unaccent",
        "CREATE EXTENSION IF NOT EXISTS vector",
        # Mirrors sijil_core.text.search_normalise (the tests check they agree), for the invoice
        # number in `tsv`; the supplier name and the text are normalised in Python into
        # `supplier_norm` and `content_norm`. NFKC, bidi controls dropped, horizontal space
        # collapsed, lower case, NFKD with the Latin and Arabic combining marks dropped, tatweel
        # dropped, alef/yeh/teh marbuta and digits folded. `ß` is the one casefold that `lower`
        # does not do. `unaccent` is not used: this folding replaces it.
        r"""CREATE FUNCTION search_normalise(s text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
        SELECT translate(
            regexp_replace(
                normalize(
                    replace(lower(regexp_replace(regexp_replace(normalize(s, NFKC),
                        '[\u200e\u200f\u061c\u202a-\u202e\u2066-\u2069]', '', 'g'),
                        '[ \t\u00a0\u202f\u2009]+', ' ', 'g')), U&'\00DF', 'ss'),
                    NFKD),
                '[\u0300-\u036f\u0610-\u061a\u064b-\u065f\u0670\u06d6-\u06dc\u06df-\u06e4'
                '\u06e7\u06e8\u06ea-\u06ed]', '', 'g'),
            'أإآٱىة٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹' || U&'\0640',
            'اااايه01234567890123456789')
    $$""",
        """CREATE TABLE search_doc (
        ledger_id uuid PRIMARY KEY,
        state text NOT NULL,
        ingested_at timestamptz NOT NULL,
        basis_date date NOT NULL,
        document_type text,
        supplier_name text,
        supplier_ice text,
        number text,
        total_ht numeric(14, 2),
        vat numeric(14, 2),
        total_ttc numeric(14, 2),
        currency text,
        corrected text[] NOT NULL DEFAULT '{}',
        tags text[] NOT NULL DEFAULT '{}',
        content text NOT NULL DEFAULT '',
        supplier_norm text NOT NULL DEFAULT '',
        content_norm text NOT NULL DEFAULT '',
        tsv tsvector GENERATED ALWAYS AS (
            setweight(to_tsvector('simple', supplier_norm), 'A')
            || setweight(to_tsvector('simple', search_normalise(coalesce(number, ''))), 'A')
            || setweight(to_tsvector('simple', content_norm), 'C')
        ) STORED,
        updated_at timestamptz NOT NULL
    )""",
        "CREATE INDEX search_doc_tsv ON search_doc USING gin (tsv)",
        "CREATE INDEX search_doc_supplier_trgm ON search_doc "
        "USING gin (supplier_norm gin_trgm_ops)",
        "CREATE INDEX search_doc_content_trgm ON search_doc USING gin (content_norm gin_trgm_ops)",
        "CREATE INDEX search_doc_basis ON search_doc (basis_date, ledger_id)",
        "CREATE INDEX search_doc_ttc ON search_doc (total_ttc, ledger_id)",
        "CREATE INDEX search_doc_state ON search_doc (state)",
        "CREATE INDEX search_doc_type ON search_doc (document_type)",
        "CREATE INDEX search_doc_ice ON search_doc (supplier_ice)",
        "CREATE INDEX search_doc_tags ON search_doc USING gin (tags)",
        """CREATE TABLE document_tag (
        ledger_id uuid NOT NULL,
        tag text NOT NULL CHECK (char_length(tag) BETWEEN 1 AND 50),
        created_by uuid NOT NULL,
        created_at timestamptz NOT NULL,
        PRIMARY KEY (ledger_id, tag)
    )""",
        # Rows may change, but leave only through `purge_document`'s gate.
        """CREATE FUNCTION index_purge_only() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF TG_OP = 'DELETE'
           AND current_setting('sijil.purge', true) IS NOT DISTINCT FROM OLD.ledger_id::text THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION 'retention.locked' USING ERRCODE = 'SJ001';
    END $$""",
        *(
            sql
            for table in ("search_doc", "document_tag")
            for sql in (
                f"""CREATE TRIGGER {table}_purge_only BEFORE DELETE ON {table}
        FOR EACH ROW EXECUTE FUNCTION index_purge_only()""",
                f"""CREATE TRIGGER {table}_no_truncate BEFORE TRUNCATE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION index_purge_only()""",
            )
        ),
        """CREATE OR REPLACE FUNCTION purge_document(target uuid, sealed boolean) RETURNS void
    LANGUAGE plpgsql AS $$
    BEGIN
        PERFORM set_config('sijil.purge', target::text, true);
        IF sealed THEN
            PERFORM set_config('sijil.purge_sealed', target::text, true);
        END IF;
        DELETE FROM document_tag WHERE ledger_id = target;
        DELETE FROM search_doc WHERE ledger_id = target;
        DELETE FROM correction_note WHERE ledger_id = target;
        DELETE FROM extraction_run WHERE ledger_id = target;
        DELETE FROM document_fields WHERE ledger_id = target;
        PERFORM set_config('sijil.purge', '', true);
        PERFORM set_config('sijil.purge_sealed', '', true);
    END $$""",
    ),
    # v4: saved views (`sijil_core.search_query`) and tag removal. A tag is an annotation, not
    # document data: removing one is a plain DELETE (`search_index.set_tags`), so the purge-only
    # trigger on `document_tag` goes; TRUNCATE is still refused, and `search_doc` keeps its
    # gate. The invoice number gets an index for the exact match of a query token. The
    # snippet's highlight (`ts_headline`, over the text as written) parses with
    # `sijil_headline`, which drops Latin accents and case, so a query typed without them
    # still marks its hits; matching itself uses the normalised columns.
    4: (
        "DROP TRIGGER document_tag_purge_only ON document_tag",
        "CREATE TEXT SEARCH CONFIGURATION sijil_headline (COPY = simple)",
        """ALTER TEXT SEARCH CONFIGURATION sijil_headline ALTER MAPPING FOR asciiword, asciihword,
        hword_asciipart, word, hword, hword_part, numword, numhword, hword_numpart
        WITH unaccent, simple""",
        "CREATE INDEX search_doc_number ON search_doc (number)",
        """CREATE TABLE saved_view (
        id uuid PRIMARY KEY,
        name text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 80),
        filters jsonb NOT NULL,
        sort text NOT NULL,
        created_by uuid NOT NULL,
        created_at timestamptz NOT NULL,
        updated_at timestamptz NOT NULL
    )""",
    ),
    # v5: a document's chunks and their embeddings (`replace_chunks`, sijil_worker.embed), for
    # Ask's retrieval by meaning. Never updated: a re-embedding replaces the whole set under the
    # gate `sijil.reembed` (one ledger id), and the purge deletes them under `sijil.purge`.
    # Every chunk carries `source_sha256`, the digest of the document's whole chunk input, so
    # the daily resync can tell chunks made from another input (`chunks_source`).
    5: (
        f"""CREATE TABLE doc_chunk (
        ledger_id uuid NOT NULL,
        n int NOT NULL CHECK (n >= 0),
        text text NOT NULL,
        embedding vector({EMBEDDING_DIM}) NOT NULL,
        model text NOT NULL,
        source_sha256 text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (ledger_id, n)
    )""",
        "CREATE INDEX doc_chunk_embedding ON doc_chunk USING hnsw (embedding vector_cosine_ops)",
        """CREATE FUNCTION doc_chunk_gated() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF TG_OP = 'DELETE'
           AND (current_setting('sijil.purge', true) IS NOT DISTINCT FROM OLD.ledger_id::text
                OR current_setting('sijil.reembed', true)
                   IS NOT DISTINCT FROM OLD.ledger_id::text) THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION 'retention.locked' USING ERRCODE = 'SJ001';
    END $$""",
        """CREATE TRIGGER doc_chunk_gated BEFORE UPDATE OR DELETE ON doc_chunk
        FOR EACH ROW EXECUTE FUNCTION doc_chunk_gated()""",
        """CREATE TRIGGER doc_chunk_no_truncate BEFORE TRUNCATE ON doc_chunk
        FOR EACH STATEMENT EXECUTE FUNCTION doc_chunk_gated()""",
        # Writes only for a live search row, which it holds FOR SHARE until the transaction
        # ends: a removal's mark or a purge then waits, and a document already marked terminal
        # or purged never gets chunks again. True when the chunks were replaced.
        """CREATE FUNCTION reembed_document(target uuid, model_name text, source text,
        texts text[], embeddings text[]) RETURNS boolean
    LANGUAGE plpgsql AS $$
    BEGIN
        IF cardinality(texts) IS DISTINCT FROM cardinality(embeddings) THEN
            RAISE EXCEPTION 'index.chunks_mismatch' USING ERRCODE = 'SJ003';
        END IF;
        PERFORM 1 FROM search_doc
            WHERE ledger_id = target AND state NOT IN ('removed', 'deleting', 'deleted')
            FOR SHARE;
        IF NOT FOUND THEN
            RETURN false;
        END IF;
        PERFORM set_config('sijil.reembed', target::text, true);
        DELETE FROM doc_chunk WHERE ledger_id = target;
        PERFORM set_config('sijil.reembed', '', true);
        INSERT INTO doc_chunk (ledger_id, n, text, embedding, model, source_sha256)
            SELECT target, i - 1, texts[i], CAST(embeddings[i] AS vector), model_name, source
            FROM generate_subscripts(texts, 1) AS i;
        RETURN true;
    END $$""",
        """CREATE OR REPLACE FUNCTION purge_document(target uuid, sealed boolean) RETURNS void
    LANGUAGE plpgsql AS $$
    BEGIN
        PERFORM set_config('sijil.purge', target::text, true);
        IF sealed THEN
            PERFORM set_config('sijil.purge_sealed', target::text, true);
        END IF;
        DELETE FROM doc_chunk WHERE ledger_id = target;
        DELETE FROM document_tag WHERE ledger_id = target;
        DELETE FROM search_doc WHERE ledger_id = target;
        DELETE FROM correction_note WHERE ledger_id = target;
        DELETE FROM extraction_run WHERE ledger_id = target;
        DELETE FROM document_fields WHERE ledger_id = target;
        PERFORM set_config('sijil.purge', '', true);
        PERFORM set_config('sijil.purge_sealed', '', true);
    END $$""",
    ),
}


class RetentionLocked(Exception):  # noqa: N818 - named for the machine code it maps to
    """The row is sealed: the database refused the change (`retention.locked`)."""


class FieldsChanged(Exception):  # noqa: N818 - named for the machine code it maps to
    """The row's values no longer hash to the digest the caller computed: they changed since
    the caller read them (`seal.fields_changed`). Nothing was sealed."""


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
    """Merges only the given names. A user edit pins its confidence at 1.0. A `document_type`
    among them also moves the `document_type` column, so the two never disagree."""
    names = list(values)
    confidence = dict.fromkeys(names, 1.0) if source == "user" else {}
    try:
        result = await conn.execute(
            text(
                f"""
                UPDATE document_fields SET
                    document_type = CASE
                        WHEN CAST(:values AS jsonb) -> 'document_type' IS NOT NULL
                        THEN CAST(:values AS jsonb) ->> 'document_type'
                        ELSE document_type END,
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
    """Seals the row once with `fields_sha256`, which must be the digest of the values it holds:
    the digest is recomputed from the row locked FOR UPDATE, and a mismatch (the values
    changed since the caller hashed them) raises `FieldsChanged`, sealing nothing. A sealed row
    with the same hash is returned as it is (a retry); with another hash, `RetentionLocked`.

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
    if fields_digest(existing.values) != fields_sha256:
        raise FieldsChanged
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


@dataclass(frozen=True, kw_only=True)
class Correction:
    """One correction note, column for column. `value` None means "the field is empty"."""

    id: UUID
    ledger_id: UUID
    field: str
    value: str | None
    reason: str
    created_by: UUID
    created_at: datetime


async def add_correction(
    conn: AsyncConnection,
    ledger_id: UUID,
    field_name: str,
    value: str | None,
    reason: str,
    *,
    created_by: UUID,
    now: datetime,
) -> Correction:
    """Appends a note (the table is append-only); the sealed `document_fields` row is not
    touched."""
    note = Correction(
        id=uuid4(),
        ledger_id=ledger_id,
        field=field_name,
        value=value,
        reason=reason,
        created_by=created_by,
        created_at=now,
    )
    await conn.execute(
        text(
            "INSERT INTO correction_note (id, ledger_id, field, value, reason, created_by, "
            "created_at) VALUES (:id, :ledger_id, :field, :value, :reason, :created_by, "
            ":created_at)"
        ),
        {
            "id": note.id,
            "ledger_id": ledger_id,
            "field": field_name,
            "value": value,
            "reason": reason,
            "created_by": created_by,
            "created_at": now,
        },
    )
    return note


async def read_corrections(conn: AsyncConnection, ledger_id: UUID) -> list[Correction]:
    """The document's notes, newest first."""
    result = await conn.execute(
        text(
            "SELECT id, ledger_id, field, value, reason, created_by, created_at "
            "FROM correction_note WHERE ledger_id = :id ORDER BY created_at DESC, id DESC"
        ),
        {"id": ledger_id},
    )
    return [Correction(**dict(m)) for m in result.mappings()]


def effective_fields(
    row: FieldsRow, corrections: list[Correction]
) -> dict[str, tuple[str | None, bool]]:
    """Each field's value and whether a note corrects it: the latest note per field wins.
    `corrections` is in `read_corrections` order (newest first)."""
    effective: dict[str, tuple[str | None, bool]] = {
        name: (value, False) for name, value in row.values.items()
    }
    seen: set[str] = set()
    for note in corrections:
        if note.field not in seen:
            seen.add(note.field)
            effective[note.field] = (note.value, True)
    return effective


async def purge_document(conn: AsyncConnection, ledger_id: UUID, *, sealed: bool = False) -> None:
    """Deletes the document's `document_fields`, `extraction_run`, `correction_note`,
    `search_doc`, `document_tag` and `doc_chunk` rows, all or nothing, through the schema's
    `purge_document()` function. Idempotent.

    The index triggers refuse every DELETE but one whose ledger id the GUC `sijil.purge` names,
    which the function sets with `set_config(..., true)` (`SET LOCAL`) and clears again before
    it returns. A sealed `document_fields` row also needs `sijil.purge_sealed`, set only with
    `sealed=True`; without it the purge raises `RetentionLocked` and deletes nothing. It is one
    statement, so this holds on an AUTOCOMMIT connection too.

    The caller owns the ledger state: removal purges an unsealed document, an approved
    deletion (ledger `deleting`) a sealed one with `sealed=True`. The services connect as the
    index role, which owns the tables and could drop the triggers anyway (backlog: a separate
    owner), so this GUC gate is no weaker than the triggers were: it stops a stray DELETE, and a
    plain DELETE or TRUNCATE is refused as before.
    """
    try:
        await conn.execute(
            text("SELECT purge_document(CAST(:id AS uuid), CAST(:sealed AS boolean))"),
            {"id": ledger_id, "sealed": sealed},
        )
    except DBAPIError as exc:
        if _locked(exc):
            raise RetentionLocked from exc
        raise


def _vector_literal(vector: Sequence[float]) -> str:
    if len(vector) != EMBEDDING_DIM:
        raise ValueError(f"an embedding has {len(vector)} dimensions, not {EMBEDDING_DIM}")
    return "[" + ",".join(repr(float(x)) for x in vector) + "]"


async def replace_chunks(
    conn: AsyncConnection,
    ledger_id: UUID,
    model: str,
    chunks: Sequence[tuple[str, Sequence[float]]],
    *,
    source_sha256: str,
) -> bool:
    """Replaces the document's chunks (text and embedding, numbered from 0 in order) with
    `chunks`, each row carrying `source_sha256` (the digest of the chunk input they were made
    from), all or nothing, through the schema's `reembed_document()`: it opens the gate
    `sijil.reembed` for this ledger id only, deletes, closes it and inserts. One statement, so
    this holds on an AUTOCOMMIT connection too.

    Writes nothing and returns False unless the document has a live `search_doc` row (none, or
    one marked removed, deleting or deleted): a removed or purged document never gets chunks
    back. An embedding of other than EMBEDDING_DIM dimensions raises ValueError."""
    literals = [_vector_literal(vector) for _, vector in chunks]
    result = await conn.execute(
        text(
            "SELECT reembed_document(CAST(:id AS uuid), :model, :source, "
            "CAST(:texts AS text[]), CAST(:embeddings AS text[]))"
        ),
        {
            "id": ledger_id,
            "model": model,
            "source": source_sha256,
            "texts": [t for t, _ in chunks],
            "embeddings": literals,
        },
    )
    return bool(result.scalar())


async def chunks_source(conn: AsyncConnection, ledger_id: UUID) -> str | None:
    """The `source_sha256` the document's chunks were made from; None when it has none."""
    result = await conn.execute(
        text("SELECT source_sha256 FROM doc_chunk WHERE ledger_id = :id ORDER BY n LIMIT 1"),
        {"id": ledger_id},
    )
    return result.scalar()


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
