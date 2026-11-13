"""The per-workspace search index: one `search_doc` row per live document (index schema v3).

Full-text search runs in the workspace's index database, never in Paperless. A row holds the
document's ledger state, its retention basis date, its effective field values (a correction
note's value wins over the sealed one, `index.effective_fields`), its tags (denormalised from
`document_tag`, both written by `set_tags`) and its OCR text. The supplier name and the text
are also stored normalised by `sijil_core.text.search_normalise` (`supplier_norm`,
`content_norm`), and the trigram
indexes and `tsv` (supplier and number weight A, text weight C) are built on them, so queries
normalised by the same function match whatever the hamza, tatweel, harakat, accents or digits.

`basis_date` is the period basis search, sums and exports share: the retention basis of the
effective date (a correction note's date wins), its plausibility judged when the row is
written.

Every change a row reflects writes it in the same index transaction as the change (field edit,
seal, correction), or right after its commit when the change lives in the control-plane
database alone (ingest, routing, removal, approved deletion); the daily `sijil.search_resync`
mends what such a step missed. The row is derived, not the document: the index role may
UPDATE it (only DELETE and TRUNCATE are gated), so the resync also compares its values with
their sources (`search_drift`) and rewrites a row changed by hand. A row in a terminal state
(`removed`, `deleting`, `deleted`) is never written again, only purged
(`index.purge_document`): `upsert_search_doc` refuses one and never revives one (but for the
resync, under the ledger's lock, of a live document), and `mark_search_state` is the only way
into those states.
"""

import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from sijil_core import index
from sijil_core.db.models import TERMINAL_STATES
from sijil_core.index import Correction, FieldsRow
from sijil_core.retention import retention_bases, retention_basis
from sijil_core.text import search_normalise

# Beyond this the OCR text is cut: a page of text is a few thousand characters, and a tsvector
# has a 1 MB ceiling that must never fail a routing or a seal.
MAX_CONTENT_CHARS: Final = 100_000
_AMOUNT_LIMIT: Final = Decimal(10) ** 12  # numeric(14, 2)
TAG_INVALID: Final = "tag.invalid"
MAX_TAG_CHARS: Final = 50
MAX_TAGS: Final = 20
_TERMINAL: Final = tuple(sorted(str(s) for s in TERMINAL_STATES))
# The columns a row takes from the ledger, the effective values and `document_tag`: what Ask,
# search and the export read, and what `search_drift` compares.
DERIVED: Final = (
    "state", "ingested_at", "basis_date", "document_type", "supplier_name", "supplier_ice",
    "number", "total_ht", "vat", "total_ttc", "currency", "corrected", "supplier_norm", "tags",
)  # fmt: skip
DRIFT_BATCH: Final = 500
_DRIFT_ROWS: Final = text(
    f"SELECT ledger_id, {', '.join(DERIVED)} FROM search_doc WHERE ledger_id = ANY(:ids)"  # noqa: S608
)


@dataclass(frozen=True, kw_only=True)
class SearchLedgerFacts:
    """What the row takes from the ledger (the control-plane database), passed by the caller
    so that neither service imports the other's rows. `now` dates the retention basis."""

    ledger_id: UUID
    state: str
    ingested_at: datetime
    region: str
    now: datetime


def _amount(value: str | None) -> Decimal | None:
    """A canonical amount string as numeric(14, 2), or None when it is not one."""
    if value is None:
        return None
    try:
        amount = Decimal(value).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None
    return amount if amount.is_finite() and abs(amount) < _AMOUNT_LIMIT else None


_UPSERT: Final = text(
    """
    INSERT INTO search_doc (ledger_id, state, ingested_at, basis_date, document_type,
        supplier_name, supplier_ice, number, total_ht, vat, total_ttc, currency, corrected,
        tags, content, supplier_norm, content_norm, updated_at)
    VALUES (:ledger_id, :state, :ingested_at, :basis_date, :document_type, :supplier_name,
        :supplier_ice, :number, :total_ht, :vat, :total_ttc, :currency,
        CAST(:corrected AS text[]),
        COALESCE((SELECT array_agg(tag ORDER BY tag) FROM document_tag
                  WHERE ledger_id = :ledger_id), '{}'),
        COALESCE(CAST(:content AS text), ''), :supplier_norm,
        COALESCE(CAST(:content_norm AS text), ''), :now)
    ON CONFLICT (ledger_id) DO UPDATE SET
        state = EXCLUDED.state, ingested_at = EXCLUDED.ingested_at,
        basis_date = EXCLUDED.basis_date, document_type = EXCLUDED.document_type,
        supplier_name = EXCLUDED.supplier_name, supplier_ice = EXCLUDED.supplier_ice,
        number = EXCLUDED.number, total_ht = EXCLUDED.total_ht, vat = EXCLUDED.vat,
        total_ttc = EXCLUDED.total_ttc, currency = EXCLUDED.currency,
        corrected = EXCLUDED.corrected, tags = EXCLUDED.tags,
        content = COALESCE(CAST(:content AS text), search_doc.content),
        supplier_norm = EXCLUDED.supplier_norm,
        content_norm = COALESCE(CAST(:content_norm AS text), search_doc.content_norm),
        updated_at = EXCLUDED.updated_at
    WHERE search_doc.state <> ALL (CAST(:terminal AS text[]))
    """
)


def _derived(
    ledger: SearchLedgerFacts, fields: FieldsRow | None, corrections: list[Correction]
) -> tuple[dict[str, Any], str | None]:
    """The row's columns that come from the ledger and the effective values, by name (every
    one of DERIVED but `tags`, which `document_tag` gives), and the effective date."""
    row = fields or FieldsRow(ledger_id=ledger.ledger_id, updated_at=ledger.now)
    effective = index.effective_fields(row, corrections)

    def value(name: str) -> str | None:
        return effective.get(name, (None, False))[0]

    return {
        "state": str(ledger.state),
        "ingested_at": ledger.ingested_at,
        "basis_date": retention_basis(value("date"), ledger.ingested_at, ledger.region, ledger.now),
        "document_type": value("document_type") or row.document_type,
        "supplier_name": value("supplier_name"),
        "supplier_ice": value("supplier_ice"),
        "number": value("number"),
        "total_ht": _amount(value("total_ht")),
        "vat": _amount(value("vat")),
        "total_ttc": _amount(value("total_ttc")),
        "currency": value("currency"),
        "corrected": sorted(name for name, (_, fixed) in effective.items() if fixed),
        "supplier_norm": search_normalise(value("supplier_name") or ""),
    }, value("date")


async def upsert_search_doc(
    conn: AsyncConnection,
    ledger: SearchLedgerFacts,
    fields: FieldsRow | None,
    corrections: list[Correction],
    content: str | None,
    *,
    revive: bool = False,
) -> None:
    """Writes the document's row from its effective values (`corrections` in
    `index.read_corrections` order). `content` None keeps the stored text (an empty one for a
    new row). A terminal `ledger.state` raises ValueError; a row already terminal is left as
    it is, unless `revive`: only for a caller holding the ledger row's lock and reading it
    live (a ledger state never leaves a terminal one, so such a row was changed by hand)."""
    if ledger.state in TERMINAL_STATES:
        raise ValueError(f"search_doc: {ledger.state} is terminal, only the purge removes it")
    kept = None if content is None else content[:MAX_CONTENT_CHARS]
    derived, _ = _derived(ledger, fields, corrections)
    await conn.execute(
        _UPSERT,
        derived
        | {
            "ledger_id": ledger.ledger_id,
            "content": kept,
            "content_norm": None if kept is None else search_normalise(kept),
            "now": ledger.now,
            "terminal": [] if revive else list(_TERMINAL),
        },
    )


async def refresh_search_doc(
    conn: AsyncConnection,
    ledger: SearchLedgerFacts,
    content: str | None = None,
    *,
    revive: bool = False,
) -> None:
    """`upsert_search_doc` from the fields row and correction notes `conn` sees (in the
    caller's transaction, so a change being made there is included)."""
    fields = await index.read_fields(conn, ledger.ledger_id)
    corrections = await index.read_corrections(conn, ledger.ledger_id)
    await upsert_search_doc(conn, ledger, fields, corrections, content, revive=revive)


async def search_drift(
    conn: AsyncConnection, ledgers: Sequence[SearchLedgerFacts]
) -> dict[UUID, list[str]]:
    """For each of these documents that has a row, the DERIVED columns whose stored value is
    not the one its sources give (the ledger facts, `document_fields`, `correction_note`,
    `document_tag`), sorted by name; a document in step is absent. The text is not compared
    (its source is Paperless). `basis_date` is in step when it is any day's judgement since
    the arrival (`retention.retention_bases`): a row keeps the one made when it was written.

    Every writer changes a row and its sources in one index transaction, so the caller reads
    them in one snapshot (a REPEATABLE READ transaction); a few queries per DRIFT_BATCH
    documents."""
    drift: dict[UUID, list[str]] = {}
    for start in range(0, len(ledgers), DRIFT_BATCH):
        batch = {facts.ledger_id: facts for facts in ledgers[start : start + DRIFT_BATCH]}
        ids = list(batch)
        stored = await conn.execute(_DRIFT_ROWS, {"ids": ids})
        fields = await index.read_fields_many(conn, ids)
        notes = await index.read_corrections_many(conn, ids)
        tags = await conn.execute(
            text(
                "SELECT ledger_id, array_agg(tag ORDER BY tag) FROM document_tag"
                " WHERE ledger_id = ANY(:ids) GROUP BY ledger_id"
            ),
            {"ids": ids},
        )
        tagged: dict[UUID, list[str]] = {ledger_id: list(found) for ledger_id, found in tags}
        for have in stored.mappings():
            facts = batch[have["ledger_id"]]
            want, dated = _derived(
                facts, fields.get(facts.ledger_id), notes.get(facts.ledger_id, [])
            )
            # Tags are a set: `set_tags` orders them by code point, the database by collation.
            want["tags"] = sorted(tagged.get(facts.ledger_id, []))
            columns = [
                name
                for name in DERIVED
                if (sorted(have[name] or []) if name == "tags" else have[name]) != want[name]
            ]
            if "basis_date" in columns and have["basis_date"] in retention_bases(
                dated, facts.ingested_at, facts.region, facts.now
            ):
                columns.remove("basis_date")
            if columns:
                drift[facts.ledger_id] = sorted(columns)
    return drift


async def mark_search_state(conn: AsyncConnection, ledger_id: UUID, state: str) -> None:
    """Moves an existing row to a terminal state (removal, approved deletion) until the purge
    deletes it. A live `state` raises ValueError: those come from `upsert_search_doc`."""
    if state not in TERMINAL_STATES:
        raise ValueError(f"search_doc: {state} is not terminal")
    await conn.execute(
        text("UPDATE search_doc SET state = :state, updated_at = now() WHERE ledger_id = :id"),
        {"state": str(state), "id": ledger_id},
    )


def normalise_tag(tag: str) -> str:
    """A tag as stored and as matched: NFKC, trimmed, casefolded."""
    return unicodedata.normalize("NFKC", tag).strip().casefold()


def clean_tags(raw: Sequence[str]) -> list[str]:
    """The tags as stored: NFKC, trimmed, casefolded, deduplicated and sorted. Raises ValueError
    (`tag.invalid`) for one outside 1-50 characters or with a control character, or for more
    than 20."""
    tags = set()
    for tag in raw:
        clean = normalise_tag(tag)
        if not 1 <= len(clean) <= MAX_TAG_CHARS:
            raise ValueError(TAG_INVALID)
        if any(unicodedata.category(c).startswith("C") for c in clean):
            raise ValueError(TAG_INVALID)
        tags.add(clean)
    if len(tags) > MAX_TAGS:
        raise ValueError(TAG_INVALID)
    return sorted(tags)


async def set_tags(
    conn: AsyncConnection, ledger_id: UUID, tags: Sequence[str], *, by: UUID, now: datetime
) -> None:
    """Replaces the document's tags (`clean_tags` output) in `document_tag` and in its search
    row, in the caller's transaction. A tag is an annotation: removing one is a plain DELETE
    (index schema v4), and nothing else of the document changes. A row in a terminal state is
    left as it is (its tags go with the purge).

    Writers of one document are serialised by a transaction-level advisory lock taken first:
    the ledger's FOR SHARE lets two of them through, and the second's DELETE would otherwise not
    see the first's rows, leaving `document_tag` the union of both sets."""
    params = {"id": ledger_id, "tags": sorted(tags), "by": by, "now": now}
    await conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:id))"), {"id": str(ledger_id)})
    await conn.execute(
        text(
            "DELETE FROM document_tag WHERE ledger_id = :id AND tag <> ALL (CAST(:tags AS text[]))"
        ),
        params,
    )
    await conn.execute(
        text(
            """
            INSERT INTO document_tag (ledger_id, tag, created_by, created_at)
            SELECT :id, tag, :by, :now FROM unnest(CAST(:tags AS text[])) AS tag
            ON CONFLICT (ledger_id, tag) DO NOTHING
            """
        ),
        params,
    )
    await conn.execute(
        text(
            """
            UPDATE search_doc SET tags = CAST(:tags AS text[]), updated_at = :now
            WHERE ledger_id = :id AND state <> ALL (CAST(:terminal AS text[]))
            """
        ),
        params | {"terminal": list(_TERMINAL)},
    )


@dataclass(frozen=True)
class IndexedState:
    """What the resync compares with the ledger: the row's state, and whether it has no text."""

    state: str
    empty: bool


async def search_states(conn: AsyncConnection) -> dict[UUID, IndexedState]:
    """Every row's state and whether its text is empty, by ledger id."""
    result = await conn.execute(text("SELECT ledger_id, state, content = '' FROM search_doc"))
    return {ledger_id: IndexedState(state, empty) for ledger_id, state, empty in result}
