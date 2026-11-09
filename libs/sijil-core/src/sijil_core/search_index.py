"""The per-workspace search index: one `search_doc` row per live document (index schema v3).

Full-text search runs in the workspace's index database, never in Paperless. A row holds the
document's ledger state, its retention basis date, its effective field values (a correction
note's value wins over the sealed one, `index.effective_fields`), its tags (denormalised from
`document_tag`) and its OCR text; `tsv` is generated from the `search_normalise`d supplier
name and number (weight A) and text (weight C), so queries normalised by
`sijil_core.text.search_normalise` match whatever the hamza, tatweel, harakat, accents or digits.

Every change a row reflects writes it in the same index transaction as the change (field edit,
seal, correction), or right after its commit when the change lives in the control-plane
database alone (ingest, routing, removal, approved deletion); the daily `sijil.search_resync`
mends what such a step missed. A row in a terminal state (`removed`, `deleting`, `deleted`)
is never written again, only purged (`index.purge_document`): `upsert_search_doc` refuses one
and never revives one, and `mark_search_state` is the only way into those states.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Final
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from sijil_core import index
from sijil_core.db.models import TERMINAL_STATES
from sijil_core.index import Correction, FieldsRow
from sijil_core.retention import retention_basis

# Beyond this the OCR text is cut: a page of text is a few thousand characters, and a tsvector
# has a 1 MB ceiling that must never fail a routing or a seal.
MAX_CONTENT_CHARS: Final = 100_000
_AMOUNT_LIMIT: Final = Decimal(10) ** 12  # numeric(14, 2)
_TERMINAL: Final = tuple(sorted(str(s) for s in TERMINAL_STATES))


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
        tags, content, updated_at)
    VALUES (:ledger_id, :state, :ingested_at, :basis_date, :document_type, :supplier_name,
        :supplier_ice, :number, :total_ht, :vat, :total_ttc, :currency,
        CAST(:corrected AS text[]),
        COALESCE((SELECT array_agg(tag ORDER BY tag) FROM document_tag
                  WHERE ledger_id = :ledger_id), '{}'),
        COALESCE(CAST(:content AS text), ''), :now)
    ON CONFLICT (ledger_id) DO UPDATE SET
        state = EXCLUDED.state, ingested_at = EXCLUDED.ingested_at,
        basis_date = EXCLUDED.basis_date, document_type = EXCLUDED.document_type,
        supplier_name = EXCLUDED.supplier_name, supplier_ice = EXCLUDED.supplier_ice,
        number = EXCLUDED.number, total_ht = EXCLUDED.total_ht, vat = EXCLUDED.vat,
        total_ttc = EXCLUDED.total_ttc, currency = EXCLUDED.currency,
        corrected = EXCLUDED.corrected, tags = EXCLUDED.tags,
        content = COALESCE(CAST(:content AS text), search_doc.content),
        updated_at = EXCLUDED.updated_at
    WHERE search_doc.state <> ALL (CAST(:terminal AS text[]))
    """
)


async def upsert_search_doc(
    conn: AsyncConnection,
    ledger: SearchLedgerFacts,
    fields: FieldsRow | None,
    corrections: list[Correction],
    content: str | None,
) -> None:
    """Writes the document's row from its effective values (`corrections` in
    `index.read_corrections` order). `content` None keeps the stored text (an empty one for a
    new row). A terminal `ledger.state` raises ValueError; a row already terminal is left as
    it is."""
    if ledger.state in TERMINAL_STATES:
        raise ValueError(f"search_doc: {ledger.state} is terminal, only the purge removes it")
    row = fields or FieldsRow(ledger_id=ledger.ledger_id, updated_at=ledger.now)
    effective = index.effective_fields(row, corrections)

    def value(name: str) -> str | None:
        return effective.get(name, (None, False))[0]

    await conn.execute(
        _UPSERT,
        {
            "ledger_id": ledger.ledger_id,
            "state": str(ledger.state),
            "ingested_at": ledger.ingested_at,
            "basis_date": retention_basis(
                value("date"), ledger.ingested_at, ledger.region, ledger.now
            ),
            "document_type": value("document_type") or row.document_type,
            "supplier_name": value("supplier_name"),
            "supplier_ice": value("supplier_ice"),
            "number": value("number"),
            "total_ht": _amount(value("total_ht")),
            "vat": _amount(value("vat")),
            "total_ttc": _amount(value("total_ttc")),
            "currency": value("currency"),
            "corrected": sorted(name for name, (_, fixed) in effective.items() if fixed),
            "content": None if content is None else content[:MAX_CONTENT_CHARS],
            "now": ledger.now,
            "terminal": list(_TERMINAL),
        },
    )


async def refresh_search_doc(
    conn: AsyncConnection, ledger: SearchLedgerFacts, content: str | None = None
) -> None:
    """`upsert_search_doc` from the fields row and correction notes `conn` sees (in the
    caller's transaction, so a change being made there is included)."""
    fields = await index.read_fields(conn, ledger.ledger_id)
    corrections = await index.read_corrections(conn, ledger.ledger_id)
    await upsert_search_doc(conn, ledger, fields, corrections, content)


async def mark_search_state(conn: AsyncConnection, ledger_id: UUID, state: str) -> None:
    """Moves an existing row to a terminal state (removal, approved deletion) until the purge
    deletes it. A live `state` raises ValueError: those come from `upsert_search_doc`."""
    if state not in TERMINAL_STATES:
        raise ValueError(f"search_doc: {state} is not terminal")
    await conn.execute(
        text("UPDATE search_doc SET state = :state, updated_at = now() WHERE ledger_id = :id"),
        {"state": str(state), "id": ledger_id},
    )


async def search_states(conn: AsyncConnection) -> dict[UUID, str]:
    """Every row's state, by ledger id (the resync compares them with the ledger)."""
    result = await conn.execute(text("SELECT ledger_id, state FROM search_doc"))
    return {ledger_id: state for ledger_id, state in result}
