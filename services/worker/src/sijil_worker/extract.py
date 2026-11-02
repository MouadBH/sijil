"""Extraction of a consumed document's fields by the in-region model (REC-2, REC-3).

`extract_document` runs only for a ledger row that is `received` with `ingest_state =
consumed`, and only in this worker's region. It:
1. reads the text Paperless extracted (never the PDF's own text layer, ruling P4), normalises
   it and keeps the first 12,000 characters; empty text is flagged `no_text`;
2. asks Ollama twice (run A: PROMPT_A, seed 1; run B: PROMPT_B, seed 2) for the fields, as a
   JSON object constrained by `extraction_schema()`;
3. parses each run's values into canonical strings (`sijil_core.fields`), keeping run A's
   value, or run B's where run A has none;
4. records per-field signals (`runs_agree`, and `paperless_agrees` for the type, from
   Paperless's own regex match against our document types);
5. writes both runs (`extraction_run`) and the result (`document_fields`) to the workspace's
   index database in one transaction, then audits `document.extracted` (field values never
   enter the audit log, ruling P5).

Transient failures (Ollama or Paperless down) raise for the caller to retry, with nothing
recorded. Both runs answering garbage records the row with the flags `extraction_failed` and
the code, and audits `document.extraction_failed`; so does `record_extraction_failed`, which
the task calls when its retries run out.

Each run first obtains the row (the stored one, or a fresh extraction: the model never runs
twice for a document), then ensures its audit event exists, rebuilt from the row if an earlier
run lost it, under a lock on the ledger row so it is written once. Scoring and routing
(`filed` / `needs_review`) follow that (Task 5); until then the ledger row stays `received`.
"""

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import and_, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core import index as index_db
from sijil_core.crypto import Envelope, KeyService, open_envelope
from sijil_core.db.base import utcnow
from sijil_core.db.models import (
    ActorType,
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    DocumentState,
    IngestState,
    Organisation,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.fields import DOCUMENT_TYPES, FIELD_NAMES, parse_field
from sijil_core.index import FieldsRow, IndexEngines
from sijil_core.ingest import Enqueue
from sijil_core.ollama import OllamaBadOutput, OllamaClient
from sijil_core.paperless import PaperlessClient
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.text import normalise
from sijil_worker.ingest import MissingCredential, WorkspaceNotActive
from sijil_worker.settings import WorkerSettings
from sijil_worker.taxonomy import ensure_taxonomy

log = logging.getLogger(__name__)

MAX_CONTENT_CHARS: Final = 12_000
SEED_A: Final = 1
SEED_B: Final = 2
NO_TEXT: Final = "no_text"
EXTRACTION_FAILED: Final = "extraction_failed"
BAD_OUTPUT: Final = "extraction.ai_bad_output"
_RESULT_ACTIONS: Final = ("document.extracted", "document.extraction_failed")
REQUEUE_AFTER: Final = timedelta(minutes=15)
REQUEUE_BATCH: Final = 500
# Serialises the write of one document's row across concurrent runs (index database).
_WRITE_LOCK_SQL: Final = "SELECT pg_advisory_xact_lock(hashtextextended(CAST(:id AS text), 7))"

DocumentType = Literal[
    "invoice", "receipt", "contract", "bank_statement", "payslip", "tax_notice", "other"
]


class RawExtraction(BaseModel):
    """What one model run answered: every field as printed, or None."""

    model_config = ConfigDict(extra="ignore", strict=True)

    document_type: DocumentType | None = None
    supplier_name: str | None = None
    supplier_ice: str | None = None
    date: str | None = None
    number: str | None = None
    total_ht: str | None = None
    vat: str | None = None
    vat_rate: str | None = None
    total_ttc: str | None = None
    currency: str | None = None


def extraction_schema() -> dict[str, Any]:
    """The JSON schema Ollama constrains its answer to (`format`)."""
    properties: dict[str, Any] = {name: {"type": ["string", "null"]} for name in FIELD_NAMES}
    properties["document_type"] = {"type": ["string", "null"], "enum": [*DOCUMENT_TYPES, None]}
    return {"type": "object", "properties": properties, "required": list(FIELD_NAMES)}


PROMPT_A: Final[str] = """\
You read the text of one business document from Morocco, written in French or Arabic, and \
fill in a JSON object with the fields below.

Copy every value exactly as printed in the text: do not translate it, reformat it, compute \
it or guess it. When a value is not printed in the document, answer null.

Fields:
- document_type: one of invoice, receipt, contract, bank_statement, payslip, tax_notice, \
other.
- supplier_name: the name of the company that issued the document (the seller). Never the \
client: the client is the party introduced by "Client" or "الزبون".
- supplier_ice: the ICE of the company that issued the document, never the client's ICE. \
The ICE is labelled "ICE" or "التعريف الموحد للمقاولة" and has 15 digits.
- date: the date of the document, as printed.
- number: the document number, as printed (after "Facture N°" or "فاتورة رقم", for example).
- total_ht: the total before tax, labelled "Total HT" or "المجموع دون احتساب الرسوم".
- vat: the VAT amount, labelled "TVA" or "الضريبة على القيمة المضافة".
- vat_rate: the VAT rate in percent, as printed next to the VAT.
- total_ttc: the total including tax, labelled "Total TTC" or "المجموع مع احتساب الرسوم".
- currency: the currency code. "DH", "DHS" and "درهم" mean MAD.

The text was extracted from the page and its lines can be out of order: a label and its \
amount may appear in a different order from the page, or on separate lines. Pair each amount \
with its label by meaning, not by position.
"""

PROMPT_B: Final[str] = """\
Extract structured data from the document text the user sends. The document is Moroccan, in \
French or in Arabic. Answer with one JSON object holding exactly these keys.

Rules:
1. Each value is copied as printed in the document, character for character. Do not convert, \
translate or calculate anything.
2. A value the document does not show is null. Never invent one.
3. supplier_name and supplier_ice describe the company that issued the document. The \
customer (after "Client" or "الزبون") is never the supplier, even when its name or ICE comes \
first.
4. Text extraction can change the order of the page: labels and amounts may appear in a \
different order than on the page. Match amounts to labels by meaning.

Keys:
- document_type: invoice, receipt, contract, bank_statement, payslip, tax_notice or other.
- supplier_name: the issuing company's name.
- supplier_ice: the issuing company's ICE ("التعريف الموحد للمقاولة"), 15 digits.
- date: the document date.
- number: the document number.
- total_ht: amount without tax ("Total HT", "المجموع دون احتساب الرسوم").
- vat: tax amount ("TVA", "الضريبة على القيمة المضافة").
- vat_rate: tax rate in percent.
- total_ttc: amount with tax ("Total TTC", "المجموع مع احتساب الرسوم").
- currency: ISO code; "DH" or "درهم" is MAD.
"""


def _user_message(content: str) -> str:
    return f"Document text:\n<<<\n{content}\n>>>"


@dataclass(frozen=True)
class _Job:
    ledger_id: UUID
    workspace: Workspace
    doc_id: int
    paperless_url: str
    token: str


@dataclass(frozen=True)
class _Run:
    """One model run: the raw answer kept as evidence, and its canonical values (None when the
    answer was unusable)."""

    raw: dict[str, Any]
    values: dict[str, str | None] | None


def _canonical(raw: RawExtraction) -> dict[str, str | None]:
    return {name: parse_field(name, getattr(raw, name)) for name in FIELD_NAMES}


async def _ask(ollama: OllamaClient, model: str, prompt: str, content: str, seed: int) -> _Run:
    """One run. OllamaUnavailable and OllamaError propagate; bad output becomes a `_Run`."""
    try:
        answer = await ollama.chat_json(
            model, prompt, _user_message(content), extraction_schema(), seed
        )
    except OllamaBadOutput as exc:
        raw: dict[str, Any] = {"error": "ai.bad_output"}
        if exc.raw is not None:
            raw["content"] = exc.raw
        return _Run(raw=raw, values=None)
    try:
        parsed = RawExtraction.model_validate(answer)
    except ValidationError:
        return _Run(raw={"error": "ai.bad_output", "answer": answer}, values=None)
    return _Run(raw=answer, values=_canonical(parsed))


def _service_token(credential: TenantCredential | None, keys: KeyService, ws: Workspace) -> str:
    if credential is None:
        raise MissingCredential("no service token")
    return open_envelope(
        Envelope.from_bytes(credential.envelope),
        keys,
        f"{ws.id}:{CredentialKind.service_token}",
    ).decode()


async def _load(
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    settings: WorkerSettings,
    ledger_id: UUID,
    workspace_id: UUID | None,
) -> _Job | None:
    """The job, or None when there is nothing to do here (wrong state, or another region's)."""
    async with sessions() as session:
        found = (
            await session.execute(
                select(DocumentLedger, Workspace, Organisation.region)
                .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(DocumentLedger.id == ledger_id)
            )
        ).first()
        if found is None:
            raise LookupError("document not found")
        ledger, workspace, region = found
        if workspace_id is not None and workspace.id != workspace_id:
            raise LookupError("document not found")
        # Before anything leaves the database: this worker only serves its own region.
        try:
            assert_region(region, settings)
        except RegionMismatch:
            log.warning(
                "extraction of document %s skipped: region %s is not served here",
                ledger_id,
                region,
            )
            return None
        if (
            ledger.state is not DocumentState.received
            or ledger.ingest_state is not IngestState.consumed
            or ledger.paperless_doc_id is None
        ):
            return None
        if workspace.state is not WorkspaceState.active or not workspace.paperless_url:
            raise WorkspaceNotActive
        credential = await session.scalar(
            select(TenantCredential).where(
                TenantCredential.workspace_id == workspace.id,
                TenantCredential.kind == CredentialKind.service_token,
            )
        )
        return _Job(
            ledger_id=ledger_id,
            workspace=workspace,
            doc_id=ledger.paperless_doc_id,
            paperless_url=workspace.paperless_url,
            token=_service_token(credential, keys, workspace),
        )


async def _stored_row(
    index_engines: IndexEngines, workspace: Workspace, ledger_id: UUID
) -> FieldsRow | None:
    async with (await index_engines.engine(workspace)).connect() as conn:
        return await index_db.read_fields(conn, ledger_id)


def _signals(
    a: Mapping[str, str | None] | None,
    b: Mapping[str, str | None] | None,
    kept: Mapping[str, str | None],
    paperless_type: str | None,
) -> dict[str, dict[str, bool | None]]:
    signals: dict[str, dict[str, bool | None]] = {}
    for name in FIELD_NAMES:
        va = None if a is None else a.get(name)
        vb = None if b is None else b.get(name)
        signals[name] = {"runs_agree": va is not None and va == vb}
    signals["document_type"]["paperless_agrees"] = (
        None if paperless_type is None else paperless_type == kept.get("document_type")
    )
    return signals


async def _paperless_type(client: PaperlessClient, doc_id: int) -> str | None:
    """The type Paperless's matching suggests, as one of our names, or None."""
    names = {type_id: name for name, type_id in (await ensure_taxonomy(client)).items()}
    suggested = (await client.document_suggestions(doc_id)).get("document_types") or []
    return names.get(suggested[0]) if suggested else None


RunName = Literal["a", "b"]


async def _write_once(
    conn: AsyncConnection, row: FieldsRow, runs: list[tuple[RunName, _Run]]
) -> FieldsRow:
    """Writes the runs and the row unless the document already has one; returns the stored
    row (ours, or the one a concurrent run wrote first)."""
    await conn.execute(text(_WRITE_LOCK_SQL), {"id": str(row.ledger_id)})
    existing = await index_db.read_fields(conn, row.ledger_id)
    if existing is not None:
        return existing
    for name, run in runs:
        await index_db.append_run(
            conn, row.ledger_id, name, row.model, run.raw, row.extracted_at or row.updated_at
        )
    await index_db.write_extraction(conn, row)
    return row


async def _append_event(
    session: AsyncSession,
    *,
    workspace_id: UUID,
    ledger_id: UUID,
    action: str,
    data: dict[str, str | int | bool | None],
    now: datetime,
) -> None:
    await audit.append(
        session,
        workspace_id=workspace_id,
        action=action,
        actor_type=ActorType.system,
        actor_id=None,
        target_type="document",
        target_id=str(ledger_id),
        data=data,
        now=now,
    )


def _event_for(row: FieldsRow) -> tuple[str, dict[str, str | int | bool | None]]:
    """The audit event a stored row stands for, rebuilt from the row alone (no values)."""
    if EXTRACTION_FAILED in row.flags:
        codes = [f for f in row.flags if f.startswith("extraction.")]
        return "document.extraction_failed", {"code": codes[0] if codes else "extraction.error"}
    agree = sum(1 for s in row.signals.values() if s.get("runs_agree") is True)
    return "document.extracted", {"model": row.model, "runs_agree_count": agree}


async def _ensure_audited(
    sessions: async_sessionmaker[AsyncSession],
    workspace_id: UUID,
    ledger_id: UUID,
    row: FieldsRow,
    now: datetime,
) -> None:
    """Appends the row's `document.extracted` / `document.extraction_failed` event unless one
    exists. The ledger row is locked FOR UPDATE, so concurrent runs write it once; a run that
    lost its audit (the commit failed after the index write) gets it back on the next run."""
    async with sessions() as session:
        await session.execute(
            select(DocumentLedger.id).where(DocumentLedger.id == ledger_id).with_for_update()
        )
        done = await session.scalar(
            select(AuditEvent.id)
            .where(
                AuditEvent.workspace_id == workspace_id,
                AuditEvent.target_type == "document",
                AuditEvent.target_id == str(ledger_id),
                AuditEvent.action.in_(_RESULT_ACTIONS),
            )
            .limit(1)
        )
        if done is not None:
            return
        action, data = _event_for(row)
        if action == "document.extraction_failed":
            log.warning("extraction of document %s failed: %s", ledger_id, data["code"])
        await _append_event(
            session,
            workspace_id=workspace_id,
            ledger_id=ledger_id,
            action=action,
            data=data,
            now=now,
        )
        await session.commit()


async def _extract(
    job: _Job,
    *,
    index: IndexEngines,
    paperless: Callable[..., PaperlessClient],
    ollama: OllamaClient,
    model: str,
    now: datetime,
) -> FieldsRow:
    """Reads the content, runs the model twice and writes the row; returns the stored row."""
    async with paperless(job.paperless_url, token=job.token) as client:
        content = normalise(await client.document_content(job.doc_id))[:MAX_CONTENT_CHARS]
        runs: list[tuple[RunName, _Run]] = []
        paperless_type: str | None = None
        if content.strip():
            # Paperless first: a failure there is retried before any model time is spent.
            paperless_type = await _paperless_type(client, job.doc_id)
            runs.append(("a", await _ask(ollama, model, PROMPT_A, content, SEED_A)))
            runs.append(("b", await _ask(ollama, model, PROMPT_B, content, SEED_B)))

    values_a = runs[0][1].values if runs else None
    values_b = runs[1][1].values if runs else None
    kept: dict[str, str | None] = {
        name: (values_a or {}).get(name) or (values_b or {}).get(name) for name in FIELD_NAMES
    }
    flags: tuple[str, ...] = ()
    if not runs:
        flags = (NO_TEXT,)
    elif values_a is None and values_b is None:
        flags = (EXTRACTION_FAILED, BAD_OUTPUT)
    row = FieldsRow(
        ledger_id=job.ledger_id,
        document_type=kept["document_type"],
        values=kept,
        confidence={},
        signals=_signals(values_a, values_b, kept, paperless_type),
        flags=flags,
        source=dict.fromkeys(FIELD_NAMES, "extraction"),
        model=model,
        extracted_at=now,
        updated_at=now,
    )
    async with (await index.engine(job.workspace)).begin() as conn:
        return await _write_once(conn, row, runs)


async def extract_document(
    ledger_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    index: IndexEngines,
    paperless: Callable[..., PaperlessClient],
    ollama: OllamaClient,
    settings: WorkerSettings,
    now: datetime,
    workspace_id: UUID | None = None,
) -> None:
    """See the module docstring. Raises OllamaUnavailable / PaperlessUnavailable (transient)
    for the caller to retry; any other exception is final for the caller."""
    job = await _load(sessions, keys, settings, ledger_id, workspace_id)
    if job is None:
        return
    # 1. Obtain the row: the stored one (a redelivery, or a run whose audit was lost), or a
    #    fresh extraction.
    row = await _stored_row(index, job.workspace, ledger_id)
    if row is None:
        row = await _extract(
            job,
            index=index,
            paperless=paperless,
            ollama=ollama,
            model=settings.extraction_model,
            now=now,
        )
    # 2. Make sure it is audited, exactly once.
    await _ensure_audited(sessions, job.workspace.id, ledger_id, row, now)
    # 3. Task 5: score and route `row` here. The ledger is still `received` (checked by
    #    `_load`), whether the row was written now or by an earlier run.


async def record_extraction_failed(
    ledger_id: UUID,
    code: str,
    *,
    sessions: async_sessionmaker[AsyncSession],
    index: IndexEngines,
    settings: WorkerSettings,
    now: datetime,
    workspace_id: UUID | None = None,
) -> None:
    """Retries ran out (or a final error): flag `extraction_failed` (with `code`) and audit it,
    once.

    A row an extraction already wrote is kept (and its own event ensured instead). Another
    region's document, one of another workspace than `workspace_id`, or one no longer
    `received` is left alone.
    """
    async with sessions() as session:
        found = (
            await session.execute(
                select(DocumentLedger, Workspace, Organisation.region)
                .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(DocumentLedger.id == ledger_id)
            )
        ).first()
    if found is None:
        return
    ledger, workspace, region = found
    if workspace_id is not None and workspace.id != workspace_id:
        return
    try:
        assert_region(region, settings)
    except RegionMismatch:
        return
    if ledger.state is not DocumentState.received:
        return
    failed = FieldsRow(
        ledger_id=ledger_id,
        values=dict.fromkeys(FIELD_NAMES),
        flags=(EXTRACTION_FAILED, code),
        source={},
        model=settings.extraction_model,
        extracted_at=now,
        updated_at=now,
    )
    async with (await index.engine(workspace)).begin() as conn:
        row = await _write_once(conn, failed, [])
    await _ensure_audited(sessions, workspace.id, ledger_id, row, now)
    # Task 5: route `row` here too (an `extraction_failed` row goes to review).


async def requeue_unextracted(
    sessions: async_sessionmaker[AsyncSession],
    enqueue: Enqueue,
    *,
    region: str,
    now: datetime | None = None,
    older_than: timedelta = REQUEUE_AFTER,
    limit: int = REQUEUE_BATCH,
) -> int:
    """Send `extract_document` again for this region's rows still `received` and `consumed`
    that arrived over `older_than` ago (a lost message or a failed enqueue). Safe to repeat:
    an extracted document is not extracted again. Returns how many were sent.
    """
    cutoff = (now or utcnow()) - older_than
    async with sessions() as session:
        rows = (
            await session.execute(
                select(DocumentLedger.id, DocumentLedger.workspace_id)
                .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(
                    and_(
                        Organisation.region == region,
                        DocumentLedger.state == DocumentState.received,
                        DocumentLedger.ingest_state == IngestState.consumed,
                        DocumentLedger.ingested_at < cutoff,
                    )
                )
                .order_by(DocumentLedger.ingested_at)
                .limit(limit)
            )
        ).all()
    sent = 0
    for ledger_id, ws_id in rows:
        try:
            await enqueue(ledger_id, ws_id)
        except Exception:
            log.exception("requeue of extraction for document %s failed", ledger_id)
            continue
        sent += 1
    return sent
