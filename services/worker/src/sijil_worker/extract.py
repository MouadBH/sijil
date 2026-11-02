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
recorded. Both runs answering garbage records the row with the flag `extraction_failed` and
audits `document.extraction_failed`; so does `record_extraction_failed`, which the task calls
when its retries run out.

A document that already has a `document_fields` row is not extracted again: a redelivered
task or the requeue sweep changes nothing. Scoring and routing (`filed` / `needs_review`) come
after this step (Task 5); until then the ledger row stays `received`.
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


async def _has_row(index_engines: IndexEngines, workspace: Workspace, ledger_id: UUID) -> bool:
    async with (await index_engines.engine(workspace)).connect() as conn:
        return await index_db.read_fields(conn, ledger_id) is not None


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
) -> bool:
    """Writes the runs and the row unless the document already has one. True if written."""
    await conn.execute(text(_WRITE_LOCK_SQL), {"id": str(row.ledger_id)})
    if await index_db.read_fields(conn, row.ledger_id) is not None:
        return False
    for name, run in runs:
        await index_db.append_run(
            conn, row.ledger_id, name, row.model, run.raw, row.extracted_at or row.updated_at
        )
    await index_db.write_extraction(conn, row)
    return True


async def _audit(
    sessions: async_sessionmaker[AsyncSession],
    workspace_id: UUID,
    ledger_id: UUID,
    action: str,
    data: dict[str, str | int | bool | None],
    now: datetime,
) -> None:
    async with sessions() as session:
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
        await session.commit()


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
    if job is None or await _has_row(index, job.workspace, ledger_id):
        return
    model = settings.extraction_model
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
        flags = (EXTRACTION_FAILED,)
    signals = _signals(values_a, values_b, kept, paperless_type)
    row = FieldsRow(
        ledger_id=ledger_id,
        document_type=kept["document_type"],
        values=kept,
        confidence={},
        signals=signals,
        flags=flags,
        source=dict.fromkeys(FIELD_NAMES, "extraction"),
        model=model,
        extracted_at=now,
        updated_at=now,
    )
    async with (await index.engine(job.workspace)).begin() as conn:
        written = await _write_once(conn, row, runs)
    if not written:
        return  # a concurrent run wrote the row first: its audit stands
    if EXTRACTION_FAILED in flags:
        log.warning("extraction of document %s failed: %s", ledger_id, BAD_OUTPUT)
        await _audit(
            sessions,
            job.workspace.id,
            ledger_id,
            "document.extraction_failed",
            {"code": BAD_OUTPUT},
            now,
        )
        return
    agree = sum(1 for s in signals.values() if s["runs_agree"] is True)
    await _audit(
        sessions,
        job.workspace.id,
        ledger_id,
        "document.extracted",
        {"model": model, "runs_agree_count": agree},
        now,
    )


async def record_extraction_failed(
    ledger_id: UUID,
    code: str,
    *,
    sessions: async_sessionmaker[AsyncSession],
    index: IndexEngines,
    settings: WorkerSettings,
    now: datetime,
) -> None:
    """Retries ran out (or a final error): flag `extraction_failed` and audit it, once.

    A row that an extraction already wrote is left alone; so is another region's document or
    one that is no longer `received`.
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
    try:
        assert_region(region, settings)
    except RegionMismatch:
        return
    if ledger.state is not DocumentState.received:
        return
    row = FieldsRow(
        ledger_id=ledger_id,
        values=dict.fromkeys(FIELD_NAMES),
        flags=(EXTRACTION_FAILED,),
        source={},
        model=settings.extraction_model,
        extracted_at=now,
        updated_at=now,
    )
    async with (await index.engine(workspace)).begin() as conn:
        written = await _write_once(conn, row, [])
    if written:
        await _audit(
            sessions, workspace.id, ledger_id, "document.extraction_failed", {"code": code}, now
        )


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
