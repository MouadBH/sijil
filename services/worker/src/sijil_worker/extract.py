"""Extraction of a consumed document's fields by the in-region model, then its filing or review
(REC-2, REC-3, design §5.3, §5.4).

`extract_document` runs only for a ledger row that is `received` with `ingest_state =
consumed` (or `unsupported`, which is only routed: see below), only in an `active` workspace
(a hibernated or closing one is a no-op), and only in this worker's region. It:
1. reads the text Paperless extracted (never the PDF's own text layer, ruling P4), normalises
   it and keeps the first 12,000 characters; empty text is flagged `no_text`;
2. asks Ollama twice (run A: PROMPT_A, seed 1; run B: PROMPT_B, seed 2) for the fields, as a
   JSON object constrained by `extraction_schema()`;
3. parses each run's values into canonical strings (`sijil_core.fields`), keeping run A's
   value, or run B's where run A has none;
4. records per-field signals (`sijil_core.confidence.compute_signals`: the runs agreeing,
   Paperless's own regex match for the type, the value in the text, the supplier already
   known for the ICE, the date, VAT and totals checks) and the scores they give;
5. writes both runs (`extraction_run`) and the result (`document_fields`) to the workspace's
   index database in one transaction, then audits `document.extracted` (field values never
   enter the audit log, ruling P5);
6. routes it (`_route`), in one transaction holding the ledger row FOR UPDATE: flag
   `possible_duplicate` when another row has the same number, ICE and TTC; `filed` iff every
   required field scores at least 0.90 and no flag blocks, else `needs_review`; raise
   `retention_until` to the fiscal date (the object lock first, then the ledger; never lower,
   ruling P1); audit `document.filed` / `document.needs_review`. Only a row still `received`
   moves, so it is routed once;
7. after the commit, best effort, names the document in its own Paperless (title and type).

An `unsupported` row has no Paperless document: it gets an empty fields row flagged
`ingest.unsupported_format` and goes straight to review, with no extraction.

Transient failures (Ollama or Paperless down) raise for the caller to retry, with nothing
recorded. Both runs answering garbage records the row with the flags `extraction_failed` and
the code, and audits `document.extraction_failed`; so does `record_extraction_failed`, which
the task calls when its retries run out.

Each run first obtains the row (the stored one, or a fresh extraction: the model never runs
twice for a document), then ensures its audit event exists, rebuilt from the row if an earlier
run lost it, under a lock on the ledger row so it is written once, then routes it. A run that
failed after the extraction (the store down, say) leaves the ledger `received`, and the next
run routes the stored row.
"""

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Final, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import and_, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core import index as index_db
from sijil_core.confidence import (
    Decision,
    compute_signals,
    kept,
    min_confidence_pct,
    route,
    scores,
)
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
from sijil_core.retention import fiscal_retain_until, region_local_date, retention_basis
from sijil_core.storage import ObjectRef
from sijil_core.text import normalise
from sijil_worker.ingest import UNSUPPORTED, MissingCredential, WorkspaceNotActive
from sijil_worker.pending import PendingExtractions
from sijil_worker.settings import ExtractionSettings
from sijil_worker.taxonomy import ensure_taxonomy

log = logging.getLogger(__name__)

MAX_CONTENT_CHARS: Final = 12_000
SEED_A: Final = 1
SEED_B: Final = 2
NO_TEXT: Final = "no_text"
EXTRACTION_FAILED: Final = "extraction_failed"
BAD_OUTPUT: Final = "extraction.ai_bad_output"
POSSIBLE_DUPLICATE: Final = "possible_duplicate"
RETENTION_EXTEND_FAILED: Final = "retention.extend_failed"
_RESULT_ACTIONS: Final = ("document.extracted", "document.extraction_failed")
REQUEUE_AFTER: Final = timedelta(minutes=15)
REQUEUE_BATCH: Final = 50  # sends per sweep run
REQUEUE_SCAN: Final = 500  # rows looked at per sweep run (in-flight ones are skipped)
# Serialises the write of one document's row across concurrent runs (index database).
_WRITE_LOCK_SQL: Final = "SELECT pg_advisory_xact_lock(hashtextextended(CAST(:id AS text), 7))"


class RetentionStore(Protocol):
    """What routing needs of the object store (`ObjectStore` satisfies it)."""

    async def extend_retention(self, ref: ObjectRef, retain_until: date) -> None: ...


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
    """A `consumed` row to extract (Paperless reachable), or an `unsupported` one to route
    (`doc_id`, `paperless_url` and `token` None)."""

    ledger_id: UUID
    workspace: Workspace
    region: str
    doc_id: int | None
    paperless_url: str | None
    token: str | None

    @property
    def unsupported(self) -> bool:
        return self.doc_id is None


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
    settings: ExtractionSettings,
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
        if ledger.state is not DocumentState.received:
            return None
        # Only an active workspace extracts: a hibernated or closing one is left alone (and
        # the sweep skips it), so it never holds the one model in flight.
        if workspace.state is not WorkspaceState.active:
            log.info(
                "extraction of document %s skipped: workspace is %s", ledger_id, workspace.state
            )
            return None
        if ledger.ingest_state is IngestState.unsupported:
            return _Job(ledger_id, workspace, region, None, None, None)
        if ledger.ingest_state is not IngestState.consumed or ledger.paperless_doc_id is None:
            return None
        if not workspace.paperless_url:
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
            region=region,
            doc_id=ledger.paperless_doc_id,
            paperless_url=workspace.paperless_url,
            token=_service_token(credential, keys, workspace),
        )


async def _stored_row(
    index_engines: IndexEngines, workspace: Workspace, ledger_id: UUID
) -> FieldsRow | None:
    async with (await index_engines.engine(workspace)).connect() as conn:
        return await index_db.read_fields(conn, ledger_id)


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
    return "document.extracted", {
        "model": row.model,
        "runs_agree_count": agree,
        # Recomputed from the stored signals, so a rebuilt event says the same.
        "min_confidence_pct": min_confidence_pct(scores(row.signals), row.document_type),
    }


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


async def _compute(
    job: _Job,
    *,
    index: IndexEngines,
    paperless: Callable[..., PaperlessClient],
    ollama: OllamaClient,
    model: str,
    now: datetime,
) -> tuple[FieldsRow, list[tuple[RunName, _Run]]]:
    """Reads the content and runs the model twice; returns the row and the runs to record.
    Writes nothing (the index is only read, for the known supplier): see `_write_once`."""
    if job.doc_id is None or job.paperless_url is None:
        raise ValueError("no Paperless document to extract")
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
    values = kept(values_a, values_b)
    flags: tuple[str, ...] = ()
    if not runs:
        flags = (NO_TEXT,)
    elif values_a is None and values_b is None:
        flags = (EXTRACTION_FAILED, BAD_OUTPUT)
    async with (await index.engine(job.workspace)).connect() as conn:
        ice = values["supplier_ice"]
        known = None if ice is None else await index_db.known_supplier(conn, ice)
    signals = compute_signals(
        values_a,
        values_b,
        content,
        paperless_type=paperless_type,
        known_supplier=known,
        today=region_local_date(now, job.region),
    )
    row = FieldsRow(
        ledger_id=job.ledger_id,
        document_type=values["document_type"],
        values=values,
        confidence=scores(signals),
        signals=signals,
        flags=flags,
        source=dict.fromkeys(FIELD_NAMES, "extraction"),
        model=model,
        extracted_at=now,
        updated_at=now,
    )
    return row, runs


async def _extract(
    job: _Job,
    *,
    index: IndexEngines,
    paperless: Callable[..., PaperlessClient],
    ollama: OllamaClient,
    model: str,
    now: datetime,
) -> FieldsRow:
    """Computes the row (`_compute`) and writes it; returns the stored row."""
    row, runs = await _compute(
        job, index=index, paperless=paperless, ollama=ollama, model=model, now=now
    )
    async with (await index.engine(job.workspace)).begin() as conn:
        return await _write_once(conn, row, runs)


async def _unsupported_row(index: IndexEngines, job: _Job, now: datetime) -> FieldsRow:
    """The empty row of a document Paperless could not consume: no extraction, no runs."""
    row = FieldsRow(
        ledger_id=job.ledger_id,
        values=dict.fromkeys(FIELD_NAMES),
        flags=(UNSUPPORTED,),
        updated_at=now,
    )
    async with (await index.engine(job.workspace)).begin() as conn:
        return await _write_once(conn, row, [])


async def _flags_and_scores(
    index: IndexEngines, workspace: Workspace, row: FieldsRow
) -> tuple[dict[str, float], tuple[str, ...]]:
    """Scores the row from its signals and looks for a duplicate (CAP-4); stores both."""
    confidence = scores(row.signals)
    flags = row.flags
    number, ice, ttc = (row.values.get(n) for n in ("number", "supplier_ice", "total_ttc"))
    async with (await index.engine(workspace)).begin() as conn:
        if number and ice and ttc and POSSIBLE_DUPLICATE not in flags:
            if await index_db.find_possible_duplicates(conn, row.ledger_id, number, ice, ttc):
                flags = (*flags, POSSIBLE_DUPLICATE)
        if confidence != dict(row.confidence) or flags != row.flags:
            await index_db.set_scores(conn, row.ledger_id, confidence, flags)
    return confidence, flags


async def _route(
    sessions: async_sessionmaker[AsyncSession],
    index: IndexEngines,
    store: RetentionStore,
    workspace: Workspace,
    row: FieldsRow,
    now: datetime,
    *,
    final: bool = False,
) -> Decision | None:
    """Files the document or sends it to review, once: under the ledger row's lock (taken
    before the audit chain's), only a row still `received` moves. None when nothing moved.

    The object lock is extended before the ledger: if the ledger commit then fails, the lock
    is only longer (safe, monotonic) and the retry computes the same date. If the extension
    fails, the error propagates for a retry; on the `final` path (nothing will retry) the
    document goes to review instead, flagged `retention.extend_failed`, its retention as the
    ledger holds it.
    """
    async with sessions() as session:
        found = (
            await session.execute(
                select(DocumentLedger, Organisation.region, Organisation.country)
                .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(DocumentLedger.id == row.ledger_id)
                .with_for_update(of=DocumentLedger)
            )
        ).first()
        if found is None:
            return None
        ledger, region, country = found
        if ledger.state is not DocumentState.received:
            return None
        confidence, flags = await _flags_and_scores(index, workspace, row)
        decision = route(confidence, flags, row.document_type)
        # Only a plausible date may raise retention (the same basis as the seal): an object
        # lock is never shortened, so a misread year must not lock the original for longer.
        basis = retention_basis(row.values.get("date"), ledger.ingested_at, region, now)
        target = max(ledger.retention_until, fiscal_retain_until(basis, country))
        if target > ledger.retention_until:
            try:
                await store.extend_retention(
                    ObjectRef(ledger.object_key, ledger.object_version_id), target
                )
            except Exception as exc:
                if not final:
                    raise
                log.warning(
                    "retention extension of document %s failed: %s",
                    row.ledger_id,
                    type(exc).__name__,
                )
                flags = (*flags, RETENTION_EXTEND_FAILED)
                decision = "needs_review"
                async with (await index.engine(workspace)).begin() as conn:
                    await index_db.set_scores(conn, row.ledger_id, confidence, flags)
            else:
                ledger.retention_until = target
        ledger.state = DocumentState(decision)
        await _append_event(
            session,
            workspace_id=workspace.id,
            ledger_id=row.ledger_id,
            action=f"document.{decision}",
            data={
                "min_confidence_pct": min_confidence_pct(confidence, row.document_type),
                "flags": ",".join(flags),
            },
            now=now,
        )
        await session.commit()
    return decision


def _title(values: Mapping[str, str | None]) -> str:
    parts = (values.get("date"), values.get("supplier_name"), values.get("number"))
    return " ".join(p for p in parts if p)


async def _name_in_paperless(
    job: _Job, paperless: Callable[..., PaperlessClient], row: FieldsRow
) -> None:
    """Best effort, after routing: the title and type in the tenant's own Paperless (the
    content stays in the workspace). A failure is logged by its class only and changes
    nothing; it is never retried, so a crash between the routing commit and this PATCH leaves
    the title unset."""
    title = _title(row.values)
    if job.doc_id is None or job.paperless_url is None or not title or not row.document_type:
        return
    try:
        async with paperless(job.paperless_url, token=job.token) as client:
            type_id = (await ensure_taxonomy(client))[row.document_type]
            await client.set_title_and_type(job.doc_id, title, type_id)
    except Exception as exc:
        log.warning(
            "paperless title update of document %s failed: %s", job.ledger_id, type(exc).__name__
        )


async def extract_document(
    ledger_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    index: IndexEngines,
    paperless: Callable[..., PaperlessClient],
    ollama: OllamaClient,
    store: RetentionStore,
    settings: ExtractionSettings,
    now: datetime,
    workspace_id: UUID | None = None,
) -> None:
    """See the module docstring. Raises OllamaUnavailable / PaperlessUnavailable (transient)
    for the caller to retry; any other exception is final for the caller."""
    job = await _load(sessions, keys, settings, ledger_id, workspace_id)
    if job is None:
        return
    if job.unsupported:
        row = await _unsupported_row(index, job, now)
    else:
        # 1. Obtain the row: the stored one (a redelivery, or a run whose audit or routing
        #    was lost), or a fresh extraction.
        stored = await _stored_row(index, job.workspace, ledger_id)
        row = stored or await _extract(
            job,
            index=index,
            paperless=paperless,
            ollama=ollama,
            model=settings.extraction_model,
            now=now,
        )
        # 2. Make sure it is audited, exactly once.
        await _ensure_audited(sessions, job.workspace.id, ledger_id, row, now)
    # 3. Score and route it, once.
    if await _route(sessions, index, store, job.workspace, row, now) is not None:
        await _name_in_paperless(job, paperless, row)


async def record_extraction_failed(
    ledger_id: UUID,
    code: str,
    *,
    sessions: async_sessionmaker[AsyncSession],
    index: IndexEngines,
    store: RetentionStore,
    settings: ExtractionSettings,
    now: datetime,
    workspace_id: UUID | None = None,
) -> None:
    """Retries ran out (or a final error): flag `extraction_failed` (with `code`), audit it and
    route the document (to review), once.

    A row an extraction already wrote is kept (its own event ensured, and the row routed,
    instead). Another region's document, one of another workspace than `workspace_id`, one no
    longer `received`, or one not `consumed` (an `unsupported` one is left to the sweep, which
    routes it) is left alone.
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
    if (
        ledger.state is not DocumentState.received
        or ledger.ingest_state is not IngestState.consumed
    ):
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
    await _route(sessions, index, store, workspace, row, now, final=True)


async def requeue_unextracted(
    sessions: async_sessionmaker[AsyncSession],
    enqueue: Enqueue,
    *,
    region: str,
    now: datetime | None = None,
    older_than: timedelta = REQUEUE_AFTER,
    limit: int = REQUEUE_BATCH,
    pending: PendingExtractions | None = None,
    scan: int = REQUEUE_SCAN,
) -> int:
    """Send `extract_document` again for this region's rows still `received` and `consumed` or
    `unsupported`, in an `active` workspace (hibernated and closing ones do not extract), that
    arrived over `older_than` ago (a lost message, a failed enqueue, or a
    run that failed before routing), oldest first. Safe to repeat: an extracted document is
    not extracted again, and a routed one is no longer `received`.

    With `pending`, a document whose extraction is still in flight is skipped, and at most
    `limit` are sent (one model at a time cannot take more in one period); skipped rows do not
    count. Returns how many were sent.
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
                        Workspace.state == WorkspaceState.active,
                        DocumentLedger.state == DocumentState.received,
                        DocumentLedger.ingest_state.in_(
                            (IngestState.consumed, IngestState.unsupported)
                        ),
                        DocumentLedger.ingested_at < cutoff,
                    )
                )
                .order_by(DocumentLedger.ingested_at, DocumentLedger.id)
                .limit(scan if pending is not None else limit)
            )
        ).all()
    sent = 0
    for ledger_id, ws_id in rows:
        if sent >= limit:
            break
        try:
            if pending is None:
                await enqueue(ledger_id, ws_id)
            elif not await pending.send(enqueue, ledger_id, ws_id):
                continue  # in flight
        except Exception:
            log.exception("requeue of extraction for document %s failed", ledger_id)
            continue
        sent += 1
    return sent
