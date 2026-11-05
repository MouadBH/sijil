"""Period exports (plan 4.6, ARC-5, design §5.6), in the export-only worker.

Product-owner ruling (2026-11-03): one worker, `worker-export`, builds exports. It reads
originals with the read-only export key (never from Paperless), is the only holder of the
region's Ed25519 signing seed, reaches no tenant network and has no Docker socket.

`build_export(job_id)`:

1. asserts the region and moves the job `queued` (or `building`, a redelivery) to `building`;
   a job in any other state is left alone;
2. reads one snapshot: the ledger rows in `received`, `needs_review`, `filed` or `sealed` and
   the audit chain head (the workspace's last event), in a single REPEATABLE READ transaction;
   then the fields and correction notes in one index transaction opened right after (sealed
   rows are frozen, so the index has no seal-time race left);
3. keeps the documents whose `retention_basis` (the plausible document date, else the
   region-local arrival date: the basis retention uses) falls in the period;
4. streams each original from the object store into the ZIP at `originals/<id>.<ext>`, hashing
   as it goes: a hash or size that is not the ledger's fails the job (`export.hash_mismatch`)
   and audits `integrity.export_mismatch` on the document;
5. writes `index.csv` (UTF-8 with BOM, `;`-separated so French-locale Excel opens it in
   columns, CRLF, RFC 4180 quoting), with the effective field values (the latest correction
   note per field wins, a free-text value that a spreadsheet would run as a formula escaped)
   and the corrected field names, then the canonical
   `integrity-report.json` and its signature (sijil_core.export_report);
6. puts the ZIP at `<workspace_id>/<job_id>.zip` in the exports bucket, sets the job `ready`
   with the ZIP's hash and size, and audits `export.ready` `{period, document_count}`.

`expire_exports` (daily) deletes exports ready for more than 7 days, sets them `expired` and
audits `export.expired` `{period}` for each.

`sweep_exports` (every 300 s, main worker) re-sends `sijil.build_export` for jobs `queued` for
more than 10 minutes (a lost message), claiming `export:pending:<id>` (1 h) before each send;
and fails, with `export.error` audited `export.failed`, jobs `building` for more than an hour
whose build lock is free (the build died). A job holding the lock is a build still running and
is left alone; a re-send of a job being built takes no lock and does nothing.
"""

import base64
import csv
import hashlib
import io
import logging
import tempfile
import zipfile
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final, Protocol
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core import index as index_db
from sijil_core.db.models import (
    ActorType,
    AuditEvent,
    DocumentLedger,
    DocumentState,
    ExportJob,
    ExportState,
    Organisation,
    Workspace,
)
from sijil_core.export_report import (
    INDEX_NAME,
    REPORT_NAME,
    SIGNATURE_NAME,
    AuditHead,
    ReportDocument,
    build_report,
    period_bounds,
    public_key,
    sign,
)
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import Correction, FieldsRow
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.retention import retention_basis
from sijil_core.settings import CoreSettings
from sijil_core.storage import ObjectRef
from sijil_worker.pending import PendingExtractions

log = logging.getLogger(__name__)

HASH_MISMATCH: Final = "export.hash_mismatch"
EXPORT_FAILED: Final = "export.error"
EXPIRY: Final = timedelta(days=7)
STALE_QUEUED: Final = timedelta(minutes=10)
STALE_BUILDING: Final = timedelta(hours=1)
SWEEP_BATCH: Final = 50
_BUILD_LOCK: Final = "SELECT pg_try_advisory_xact_lock(hashtextextended('export:' || :id, 0))"
INCLUDED: Final = (
    DocumentState.received,
    DocumentState.needs_review,
    DocumentState.filed,
    DocumentState.sealed,
)
_BUILDABLE: Final = (ExportState.queued, ExportState.building)
# The file extension of each `content_kind` (sijil_core.filetypes.FileKind).
_EXTENSIONS: Final = {"jpeg": "jpg", "tiff": "tif"}
# The extracted fields people type or OCR reads as they are; every other column is an id, a
# hash, a state, a date or a canonical value.
FREE_TEXT_FIELDS: Final = frozenset({"supplier_name", "number"})
_FORMULA_STARTS: Final = ("=", "+", "-", "@", "\t", "\r")
CSV_COLUMNS: Final = (
    "document_id",
    "state",
    "sha256",
    "size_bytes",
    "content_kind",
    "source",
    "ingested_at",
    "sealed_at",
    "fields_sha256",
    "retention_until",
    *FIELD_NAMES,
    "corrected_fields",
)


class IndexSource(Protocol):
    """The workspaces' index databases (`IndexEngines` satisfies it)."""

    async def engine(self, workspace: Workspace) -> AsyncEngine: ...


class OriginalReader(Protocol):
    def open(self, ref: ObjectRef) -> AsyncIterator[bytes]: ...


class ExportWriter(Protocol):
    async def put_export(self, key: str, path: Path) -> None: ...

    async def delete_export(self, key: str) -> None: ...


class HashMismatch(Exception):  # noqa: N818 - named for the machine code it maps to
    """An original read back from the store is not the one the ledger recorded."""

    def __init__(self, ledger_id: UUID) -> None:
        super().__init__(str(ledger_id))
        self.ledger_id = ledger_id


@dataclass(frozen=True)
class _Doc:
    ledger: DocumentLedger
    fields: FieldsRow | None
    corrections: list[Correction]

    @property
    def path(self) -> str:
        kind = self.ledger.content_kind
        return f"originals/{self.ledger.id}.{_EXTENSIONS.get(kind, kind)}"


def _ts(value: datetime | None) -> str:
    return "" if value is None else value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _cell(name: str, value: str | None) -> str:
    """A free-text value a spreadsheet would run as a formula gets a leading apostrophe;
    canonical values (amounts, dates, codes) never start with one of those but `-`, and stay."""
    text = value or ""
    if name in FREE_TEXT_FIELDS and text.startswith(_FORMULA_STARTS):
        return "'" + text
    return text


def index_csv(docs: Sequence[_Doc]) -> bytes:
    """UTF-8 with a BOM, `;` separators, CRLF line ends, RFC 4180 quoting (only where needed).
    No `sep=` line: Excel drops the BOM after one."""
    out = io.StringIO(newline="")
    writer = csv.writer(out, delimiter=";", lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(CSV_COLUMNS)
    for doc in docs:
        row = doc.ledger
        effective = (
            index_db.effective_fields(doc.fields, doc.corrections) if doc.fields is not None else {}
        )
        values = [_cell(name, effective.get(name, (None, False))[0]) for name in FIELD_NAMES]
        corrected = sorted(name for name, (_v, flag) in effective.items() if flag)
        writer.writerow(
            [
                str(row.id),
                row.state.value,
                row.sha256,
                str(row.size_bytes),
                row.content_kind,
                row.source.value,
                _ts(row.ingested_at),
                _ts(row.sealed_at),
                row.fields_sha256 or "",
                row.retention_until.isoformat(),
                *values,
                ";".join(corrected),
            ]
        )
    return b"\xef\xbb\xbf" + out.getvalue().encode("utf-8")


async def _claim(
    sessions: async_sessionmaker[AsyncSession], job_id: UUID, settings: CoreSettings
) -> tuple[ExportJob, Workspace, str] | None:
    async with sessions() as session:
        found = (
            await session.execute(
                select(ExportJob, Workspace, Organisation.region)
                .join(Workspace, Workspace.id == ExportJob.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(ExportJob.id == job_id)
                .with_for_update(of=ExportJob)
            )
        ).first()
        if found is None:
            log.warning("export %s not found", job_id)
            return None
        job, workspace, region = found
        try:
            assert_region(region, settings)
        except RegionMismatch:
            log.warning("export %s skipped: region %s not served", job_id, region)
            return None
        if job.state not in _BUILDABLE:
            log.info("export %s skipped: state %s", job_id, job.state.value)
            return None
        job.state = ExportState.building
        await session.commit()
        return job, workspace, region


async def _snapshot(
    sessions: async_sessionmaker[AsyncSession], workspace_id: UUID
) -> tuple[list[DocumentLedger], AuditHead]:
    async with sessions() as session:
        # The first statement of the transaction: the ledger and the chain head agree.
        await session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        rows = list(
            await session.scalars(
                select(DocumentLedger)
                .where(
                    DocumentLedger.workspace_id == workspace_id,
                    DocumentLedger.state.in_(INCLUDED),
                )
                .order_by(DocumentLedger.ingested_at, DocumentLedger.id)
            )
        )
        head = (
            await session.execute(
                select(AuditEvent.id, AuditEvent.hash)
                .where(AuditEvent.workspace_id == workspace_id)
                .order_by(AuditEvent.seq.desc())
                .limit(1)
            )
        ).first()
        await session.commit()
    return rows, AuditHead(id=head[0], hash=head[1]) if head else AuditHead(id=None, hash=None)


async def _fields(
    index: IndexSource, workspace: Workspace, rows: Sequence[DocumentLedger]
) -> dict[UUID, tuple[FieldsRow | None, list[Correction]]]:
    found: dict[UUID, tuple[FieldsRow | None, list[Correction]]] = {}
    if not rows:
        return found  # an empty archive needs no index connection
    engine = await index.engine(workspace)
    async with engine.connect() as conn:
        await conn.execution_options(isolation_level="REPEATABLE READ")
        async with conn.begin():
            for row in rows:
                fields = await index_db.read_fields(conn, row.id)
                notes = await index_db.read_corrections(conn, row.id) if fields else []
                found[row.id] = (fields, notes)
    return found


def _in_period(doc: _Doc, region: str, now: datetime, bounds: tuple[date, date]) -> bool:
    dated = doc.fields.values.get("date") if doc.fields is not None else None
    basis = retention_basis(dated, doc.ledger.ingested_at, region, now)
    return bounds[0] <= basis <= bounds[1]


async def _write_original(zf: zipfile.ZipFile, doc: _Doc, originals: OriginalReader) -> None:
    row = doc.ledger
    digest, size = hashlib.sha256(), 0
    with zf.open(doc.path, "w", force_zip64=True) as entry:
        async for chunk in originals.open(ObjectRef(row.object_key, row.object_version_id)):
            digest.update(chunk)
            size += len(chunk)
            entry.write(chunk)
    if digest.hexdigest() != row.sha256 or size != row.size_bytes:
        raise HashMismatch(row.id)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


async def build_export(
    job_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    index: IndexSource,
    originals: OriginalReader,
    exports: ExportWriter,
    settings: CoreSettings,
    seed: bytes,
    now: datetime,
) -> bool:
    """See the module docstring. True when this run made the export ready; False when there
    was nothing to do here or the job failed on a hash mismatch. Any other error propagates
    for the task to retry or record (`record_export_failed`).

    One build per job at a time: a transaction-scoped advisory lock is held for the whole
    build, and a delivery that cannot take it leaves the job alone (it would otherwise
    overwrite the ZIP the running build is about to record)."""
    async with sessions() as guard:
        held = await guard.scalar(text(_BUILD_LOCK), {"id": str(job_id)})
        if not held:
            log.info("export %s skipped: another build of it is running", job_id)
            return False
        try:
            return await _build(
                job_id, sessions=sessions, index=index, originals=originals, exports=exports,
                settings=settings, seed=seed, now=now,
            )  # fmt: skip
        finally:
            await guard.rollback()


async def _build(
    job_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    index: IndexSource,
    originals: OriginalReader,
    exports: ExportWriter,
    settings: CoreSettings,
    seed: bytes,
    now: datetime,
) -> bool:
    claimed = await _claim(sessions, job_id, settings)
    if claimed is None:
        return False
    job, workspace, region = claimed
    bounds = period_bounds(job.period)
    rows, head = await _snapshot(sessions, workspace.id)
    fields = await _fields(index, workspace, rows)
    docs = [d for row in rows if _in_period(d := _Doc(row, *fields[row.id]), region, now, bounds)]
    key = f"{workspace.id}/{job.id}.zip"
    with tempfile.TemporaryDirectory(prefix="sijil-export-") as tmp:
        path = Path(tmp) / "export.zip"
        try:
            with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for doc in docs:
                    await _write_original(zf, doc, originals)
                csv_bytes = index_csv(docs)
                zf.writestr(INDEX_NAME, csv_bytes)
                report = build_report(
                    workspace_id=workspace.id,
                    region=region,
                    period=job.period,
                    generated_at=now,
                    documents=[
                        ReportDocument(
                            id=d.ledger.id,
                            path=d.path,
                            sha256=d.ledger.sha256,
                            size_bytes=d.ledger.size_bytes,
                            fields_sha256=d.ledger.fields_sha256,
                        )
                        for d in docs
                    ],
                    index_csv_sha256=hashlib.sha256(csv_bytes).hexdigest(),
                    audit_head=head,
                    public_key=public_key(seed),
                )
                zf.writestr(REPORT_NAME, report)
                zf.writestr(SIGNATURE_NAME, base64.b64encode(sign(report, seed)))
        except HashMismatch as exc:
            await _record_mismatch(sessions, job_id, workspace.id, exc.ledger_id, now)
            return False
        sha256, size = _file_sha256(path), path.stat().st_size
        await exports.put_export(key, path)
    async with sessions() as session:
        locked = await session.get(ExportJob, job_id, with_for_update=True)
        if locked is None or locked.state is not ExportState.building:
            return False
        locked.state = ExportState.ready
        locked.object_key = key
        locked.sha256 = sha256
        locked.size_bytes = size
        locked.document_count = len(docs)
        locked.ready_at = now
        await _audit(session, locked, "export.ready", now, document_count=len(docs))
        await session.commit()
    return True


async def _audit(
    session: AsyncSession, job: ExportJob, action: str, now: datetime, **data: str | int
) -> None:
    await audit.append(
        session,
        workspace_id=job.workspace_id,
        action=action,
        actor_type=ActorType.system,
        actor_id=None,
        target_type="export",
        target_id=str(job.id),
        data={"period": job.period, **data},
        now=now,
    )


async def _record_mismatch(
    sessions: async_sessionmaker[AsyncSession],
    job_id: UUID,
    workspace_id: UUID,
    ledger_id: UUID,
    now: datetime,
) -> None:
    async with sessions() as session:
        job = await session.get(ExportJob, job_id, with_for_update=True)
        if job is None or job.state is not ExportState.building:
            return
        job.state = ExportState.failed
        job.error_code = HASH_MISMATCH
        log.error("export %s failed: document %s does not match its hash", job_id, ledger_id)
        await audit.append(
            session,
            workspace_id=workspace_id,
            action="integrity.export_mismatch",
            actor_type=ActorType.system,
            actor_id=None,
            target_type="document",
            target_id=str(ledger_id),
            data={"export_id": str(job_id)},
            now=now,
        )
        await session.commit()


async def record_export_failed(
    job_id: UUID,
    code: str,
    *,
    sessions: async_sessionmaker[AsyncSession],
    settings: CoreSettings,
    now: datetime,
) -> bool:
    """A job still `queued` or `building` becomes `failed` with `code`, audited `export.failed`
    `{period, code}`. False (nothing recorded) for an unknown job, another region's, or one
    that already finished."""
    async with sessions() as session:
        found = (
            await session.execute(
                select(ExportJob, Organisation.region)
                .join(Workspace, Workspace.id == ExportJob.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(ExportJob.id == job_id)
                .with_for_update(of=ExportJob)
            )
        ).first()
        if found is None:
            return False
        job, region = found
        assert_region(region, settings)
        if job.state not in _BUILDABLE:
            return False
        job.state = ExportState.failed
        job.error_code = code
        await _audit(session, job, "export.failed", now, code=code)
        await session.commit()
    return True


async def expire_exports(
    sessions: async_sessionmaker[AsyncSession],
    exports: ExportWriter,
    *,
    settings: CoreSettings,
    now: datetime,
) -> int:
    """Delete this region's exports ready for more than 7 days, oldest first; each becomes
    `expired`, audited `export.expired` `{period}`. The object goes first: a crash between the
    delete and the commit only repeats the (idempotent) delete. Returns how many expired."""
    async with sessions() as session:
        due = (
            await session.scalars(
                select(ExportJob.id)
                .join(Workspace, Workspace.id == ExportJob.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(
                    Organisation.region == settings.region,
                    ExportJob.state == ExportState.ready,
                    ExportJob.ready_at < now - EXPIRY,
                )
                .order_by(ExportJob.ready_at, ExportJob.id)
            )
        ).all()
    expired = 0
    for job_id in due:
        async with sessions() as session:
            job = await session.get(ExportJob, job_id, with_for_update=True)
            if job is None or job.state is not ExportState.ready or job.object_key is None:
                continue
            await exports.delete_export(job.object_key)
            job.state = ExportState.expired
            await _audit(session, job, "export.expired", now)
            await session.commit()
        expired += 1
    return expired


async def _stale(
    sessions: async_sessionmaker[AsyncSession],
    state: ExportState,
    before: datetime,
    region: str,
    limit: int,
) -> list[UUID]:
    # `created_at` stands for when a job went stale: a `building` job also needs its build lock
    # free to be failed, so a long build that is still running is never cut short.
    async with sessions() as session:
        return list(
            await session.scalars(
                select(ExportJob.id)
                .join(Workspace, Workspace.id == ExportJob.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(
                    Organisation.region == region,
                    ExportJob.state == state,
                    ExportJob.created_at < before,
                )
                .order_by(ExportJob.created_at, ExportJob.id)
                .limit(limit)
            )
        )


async def _fail_if_abandoned(
    sessions: async_sessionmaker[AsyncSession], job_id: UUID, now: datetime
) -> bool:
    async with sessions() as session:
        if not await session.scalar(text(_BUILD_LOCK), {"id": str(job_id)}):
            return False  # a build of it is running
        job = await session.get(ExportJob, job_id, with_for_update=True)
        if job is None or job.state is not ExportState.building:
            return False
        job.state = ExportState.failed
        job.error_code = EXPORT_FAILED
        await _audit(session, job, "export.failed", now, code=EXPORT_FAILED)
        await session.commit()
    log.error("export %s failed: its build stopped without finishing", job_id)
    return True


async def sweep_exports(
    sessions: async_sessionmaker[AsyncSession],
    send: Callable[[UUID], Awaitable[None]],
    *,
    pending: PendingExtractions,
    region: str,
    now: datetime,
    limit: int = SWEEP_BATCH,
) -> tuple[int, int]:
    """See the module docstring. Returns (re-sent, failed). A failed send releases its claim
    and is logged; the sweep goes on."""
    resent = 0
    for job_id in await _stale(sessions, ExportState.queued, now - STALE_QUEUED, region, limit):
        if not await pending.claim(job_id):
            continue
        try:
            await send(job_id)
        except Exception as exc:
            await pending.clear(job_id)
            log.warning("export %s re-send failed: %s", job_id, type(exc).__name__)
            continue
        resent += 1
    failed = 0
    for job_id in await _stale(sessions, ExportState.building, now - STALE_BUILDING, region, limit):
        failed += await _fail_if_abandoned(sessions, job_id, now)
    return resent, failed
