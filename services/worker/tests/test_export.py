"""`build_export`: a period export built from the object store, indexed and signed (ARC-5).

The originals and the exports bucket are the in-memory doubles; the ledger is the real test
database and the fields a throw-away index database. SAMPLE values only.
"""

import csv
import hashlib
import io
import os
import uuid
import zipfile
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from redis.asyncio import Redis
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sijil_core import audit, index
from sijil_core.db.models import (
    ActorType,
    AuditEvent,
    DocumentState,
    ExportJob,
    ExportState,
    User,
    Workspace,
)
from sijil_core.export_report import (
    INDEX_NAME,
    REPORT_NAME,
    SIGNATURE_NAME,
    public_key,
    read_report,
    verify_zip,
)
from sijil_core.fields import FIELD_NAMES
from sijil_core.pending import EXPORT_PREFIX
from sijil_core.search_index import SearchLedgerFacts, refresh_search_doc
from sijil_core.storage import ObjectRef
from sijil_worker import tasks
from sijil_worker.export import build_export, expire_exports, sweep_exports
from sijil_worker.pending import PendingExtractions
from sijil_worker.settings import ExportSettings
from tests.support.exports import OneIndex, add_exportable
from tests.support.exports import invoice_values as _values
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryExportStore, MemoryObjectStore
from tests.support.worker_tasks import WorkerTasks, worker_tasks

pytestmark = pytest.mark.db

NOW = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
SEED = bytes(range(32))  # not the tasks' seed: any seed signs
SETTINGS = ExportSettings.model_construct(region="ma-cas", environment="test")
FAR = date(2036, 12, 31)
REASON = "Avoir du fournisseur reçu après le scellement (SAMPLE)."
HEADER = [
    "document_id", "state", "sha256", "size_bytes", "content_kind", "source", "ingested_at",
    "sealed_at", "fields_sha256", "retention_until", *FIELD_NAMES, "corrected_fields",
]  # fmt: skip


@pytest.fixture(scope="module")
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
async def idx(index_db_url: str) -> AsyncIterator[OneIndex]:
    found = OneIndex(index_db_url)
    yield found
    await found.aclose()


@pytest.fixture
async def index_conn(index_db_url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(index_db_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as conn:
        await index.ensure_schema(conn)
        yield conn
    await engine.dispose()


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


@pytest.fixture
def exports() -> MemoryExportStore:
    return MemoryExportStore()


async def _job(
    db: AsyncSession,
    ws: Workspace,
    user: User,
    period: str = "2026-10",
    *,
    age: timedelta | None = None,
) -> ExportJob:
    created = NOW - (age or timedelta(minutes=1))
    job = ExportJob(workspace_id=ws.id, period=period, requested_by=user.id, created_at=created)
    db.add(job)
    await db.flush()
    await audit.append(
        db,
        workspace_id=ws.id,
        action="export.requested",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        target_type="export",
        target_id=str(job.id),
        data={"period": period},
        now=NOW - timedelta(minutes=1),
    )
    await db.commit()
    return job


async def _build(job: ExportJob, sessions: Any, idx: OneIndex, store: Any, exports: Any) -> bool:
    return await build_export(
        job.id,
        sessions=sessions,
        index=idx,
        originals=store,
        exports=exports,
        settings=SETTINGS,
        seed=SEED,
        now=NOW,
    )


async def _fresh(db: AsyncSession, job: ExportJob) -> ExportJob:
    found = await db.scalar(
        select(ExportJob).where(ExportJob.id == job.id).execution_options(populate_existing=True)
    )
    assert found is not None
    return found


async def _events(db: AsyncSession, ws: Workspace) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id)
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


def _zip(exports: MemoryExportStore, job: ExportJob, tmp_path: Path) -> Path:
    path = tmp_path / "export.zip"
    path.write_bytes(exports.objects[f"{job.workspace_id}/{job.id}.zip"])
    return path


def _rows(path: Path) -> list[dict[str, str]]:
    with zipfile.ZipFile(path) as zf:
        raw = zf.read(INDEX_NAME)
    return list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline=""), delimiter=";"))


async def test_export_contains_all_period_docs(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, idx: OneIndex,
    index_conn: AsyncConnection, tmp_path: Path,
) -> None:  # fmt: skip
    oct_ = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)
    args = (db, memory_store, index_conn, workspace, user)
    sealed = await add_exportable(
        *args, state=DocumentState.sealed, arrived=oct_, values=_values(date="2026-10-15")
    )
    undated = await add_exportable(*args, arrived=datetime(2026, 10, 20, 9, 0, tzinfo=UTC))
    # Dated in the period, arrived after it: in.
    late = await add_exportable(
        *args, state=DocumentState.needs_review, arrived=datetime(2026, 11, 2, 9, 0, tzinfo=UTC),
        values=_values(date="2026-10-31"), content_kind="jpeg",
    )  # fmt: skip
    # Dated before the period: out. Undated, arrived at 00:30 on 1 November in Casablanca
    # (whatever the UTC date): out. Removed: out.
    await add_exportable(
        *args, state=DocumentState.received, arrived=oct_, values=_values(date="2026-09-30")
    )
    await add_exportable(
        *args, arrived=datetime(2026, 11, 1, 0, 30, tzinfo=ZoneInfo("Africa/Casablanca"))
    )
    await add_exportable(
        *args, state=DocumentState.removed, arrived=oct_, values=_values(date="2026-10-10")
    )
    job = await _job(db, workspace, user)
    head = (await _events(db, workspace))[-1]

    assert await _build(job, sessions, idx, memory_store, exports)

    done = await _fresh(db, job)
    assert done.state is ExportState.ready
    assert done.document_count == 3
    assert done.object_key == f"{workspace.id}/{job.id}.zip"
    assert done.ready_at == NOW
    path = _zip(exports, job, tmp_path)
    assert done.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert done.size_bytes == path.stat().st_size
    with zipfile.ZipFile(path) as zf:
        assert sorted(zf.namelist()) == sorted(
            [
                f"originals/{sealed.id}.pdf",
                f"originals/{undated.id}.pdf",
                f"originals/{late.id}.jpg",
                INDEX_NAME,
                REPORT_NAME,
                SIGNATURE_NAME,
            ]
        )
        original = memory_store.objects[ObjectRef(sealed.object_key, "v1")].data
        assert zf.read(f"originals/{sealed.id}.pdf") == original
    assert verify_zip(path, public_key(SEED)).ok
    report = read_report(path)
    assert report is not None
    assert (report["workspace_id"], report["region"], report["period"]) == (
        str(workspace.id), "ma-cas", "2026-10",
    )  # fmt: skip
    assert report["audit_head"] == {"id": str(head.id), "hash": head.hash}
    by_id = {d["id"]: d for d in report["documents"]}
    assert by_id[str(sealed.id)]["fields_sha256"] == sealed.fields_sha256
    assert by_id[str(undated.id)]["fields_sha256"] is None
    assert by_id[str(late.id)]["sha256"] == late.sha256
    states = {r["document_id"]: r["state"] for r in _rows(path)}
    assert states == {
        str(sealed.id): "sealed", str(undated.id): "filed", str(late.id): "needs_review",
    }  # fmt: skip
    ready = (await _events(db, workspace))[-1]
    assert (ready.action, ready.target_id, ready.data) == (
        "export.ready", str(job.id), {"period": "2026-10", "document_count": 3},
    )  # fmt: skip


async def test_index_csv_bom_crlf_arabic_and_quotes(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, idx: OneIndex,
    index_conn: AsyncConnection, tmp_path: Path,
) -> None:  # fmt: skip
    name = 'شركة الأطلس "SAMPLE"; ش.م.م'
    row = await add_exportable(
        db, memory_store, index_conn, workspace, user, state=DocumentState.sealed,
        arrived=datetime(2026, 10, 3, 8, 0, tzinfo=UTC),
        values=_values(date="2026-10-02", supplier_name=name, number="F-1\nbis"),
    )  # fmt: skip
    job = await _job(db, workspace, user)
    assert await _build(job, sessions, idx, memory_store, exports)
    path = _zip(exports, job, tmp_path)
    with zipfile.ZipFile(path) as zf:
        raw = zf.read(INDEX_NAME)
    assert raw.startswith(b"\xef\xbb\xbf")
    body = raw[3:].decode("utf-8")
    assert body.endswith("\r\n")
    # `;`-separated, so French-locale Excel (fr-MA) opens it in columns; no `sep=` line,
    # which would make Excel drop the BOM.
    assert body.split("\r\n", 1)[0] == ";".join(HEADER)
    assert not body.startswith("sep=")
    assert '"شركة الأطلس ""SAMPLE""; ش.م.م"' in body  # RFC 4180: quoted, quotes doubled
    [parsed] = _rows(path)
    assert list(parsed) == HEADER
    assert parsed["supplier_name"] == name
    assert parsed["number"] == "F-1\nbis"
    assert parsed["document_id"] == str(row.id)
    assert parsed["sha256"] == row.sha256
    assert parsed["size_bytes"] == str(row.size_bytes)
    assert parsed["ingested_at"] == "2026-10-03T08:00:00Z"
    assert parsed["sealed_at"] == "2026-10-03T09:00:00Z"
    assert parsed["retention_until"] == "2036-12-31"
    assert parsed["fields_sha256"] == row.fields_sha256
    assert parsed["corrected_fields"] == ""


async def test_corrected_value_flagged(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, idx: OneIndex,
    index_conn: AsyncConnection, tmp_path: Path,
) -> None:  # fmt: skip
    row = await add_exportable(
        db, memory_store, index_conn, workspace, user, state=DocumentState.sealed,
        arrived=datetime(2026, 10, 3, 8, 0, tzinfo=UTC), values=_values(date="2026-10-02"),
    )  # fmt: skip
    for value, at in (("1400.00", NOW - timedelta(hours=2)), ("1200.00", NOW - timedelta(hours=1))):
        await index.add_correction(
            index_conn, row.id, "total_ttc", value, REASON, created_by=user.id, now=at
        )
    await index.add_correction(
        index_conn, row.id, "number", None, REASON, created_by=user.id, now=NOW
    )
    job = await _job(db, workspace, user)
    assert await _build(job, sessions, idx, memory_store, exports)
    path = _zip(exports, job, tmp_path)
    [parsed] = _rows(path)
    assert parsed["total_ttc"] == "1200.00"  # the latest note wins
    assert parsed["number"] == ""
    assert parsed["supplier_name"] == "Atlas Télécom SAMPLE SARL"  # not corrected
    assert parsed["corrected_fields"] == "number;total_ttc"
    with zipfile.ZipFile(path) as zf:
        assert ';"number;total_ttc"\r\n' in zf.read(INDEX_NAME).decode("utf-8-sig")
    # The sealed record is unchanged: its digest is still the one signed.
    report = read_report(path)
    assert report is not None
    assert report["documents"][0]["fields_sha256"] == row.fields_sha256


async def test_hash_mismatch_fails_job(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, idx: OneIndex,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_exportable(
        db, memory_store, index_conn, workspace, user,
        arrived=datetime(2026, 10, 3, 8, 0, tzinfo=UTC),
    )  # fmt: skip
    stored = memory_store.objects[ObjectRef(row.object_key, "v1")]
    stored.data = stored.data[:-1] + bytes([stored.data[-1] ^ 1])
    job = await _job(db, workspace, user)

    assert not await _build(job, sessions, idx, memory_store, exports)

    failed = await _fresh(db, job)
    assert failed.state is ExportState.failed
    assert failed.error_code == "export.hash_mismatch"
    assert failed.object_key is None
    assert exports.objects == {}
    event = (await _events(db, workspace))[-1]
    assert (event.action, event.target_type, event.target_id) == (
        "integrity.export_mismatch", "document", str(row.id),
    )  # fmt: skip
    assert event.data == {"export_id": str(job.id)}


async def test_a_finished_job_is_not_built_again(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, idx: OneIndex,
) -> None:  # fmt: skip
    job = await _job(db, workspace, user)
    assert await _build(job, sessions, idx, memory_store, exports)
    before = await _events(db, workspace)
    assert not await _build(job, sessions, idx, memory_store, exports)
    assert len(await _events(db, workspace)) == len(before)
    assert (await _fresh(db, job)).document_count == 0


async def test_expired_exports_are_deleted_and_audited(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, idx: OneIndex,
) -> None:  # fmt: skip
    old = await _job(db, workspace, user, period="2026-09")
    recent = await _job(db, workspace, user)
    assert await _build(old, sessions, idx, memory_store, exports)
    assert await _build(recent, sessions, idx, memory_store, exports)
    await db.execute(
        text("UPDATE export_job SET ready_at = :at WHERE id = :id"),
        {"at": NOW - timedelta(days=7, minutes=1), "id": old.id},
    )
    await db.commit()

    assert await expire_exports(sessions, exports, settings=SETTINGS, now=NOW) == 1

    assert (await _fresh(db, old)).state is ExportState.expired
    assert (await _fresh(db, recent)).state is ExportState.ready
    assert exports.deleted == [f"{workspace.id}/{old.id}.zip"]
    assert list(exports.objects) == [f"{workspace.id}/{recent.id}.zip"]
    event = (await _events(db, workspace))[-1]
    assert (event.action, event.target_id, event.data) == (
        "export.expired", str(old.id), {"period": "2026-09"},
    )  # fmt: skip
    assert await expire_exports(sessions, exports, settings=SETTINGS, now=NOW) == 0


def _runner(
    monkeypatch: pytest.MonkeyPatch, migrated_db: Any, store: Any, exports: Any
) -> WorkerTasks:
    return worker_tasks(
        monkeypatch,
        database_url=migrated_db.app_url,
        redis_url=os.environ.get("SIJIL_TEST_REDIS_URL", "redis://127.0.0.1:1/0"),
        store=store,
        exports=exports,
    )


async def test_build_export_task_builds_with_the_signing_seed(
    db: AsyncSession, workspace: Workspace, user: User, migrated_db: Any,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    """The task as `worker-export` runs it: the seed from SIJIL_EXPORT_SIGNING_KEY."""
    job = await _job(db, workspace, user)
    runner = _runner(monkeypatch, migrated_db, memory_store, exports)
    result = await runner.run("sijil.build_export", job_id=str(job.id))
    assert result.successful(), result.traceback
    assert (await _fresh(db, job)).state is ExportState.ready
    seed = ExportSettings().signing_seed()
    assert verify_zip(_zip(exports, job, tmp_path), public_key(seed)).ok


async def test_build_export_task_records_a_final_failure(
    db: AsyncSession, workspace: Workspace, user: User, migrated_db: Any,
    memory_store: MemoryObjectStore, exports: MemoryExportStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    job = await _job(db, workspace, user)
    runner = _runner(monkeypatch, migrated_db, memory_store, exports)

    async def broken(*_: Any, **__: Any) -> bool:
        raise ValueError("a bug")

    monkeypatch.setattr(tasks, "build_export", broken)
    result = await runner.run("sijil.build_export", job_id=str(job.id))
    assert result.successful()  # recorded, not retried
    failed = await _fresh(db, job)
    assert (failed.state, failed.error_code) == (ExportState.failed, "export.error")
    event = (await _events(db, workspace))[-1]
    assert (event.action, event.data) == (
        "export.failed", {"period": "2026-10", "code": "export.error"},
    )  # fmt: skip


async def test_the_app_role_cannot_delete_a_job(
    db: AsyncSession, workspace: Workspace, user: User
) -> None:
    job = await _job(db, workspace, user)
    with pytest.raises(DBAPIError, match="permission denied"):
        await db.execute(text("DELETE FROM export_job WHERE id = :id"), {"id": job.id})


async def test_a_second_delivery_while_one_builds_does_nothing(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, idx: OneIndex,
) -> None:  # fmt: skip
    """Two deliveries of one job must not both write `<ws>/<job>.zip`: the ZIP one records
    would be overwritten by the other's."""
    job = await _job(db, workspace, user)
    async with sessions() as running:  # the first delivery, mid-build
        await running.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended('export:' || :id, 0))"),
            {"id": str(job.id)},
        )
        assert not await _build(job, sessions, idx, memory_store, exports)
        await running.rollback()
    assert (await _fresh(db, job)).state is ExportState.queued
    assert exports.objects == {}
    assert await _build(job, sessions, idx, memory_store, exports)  # once it is done


async def test_index_csv_escapes_formulas_in_free_text_only(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, idx: OneIndex,
    index_conn: AsyncConnection, tmp_path: Path,
) -> None:  # fmt: skip
    """A free-text value starting with = + - @ TAB or CR gets a leading apostrophe, so a
    spreadsheet shows it rather than runs it; canonical values (a negative amount) stay as they
    are. The report hashes the file as written."""
    formula = '=HYPERLINK("http://example.invalid","SAMPLE")'
    await add_exportable(
        db, memory_store, index_conn, workspace, user, state=DocumentState.sealed,
        arrived=datetime(2026, 10, 3, 8, 0, tzinfo=UTC),
        values=_values(
            date="2026-10-02", supplier_name=formula, number="@SUM(1)", total_ttc="-150.00",
            total_ht="-125.00",
        ),
    )  # fmt: skip
    for value in ("+1 SAMPLE", "-SAMPLE", "\tSAMPLE", "\rSAMPLE"):
        await add_exportable(
            db, memory_store, index_conn, workspace, user,
            arrived=datetime(2026, 10, 4, 8, 0, tzinfo=UTC),
            values=_values(date="2026-10-02", supplier_name=value),
        )  # fmt: skip
    job = await _job(db, workspace, user)
    assert await _build(job, sessions, idx, memory_store, exports)
    path = _zip(exports, job, tmp_path)
    rows = _rows(path)
    first = next(r for r in rows if r["total_ttc"] == "-150.00")
    assert first["supplier_name"] == "'" + formula
    assert first["number"] == "'@SUM(1)"
    assert (first["total_ttc"], first["total_ht"]) == ("-150.00", "-125.00")  # canonical
    assert sorted(r["supplier_name"] for r in rows if r is not first) == sorted(
        ["'+1 SAMPLE", "'-SAMPLE", "'\tSAMPLE", "'\rSAMPLE"]
    )
    assert all(r["state"] in ("sealed", "filed") and r["date"] == "2026-10-02" for r in rows)
    assert verify_zip(path, public_key(SEED)).ok  # the escaped file is the one signed


class Sent:
    def __init__(self) -> None:
        self.jobs: list[uuid.UUID] = []

    async def __call__(self, job_id: uuid.UUID) -> None:
        self.jobs.append(job_id)


async def _sweep(sessions: Any, redis: Redis, sent: Sent) -> tuple[int, int]:
    return await sweep_exports(
        sessions, sent, pending=PendingExtractions(redis, prefix=EXPORT_PREFIX),
        region="ma-cas", now=NOW,
    )  # fmt: skip


async def _set_state(db: AsyncSession, job: ExportJob, state: str) -> None:
    await db.execute(
        text("UPDATE export_job SET state = :s WHERE id = :id"), {"s": state, "id": job.id}
    )
    await db.commit()


async def test_sweep_resends_a_job_queued_for_more_than_10_minutes_once(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User, redis: Redis
) -> None:
    stuck = await _job(db, workspace, user, age=timedelta(minutes=11))
    fresh = await _job(db, workspace, user, age=timedelta(minutes=5))
    done = await _job(db, workspace, user, age=timedelta(hours=3))
    await _set_state(db, done, "failed")
    sent = Sent()
    assert await _sweep(sessions, redis, sent) == (1, 0)
    assert sent.jobs == [stuck.id]
    assert await redis.ttl(f"export:pending:{stuck.id}") > 3500  # claimed before the send
    assert await _sweep(sessions, redis, sent) == (0, 0)  # the key holds it for an hour
    assert sent.jobs == [stuck.id]
    assert (await _fresh(db, fresh)).state is ExportState.queued


async def test_sweep_releases_the_claim_when_the_send_fails(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User, redis: Redis
) -> None:
    stuck = await _job(db, workspace, user, age=timedelta(minutes=11))

    async def broken(_job_id: uuid.UUID) -> None:
        raise ConnectionError("broker down")

    assert await sweep_exports(
        sessions, broken, pending=PendingExtractions(redis, prefix=EXPORT_PREFIX),
        region="ma-cas", now=NOW,
    ) == (0, 0)  # fmt: skip
    assert await redis.exists(f"export:pending:{stuck.id}") == 0


async def test_sweep_fails_a_job_building_for_more_than_an_hour_unless_a_build_runs(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User, redis: Redis
) -> None:
    dead = await _job(db, workspace, user, age=timedelta(hours=2))
    running = await _job(db, workspace, user, age=timedelta(hours=2))
    young = await _job(db, workspace, user, age=timedelta(minutes=30))
    for job in (dead, running, young):
        await _set_state(db, job, "building")
    async with sessions() as build:  # a build of `running` holds its lock
        await build.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended('export:' || :id, 0))"),
            {"id": str(running.id)},
        )
        assert await _sweep(sessions, redis, Sent()) == (0, 1)
        await build.rollback()
    failed = await _fresh(db, dead)
    assert (failed.state, failed.error_code) == (ExportState.failed, "export.error")
    assert (await _fresh(db, running)).state is ExportState.building
    assert (await _fresh(db, young)).state is ExportState.building
    event = (await _events(db, workspace))[-1]
    assert (event.action, event.target_id, event.data) == (
        "export.failed", str(dead.id), {"period": "2026-10", "code": "export.error"},
    )  # fmt: skip


async def test_a_resent_job_never_builds_twice(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, idx: OneIndex, redis: Redis,
) -> None:  # fmt: skip
    """The sweep's re-send of a job whose first message arrives too: one build, one ZIP."""
    job = await _job(db, workspace, user, age=timedelta(minutes=11))
    sent = Sent()
    await _sweep(sessions, redis, sent)
    assert sent.jobs == [job.id]
    assert await _build(job, sessions, idx, memory_store, exports)  # the first message
    assert not await _build(job, sessions, idx, memory_store, exports)  # the re-send
    assert len(await _events(db, workspace)) == 2  # requested, ready


# One period basis for search, sums and exports (fix round 1) ---------------------------------


async def _search_period(conn: AsyncConnection, first: date, last: date) -> set[uuid.UUID]:
    found = await conn.scalars(
        text("SELECT ledger_id FROM search_doc WHERE basis_date BETWEEN :a AND :b"),
        {"a": first, "b": last},
    )
    return set(found)


async def test_export_and_search_agree_on_a_corrected_date(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, idx: OneIndex,
    index_conn: AsyncConnection, tmp_path: Path,
) -> None:  # fmt: skip
    """Sealed with a September date, corrected to October: both the search row and the export
    put it in October (its `basis_date`), not in September."""
    row = await add_exportable(
        db, memory_store, index_conn, workspace, user, state=DocumentState.sealed,
        arrived=datetime(2026, 10, 3, 8, 0, tzinfo=UTC), values=_values(date="2026-09-30"),
    )  # fmt: skip
    await index.add_correction(
        index_conn, row.id, "date", "2026-10-02", REASON, created_by=user.id, now=NOW
    )
    facts = SearchLedgerFacts(
        ledger_id=row.id, state=DocumentState.sealed, ingested_at=row.ingested_at,
        region="ma-cas", now=NOW,
    )  # fmt: skip
    await refresh_search_doc(index_conn, facts, "Facture SAMPLE")
    october = await _job(db, workspace, user, "2026-10")
    september = await _job(db, workspace, user, "2026-09")
    assert await _build(october, sessions, idx, memory_store, exports)
    assert await _build(september, sessions, idx, memory_store, exports)
    exported = {r["document_id"] for r in _rows(_zip(exports, october, tmp_path))}
    assert str(row.id) in exported
    assert (await _fresh(db, september)).document_count == 0
    assert row.id in await _search_period(index_conn, date(2026, 10, 1), date(2026, 10, 31))
    assert row.id not in await _search_period(index_conn, date(2026, 9, 1), date(2026, 9, 30))


async def test_a_document_without_a_search_row_is_exported_and_logged(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, exports: MemoryExportStore, idx: OneIndex,
    index_conn: AsyncConnection, tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:  # fmt: skip
    """An unindexed workspace still exports: the basis is computed the same way (the effective
    date), and the gap is logged with a count."""
    row = await add_exportable(
        db, memory_store, index_conn, workspace, user, state=DocumentState.sealed,
        arrived=datetime(2026, 10, 3, 8, 0, tzinfo=UTC), values=_values(date="2026-09-30"),
    )  # fmt: skip
    await index.add_correction(
        index_conn, row.id, "date", "2026-10-02", REASON, created_by=user.id, now=NOW
    )
    job = await _job(db, workspace, user, "2026-10")
    with caplog.at_level("WARNING"):
        assert await _build(job, sessions, idx, memory_store, exports)
    assert [r["document_id"] for r in _rows(_zip(exports, job, tmp_path))] == [str(row.id)]
    assert "1 document(s) not in the search index" in caplog.text
