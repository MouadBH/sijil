"""`closure_daily`: after the grace period a workspace is destroyed, or hibernated while a
document is under retention; a take-over ends the hosting but never deletes an original before
its retention (ADM-5, design §7 item 11).

The ledger is the real test database, whose guards use the real date (`sijil_region_today()`):
retention dates are set around the real today. The store is the in-memory double (RustFS
semantics, the real date too); the runtime records what it is asked. SAMPLE data only.
"""

import asyncio
import hashlib
import uuid
from collections.abc import AsyncIterator
from datetime import date, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from celery.schedules import crontab
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import ingest
from sijil_core.db.base import utcnow
from sijil_core.db.models import (
    TERMINAL_STATES,
    ActorType,
    AuditEvent,
    Closure,
    ClosureState,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    ExportDocument,
    ExportJob,
    ExportState,
    IngestState,
    Organisation,
    OrgStatus,
    Subscription,
    SubscriptionStatus,
    TakeoverStatement,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.retention import region_local_date
from sijil_core.storage import ObjectRef, RetentionActive
from sijil_core.tenancy import tenant_ref
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.closure import closure_daily
from sijil_worker.removal import remove_original
from sijil_worker.settings import BrokerSettings, WorkerSettings
from tests.support.memory_store import MemoryObjectStore, StoredObject

pytestmark = pytest.mark.db

SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")
FAR = date(2036, 12, 31)
PAST = date(2020, 12, 31)


class RecordingRuntime:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def stop(self, ref: str) -> None:
        self.calls.append(("stop", ref))

    async def destroy(self, ref: str) -> None:
        self.calls.append(("destroy", ref))


class Removals:
    def __init__(self) -> None:
        self.sent: list[tuple[UUID, UUID]] = []

    async def __call__(self, ledger_id: UUID, workspace_id: UUID) -> None:
        self.sent.append((ledger_id, workspace_id))


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


def _today() -> date:
    return region_local_date(utcnow(), "ma-cas")


async def _document(
    db: AsyncSession, store: MemoryObjectStore, ws: Workspace, user: User, retain: date,
    *, sealed: bool = False,
) -> UUID:  # fmt: skip
    """A filed (or sealed and COMPLIANCE-locked) SAMPLE document retained until `retain`."""
    data = b"%PDF-1.7 SAMPLE " + uuid.uuid4().bytes
    sha = hashlib.sha256(data).hexdigest()
    now = utcnow()
    row = DocumentLedger(
        workspace_id=ws.id, sha256=sha, md5="0" * 32, size_bytes=len(data), content_kind="pdf",
        source=DocumentSource.upload, object_key=f"k/{sha}", object_version_id="v1",
        state=DocumentState.sealed if sealed else DocumentState.filed,
        ingest_state=IngestState.consumed, retention_until=retain,
        ingested_at=now - timedelta(days=400),
        fields_sha256="f" * 64 if sealed else None,
        sealed_at=now - timedelta(days=399) if sealed else None,
        sealed_by=user.id if sealed else None,
        object_locked_at=now - timedelta(days=399) if sealed else None,
    )  # fmt: skip
    db.add(row)
    await db.commit()
    store.objects[ObjectRef(row.object_key, "v1")] = StoredObject(
        data=data, mode="COMPLIANCE" if sealed else "GOVERNANCE", retain_until=retain
    )
    return row.id


async def _closure(
    db: AsyncSession, org: Organisation, user: User, *, days_ago: int = 31
) -> Closure:
    requested = utcnow() - timedelta(days=days_ago)
    org.status = OrgStatus.closing
    row = Closure(
        org_id=org.id, requested_at=requested, requested_by=user.id,
        grace_ends_at=requested + timedelta(days=30), state=ClosureState.grace,
    )  # fmt: skip
    db.add(row)
    await db.commit()
    return row


async def _run(
    sessions: async_sessionmaker[AsyncSession], store: MemoryObjectStore,
    runtime: RecordingRuntime, removals: Removals, now: datetime | None = None,
) -> Any:  # fmt: skip
    return await closure_daily(
        sessions, runtime=runtime, locks=store, settings=SETTINGS, send_removal=removals,
        now=now or utcnow(),
    )  # fmt: skip


async def _remove(
    sessions: async_sessionmaker[AsyncSession], store: MemoryObjectStore, removals: Removals
) -> list[UUID]:
    """Runs the removal worker on every removal sent; the ids it removed."""
    purged: list[UUID] = []

    async def purge(ledger_id: UUID, _workspace_id: UUID) -> None:
        purged.append(ledger_id)

    removed = []
    for ledger_id, workspace_id in removals.sent:
        if await remove_original(
            ledger_id, sessions=sessions, store=store, settings=SETTINGS, now=utcnow(),
            workspace_id=workspace_id, purge=purge,
        ):  # fmt: skip
            removed.append(ledger_id)
    assert purged == removed
    return removed


async def _row(db: AsyncSession, model: Any, key: Any) -> Any:
    found = await db.get(model, key, populate_existing=True)
    assert found is not None
    return found


async def _events(db: AsyncSession, ws: Workspace, prefix: str) -> list[tuple[str, Any, Any]]:
    rows = await db.execute(
        select(AuditEvent.action, AuditEvent.target_id, AuditEvent.data)
        .where(AuditEvent.workspace_id == ws.id, AuditEvent.action.like(f"{prefix}%"))
        .order_by(AuditEvent.seq)
    )
    return [tuple(r) for r in rows]


def test_closure_daily_runs_on_the_main_worker_after_marking_eligibility() -> None:
    settings = BrokerSettings(region="ma-cas", environment="test", redis_url="redis://r:6379/0")
    entry = make_celery(settings).conf.beat_schedule["closure-daily"]
    assert entry == {"task": "sijil.closure_daily", "schedule": crontab(hour=2, minute=15)}
    assert tasks.closure_daily_task.name == "sijil.closure_daily"


async def test_nothing_happens_during_the_grace_period(
    db: AsyncSession, sessions: Any, org: Organisation, user: User, workspace: Workspace,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    """The grace period runs to the end of its last region-local day."""
    await _document(db, memory_store, workspace, user, PAST)
    closure = await _closure(db, org, user, days_ago=30)
    runtime, removals = RecordingRuntime(), Removals()
    # On its last day (whatever the hour), still grace.
    last_day_noon = closure.grace_ends_at.replace(hour=11, minute=0)
    await _run(sessions, memory_store, runtime, removals, now=last_day_noon)
    assert (runtime.calls, removals.sent) == ([], [])
    assert (await _row(db, Closure, closure.id)).state is ClosureState.grace
    assert (await _row(db, Workspace, workspace.id)).state is WorkspaceState.active


async def test_closure_without_retained_documents_destroys_tenant(
    db: AsyncSession, sessions: Any, org: Organisation, user: User, workspace: Workspace,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    """Every document past retention: approved by the closure, the tenant destroyed (its
    Paperless and index databases with it), the originals removed by the removal worker, the
    ledger rows kept as tombstones."""
    filed = await _document(db, memory_store, workspace, user, PAST)
    sealed = await _document(db, memory_store, workspace, user, PAST, sealed=True)
    closure = await _closure(db, org, user)
    runtime, removals = RecordingRuntime(), Removals()

    run = await _run(sessions, memory_store, runtime, removals)

    assert (run.approved, run.destroyed, run.hibernated) == (2, 1, 0)
    assert runtime.calls == [("destroy", tenant_ref(workspace.id))]
    assert sorted(removals.sent) == sorted([(filed, workspace.id), (sealed, workspace.id)])
    for doc in (filed, sealed):
        row = await _row(db, DocumentLedger, doc)
        assert (row.state, row.deletion_approved_by) == (DocumentState.deleting, user.id)
    ws = await _row(db, Workspace, workspace.id)
    assert ws.state is WorkspaceState.closed
    done = await _row(db, Closure, closure.id)
    assert (done.state, done.destroyed_at is not None) == (ClosureState.destroyed, True)
    assert (await _row(db, Organisation, org.id)).status is OrgStatus.closed
    sub = await db.scalar(select(Subscription).where(Subscription.org_id == org.id))
    assert sub is not None and sub.status is SubscriptionStatus.closed
    approved = [e for e in await _events(db, workspace, "closure.deletion_approved")]
    assert sorted(e[1] for e in approved) == sorted([str(filed), str(sealed)])
    assert {str(e[2]["closure_id"]) for e in approved} == {str(closure.id)}
    assert await _events(db, workspace, "closure.destroyed") == [
        ("closure.destroyed", str(closure.id), {"closure_id": str(closure.id), "retained": "0"})
    ]

    assert sorted(await _remove(sessions, memory_store, removals)) == sorted([filed, sealed])
    for doc in (filed, sealed):
        row = await _row(db, DocumentLedger, doc)
        assert row.state is DocumentState.deleted
        assert row.original_removed_at is not None
    assert not any(ref.key.startswith("k/") for ref in memory_store.objects)

    # A second run changes nothing.
    again = await _run(sessions, memory_store, RecordingRuntime(), Removals())
    assert (again.approved, again.destroyed, again.hibernated) == (0, 0, 0)


async def test_closure_hibernates_then_destroys_once_retention_ends(
    db: AsyncSession, sessions: Any, org: Organisation, user: User, workspace: Workspace,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    """A document under retention: hibernated, never deleted early; the day its retention has
    passed (the run's clock moves, the ledger's guard uses the real date), it is approved by
    the closure and the tenant destroyed."""
    yesterday = _today() - timedelta(days=1)
    held = await _document(db, memory_store, workspace, user, yesterday, sealed=True)
    expired = await _document(db, memory_store, workspace, user, PAST)
    closure = await _closure(db, org, user, days_ago=40)
    runtime, removals = RecordingRuntime(), Removals()
    two_days_ago = utcnow() - timedelta(days=2)

    run = await _run(sessions, memory_store, runtime, removals, now=two_days_ago)

    assert (run.approved, run.hibernated, run.destroyed) == (1, 1, 0)
    assert runtime.calls == [("stop", tenant_ref(workspace.id))]
    assert removals.sent == [(expired, workspace.id)]
    assert (await _row(db, DocumentLedger, held)).state is DocumentState.sealed
    assert (await _row(db, Workspace, workspace.id)).state is WorkspaceState.hibernated
    assert (await _row(db, Closure, closure.id)).state is ClosureState.hibernated
    assert (await _row(db, Organisation, org.id)).status is OrgStatus.closing
    assert await _events(db, workspace, "closure.hibernated") == [
        ("closure.hibernated", str(closure.id), {"closure_id": str(closure.id), "retained": "1"})
    ]

    # The next day: retention over for the held one too.
    runtime, removals = RecordingRuntime(), Removals()
    run = await _run(sessions, memory_store, runtime, removals)
    assert (run.approved, run.hibernated, run.destroyed) == (1, 0, 1)
    assert runtime.calls == [("destroy", tenant_ref(workspace.id))]
    assert removals.sent == [(held, workspace.id)]
    assert (await _row(db, Closure, closure.id)).state is ClosureState.destroyed
    assert (await _row(db, Organisation, org.id)).status is OrgStatus.closed


async def test_a_compliance_lock_beyond_retention_keeps_the_workspace_hibernated(
    db: AsyncSession, sessions: Any, org: Organisation, user: User, workspace: Workspace,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    """A sealed original whose COMPLIANCE lock outlasts its `retention_until` is retained until
    the lock ends: nothing is approved, the tenant is stopped, not destroyed."""
    doc = await _document(db, memory_store, workspace, user, PAST, sealed=True)
    row = await _row(db, DocumentLedger, doc)
    memory_store.objects[ObjectRef(row.object_key, "v1")].retain_until = FAR
    await _closure(db, org, user)
    runtime, removals = RecordingRuntime(), Removals()
    run = await _run(sessions, memory_store, runtime, removals)
    assert (run.approved, run.hibernated, run.destroyed) == (0, 1, 0)
    assert removals.sent == []
    assert (await _row(db, DocumentLedger, doc)).state is DocumentState.sealed


async def _full_export(
    db: AsyncSession, ws_id: UUID, user_id: UUID, created: datetime, docs: list[UUID],
    *, state: ExportState = ExportState.ready,
) -> UUID:  # fmt: skip
    """A built full export (period `all`) created at `created`, holding exactly `docs`."""
    job = ExportJob(
        workspace_id=ws_id, period="all", requested_by=user_id, created_at=created, state=state,
        object_key=f"{ws_id}/x.zip", sha256="a" * 64, size_bytes=1, document_count=len(docs),
        ready_at=created + timedelta(minutes=5),
    )  # fmt: skip
    db.add(job)
    await db.flush()
    db.add_all(ExportDocument(export_job_id=job.id, ledger_id=doc) for doc in docs)
    await db.commit()
    return job.id


async def _takeover(db: AsyncSession, org: Organisation, user: User, closure: Closure) -> None:
    db.add(
        TakeoverStatement(
            closure_id=closure.id, org_id=org.id, signed_by=user.id, signer_name="SAMPLE",
            signer_capacity="Gérant", object_key=f"takeover/{org.id}/x.pdf",
            object_version_id="v1", sha256="a" * 64, size_bytes=10,
            created_at=utcnow() - timedelta(days=20),
        )
    )  # fmt: skip
    await db.commit()


async def test_takeover_statement_allows_destruction_after_grace(
    db: AsyncSession, sessions: Any, org: Organisation, user: User, workspace: Workspace,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    """A signed take-over ends the hosting after the grace period: the tenant is destroyed with
    documents still under retention, whose originals stay, locked, untouched; each is approved
    and removed only once its own retention has passed, with no further approval."""
    yesterday = _today() - timedelta(days=1)
    long_held = await _document(db, memory_store, workspace, user, FAR, sealed=True)
    soon = await _document(db, memory_store, workspace, user, yesterday, sealed=True)
    closure = await _closure(db, org, user, days_ago=40)
    # Its full export holds both: built, downloaded, then expired after its 7 days (it still
    # counts: what it held is recorded).
    await _full_export(
        db, workspace.id, user.id, closure.requested_at + timedelta(hours=1), [long_held, soon],
        state=ExportState.expired,
    )  # fmt: skip
    await _takeover(db, org, user, closure)
    runtime, removals = RecordingRuntime(), Removals()
    two_days_ago = utcnow() - timedelta(days=2)

    run = await _run(sessions, memory_store, runtime, removals, now=two_days_ago)

    assert (run.approved, run.hibernated, run.destroyed) == (0, 0, 1)
    assert runtime.calls == [("destroy", tenant_ref(workspace.id))]
    assert removals.sent == []
    assert (await _row(db, Workspace, workspace.id)).state is WorkspaceState.closed
    assert (await _row(db, Closure, closure.id)).state is ClosureState.destroyed
    assert await _events(db, workspace, "closure.destroyed") == [
        ("closure.destroyed", str(closure.id), {"closure_id": str(closure.id), "retained": "2"})
    ]
    for doc in (long_held, soon):
        row = await _row(db, DocumentLedger, doc)
        assert row.state is DocumentState.sealed
        stored = memory_store.objects[ObjectRef(row.object_key, "v1")]
        assert stored.mode == "COMPLIANCE"
    # The removal worker itself refuses a version still locked, were it ever asked.
    row = await _row(db, DocumentLedger, long_held)
    with pytest.raises(RetentionActive):
        await memory_store.remove_original(ObjectRef(row.object_key, "v1"), governance=False)

    # The next day `soon` is past its retention: approved by the closure, then removed; the
    # long-held one stays, and so does its lock.
    runtime, removals = RecordingRuntime(), Removals()
    run = await _run(sessions, memory_store, runtime, removals)
    assert (run.approved, run.hibernated, run.destroyed) == (1, 0, 0)
    assert runtime.calls == []
    assert removals.sent == [(soon, workspace.id)]
    assert await _remove(sessions, memory_store, removals) == [soon]
    assert (await _row(db, DocumentLedger, soon)).state is DocumentState.deleted
    held = await _row(db, DocumentLedger, long_held)
    assert held.state is DocumentState.sealed
    stored = memory_store.objects[ObjectRef(held.object_key, "v1")]
    assert (stored.mode, stored.retain_until) == ("COMPLIANCE", FAR)
    approvals = await _events(db, workspace, "closure.deletion_approved")
    assert [e[1] for e in approvals] == [str(soon)]


async def test_a_document_captured_after_the_export_defers_the_takeover(
    db: AsyncSession, sessions: Any, org: Organisation, user: User, workspace: Workspace,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    """Final review I1: capture stays open during the grace period, so a document can arrive
    after the full export the take-over relied on. The workspace then hibernates (its Owner
    keeps reading it), the deferral audited with a code, and the take-over is honoured at a
    later run, once a newer full export holds every live document."""
    ws_id, user_id = workspace.id, user.id
    held = await _document(db, memory_store, workspace, user, FAR, sealed=True)
    closure = await _closure(db, org, user, days_ago=40)
    closure_id, requested = closure.id, closure.requested_at
    first = await _full_export(db, ws_id, user_id, requested + timedelta(hours=1), [held])
    await _takeover(db, org, user, closure)
    # Captured during the grace period, after that export.
    late = await _capture(sessions, memory_store, workspace)
    assert isinstance(late, ingest.Accepted)
    # An older full export, from before the closure, never counts, even holding everything.
    await _full_export(db, ws_id, user_id, requested - timedelta(days=1), [held, late.document_id])

    runtime = RecordingRuntime()
    run = await _run(sessions, memory_store, runtime, Removals())

    assert (run.hibernated, run.destroyed) == (1, 0)
    assert runtime.calls == [("stop", tenant_ref(ws_id))]
    assert (await _row(db, Workspace, ws_id)).state is WorkspaceState.hibernated
    assert (await _row(db, Closure, closure_id)).state is ClosureState.hibernated
    deferred = {
        "closure_id": str(closure_id), "code": "takeover.export_incomplete",
        "export_id": str(first), "uncovered": "1",
    }  # fmt: skip
    assert await _events(db, workspace, "closure.takeover_deferred") == [
        ("closure.takeover_deferred", str(closure_id), deferred)
    ]
    assert await _events(db, workspace, "closure.destroyed") == []
    assert set(await _live(db, ws_id)) == {held, late.document_id}

    # The next run, still no newer export: still hosted, the deferral not audited again.
    runtime = RecordingRuntime()
    run = await _run(sessions, memory_store, runtime, Removals())
    assert run.destroyed == 0
    assert ("destroy", tenant_ref(ws_id)) not in runtime.calls
    workspace = await _row(db, Workspace, ws_id)  # `_live` rolled back: reloaded
    assert len(await _events(db, workspace, "closure.takeover_deferred")) == 1

    # The Owner exports again (the workspace is frozen, so it holds everything): the take-over
    # is honoured at the next run.
    await _full_export(db, ws_id, user_id, utcnow(), [held, late.document_id])
    runtime = RecordingRuntime()
    run = await _run(sessions, memory_store, runtime, Removals())
    assert run.destroyed == 1
    assert runtime.calls == [("destroy", tenant_ref(ws_id))]
    assert (await _row(db, Workspace, ws_id)).state is WorkspaceState.closed
    assert (await _row(db, Closure, closure_id)).state is ClosureState.destroyed
    assert set(await _live(db, ws_id)) == {held, late.document_id}  # locked, never deleted early


async def test_a_cancelled_or_other_region_closure_is_left_alone(
    db: AsyncSession, sessions: Any, org: Organisation, user: User, workspace: Workspace,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    await _document(db, memory_store, workspace, user, PAST)
    closure = await _closure(db, org, user)
    closure.state, closure.cancelled_at, closure.cancelled_by = (
        ClosureState.cancelled, utcnow(), user.id,
    )  # fmt: skip
    org.status = OrgStatus.active
    elsewhere = Organisation(name="Paris SAMPLE", type=org.type, country="FR", region="eu-par")
    db.add(elsewhere)
    await db.commit()
    await _closure(db, elsewhere, user)
    runtime, removals = RecordingRuntime(), Removals()
    run = await _run(sessions, memory_store, runtime, removals)
    assert (run.approved, run.hibernated, run.destroyed) == (0, 0, 0)
    assert (runtime.calls, removals.sent) == ([], [])


async def _capture(
    sessions: async_sessionmaker[AsyncSession], store: MemoryObjectStore, ws: Workspace
) -> ingest.Accepted | ingest.Duplicate:
    """An inbound SAMPLE document, as the inbound mail path archives one."""

    async def chunks() -> AsyncIterator[bytes]:
        yield b"%PDF-1.7 SAMPLE late " + uuid.uuid4().bytes

    async def enqueue(_ledger_id: UUID, _workspace_id: UUID) -> None:
        return None

    return await ingest.accept_file(
        sessions=sessions, store=store, enqueue=enqueue, workspace=ws, region="ma-cas",
        chunks=chunks(), source=DocumentSource.email, audit_action="email.received",
        actor_type=ActorType.email, actor_id=None, ip=None, now=utcnow(),
    )  # fmt: skip


async def _live(db: AsyncSession, ws_id: UUID) -> list[UUID]:
    rows = await db.scalars(
        select(DocumentLedger.id).where(
            DocumentLedger.workspace_id == ws_id, DocumentLedger.state.not_in(TERMINAL_STATES)
        )
    )
    found = list(rows)
    await db.rollback()
    return found


async def test_a_capture_racing_the_destruction_never_lands_in_a_closed_workspace(
    db: AsyncSession, sessions: Any, org: Organisation, user: User, workspace: Workspace,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    """Review fix 1: the workspace stops capture, under its row lock, before the documents
    under retention are counted. A document arriving after the count (here while the tenant is
    being destroyed) is refused; it never becomes a live document, hidden from its Owner, in
    a destroyed workspace with no take-over."""
    ws_id = workspace.id
    await _document(db, memory_store, workspace, user, PAST)
    await _closure(db, org, user)
    outcomes: list[str] = []

    class Racing(RecordingRuntime):
        async def destroy(self, ref: str) -> None:
            try:
                await _capture(sessions, memory_store, workspace)
                outcomes.append("accepted")
            except ingest.WorkspaceNotAccepting:
                outcomes.append("refused")
            await super().destroy(ref)

    await _run(sessions, memory_store, Racing(), Removals())

    assert outcomes == ["refused"]
    assert (await _row(db, Workspace, ws_id)).state is WorkspaceState.closed
    assert await _live(db, ws_id) == []


async def test_a_document_captured_before_the_count_leads_to_hibernation(
    db: AsyncSession, sessions: Any, org: Organisation, user: User, workspace: Workspace,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    """A document that arrived after the grace period ended but before the job froze the
    workspace is counted: the workspace hibernates, it is never destroyed."""
    ws_id = workspace.id
    await _document(db, memory_store, workspace, user, PAST)
    await _closure(db, org, user)
    late = await _capture(sessions, memory_store, workspace)
    assert isinstance(late, ingest.Accepted)
    runtime = RecordingRuntime()
    run = await _run(sessions, memory_store, runtime, Removals())
    assert (run.hibernated, run.destroyed) == (1, 0)
    assert runtime.calls == [("stop", tenant_ref(ws_id))]
    assert (await _row(db, Workspace, ws_id)).state is WorkspaceState.hibernated
    assert await _live(db, ws_id) == [late.document_id]


async def test_accept_file_waits_for_a_freeze_and_then_refuses(
    db: AsyncSession, sessions: Any, workspace: Workspace, memory_store: MemoryObjectStore
) -> None:
    """Capture serialises with the closure's freeze on the workspace row: an archive that
    reaches its ledger insert while the freeze holds the row waits for it, then refuses."""
    ws_id = workspace.id
    async with sessions() as freezer:
        frozen = await freezer.get(Workspace, ws_id, with_for_update=True)
        assert frozen is not None
        frozen.state = WorkspaceState.hibernated
        await freezer.flush()
        racing = asyncio.create_task(_capture(sessions, memory_store, workspace))
        await asyncio.sleep(0.5)
        assert not racing.done()  # blocked on the row lock
        await freezer.commit()
    with pytest.raises(ingest.WorkspaceNotAccepting):
        await racing
    assert await _live(db, ws_id) == []


async def test_one_failing_closure_leaves_the_others_to_run(
    db: AsyncSession, sessions: Any, org: Organisation, user: User, workspace: Workspace,
    memory_store: MemoryObjectStore, caplog: pytest.LogCaptureFixture,
) -> None:  # fmt: skip
    class Broken(RecordingRuntime):
        async def destroy(self, ref: str) -> None:
            raise OSError("docker down")

    await _closure(db, org, user)
    runtime = Broken()
    run = await _run(sessions, memory_store, runtime, Removals())
    assert run.destroyed == 0
    # Frozen before the count (capture stopped), not destroyed.
    assert (await _row(db, Workspace, workspace.id)).state is WorkspaceState.hibernated
    assert "failed; the next run resumes it" in caplog.text
    # The next run resumes it.
    run = await _run(sessions, memory_store, RecordingRuntime(), Removals())
    assert run.destroyed == 1


async def test_closure_daily_task_sends_removals_to_the_removal_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: list[tuple[str, dict[str, str], str]] = []

    class App:
        def send_task(self, name: str, kwargs: dict[str, str], queue: str) -> None:
            sent.append((name, kwargs, queue))

    monkeypatch.setattr(tasks, "current_app", App())
    ledger_id, workspace_id = uuid.uuid4(), uuid.uuid4()
    await tasks.send_removal(ledger_id, workspace_id)
    assert sent == [
        ("sijil.remove_original",
         {"ledger_id": str(ledger_id), "workspace_id": str(workspace_id)}, "removal"),
    ]  # fmt: skip
