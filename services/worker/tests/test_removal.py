"""`remove_original`: the removal worker deletes a removed or approved original, then the purge.

The store is the in-memory double (RustFS semantics) except where `S3ObjectStore` itself is
under test (botocore's Stubber: no network); the database is the real test one.
"""

import hashlib
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from pydantic import AnyHttpUrl, SecretStr
from redis.asyncio import Redis
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    Organisation,
    OrgType,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.storage import ObjectRef, RetentionActive, S3ObjectStore, StorageSettings
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.pending import PendingExtractions
from sijil_worker.removal import record_removal_failed, remove_original, requeue_unremoved
from sijil_worker.settings import RemovalSettings, WorkerSettings
from tests.support.memory_store import MemoryObjectStore, StoredObject

NOW = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
FAR = date(2036, 12, 31)
PAST = date(2020, 12, 31)  # a retention long over: the row may be approved for deletion
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")
REASON = "Doublon déposé par erreur (SAMPLE)"


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


async def _ledger(
    db: AsyncSession,
    store: MemoryObjectStore,
    ws: Workspace,
    user: User,
    state: DocumentState,
    *,
    mode: str = "GOVERNANCE",
    lock_until: date = FAR,
    stored: bool = True,
) -> DocumentLedger:
    """A row brought to `state` through the transitions 0011 allows, with its original."""
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    sealed = state is DocumentState.sealed
    row = DocumentLedger(
        workspace_id=ws.id,
        sha256=sha,
        md5="0" * 32,
        size_bytes=6,
        content_kind="pdf",
        source=DocumentSource.upload,
        object_key=f"k/{sha}",
        object_version_id="v1",
        state=DocumentState.sealed if sealed else DocumentState.filed,
        ingest_state=IngestState.consumed,
        retention_until=PAST if state in (DocumentState.deleting, DocumentState.deleted) else FAR,
        ingested_at=NOW - timedelta(days=1),
        fields_sha256="f" * 64 if sealed else None,
        sealed_at=NOW - timedelta(minutes=10) if sealed else None,
        sealed_by=user.id if sealed else None,
    )
    if state in (DocumentState.received, DocumentState.needs_review):
        row.state = state
    db.add(row)
    await db.commit()
    params = {"id": row.id, "u": user.id, "reason": REASON}
    if state is DocumentState.removed:
        await db.execute(
            text(
                "UPDATE document_ledger SET state = 'removed', removed_at = now(),"
                " removed_by = :u, removal_reason = :reason WHERE id = :id"
            ),
            params,
        )
    if state in (DocumentState.deleting, DocumentState.deleted):
        await db.execute(
            text(
                "UPDATE document_ledger SET state = 'deleting', deletion_approved_at = now(),"
                " deletion_approved_by = :u WHERE id = :id"
            ),
            params,
        )
    if state is DocumentState.deleted:
        await db.execute(
            text(
                "UPDATE document_ledger SET state = 'deleted', deleted_at = now(),"
                " original_removed_at = now() WHERE id = :id"
            ),
            params,
        )
    await db.commit()
    if stored:
        store.objects[ObjectRef(row.object_key, "v1")] = StoredObject(
            data=b"SAMPLE", mode=mode, retain_until=lock_until
        )
    return await _fresh(db, row)


async def _fresh(db: AsyncSession, row: DocumentLedger) -> DocumentLedger:
    found = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == row.id)
        .execution_options(populate_existing=True)
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


class RecordingStore:
    """The memory store, recording each removal and its mode."""

    def __init__(self, inner: MemoryObjectStore) -> None:
        self.inner = inner
        self.calls: list[tuple[ObjectRef, bool]] = []

    async def remove_original(self, ref: ObjectRef, *, governance: bool) -> None:
        self.calls.append((ref, governance))
        await self.inner.remove_original(ref, governance=governance)


class Purges:
    def __init__(self) -> None:
        self.sent: list[tuple[uuid.UUID, uuid.UUID]] = []

    async def __call__(self, ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        self.sent.append((ledger_id, workspace_id))


async def _remove(row: DocumentLedger, sessions: Any, store: Any, **over: Any) -> bool:
    args: dict[str, Any] = {
        "sessions": sessions,
        "store": store,
        "settings": SETTINGS,
        "now": NOW,
        "workspace_id": row.workspace_id,
        "purge": Purges(),
        **over,
    }
    return await remove_original(row.id, **args)


def _ref(row: DocumentLedger) -> ObjectRef:
    return ObjectRef(row.object_key, row.object_version_id)


# The task's core --------------------------------------------------------------------------


@pytest.mark.db
@pytest.mark.parametrize(
    "state",
    [
        DocumentState.received,
        DocumentState.needs_review,
        DocumentState.filed,
        DocumentState.sealed,
        DocumentState.deleted,
    ],
)
async def test_remove_task_ignores_rows_not_removed_or_deleting(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, state: DocumentState,
) -> None:  # fmt: skip
    """A forged or stale message never deletes: nothing reaches the store, nothing is
    audited, no purge is sent."""
    row = await _ledger(db, memory_store, workspace, user, state)
    store, purge = RecordingStore(memory_store), Purges()
    assert not await _remove(row, sessions, store, purge=purge)
    assert store.calls == []
    assert _ref(row) in memory_store.objects
    assert await _events(db, workspace) == []
    assert purge.sent == []
    assert (await _fresh(db, row)).state is state


@pytest.mark.db
async def test_remove_task_sets_original_removed_and_audits(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    row = await _ledger(db, memory_store, workspace, user, DocumentState.removed)
    store = RecordingStore(memory_store)
    seen: list[datetime | None] = []

    async def purge(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        # Sent after the commit: another session already sees the original gone.
        async with sessions() as other:
            seen.append(
                await other.scalar(
                    select(DocumentLedger.original_removed_at).where(DocumentLedger.id == ledger_id)
                )
            )
        assert (ledger_id, workspace_id) == (row.id, workspace.id)

    assert await _remove(row, sessions, store, purge=purge)
    assert store.calls == [(_ref(row), True)]  # the pre-seal removal bypasses GOVERNANCE
    assert _ref(row) not in memory_store.objects
    assert seen == [NOW]
    fresh = await _fresh(db, row)
    assert (fresh.state, fresh.original_removed_at, fresh.deleted_at) == (
        DocumentState.removed,
        NOW,
        None,
    )
    [event] = await _events(db, workspace)
    assert event.action == "document.original_removed"
    assert event.data == {"mode": "governance"}
    assert (event.target_type, event.target_id) == ("document", str(row.id))
    # Done once: a redelivery is a no-op.
    assert not await _remove(row, sessions, store)
    assert len(store.calls) == 1
    assert len(await _events(db, workspace)) == 1


@pytest.mark.db
async def test_deleting_becomes_deleted_after_store_delete(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    row = await _ledger(
        db, memory_store, workspace, user, DocumentState.deleting,
        mode="COMPLIANCE", lock_until=PAST,
    )  # fmt: skip
    store, purge = RecordingStore(memory_store), Purges()
    assert await _remove(row, sessions, store, purge=purge)
    assert store.calls == [(_ref(row), False)]  # after retention: no bypass
    assert _ref(row) not in memory_store.objects
    fresh = await _fresh(db, row)
    assert (fresh.state, fresh.original_removed_at, fresh.deleted_at) == (
        DocumentState.deleted,
        NOW,
        NOW,
    )
    [event] = await _events(db, workspace)
    assert (event.action, event.data) == ("document.original_removed", {"mode": "expired"})
    assert purge.sent == [(row.id, workspace.id)]


@pytest.mark.db
async def test_unexpired_compliance_refusal_records_nothing(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    row = await _ledger(
        db, memory_store, workspace, user, DocumentState.deleting, mode="COMPLIANCE"
    )
    purge = Purges()
    with pytest.raises(RetentionActive):
        await _remove(row, sessions, memory_store, purge=purge)
    assert _ref(row) in memory_store.objects
    fresh = await _fresh(db, row)
    assert (fresh.state, fresh.original_removed_at) == (DocumentState.deleting, None)
    assert await _events(db, workspace) == []
    assert purge.sent == []


@pytest.mark.db
@pytest.mark.parametrize("first", ["tombstone", "re-upload"])
async def test_two_rows_sharing_a_key_each_remove_only_their_own_version(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, first: str,
) -> None:  # fmt: skip
    """A removed document's bytes archived again (0013): one content key, two versions. Each
    removal deletes its own version id only, whichever runs first."""
    tomb = await _ledger(db, memory_store, workspace, user, DocumentState.removed)
    again = DocumentLedger(
        workspace_id=workspace.id, sha256=tomb.sha256, md5="0" * 32, size_bytes=6,
        content_kind="pdf", source=DocumentSource.upload, object_key=tomb.object_key,
        object_version_id="v2", state=DocumentState.filed, ingest_state=IngestState.consumed,
        retention_until=FAR, ingested_at=NOW,
    )  # fmt: skip
    db.add(again)
    await db.commit()
    memory_store.objects[_ref(again)] = StoredObject(
        data=b"SAMPLE", mode="GOVERNANCE", retain_until=FAR
    )
    await db.execute(
        text(
            "UPDATE document_ledger SET state = 'removed', removed_at = now(),"
            " removed_by = :u, removal_reason = :reason WHERE id = :id"
        ),
        {"id": again.id, "u": user.id, "reason": REASON},
    )
    await db.commit()
    rows = [tomb, await _fresh(db, again)]
    if first == "re-upload":
        rows.reverse()
    store = RecordingStore(memory_store)
    assert await _remove(rows[0], sessions, store)
    assert store.calls == [(_ref(rows[0]), True)]
    assert _ref(rows[1]) in memory_store.objects  # the other version untouched
    assert (await _fresh(db, rows[1])).original_removed_at is None
    assert await _remove(rows[1], sessions, store)
    assert store.calls == [(_ref(rows[0]), True), (_ref(rows[1]), True)]
    assert memory_store.versions(tomb.object_key) == []
    assert [(await _fresh(db, r)).object_version_id for r in (tomb, again)] == ["v1", "v2"]


@pytest.mark.db
async def test_absent_version_counts_as_removed(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    """A crash between the store delete and the commit: the next run finds no version and
    records the removal."""
    row = await _ledger(db, memory_store, workspace, user, DocumentState.removed, stored=False)
    assert await _remove(row, sessions, memory_store)
    assert (await _fresh(db, row)).original_removed_at == NOW
    [event] = await _events(db, workspace)
    assert event.action == "document.original_removed"


@pytest.mark.db
async def test_remove_skips_another_regions_document_and_another_workspace(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    row = await _ledger(db, memory_store, workspace, user, DocumentState.removed)
    other = WorkerSettings.model_construct(region="eu-fra", environment="test")
    assert not await _remove(row, sessions, memory_store, settings=other)
    with pytest.raises(LookupError):
        await _remove(row, sessions, memory_store, workspace_id=uuid.uuid4())
    assert _ref(row) in memory_store.objects
    assert (await _fresh(db, row)).original_removed_at is None
    assert await _events(db, workspace) == []


@pytest.mark.db
async def test_final_failure_audits_removal_failed_and_keeps_the_state(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    row = await _ledger(db, memory_store, workspace, user, DocumentState.deleting)
    assert await record_removal_failed(
        row.id, workspace.id, "retention.locked", sessions=sessions, settings=SETTINGS, now=NOW
    )
    [event] = await _events(db, workspace)
    assert (event.action, event.data) == ("document.removal_failed", {"code": "retention.locked"})
    assert (event.target_type, event.target_id) == ("document", str(row.id))
    assert (await _fresh(db, row)).state is DocumentState.deleting
    # An unknown document (or another workspace's) records nothing.
    assert not await record_removal_failed(
        row.id, uuid.uuid4(), "AccessDenied", sessions=sessions, settings=SETTINGS, now=NOW
    )
    assert len(await _events(db, workspace)) == 1


# The store ---------------------------------------------------------------------------------

STORE_SETTINGS = StorageSettings.model_construct(
    region="ma-cas",
    environment="test",
    objectstore_endpoint=AnyHttpUrl("http://objectstore:9000"),
    objectstore_access_key=SecretStr("sijil-removal"),
    objectstore_secret_key=SecretStr("removal-secret"),
    objectstore_bucket="sijil-originals-ma-cas",
)
REF = ObjectRef("k/abc", "v1")
TARGET = {"Bucket": "sijil-originals-ma-cas", "Key": "k/abc", "VersionId": "v1"}


@pytest.fixture
def stubbed() -> Iterator[tuple[S3ObjectStore, Stubber]]:
    store = S3ObjectStore(STORE_SETTINGS)
    with Stubber(store.client) as stub:
        yield store, stub
        stub.assert_no_pending_responses()


def _retention(stub: Stubber, mode: str, until: datetime) -> None:
    stub.add_response(
        "get_object_retention",
        {"Retention": {"Mode": mode, "RetainUntilDate": until}},
        TARGET,
    )


def _end_of(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=UTC)


@pytest.mark.parametrize("governance", [True, False])
@pytest.mark.parametrize("days", [0, 1, 3650])
async def test_remove_original_refuses_unexpired_compliance(
    stubbed: tuple[S3ObjectStore, Stubber], governance: bool, days: int
) -> None:
    """COMPLIANCE until today or later: refused before any delete is sent (the Stubber fails
    on an unexpected call). The store refuses too (tests/integration/test_removal_key.py)."""
    store, stub = stubbed
    _retention(stub, "COMPLIANCE", _end_of(datetime.now(UTC).date() + timedelta(days=days)))
    with pytest.raises(RetentionActive) as raised:
        await store.remove_original(REF, governance=governance)
    assert isinstance(raised.value, RuntimeError)


async def test_remove_original_after_retention_deletes_without_bypass(
    stubbed: tuple[S3ObjectStore, Stubber],
) -> None:
    store, stub = stubbed
    _retention(stub, "COMPLIANCE", _end_of(datetime.now(UTC).date() - timedelta(days=1)))
    stub.add_response("delete_object", {}, TARGET)  # exactly these params: no bypass flag
    await store.remove_original(REF, governance=False)


async def test_remove_original_before_seal_bypasses_governance(
    stubbed: tuple[S3ObjectStore, Stubber],
) -> None:
    store, stub = stubbed
    _retention(stub, "GOVERNANCE", _end_of(FAR))
    stub.add_response("delete_object", {}, {**TARGET, "BypassGovernanceRetention": True})
    await store.remove_original(REF, governance=True)


@pytest.mark.parametrize(
    ("code", "status", "message"),
    [
        ("NoSuchVersion", 404, ""),
        ("NoSuchKey", 404, ""),
        # RustFS 1.0.4 answers GetObjectRetention on any absent version (deleted, unknown
        # version, unknown key) this way, never 404 (probed; tests/integration/test_removal_key).
        ("InternalError", 500, "Version not found: sijil-originals-ma-cas/k/abc-v1"),
    ],
)
@pytest.mark.parametrize("governance", [True, False])
async def test_an_absent_read_still_sends_the_versioned_delete(
    stubbed: tuple[S3ObjectStore, Stubber], code: str, status: int, message: str,
    governance: bool,
) -> None:  # fmt: skip
    """The read alone never decides that an original is gone (a misread 500, a changed message,
    a wrong version id): the versioned delete is sent anyway and the store's answer decides."""
    store, stub = stubbed
    stub.add_client_error(
        "get_object_retention", code, service_message=message, http_status_code=status
    )
    bypass = {"BypassGovernanceRetention": True} if governance else {}
    stub.add_response("delete_object", {}, {**TARGET, **bypass})
    await store.remove_original(REF, governance=governance)


@pytest.mark.parametrize("code", ["NoSuchVersion", "NoSuchKey"])
async def test_an_absent_delete_counts_as_removed(
    stubbed: tuple[S3ObjectStore, Stubber], code: str
) -> None:
    store, stub = stubbed
    stub.add_client_error("get_object_retention", "NoSuchVersion", http_status_code=404)
    stub.add_client_error("delete_object", code, http_status_code=404)
    await store.remove_original(REF, governance=True)


async def test_a_refused_delete_after_an_absent_read_propagates(
    stubbed: tuple[S3ObjectStore, Stubber],
) -> None:
    store, stub = stubbed
    stub.add_client_error("get_object_retention", "NoSuchVersion", http_status_code=404)
    stub.add_client_error("delete_object", "AccessDenied", http_status_code=403)
    with pytest.raises(ClientError):
        await store.remove_original(REF, governance=False)


async def test_retention_of_an_absent_version_is_none(
    stubbed: tuple[S3ObjectStore, Stubber],
) -> None:
    store, stub = stubbed
    stub.add_client_error(
        "get_object_retention",
        "InternalError",
        service_message="Version not found: sijil-originals-ma-cas/k/abc-v1",
        http_status_code=500,
    )
    assert await store.retention(REF) is None


async def test_any_other_internal_error_still_propagates(
    stubbed: tuple[S3ObjectStore, Stubber],
) -> None:
    store, stub = stubbed
    stub.add_client_error(
        "get_object_retention", "InternalError", service_message="disk", http_status_code=500
    )
    with pytest.raises(ClientError):
        await store.remove_original(REF, governance=True)


async def test_remove_original_propagates_other_store_errors(
    stubbed: tuple[S3ObjectStore, Stubber],
) -> None:
    store, stub = stubbed
    _retention(stub, "GOVERNANCE", _end_of(FAR))
    stub.add_client_error("delete_object", "AccessDenied", http_status_code=403)
    with pytest.raises(ClientError):
        await store.remove_original(REF, governance=True)


# The sweep --------------------------------------------------------------------------------


@pytest.fixture
async def pending(redis: Redis) -> PendingExtractions:
    return PendingExtractions(redis, prefix="remove")


async def _foreign_workspace(db: AsyncSession) -> Workspace:
    org = Organisation(name="F", type=OrgType.business, country="FR", region="eu-fra")
    db.add(org)
    await db.flush()
    ws = Workspace(
        org_id=org.id, name="F", slug=f"f-{uuid.uuid4().hex[:8]}",
        state=WorkspaceState.active, db_name="paperless_f", index_db_name="index_f",
        kms_key_id="kek-f",
    )  # fmt: skip
    db.add(ws)
    await db.commit()
    return ws


@pytest.mark.db
async def test_sweep_resends_unremoved_and_dedupes(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, pending: PendingExtractions, redis: Redis,
) -> None:  # fmt: skip
    removed = await _ledger(db, memory_store, workspace, user, DocumentState.removed)
    deleting = await _ledger(db, memory_store, workspace, user, DocumentState.deleting)
    done = await _ledger(db, memory_store, workspace, user, DocumentState.removed)
    assert await _remove(done, sessions, memory_store)
    for state in (DocumentState.filed, DocumentState.sealed, DocumentState.deleted):
        await _ledger(db, memory_store, workspace, user, state)
    await _ledger(db, memory_store, await _foreign_workspace(db), user, DocumentState.removed)
    # Active workspaces or not: a closing workspace's removal still completes.
    closing = Workspace(
        org_id=workspace.org_id, name="C", slug=f"c-{uuid.uuid4().hex[:8]}",
        state=WorkspaceState.closing, db_name="paperless_c", index_db_name="index_c",
        kms_key_id="kek-c",
    )  # fmt: skip
    db.add(closing)
    await db.commit()
    in_closing = await _ledger(db, memory_store, closing, user, DocumentState.removed)
    sent: list[tuple[uuid.UUID, uuid.UUID]] = []

    async def send(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        sent.append((ledger_id, workspace_id))

    assert await requeue_unremoved(sessions, send, region="ma-cas", pending=pending) == 3
    assert set(sent) == {
        (removed.id, workspace.id),
        (deleting.id, workspace.id),
        (in_closing.id, closing.id),
    }
    for row in (removed, deleting, in_closing):
        assert await redis.exists(f"remove:pending:{row.id}")
        assert 0 < await redis.ttl(f"remove:pending:{row.id}") <= 3600
    # In flight: the next sweep sends nothing again.
    assert await requeue_unremoved(sessions, send, region="ma-cas", pending=pending) == 0
    assert len(sent) == 3


@pytest.mark.db
async def test_sweep_is_capped_and_survives_a_failed_send(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, pending: PendingExtractions,
) -> None:  # fmt: skip
    rows = [
        await _ledger(db, memory_store, workspace, user, DocumentState.removed) for _ in range(4)
    ]
    sent: list[uuid.UUID] = []

    async def send(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        if not sent and ledger_id == rows[0].id:
            sent.append(uuid.UUID(int=0))
            raise ConnectionError("broker down")
        sent.append(ledger_id)

    assert await requeue_unremoved(
        sessions, send, region="ma-cas", pending=pending, limit=2
    ) == 2  # fmt: skip
    assert len([s for s in sent if s.int]) == 2


# The tasks --------------------------------------------------------------------------------

ARGS = {
    "ledger_id": "00000000-0000-0000-0000-000000000001",
    "workspace_id": "00000000-0000-0000-0000-000000000002",
}


def _settings() -> WorkerSettings:
    return WorkerSettings(
        region="ma-cas",
        environment="test",
        redis_url="redis://127.0.0.1:1/0",
        master_key="a" * 43 + "=",
        database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
        provisioner_database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
        tenant_image="img:1",
        compose_project="sijil",
        tessdata_volume="sijil_tessdata",
    )


class RecordingPending:
    """Stands in for the broker Redis key: records what the task does to it."""

    def __init__(self) -> None:
        self.ops: list[str] = []

    async def clear(self, ledger_id: uuid.UUID) -> None:
        self.ops.append("clear")

    async def refresh(self, ledger_id: uuid.UUID, ttl_s: int) -> None:
        self.ops.append(f"refresh {ttl_s}")


@pytest.fixture
def key(monkeypatch: pytest.MonkeyPatch) -> RecordingPending:
    pending = RecordingPending()

    def quietly(op: Any) -> None:
        import asyncio

        asyncio.run(op(pending))

    monkeypatch.setattr(tasks, "_pending_remove_quietly", quietly)
    return pending


@pytest.fixture
def eager_app(key: RecordingPending) -> Any:
    app = make_celery(_settings())
    app.conf.task_always_eager = True
    app.set_current()
    return app


@pytest.fixture
def failures(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str, str]]:
    recorded: list[tuple[str, str, str]] = []

    async def failed(ledger_id: str, workspace_id: str, code: str) -> bool:
        recorded.append((ledger_id, workspace_id, code))
        return True

    monkeypatch.setattr(tasks, "_removal_failed", failed)
    return recorded


def test_remove_task_runs_on_the_removal_queue() -> None:
    assert tasks.remove_original_task.name == "sijil.remove_original"
    assert getattr(tasks.remove_original_task, "queue", None) == "removal"
    assert "removal" in [q.name for q in make_celery(_settings()).conf.task_queues]


def test_beat_sweeps_unremoved_originals_every_five_minutes() -> None:
    entry = make_celery(_settings()).conf.beat_schedule["requeue-unremoved"]
    assert entry == {"task": "sijil.requeue_unremoved", "schedule": 300}


def test_purge_is_sent_by_name_on_default(eager_app: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[tuple[str, dict[str, Any]]] = []

    def send_task(name: str, **options: Any) -> None:
        sent.append((name, options))

    monkeypatch.setattr(eager_app, "send_task", send_task)
    import asyncio

    lid, wid = uuid.uuid4(), uuid.uuid4()
    asyncio.run(tasks._send_purge(lid, wid))
    assert sent == [
        (
            "sijil.purge_document",
            {"kwargs": {"ledger_id": str(lid), "workspace_id": str(wid)}, "queue": "default"},
        )
    ]


ALL_RETRIES = tasks.REMOVAL_MAX_RETRIES + 1


def _client_error(code: str, status: int) -> ClientError:
    response: Any = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}
    return ClientError(response, "DeleteObject")


@pytest.mark.parametrize(
    ("error", "attempts", "code"),
    [
        (_client_error("ServiceUnavailable", 503), ALL_RETRIES, "ServiceUnavailable"),
        (ConnectionRefusedError("store down"), ALL_RETRIES, "ConnectionRefusedError"),
        (_client_error("AccessDenied", 403), 1, "AccessDenied"),
        (RetentionActive("k/abc@v1"), 1, "retention.locked"),
    ],
)
def test_remove_task_retries_transient_store_errors_then_audits_the_failure(
    eager_app: Any, key: RecordingPending, failures: list[tuple[str, str, str]],
    monkeypatch: pytest.MonkeyPatch, error: Exception, attempts: int, code: str,
) -> None:  # fmt: skip
    calls: list[str] = []

    async def remove(ledger_id: str, workspace_id: str) -> None:
        calls.append(ledger_id)
        raise error

    monkeypatch.setattr(tasks, "_remove_original", remove)
    tasks.remove_original_task.apply(kwargs=ARGS)
    assert len(calls) == attempts
    assert failures == [(ARGS["ledger_id"], ARGS["workspace_id"], code)]
    # The state stays as it is, so the sweep would find the row again: its key is kept for
    # the full TTL (1 h), never cleared, so it is re-sent at most hourly.
    assert "clear" not in key.ops
    assert key.ops[-1] == "refresh 3600"


def test_an_unrecorded_failure_keeps_its_key_too(
    eager_app: Any, key: RecordingPending, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def remove(ledger_id: str, workspace_id: str) -> None:
        raise _client_error("AccessDenied", 403)

    async def failed(ledger_id: str, workspace_id: str, code: str) -> bool:
        raise ConnectionError("database down")

    monkeypatch.setattr(tasks, "_remove_original", remove)
    monkeypatch.setattr(tasks, "_removal_failed", failed)
    tasks.remove_original_task.apply(kwargs=ARGS)
    assert "clear" not in key.ops


def test_a_done_or_skipped_removal_clears_its_key(
    eager_app: Any, key: RecordingPending, failures: list[tuple[str, str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    async def remove(ledger_id: str, workspace_id: str) -> None:
        return None

    monkeypatch.setattr(tasks, "_remove_original", remove)
    tasks.remove_original_task.apply(kwargs=ARGS)
    assert key.ops == ["clear"]
    assert failures == []


def test_removal_settings_hold_no_master_key_nor_provisioner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name, value in {
        "SIJIL_REGION": "ma-cas",
        "SIJIL_ENVIRONMENT": "test",
        "SIJIL_REDIS_URL": "redis://redis:6379/0",
        "SIJIL_DATABASE_URL": "postgresql+asyncpg://app:x@postgres/sijil_cp",
        "SIJIL_OBJECTSTORE_ENDPOINT": "http://objectstore:9000",
        "SIJIL_OBJECTSTORE_ACCESS_KEY": "sijil-removal",
        "SIJIL_OBJECTSTORE_SECRET_KEY": "removal-secret",
        "SIJIL_OBJECTSTORE_BUCKET": "sijil-originals-ma-cas",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("SIJIL_MASTER_KEY", raising=False)
    settings = RemovalSettings()
    assert settings.objectstore_access_key.get_secret_value() == "sijil-removal"
    assert not hasattr(settings, "master_key")
    assert not hasattr(settings, "provisioner_database_url")
