"""`lock_sealed`: the seal worker switches a sealed original to COMPLIANCE, after the commit.

The store is the in-memory double (RustFS semantics); the database is the real test one.
"""

import hashlib
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError
from redis.asyncio import Redis
from sqlalchemy import select
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
from sijil_core.storage import ObjectRef
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.lock import lock_sealed, requeue_unlocked
from sijil_worker.pending import PendingExtractions
from sijil_worker.settings import SealSettings, WorkerSettings
from tests.support.memory_store import MemoryObjectStore, StoredObject

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
FINAL = date(2036, 12, 31)
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


async def _ledger(
    db: AsyncSession,
    store: MemoryObjectStore,
    ws: Workspace,
    user: User,
    *,
    sealed: bool = True,
    sealed_at: datetime = NOW - timedelta(minutes=10),
    retention: date = FINAL,
) -> DocumentLedger:
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
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
        retention_until=retention,
        ingested_at=NOW - timedelta(days=1),
        fields_sha256="f" * 64 if sealed else None,
        sealed_at=sealed_at if sealed else None,
        sealed_by=user.id if sealed else None,
    )
    db.add(row)
    await db.commit()
    # The provisional GOVERNANCE lock the original got at arrival.
    store.objects[ObjectRef(row.object_key, "v1")] = StoredObject(
        data=b"SAMPLE", mode="GOVERNANCE", retain_until=date(2036, 12, 31)
    )
    return row


def _stored(store: MemoryObjectStore, row: DocumentLedger) -> StoredObject:
    return store.objects[ObjectRef(row.object_key, row.object_version_id)]


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


async def _lock(row: DocumentLedger, sessions: Any, store: Any, **over: Any) -> bool:
    return await lock_sealed(
        row.id,
        **{"sessions": sessions, "store": store, "settings": SETTINGS, "now": NOW, **over},
    )


@pytest.mark.db
async def test_lock_switches_to_compliance_at_the_committed_retention(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    row = await _ledger(db, memory_store, workspace, user, retention=date(2037, 12, 31))
    assert await _lock(row, sessions, memory_store, workspace_id=workspace.id)
    assert (_stored(memory_store, row).mode, _stored(memory_store, row).retain_until) == (
        "COMPLIANCE",
        date(2037, 12, 31),  # the ledger's committed value, never recomputed
    )
    locked = await _fresh(db, row)
    assert locked.object_locked_at == NOW
    [event] = await _events(db, workspace)
    assert event.action == "document.object_locked"
    assert event.data == {"retention_until": "2037-12-31"}
    assert (event.target_type, event.target_id) == ("document", str(row.id))


@pytest.mark.db
async def test_lock_is_a_noop_once_locked_or_unless_sealed(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    row = await _ledger(db, memory_store, workspace, user)
    assert await _lock(row, sessions, memory_store)
    assert not await _lock(row, sessions, memory_store)  # already locked: nothing again
    assert len(await _events(db, workspace)) == 1
    unsealed = await _ledger(db, memory_store, workspace, user, sealed=False)
    assert not await _lock(unsealed, sessions, memory_store)
    assert _stored(memory_store, unsealed).mode == "GOVERNANCE"


@pytest.mark.db
async def test_lock_skips_another_regions_document(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    row = await _ledger(db, memory_store, workspace, user)
    other = WorkerSettings.model_construct(region="eu-fra", environment="test")
    assert not await _lock(row, sessions, memory_store, settings=other)
    assert _stored(memory_store, row).mode == "GOVERNANCE"
    assert (await _fresh(db, row)).object_locked_at is None


@pytest.mark.db
async def test_lock_of_another_workspace_is_refused(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    row = await _ledger(db, memory_store, workspace, user)
    with pytest.raises(LookupError):
        await _lock(row, sessions, memory_store, workspace_id=uuid.uuid4())
    assert _stored(memory_store, row).mode == "GOVERNANCE"


class FailingStore:
    def __init__(self, error: Exception) -> None:
        self.error = error

    async def seal(self, ref: ObjectRef, retain_until: date) -> None:
        raise self.error


@pytest.mark.db
async def test_store_failure_records_nothing(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    row = await _ledger(db, memory_store, workspace, user)
    with pytest.raises(ConnectionError):
        await _lock(row, sessions, FailingStore(ConnectionError("store down")))
    assert (await _fresh(db, row)).object_locked_at is None
    assert await _events(db, workspace) == []


# The sweep --------------------------------------------------------------------------------


@pytest.fixture
async def pending(redis: Redis) -> PendingExtractions:
    return PendingExtractions(redis, prefix="lock")


@pytest.mark.db
async def test_requeue_unlocked_sends_old_unlocked_sealed_rows_of_this_region_once(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, pending: PendingExtractions, redis: Redis,
) -> None:  # fmt: skip
    due = await _ledger(db, memory_store, workspace, user, sealed_at=NOW - timedelta(minutes=5))
    await _ledger(db, memory_store, workspace, user, sealed_at=NOW - timedelta(seconds=30))
    await _ledger(db, memory_store, workspace, user, sealed=False)
    locked = await _ledger(db, memory_store, workspace, user)
    assert await _lock(locked, sessions, memory_store)
    foreign_org = Organisation(name="F", type=OrgType.business, country="FR", region="eu-fra")
    db.add(foreign_org)
    await db.flush()
    foreign = Workspace(
        org_id=foreign_org.id, name="F", slug=f"f-{uuid.uuid4().hex[:8]}",
        state=WorkspaceState.active, db_name="paperless_f", index_db_name="index_f",
        kms_key_id="kek-f",
    )  # fmt: skip
    db.add(foreign)
    await db.commit()
    await _ledger(db, memory_store, foreign, user)
    sent: list[tuple[uuid.UUID, uuid.UUID]] = []

    async def send(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        sent.append((ledger_id, workspace_id))

    assert await requeue_unlocked(sessions, send, region="ma-cas", pending=pending, now=NOW) == 1
    assert sent == [(due.id, workspace.id)]
    assert await redis.exists(f"lock:pending:{due.id}")
    # In flight: the next sweep does not send it again.
    assert await requeue_unlocked(sessions, send, region="ma-cas", pending=pending, now=NOW) == 0
    assert len(sent) == 1


@pytest.mark.db
async def test_requeue_unlocked_caps_each_run(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, pending: PendingExtractions,
) -> None:  # fmt: skip
    for _ in range(3):
        await _ledger(db, memory_store, workspace, user)
    sent: list[uuid.UUID] = []

    async def send(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        sent.append(ledger_id)

    count = await requeue_unlocked(
        sessions, send, region="ma-cas", pending=pending, now=NOW, limit=2
    )
    assert count == 2 == len(sent)


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


@pytest.fixture
def eager_app(monkeypatch: pytest.MonkeyPatch) -> None:
    app = make_celery(_settings())
    app.conf.task_always_eager = True
    app.set_current()
    monkeypatch.setattr(tasks, "_pending_lock_quietly", lambda op: None)


def test_lock_task_runs_on_the_seal_queue() -> None:
    assert tasks.lock_sealed_task.name == "sijil.lock_sealed"
    assert getattr(tasks.lock_sealed_task, "queue", None) == "seal"


def test_beat_sweeps_unlocked_seals_every_five_minutes() -> None:
    entry = make_celery(_settings()).conf.beat_schedule["requeue-unlocked"]
    assert entry == {"task": "sijil.requeue_unlocked", "schedule": 300}
    assert "seal" in [q.name for q in make_celery(_settings()).conf.task_queues]


def _client_error(code: str, status: int) -> ClientError:
    response: Any = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}
    return ClientError(response, "PutObjectRetention")


@pytest.mark.parametrize(
    ("error", "attempts", "raises"),
    [
        (_client_error("ServiceUnavailable", 503), tasks.LOCK_MAX_RETRIES + 1, True),
        (ConnectionRefusedError("store down"), tasks.LOCK_MAX_RETRIES + 1, True),
        # Permanent: logged by its code and left to the sweep and the drift check.
        (_client_error("AccessDenied", 403), 1, False),
    ],
)
def test_lock_task_retries_transient_store_errors_only(
    eager_app: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    error: Exception, attempts: int, raises: bool,
) -> None:  # fmt: skip
    calls: list[str] = []

    async def lock(ledger_id: str, workspace_id: str) -> None:
        calls.append(ledger_id)
        raise error

    monkeypatch.setattr(tasks, "_lock_sealed", lock)
    result = tasks.lock_sealed_task.apply(kwargs=ARGS)
    assert len(calls) == attempts
    assert isinstance(result.result, type(error)) is raises
    if not raises:
        assert "AccessDenied" in caplog.text


def test_seal_settings_hold_no_master_key_nor_provisioner(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in {
        "SIJIL_REGION": "ma-cas",
        "SIJIL_ENVIRONMENT": "test",
        "SIJIL_REDIS_URL": "redis://redis:6379/0",
        "SIJIL_DATABASE_URL": "postgresql+asyncpg://app:x@postgres/sijil_cp",
        "SIJIL_OBJECTSTORE_ENDPOINT": "http://objectstore:9000",
        "SIJIL_OBJECTSTORE_ACCESS_KEY": "sijil-seal",
        "SIJIL_OBJECTSTORE_SECRET_KEY": "seal-secret",
        "SIJIL_OBJECTSTORE_BUCKET": "sijil-originals-ma-cas",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("SIJIL_MASTER_KEY", raising=False)
    settings = SealSettings()
    assert settings.objectstore_access_key.get_secret_value() == "sijil-seal"
    assert not hasattr(settings, "master_key")
    assert not hasattr(settings, "provisioner_database_url")


class RecordingPending:
    """Stands in for the broker Redis key: records what the task does to it."""

    def __init__(self) -> None:
        self.ops: list[str] = []

    async def clear(self, ledger_id: uuid.UUID) -> None:
        self.ops.append("clear")

    async def refresh(self, ledger_id: uuid.UUID, ttl_s: int) -> None:
        self.ops.append("refresh")


@pytest.mark.parametrize(
    ("error", "cleared"),
    [
        (None, True),  # locked: the key goes
        (_client_error("ServiceUnavailable", 503), True),  # retries ran out: the sweep re-sends
        # Permanent: the key stays until its TTL (1 h), so the row is not re-sent at every sweep
        # and can never hold the head of the sweep against newer rows.
        (_client_error("AccessDenied", 403), False),
    ],
)
def test_a_permanent_lock_failure_keeps_its_pending_key(
    eager_app: None, monkeypatch: pytest.MonkeyPatch, error: Exception | None, cleared: bool
) -> None:
    pending = RecordingPending()

    def quietly(op: Any) -> None:
        import asyncio

        asyncio.run(op(pending))

    async def lock(ledger_id: str, workspace_id: str) -> None:
        if error is not None:
            raise error

    monkeypatch.setattr(tasks, "_pending_lock_quietly", quietly)
    monkeypatch.setattr(tasks, "_lock_sealed", lock)
    tasks.lock_sealed_task.apply(kwargs=ARGS)
    assert ("clear" in pending.ops) is cleared


@pytest.mark.db
async def test_permanently_failing_rows_never_starve_the_sweep(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User,
    memory_store: MemoryObjectStore, pending: PendingExtractions, redis: Redis,
) -> None:  # fmt: skip
    """60 old sealed rows whose lock keeps failing permanently (their keys left in place) and
    one newer row whose first message was lost: the next sweep sends the newer row."""
    failing = [
        await _ledger(db, memory_store, workspace, user, sealed_at=NOW - timedelta(hours=1))
        for _ in range(60)
    ]
    for row in failing:
        assert await pending.claim(row.id)  # what their (failed) runs left behind
    newer = await _ledger(db, memory_store, workspace, user, sealed_at=NOW - timedelta(minutes=5))
    sent: list[uuid.UUID] = []

    async def send(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        sent.append(ledger_id)

    assert await requeue_unlocked(sessions, send, region="ma-cas", pending=pending, now=NOW) == 1
    assert sent == [newer.id]
