"""`purge_document`: once the original is removed, the Paperless copy and the index rows go too.

Paperless is respx (recording every call, with the credential it carried); the index a
throw-away database behind a real `IndexEngines`; the ledger the real test database.
"""

import asyncio
import hashlib
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from redis.asyncio import Redis
from sqlalchemy import select, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sijil_core import index
from sijil_core.audit import append
from sijil_core.crypto import LocalKeyService, seal
from sijil_core.db.models import (
    ActorType,
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    TenantCredential,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.fields import FIELD_NAMES, fields_digest
from sijil_core.index import FieldsRow, IndexEngines
from sijil_core.paperless import PaperlessClient, PaperlessForbidden, PaperlessUnavailable
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.ingest import WorkspaceNotActive
from sijil_worker.pending import PendingExtractions
from sijil_worker.purge import (
    ConsumeInFlight,
    PurgeIncomplete,
    purge_document,
    requeue_unpurged,
)
from sijil_worker.settings import WorkerSettings
from tests.support.fake_paperless import FakePaperless, fake_paperless
from tests.support.index_db import throwaway_index_db

PAPERLESS = "http://paperless-ws:8000"
SERVICE_TOKEN = "svc-" + "s" * 36
ADMIN_PASSWORD = "admin-" + "p" * 26
ADMIN_TOKEN = "adm-" + "a" * 36
DOC_ID = 7
NOW = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
FAR = date(2036, 12, 31)
PAST = date(2020, 12, 31)
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")
REASON = "Doublon déposé par erreur (SAMPLE)"
# SAMPLE values (fictitious supplier).
VALUES: dict[str, str | None] = dict.fromkeys(FIELD_NAMES) | {
    "document_type": "invoice",
    "supplier_name": "Atlas Télécom SAMPLE SARL",
    "supplier_ice": "001905837000041",
    "number": "F-2026-01001",
    "total_ttc": "1500.00",
}

pytestmark = pytest.mark.db


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sijil_core.paperless._BACKOFF_S", 0)


@pytest.fixture
def keys() -> LocalKeyService:
    return LocalKeyService(b"k" * 32)


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


@pytest.fixture(scope="module")
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


def _credential(ws: Workspace, keys: LocalKeyService, kind: CredentialKind, secret: str) -> Any:
    envelope = seal(secret.encode(), keys, ws.kms_key_id, f"{ws.id}:{kind}")
    return TenantCredential(workspace_id=ws.id, kind=kind, envelope=envelope.to_bytes())


@pytest.fixture
async def ready(db: AsyncSession, workspace: Workspace, keys: LocalKeyService) -> Workspace:
    workspace.paperless_url = PAPERLESS
    db.add(_credential(workspace, keys, CredentialKind.service_token, SERVICE_TOKEN))
    db.add(_credential(workspace, keys, CredentialKind.admin_password, ADMIN_PASSWORD))
    db.add(_credential(workspace, keys, CredentialKind.index_db_password, "pw"))
    await db.commit()
    return workspace


@pytest.fixture
async def engines(
    index_db_url: str, keys: LocalKeyService, sessions: async_sessionmaker[AsyncSession]
) -> AsyncIterator[IndexEngines]:
    def factory(_url: URL) -> AsyncEngine:
        return create_async_engine(index_db_url)

    found = IndexEngines(SETTINGS, keys, sessions, engine_factory=factory)
    yield found
    await found.aclose()


async def _document(
    db: AsyncSession,
    engines: IndexEngines,
    ws: Workspace,
    user: User,
    state: DocumentState,
    *,
    original_removed: bool = True,
    paperless_doc_id: int | None = DOC_ID,
    ingest_state: IngestState = IngestState.consumed,
    task_id: str | None = None,
    claimed_at: datetime | None = None,
    next_attempt_at: datetime | None = None,
) -> DocumentLedger:
    """A row brought to `state` through the transitions 0011 allows, with its index row
    (sealed for a sealed or deleted document). The ingest columns say how far ingest got."""
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    sealed = state in (DocumentState.sealed, DocumentState.deleted)
    row = DocumentLedger(
        workspace_id=ws.id, sha256=sha, md5="0" * 32, size_bytes=10, content_kind="pdf",
        source=DocumentSource.upload, object_key=f"k/{sha}", object_version_id="v1",
        state=DocumentState.sealed if sealed else DocumentState.filed,
        ingest_state=ingest_state, paperless_doc_id=paperless_doc_id,
        paperless_task_id=task_id, ingest_claimed_at=claimed_at,
        ingest_next_attempt_at=next_attempt_at,
        retention_until=PAST if state is DocumentState.deleted else FAR,
        ingested_at=NOW - timedelta(days=2),
        fields_sha256=fields_digest(VALUES) if sealed else None,
        sealed_at=NOW - timedelta(days=1) if sealed else None,
        sealed_by=user.id if sealed else None,
    )  # fmt: skip
    db.add(row)
    await db.commit()
    async with (await engines.engine(ws)).begin() as conn:
        await index.write_extraction(
            conn,
            FieldsRow(
                ledger_id=row.id, document_type="invoice", values=VALUES,
                confidence=dict.fromkeys(FIELD_NAMES, 0.9), signals={}, flags=(),
                source=dict.fromkeys(FIELD_NAMES, "extraction"), model="qwen3:4b",
                extracted_at=NOW, updated_at=NOW,
            ),
        )  # fmt: skip
        if sealed:
            await index.seal_fields(
                conn, row.id, fields_sha256=fields_digest(VALUES), sealed_at=NOW
            )
    params = {"id": row.id, "u": user.id, "reason": REASON}
    if state is DocumentState.removed:
        await db.execute(
            text(
                "UPDATE document_ledger SET state = 'removed', removed_at = now(),"
                " removed_by = :u, removal_reason = :reason WHERE id = :id"
            ),
            params,
        )
        if original_removed:
            await db.execute(
                text("UPDATE document_ledger SET original_removed_at = now() WHERE id = :id"),
                params,
            )
    if state is DocumentState.deleted:
        await db.execute(
            text(
                "UPDATE document_ledger SET state = 'deleting', deletion_approved_at = now(),"
                " deletion_approved_by = :u WHERE id = :id"
            ),
            params,
        )
        await db.execute(
            text(
                "UPDATE document_ledger SET state = 'deleted', deleted_at = now(),"
                " original_removed_at = now() WHERE id = :id"
            ),
            params,
        )
    await db.commit()
    return row


async def _events(db: AsyncSession, ws: Workspace) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id)
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def _index_row(engines: IndexEngines, ws: Workspace, ledger_id: uuid.UUID) -> Any:
    async with (await engines.engine(ws)).connect() as conn:
        return await index.read_fields(conn, ledger_id)


def paperless(base_url: str, **kw: Any) -> PaperlessClient:
    return PaperlessClient(base_url, **kw)


async def _purge(row: DocumentLedger, sessions: Any, keys: Any, engines: Any, **over: Any) -> bool:
    args: dict[str, Any] = {
        "sessions": sessions,
        "keys": keys,
        "index": engines,
        "paperless": paperless,
        "settings": SETTINGS,
        "now": NOW,
        "workspace_id": row.workspace_id,
        **over,
    }
    return await purge_document(row.id, **args)


TASK_ID = "11111111-2222-3333-4444-555555555555"


@pytest.fixture
def fake() -> Iterator[FakePaperless]:
    with fake_paperless(PAPERLESS, ADMIN_PASSWORD, ADMIN_TOKEN) as found:
        yield found


@pytest.mark.parametrize("state", [DocumentState.removed, DocumentState.deleted])
async def test_purge_deletes_in_paperless_with_admin_and_empties_trash(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, state: DocumentState, fake: FakePaperless,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user, state)
    fake.add(DOC_ID, row.sha256)
    assert await _purge(row, sessions, keys, engines)
    # A token for the tenant admin, then everything with it: the service token has no delete
    # right and is never used here.
    assert fake.calls[0] == (
        "POST", "/api/token/", None, {"username": "sijil-admin", "password": ADMIN_PASSWORD}
    )  # fmt: skip
    assert {auth for _, _, auth, _ in fake.calls[1:]} == {f"Token {ADMIN_TOKEN}"}
    assert ("DELETE", f"/api/documents/{DOC_ID}/") in fake.methods()
    assert (
        "POST",
        "/api/trash/",
        f"Token {ADMIN_TOKEN}",
        {"documents": [DOC_ID], "action": "empty"},
    ) in fake.calls  # noqa: E501
    assert (fake.live, fake.trash) == ({}, {})  # Paperless keeps no copy
    # The index rows are gone, a sealed one too for an approved deletion.
    assert await _index_row(engines, ready, row.id) is None
    [event] = [e for e in await _events(db, ready) if e.action == "document.purged"]
    assert (event.target_type, event.target_id, event.actor_type) == (
        "document", str(row.id), ActorType.system
    )  # fmt: skip
    assert event.data == {"paperless": True}


@pytest.mark.parametrize(
    ("state", "original_removed"),
    [
        (DocumentState.removed, False),
        (DocumentState.filed, False),
        (DocumentState.sealed, False),
    ],
)
async def test_purge_refuses_unless_original_removed(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, state: DocumentState, original_removed: bool, fake: FakePaperless,
) -> None:  # fmt: skip
    """Only once the ledger says the original is gone: a forged or early message purges
    nothing, so Paperless is never called and the index row stays."""
    row = await _document(db, engines, ready, user, state, original_removed=original_removed)
    fake.add(DOC_ID, row.sha256)
    assert not await _purge(row, sessions, keys, engines)
    assert fake.calls == []
    assert await _index_row(engines, ready, row.id) is not None
    assert [e.action for e in await _events(db, ready)] == []


async def test_an_absent_paperless_document_counts_as_deleted(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    """A second run (or a document deleted and emptied in Paperless already): 404 on the
    delete, nothing of it in the trash."""
    row = await _document(db, engines, ready, user, DocumentState.removed)
    assert await _purge(row, sessions, keys, engines)
    assert ("DELETE", f"/api/documents/{DOC_ID}/") in fake.methods()
    assert await _index_row(engines, ready, row.id) is None
    [event] = [e for e in await _events(db, ready) if e.action == "document.purged"]
    assert event.data == {"paperless": False}


async def test_a_document_never_recorded_in_paperless_is_found_by_its_checksum(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    """Ingest stopped on the removal after Paperless had consumed the file, before the ledger
    recorded its id: the copy is found by its SHA-256."""
    row = await _document(db, engines, ready, user, DocumentState.removed, paperless_doc_id=None)
    fake.add(DOC_ID, row.sha256)
    assert await _purge(row, sessions, keys, engines)
    assert (fake.live, fake.trash) == ({}, {})


async def test_every_checksum_match_is_deleted_across_pages(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, fake: FakePaperless, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    """Paperless 3.4.1 does not refuse duplicates: every copy with the SHA-256 goes, however many
    pages the lookup takes."""
    monkeypatch.setattr("sijil_core.paperless.PAGE_SIZE", 2)
    row = await _document(db, engines, ready, user, DocumentState.removed)
    for doc_id in range(DOC_ID, DOC_ID + 5):
        fake.add(doc_id, row.sha256, name=f"copy-{doc_id}.pdf")  # some uploaded by hand
    fake.add(99, hashlib.sha256(b"another").hexdigest())  # another document stays
    assert await _purge(row, sessions, keys, engines)
    assert list(fake.live) == [99]
    assert fake.trash == {}


async def test_a_trashed_copy_is_found_by_its_file_name_and_emptied(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    """A copy deleted already (a run whose `empty` failed, or a person in Paperless) is in the
    trash, where the checksum lookup does not see it: ingest's file name `{sha256[:12]}.{ext}`
    finds it. Someone else's trashed document stays."""
    row = await _document(db, engines, ready, user, DocumentState.removed, paperless_doc_id=None)
    fake.add(DOC_ID, row.sha256, trashed=True)
    fake.add(99, hashlib.sha256(b"another").hexdigest(), trashed=True)
    assert await _purge(row, sessions, keys, engines)
    assert list(fake.trash) == [99]
    [event] = [e for e in await _events(db, ready) if e.action == "document.purged"]
    assert event.data == {"paperless": True}


async def test_a_copy_left_in_the_trash_is_not_recorded_purged(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    """`document.purged` only once the trash holds none of it: otherwise a transient error, so
    the task retries and the sweep keeps the row."""
    row = await _document(db, engines, ready, user, DocumentState.removed)
    fake.add(DOC_ID, row.sha256)
    fake.keep_in_trash = True
    with pytest.raises(PurgeIncomplete):
        await _purge(row, sessions, keys, engines)
    assert await _index_row(engines, ready, row.id) is not None
    assert [e.action for e in await _events(db, ready)] == []
    assert tasks.is_purge_transient(PurgeIncomplete())


async def test_nothing_in_paperless_still_purges_the_index(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user, DocumentState.removed, paperless_doc_id=None)
    assert await _purge(row, sessions, keys, engines)
    assert await _index_row(engines, ready, row.id) is None
    [event] = [e for e in await _events(db, ready) if e.action == "document.purged"]
    assert event.data == {"paperless": False}


@pytest.mark.parametrize(
    ("ingest", "task"),
    [
        # A consume task Paperless still runs, however old the claim.
        (
            {"ingest_state": IngestState.sending, "task_id": TASK_ID,
             "claimed_at": NOW - timedelta(hours=1)},
            {"task_id": TASK_ID, "status": "started"},
        ),
        # Uploaded, then stopped before its task id was recorded: a claim within the window.
        ({"ingest_state": IngestState.sending, "claimed_at": NOW - timedelta(minutes=2)}, None),
        # A failure after the upload, a Celery retry pending: within the window too.
        ({"ingest_state": IngestState.queued, "next_attempt_at": NOW + timedelta(minutes=1)}, None),
    ],
)  # fmt: skip
async def test_an_upload_still_consuming_is_checked_again_later(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, fake: FakePaperless, ingest: dict[str, Any], task: dict[str, Any] | None,
) -> None:  # fmt: skip
    """No Paperless id recorded and an upload that may still be consuming: the copy may appear
    after a purge that found nothing, so nothing is deleted or recorded yet."""
    row = await _document(
        db, engines, ready, user, DocumentState.removed, paperless_doc_id=None, **ingest
    )
    if task is not None:
        fake.tasks[TASK_ID] = task
    with pytest.raises(ConsumeInFlight):
        await _purge(row, sessions, keys, engines)
    assert not any(method == "DELETE" for method, _ in fake.methods())
    assert await _index_row(engines, ready, row.id) is not None
    assert [e.action for e in await _events(db, ready)] == []


async def test_after_the_consume_window_the_checksum_decides(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    row = await _document(
        db, engines, ready, user, DocumentState.removed, paperless_doc_id=None,
        ingest_state=IngestState.sending, claimed_at=NOW - timedelta(minutes=11),
    )  # fmt: skip
    fake.add(DOC_ID, row.sha256)
    assert await _purge(row, sessions, keys, engines)
    assert (fake.live, fake.trash) == ({}, {})


async def test_a_finished_task_names_the_copy_the_ledger_never_recorded(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    row = await _document(
        db, engines, ready, user, DocumentState.removed, paperless_doc_id=None,
        ingest_state=IngestState.sending, task_id=TASK_ID, claimed_at=NOW - timedelta(minutes=1),
    )  # fmt: skip
    fake.tasks[TASK_ID] = {"task_id": TASK_ID, "status": "success",
                           "related_document_ids": [DOC_ID]}  # fmt: skip
    fake.add(DOC_ID, "0" * 64, name="adopted.pdf")  # not findable by checksum or name
    assert await _purge(row, sessions, keys, engines)
    assert (fake.live, fake.trash) == ({}, {})


@pytest.mark.parametrize("state", [WorkspaceState.hibernated, WorkspaceState.closing])
async def test_a_workspace_not_active_is_retried_later(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, state: WorkspaceState, fake: FakePaperless,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user, DocumentState.removed)
    ready.state = state
    await db.commit()
    with pytest.raises(WorkspaceNotActive):
        await _purge(row, sessions, keys, engines)
    assert fake.calls == []
    assert await _index_row(engines, ready, row.id) is not None


async def test_paperless_down_raises_and_keeps_the_index_row(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    """The index row is purged only after Paperless: a retry finds everything to do again."""
    row = await _document(db, engines, ready, user, DocumentState.removed)
    fake.down = True
    with pytest.raises(PaperlessUnavailable):
        await _purge(row, sessions, keys, engines)
    assert await _index_row(engines, ready, row.id) is not None
    assert [e.action for e in await _events(db, ready)] == []


async def test_purge_skips_another_workspace_and_another_region(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ready: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user, DocumentState.removed)
    with pytest.raises(LookupError):
        await _purge(row, sessions, keys, engines, workspace_id=uuid.uuid4())
    elsewhere = WorkerSettings.model_construct(region="fr-par", environment="test")
    assert not await _purge(row, sessions, keys, engines, settings=elsewhere)
    assert fake.calls == []


# The sweep --------------------------------------------------------------------------------


@pytest.fixture
async def pending(redis: Redis) -> PendingExtractions:
    return PendingExtractions(redis, prefix="purge")


async def test_sweep_resends_unpurged_and_dedupes(
    db: AsyncSession, sessions: Any, engines: IndexEngines, ready: Workspace, user: User,
    pending: PendingExtractions, redis: Redis,
) -> None:  # fmt: skip
    removed = await _document(db, engines, ready, user, DocumentState.removed)
    deleted = await _document(db, engines, ready, user, DocumentState.deleted)
    # Not yet: the original is still there; or done: `document.purged` is recorded.
    await _document(db, engines, ready, user, DocumentState.removed, original_removed=False)
    await _document(db, engines, ready, user, DocumentState.filed)
    done = await _document(db, engines, ready, user, DocumentState.removed)
    await append(
        db, workspace_id=ready.id, action="document.purged", actor_type=ActorType.system,
        actor_id=None, target_type="document", target_id=str(done.id),
        data={"paperless": True}, now=NOW,
    )  # fmt: skip
    await db.commit()
    sent: list[tuple[uuid.UUID, uuid.UUID]] = []

    async def send(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        sent.append((ledger_id, workspace_id))

    assert await requeue_unpurged(sessions, send, region="ma-cas", pending=pending) == 2
    assert set(sent) == {(removed.id, ready.id), (deleted.id, ready.id)}
    for row in (removed, deleted):
        assert 0 < await redis.ttl(f"purge:pending:{row.id}") <= 3600
    # In flight: the next sweep sends nothing again.
    assert await requeue_unpurged(sessions, send, region="ma-cas", pending=pending) == 0
    # Another region's rows are its own worker's.
    assert await requeue_unpurged(sessions, send, region="fr-par", pending=pending) == 0
    assert len(sent) == 2


async def test_sweep_skips_inactive_workspaces_and_is_capped(
    db: AsyncSession, sessions: Any, engines: IndexEngines, ready: Workspace, user: User,
    pending: PendingExtractions,
) -> None:  # fmt: skip
    rows = [await _document(db, engines, ready, user, DocumentState.removed) for _ in range(3)]
    sent: list[uuid.UUID] = []

    async def send(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        if not sent:
            sent.append(uuid.UUID(int=0))
            raise ConnectionError("broker down")
        sent.append(ledger_id)

    ready.state = WorkspaceState.hibernated
    await db.commit()
    assert await requeue_unpurged(sessions, send, region="ma-cas", pending=pending) == 0
    ready.state = WorkspaceState.active
    await db.commit()
    # The failed send releases its claim and the sweep goes on, up to `limit`.
    assert await requeue_unpurged(sessions, send, region="ma-cas", pending=pending, limit=1) == 1
    assert len([s for s in sent if s.int]) == 1
    assert {s for s in sent if s.int} <= {r.id for r in rows}


# The task ---------------------------------------------------------------------------------

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
    def __init__(self) -> None:
        self.ops: list[str] = []

    async def clear(self, ledger_id: uuid.UUID) -> None:
        self.ops.append("clear")

    async def refresh(self, ledger_id: uuid.UUID, ttl_s: int) -> None:
        self.ops.append(f"refresh {ttl_s}")


@pytest.fixture
def key(monkeypatch: pytest.MonkeyPatch) -> RecordingPending:
    recorded = RecordingPending()

    def quietly(op: Any) -> None:
        asyncio.run(op(recorded))

    monkeypatch.setattr(tasks, "_pending_purge_quietly", quietly)
    return recorded


@pytest.fixture
def eager_app(key: RecordingPending) -> Any:
    app = make_celery(_settings())
    app.conf.task_always_eager = True
    app.set_current()
    return app


def test_purge_task_runs_on_default() -> None:
    assert tasks.purge_document_task.name == "sijil.purge_document"
    assert getattr(tasks.purge_document_task, "queue", None) == "default"


def test_beat_sweeps_unpurged_documents_every_five_minutes() -> None:
    entry = make_celery(_settings()).conf.beat_schedule["requeue-unpurged"]
    assert entry == {"task": "sijil.requeue_unpurged", "schedule": 300}
    assert tasks.requeue_unpurged_task.name == "sijil.requeue_unpurged"


@pytest.mark.parametrize(
    ("error", "attempts"),
    [
        (WorkspaceNotActive(), tasks.PURGE_MAX_RETRIES + 1),
        (PaperlessUnavailable("x"), tasks.PURGE_MAX_RETRIES + 1),
    ],
)
def test_purge_task_retries_transient_failures(
    eager_app: Any, key: RecordingPending, monkeypatch: pytest.MonkeyPatch,
    error: Exception, attempts: int,
) -> None:  # fmt: skip
    calls: list[str] = []

    async def purge(ledger_id: str, workspace_id: str) -> None:
        calls.append(ledger_id)
        raise error

    monkeypatch.setattr(tasks, "_purge", purge)
    result = tasks.purge_document_task.apply(kwargs=ARGS)
    assert len(calls) == attempts
    assert isinstance(result.result, type(error))
    # Retries ran out: the next sweep may send it again.
    assert key.ops[-1] == "clear"


def test_a_permanent_failure_keeps_its_key(
    eager_app: Any, key: RecordingPending, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Logged by its code only, nothing recorded: the key is kept for the full TTL, so the sweep
    re-sends the row at most hourly."""

    async def purge(ledger_id: str, workspace_id: str) -> None:
        raise PaperlessForbidden("DELETE /api/documents/7/: HTTP 403", 403)

    monkeypatch.setattr(tasks, "_purge", purge)
    tasks.purge_document_task.apply(kwargs=ARGS)
    assert "clear" not in key.ops
    assert key.ops == ["refresh 3600"]


def test_a_done_purge_clears_its_key(
    eager_app: Any, key: RecordingPending, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def purge(ledger_id: str, workspace_id: str) -> None:
        return None

    monkeypatch.setattr(tasks, "_purge", purge)
    tasks.purge_document_task.apply(kwargs=ARGS)
    assert key.ops == ["clear"]


@pytest.mark.parametrize("retries_before", [0, tasks.PURGE_MAX_RETRIES])
def test_a_consume_in_flight_is_checked_again_without_spending_a_retry(
    eager_app: Any, key: RecordingPending, monkeypatch: pytest.MonkeyPatch, retries_before: int
) -> None:
    """Sent again by name with a countdown (a fresh message, its retries untouched), its key kept
    past the countdown; nothing audited, nothing logged as a failure."""
    calls: list[str] = []
    sent: list[tuple[str, dict[str, Any]]] = []

    async def purge(ledger_id: str, workspace_id: str) -> None:
        calls.append(ledger_id)
        raise ConsumeInFlight

    def send_task(name: str, **options: Any) -> None:
        sent.append((name, options))

    monkeypatch.setattr(tasks, "_purge", purge)
    monkeypatch.setattr(eager_app, "send_task", send_task)
    tasks.purge_document_task.apply(kwargs=ARGS, retries=retries_before)
    assert len(calls) == 1
    assert sent == [
        (
            "sijil.purge_document",
            {"kwargs": ARGS, "queue": "default", "countdown": tasks.CONSUME_RECHECK_S},
        )
    ]
    assert key.ops == [f"refresh {tasks.CONSUME_RECHECK_S + tasks.PENDING_MARGIN_S}"]
