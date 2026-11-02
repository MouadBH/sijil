"""Nightly integrity drift check (respx + in-memory store + real database)."""

import hashlib
import logging
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import respx
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core.crypto import LocalKeyService, seal
from sijil_core.db.base import utcnow
from sijil_core.db.models import (
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    DocumentSource,
    IngestState,
    IntegrityAlert,
    Organisation,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.paperless import PaperlessClient, PaperlessUnavailable
from sijil_core.region import RegionMismatch
from sijil_core.settings import CoreSettings
from sijil_core.tenancy import tenant_ref
from sijil_worker import drift
from sijil_worker.drift import DriftReport, drift_check, enqueue_drift_checks
from tests.support.memory_store import MemoryObjectStore

pytestmark = pytest.mark.db

URL = "http://paperless-ws:8000"
TOKEN = "tok-" + "z" * 36
SETTINGS = CoreSettings.model_construct(region="ma-cas", environment="test")
NOW = datetime(2026, 11, 2, 1, 30, tzinfo=UTC)


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sijil_core.paperless._BACKOFF_S", 0)


@pytest.fixture
def keys() -> LocalKeyService:
    return LocalKeyService(b"k" * 32)


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


@pytest.fixture
async def ready(db: AsyncSession, workspace: Workspace, keys: LocalKeyService) -> Workspace:
    workspace.paperless_url = URL
    envelope = seal(
        TOKEN.encode(), keys, workspace.kms_key_id, f"{workspace.id}:{CredentialKind.service_token}"
    )
    db.add(
        TenantCredential(
            workspace_id=workspace.id,
            kind=CredentialKind.service_token,
            envelope=envelope.to_bytes(),
        )
    )
    await db.commit()
    return workspace


async def make_doc(
    db: AsyncSession,
    store: MemoryObjectStore,
    workspace: Workspace,
    tmp_path: Path,
    doc_id: int,
    *,
    state: IngestState = IngestState.consumed,
) -> DocumentLedger:
    data = f"original-{doc_id}".encode()
    sha = hashlib.sha256(data).hexdigest()
    path = tmp_path / sha
    path.write_bytes(data)
    retain = utcnow().date() + timedelta(days=3650)
    ref = await store.put_original(f"{tenant_ref(workspace.id)}/{sha}", path, retain)
    row = DocumentLedger(
        workspace_id=workspace.id,
        sha256=sha,
        md5=hashlib.md5(data, usedforsecurity=False).hexdigest(),
        size_bytes=len(data),
        content_kind="pdf",
        source=DocumentSource.upload,
        object_key=ref.key,
        object_version_id=ref.version_id,
        retention_until=retain,
        ingest_state=state,
        paperless_doc_id=doc_id if state is IngestState.consumed else None,
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return row


def paperless(base_url: str, **kw: Any) -> PaperlessClient:
    return PaperlessClient(base_url, **kw)


async def run(
    workspace: Workspace,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    store: MemoryObjectStore,
    *,
    settings: CoreSettings = SETTINGS,
) -> DriftReport:
    return await drift_check(
        workspace.id,
        sessions=sessions,
        keys=keys,
        store=store,
        paperless=paperless,
        settings=settings,
        now=NOW,
    )


def metadata(router: respx.MockRouter, doc_id: int, checksum: str | None) -> respx.Route:
    route = router.get(f"{URL}/api/documents/{doc_id}/metadata/")
    if checksum is None:
        return route.respond(404, json={"detail": "Not found."})
    return route.respond(200, json={"original_checksum": checksum, "original_size": 1})


async def alerts(db: AsyncSession) -> list[IntegrityAlert]:
    rows = await db.scalars(
        select(IntegrityAlert)
        .order_by(IntegrityAlert.detected_at)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def events(db: AsyncSession, workspace: Workspace) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == workspace.id)
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def test_clean_workspace_reports_ok(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    docs = [await make_doc(db, memory_store, ready, tmp_path, i) for i in (1, 2)]
    # Not in Paperless yet: only its object is checked.
    await make_doc(db, memory_store, ready, tmp_path, 3, state=IngestState.queued)
    with respx.mock(assert_all_called=True) as router:
        for i, doc in enumerate(docs, start=1):
            metadata(router, i, doc.sha256.upper())  # case does not matter
        report = await run(ready, sessions, keys, memory_store)
        assert router.calls.call_count == 2
    assert report == DriftReport(checked=3, problems=0)
    assert await alerts(db) == []
    (event,) = await events(db, ready)
    assert event.action == "integrity.check_ok"
    assert event.actor_type.value == "system"
    assert event.data == {"checked": 3}
    assert (await audit.verify(db, ready.id)).ok


async def test_drift_check_flags_missing_paperless_document(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    gone = await make_doc(db, memory_store, ready, tmp_path, 1)
    fine = await make_doc(db, memory_store, ready, tmp_path, 2)
    with respx.mock(assert_all_called=True) as router:
        route = metadata(router, 1, None)
        metadata(router, 2, fine.sha256)
        report = await run(ready, sessions, keys, memory_store)
    assert route.calls.last.request.headers["Authorization"] == f"Token {TOKEN}"
    assert report == DriftReport(checked=2, problems=1)
    (alert,) = await alerts(db)
    assert (alert.ledger_id, alert.kind, alert.workspace_id) == (
        gone.id,
        "paperless_missing",
        ready.id,
    )
    assert alert.resolved_at is None
    assert alert.detected_at == NOW
    drift_event, summary = await events(db, ready)
    assert drift_event.action == "integrity.drift"
    assert drift_event.actor_type.value == "system"
    assert drift_event.target_id == str(gone.id)
    assert drift_event.data == {"kind": "paperless_missing"}
    assert summary.action == "integrity.drift_summary"
    assert summary.data == {"checked": 2, "problems": 1, "new": 1}
    assert (await audit.verify(db, ready.id)).ok


async def test_drift_check_flags_modified_file(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    doc = await make_doc(db, memory_store, ready, tmp_path, 1)
    with respx.mock(assert_all_called=True) as router:
        metadata(router, 1, hashlib.sha256(b"tampered").hexdigest())
        report = await run(ready, sessions, keys, memory_store)
    assert report.problems == 1
    (alert,) = await alerts(db)
    assert (alert.ledger_id, alert.kind) == (doc.id, "paperless_modified")
    assert alert.detail == {"paperless_doc_id": 1}


async def test_object_missing_flagged(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    doc = await make_doc(db, memory_store, ready, tmp_path, 1)
    memory_store.objects.clear()
    with respx.mock(assert_all_called=True) as router:
        metadata(router, 1, doc.sha256)
        report = await run(ready, sessions, keys, memory_store)
    assert report.problems == 1
    (alert,) = await alerts(db)
    assert (alert.ledger_id, alert.kind) == (doc.id, "object_missing")


@pytest.mark.parametrize(
    "state", [IngestState.unsupported, IngestState.failed, IngestState.queued, IngestState.sending]
)
async def test_object_checked_for_every_row_whatever_its_ingest_state(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
    state: IngestState,
) -> None:
    # An archived original is ours to keep even when Paperless never consumed it.
    doc = await make_doc(db, memory_store, ready, tmp_path, 1, state=state)
    memory_store.objects.clear()
    with respx.mock(assert_all_called=False) as router:
        report = await run(ready, sessions, keys, memory_store)
        assert router.calls.call_count == 0  # never asked Paperless about it
    assert report == DriftReport(checked=1, problems=1)
    (alert,) = await alerts(db)
    assert (alert.ledger_id, alert.kind) == (doc.id, "object_missing")


async def test_object_size_mismatch_flagged(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    doc = await make_doc(db, memory_store, ready, tmp_path, 1)
    (stored,) = memory_store.objects.values()
    stored.data = stored.data + b"!"
    with respx.mock(assert_all_called=True) as router:
        metadata(router, 1, doc.sha256)
        await run(ready, sessions, keys, memory_store)
    (alert,) = await alerts(db)
    assert alert.kind == "object_size_mismatch"
    assert alert.detail == {"expected_size": doc.size_bytes, "observed_size": doc.size_bytes + 1}


async def test_alerts_not_duplicated_on_rerun(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    await make_doc(db, memory_store, ready, tmp_path, 1)
    for _ in range(2):
        with respx.mock(assert_all_called=True) as router:
            metadata(router, 1, None)
            report = await run(ready, sessions, keys, memory_store)
        assert report.problems == 1  # still a problem, but not a second alert
    assert len(await alerts(db)) == 1
    actions = [e.action for e in await events(db, ready)]
    assert actions == ["integrity.drift", "integrity.drift_summary", "integrity.drift_summary"]
    # Once an operator resolves it, a recurrence opens a fresh alert.
    (alert,) = await alerts(db)
    alert.resolved_at = NOW
    await db.commit()
    with respx.mock(assert_all_called=True) as router:
        metadata(router, 1, None)
        await run(ready, sessions, keys, memory_store)
    assert len(await alerts(db)) == 2


async def test_paperless_down_is_transient_no_alerts(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    await make_doc(db, memory_store, ready, tmp_path, 1)
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{URL}/api/documents/1/metadata/").respond(503)
        with pytest.raises(PaperlessUnavailable):
            await run(ready, sessions, keys, memory_store)
    assert await alerts(db) == []
    assert await events(db, ready) == []


async def test_large_workspace_is_processed_in_batches(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(drift, "BATCH_SIZE", 2)
    docs = [await make_doc(db, memory_store, ready, tmp_path, i) for i in range(1, 6)]
    with respx.mock(assert_all_called=True) as router:
        for i, doc in enumerate(docs, start=1):
            metadata(router, i, doc.sha256 if i % 2 else None)
        report = await run(ready, sessions, keys, memory_store)
    assert report == DriftReport(checked=5, problems=2)
    assert len(await alerts(db)) == 2


async def test_wrong_region_checks_nothing(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    await make_doc(db, memory_store, ready, tmp_path, 1)
    other = CoreSettings.model_construct(region="eu-fra", environment="test")
    with respx.mock(assert_all_called=False) as router:
        route = router.get(f"{URL}/api/documents/1/metadata/")
        with pytest.raises(RegionMismatch):
            await run(ready, sessions, keys, memory_store, settings=other)
    assert not route.called


async def test_token_never_logged(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    await make_doc(db, memory_store, ready, tmp_path, 1)
    with caplog.at_level(logging.DEBUG), respx.mock(assert_all_called=False) as router:
        metadata(router, 1, None)
        await run(ready, sessions, keys, memory_store)
    assert TOKEN not in caplog.text


async def test_enqueue_covers_active_workspaces_of_this_region_only(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    org: Organisation,
    ready: Workspace,
) -> None:
    eu = Organisation(name="EU", type=org.type, country="FR", region="eu-fra")
    db.add(eu)
    await db.flush()
    slug = f"ws-{uuid.uuid4().hex[:8]}"

    def other(org_id: uuid.UUID, suffix: str, state: WorkspaceState) -> Workspace:
        return Workspace(
            org_id=org_id,
            name="x",
            slug=slug + suffix,
            state=state,
            db_name=f"p_{slug}{suffix}",
            index_db_name=f"i_{slug}{suffix}",
            kms_key_id=f"k{suffix}",
        )

    db.add_all(
        [
            other(eu.id, "a", WorkspaceState.active),
            other(org.id, "b", WorkspaceState.provisioning),
        ]
    )
    await db.commit()
    sent: list[uuid.UUID] = []

    async def enqueue(workspace_id: uuid.UUID) -> None:
        sent.append(workspace_id)

    assert await enqueue_drift_checks(sessions, SETTINGS, enqueue) == 1
    assert sent == [ready.id]


async def workspace_alerts(db: AsyncSession) -> list[IntegrityAlert]:
    return [a for a in await alerts(db) if a.ledger_id is None]


@pytest.mark.parametrize("status", [401, 403])
async def test_revoked_service_token_is_a_finding(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
    status: int,
) -> None:
    # An attacker who deleted a document then revokes the token: the check must not go quiet.
    await make_doc(db, memory_store, ready, tmp_path, 1)
    for _ in range(2):
        with respx.mock(assert_all_called=False) as router:
            router.get(f"{URL}/api/documents/1/metadata/").respond(status)
            report = await run(ready, sessions, keys, memory_store)  # no exception
        assert report.failed == "engine_forbidden"
    (alert,) = await alerts(db)  # deduplicated across runs
    assert (alert.ledger_id, alert.kind, alert.workspace_id) == (None, "engine_forbidden", ready.id)
    assert alert.resolved_at is None
    actions = [(e.action, e.data) for e in await events(db, ready)]
    assert actions == [("integrity.check_failed", {"code": "engine_forbidden"})] * 2
    assert (await audit.verify(db, ready.id)).ok


async def test_missing_credential_is_a_finding(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    await make_doc(db, memory_store, ready, tmp_path, 1)
    await db.execute(delete(TenantCredential).where(TenantCredential.workspace_id == ready.id))
    await db.commit()
    with respx.mock(assert_all_called=False) as router:
        report = await run(ready, sessions, keys, memory_store)
        assert router.calls.call_count == 0
    assert report.failed == "engine_credential_missing"
    (alert,) = await workspace_alerts(db)
    assert alert.kind == "engine_credential_missing"
    (event,) = await events(db, ready)
    assert (event.action, event.data) == (
        "integrity.check_failed",
        {"code": "engine_credential_missing"},
    )


async def test_undecryptable_credential_is_a_finding(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    await make_doc(db, memory_store, ready, tmp_path, 1)
    with respx.mock(assert_all_called=False):
        report = await run(ready, sessions, LocalKeyService(b"x" * 32), memory_store)
    assert report.failed == "engine_credential_missing"
    (alert,) = await workspace_alerts(db)
    assert alert.kind == "engine_credential_missing"


async def test_unexpected_error_response_is_a_finding(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    await make_doc(db, memory_store, ready, tmp_path, 1)
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{URL}/api/documents/1/metadata/").respond(400)
        report = await run(ready, sessions, keys, memory_store)
    assert report.failed == "engine_bad_response"
    (alert,) = await workspace_alerts(db)
    assert alert.kind == "engine_bad_response"
    assert [e.action for e in await events(db, ready)] == ["integrity.check_failed"]


@pytest.mark.parametrize(
    "body",
    [{"original_size": 1}, {"original_checksum": ""}, ["not", "a", "dict"], "not json"],
)
async def test_bad_metadata_for_one_document_does_not_block_the_others(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
    body: Any,
) -> None:
    odd = await make_doc(db, memory_store, ready, tmp_path, 1)
    gone = await make_doc(db, memory_store, ready, tmp_path, 2)
    fine = await make_doc(db, memory_store, ready, tmp_path, 3)
    with respx.mock(assert_all_called=True) as router:
        route = router.get(f"{URL}/api/documents/1/metadata/")
        if isinstance(body, str):
            route.respond(200, text=body, headers={"Content-Type": "application/json"})
        else:
            route.respond(200, json=body)
        metadata(router, 2, None)
        metadata(router, 3, fine.sha256)
        report = await run(ready, sessions, keys, memory_store)
    assert report == DriftReport(checked=3, problems=2)
    found = {(a.ledger_id, a.kind) for a in await alerts(db)}
    assert found == {(odd.id, "paperless_bad_metadata"), (gone.id, "paperless_missing")}


async def test_exhausted_retries_record_the_check_as_failed(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    ready: Workspace,
) -> None:
    for _ in range(2):
        await drift.record_check_failed(sessions, ready.id, "engine_unreachable", now=NOW)
    (alert,) = await alerts(db)
    assert (alert.ledger_id, alert.kind, alert.detected_at) == (None, "engine_unreachable", NOW)
    actions = [(e.action, e.data) for e in await events(db, ready)]
    assert actions == [("integrity.check_failed", {"code": "engine_unreachable"})] * 2
