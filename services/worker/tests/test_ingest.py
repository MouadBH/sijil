"""Ingest of an accepted original into the workspace's Paperless (respx + in-memory store)."""

import hashlib
import logging
import uuid
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from sqlalchemy import select
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
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.filetypes import FileKind
from sijil_core.paperless import PaperlessClient, PaperlessUnavailable
from sijil_core.region import RegionMismatch
from sijil_core.settings import CoreSettings
from sijil_core.tenancy import tenant_ref
from sijil_worker.ingest import (
    WorkspaceNotActive,
    ingest_document,
    requeue_stuck_ingest,
)
from tests.support.memory_store import MemoryObjectStore

pytestmark = pytest.mark.db

URL = "http://paperless-ws:8000"
TOKEN = "tok-" + "z" * 36
SETTINGS = CoreSettings.model_construct(region="ma-cas", environment="test")
FORMATS = Path(__file__).parents[3] / "fixtures" / "formats"
PDF = (FORMATS / "sample.pdf").read_bytes()
PDF_SHA = hashlib.sha256(PDF).hexdigest()
TASK_ID = "11111111-2222-3333-4444-555555555555"
USER_FILENAME = "Facture-Client-Secret.pdf"  # never stored anywhere; must never be sent


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
    """The workspace as provisioning leaves it: active, with a URL and a sealed service token."""
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


async def make_ledger(
    db: AsyncSession,
    store: MemoryObjectStore,
    workspace: Workspace,
    tmp_path: Path,
    *,
    data: bytes = PDF,
    kind: FileKind = FileKind.pdf,
    age: timedelta = timedelta(0),
) -> DocumentLedger:
    sha = hashlib.sha256(data).hexdigest()
    path = tmp_path / sha
    path.write_bytes(data)
    key = f"{tenant_ref(workspace.id)}/{sha}"
    retain = utcnow().date() + timedelta(days=3650)
    ref = await store.put_original(key, path, retain)
    row = DocumentLedger(
        workspace_id=workspace.id,
        sha256=sha,
        md5=hashlib.md5(data, usedforsecurity=False).hexdigest(),
        size_bytes=len(data),
        content_kind=kind.value,
        source=DocumentSource.upload,
        object_key=ref.key,
        object_version_id=ref.version_id,
        retention_until=retain,
        ingested_at=utcnow() - age,
    )
    db.add(row)
    await db.commit()
    return row


def paperless(base_url: str, **kw: Any) -> PaperlessClient:
    return PaperlessClient(base_url, **kw)


async def run(
    ledger: DocumentLedger,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    store: MemoryObjectStore,
    *,
    will_retry: bool = False,
    settings: CoreSettings = SETTINGS,
) -> None:
    await ingest_document(
        ledger.id,
        sessions=sessions,
        keys=keys,
        store=store,
        paperless=paperless,
        poll_timeout_s=5,
        poll_interval_s=0,
        settings=settings,
        will_retry=lambda _exc: will_retry,
    )


def task_row(status: str, **fields: Any) -> list[dict[str, Any]]:
    return [{"task_id": TASK_ID, "status": status, **fields}]


def mock_flow(
    router: respx.MockRouter,
    *,
    existing: list[dict[str, Any]] | None = None,
    tasks: list[httpx.Response] | None = None,
) -> tuple[respx.Route, respx.Route]:
    router.get(f"{URL}/api/documents/").respond(200, json={"results": existing or []})
    upload = router.post(f"{URL}/api/documents/post_document/").respond(200, json=TASK_ID)
    route = router.get(f"{URL}/api/tasks/")
    route.side_effect = tasks or [
        httpx.Response(200, json=task_row("success", related_document_ids=[7]))
    ]
    return upload, route


async def reload(db: AsyncSession, ledger: DocumentLedger) -> DocumentLedger:
    row = await db.get(DocumentLedger, ledger.id, populate_existing=True)
    assert row is not None
    return row


async def events(db: AsyncSession, workspace: Workspace) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent).where(AuditEvent.workspace_id == workspace.id).order_by(AuditEvent.seq)
    )
    return list(rows)


async def test_ingest_consumed_sets_doc_id_and_audits(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    with respx.mock(assert_all_called=False) as router:
        upload, _ = mock_flow(router)
        await run(ledger, sessions, keys, memory_store)
    row = await reload(db, ledger)
    assert row.ingest_state is IngestState.consumed
    assert row.paperless_doc_id == 7
    assert row.paperless_task_id == TASK_ID
    assert row.ingest_error_code is None
    assert upload.calls.last.request.headers["Authorization"] == f"Token {TOKEN}"
    (event,) = await events(db, ready)
    assert event.action == "document.ingested"
    assert event.actor_type.value == "system"
    assert event.target_id == str(ledger.id)
    assert event.data == {"paperless_doc_id": 7, "sha256": PDF_SHA}
    assert (await audit.verify(db, ready.id)).ok


async def test_filename_sent_to_paperless_is_derived(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    with respx.mock(assert_all_called=False) as router:
        upload, _ = mock_flow(router)
        await run(ledger, sessions, keys, memory_store)
    body = upload.calls.last.request.content
    assert f'filename="{PDF_SHA[:12]}.pdf"'.encode() in body
    assert USER_FILENAME.encode() not in body
    assert PDF in body  # the original, byte for byte
    assert b'name="title"' in body
    assert f" {FileKind.pdf.value}".encode() in body


async def test_ingest_unsupported_not_retried(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    retried: list[Exception] = []

    def will_retry(exc: Exception) -> bool:
        retried.append(exc)
        return True

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{URL}/api/documents/").respond(200, json={"results": []})
        router.post(f"{URL}/api/documents/post_document/").respond(
            400, json={"document": ["File type text/xml not supported"]}
        )
        await ingest_document(
            ledger.id,
            sessions=sessions,
            keys=keys,
            store=memory_store,
            paperless=paperless,
            poll_timeout_s=5,
            poll_interval_s=0,
            settings=SETTINGS,
            will_retry=will_retry,
        )  # returns normally: nothing for Celery to retry
    assert retried == []
    row = await reload(db, ledger)
    assert row.ingest_state is IngestState.unsupported
    assert row.ingest_error_code == "ingest.unsupported_format"
    assert row.paperless_doc_id is None
    (event,) = await events(db, ready)
    assert event.action == "document.ingest_failed"
    assert event.data == {"code": "ingest.unsupported_format", "sha256": PDF_SHA}


async def test_ingest_unsupported_reported_by_the_consume_task(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    failure = task_row(
        "failure",
        result_data={"error_message": "x.heic: Unexpected error: cannot identify image file '/t'"},
    )
    with respx.mock(assert_all_called=False) as router:
        mock_flow(router, tasks=[httpx.Response(200, json=failure)])
        await run(ledger, sessions, keys, memory_store, will_retry=True)
    row = await reload(db, ledger)
    assert row.ingest_state is IngestState.unsupported
    assert row.ingest_error_code == "ingest.unsupported_format"


async def test_ingest_retries_until_paperless_returns(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{URL}/api/documents/").respond(503)
        with pytest.raises(PaperlessUnavailable):
            await run(ledger, sessions, keys, memory_store, will_retry=True)
    row = await reload(db, ledger)
    assert row.ingest_state in {IngestState.queued, IngestState.sending}  # non-final
    assert row.ingest_error_code == "ingest.paperless_unavailable"
    assert await events(db, ready) == []

    with respx.mock(assert_all_called=False) as router:
        mock_flow(router)
        await run(ledger, sessions, keys, memory_store, will_retry=True)
    row = await reload(db, ledger)
    assert row.ingest_state is IngestState.consumed
    assert row.ingest_error_code is None
    assert row.paperless_doc_id == 7


async def test_ingest_fails_when_retries_are_exhausted(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    with (
        caplog.at_level(logging.DEBUG),
        respx.mock(assert_all_called=False) as router,
    ):
        router.get(f"{URL}/api/documents/").respond(503)
        with pytest.raises(PaperlessUnavailable):
            await run(ledger, sessions, keys, memory_store, will_retry=False)
    row = await reload(db, ledger)
    assert row.ingest_state is IngestState.failed
    assert row.ingest_error_code == "ingest.paperless_unavailable"
    (event,) = await events(db, ready)
    assert event.action == "document.ingest_failed"
    assert event.data == {"code": "ingest.paperless_unavailable", "sha256": PDF_SHA}
    assert TOKEN not in caplog.text


async def test_ingest_redelivery_is_noop(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    with respx.mock(assert_all_called=False) as router:
        mock_flow(router)
        await run(ledger, sessions, keys, memory_store)
    with respx.mock(assert_all_called=False) as router:
        await run(ledger, sessions, keys, memory_store)  # any HTTP call would raise: no routes
        assert router.calls.call_count == 0
    assert (await reload(db, ledger)).paperless_doc_id == 7
    assert len(await events(db, ready)) == 1


async def test_ingest_duplicate_in_paperless_resolves_to_existing_doc(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    with respx.mock(assert_all_called=False) as router:
        upload, _ = mock_flow(router, existing=[{"id": 42}])
        await run(ledger, sessions, keys, memory_store)
        assert not upload.called  # found by SHA-256 first: nothing is uploaded twice
        lookup = router.calls[0].request
        assert lookup.url.params["checksum__iexact"] == PDF_SHA
    row = await reload(db, ledger)
    assert row.ingest_state is IngestState.consumed
    assert row.paperless_doc_id == 42


async def test_ingest_duplicate_refused_by_the_consume_task_resolves_by_checksum(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    failure = task_row(
        "failure", result_data={"error_message": "x.pdf: Not consuming x.pdf: It is a duplicate"}
    )
    with respx.mock(assert_all_called=False) as router:
        lookup = router.get(f"{URL}/api/documents/")
        lookup.side_effect = [
            httpx.Response(200, json={"results": []}),
            httpx.Response(200, json={"results": [{"id": 9}]}),
        ]
        router.post(f"{URL}/api/documents/post_document/").respond(200, json=TASK_ID)
        router.get(f"{URL}/api/tasks/").respond(200, json=failure)
        await run(ledger, sessions, keys, memory_store)
    row = await reload(db, ledger)
    assert row.ingest_state is IngestState.consumed
    assert row.paperless_doc_id == 9


async def test_ingest_polls_until_the_task_finishes(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    with respx.mock(assert_all_called=False) as router:
        _, tasks = mock_flow(
            router,
            tasks=[
                httpx.Response(200, json=[]),
                httpx.Response(200, json=task_row("started")),
                httpx.Response(200, json=task_row("success", related_document_ids=[3])),
            ],
        )
        await run(ledger, sessions, keys, memory_store)
        assert tasks.call_count == 3
    assert (await reload(db, ledger)).paperless_doc_id == 3


async def test_ingest_poll_timeout_is_transient(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{URL}/api/documents/").respond(200, json={"results": []})
        router.post(f"{URL}/api/documents/post_document/").respond(200, json=TASK_ID)
        router.get(f"{URL}/api/tasks/").respond(200, json=task_row("started"))
        with pytest.raises(PaperlessUnavailable):
            await ingest_document(
                ledger.id,
                sessions=sessions,
                keys=keys,
                store=memory_store,
                paperless=paperless,
                poll_timeout_s=0.05,
                poll_interval_s=0.01,
                settings=SETTINGS,
                will_retry=lambda _exc: True,
            )
    row = await reload(db, ledger)
    assert row.ingest_state is IngestState.queued
    assert row.ingest_error_code == "ingest.paperless_unavailable"
    assert row.paperless_task_id == TASK_ID


async def test_other_consume_failure_is_retried_then_failed(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    failure = task_row("failure", result_data={"error_message": "x.pdf: Error while consuming"})
    for will_retry, state in ((True, IngestState.queued), (False, IngestState.failed)):
        with respx.mock(assert_all_called=False) as router:
            mock_flow(router, tasks=[httpx.Response(200, json=failure)])
            with pytest.raises(Exception, match="consume"):
                await run(ledger, sessions, keys, memory_store, will_retry=will_retry)
        row = await reload(db, ledger)
        assert row.ingest_state is state
        assert row.ingest_error_code == "ingest.consume_failed"


async def test_inactive_workspace_is_transient(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    ready.state = WorkspaceState.hibernated
    await db.commit()
    with respx.mock(assert_all_called=False) as router:
        with pytest.raises(WorkspaceNotActive):
            await run(ledger, sessions, keys, memory_store, will_retry=True)
        assert router.calls.call_count == 0  # Paperless is not touched
    row = await reload(db, ledger)
    assert row.ingest_state is IngestState.queued
    assert row.ingest_error_code == "ingest.workspace_not_active"


async def test_region_mismatch_fails_at_once(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    with respx.mock(assert_all_called=False) as router:
        with pytest.raises(RegionMismatch):
            await run(
                ledger,
                sessions,
                keys,
                memory_store,
                will_retry=True,  # never retried even so
                settings=CoreSettings.model_construct(region="eu-fra", environment="test"),
            )
        assert router.calls.call_count == 0
    row = await reload(db, ledger)
    assert row.ingest_state is IngestState.failed
    assert row.ingest_error_code == "ingest.region_mismatch"


async def test_missing_original_is_final(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    ledger = await make_ledger(db, memory_store, ready, tmp_path)
    memory_store.objects.clear()
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{URL}/api/documents/").respond(200, json={"results": []})
        with pytest.raises(FileNotFoundError):
            await run(ledger, sessions, keys, memory_store, will_retry=False)
    row = await reload(db, ledger)
    assert row.ingest_state is IngestState.failed
    assert row.ingest_error_code == "ingest.original_missing"


@pytest.fixture
def enqueued() -> list[tuple[uuid.UUID, uuid.UUID]]:
    return []


@pytest.fixture
def enqueue(
    enqueued: list[tuple[uuid.UUID, uuid.UUID]],
) -> Callable[[uuid.UUID, uuid.UUID], Any]:
    async def _enqueue(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        enqueued.append((ledger_id, workspace_id))

    return _enqueue


async def test_requeue_sweep_resends_old_queued_rows_only(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
    enqueued: list[tuple[uuid.UUID, uuid.UUID]],
    enqueue: Callable[[uuid.UUID, uuid.UUID], Any],
) -> None:
    old = await make_ledger(
        db, memory_store, ready, tmp_path, data=PDF + b"1", age=timedelta(minutes=11)
    )
    await make_ledger(db, memory_store, ready, tmp_path, data=PDF + b"2", age=timedelta(minutes=9))
    done = await make_ledger(
        db, memory_store, ready, tmp_path, data=PDF + b"3", age=timedelta(hours=2)
    )
    sending = await make_ledger(
        db, memory_store, ready, tmp_path, data=PDF + b"4", age=timedelta(hours=2)
    )
    done.ingest_state = IngestState.consumed
    sending.ingest_state = IngestState.sending
    await db.commit()

    count = await requeue_stuck_ingest(sessions, enqueue)
    assert count == 1
    assert enqueued == [(old.id, ready.id)]


async def test_requeue_sweep_is_bounded(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
    enqueued: list[tuple[uuid.UUID, uuid.UUID]],
    enqueue: Callable[[uuid.UUID, uuid.UUID], Any],
) -> None:
    for n in range(3):
        await make_ledger(
            db, memory_store, ready, tmp_path, data=PDF + bytes([n]), age=timedelta(minutes=30 + n)
        )
    assert await requeue_stuck_ingest(sessions, enqueue, limit=2) == 2
    assert len(enqueued) == 2


async def test_requeue_sweep_survives_a_failed_enqueue(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    memory_store: MemoryObjectStore,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    for n in range(2):
        await make_ledger(
            db, memory_store, ready, tmp_path, data=PDF + bytes([n]), age=timedelta(minutes=30)
        )
    calls: list[uuid.UUID] = []

    async def flaky(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        calls.append(ledger_id)
        if len(calls) == 1:
            raise ConnectionError("broker down")

    assert await requeue_stuck_ingest(sessions, flaky) == 1
    assert len(calls) == 2
