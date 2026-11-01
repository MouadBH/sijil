"""CAP-2 web upload and CAP-4 content-hash duplicates, through the shared ingest path."""

import asyncio
import hashlib
import tempfile
import time
import tracemalloc
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
import respx
from fastapi import FastAPI
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit, ingest
from sijil_core.db.models import (
    ActorType,
    AuditEvent,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    Membership,
    Organisation,
    Role,
    Workspace,
)
from sijil_core.filetypes import MAX_UPLOAD_BYTES
from sijil_core.retention import provisional_retain_until, region_local_date
from sijil_core.storage import ObjectRef
from sijil_core.tenancy import tenant_ref
from tests.support.memory_store import MemoryObjectStore

# conftest's Actor, Clock and RecordingEnqueuer (conftest is not importable as a module).
Actor = Any
Clock = Any
RecordingEnqueuer = Any

pytestmark = pytest.mark.db

FORMATS = Path(__file__).resolve().parents[3] / "fixtures" / "formats"
PDF = (FORMATS / "sample.pdf").read_bytes()
EXE = (FORMATS / "notallowed.exe").read_bytes()
MiB = 1024 * 1024


def _url(workspace: Workspace) -> str:
    return f"/v1/workspaces/{workspace.id}/documents"


async def _upload(
    actor: Actor,
    workspace: Workspace,
    data: bytes,
    filename: str = "facture.pdf",
    content_type: str = "application/pdf",
) -> httpx.Response:
    r: httpx.Response = await actor.client.post(
        _url(workspace), files={"file": (filename, data, content_type)}
    )
    return r


@pytest.fixture
async def uploader(
    make_actor: Callable[[str], Awaitable[Actor]], db: AsyncSession, workspace: Workspace
) -> Actor:
    actor = await make_actor("uploader@example.com")
    db.add(Membership(user_id=actor.user_id, workspace_id=workspace.id, role=Role.member))
    await db.commit()
    return actor


async def _ledger(db: AsyncSession, workspace: Workspace) -> list[DocumentLedger]:
    rows = await db.scalars(
        select(DocumentLedger)
        .where(DocumentLedger.workspace_id == workspace.id)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def _events(db: AsyncSession, workspace: Workspace) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == workspace.id)
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def test_upload_stores_original_and_ledger_row(
    uploader: Actor,
    workspace: Workspace,
    db: AsyncSession,
    memory_store: MemoryObjectStore,
    enqueuer: RecordingEnqueuer,
    clock: Clock,
) -> None:
    r = await _upload(uploader, workspace, PDF)
    assert r.status_code == 202, r.text
    body = r.json()
    assert body == {
        "document_id": body["document_id"],
        "state": "received",
        "ingest_state": "queued",
    }
    document_id = UUID(body["document_id"])

    [row] = await _ledger(db, workspace)
    sha256 = hashlib.sha256(PDF).hexdigest()
    assert row.id == document_id
    assert row.sha256 == sha256
    assert row.md5 == hashlib.md5(PDF, usedforsecurity=False).hexdigest()
    assert row.size_bytes == len(PDF)
    assert row.content_kind == "pdf"
    assert row.source is DocumentSource.upload
    assert row.uploaded_by == uploader.user_id
    assert row.source_email_id is None
    assert row.state is DocumentState.received
    assert row.ingest_state is IngestState.queued
    retain_until = provisional_retain_until(region_local_date(clock.now, "ma-cas"), "MA")
    assert row.retention_until == retain_until
    assert row.ingested_at == clock.now

    key = f"{tenant_ref(workspace.id)}/{sha256}"
    assert row.object_key == key
    [put] = memory_store.puts
    assert (put.key, put.version_id, put.retain_until) == (key, row.object_version_id, retain_until)
    [ref] = memory_store.versions(key)
    assert memory_store.objects[ref].data == PDF
    assert memory_store.objects[ref].mode == "GOVERNANCE"

    assert enqueuer.ingests == [(document_id, workspace.id)]


async def test_retention_uses_region_local_arrival_date(
    uploader: Actor,
    workspace: Workspace,
    db: AsyncSession,
    memory_store: MemoryObjectStore,
    clock: Clock,
) -> None:
    # 23:30 UTC on 31 December 2025 was 00:30 on 1 January 2026 in Casablanca (UTC+1 then):
    # a 2026 arrival, kept until the end of 2036, in the lock and in the ledger.
    clock.now = datetime(2025, 12, 31, 23, 30, tzinfo=UTC)
    r = await _upload(uploader, workspace, PDF)
    assert r.status_code == 202, r.text
    [row] = await _ledger(db, workspace)
    assert row.retention_until == date(2036, 12, 31)
    [put] = memory_store.puts
    assert put.retain_until == date(2036, 12, 31)


async def test_upload_same_file_twice_returns_409_with_existing_id(
    uploader: Actor,
    workspace: Workspace,
    db: AsyncSession,
    memory_store: MemoryObjectStore,
    enqueuer: RecordingEnqueuer,
) -> None:
    first = await _upload(uploader, workspace, PDF)
    assert first.status_code == 202
    second = await _upload(uploader, workspace, PDF, filename="other-name.pdf")
    assert second.status_code == 409
    assert second.json() == {
        "error": {"code": "document.duplicate"},
        "document_id": first.json()["document_id"],
    }
    assert len(await _ledger(db, workspace)) == 1
    # The duplicate was caught before the store: nothing new was locked.
    assert len(memory_store.puts) == 1
    assert len(enqueuer.ingests) == 1
    actions = [(e.action, e.target_id) for e in await _events(db, workspace)]
    existing = first.json()["document_id"]
    assert actions == [
        ("document.uploaded", existing),
        ("document.duplicate_rejected", existing),
    ]


class BarrierStore(MemoryObjectStore):
    """Holds every put until `parties` uploads have all passed the duplicate pre-check."""

    def __init__(self, parties: int) -> None:
        super().__init__()
        self.barrier = asyncio.Barrier(parties)

    async def put_original(self, key: str, path: Path, retain_until: date) -> ObjectRef:
        await asyncio.wait_for(self.barrier.wait(), timeout=10)
        return await super().put_original(key, path, retain_until)


async def test_concurrent_identical_uploads_create_one_row(
    uploader: Actor,
    workspace: Workspace,
    db: AsyncSession,
    cp_app: FastAPI,
    enqueuer: RecordingEnqueuer,
) -> None:
    # Review Focus 1. The barrier makes all ten race past the pre-check, so the unique
    # constraint (ON CONFLICT) is what decides.
    store = BarrierStore(10)
    cp_app.state.store = store
    responses = await asyncio.gather(*(_upload(uploader, workspace, PDF) for _ in range(10)))
    codes = sorted(r.status_code for r in responses)
    assert codes == [202] + [409] * 9
    [winner] = [r for r in responses if r.status_code == 202]
    winner_id = winner.json()["document_id"]
    for r in responses:
        if r.status_code == 409:
            assert r.json() == {"error": {"code": "document.duplicate"}, "document_id": winner_id}
    [row] = await _ledger(db, workspace)
    assert str(row.id) == winner_id
    assert enqueuer.ingests == [(row.id, workspace.id)]
    # Every racer put the same content-addressed key; the ledger points at one version.
    assert {p.key for p in store.puts} == {row.object_key}
    events = await _events(db, workspace)
    assert sorted(e.action for e in events) == ["document.duplicate_rejected"] * 9 + [
        "document.uploaded"
    ]
    assert all(e.target_id == winner_id for e in events)
    assert (await audit.verify(db, workspace.id)).ok


def _multipart(
    boundary: str, total: int, chunk: int = MiB
) -> tuple[AsyncIterator[bytes], list[int]]:
    """A streamed multipart body with a `total`-byte file part, never built in memory."""
    sent = [0]
    block = b"\0" * chunk

    async def body() -> AsyncIterator[bytes]:
        head = (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="file"; filename="big.pdf"\r\n'
            "Content-Type: application/pdf\r\n\r\n"
        ).encode()
        yield head
        left = total
        while left > 0:
            piece = block if left >= chunk else block[:left]
            left -= len(piece)
            sent[0] += len(piece)
            yield piece
        yield f"\r\n--{boundary}--\r\n".encode()

    return body(), sent


async def test_upload_rejects_over_100mb_without_storing(
    uploader: Actor,
    workspace: Workspace,
    db: AsyncSession,
    memory_store: MemoryObjectStore,
    enqueuer: RecordingEnqueuer,
) -> None:
    # Review Focus 4: a 150 MB stream is refused once it passes 100 MB, in bounded memory.
    boundary = "sijil-test-boundary"
    body, sent = _multipart(boundary, 150 * 1000 * 1000)
    tracemalloc.start()
    try:
        r = await uploader.client.post(
            _url(workspace),
            content=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert r.status_code == 413
    assert r.json() == {"error": {"code": "upload.too_large"}}
    assert peak < 32 * MiB, f"peak {peak / MiB:.1f} MiB"
    # Refused as the stream passed the limit, not after reading the remaining 50 MB.
    assert MAX_UPLOAD_BYTES < sent[0] <= MAX_UPLOAD_BYTES + 2 * MiB
    assert memory_store.puts == []
    assert await _ledger(db, workspace) == []
    assert enqueuer.ingests == []


async def test_upload_body_bounded_outside_the_file_part(
    uploader: Actor,
    workspace: Workspace,
    db: AsyncSession,
    memory_store: MemoryObjectStore,
    enqueuer: RecordingEnqueuer,
) -> None:
    # A small `file` part followed by a huge extra part: the whole body is bounded, not just
    # the file, so the reader stops once the body passes the limit plus 1 MiB of slack.
    boundary = "sijil-test-boundary"
    sent = [0]
    block = b"\0" * MiB

    async def body() -> AsyncIterator[bytes]:
        yield (
            (
                f"--{boundary}\r\n"
                'Content-Disposition: form-data; name="file"; filename="a.pdf"\r\n\r\n'
            ).encode()
            + PDF
            + f"\r\n--{boundary}\r\n".encode()
        )
        yield b'Content-Disposition: form-data; name="padding"\r\n\r\n'
        for _ in range(150):
            sent[0] += len(block)
            yield block
        yield f"\r\n--{boundary}--\r\n".encode()

    r = await uploader.client.post(
        _url(workspace),
        content=body(),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    assert r.status_code == 413
    assert r.json() == {"error": {"code": "upload.too_large"}}
    assert sent[0] <= MAX_UPLOAD_BYTES + 2 * MiB
    assert memory_store.puts == []
    assert await _ledger(db, workspace) == []
    assert enqueuer.ingests == []


async def test_upload_exactly_100mb_is_not_too_large(
    uploader: Actor, workspace: Workspace, memory_store: MemoryObjectStore
) -> None:
    boundary = "sijil-test-boundary"
    body, _ = _multipart(boundary, MAX_UPLOAD_BYTES)
    r = await uploader.client.post(
        _url(workspace),
        content=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    # All zeros: within the limit, so it reaches sniffing and is refused as a type instead.
    assert r.status_code == 415
    assert memory_store.puts == []


async def test_upload_rejects_exe(
    uploader: Actor, workspace: Workspace, db: AsyncSession, memory_store: MemoryObjectStore
) -> None:
    # Named and typed as a PDF by the client: neither is trusted.
    r = await _upload(uploader, workspace, EXE, filename="invoice.pdf")
    assert r.status_code == 415
    assert r.json() == {"error": {"code": "upload.unsupported_type"}}
    assert memory_store.puts == []
    assert await _ledger(db, workspace) == []


async def test_upload_rejects_eml_on_web_path(uploader: Actor, workspace: Workspace) -> None:
    eml = (FORMATS / "sample.eml").read_bytes()
    r = await _upload(uploader, workspace, eml, filename="m.eml", content_type="message/rfc822")
    assert r.status_code == 415


async def test_upload_rejects_empty(
    uploader: Actor, workspace: Workspace, db: AsyncSession, memory_store: MemoryObjectStore
) -> None:
    r = await _upload(uploader, workspace, b"")
    assert r.status_code == 422
    assert r.json() == {"error": {"code": "upload.empty"}}
    assert memory_store.puts == []
    assert await _ledger(db, workspace) == []


async def test_upload_requires_file_part(uploader: Actor, workspace: Workspace) -> None:
    r = await uploader.client.post(
        _url(workspace), files={"other": ("a.pdf", PDF, "application/pdf")}
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation"
    r = await uploader.client.post(_url(workspace), json={"file": "x"})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation"


async def test_upload_accepted_when_tenant_unreachable(
    uploader: Actor,
    workspace: Workspace,
    db: AsyncSession,
    memory_store: MemoryObjectStore,
    enqueuer: RecordingEnqueuer,
) -> None:
    # Review Focus 3: Paperless is not on this path at all. The tenant here is down; any
    # outbound HTTP from the control plane would hit this route and fail.
    workspace.paperless_url = "http://paperless-down.invalid:8000"
    await db.commit()
    with respx.mock(assert_all_called=False) as mock:
        outbound = mock.route().mock(side_effect=httpx.ConnectError("tenant down"))
        started = time.monotonic()
        r = await _upload(uploader, workspace, PDF)
        elapsed = time.monotonic() - started
    assert r.status_code == 202, r.text
    assert elapsed < 2.0
    assert outbound.call_count == 0
    assert len(memory_store.puts) == 1
    assert len(enqueuer.ingests) == 1


async def test_readonly_cannot_upload(
    make_actor: Callable[[str], Awaitable[Actor]],
    workspace: Workspace,
    db: AsyncSession,
    memory_store: MemoryObjectStore,
) -> None:
    reader = await make_actor("reader@example.com")
    db.add(Membership(user_id=reader.user_id, workspace_id=workspace.id, role=Role.readonly))
    await db.commit()
    r = await _upload(reader, workspace, PDF)
    assert r.status_code == 403
    assert r.json() == {"error": {"code": "forbidden"}}
    assert memory_store.puts == []


async def test_non_member_upload_is_404(
    make_actor: Callable[[str], Awaitable[Actor]], workspace: Workspace
) -> None:
    stranger = await make_actor("stranger@example.com")
    r = await _upload(stranger, workspace, PDF)
    assert r.status_code == 404


async def test_upload_audited(
    uploader: Actor, workspace: Workspace, db: AsyncSession, clock: Clock
) -> None:
    r = await _upload(uploader, workspace, PDF)
    assert r.status_code == 202
    [event] = await _events(db, workspace)
    assert event.action == "document.uploaded"
    assert event.actor_type is ActorType.user
    assert event.actor_id == str(uploader.user_id)
    assert event.target_type == "document"
    assert event.target_id == r.json()["document_id"]
    assert event.at == clock.now
    assert event.data == {
        "content_kind": "pdf",
        "sha256": hashlib.sha256(PDF).hexdigest(),
        "size_bytes": len(PDF),
        "source": "upload",
    }
    assert (await audit.verify(db, workspace.id)).ok


async def test_no_filename_in_shared_db(
    uploader: Actor, workspace: Workspace, db: AsyncSession
) -> None:
    secret = "secret-name-xyz"
    r = await _upload(uploader, workspace, PDF, filename=f"{secret}.pdf")
    assert r.status_code == 202
    again = await _upload(uploader, workspace, PDF, filename=f"{secret}-2.pdf")
    assert again.status_code == 409
    for table in ("document_ledger", "audit_event"):
        dumps = list(await db.scalars(text(f"SELECT CAST(t AS text) FROM {table} t")))  # noqa: S608
        assert dumps
        assert not any(secret in dump for dump in dumps), table


# --- accept_file directly: the contract the SMTP receiver will rely on -------------------


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


def _left_over(spool: Path) -> list[Path]:
    return list(spool.iterdir())


@pytest.fixture
def spool_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    spool = tmp_path / "spool"
    spool.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(spool))
    return spool


async def test_accept_file_cleans_up_spool_files(
    cp_app: FastAPI,
    db: AsyncSession,
    workspace: Workspace,
    org: Organisation,
    memory_store: MemoryObjectStore,
    clock: Clock,
    spool_dir: Path,
) -> None:
    enqueued: list[tuple[UUID, UUID]] = []

    async def enqueue(ledger_id: UUID, workspace_id: UUID) -> None:
        enqueued.append((ledger_id, workspace_id))

    async def accept(*parts: bytes) -> ingest.Accepted | ingest.Duplicate:
        return await ingest.accept_file(
            sessions=cp_app.state.sessionmaker,
            store=memory_store,
            enqueue=enqueue,
            workspace=workspace,
            country=org.country,
            region=org.region,
            chunks=_chunks(*parts),
            source=DocumentSource.upload,
            actor_type=ActorType.system,
            actor_id=None,
            ip=None,
            now=clock.now,
        )

    accepted = await accept(PDF[:100], PDF[100:])
    assert isinstance(accepted, ingest.Accepted)
    assert _left_over(spool_dir) == []
    duplicate = await accept(PDF)
    assert duplicate == ingest.Duplicate(accepted.document_id)
    assert _left_over(spool_dir) == []
    with pytest.raises(ingest.Unsupported):
        await accept(EXE)
    assert _left_over(spool_dir) == []
    with pytest.raises(ingest.Empty):
        await accept()
    with pytest.raises(ingest.TooLarge):
        await accept(*([b"%PDF-" + b"\0" * (MiB - 5)] + [b"\0" * MiB] * 100))
    assert _left_over(spool_dir) == []
    assert enqueued == [(accepted.document_id, workspace.id)]


async def test_accept_file_enqueues_only_after_commit(
    cp_app: FastAPI,
    workspace: Workspace,
    org: Organisation,
    memory_store: MemoryObjectStore,
    clock: Clock,
) -> None:
    seen: list[int] = []

    async def enqueue(ledger_id: UUID, workspace_id: UUID) -> None:
        # A different session must already see the committed row and its audit event.
        async with cp_app.state.sessionmaker() as other:
            seen.append(
                await other.scalar(
                    select(func.count())
                    .select_from(DocumentLedger)
                    .where(DocumentLedger.id == ledger_id)
                )
            )
            seen.append(
                await other.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.target_id == str(ledger_id))
                )
            )

    result = await ingest.accept_file(
        sessions=cp_app.state.sessionmaker,
        store=memory_store,
        enqueue=enqueue,
        workspace=workspace,
        country=org.country,
        region=org.region,
        chunks=_chunks(PDF),
        source=DocumentSource.upload,
        actor_type=ActorType.system,
        actor_id=None,
        ip=None,
        now=clock.now,
    )
    assert isinstance(result, ingest.Accepted)
    assert seen == [1, 1]


async def test_accept_file_email_source_audits_email_received(
    cp_app: FastAPI,
    db: AsyncSession,
    workspace: Workspace,
    org: Organisation,
    memory_store: MemoryObjectStore,
    clock: Clock,
) -> None:
    async def enqueue(ledger_id: UUID, workspace_id: UUID) -> None:
        return None

    eml = (FORMATS / "sample.eml").read_bytes()
    with pytest.raises(ingest.Unsupported):
        await ingest.accept_file(
            sessions=cp_app.state.sessionmaker,
            store=memory_store,
            enqueue=enqueue,
            workspace=workspace,
            country=org.country,
            region=org.region,
            chunks=_chunks(eml),
            source=DocumentSource.email,
            actor_type=ActorType.email,
            actor_id=None,
            ip=None,
            now=clock.now,
        )
    result = await ingest.accept_file(
        sessions=cp_app.state.sessionmaker,
        store=memory_store,
        enqueue=enqueue,
        workspace=workspace,
        country=org.country,
        region=org.region,
        chunks=_chunks(eml),
        source=DocumentSource.email,
        actor_type=ActorType.email,
        actor_id=None,
        ip=None,
        now=clock.now,
        allow_eml=True,
    )
    assert isinstance(result, ingest.Accepted)
    [row] = await _ledger(db, workspace)
    assert (row.content_kind, row.source, row.uploaded_by) == ("eml", DocumentSource.email, None)
    [event] = await _events(db, workspace)
    assert (event.action, event.actor_type, event.target_id) == (
        "email.received",
        ActorType.email,
        str(result.document_id),
    )
