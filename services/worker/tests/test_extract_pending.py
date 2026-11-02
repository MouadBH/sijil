"""One extraction in flight per document: `extract:pending:<ledger_id>` on the broker Redis."""

import asyncio
import hashlib
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core.db.models import (
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    Workspace,
)
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.extract import requeue_unextracted
from sijil_worker.pending import PENDING_TTL_S, PendingExtractions, pending_key
from sijil_worker.settings import WorkerSettings

NOW = datetime(2026, 10, 2, 9, 30, tzinfo=UTC)
OLD = timedelta(hours=1)


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


async def _ledgers(db: AsyncSession, ws: Workspace, n: int) -> list[DocumentLedger]:
    rows = []
    for i in range(n):
        sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
        rows.append(
            DocumentLedger(
                workspace_id=ws.id,
                sha256=sha,
                md5="0" * 32,
                size_bytes=1,
                content_kind="pdf",
                source=DocumentSource.upload,
                object_key=f"k/{sha}",
                object_version_id="v1",
                state=DocumentState.received,
                ingest_state=IngestState.consumed,
                paperless_doc_id=7,
                retention_until=date(2036, 12, 31),
                ingested_at=NOW - OLD - timedelta(minutes=i),  # i = 0 is the newest
            )
        )
    db.add_all(rows)
    await db.commit()
    return rows


class Sent:
    def __init__(self) -> None:
        self.ids: list[uuid.UUID] = []

    async def __call__(self, ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        self.ids.append(ledger_id)


# The pending set ----------------------------------------------------------------------------


async def test_claim_is_once_until_cleared_or_expired(redis: Redis) -> None:
    pending = PendingExtractions(redis)
    lid = uuid.uuid4()
    assert await pending.claim(lid) is True
    assert await pending.claim(lid) is False
    assert 0 < await redis.ttl(pending_key(lid)) <= PENDING_TTL_S == 3600
    await pending.clear(lid)
    assert await pending.claim(lid) is True


async def test_expired_key_allows_a_resend(redis: Redis) -> None:
    pending = PendingExtractions(redis, ttl_s=1)
    sent = Sent()
    enqueue = pending.deduplicated(sent)
    lid, wid = uuid.uuid4(), uuid.uuid4()
    await enqueue(lid, wid)
    await enqueue(lid, wid)
    assert sent.ids == [lid]
    await asyncio.sleep(1.2)
    await enqueue(lid, wid)
    assert sent.ids == [lid, lid]


async def test_failed_send_releases_the_claim(redis: Redis) -> None:
    pending = PendingExtractions(redis)
    lid = uuid.uuid4()

    async def broken(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        raise ConnectionError("broker down")

    with pytest.raises(ConnectionError):
        await pending.deduplicated(broken)(lid, uuid.uuid4())
    assert await redis.exists(pending_key(lid)) == 0


async def test_refresh_extends_the_ttl(redis: Redis) -> None:
    pending = PendingExtractions(redis)
    lid = uuid.uuid4()
    await pending.claim(lid)
    await pending.refresh(lid, 7200)
    assert 3600 < await redis.ttl(pending_key(lid)) <= 7200


# The sweep ----------------------------------------------------------------------------------


async def test_sweep_run_twice_sends_once(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], workspace: Workspace,
    redis: Redis,
) -> None:  # fmt: skip
    (row,) = await _ledgers(db, workspace, 1)
    pending = PendingExtractions(redis)
    sent = Sent()
    first = await requeue_unextracted(sessions, sent, region="ma-cas", now=NOW, pending=pending)
    second = await requeue_unextracted(sessions, sent, region="ma-cas", now=NOW, pending=pending)
    assert (first, second) == (1, 0)
    assert sent.ids == [row.id]


async def test_sweep_sends_at_most_50_oldest_first_skipped_not_counted(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], workspace: Workspace,
    redis: Redis,
) -> None:  # fmt: skip
    rows = await _ledgers(db, workspace, 60)
    oldest_first = [r.id for r in reversed(rows)]
    pending = PendingExtractions(redis)
    for lid in oldest_first[:3]:  # already in flight
        await pending.claim(lid)
    sent = Sent()
    count = await requeue_unextracted(sessions, sent, region="ma-cas", now=NOW, pending=pending)
    assert count == 50
    assert sent.ids == oldest_first[3:53]


# The task keeps the key while a retry is in flight, and clears it when done --------------------


class FakePending:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int | None]] = []

    async def clear(self, ledger_id: uuid.UUID) -> None:
        self.calls.append(("clear", str(ledger_id), None))

    async def refresh(self, ledger_id: uuid.UUID, ttl_s: int) -> None:
        self.calls.append(("refresh", str(ledger_id), ttl_s))


LID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def eager(monkeypatch: pytest.MonkeyPatch) -> FakePending:
    app = make_celery(
        WorkerSettings(
            region="ma-cas",
            environment="test",
            redis_url="redis://127.0.0.1:1/0",
            master_key="a" * 43 + "=",
            database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
            provisioner_database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
            tenant_image="img:1",
            compose_project="sijil",
            tessdata_volume="t",
        )
    )
    app.conf.task_always_eager = True
    app.set_current()
    fake = FakePending()

    async def use(op: Any) -> None:
        await op(fake)

    monkeypatch.setattr(tasks, "_with_pending", use)

    async def failed(ledger_id: str, workspace_id: str | None, code: str) -> None:
        return None

    monkeypatch.setattr(tasks, "_extract_failed", failed)
    return fake


def test_task_clears_the_key_when_routed(
    eager: FakePending, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def ok(ledger_id: str, workspace_id: str | None) -> None:
        return None

    monkeypatch.setattr(tasks, "_extract", ok)
    tasks.extract_document_task.apply(kwargs={"ledger_id": LID})
    assert eager.calls == [("clear", LID, None)]


def test_task_clears_the_key_on_final_failure(
    eager: FakePending, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def boom(ledger_id: str, workspace_id: str | None) -> None:
        raise ValueError("final")

    monkeypatch.setattr(tasks, "_extract", boom)
    tasks.extract_document_task.apply(kwargs={"ledger_id": LID})
    assert eager.calls == [("clear", LID, None)]


def test_task_leaves_the_key_to_expire_when_recording_the_failure_fails(
    eager: FakePending, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A row that can neither be extracted nor recorded as failed (its index database gone,
    say) stays `received`. Its key is left to its TTL, so the sweep skips it for an hour
    instead of resending it every run and starving newer rows."""

    async def boom(ledger_id: str, workspace_id: str | None) -> None:
        raise LookupError("no index database credential")

    async def unrecordable(ledger_id: str, workspace_id: str | None, code: str) -> None:
        raise LookupError("no index database credential")

    monkeypatch.setattr(tasks, "_extract", boom)
    monkeypatch.setattr(tasks, "_extract_failed", unrecordable)
    tasks.extract_document_task.apply(kwargs={"ledger_id": LID})
    assert eager.calls == []


def test_task_defers_while_ingest_runs_without_counting_a_retry(
    eager: FakePending, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ingest still running in the workspace: the same message is sent again later (a fresh
    one, so it never spends the retry budget), the key is kept, and nothing is recorded."""
    from sijil_worker.extract import IngestInProgress

    async def busy(ledger_id: str, workspace_id: str | None) -> None:
        raise IngestInProgress

    deferred: list[tuple[str, str | None, int]] = []

    def defer(ledger_id: str, workspace_id: str | None, countdown: int) -> None:
        deferred.append((ledger_id, workspace_id, countdown))

    async def must_not_record(ledger_id: str, workspace_id: str | None, code: str) -> None:
        raise AssertionError("a deferral is not a failure")

    monkeypatch.setattr(tasks, "_extract", busy)
    monkeypatch.setattr(tasks, "_defer_extract", defer)
    monkeypatch.setattr(tasks, "_extract_failed", must_not_record)
    result = tasks.extract_document_task.apply(kwargs={"ledger_id": LID, "workspace_id": None})
    assert result.successful()
    assert deferred == [(LID, None, tasks.INGEST_DEFER_S)]
    assert eager.calls == [("refresh", LID, tasks.INGEST_DEFER_S + tasks.PENDING_MARGIN_S)]


def test_task_keeps_and_refreshes_the_key_on_retry(
    eager: FakePending, monkeypatch: pytest.MonkeyPatch
) -> None:
    from sijil_core.ollama import OllamaUnavailable

    attempts: list[int] = []

    async def flaky(ledger_id: str, workspace_id: str | None) -> None:
        attempts.append(1)
        if len(attempts) < 3:
            raise OllamaUnavailable("ai.unavailable: ReadTimeout")

    monkeypatch.setattr(tasks, "_extract", flaky)
    monkeypatch.setattr(tasks, "extract_countdown", lambda retries: 100.0 * (retries + 1))
    tasks.extract_document_task.apply(kwargs={"ledger_id": LID})
    assert eager.calls == [
        ("refresh", LID, 100 + tasks.PENDING_MARGIN_S),
        ("refresh", LID, 200 + tasks.PENDING_MARGIN_S),
        ("clear", LID, None),
    ]
