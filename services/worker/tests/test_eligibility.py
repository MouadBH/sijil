"""`sijil.mark_eligible`: documents past their retention become eligible for deletion, and
nothing is ever purged automatically (plan 4.4, design §5.4).

The database is the real test one; the store is the in-memory double.
"""

import hashlib
import os
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from celery.schedules import crontab
from redis.asyncio import Redis
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sijil_core import index
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
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow
from sijil_core.storage import ObjectRef
from sijil_worker.app import make_celery
from sijil_worker.eligibility import mark_eligible
from sijil_worker.settings import BrokerSettings, WorkerSettings
from tests.support.fake_paperless import fake_paperless
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore, StoredObject
from tests.support.worker_tasks import worker_tasks

pytestmark = pytest.mark.db

CASABLANCA = ZoneInfo("Africa/Casablanca")
NOW = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
FAR = date(2036, 12, 31)
PAST = date(2020, 12, 31)
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")
REASON = "Doublon déposé par erreur (SAMPLE)"
PAPERLESS_URL = "http://paperless-sample:8000"


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


async def _ledger(
    db: AsyncSession,
    ws: Workspace,
    user: User,
    *,
    retention: date = PAST,
    state: DocumentState = DocumentState.filed,
    store: MemoryObjectStore | None = None,
) -> DocumentLedger:
    """A row brought to `state` through the transitions 0011 allows (SAMPLE content)."""
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    sealed = state in (DocumentState.sealed, DocumentState.deleting, DocumentState.deleted)
    row = DocumentLedger(
        workspace_id=ws.id, sha256=sha, md5="0" * 32, size_bytes=6, content_kind="pdf",
        source=DocumentSource.upload, object_key=f"k/{sha}", object_version_id="v1",
        state=DocumentState.sealed if sealed else DocumentState.filed,
        ingest_state=IngestState.consumed, paperless_doc_id=7, retention_until=retention,
        ingested_at=NOW - timedelta(days=1),
        fields_sha256="f" * 64 if sealed else None,
        sealed_at=NOW - timedelta(days=1) if sealed else None,
        sealed_by=user.id if sealed else None,
        object_locked_at=NOW - timedelta(days=1) if sealed else None,
    )  # fmt: skip
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
    if store is not None:
        store.objects[ObjectRef(row.object_key, "v1")] = StoredObject(
            data=b"SAMPLE",
            mode="COMPLIANCE" if sealed else "GOVERNANCE",
            retain_until=retention,
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


def _columns(row: DocumentLedger, *, but: str = "deletion_eligible_at") -> dict[str, Any]:
    return {c.key: getattr(row, c.key) for c in DocumentLedger.__table__.columns if c.key != but}


async def _events(db: AsyncSession, ws: Workspace) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id)
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def _workspace(db: AsyncSession, org: Organisation, name: str) -> Workspace:
    slug = f"{name}-{uuid.uuid4().hex[:8]}"
    ws = Workspace(
        org_id=org.id, name=name, slug=slug, state=WorkspaceState.active,
        db_name=f"paperless_{slug}", index_db_name=f"index_{slug}", kms_key_id=f"kek-{slug}",
    )  # fmt: skip
    db.add(ws)
    await db.commit()
    return ws


async def test_eligible_only_after_retention_until(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User
) -> None:
    """Retention runs to the end of `retention_until` in Casablanca: at 23:30 local on that day
    nothing is eligible; at 00:10 local the next day it is."""
    row = await _ledger(db, workspace, user, retention=FAR)
    before = datetime(2036, 12, 31, 23, 30, tzinfo=CASABLANCA)
    assert await mark_eligible(sessions, settings=SETTINGS, now=before) == 0
    assert (await _fresh(db, row)).deletion_eligible_at is None
    assert await _events(db, workspace) == []

    after = datetime(2037, 1, 1, 0, 10, tzinfo=CASABLANCA)
    assert await mark_eligible(sessions, settings=SETTINGS, now=after) == 1
    assert (await _fresh(db, row)).deletion_eligible_at == after


async def test_eligibility_follows_the_casablanca_date_not_utc(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User
) -> None:
    """The zone data has Morocco on UTC+0 from late 2026, so the case above cannot tell the
    local date from the UTC one: on 1 September 2026 (UTC+1) it can. At 00:10 local it is
    still 31 August in UTC, and the document is eligible all the same."""
    row = await _ledger(db, workspace, user, retention=date(2026, 8, 31))
    after = datetime(2026, 9, 1, 0, 10, tzinfo=CASABLANCA)
    assert after.utcoffset() == timedelta(hours=1)
    assert after.astimezone(UTC).date() == date(2026, 8, 31)
    assert await mark_eligible(sessions, settings=SETTINGS, now=after) == 1
    assert (await _fresh(db, row)).deletion_eligible_at == after


async def test_mark_eligible_audits_count_once(
    db: AsyncSession, sessions: Any, workspace: Workspace, user: User, org: Organisation
) -> None:
    expired = [await _ledger(db, workspace, user) for _ in range(2)]
    expired.append(await _ledger(db, workspace, user, state=DocumentState.sealed))
    future = await _ledger(db, workspace, user, retention=FAR)
    terminal = [
        await _ledger(db, workspace, user, state=state)
        for state in (DocumentState.removed, DocumentState.deleting, DocumentState.deleted)
    ]
    other_ws = await _workspace(db, org, "other")
    in_other = await _ledger(db, other_ws, user)
    empty_ws = await _workspace(db, org, "empty")
    foreign_org = Organisation(name="F", type=OrgType.business, country="FR", region="eu-fra")
    db.add(foreign_org)
    await db.commit()
    foreign_ws = await _workspace(db, foreign_org, "foreign")
    foreign = await _ledger(db, foreign_ws, user)
    before = {r.id: _columns(r) for r in [*expired, future, *terminal, in_other, foreign]}

    assert await mark_eligible(sessions, settings=SETTINGS, now=NOW) == 4

    for row in [*expired, in_other]:
        fresh = await _fresh(db, row)
        assert fresh.deletion_eligible_at == NOW
        assert _columns(fresh) == before[row.id]  # nothing else changes
    for row in [future, *terminal, foreign]:
        fresh = await _fresh(db, row)
        assert fresh.deletion_eligible_at is None
        assert _columns(fresh) == before[row.id]
    [event] = await _events(db, workspace)
    assert (event.action, event.data) == ("retention.eligible", {"count": 3})
    assert (event.actor_type.value, event.actor_id) == ("system", None)
    [other_event] = await _events(db, other_ws)
    assert other_event.data == {"count": 1}
    assert await _events(db, empty_ws) == []  # no event for a count of 0
    assert await _events(db, foreign_ws) == []

    # The next night: the marker is set once, and nothing new means no event.
    later = NOW + timedelta(days=1)
    assert await mark_eligible(sessions, settings=SETTINGS, now=later) == 0
    assert (await _fresh(db, expired[0])).deletion_eligible_at == NOW
    assert len(await _events(db, workspace)) == 1
    # A document that expired since is marked and counted on its own.
    newly = await _ledger(db, workspace, user)
    assert await mark_eligible(sessions, settings=SETTINGS, now=later) == 1
    assert (await _fresh(db, newly)).deletion_eligible_at == later
    assert [e.data for e in await _events(db, workspace)] == [{"count": 3}, {"count": 1}]


def test_mark_eligible_runs_daily_at_0200_casablanca() -> None:
    app = make_celery(
        BrokerSettings(region="ma-cas", environment="test", redis_url="redis://x:1/0")
    )
    entry = app.conf.beat_schedule["mark-eligible-nightly"]
    assert entry["task"] == "sijil.mark_eligible"
    assert entry["schedule"] == crontab(hour=2, minute=0)
    assert str(app.conf.timezone) == "Africa/Casablanca"


# Nothing is purged automatically -----------------------------------------------------------


@pytest.fixture(scope="module")
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
async def index_conn(index_db_url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(index_db_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as conn:
        await index.ensure_schema(conn)
        yield conn
    await engine.dispose()


async def _index_row(conn: AsyncConnection, row: DocumentLedger) -> None:
    values = dict.fromkeys(FIELD_NAMES, None) | {"document_type": "invoice"}
    await index.write_extraction(
        conn,
        FieldsRow(
            ledger_id=row.id, document_type="invoice", values=values,
            confidence=dict.fromkeys(FIELD_NAMES, 0.95), signals={}, flags=(),
            source=dict.fromkeys(FIELD_NAMES, "extraction"), model="qwen3:4b",
            extracted_at=NOW, updated_at=NOW,
        ),
    )  # fmt: skip


async def test_nothing_purged_automatically(
    db: AsyncSession, workspace: Workspace, user: User, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, redis: Redis, migrated_db: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    """`mark_eligible` and every task of the beat schedule, run against expired documents
    (one filed, one sealed with its COMPLIANCE lock over), delete nothing and send nothing that
    could: the original, the Paperless document and the index row all stay."""
    workspace.paperless_url = PAPERLESS_URL
    await db.commit()
    rows = [
        await _ledger(db, workspace, user, store=memory_store),
        await _ledger(db, workspace, user, state=DocumentState.sealed, store=memory_store),
    ]
    for row in rows:
        await _index_row(index_conn, row)
    runner = worker_tasks(
        monkeypatch,
        database_url=migrated_db.app_url,
        redis_url=os.environ["SIJIL_TEST_REDIS_URL"],
        store=memory_store,
    )
    schedule = runner.app.conf.beat_schedule
    assert "sijil.mark_eligible" in {entry["task"] for entry in schedule.values()}
    with fake_paperless(PAPERLESS_URL, "admin-pw", "admin-token") as paperless:
        for i, row in enumerate(rows):
            paperless.add(10 + i, row.sha256)
        for entry in schedule.values():
            result = await runner.run(entry["task"])
            assert result.successful(), (entry["task"], result.result)
        for _ in range(2):  # the sweeps again, with the rows now marked eligible
            for entry in schedule.values():
                assert (await runner.run(entry["task"])).successful()
        assert paperless.calls == []
        assert set(paperless.live) == {10, 11}

    # Only the read-only drift check was handed on; no removal, no purge.
    assert {s.name for s in runner.sent} == {"sijil.drift_check"}
    for row in rows:
        fresh = await _fresh(db, row)
        assert fresh.deletion_eligible_at is not None
        assert fresh.state is row.state
        assert (fresh.original_removed_at, fresh.deleted_at) == (None, None)
        assert ObjectRef(row.object_key, "v1") in memory_store.objects
        assert await index.read_fields(index_conn, row.id) is not None
    actions = [e.action for e in await _events(db, workspace)]
    assert actions == ["retention.eligible"]
    assert memory_store.removed == []
