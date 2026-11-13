"""The search index kept in step: `sijil.search_resync` (daily) and `sijil.search_backfill`.

The resync compares each `search_doc` row's state with the ledger, fixes the ones that differ
(a terminal ledger state is marked, never upserted), creates the missing ones (content fetched
from Paperless once, fields from `document_fields`), fills an empty text, purges the row of
a purged document, rewrites a row whose values drifted from their sources (audited
`search.repaired`) and audits `search.resynced` `{fixed, failed}`. The
backfill rebuilds every live row. Ingest and extraction upkeep are in test_ingest.py and
test_extract.py, next to their harnesses.

The databases are the real test ones; Paperless is respx.
"""

import hashlib
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
from celery.schedules import crontab
from sqlalchemy import select, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sijil_core import audit, index
from sijil_core.crypto import LocalKeyService, seal
from sijil_core.db.models import (
    ActorType,
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    Organisation,
    OrgType,
    TenantCredential,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow, IndexEngines
from sijil_core.paperless import PaperlessClient
from sijil_core.search_index import (
    SearchLedgerFacts,
    mark_search_state,
    refresh_search_doc,
    set_tags,
)
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.embed import chunk_input, source_digest
from sijil_worker.search import resync_search
from sijil_worker.settings import BrokerSettings, WorkerSettings
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

PAPERLESS = "http://paperless-search:8000"
TOKEN = "tok-" + "s" * 36
NOW = datetime(2026, 11, 9, 3, 30, tzinfo=UTC)
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")
REASON = "Doublon déposé par erreur (SAMPLE)"
# SAMPLE values (fictitious supplier and ICE).
VALUES: dict[str, str | None] = dict.fromkeys(FIELD_NAMES) | {
    "document_type": "invoice",
    "supplier_name": "Atlas Télécom SAMPLE SARL",
    "supplier_ice": "001905837000041",
    "date": "2026-09-30",
    "number": "F-2026-01001",
    "total_ttc": "1500.00",
    "currency": "MAD",
}


@pytest.fixture
def keys() -> LocalKeyService:
    return LocalKeyService(b"k" * 32)


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


@pytest.fixture
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


def _credential(ws: Workspace, keys: LocalKeyService, kind: CredentialKind, secret: str) -> Any:
    envelope = seal(secret.encode(), keys, ws.kms_key_id, f"{ws.id}:{kind}")
    return TenantCredential(workspace_id=ws.id, kind=kind, envelope=envelope.to_bytes())


@pytest.fixture
async def ready(db: AsyncSession, workspace: Workspace, keys: LocalKeyService) -> Workspace:
    workspace.paperless_url = PAPERLESS
    db.add(_credential(workspace, keys, CredentialKind.service_token, TOKEN))
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


@pytest.fixture
async def conn(index_db_url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(index_db_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as connection:
        await index.ensure_schema(connection)
        yield connection
    await engine.dispose()


async def _ledger(
    db: AsyncSession, ws: Workspace, user: User, state: DocumentState, doc_id: int
) -> DocumentLedger:
    """A row in `state`, through the transitions 0011 allows (SAMPLE content)."""
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    sealed = state in (DocumentState.sealed, DocumentState.deleting)
    row = DocumentLedger(
        workspace_id=ws.id, sha256=sha, md5="0" * 32, size_bytes=6, content_kind="pdf",
        source=DocumentSource.upload, object_key=f"k/{sha}", object_version_id="v1",
        state=(
            DocumentState.sealed if sealed
            else DocumentState.filed if state is DocumentState.removed else state
        ),
        ingest_state=IngestState.consumed, paperless_doc_id=doc_id,
        retention_until=datetime(2036, 12, 31).date(),
        ingested_at=NOW - timedelta(days=40),
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
        await db.commit()
    return row


async def _fields(conn: AsyncConnection, ledger_id: uuid.UUID) -> None:
    await index.write_extraction(
        conn,
        FieldsRow(
            ledger_id=ledger_id, document_type="invoice", values=VALUES,
            source=dict.fromkeys(VALUES, "extraction"), updated_at=NOW,
        ),
    )  # fmt: skip


async def _search_row(
    conn: AsyncConnection, row: DocumentLedger, state: DocumentState, content: str
) -> None:
    """The row as production writes it: from the fields and notes the index holds."""
    facts = SearchLedgerFacts(
        ledger_id=row.id, state=state, ingested_at=row.ingested_at, region="ma-cas", now=NOW
    )
    await refresh_search_doc(conn, facts, content)


async def _search(conn: AsyncConnection, ledger_id: uuid.UUID) -> dict[str, Any] | None:
    found = await conn.execute(
        text("SELECT state, content, supplier_name FROM search_doc WHERE ledger_id = :id"),
        {"id": ledger_id},
    )
    row = found.mappings().first()
    return None if row is None else dict(row)


async def _resynced(db: AsyncSession, ws: Workspace) -> list[dict[str, Any]]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id, AuditEvent.action == "search.resynced")
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return [dict(e.data) for e in rows]


def _paperless(router: respx.MockRouter, contents: dict[int, str]) -> dict[int, respx.Route]:
    return {
        doc_id: router.get(f"{PAPERLESS}/api/documents/{doc_id}/").respond(
            200, json={"id": doc_id, "content": content}
        )
        for doc_id, content in contents.items()
    }


async def _run(
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    **kw: Any,
) -> int:
    return await resync_search(
        sessions=sessions, keys=keys, index=engines, paperless=PaperlessClient,
        settings=SETTINGS, now=NOW, **kw,
    )  # fmt: skip


async def test_resync_fixes_state_and_creates_missing_rows(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    stale = await _ledger(db, ready, user, DocumentState.filed, 101)
    missing = await _ledger(db, ready, user, DocumentState.needs_review, 102)
    removed = await _ledger(db, ready, user, DocumentState.removed, 103)
    in_step = await _ledger(db, ready, user, DocumentState.sealed, 104)
    for row in (stale, missing, in_step):
        await _fields(conn, row.id)
    await _search_row(conn, stale, DocumentState.received, "Facture SAMPLE déjà indexée")
    await _search_row(conn, removed, DocumentState.filed, "Facture SAMPLE retirée")
    await _search_row(conn, in_step, DocumentState.sealed, "Facture SAMPLE scellée")
    with respx.mock(assert_all_called=False) as router:
        routes = _paperless(router, {d: f"SAMPLE فاتورة {d}" for d in (101, 102, 103, 104)})
        assert await _run(sessions, keys, engines) == 3
    # Content is fetched once, for the row that had none; the others keep theirs.
    assert [d for d, r in routes.items() if r.called] == [102]
    assert await _search(conn, stale.id) == {
        "state": "filed", "content": "Facture SAMPLE déjà indexée",
        "supplier_name": VALUES["supplier_name"],
    }  # fmt: skip
    assert await _search(conn, missing.id) == {
        "state": "needs_review", "content": "SAMPLE فاتورة 102",
        "supplier_name": VALUES["supplier_name"],
    }  # fmt: skip
    assert (await _search(conn, removed.id) or {})["state"] == "removed"
    assert (await _search(conn, in_step.id) or {})["content"] == "Facture SAMPLE scellée"
    assert await _resynced(db, ready) == [{"fixed": 3, "failed": 0}]
    # In step now: nothing to fix, nothing audited.
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        assert await _run(sessions, keys, engines) == 0
    assert await _resynced(db, ready) == [{"fixed": 3, "failed": 0}]


async def test_resync_never_creates_a_row_for_a_terminal_document(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    removed = await _ledger(db, ready, user, DocumentState.removed, 111)
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {111: "SAMPLE"})
        assert await _run(sessions, keys, engines) == 0
    assert await _search(conn, removed.id) is None


async def test_resync_covers_active_workspaces_of_this_region_only(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    org = Organisation(name="SAMPLE FR", type=OrgType.business, country="FR", region="fr-par")
    db.add(org)
    await db.commit()
    foreign = Workspace(
        org_id=org.id, name="SAMPLE", slug=f"ws-{uuid.uuid4().hex[:8]}",
        state=WorkspaceState.active, db_name="paperless_x", index_db_name="index_x",
        kms_key_id="kek-x", paperless_url=PAPERLESS,
    )  # fmt: skip
    db.add(foreign)
    await db.commit()
    elsewhere = await _ledger(db, foreign, user, DocumentState.filed, 121)
    ready.state = WorkspaceState.hibernated
    await db.commit()
    paused = await _ledger(db, ready, user, DocumentState.filed, 122)
    with respx.mock(assert_all_called=False) as router:
        routes = _paperless(router, {121: "SAMPLE", 122: "SAMPLE"})
        assert await _run(sessions, keys, engines) == 0
    assert not any(r.called for r in routes.values())
    assert await _search(conn, elsewhere.id) is None
    assert await _search(conn, paused.id) is None


async def test_backfill_rebuilds_every_live_row(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    filed = await _ledger(db, ready, user, DocumentState.filed, 131)
    sealed = await _ledger(db, ready, user, DocumentState.sealed, 132)
    removed = await _ledger(db, ready, user, DocumentState.removed, 133)
    for row in (filed, sealed):
        await _fields(conn, row.id)
    await _search_row(conn, sealed, DocumentState.sealed, "texte SAMPLE périmé")
    with respx.mock(assert_all_called=False) as router:
        routes = _paperless(router, {131: "SAMPLE 131", 132: "SAMPLE 132", 133: "SAMPLE 133"})
        assert await _run(sessions, keys, engines, workspace_id=ready.id, full=True) == 2
    assert sorted(d for d, r in routes.items() if r.called) == [131, 132]
    assert (await _search(conn, filed.id) or {})["content"] == "SAMPLE 131"
    assert (await _search(conn, sealed.id) or {})["content"] == "SAMPLE 132"
    assert await _search(conn, removed.id) is None
    assert await _resynced(db, ready) == [{"fixed": 2, "failed": 0}]


def test_resync_runs_daily_and_backfill_is_a_task() -> None:
    settings = BrokerSettings(region="ma-cas", environment="test", redis_url="redis://r:6379/0")
    entry = make_celery(settings).conf.beat_schedule["search-resync-daily"]
    assert entry == {"task": "sijil.search_resync", "schedule": crontab(hour=3, minute=30)}
    assert tasks.search_resync_task.name == "sijil.search_resync"
    assert tasks.search_backfill_task.name == "sijil.search_backfill"


# Fix round 1: the ledger locked across each write, purged documents, empty text, isolation --


async def _seal_now(
    sessions: async_sessionmaker[AsyncSession], row: DocumentLedger, user: User
) -> None:
    async with sessions() as session:
        await session.execute(
            text(
                "UPDATE document_ledger SET state = 'sealed', fields_sha256 = :sha,"
                " sealed_at = now(), sealed_by = :u WHERE id = :id"
            ),
            {"id": row.id, "u": user.id, "sha": "f" * 64},
        )
        await session.commit()


async def _remove_and_purge(
    sessions: async_sessionmaker[AsyncSession], engines: IndexEngines, ws: Workspace,
    row: DocumentLedger, user: User,
) -> None:  # fmt: skip
    """The Owner's removal, the removal worker and the purge, as they commit in production."""
    async with sessions() as session:
        await session.execute(
            text(
                "UPDATE document_ledger SET state = 'removed', removed_at = now(),"
                " removed_by = :u, removal_reason = :reason WHERE id = :id"
            ),
            {"id": row.id, "u": user.id, "reason": REASON},
        )
        await session.commit()
        await session.execute(
            text("UPDATE document_ledger SET original_removed_at = now() WHERE id = :id"),
            {"id": row.id},
        )
        await session.commit()
    async with (await engines.engine(ws)).begin() as conn:
        await index.purge_document(conn, row.id)
    async with sessions() as session:
        await audit.append(
            session, workspace_id=ws.id, action="document.purged", actor_type=ActorType.system,
            actor_id=None, target_type="document", target_id=str(row.id),
            data={"paperless": True}, now=NOW,
        )  # fmt: skip
        await session.commit()


async def test_a_seal_during_the_resync_is_not_reverted(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    """The document is sealed while the resync fetches its text: the row takes the state the
    ledger holds under its lock, not the one read at the start of the run."""
    row = await _ledger(db, ready, user, DocumentState.filed, 141)
    await _fields(conn, row.id)

    async def seal_then_answer(request: Any) -> httpx.Response:
        await _seal_now(sessions, row, user)
        return httpx.Response(200, json={"id": 141, "content": "SAMPLE 141"})

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{PAPERLESS}/api/documents/141/").mock(side_effect=seal_then_answer)
        assert await _run(sessions, keys, engines) == 1
    assert await _search(conn, row.id) == {
        "state": "sealed", "content": "SAMPLE 141", "supplier_name": VALUES["supplier_name"],
    }  # fmt: skip


async def test_a_removal_and_purge_during_a_backfill_leave_no_row(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await _ledger(db, ready, user, DocumentState.filed, 151)
    await _fields(conn, row.id)

    async def purge_then_answer(request: Any) -> httpx.Response:
        await _remove_and_purge(sessions, engines, ready, row, user)
        return httpx.Response(200, json={"id": 151, "content": "SAMPLE texte retiré"})

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{PAPERLESS}/api/documents/151/").mock(side_effect=purge_then_answer)
        await _run(sessions, keys, engines, workspace_id=ready.id, full=True)
    assert await _search(conn, row.id) is None


async def test_resync_purges_a_stray_row_of_a_purged_document(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    purged = await _ledger(db, ready, user, DocumentState.filed, 161)
    waiting = await _ledger(db, ready, user, DocumentState.filed, 162)
    await _remove_and_purge(sessions, engines, ready, purged, user)
    await _search_row(conn, purged, DocumentState.filed, "SAMPLE texte d'un document purgé")
    await _search_row(conn, waiting, DocumentState.filed, "SAMPLE en attente de purge")
    async with sessions() as session:  # removed, its purge not run yet
        await session.execute(
            text(
                "UPDATE document_ledger SET state = 'removed', removed_at = now(),"
                " removed_by = :u, removal_reason = :reason WHERE id = :id"
            ),
            {"id": waiting.id, "u": user.id, "reason": REASON},
        )
        await session.commit()
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        assert await _run(sessions, keys, engines) == 2
    assert await _search(conn, purged.id) is None
    assert (await _search(conn, waiting.id) or {})["state"] == "removed"


@pytest.mark.parametrize(
    "origin",
    [
        # Each path that writes a row without reading the text leaves `content` empty:
        DocumentState.received,  # ingest at consumed
        DocumentState.filed,  # a redelivered extraction routing its stored row
        DocumentState.needs_review,  # record_extraction_failed
        DocumentState.sealed,  # a seal or a correction creating the row
    ],
)
async def test_resync_fills_empty_text_of_a_consumed_document(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection, origin: DocumentState,
) -> None:  # fmt: skip
    row = await _ledger(db, ready, user, origin, 171)
    await _fields(conn, row.id)
    facts = SearchLedgerFacts(
        ledger_id=row.id, state=origin, ingested_at=row.ingested_at, region="ma-cas", now=NOW
    )
    async with (await engines.engine(ready)).begin() as c:
        await refresh_search_doc(c, facts)  # as that path calls it: no text
    assert (await _search(conn, row.id) or {})["content"] == ""
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {171: "SAMPLE فاتورة 171"})
        assert await _run(sessions, keys, engines) == 1
    found = await _search(conn, row.id)
    assert found is not None and (found["state"], found["content"]) == (
        origin.value,
        "SAMPLE فاتورة 171",
    )
    assert await _resynced(db, ready) == [{"fixed": 1, "failed": 0}]


async def test_one_unreadable_document_does_not_stop_the_resync(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
    caplog: pytest.LogCaptureFixture,
) -> None:  # fmt: skip
    rows = [await _ledger(db, ready, user, DocumentState.filed, d) for d in (181, 182, 183)]
    removed = await _ledger(db, ready, user, DocumentState.removed, 184)
    await _search_row(conn, removed, DocumentState.filed, "SAMPLE retiré")
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {181: "SAMPLE 181", 183: "SAMPLE 183"})
        router.get(f"{PAPERLESS}/api/documents/182/").respond(404, json={"detail": "Not found."})
        assert await _run(sessions, keys, engines) == 3
    assert (await _search(conn, rows[0].id) or {})["content"] == "SAMPLE 181"
    assert await _search(conn, rows[1].id) is None
    assert (await _search(conn, rows[2].id) or {})["content"] == "SAMPLE 183"
    assert (await _search(conn, removed.id) or {})["state"] == "removed"
    assert await _resynced(db, ready) == [{"fixed": 3, "failed": 1}]
    assert "SAMPLE" not in caplog.text  # logged by class only


# Embeddings ---------------------------------------------------------------------------------
# The resync also sends `sijil.embed_document` for each live document whose chunks are missing
# or were made from another chunk input (`doc_chunk.source_sha256`): a lost message, or a change
# no message followed (a review edit). Tags are not chunk input.


def _recorder() -> tuple[list[tuple[uuid.UUID, uuid.UUID]], Any]:
    sent: list[tuple[uuid.UUID, uuid.UUID]] = []

    async def send(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        sent.append((ledger_id, workspace_id))

    return sent, send


async def _embedded(conn: AsyncConnection, ledger_id: uuid.UUID) -> None:
    """Chunks made from the document's current chunk input (vectors are placeholders)."""
    texts = await chunk_input(conn, ledger_id)
    assert texts
    vectors = [[0.5] * index.EMBEDDING_DIM for _ in texts]
    assert await index.replace_chunks(
        conn, ledger_id, "bge-m3", list(zip(texts, vectors, strict=True)),
        source_sha256=source_digest(texts),
    )  # fmt: skip


async def _live(
    db: AsyncSession, conn: AsyncConnection, ws: Workspace, user: User, state: DocumentState,
    doc_id: int,
) -> DocumentLedger:  # fmt: skip
    row = await _ledger(db, ws, user, state, doc_id)
    await _fields(conn, row.id)
    await _search_row(conn, row, state, f"Facture SAMPLE {doc_id}")
    return row


async def test_resync_enqueues_a_lost_embedding(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    lost = [
        await _live(db, conn, ready, user, state, 190 + n)
        for n, state in enumerate(
            (DocumentState.received, DocumentState.needs_review, DocumentState.filed,
             DocumentState.sealed)
        )
    ]  # fmt: skip
    sent, send = _recorder()
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        await _run(sessions, keys, engines, enqueue_embed=send)
    assert sorted(sent) == sorted((row.id, ready.id) for row in lost)


async def test_resync_enqueues_after_a_needs_review_field_edit(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await _live(db, conn, ready, user, DocumentState.needs_review, 201)
    await _embedded(conn, row.id)
    # A reviewer's edit (PATCH fields): the fields row and the search row, no embedding sent.
    await index.update_fields(
        conn, row.id, {"supplier_name": "Rif Imprimerie SAMPLE"}, source="user",
        updated_by=user.id, now=NOW,
    )  # fmt: skip
    facts = SearchLedgerFacts(
        ledger_id=row.id, state="needs_review", ingested_at=row.ingested_at, region="ma-cas",
        now=NOW,
    )  # fmt: skip
    await refresh_search_doc(conn, facts)
    sent, send = _recorder()
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        await _run(sessions, keys, engines, enqueue_embed=send)
    assert sent == [(row.id, ready.id)]


async def test_resync_does_not_enqueue_a_tag_only_change(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    """A tag write bumps `search_doc.updated_at` but is not chunk input."""
    row = await _live(db, conn, ready, user, DocumentState.filed, 211)
    await _embedded(conn, row.id)
    await set_tags(conn, row.id, ["bureau", "urgent"], by=user.id, now=NOW + timedelta(days=1))
    sent, send = _recorder()
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        await _run(sessions, keys, engines, enqueue_embed=send)
    assert sent == []


async def test_resync_does_not_enqueue_an_up_to_date_document(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    rows = [
        await _live(db, conn, ready, user, state, 220 + n)
        for n, state in enumerate((DocumentState.filed, DocumentState.sealed))
    ]
    for row in rows:
        await _embedded(conn, row.id)
    sent, send = _recorder()
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        assert await _run(sessions, keys, engines, enqueue_embed=send) == 0
    assert sent == []
    assert await _resynced(db, ready) == []


async def test_resync_never_enqueues_a_terminal_document(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    """Removed with a row still live (marked by this run), or already marked: never sent."""
    unmarked = await _ledger(db, ready, user, DocumentState.removed, 231)
    await _fields(conn, unmarked.id)
    await _search_row(conn, unmarked, DocumentState.filed, "Facture SAMPLE retirée")
    marked = await _ledger(db, ready, user, DocumentState.removed, 232)
    await _fields(conn, marked.id)
    await _search_row(conn, marked, DocumentState.filed, "Facture SAMPLE retirée")
    await mark_search_state(conn, marked.id, DocumentState.removed)
    sent, send = _recorder()
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        await _run(sessions, keys, engines, enqueue_embed=send)
    assert sent == []


# Final review I1: the resync compares values, not only the state ------------------------------
# `search_doc` is a derived row the index role may UPDATE (only DELETE and TRUNCATE are gated),
# so the resync compares each live row's derived columns and tags with what its sources (the
# ledger, `document_fields`, `correction_note`, `document_tag`) give, rewrites any that differ
# and audits `search.repaired` `{columns}` (column names only) per document.


async def _repaired(db: AsyncSession, ws: Workspace) -> list[tuple[str | None, dict[str, Any]]]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id, AuditEvent.action == "search.repaired")
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return [(e.target_id, dict(e.data)) for e in rows]


async def _values(conn: AsyncConnection, ledger_id: uuid.UUID) -> dict[str, Any]:
    found = await conn.execute(
        text(
            "SELECT state, supplier_name, supplier_ice, total_ttc, currency, basis_date, tags,"
            " supplier_norm, corrected FROM search_doc WHERE ledger_id = :id"
        ),
        {"id": ledger_id},
    )
    return dict(found.mappings().one())


async def test_resync_repairs_values_changed_by_direct_sql(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    sealed = await _live(db, conn, ready, user, DocumentState.sealed, 241)
    untouched = await _live(db, conn, ready, user, DocumentState.filed, 242)
    await set_tags(conn, sealed.id, ["bureau"], by=user.id, now=NOW)
    before = await _values(conn, sealed.id)
    # The index role's credentials, no gate: Ask's sums and the export read these columns.
    await conn.execute(
        text(
            "UPDATE search_doc SET total_ttc = 1.00, supplier_ice = '009999999000099',"
            " supplier_name = 'Autre SAMPLE', supplier_norm = 'autre sample',"
            " basis_date = '2019-01-01', tags = '{faux}' WHERE ledger_id = :id"
        ),
        {"id": sealed.id},
    )
    with respx.mock(assert_all_called=False) as router:
        routes = _paperless(router, {241: "SAMPLE 241", 242: "SAMPLE 242"})
        assert await _run(sessions, keys, engines) == 1
    assert not any(r.called for r in routes.values())  # the text is kept, never re-fetched
    assert await _values(conn, sealed.id) == before
    assert (await _search(conn, sealed.id) or {})["content"] == "Facture SAMPLE 241"
    assert await _repaired(db, ready) == [
        (
            str(sealed.id),
            {"columns": "basis_date,supplier_ice,supplier_name,supplier_norm,tags,total_ttc"},
        )
    ]
    assert await _resynced(db, ready) == [{"fixed": 1, "failed": 0}]
    assert (await _search(conn, untouched.id) or {})["state"] == "filed"
    # In step now: nothing repaired twice.
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        assert await _run(sessions, keys, engines) == 0
    assert len(await _repaired(db, ready)) == 1


async def test_resync_repairs_a_corrected_value_and_a_currency_changed_by_sql(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    """The effective value is the correction note's, not the sealed one."""
    row = await _live(db, conn, ready, user, DocumentState.sealed, 251)
    await index.add_correction(
        conn, row.id, "total_ttc", "1600.00", REASON, created_by=user.id, now=NOW
    )
    facts = SearchLedgerFacts(
        ledger_id=row.id, state="sealed", ingested_at=row.ingested_at, region="ma-cas", now=NOW
    )
    await refresh_search_doc(conn, facts)
    await conn.execute(
        text(
            "UPDATE search_doc SET total_ttc = 1500.00, currency = 'EUR', corrected = '{}'"
            " WHERE ledger_id = :id"
        ),
        {"id": row.id},
    )
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        assert await _run(sessions, keys, engines) == 1
    found = await _values(conn, row.id)
    assert (str(found["total_ttc"]), found["currency"]) == ("1600.00", "MAD")
    assert found["corrected"] == ["total_ttc"]
    assert await _repaired(db, ready) == [
        (str(row.id), {"columns": "corrected,currency,total_ttc"})
    ]


async def test_resync_revives_a_live_document_marked_terminal_by_sql(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    """A terminal state is one-way in the ledger: a live ledger row (read under its lock) with
    a terminal search row was changed by hand, and would drop out of search and every sum."""
    row = await _live(db, conn, ready, user, DocumentState.sealed, 261)
    await conn.execute(
        text("UPDATE search_doc SET state = 'removed' WHERE ledger_id = :id"), {"id": row.id}
    )
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        assert await _run(sessions, keys, engines) == 1
    assert (await _search(conn, row.id) or {})["state"] == "sealed"
    assert await _repaired(db, ready) == [(str(row.id), {"columns": "state"})]


async def test_resync_does_not_repair_accented_tags_set_through_the_api(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    """`set_tags` orders tags by code point, the index database by its collation: `à payer`
    sorts after `achats` in one and before it in the other. A tag order is no drift."""
    row = await _live(db, conn, ready, user, DocumentState.filed, 281)
    await set_tags(conn, row.id, ["à payer", "achats", "Été"], by=user.id, now=NOW)
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        await _run(sessions, keys, engines)
    assert await _repaired(db, ready) == []


async def test_resync_does_not_audit_a_routine_state_fix_as_a_repair(
    db: AsyncSession, user: User, ready: Workspace, sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService, engines: IndexEngines, conn: AsyncConnection,
) -> None:  # fmt: skip
    """A row a best-effort write left behind (routing after extraction) is a fix, not a
    repair: its state lags the ledger's."""
    row = await _ledger(db, ready, user, DocumentState.filed, 271)
    await _search_row(conn, row, DocumentState.received, "Facture SAMPLE 271")
    await _fields(conn, row.id)
    with respx.mock(assert_all_called=False) as router:
        _paperless(router, {})
        assert await _run(sessions, keys, engines) == 1
    assert (await _values(conn, row.id))["supplier_name"] == VALUES["supplier_name"]
    assert await _repaired(db, ready) == []
    assert await _resynced(db, ready) == [{"fixed": 1, "failed": 0}]
