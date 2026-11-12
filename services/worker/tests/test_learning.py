"""`learn_from_document`: the supplier a reviewer confirmed or corrected becomes a workspace
supplier record, then a Paperless correspondent matching its ICE literally (REC-5).

Paperless is respx; the index a throw-away database behind a real `IndexEngines`.
"""

import hashlib
import json
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
from sqlalchemy import select, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sijil_core import index, suppliers
from sijil_core.crypto import LocalKeyService, seal
from sijil_core.db.models import (
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
from sijil_core.paperless import PaperlessClient, PaperlessError, PaperlessUnavailable
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.ingest import WorkspaceNotActive
from sijil_worker.learning import learn_from_document
from sijil_worker.settings import WorkerSettings
from sijil_worker.taxonomy import MATCH_LITERAL
from tests.support.index_db import throwaway_index_db

PAPERLESS = "http://paperless-ws:8000"
TOKEN = "tok-" + "l" * 36
DOC_ID = 7
CORRESPONDENT_ID = 31
NOW = datetime(2026, 11, 12, 9, 0, tzinfo=UTC)
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")
ICE = "001905837000041"  # SAMPLE: Atlas Télécom Services SARL (fictitious)
NAME = "Atlas Télécom Services SAMPLE SARL"
VALUES: dict[str, str | None] = {
    "document_type": "invoice",
    "supplier_name": NAME,
    "supplier_ice": ICE,
    "date": "2026-09-30",
    "number": "F-2026-01001",
    "total_ht": "1250.00",
    "vat": "250.00",
    "vat_rate": "20",
    "total_ttc": "1500.00",
    "currency": "MAD",
}


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
def index_db_url() -> Iterator[str]:
    """One per test: supplier records are keyed by ICE across the workspace's index."""
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


async def _document(
    db: AsyncSession,
    engines: IndexEngines,
    ws: Workspace,
    user: User,
    *,
    state: DocumentState = DocumentState.sealed,
    values: dict[str, str | None] = VALUES,
    source: str = "extraction",
) -> DocumentLedger:
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    sealed = state is DocumentState.sealed
    digest = fields_digest(values)
    row = DocumentLedger(
        workspace_id=ws.id,
        sha256=sha,
        md5="0" * 32,
        size_bytes=10,
        content_kind="pdf",
        source=DocumentSource.upload,
        object_key=f"k/{sha}",
        object_version_id="v1",
        state=state,
        ingest_state=IngestState.consumed,
        paperless_doc_id=DOC_ID,
        retention_until=date(2036, 12, 31),
        ingested_at=NOW - timedelta(days=1),
        fields_sha256=digest if sealed else None,
        sealed_at=NOW if sealed else None,
        sealed_by=user.id if sealed else None,
    )
    db.add(row)
    await db.commit()
    async with (await engines.engine(ws)).begin() as conn:
        await index.write_extraction(
            conn,
            FieldsRow(
                ledger_id=row.id,
                document_type=values.get("document_type"),
                values={name: values.get(name) for name in FIELD_NAMES},
                source=dict.fromkeys(FIELD_NAMES, source),
                updated_at=NOW,
                updated_by=user.id if source == "user" else None,
            ),
        )
        if sealed:
            await index.seal_fields(conn, row.id, fields_sha256=digest, sealed_at=NOW)
    return row


def _correspondents(created: list[dict[str, Any]]) -> Any:
    def answer(request: httpx.Request) -> httpx.Response:
        name = request.url.params["name__iexact"]
        rows = [{"id": CORRESPONDENT_ID, **c} for c in created if c["name"] == name]
        return httpx.Response(200, json={"results": rows})

    return answer


def _mock(router: respx.MockRouter) -> tuple[respx.Route, respx.Route]:
    created: list[dict[str, Any]] = []

    def create(request: httpx.Request) -> httpx.Response:
        created.append(json.loads(request.content))
        return httpx.Response(201, json={"id": CORRESPONDENT_ID})

    router.get(f"{PAPERLESS}/api/correspondents/").mock(side_effect=_correspondents(created))
    post = router.post(f"{PAPERLESS}/api/correspondents/").mock(side_effect=create)
    # The one an earlier learning created, as it left it (ours: literal, on the ICE).
    router.get(f"{PAPERLESS}/api/correspondents/{CORRESPONDENT_ID}/").respond(200, json=_ours())
    router.patch(f"{PAPERLESS}/api/correspondents/{CORRESPONDENT_ID}/").respond(200, json={})
    patch = router.patch(f"{PAPERLESS}/api/documents/{DOC_ID}/").respond(200, json={"id": DOC_ID})
    return post, patch


async def _learn(
    ledger_id: uuid.UUID,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    **over: Any,
) -> bool:
    return await learn_from_document(
        ledger_id,
        **{
            "sessions": sessions,
            "keys": keys,
            "index": engines,
            "paperless": PaperlessClient,
            "settings": SETTINGS,
            "now": NOW,
            **over,
        },
    )


async def _record(engines: IndexEngines, ws: Workspace) -> suppliers.Supplier | None:
    async with (await engines.engine(ws)).connect() as conn:
        return await suppliers.get(conn, ICE)


async def _events(db: AsyncSession, ws: Workspace) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id, AuditEvent.action == "supplier.learned")
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


@pytest.mark.db
async def test_learning_creates_paperless_correspondent_with_literal_ice_match(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user)
    with respx.mock(assert_all_called=False) as router:
        post, patch = _mock(router)
        assert await _learn(row.id, sessions, keys, engines, workspace_id=ready.id)
    assert MATCH_LITERAL == 3
    assert json.loads(post.calls.last.request.content) == {
        "name": NAME,
        "match": ICE,
        "matching_algorithm": MATCH_LITERAL,
        "is_insensitive": True,
    }
    assert post.calls.last.request.headers["Authorization"] == f"Token {TOKEN}"
    assert json.loads(patch.calls.last.request.content) == {"correspondent": CORRESPONDENT_ID}

    record = await _record(engines, ready)
    assert record is not None
    assert (record.name, record.source, record.updated_by) == (NAME, "seal", user.id)
    assert record.paperless_correspondent_id == CORRESPONDENT_ID
    [event] = await _events(db, ready)
    assert event.data == {"source": "seal", "new": True}
    assert (event.target_type, event.target_id) == ("document", str(row.id))
    dumped = json.dumps(event.data)
    for value in ("Atlas", ICE):
        assert value not in dumped


@pytest.mark.db
async def test_learning_reads_the_correction_notes(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    """The effective value wins: a note's name replaces the sealed one, the sealed one stays
    an alias, and the sealed row is untouched."""
    row = await _document(db, engines, ready, user)
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        assert await _learn(row.id, sessions, keys, engines)
    corrector = uuid.uuid4()
    async with (await engines.engine(ready)).begin() as conn:
        await index.add_correction(
            conn, row.id, "supplier_name", "Atlas Télécom SAMPLE", "The legal name changed.",
            created_by=corrector, now=NOW,
        )  # fmt: skip
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        assert await _learn(row.id, sessions, keys, engines)
    record = await _record(engines, ready)
    assert record is not None
    assert (record.name, record.aliases) == ("Atlas Télécom SAMPLE", (NAME,))
    assert (record.source, record.updated_by) == ("correction", corrector)
    assert [e.data for e in await _events(db, ready)] == [
        {"source": "seal", "new": True},
        {"source": "correction", "new": False},
    ]
    async with (await engines.engine(ready)).connect() as conn:
        frozen = await index.read_fields(conn, row.id)
    assert frozen is not None and frozen.values["supplier_name"] == NAME


@pytest.mark.db
async def test_learning_from_a_review_edit(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user, state=DocumentState.needs_review, source="user")
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        assert await _learn(row.id, sessions, keys, engines)
    record = await _record(engines, ready)
    assert record is not None
    assert (record.source, record.updated_by) == ("review", user.id)


@pytest.mark.db
@pytest.mark.parametrize("state", [DocumentState.removed, DocumentState.received])
async def test_a_removed_or_unreviewed_document_teaches_nothing(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User, state: DocumentState,
) -> None:  # fmt: skip
    removed = state is DocumentState.removed
    row = await _document(db, engines, ready, user, state=DocumentState.filed if removed else state)
    if removed:  # removed after the edit that sent the learning (the ledger's own path)
        await db.execute(
            text(
                "UPDATE document_ledger SET state = 'removed', removed_at = now(), "
                "removed_by = :u, removal_reason = 'a duplicate upload' WHERE id = :id"
            ),
            {"u": user.id, "id": row.id},
        )
        await db.commit()
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        assert not await _learn(row.id, sessions, keys, engines)
    assert not router.calls
    assert await _record(engines, ready) is None
    assert await _events(db, ready) == []


@pytest.mark.db
async def test_without_an_ice_nothing_is_learnt(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    values = VALUES | {"document_type": "receipt", "supplier_ice": None}
    row = await _document(db, engines, ready, user, values=values)
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        assert not await _learn(row.id, sessions, keys, engines)
    assert not router.calls
    assert await _events(db, ready) == []


@pytest.mark.db
async def test_learning_skips_another_regions_document(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user)
    other_region = WorkerSettings.model_construct(region="eu-fra", environment="test")
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        assert not await _learn(row.id, sessions, keys, engines, settings=other_region)
        with pytest.raises(LookupError):
            await _learn(row.id, sessions, keys, engines, workspace_id=uuid.uuid4())
    assert not router.calls
    assert await _record(engines, ready) is None


@pytest.mark.db
async def test_paperless_down_keeps_the_record_and_raises_for_a_retry(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    """The record and its audit stand; the retry sets the correspondent and audits nothing
    again (nothing new was learnt)."""
    row = await _document(db, engines, ready, user)
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{PAPERLESS}/api/correspondents/").respond(503)
        with pytest.raises(PaperlessUnavailable):
            await _learn(row.id, sessions, keys, engines)
    record = await _record(engines, ready)
    assert record is not None and record.paperless_correspondent_id is None
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        assert await _learn(row.id, sessions, keys, engines)
    record = await _record(engines, ready)
    assert record is not None and record.paperless_correspondent_id == CORRESPONDENT_ID
    assert len(await _events(db, ready)) == 1


@pytest.mark.db
async def test_a_paperless_refusal_is_logged_and_the_record_kept(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user)
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{PAPERLESS}/api/correspondents/").respond(200, json={"results": []})
        router.post(f"{PAPERLESS}/api/correspondents/").respond(400, json={"match": ["bad"]})
        assert await _learn(row.id, sessions, keys, engines)
    assert await _record(engines, ready) is not None
    assert len(await _events(db, ready)) == 1


@pytest.mark.db
async def test_learning_waits_while_the_workspace_is_not_active(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user)
    ready.state = WorkspaceState.hibernated
    await db.commit()
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        with pytest.raises(WorkspaceNotActive):
            await _learn(row.id, sessions, keys, engines)
    assert not router.calls
    assert await _record(engines, ready) is not None  # learnt; Paperless follows on the retry


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


@pytest.fixture
def eager_app() -> None:
    app = make_celery(_settings())
    app.conf.task_always_eager = True
    app.set_current()


def test_learning_task_runs_on_the_main_workers_default_queue() -> None:
    assert tasks.learn_from_document_task.name == "sijil.learn_from_document"
    assert getattr(tasks.learn_from_document_task, "queue", None) == "default"


@pytest.mark.parametrize(
    ("error", "attempts"),
    [
        (PaperlessUnavailable("down"), tasks.LEARN_MAX_RETRIES + 1),
        (WorkspaceNotActive(), tasks.LEARN_MAX_RETRIES + 1),
        (PaperlessError("HTTP 400", 400), 1),
    ],
)
def test_learning_task_retries_transient_errors_only(
    eager_app: None, monkeypatch: pytest.MonkeyPatch, error: Exception, attempts: int
) -> None:
    calls: list[str] = []

    async def learn(ledger_id: str, workspace_id: str) -> None:
        calls.append(ledger_id)
        raise error

    monkeypatch.setattr(tasks, "_learn", learn)
    result = tasks.learn_from_document_task.apply(kwargs=ARGS)
    assert isinstance(result.result, type(error))
    assert len(calls) == attempts


# Renaming the correspondent -----------------------------------------------------------------

RENAMED = "Atlas Télécom SAMPLE"


async def _learnt_then_renamed(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ws: Workspace, user: User,
) -> DocumentLedger:  # fmt: skip
    """Learnt (our correspondent CORRESPONDENT_ID created), then a note renames the supplier."""
    row = await _document(db, engines, ws, user)
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        assert await _learn(row.id, sessions, keys, engines)
    async with (await engines.engine(ws)).begin() as conn:
        await index.add_correction(
            conn, row.id, "supplier_name", RENAMED, "The legal name changed.",
            created_by=user.id, now=NOW,
        )  # fmt: skip
    return row


def _ours(**over: Any) -> dict[str, Any]:
    return {
        "id": CORRESPONDENT_ID, "name": NAME, "match": ICE,
        "matching_algorithm": MATCH_LITERAL, "is_insensitive": True,
    } | over  # fmt: skip


@pytest.mark.db
async def test_a_rename_renames_the_correspondent_we_created(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    row = await _learnt_then_renamed(db, sessions, keys, engines, ready, user)
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{PAPERLESS}/api/correspondents/{CORRESPONDENT_ID}/").respond(200, json=_ours())
        rename = router.patch(f"{PAPERLESS}/api/correspondents/{CORRESPONDENT_ID}/").respond(
            200, json=_ours(name=RENAMED)
        )
        create = router.post(f"{PAPERLESS}/api/correspondents/").respond(201, json={"id": 99})
        assign = router.patch(f"{PAPERLESS}/api/documents/{DOC_ID}/").respond(200, json={})
        assert await _learn(row.id, sessions, keys, engines)
    assert json.loads(rename.calls.last.request.content) == {"name": RENAMED}
    assert not create.called
    assert json.loads(assign.calls.last.request.content) == {"correspondent": CORRESPONDENT_ID}
    record = await _record(engines, ready)
    assert record is not None
    assert (record.name, record.paperless_correspondent_id) == (RENAMED, CORRESPONDENT_ID)


@pytest.mark.db
@pytest.mark.parametrize(
    "stored",
    [
        _ours(matching_algorithm=6),  # the tenant made it automatic
        _ours(match="Atlas"),  # the tenant changed its rule
        None,  # deleted in Paperless
    ],
    ids=["algorithm", "match", "gone"],
)
async def test_a_correspondent_no_longer_ours_is_never_renamed(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User, stored: dict[str, Any] | None,
) -> None:  # fmt: skip
    row = await _learnt_then_renamed(db, sessions, keys, engines, ready, user)
    with respx.mock(assert_all_called=False) as router:
        if stored is None:
            router.get(f"{PAPERLESS}/api/correspondents/{CORRESPONDENT_ID}/").respond(404)
        else:
            router.get(f"{PAPERLESS}/api/correspondents/{CORRESPONDENT_ID}/").respond(
                200, json=stored
            )
        rename = router.patch(f"{PAPERLESS}/api/correspondents/{CORRESPONDENT_ID}/")
        router.get(f"{PAPERLESS}/api/correspondents/").respond(200, json={"results": []})
        create = router.post(f"{PAPERLESS}/api/correspondents/").respond(201, json={"id": 99})
        router.patch(f"{PAPERLESS}/api/documents/{DOC_ID}/").respond(200, json={})
        assert await _learn(row.id, sessions, keys, engines)
    assert not rename.called
    assert json.loads(create.calls.last.request.content)["name"] == RENAMED
    record = await _record(engines, ready)
    assert record is not None and record.paperless_correspondent_id == 99
