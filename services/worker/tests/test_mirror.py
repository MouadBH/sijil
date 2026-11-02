"""`mirror_fields`: a sealed document's fields copied into its own Paperless as custom fields.

Paperless is respx; the index a throw-away database behind a real `IndexEngines`.
"""

import hashlib
import json
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime
from typing import Any

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sijil_core import index
from sijil_core.crypto import LocalKeyService, seal
from sijil_core.db.models import (
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    IntegrityAlert,
    TenantCredential,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.fields import DOCUMENT_TYPES, FIELD_NAMES, fields_digest
from sijil_core.index import FieldsRow, IndexEngines
from sijil_core.paperless import PaperlessClient, PaperlessError, PaperlessUnavailable
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.ingest import WorkspaceNotActive
from sijil_worker.mirror import CUSTOM_FIELDS, mirror_fields
from sijil_worker.settings import WorkerSettings
from tests.support.index_db import throwaway_index_db

PAPERLESS = "http://paperless-ws:8000"
TOKEN = "tok-" + "m" * 36
DOC_ID = 7
NOW = datetime(2026, 10, 5, 9, 0, tzinfo=UTC)
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")
TYPE_IDS = {name: n + 1 for n, name in enumerate(DOCUMENT_TYPES)}
FIELD_IDS = {name: 100 + n for n, (name, _, _) in enumerate(CUSTOM_FIELDS)}
# SAMPLE values (fictitious supplier).
VALUES: dict[str, str | None] = {
    "document_type": "invoice",
    "supplier_name": "Atlas Télécom SAMPLE SARL",
    "supplier_ice": "001905837000041",
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


@pytest.fixture(scope="module")
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


async def _document(
    db: AsyncSession,
    engines: IndexEngines,
    ws: Workspace,
    user: User,
    *,
    sealed: bool = True,
    values: dict[str, str | None] = VALUES,
    ledger_digest: str | None = None,
) -> DocumentLedger:
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
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
        state=DocumentState.sealed if sealed else DocumentState.filed,
        ingest_state=IngestState.consumed,
        paperless_doc_id=DOC_ID,
        retention_until=date(2036, 12, 31),
        ingested_at=NOW,
        fields_sha256=(ledger_digest or digest) if sealed else None,
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
                source=dict.fromkeys(FIELD_NAMES, "extraction"),
                updated_at=NOW,
            ),
        )
        if sealed:
            await index.seal_fields(conn, row.id, fields_sha256=digest, sealed_at=NOW)
    return row


def _custom_fields(request: httpx.Request) -> httpx.Response:
    name = request.url.params["name__iexact"]
    return httpx.Response(200, json={"results": [{"id": FIELD_IDS[name], "name": name}]})


def _document_types(request: httpx.Request) -> httpx.Response:
    name = request.url.params["name__iexact"]
    return httpx.Response(200, json={"results": [{"id": TYPE_IDS[name], "name": name}]})


def _mock(router: respx.MockRouter) -> respx.Route:
    router.get(f"{PAPERLESS}/api/custom_fields/").mock(side_effect=_custom_fields)
    router.get(f"{PAPERLESS}/api/document_types/").mock(side_effect=_document_types)
    router.get(f"{PAPERLESS}/api/documents/{DOC_ID}/").respond(
        200, json={"id": DOC_ID, "custom_fields": [{"field": 1, "value": "tenant's own"}]}
    )
    return router.patch(f"{PAPERLESS}/api/documents/{DOC_ID}/").respond(200, json={"id": DOC_ID})


async def _mirror(
    ledger_id: uuid.UUID,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    **over: Any,
) -> bool:
    return await mirror_fields(
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


async def _events(db: AsyncSession, ws: Workspace) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id)
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


@pytest.mark.db
async def test_mirror_sets_custom_fields(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user)
    with respx.mock(assert_all_called=True) as router:
        patch = _mock(router)
        assert await _mirror(row.id, sessions, keys, engines, workspace_id=ready.id)
    request = patch.calls.last.request
    assert request.headers["Authorization"] == f"Token {TOKEN}"
    body = json.loads(request.content)
    assert body["document_type"] == TYPE_IDS["invoice"]
    sent = {f["field"]: f["value"] for f in body["custom_fields"]}
    assert sent == {
        1: "tenant's own",  # the tenant's own custom fields are kept
        FIELD_IDS["sijil_supplier"]: "Atlas Télécom SAMPLE SARL",
        FIELD_IDS["sijil_ice"]: "001905837000041",
        FIELD_IDS["sijil_number"]: "F-2026-01001",
        FIELD_IDS["sijil_date"]: "2026-09-30",
        FIELD_IDS["sijil_total_ht"]: "MAD1250.00",
        FIELD_IDS["sijil_vat"]: "MAD250.00",
        FIELD_IDS["sijil_total_ttc"]: "MAD1500.00",
        FIELD_IDS["sijil_sealed"]: True,
    }
    [event] = await _events(db, ready)
    assert event.action == "document.fields_mirrored"
    assert (event.target_type, event.target_id) == ("document", str(row.id))
    dumped = json.dumps(event.data)
    for value in ("Atlas", "001905837000041", "1500", "F-2026"):
        assert value not in dumped


def test_custom_field_names_and_types() -> None:
    assert [(name, kind) for name, kind, _ in CUSTOM_FIELDS] == [
        ("sijil_supplier", "string"),
        ("sijil_ice", "string"),
        ("sijil_number", "string"),
        ("sijil_date", "date"),
        ("sijil_total_ht", "monetary"),
        ("sijil_vat", "monetary"),
        ("sijil_total_ttc", "monetary"),
        ("sijil_sealed", "boolean"),
    ]


@pytest.mark.db
async def test_mirror_creates_missing_custom_fields_and_skips_empty_values(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    contract: dict[str, str | None] = {"document_type": "contract", "date": "2026-09-30"}
    row = await _document(db, engines, ready, user, values=contract)
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{PAPERLESS}/api/custom_fields/").respond(200, json={"results": []})
        created = router.post(f"{PAPERLESS}/api/custom_fields/").mock(
            side_effect=lambda request: httpx.Response(
                201, json={"id": FIELD_IDS[json.loads(request.content)["name"]]}
            )
        )
        router.get(f"{PAPERLESS}/api/document_types/").mock(side_effect=_document_types)
        router.get(f"{PAPERLESS}/api/documents/{DOC_ID}/").respond(
            200, json={"id": DOC_ID, "custom_fields": []}
        )
        patch = router.patch(f"{PAPERLESS}/api/documents/{DOC_ID}/").respond(200, json={})
        assert await _mirror(row.id, sessions, keys, engines)
    assert created.call_count == len(CUSTOM_FIELDS)
    body = json.loads(patch.calls.last.request.content)
    assert body["document_type"] == TYPE_IDS["contract"]
    assert {f["field"]: f["value"] for f in body["custom_fields"]} == {
        FIELD_IDS["sijil_date"]: "2026-09-30",
        FIELD_IDS["sijil_sealed"]: True,
    }


@pytest.mark.db
async def test_mirror_is_a_noop_unless_sealed(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user, sealed=False)
    with respx.mock(assert_all_called=False) as router:
        patch = _mock(router)
        assert not await _mirror(row.id, sessions, keys, engines)
    assert not patch.called
    assert not router.calls
    assert await _events(db, ready) == []


@pytest.mark.db
async def test_mirror_skips_another_regions_document(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user)
    other_region = WorkerSettings.model_construct(region="eu-fra", environment="test")
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        assert not await _mirror(row.id, sessions, keys, engines, settings=other_region)
    assert not router.calls
    assert await _events(db, ready) == []


@pytest.mark.db
async def test_mirror_of_another_workspace_is_refused(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user)
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        with pytest.raises(LookupError):
            await _mirror(row.id, sessions, keys, engines, workspace_id=uuid.uuid4())
    assert not router.calls


@pytest.mark.db
async def test_mirror_paperless_down_raises_for_a_retry(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user)
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{PAPERLESS}/api/custom_fields/").respond(503)
        with pytest.raises(PaperlessUnavailable):
            await _mirror(row.id, sessions, keys, engines)
    assert await _events(db, ready) == []


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


def test_mirror_task_runs_on_the_main_workers_default_queue() -> None:
    assert tasks.mirror_fields_task.name == "sijil.mirror_fields"
    assert getattr(tasks.mirror_fields_task, "queue", None) == "default"


@pytest.mark.parametrize(
    ("error", "attempts"),
    [
        (PaperlessUnavailable("down"), tasks.MIRROR_MAX_RETRIES + 1),
        (PaperlessError("HTTP 400", 400), 1),
    ],
)
def test_mirror_task_retries_transient_errors_only(
    eager_app: None, monkeypatch: pytest.MonkeyPatch, error: Exception, attempts: int
) -> None:
    calls: list[str] = []

    async def mirror(ledger_id: str, workspace_id: str) -> None:
        calls.append(ledger_id)
        raise error

    monkeypatch.setattr(tasks, "_mirror", mirror)
    result = tasks.mirror_fields_task.apply(kwargs=ARGS)
    assert isinstance(result.result, type(error))
    assert len(calls) == attempts


@pytest.mark.db
async def test_mirror_refuses_values_that_do_not_hash_to_the_sealed_digest(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User, caplog: pytest.LogCaptureFixture,
) -> None:  # fmt: skip
    """The index row's values are recomputed into the digest: when they are not what the
    ledger sealed, nothing reaches Paperless and an integrity alert says so."""
    row = await _document(db, engines, ready, user, ledger_digest="e" * 64)
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        assert not await _mirror(row.id, sessions, keys, engines)
    assert not router.calls
    assert "mirror.hash_mismatch" in caplog.text
    alerts = list(
        await db.scalars(select(IntegrityAlert).where(IntegrityAlert.ledger_id == row.id))
    )
    assert [a.kind for a in alerts] == ["fields_hash_mismatch"]
    # Deduplicated while open.
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        await _mirror(row.id, sessions, keys, engines)
    again = list(await db.scalars(select(IntegrityAlert).where(IntegrityAlert.ledger_id == row.id)))
    assert len(again) == 1


@pytest.mark.db
@pytest.mark.parametrize("state", [WorkspaceState.hibernated, WorkspaceState.provisioning])
async def test_mirror_waits_while_the_workspace_is_not_active(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], keys: LocalKeyService,
    engines: IndexEngines, ready: Workspace, user: User, state: WorkspaceState,
) -> None:  # fmt: skip
    row = await _document(db, engines, ready, user)
    ready.state = state
    await db.commit()
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        with pytest.raises(WorkspaceNotActive):
            await _mirror(row.id, sessions, keys, engines)
    assert not router.calls
    assert tasks.is_mirror_transient(WorkspaceNotActive())
