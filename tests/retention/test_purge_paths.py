"""The purge is a path that deletes (CLAUDE.md rule 4): a document's Paperless copies with the
tenant admin's credential, and its index rows, the sealed one included for an approved deletion.
It runs only once the ledger says the original is gone, purges a sealed index row only for
`deleted`, never deletes with the service token, and asserts the region before anything else.

Its everyday behaviour is services/worker/tests/test_purge.py; Paperless's own rules it relies on
are tests/integration/test_paperless_contract.py.
"""

import hashlib
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import select, text
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
    TenantCredential,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.fields import FIELD_NAMES, fields_digest
from sijil_core.index import FieldsRow, IndexEngines, RetentionLocked
from sijil_core.paperless import PaperlessClient
from sijil_worker.purge import TooManyMatches, purge_document
from sijil_worker.settings import WorkerSettings
from tests.support.fake_paperless import FakePaperless, fake_paperless
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

PAPERLESS = "http://paperless-lock:8000"
SERVICE_TOKEN = "svc-" + "s" * 36
ADMIN_PASSWORD = "admin-" + "p" * 26
ADMIN_TOKEN = "adm-" + "a" * 36
DOC_ID = 7
NOW = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")
# SAMPLE values (fictitious supplier).
VALUES: dict[str, str | None] = dict.fromkeys(FIELD_NAMES) | {
    "document_type": "invoice",
    "supplier_name": "Atlas Télécom SAMPLE SARL",
    "total_ttc": "1500.00",
}
_ACTOR_SQL = {
    "removed": [
        "UPDATE document_ledger SET state = 'removed', removed_at = now(), removed_by = :u,"
        " removal_reason = 'Déposé par erreur (SAMPLE)' WHERE id = :id"
    ],
    "deleting": [
        "UPDATE document_ledger SET state = 'deleting', deletion_approved_at = now(),"
        " deletion_approved_by = :u WHERE id = :id"
    ],
}
_ORIGINAL_GONE = "UPDATE document_ledger SET original_removed_at = now() WHERE id = :id"
_DELETED = (
    "UPDATE document_ledger SET state = 'deleted', deleted_at = now(),"
    " original_removed_at = now() WHERE id = :id"
)


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sijil_core.paperless._BACKOFF_S", 0)


@pytest.fixture(scope="module")
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
def keys() -> LocalKeyService:
    return LocalKeyService(b"k" * 32)


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


@pytest.fixture
async def ws(db: AsyncSession, workspace: Workspace, keys: LocalKeyService) -> Workspace:
    """Active, with a Paperless, the tenant admin's password, the service token (which must
    never be used to delete) and the index password."""
    workspace.paperless_url = PAPERLESS
    for kind, secret in (
        (CredentialKind.admin_password, ADMIN_PASSWORD),
        (CredentialKind.service_token, SERVICE_TOKEN),
        (CredentialKind.index_db_password, "pw"),
    ):
        envelope = seal(secret.encode(), keys, workspace.kms_key_id, f"{workspace.id}:{kind}")
        db.add(TenantCredential(workspace_id=workspace.id, kind=kind, envelope=envelope.to_bytes()))
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
def fake() -> Iterator[FakePaperless]:
    with fake_paperless(PAPERLESS, ADMIN_PASSWORD, ADMIN_TOKEN) as found:
        yield found


async def _row(
    db: AsyncSession,
    engines: IndexEngines,
    ws: Workspace,
    user: User,
    state: DocumentState,
    *,
    original_gone: bool = True,
    index_sealed: bool = False,
) -> DocumentLedger:
    """A row brought to `state` by the transitions 0011 allows, its index row (sealed when the
    ledger is, or when `index_sealed`: a half-done seal) and its copy in the fake Paperless."""
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    sealed = state in (DocumentState.sealed, DocumentState.deleting, DocumentState.deleted)
    over = state in (DocumentState.deleting, DocumentState.deleted)
    row = DocumentLedger(
        workspace_id=ws.id, sha256=sha, md5="0" * 32, size_bytes=10, content_kind="pdf",
        source=DocumentSource.upload, object_key=f"k/{sha}", object_version_id="v1",
        state=DocumentState.sealed if sealed else DocumentState.filed,
        ingest_state=IngestState.consumed, paperless_doc_id=DOC_ID,
        retention_until=date(2020, 12, 31) if over else date(2036, 12, 31),
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
        if sealed or index_sealed:
            await index.seal_fields(
                conn, row.id, fields_sha256=fields_digest(VALUES), sealed_at=NOW
            )
    statements: list[str] = []
    if state is DocumentState.removed:
        statements = _ACTOR_SQL["removed"] + ([_ORIGINAL_GONE] if original_gone else [])
    elif state is DocumentState.deleting:
        statements = list(_ACTOR_SQL["deleting"])
    elif state is DocumentState.deleted:
        statements = [*_ACTOR_SQL["deleting"], _DELETED]
    for statement in statements:
        await db.execute(text(statement), {"id": row.id, "u": user.id})
    await db.commit()
    return row


def paperless(base_url: str, **kw: Any) -> PaperlessClient:
    return PaperlessClient(base_url, **kw)


async def _purge(row: DocumentLedger, sessions: Any, keys: Any, engines: Any, **over: Any) -> bool:
    args: dict[str, Any] = {
        "sessions": sessions, "keys": keys, "index": engines, "paperless": paperless,
        "settings": SETTINGS, "now": NOW, "workspace_id": row.workspace_id, **over,
    }  # fmt: skip
    return await purge_document(row.id, **args)


async def _index_row(engines: IndexEngines, ws: Workspace, ledger_id: uuid.UUID) -> Any:
    async with (await engines.engine(ws)).connect() as conn:
        return await index.read_fields(conn, ledger_id)


async def _purged(db: AsyncSession, ws: Workspace) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent).where(
            AuditEvent.workspace_id == ws.id, AuditEvent.action == "document.purged"
        )
    )
    return list(rows)


@pytest.mark.parametrize(
    ("state", "original_gone"),
    [
        (DocumentState.filed, False),
        (DocumentState.sealed, False),
        (DocumentState.deleting, False),  # approved, but the original is not removed yet
        (DocumentState.removed, False),  # removed, but the original is not removed yet
    ],
)
async def test_no_purge_unless_removed_or_deleted_with_the_original_gone(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ws: Workspace,
    user: User, fake: FakePaperless, state: DocumentState, original_gone: bool,
) -> None:  # fmt: skip
    """A forged, stale or early message deletes nothing: no Paperless call at all, the index
    row (sealed or not) kept, nothing recorded."""
    row = await _row(db, engines, ws, user, state, original_gone=original_gone)
    fake.add(DOC_ID, row.sha256)
    assert not await _purge(row, sessions, keys, engines)
    assert fake.calls == []
    assert DOC_ID in fake.live
    assert await _index_row(engines, ws, row.id) is not None
    assert await _purged(db, ws) == []


async def test_a_sealed_index_row_is_purged_for_an_approved_deletion(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ws: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    row = await _row(db, engines, ws, user, DocumentState.deleted)
    assert await _purge(row, sessions, keys, engines)
    assert await _index_row(engines, ws, row.id) is None


async def test_a_removed_document_never_purges_a_sealed_index_row(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ws: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    """`removed` purges without the sealed gate: a frozen index row (values a person confirmed,
    which the API never lets be removed) is refused by the index itself, kept, and the purge
    is not recorded."""
    row = await _row(db, engines, ws, user, DocumentState.removed, index_sealed=True)
    with pytest.raises(RetentionLocked):
        await _purge(row, sessions, keys, engines)
    frozen = await _index_row(engines, ws, row.id)
    assert frozen is not None
    assert frozen.sealed_at is not None
    assert await _purged(db, ws) == []


@pytest.mark.parametrize("state", [DocumentState.removed, DocumentState.deleted])
async def test_the_service_token_never_deletes(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ws: Workspace,
    user: User, fake: FakePaperless, state: DocumentState,
) -> None:  # fmt: skip
    row = await _row(db, engines, ws, user, state)
    fake.add(DOC_ID, row.sha256)
    assert await _purge(row, sessions, keys, engines)
    assert ("DELETE", f"/api/documents/{DOC_ID}/") in fake.methods()
    assert all(auth != f"Token {SERVICE_TOKEN}" for _, _, auth, _ in fake.calls)
    writes = [c for c in fake.calls if c[0] in ("DELETE", "POST") and c[1] != "/api/token/"]
    assert writes
    assert {auth for _, _, auth, _ in writes} == {f"Token {ADMIN_TOKEN}"}


async def test_the_region_is_asserted_first(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ws: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    """Another region's document: nothing is decrypted, sent or deleted, before the workspace's
    state or credential is even looked at (a hibernated workspace without an admin password
    here would otherwise raise)."""
    row = await _row(db, engines, ws, user, DocumentState.removed)
    fake.add(DOC_ID, row.sha256)
    ws.state = WorkspaceState.hibernated
    await db.execute(
        text("DELETE FROM tenant_credential WHERE workspace_id = :w AND kind = 'admin_password'"),
        {"w": ws.id},
    )
    await db.commit()
    elsewhere = WorkerSettings.model_construct(region="fr-par", environment="test")
    assert not await _purge(row, sessions, keys, engines, settings=elsewhere)
    assert fake.calls == []
    assert DOC_ID in fake.live
    assert await _index_row(engines, ws, row.id) is not None
    assert await _purged(db, ws) == []


@pytest.mark.parametrize("state", [DocumentState.removed, DocumentState.deleted])
async def test_a_filter_paperless_ignores_never_widens_the_purge(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ws: Workspace,
    user: User, fake: FakePaperless, state: DocumentState,
) -> None:  # fmt: skip
    """The one automatic bulk delete never trusts Paperless's filter alone: when it answers every
    document, only the target's copy goes and the working copies of sealed documents stay."""
    row = await _row(db, engines, ws, user, state)
    fake.ignore_checksum_filter = True
    fake.add(DOC_ID, row.sha256)
    for doc_id in range(20, 25):  # other documents, sealed ones among them
        fake.add(doc_id, hashlib.sha256(f"sealed-{doc_id}".encode()).hexdigest())
    assert await _purge(row, sessions, keys, engines)
    assert sorted(fake.live) == list(range(20, 25))
    assert [p for m, p in fake.methods() if m == "DELETE"] == [f"/api/documents/{DOC_ID}/"]


async def test_more_than_ten_matches_deletes_nothing(
    db: AsyncSession, sessions: Any, keys: Any, engines: IndexEngines, ws: Workspace,
    user: User, fake: FakePaperless,
) -> None:  # fmt: skip
    """More than ten true matches: the purge refuses before any delete and records nothing."""
    row = await _row(db, engines, ws, user, DocumentState.removed)
    for doc_id in range(DOC_ID, DOC_ID + 11):
        fake.add(doc_id, row.sha256)
    with pytest.raises(TooManyMatches):
        await _purge(row, sessions, keys, engines)
    assert len(fake.live) == 11
    assert not any(m == "DELETE" or (m == "POST" and p == "/api/trash/")
                   for m, p in fake.methods())  # fmt: skip
    assert await _index_row(engines, ws, row.id) is not None
    assert await _purged(db, ws) == []
