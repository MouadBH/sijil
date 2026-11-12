"""Learning a supplier is a path that writes beside document data (CLAUDE.md rule 4, REC-5).

`sijil.learn_from_document` reads a document's effective values and writes the workspace's
`supplier` record, then sets a correspondent on the document's Paperless copy. It never changes
the sealed `document_fields` row, its `fields_sha256` or the ledger, and a document already
removed (or deleted) teaches nothing and gets no Paperless call. A record is workspace knowledge:
the purge of the document it was learnt from leaves it.
"""

import hashlib
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
import respx
from sqlalchemy import text
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
    CredentialKind,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    TenantCredential,
    User,
    Workspace,
)
from sijil_core.fields import FIELD_NAMES, fields_digest
from sijil_core.index import FieldsRow, IndexEngines
from sijil_core.paperless import PaperlessClient
from sijil_worker.learning import learn_from_document
from sijil_worker.settings import WorkerSettings
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

PAPERLESS = "http://paperless-ws:8000"
NOW = datetime(2026, 11, 12, 9, 0, tzinfo=UTC)
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")
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
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
async def engines(
    db: AsyncSession, workspace: Workspace, index_db_url: str, keys: LocalKeyService
) -> AsyncIterator[IndexEngines]:
    workspace.paperless_url = PAPERLESS
    secrets = {CredentialKind.service_token: "tok", CredentialKind.index_db_password: "pw"}
    for kind, secret in secrets.items():
        envelope = seal(secret.encode(), keys, workspace.kms_key_id, f"{workspace.id}:{kind}")
        db.add(TenantCredential(workspace_id=workspace.id, kind=kind, envelope=envelope.to_bytes()))
    await db.commit()

    def factory(_url: URL) -> AsyncEngine:
        return create_async_engine(index_db_url)

    sessions = async_sessionmaker(db.bind, expire_on_commit=False)
    found = IndexEngines(SETTINGS, keys, sessions, engine_factory=factory)
    yield found
    await found.aclose()


async def _document(
    db: AsyncSession, engines: IndexEngines, ws: Workspace, user: User, *, sealed: bool = True
) -> Any:
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    digest = fields_digest(VALUES)
    row = DocumentLedger(
        workspace_id=ws.id, sha256=sha, md5="0" * 32, size_bytes=6, content_kind="pdf",
        source=DocumentSource.upload, object_key=f"k/{sha}", object_version_id="v1",
        state=DocumentState.sealed if sealed else DocumentState.filed,
        ingest_state=IngestState.consumed, paperless_doc_id=7,
        retention_until=date(2036, 12, 31), ingested_at=NOW - timedelta(days=1),
        fields_sha256=digest if sealed else None, sealed_at=NOW if sealed else None,
        sealed_by=user.id if sealed else None,
    )  # fmt: skip
    db.add(row)
    await db.commit()
    async with (await engines.engine(ws)).begin() as conn:
        await index.write_extraction(
            conn, FieldsRow(ledger_id=row.id, values=VALUES, updated_at=NOW)
        )
        if sealed:
            await index.seal_fields(conn, row.id, fields_sha256=digest, sealed_at=NOW)
    return row


def _paperless(router: respx.MockRouter) -> None:
    router.get(f"{PAPERLESS}/api/correspondents/").respond(200, json={"results": []})
    router.post(f"{PAPERLESS}/api/correspondents/").respond(201, json={"id": 3})
    router.patch(f"{PAPERLESS}/api/documents/7/").respond(200, json={"id": 7})


async def _learn(db: AsyncSession, engines: IndexEngines, keys: Any, ledger_id: Any) -> bool:
    return await learn_from_document(
        ledger_id, sessions=async_sessionmaker(db.bind, expire_on_commit=False), keys=keys,
        index=engines, paperless=PaperlessClient, settings=SETTINGS, now=NOW,
    )  # fmt: skip


async def test_learning_never_changes_the_sealed_row_or_the_ledger(
    db: AsyncSession, engines: IndexEngines, keys: LocalKeyService, workspace: Workspace,
    user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, workspace, user)
    async with (await engines.engine(workspace)).begin() as conn:
        await index.add_correction(
            conn, row.id, "supplier_name", "Atlas SAMPLE Corrigé", "The legal name changed.",
            created_by=user.id, now=NOW,
        )  # fmt: skip
    ledger_before = (
        await db.execute(
            text("SELECT state, fields_sha256, sealed_at, retention_until FROM document_ledger "
                 "WHERE id = :id"), {"id": row.id},
        )
    ).one()  # fmt: skip
    with respx.mock(assert_all_called=True) as router:
        _paperless(router)
        assert await _learn(db, engines, keys, row.id)
    async with (await engines.engine(workspace)).connect() as conn:
        frozen = await index.read_fields(conn, row.id)
        record = await suppliers.get(conn, "001905837000041")
    assert frozen is not None
    assert dict(frozen.values) == VALUES
    assert frozen.fields_sha256 == fields_digest(VALUES) and frozen.sealed_at == NOW
    assert record is not None and record.name == "Atlas SAMPLE Corrigé"
    ledger_after = (
        await db.execute(
            text("SELECT state, fields_sha256, sealed_at, retention_until FROM document_ledger "
                 "WHERE id = :id"), {"id": row.id},
        )
    ).one()  # fmt: skip
    assert ledger_after == ledger_before


async def test_a_removed_document_teaches_nothing_and_its_paperless_copy_is_untouched(
    db: AsyncSession, engines: IndexEngines, keys: LocalKeyService, workspace: Workspace,
    user: User,
) -> None:  # fmt: skip
    """Removed after the edit that sent the learning (the ledger's own removal UPDATE)."""
    row = await _document(db, engines, workspace, user, sealed=False)
    await db.execute(
        text(
            "UPDATE document_ledger SET state = 'removed', removed_at = now(), "
            "removed_by = :u, removal_reason = 'a duplicate upload' WHERE id = :id"
        ),
        {"u": user.id, "id": row.id},
    )
    await db.commit()
    with respx.mock(assert_all_called=False) as router:
        _paperless(router)
        assert not await _learn(db, engines, keys, row.id)
    assert not router.calls
    async with (await engines.engine(workspace)).connect() as conn:
        assert await suppliers.get(conn, "001905837000041") is None


async def test_the_purge_leaves_the_learnt_record(
    db: AsyncSession, engines: IndexEngines, keys: LocalKeyService, workspace: Workspace,
    user: User,
) -> None:  # fmt: skip
    row = await _document(db, engines, workspace, user)
    with respx.mock(assert_all_called=False) as router:
        _paperless(router)
        assert await _learn(db, engines, keys, row.id)
    async with (await engines.engine(workspace)).begin() as conn:
        await index.purge_document(conn, row.id, sealed=True)
        assert await index.read_fields(conn, row.id) is None
        assert await suppliers.get(conn, "001905837000041") is not None
