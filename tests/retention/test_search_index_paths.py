"""The search index is a path that writes and deletes document data (CLAUDE.md rule 4).

`search_doc` rows leave only through `index.purge_document` (the `sijil.purge` gate, opened for
one ledger id): a plain DELETE or TRUNCATE is refused like on every other index table.
`document_tag` rows also leave by a plain DELETE since index schema v4 (removing a tag, an
annotation: test_tag_paths.py); TRUNCATE is still refused. Writing search rows (upsert,
state marks, tags) never touches the sealed `document_fields` row, its values or its
`fields_sha256`. The daily resync purges the search rows of a document whose purge is done, and
nothing else.
"""

import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime

import pytest
import respx
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError
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
from sijil_core.search_index import (
    SearchLedgerFacts,
    mark_search_state,
    refresh_search_doc,
)
from sijil_worker.search import resync_search
from sijil_worker.settings import WorkerSettings
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

NOW = datetime(2026, 11, 9, 10, 0, tzinfo=UTC)
ARRIVAL = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
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
SEARCH_TABLES = ("search_doc", "document_tag")
PAPERLESS = "http://paperless-search-lock:8000"
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")


@pytest.fixture(scope="module")
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
async def conn(index_db_url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(index_db_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as connection:
        await index.ensure_schema(connection)
        yield connection
    await engine.dispose()


async def _document(conn: AsyncConnection, *, sealed: bool) -> uuid.UUID:
    """A fields row (sealed or not), its search row and two tags."""
    ledger_id = uuid.uuid4()
    await index.write_extraction(
        conn,
        FieldsRow(
            ledger_id=ledger_id, document_type="invoice", values=VALUES,
            source=dict.fromkeys(VALUES, "extraction"), updated_at=ARRIVAL,
        ),
    )  # fmt: skip
    if sealed:
        await index.seal_fields(conn, ledger_id, fields_sha256=fields_digest(VALUES), sealed_at=NOW)
    await _tag(conn, ledger_id, "bureau")
    await _tag(conn, ledger_id, "urgent")
    state = DocumentState.sealed if sealed else DocumentState.filed
    await refresh_search_doc(conn, _facts(ledger_id, state), "Facture SAMPLE Télécom")
    return ledger_id


def _facts(ledger_id: uuid.UUID, state: str) -> SearchLedgerFacts:
    return SearchLedgerFacts(
        ledger_id=ledger_id, state=state, ingested_at=ARRIVAL, region="ma-cas", now=NOW
    )


async def _tag(conn: AsyncConnection, ledger_id: uuid.UUID, tag: str) -> None:
    await conn.execute(
        text(
            "INSERT INTO document_tag (ledger_id, tag, created_by, created_at) "
            "VALUES (:id, :tag, :by, :at)"
        ),
        {"id": ledger_id, "tag": tag, "by": uuid.uuid4(), "at": NOW},
    )


async def _count(conn: AsyncConnection, table: str, ledger_id: uuid.UUID) -> int:
    found = await conn.scalar(
        text(f"SELECT count(*) FROM {table} WHERE ledger_id = :id"),  # noqa: S608
        {"id": ledger_id},
    )
    return int(found or 0)


@pytest.mark.parametrize("sealed", [False, True])
async def test_purge_deletes_search_doc_and_tags(conn: AsyncConnection, sealed: bool) -> None:
    ledger_id = await _document(conn, sealed=sealed)
    other = await _document(conn, sealed=sealed)
    if sealed:
        await mark_search_state(conn, ledger_id, DocumentState.deleting)
    else:
        await mark_search_state(conn, ledger_id, DocumentState.removed)
    await index.purge_document(conn, ledger_id, sealed=sealed)
    for table in (*SEARCH_TABLES, "document_fields"):
        assert await _count(conn, table, ledger_id) == 0
        assert await _count(conn, table, other) > 0, "another document's rows were touched"
    await index.purge_document(conn, ledger_id, sealed=sealed)  # idempotent


async def test_plain_delete_of_search_doc_refused(conn: AsyncConnection) -> None:
    """`search_doc` keeps its gate; `document_tag` only refuses TRUNCATE (index schema v4: a tag
    is removed by a plain DELETE, tests/retention/test_tag_paths.py)."""
    table = "search_doc"
    ledger_id = await _document(conn, sealed=False)
    with pytest.raises(DBAPIError, match="retention.locked"):
        await conn.execute(text("TRUNCATE document_tag"))
    with pytest.raises(DBAPIError, match="retention.locked"):
        await conn.execute(
            text(f"DELETE FROM {table} WHERE ledger_id = :id"),  # noqa: S608
            {"id": ledger_id},
        )
    with pytest.raises(DBAPIError, match="retention.locked"):
        await conn.execute(text(f"TRUNCATE {table}"))
    # A gate opened for another document does not open this one.
    await conn.execute(
        text("SELECT set_config('sijil.purge', :other, false)"), {"other": str(uuid.uuid4())}
    )
    try:
        with pytest.raises(DBAPIError, match="retention.locked"):
            await conn.execute(
                text(f"DELETE FROM {table} WHERE ledger_id = :id"),  # noqa: S608
                {"id": ledger_id},
            )
    finally:
        await conn.execute(text("SELECT set_config('sijil.purge', '', false)"))
    assert await _count(conn, table, ledger_id) > 0


async def test_search_doc_never_changes_sealed_fields(conn: AsyncConnection) -> None:
    ledger_id = await _document(conn, sealed=True)
    before = await index.read_fields(conn, ledger_id)
    assert before is not None and before.sealed_at is not None
    await index.add_correction(
        conn, ledger_id, "total_ttc", "1600.00", "Avoir SAMPLE reçu après le scellement",
        created_by=uuid.uuid4(), now=NOW,
    )  # fmt: skip
    await refresh_search_doc(conn, _facts(ledger_id, DocumentState.sealed), None)
    await _tag(conn, ledger_id, "audit")
    await refresh_search_doc(conn, _facts(ledger_id, DocumentState.sealed), "")
    await mark_search_state(conn, ledger_id, DocumentState.deleting)
    after = await index.read_fields(conn, ledger_id)
    assert after == before
    assert after is not None and fields_digest(after.values) == before.fields_sha256
    total = await conn.scalar(
        text("SELECT total_ttc FROM search_doc WHERE ledger_id = :id"), {"id": ledger_id}
    )
    assert str(total) == "1600.00"  # the search row shows the correction; the seal is intact


# The resync deletes, so it is a lock-suite path too (fix round 1) ----------------------------


@pytest.fixture
def keys() -> LocalKeyService:
    return LocalKeyService(b"k" * 32)


async def test_resync_purges_the_stray_row_of_a_purged_document_only(
    db: AsyncSession, workspace: Workspace, user: User, index_db_url: str,
    conn: AsyncConnection, keys: LocalKeyService,
) -> None:  # fmt: skip
    """A search row left (or written late) for a document whose purge is done is deleted by
    the resync through `purge_document`'s gate; a sealed document's rows, and the rows of a
    removed one whose purge has not run, are never deleted by it."""
    sessions = async_sessionmaker(db.bind, expire_on_commit=False)
    workspace.paperless_url = PAPERLESS
    for kind, secret in (
        (CredentialKind.service_token, "tok"),
        (CredentialKind.index_db_password, "pw"),
    ):
        envelope = seal(secret.encode(), keys, workspace.kms_key_id, f"{workspace.id}:{kind}")
        db.add(TenantCredential(workspace_id=workspace.id, kind=kind, envelope=envelope.to_bytes()))
    await db.commit()

    async def ledger(state: str) -> uuid.UUID:
        row = DocumentLedger(
            workspace_id=workspace.id, sha256=uuid.uuid4().hex * 2, md5="0" * 32, size_bytes=6,
            content_kind="pdf", source=DocumentSource.upload, object_key=f"k/{uuid.uuid4()}",
            object_version_id="v1", state=state, ingest_state=IngestState.consumed,
            paperless_doc_id=7, retention_until=date(2036, 12, 31), ingested_at=ARRIVAL,
            fields_sha256="f" * 64 if state == "sealed" else None,
            sealed_at=NOW if state == "sealed" else None,
            sealed_by=user.id if state == "sealed" else None,
            object_locked_at=NOW if state == "sealed" else None,
        )  # fmt: skip
        db.add(row)
        await db.commit()
        return row.id

    async def remove(ledger_id: uuid.UUID, *, purged: bool) -> None:
        await db.execute(
            text(
                "UPDATE document_ledger SET state = 'removed', removed_at = now(),"
                " removed_by = :u, removal_reason = 'Doublon SAMPLE par erreur' WHERE id = :id"
            ),
            {"id": ledger_id, "u": user.id},
        )
        await db.commit()
        if purged:
            await db.execute(
                text("UPDATE document_ledger SET original_removed_at = now() WHERE id = :id"),
                {"id": ledger_id},
            )
            await audit.append(
                db, workspace_id=workspace.id, action="document.purged",
                actor_type=ActorType.system, actor_id=None, target_type="document",
                target_id=str(ledger_id), data={"paperless": True}, now=NOW,
            )  # fmt: skip
            await db.commit()

    stray, waiting, sealed = await ledger("filed"), await ledger("filed"), await ledger("sealed")
    await _document_rows(conn, sealed, sealed_fields=True)
    await _document_rows(conn, waiting, sealed_fields=False)
    await remove(stray, purged=True)
    await remove(waiting, purged=False)
    # The stray row: written by a late upsert after the purge had run.
    await refresh_search_doc(conn, _facts(stray, DocumentState.filed), "SAMPLE texte retiré")

    def factory(_url: URL) -> AsyncEngine:
        return create_async_engine(index_db_url)

    engines = IndexEngines(SETTINGS, keys, sessions, engine_factory=factory)
    try:
        with respx.mock(assert_all_called=False):
            await resync_search(
                sessions=sessions, keys=keys, index=engines, paperless=PaperlessClient,
                settings=SETTINGS, now=NOW,
            )  # fmt: skip
    finally:
        await engines.aclose()
    assert await _count(conn, "search_doc", stray) == 0
    assert await _count(conn, "search_doc", waiting) == 1
    assert await _count(conn, "document_tag", waiting) == 2
    assert await _count(conn, "document_fields", waiting) == 1
    assert await _count(conn, "search_doc", sealed) == 1
    frozen = await index.read_fields(conn, sealed)
    assert frozen is not None and frozen.sealed_at is not None


async def _document_rows(
    conn: AsyncConnection, ledger_id: uuid.UUID, *, sealed_fields: bool
) -> None:
    """The index rows `_document` writes, for a ledger id the test chose."""
    await index.write_extraction(
        conn,
        FieldsRow(
            ledger_id=ledger_id, document_type="invoice", values=VALUES,
            source=dict.fromkeys(VALUES, "extraction"), updated_at=ARRIVAL,
        ),
    )  # fmt: skip
    if sealed_fields:
        await index.seal_fields(conn, ledger_id, fields_sha256=fields_digest(VALUES), sealed_at=NOW)
    await _tag(conn, ledger_id, "bureau")
    await _tag(conn, ledger_id, "urgent")
    state = DocumentState.sealed if sealed_fields else DocumentState.filed
    await refresh_search_doc(conn, _facts(ledger_id, state), "Facture SAMPLE")
