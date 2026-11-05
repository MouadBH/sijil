"""The retention lock suite (CLAUDE.md rule 4): a sealed document is refused on every path that
could change it, one test per layer (architecture.md "Retention lock").

| Layer | Where | Tests |
| --- | --- | --- |
| 1 API | control plane answers `409 retention.locked` | test_api_* |
| 2 Paperless | the service user has no delete right | test_paperless_service_user_cannot_delete |
| 3 database | ledger trigger (0006/0008), index row freeze trigger | test_ledger_*, test_index_* |
| 4 object lock | COMPLIANCE once sealed (the seal worker), never shortened | test_object_lock_* |

Routing (Task 5) is a path that writes the ledger, the index row and the object lock, so it is
here too: it only ever raises retention, extends the lock before the ledger and refuses a
sealed index row.
"""

import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import select, text, update
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sijil_core import index
from sijil_core.crypto import LocalKeyService
from sijil_core.crypto import seal as seal_envelope
from sijil_core.db.models import (
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    DocumentState,
    Membership,
    Role,
    TenantCredential,
    Workspace,
)
from sijil_core.fields import FIELD_NAMES, fields_digest
from sijil_core.index import FieldsChanged, FieldsRow, IndexEngines, RetentionLocked
from sijil_core.paperless import SERVICE_PERMISSIONS
from sijil_core.storage import ObjectRef, RetentionActive, S3ObjectStore, StorageSettings
from sijil_worker.extract import _route
from sijil_worker.lock import lock_sealed
from sijil_worker.settings import WorkerSettings
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import (
    INVOICE,
    PROVISIONAL,
    add_document,
    add_index_credential,
    engines_on,
    stored,
)

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]
NOW = datetime(2026, 10, 2, 9, 30, tzinfo=UTC)


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


@pytest.fixture
async def ws(
    db: AsyncSession, workspace: Workspace, cp_app: FastAPI, index_db_url: str
) -> AsyncIterator[Workspace]:
    await add_index_credential(db, cp_app, workspace)
    engines = engines_on(cp_app, index_db_url)
    cp_app.state.index = engines
    yield workspace
    await engines.aclose()


@pytest.fixture
async def member(make_actor: MakeActor, db: AsyncSession, ws: Workspace) -> Actor:
    actor = await make_actor("lock-member@example.com")
    db.add(Membership(user_id=actor.user_id, workspace_id=ws.id, role=Role.member))
    await db.commit()
    return actor


async def _sealed_via_api(
    member: Actor,
    ws: Workspace,
    db: AsyncSession,
    store: MemoryObjectStore,
    conn: AsyncConnection,
) -> DocumentLedger:
    row = await add_document(db, store, conn, ws)
    r = await member.client.post(f"/v1/workspaces/{ws.id}/documents/{row.id}/seal")
    assert r.status_code == 200, r.text
    # What the seal worker does once `sijil.lock_sealed` arrives.
    sessions = async_sessionmaker(db.bind, expire_on_commit=False)
    assert await lock_sealed(row.id, sessions=sessions, store=store, settings=ROUTING, now=NOW)
    return row


# Layer 1: the API ----------------------------------------------------------------------------


@pytest.mark.db
async def test_api_patch_after_seal_409(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await _sealed_via_api(member, ws, db, memory_store, index_conn)
    for name in FIELD_NAMES:
        r = await member.client.patch(
            f"/v1/workspaces/{ws.id}/documents/{row.id}/fields", json={"fields": {name: None}}
        )
        assert r.status_code == 409, name
        assert r.json()["error"]["code"] == "retention.locked"
    frozen = await index.read_fields(index_conn, row.id)
    assert frozen is not None
    assert dict(frozen.values) == INVOICE


@pytest.mark.db
async def test_api_reseal_and_batch_never_touch_a_sealed_document(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await _sealed_via_api(member, ws, db, memory_store, index_conn)
    r = await member.client.post(f"/v1/workspaces/{ws.id}/documents/{row.id}/seal")
    assert (r.status_code, r.json()["error"]["code"]) == (409, "retention.locked")
    r = await member.client.post(f"/v1/workspaces/{ws.id}/seal-batch", json={"month": "2026-10"})
    assert r.json()["sealed"] == 0
    events = await db.scalars(
        select(AuditEvent.id).where(
            AuditEvent.workspace_id == ws.id, AuditEvent.action == "document.sealed"
        )
    )
    assert len(list(events)) == 1


# Layer 2: Paperless --------------------------------------------------------------------------


def test_paperless_service_user_cannot_delete() -> None:
    """The service user (also the one `mirror_fields` writes with) holds no delete right; the
    live check is tests/integration/test_paperless_contract.py::test_service_user_cannot_delete.
    """
    assert SERVICE_PERMISSIONS
    assert not [p for p in SERVICE_PERMISSIONS if p.startswith("delete_")]


# Layer 3: the databases ----------------------------------------------------------------------


@pytest.mark.db
async def test_index_update_after_seal_refused(index_conn: AsyncConnection) -> None:
    """The index row of a sealed document refuses every writer: the API's edit, extraction,
    routing's scores, a second seal with another hash, and direct SQL."""
    ledger_id = uuid.uuid4()
    values = {name: INVOICE.get(name) for name in FIELD_NAMES}
    row = FieldsRow(
        ledger_id=ledger_id,
        document_type="invoice",
        values=values,
        source=dict.fromkeys(FIELD_NAMES, "extraction"),
        updated_at=NOW,
    )
    await index.write_extraction(index_conn, row)
    digest = fields_digest(values)
    await index.seal_fields(index_conn, ledger_id, fields_sha256=digest, sealed_at=NOW)

    with pytest.raises(RetentionLocked):
        await index.update_fields(
            index_conn, ledger_id, {"total_ttc": "1.00"}, source="user", updated_by=None, now=NOW
        )
    with pytest.raises(RetentionLocked):
        await index.write_extraction(index_conn, row)
    with pytest.raises(RetentionLocked):
        await index.set_scores(index_conn, ledger_id, {"total_ttc": 1.0}, ())
    with pytest.raises(RetentionLocked):
        await index.seal_fields(index_conn, ledger_id, fields_sha256="b" * 64, sealed_at=NOW)
    for sql in (
        "UPDATE document_fields SET \"values\" = '{}' WHERE ledger_id = :id",
        "UPDATE document_fields SET sealed_at = NULL, fields_sha256 = NULL WHERE ledger_id = :id",
        "DELETE FROM document_fields WHERE ledger_id = :id",
    ):
        with pytest.raises(DBAPIError, match="retention.locked"):
            await index_conn.execute(text(sql), {"id": ledger_id})
    frozen = await index.read_fields(index_conn, ledger_id)
    assert frozen is not None
    assert (dict(frozen.values), frozen.fields_sha256) == (values, digest)


@pytest.mark.db
async def test_index_seal_refuses_a_hash_of_values_that_changed(
    index_conn: AsyncConnection,
) -> None:
    """The freeze recomputes the digest from the row it locks: a hash of values an edit has
    since replaced is never stamped onto the new ones."""
    ledger_id = uuid.uuid4()
    values = {name: INVOICE.get(name) for name in FIELD_NAMES}
    await index.write_extraction(
        index_conn,
        FieldsRow(
            ledger_id=ledger_id, document_type="invoice", values=values,
            source=dict.fromkeys(FIELD_NAMES, "extraction"), updated_at=NOW,
        ),
    )  # fmt: skip
    stale = fields_digest(values)
    await index.update_fields(
        index_conn, ledger_id, {"total_ttc": "1.00"}, source="user", updated_by=None, now=NOW
    )
    with pytest.raises(FieldsChanged):
        await index.seal_fields(index_conn, ledger_id, fields_sha256=stale, sealed_at=NOW)
    current = await index.read_fields(index_conn, ledger_id)
    assert current is not None and current.sealed_at is None


@pytest.mark.db
async def test_ledger_fields_sha256_state_retention_frozen_after_seal(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, owner_conn: AsyncConnection,
) -> None:  # fmt: skip
    """A ledger row sealed through the API: the app role (ORM) and the table owner (SQL) are
    both refused any change of hash, state, retention or seal columns, and the delete."""
    row_id = (await _sealed_via_api(member, ws, db, memory_store, index_conn)).id
    changes: list[dict[str, Any]] = [
        {"fields_sha256": "e" * 64},
        {"fields_sha256": None},
        {"state": DocumentState.filed},
        {"state": DocumentState.removed},
        {"retention_until": date(2040, 12, 31)},
        {"retention_until": date(2030, 12, 31)},
        {"sealed_at": None},
        {"sealed_by": None},
    ]
    for values in changes:
        with pytest.raises(DBAPIError, match="ledger.immutable"):
            await db.execute(
                update(DocumentLedger).where(DocumentLedger.id == row_id).values(**values)
            )
        await db.rollback()
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await owner_conn.execute(
            text("UPDATE document_ledger SET state = 'filed' WHERE id = :id"), {"id": row_id}
        )
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await owner_conn.execute(text("DELETE FROM document_ledger WHERE id = :id"), {"id": row_id})
    sealed = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == row_id)
        .execution_options(populate_existing=True)
    )
    assert sealed is not None
    assert (sealed.state, sealed.retention_until) == (DocumentState.sealed, PROVISIONAL)


# Layer 4: the object lock --------------------------------------------------------------------


@pytest.mark.db
async def test_object_lock_cannot_shorten_after_seal(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    """The in-memory store mirrors RustFS (see test_object_lock_cannot_shorten_after_seal_rustfs
    and tests/integration/test_object_store.py::test_store_honours_governance_and_compliance)."""
    row = await _sealed_via_api(member, ws, db, memory_store, index_conn)
    ref = ObjectRef(row.object_key, row.object_version_id)
    assert stored(memory_store, row).mode == "COMPLIANCE"
    # A second lock run (a redelivery, the sweep) changes nothing.
    sessions = async_sessionmaker(db.bind, expire_on_commit=False)
    assert not await lock_sealed(
        row.id, sessions=sessions, store=memory_store, settings=ROUTING, now=NOW
    )
    with pytest.raises(ValueError):
        await memory_store.extend_retention(ref, PROVISIONAL - timedelta(days=1))
    await memory_store.seal(ref, date(2027, 12, 31))  # a later, shorter seal keeps the date
    with pytest.raises(RetentionActive):
        await memory_store.remove_original(ref, governance=True)
    assert (stored(memory_store, row).mode, stored(memory_store, row).retain_until) == (
        "COMPLIANCE",
        PROVISIONAL,
    )


def _store(access: str, secret: str) -> S3ObjectStore:
    settings = StorageSettings()  # type: ignore[call-arg]
    settings.objectstore_access_key = SecretStr(os.environ[access])
    settings.objectstore_secret_key = SecretStr(os.environ[secret])
    return S3ObjectStore(settings)


@pytest.mark.db
async def test_object_lock_follows_the_committed_ledger_seal(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    """The API commits the seal and sends the lock; the original stays GOVERNANCE at its
    provisional date until the seal worker switches it to COMPLIANCE at the ledger's committed
    retention, and records `object_locked_at` once (frozen by the 0009 trigger)."""
    row = await add_document(db, memory_store, index_conn, ws, retention_until=date(2035, 12, 31))
    r = await member.client.post(f"/v1/workspaces/{ws.id}/documents/{row.id}/seal")
    assert r.status_code == 200, r.text
    assert enqueuer.locks == [(row.id, ws.id)]
    assert (stored(memory_store, row).mode, stored(memory_store, row).retain_until) == (
        "GOVERNANCE",
        date(2035, 12, 31),
    )
    sessions = async_sessionmaker(db.bind, expire_on_commit=False)
    assert await lock_sealed(
        row.id, sessions=sessions, store=memory_store, settings=ROUTING, now=NOW
    )
    assert (stored(memory_store, row).mode, stored(memory_store, row).retain_until) == (
        "COMPLIANCE",
        date(2036, 12, 31),
    )
    locked = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == row.id)
        .execution_options(populate_existing=True)
    )
    assert locked is not None and locked.object_locked_at == NOW
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await db.execute(
            update(DocumentLedger).where(DocumentLedger.id == row.id).values(object_locked_at=None)
        )
    await db.rollback()


@pytest.mark.integration
async def test_object_lock_cannot_shorten_after_seal_rustfs(tmp_path: Path) -> None:
    """RustFS, through the seal worker's credential: sealing twice at the same date is a no-op,
    and once COMPLIANCE neither that key nor the root key (with the bypass flag) can shorten
    the lock or delete the version."""
    from botocore.exceptions import ClientError

    root = S3ObjectStore(StorageSettings())  # type: ignore[call-arg]
    app = _store("SIJIL_OBJECTSTORE_APP_ACCESS_KEY", "SIJIL_OBJECTSTORE_APP_SECRET_KEY")
    sealer = _store("SIJIL_OBJECTSTORE_SEAL_ACCESS_KEY", "SIJIL_OBJECTSTORE_SEAL_SECRET_KEY")
    path = tmp_path / "original"
    path.write_bytes(b"SAMPLE " + uuid.uuid4().bytes)
    ref = await app.put_original(
        f"it-{uuid.uuid4().hex}/{uuid.uuid4().hex}", path, date.today() + timedelta(days=1)
    )
    final = date.today() + timedelta(days=3)
    await sealer.seal(ref, final)
    assert sealer._current(ref)[0] == "COMPLIANCE"  # noqa: SLF001
    await sealer.seal(ref, final)  # a retried lock task: idempotent at the same date
    await sealer.seal(ref, final - timedelta(days=1))  # never shortens, never fails
    assert root._current(ref)[1].date() == final  # noqa: SLF001
    sooner = datetime.now(UTC) + timedelta(hours=1)
    target = {"Bucket": root.bucket, "Key": ref.key, "VersionId": ref.version_id}
    for store in (sealer, root):
        for mode in ("COMPLIANCE", "GOVERNANCE"):
            with pytest.raises(ClientError):
                store.client.put_object_retention(
                    **target,
                    Retention={"Mode": mode, "RetainUntilDate": sooner},  # type: ignore[typeddict-item]
                    BypassGovernanceRetention=True,
                )
        with pytest.raises(ClientError):
            store.client.delete_object(**target, BypassGovernanceRetention=True)
    with pytest.raises(ValueError):
        await sealer.extend_retention(ref, final - timedelta(days=1))
    assert root._current(ref)[1].date() == final  # noqa: SLF001


# Routing (Task 5): raises retention only, the lock before the ledger, never a sealed row --------

ROUTING = WorkerSettings.model_construct(region="ma-cas", environment="test")


class ObservingStore:
    """Records each extension and what the ledger held at that moment."""

    def __init__(self, observe: Callable[[], Awaitable[date]]) -> None:
        self.observe = observe
        self.extended: list[tuple[ObjectRef, date, date]] = []

    async def extend_retention(self, ref: ObjectRef, retain_until: date) -> None:
        self.extended.append((ref, retain_until, await self.observe()))


@pytest.fixture
async def routing(
    db: AsyncSession, workspace: Workspace, index_db_url: str
) -> AsyncIterator[tuple[async_sessionmaker[AsyncSession], IndexEngines]]:
    keys = LocalKeyService(b"k" * 32)
    kind = CredentialKind.index_db_password
    envelope = seal_envelope(b"pw", keys, workspace.kms_key_id, f"{workspace.id}:{kind}")
    db.add(TenantCredential(workspace_id=workspace.id, kind=kind, envelope=envelope.to_bytes()))
    await db.commit()
    sessions = async_sessionmaker(db.bind, expire_on_commit=False)

    def factory(_url: URL) -> AsyncEngine:
        return create_async_engine(index_db_url)

    engines = IndexEngines(ROUTING, keys, sessions, engine_factory=factory)
    yield sessions, engines
    await engines.aclose()


async def _received(
    db: AsyncSession, store: MemoryObjectStore, conn: AsyncConnection, ws: Workspace,
    retention: date, doc_date: str,
) -> tuple[DocumentLedger, FieldsRow]:  # fmt: skip
    row = await add_document(
        db, store, conn, ws, state=DocumentState.received, retention_until=retention,
        values={**INVOICE, "date": doc_date},
    )  # fmt: skip
    fields = await index.read_fields(conn, row.id)
    assert fields is not None
    return row, fields


@pytest.mark.db
@pytest.mark.parametrize(
    ("retention", "doc_date", "final"),
    [
        (date(2035, 12, 31), "2026-09-30", date(2036, 12, 31)),  # raised to the fiscal year
        (date(2040, 12, 31), "2026-09-30", date(2040, 12, 31)),  # never lowered
        (PROVISIONAL, "2024-01-15", PROVISIONAL),  # an earlier year never lowers it
    ],
)
async def test_routing_raises_retention_and_never_lowers_it(
    routing: Any, db: AsyncSession, workspace: Workspace, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, retention: date, doc_date: str, final: date,
) -> None:  # fmt: skip
    sessions, engines = routing
    row, fields = await _received(db, memory_store, index_conn, workspace, retention, doc_date)
    assert await _route(sessions, engines, memory_store, workspace, fields, NOW) is not None
    routed = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == row.id)
        .execution_options(populate_existing=True)
    )
    assert routed is not None
    assert routed.retention_until == final
    assert stored(memory_store, row).retain_until == final
    assert stored(memory_store, row).mode == "GOVERNANCE"  # filing never seals


@pytest.mark.db
async def test_routing_extends_the_object_lock_before_the_ledger(
    routing: Any, db: AsyncSession, workspace: Workspace, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, migrated_db: Any,
) -> None:  # fmt: skip
    sessions, engines = routing
    row, fields = await _received(
        db, memory_store, index_conn, workspace, date(2035, 12, 31), "2026-09-30"
    )
    observer = create_async_engine(migrated_db.app_url)

    async def ledger_retention() -> date:
        async with observer.connect() as conn:
            found = await conn.scalar(
                select(DocumentLedger.retention_until).where(DocumentLedger.id == row.id)
            )
        assert found is not None
        return found

    store = ObservingStore(ledger_retention)
    try:
        await _route(sessions, engines, store, workspace, fields, NOW)
    finally:
        await observer.dispose()
    [(_, extended_to, ledger_then)] = store.extended
    assert (extended_to, ledger_then) == (date(2036, 12, 31), date(2035, 12, 31))


@pytest.mark.db
async def test_routing_never_moves_a_sealed_document(
    routing: Any, db: AsyncSession, workspace: Workspace, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, user: Any,
) -> None:  # fmt: skip
    """A late or redelivered extraction routes nothing once sealed: the ledger is not
    `received`, so neither the index row, the lock nor the ledger is touched."""
    sessions, engines = routing
    row, fields = await _received(
        db, memory_store, index_conn, workspace, PROVISIONAL, "2026-09-30"
    )
    digest = fields_digest(fields.values)
    await index.seal_fields(index_conn, row.id, fields_sha256=digest, sealed_at=NOW)
    row.state = DocumentState.sealed
    row.fields_sha256 = digest
    row.sealed_at = NOW
    row.sealed_by = user.id
    await db.commit()
    assert await _route(sessions, engines, memory_store, workspace, fields, NOW) is None
    assert stored(memory_store, row).retain_until == PROVISIONAL
