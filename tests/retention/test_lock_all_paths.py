"""The retention-lock proof (plan 4.2, CLAUDE.md rule 4): every path that could change or delete
a document, for every role, on an unsealed and on a sealed document.

| Path | Roles | Sealed | Unsealed |
| --- | --- | --- | --- |
| 1 API `POST …/remove` | each | refused | the Owner only (202) |
| 1 API `DELETE …/documents/{id}` | each | no such route | no such route |
| 2 API `PATCH …/fields` | each | refused | the review edit (Owner, Admin, Member) |
| 3 API approve deletion, retention running | each | refused | refused |
| 4 Paperless delete, tenant service token | none | refused (403) | refused (403) |
| 5 ledger SQL: DELETE, `sha256`, bare `removed`, `fields_sha256` | none | refused | refused |
| 6 object-store delete: app, seal (and removal, sealed) key | none | refused | refused |
| 7 index SQL without the purge GUC | none | refused | DELETE refused |

Paths 4 to 7 have no role: no Sijil role (the Owner's included) reaches them through any API,
and each refuses whoever holds the credential. 4 and 6 run against the live tenant t1 and
RustFS (`make itest`); the rest against the test database (`make check`).

A field edit of an unsealed document is the review M3 shipped (a Reviewer's right), not a
deletion; it is in the matrix to prove it is the only change, besides the Owner's pre-seal
removal, that any role can make, and that it ends at the seal.

Every refused API path leaves the ledger row, its index rows and its stored original exactly
as they were and sends nothing to the removal worker; a refused removal or approval is audited
`document.removal_refused` `{code}` (design §5.5), a refused field edit is not audited.
"""

import hashlib
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import astuple, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sijil_core import index
from sijil_core.db.models import AuditEvent, Membership, Role, Workspace
from sijil_core.filetypes import FileKind
from sijil_core.storage import ObjectRef, S3ObjectStore, StorageSettings
from sijil_worker.lock import lock_sealed
from sijil_worker.settings import WorkerSettings
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import add_document, add_index_credential, engines_on

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

ROLES = [Role.owner, Role.admin, Role.member, Role.readonly]
STATES = ["unsealed", "sealed"]
REASON = "Document déposé par erreur (SAMPLE)"
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")
REFUSED = "document.removal_refused"


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


async def _member(make_actor: MakeActor, db: AsyncSession, ws: Workspace, role: Role) -> Actor:
    """A member of `ws` with `role`, TOTP enrolled and stepped up: whatever is refused is
    refused for the role or the document, never for a missing step-up."""
    actor = await make_actor(f"lock-{role.value}-{uuid.uuid4().hex[:6]}@example.com")
    db.add(Membership(user_id=actor.user_id, workspace_id=ws.id, role=role))
    await db.commit()
    await actor.enable_totp()
    await actor.step_up()
    return actor


@dataclass(frozen=True)
class Doc:
    """A document's identity, kept apart from the ORM row (a rollback expires that)."""

    id: uuid.UUID
    ref: ObjectRef


async def _document(
    state: str, make_actor: MakeActor, db: AsyncSession, ws: Workspace,
    store: MemoryObjectStore, conn: AsyncConnection,
) -> Doc:  # fmt: skip
    """A filed document; sealed through the API and locked COMPLIANCE as the seal worker
    does, when `state` is "sealed"."""
    row = await add_document(db, store, conn, ws)
    doc = Doc(row.id, ObjectRef(row.object_key, row.object_version_id))
    if state == "sealed":
        sealer = await _member(make_actor, db, ws, Role.member)
        r = await sealer.client.post(f"/v1/workspaces/{ws.id}/documents/{doc.id}/seal")
        assert r.status_code == 200, r.text
        sessions = async_sessionmaker(db.bind, expire_on_commit=False)
        assert await lock_sealed(
            doc.id, sessions=sessions, store=store, settings=SETTINGS, now=datetime.now(UTC)
        )
    return doc


async def _snapshot(
    db: AsyncSession, conn: AsyncConnection, store: MemoryObjectStore, row: Doc
) -> tuple[Any, ...]:
    """Everything a path could change: the whole ledger row, the index rows, the original."""
    ledger = await db.scalar(
        text("SELECT to_jsonb(l) FROM document_ledger l WHERE l.id = :id"), {"id": row.id}
    )
    await db.rollback()
    stored = store.objects.get(row.ref)
    return (
        ledger,
        await index.read_fields(conn, row.id),
        await index.read_corrections(conn, row.id),
        None if stored is None else astuple(stored),
        list(store.removed),
    )


async def _events(db: AsyncSession, ws_id: uuid.UUID) -> list[tuple[str, Any, str | None]]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws_id)
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    events = [(e.action, e.data, e.target_id) for e in rows]
    await db.rollback()
    return events


# Paths 1 to 3: the API, per role -------------------------------------------------------------


def _expected(path: str, role: Role, state: str) -> tuple[int, str | None]:
    """The status and error code each combination must answer (None: no error body)."""
    sealed = state == "sealed"
    if path == "remove":
        if role is not Role.owner:
            return 403, "forbidden"
        return (409, "document.sealed_cannot_be_removed") if sealed else (202, None)
    if path == "approve":
        return (409, "retention.not_eligible") if role is Role.owner else (403, "forbidden")
    if path == "fields":
        if role is Role.readonly:
            return 403, "forbidden"
        return (409, "retention.locked") if sealed else (200, None)
    raise AssertionError(path)


async def _call(actor: Actor, ws_id: uuid.UUID, row: Doc, path: str) -> httpx.Response:
    doc = f"/v1/workspaces/{ws_id}/documents/{row.id}"
    client: httpx.AsyncClient = actor.client
    if path == "remove":
        return await client.post(f"{doc}/remove", json={"reason": REASON})
    if path == "approve":
        return await client.post(
            f"/v1/workspaces/{ws_id}/retention/approve-deletion", json={"ids": [str(row.id)]}
        )
    if path == "fields":
        return await client.patch(f"{doc}/fields", json={"fields": {"total_ttc": "1.00"}})
    raise AssertionError(path)


@pytest.mark.db
@pytest.mark.parametrize("state", STATES)
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("path", ["remove", "approve", "fields"])
async def test_api_path(
    path: str, role: Role, state: str, make_actor: MakeActor, db: AsyncSession, ws: Workspace,
    memory_store: MemoryObjectStore, index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    ws_id = ws.id  # a rollback expires the ORM row
    row = await _document(state, make_actor, db, ws, memory_store, index_conn)
    actor = await _member(make_actor, db, ws, role)
    before = await _snapshot(db, index_conn, memory_store, row)
    events_before = await _events(db, ws_id)
    locks_before = list(enqueuer.locks)

    r = await _call(actor, ws_id, row, path)

    status, code = _expected(path, role, state)
    assert r.status_code == status, r.text
    new_events = (await _events(db, ws_id))[len(events_before) :]
    after = await _snapshot(db, index_conn, memory_store, row)
    assert enqueuer.locks == locks_before
    if code is None:  # the two changes the matrix allows
        if path == "remove":
            assert (r.json()["state"], after[0]["state"]) == ("removed", "removed")
            # The tombstone keeps the hash and the object reference.
            for column in ("sha256", "object_key", "object_version_id"):
                assert after[0][column] == before[0][column], column
            assert after[1:] == before[1:]  # the workers purge, not the API
            assert enqueuer.removals == [(row.id, ws_id)]
            assert [e[0] for e in new_events] == ["document.removed"]
        else:
            assert after[0] == before[0]
            assert after[1] is not None and after[1].values["total_ttc"] == "1.00"
            assert enqueuer.removals == []
            assert [e[0] for e in new_events] == ["document.fields_edited"]
        return
    assert r.json()["error"]["code"] == code
    assert after == before  # nothing written to the ledger, the index or the store
    assert enqueuer.removals == []
    if path == "fields":
        assert new_events == []
    else:
        # One refusal; its target is the document unless the role was refused (no ids read).
        target = None if (path == "approve" and code == "forbidden") else str(row.id)
        assert new_events == [(REFUSED, {"code": code}, target)]


@pytest.mark.db
@pytest.mark.parametrize("state", STATES)
@pytest.mark.parametrize("role", ROLES)
async def test_api_delete_route_does_not_exist(
    role: Role, state: str, make_actor: MakeActor, db: AsyncSession, ws: Workspace,
    memory_store: MemoryObjectStore, index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    """No route deletes a document: `DELETE …/documents/{id}` is 405 (the path exists for
    GET) or 404, never 2xx, and changes nothing."""
    ws_id = ws.id  # a rollback expires the ORM row
    row = await _document(state, make_actor, db, ws, memory_store, index_conn)
    actor = await _member(make_actor, db, ws, role)
    before = await _snapshot(db, index_conn, memory_store, row)
    events_before = await _events(db, ws_id)
    r = await actor.client.delete(f"/v1/workspaces/{ws_id}/documents/{row.id}")
    assert r.status_code in (404, 405), r.text
    assert await _snapshot(db, index_conn, memory_store, row) == before
    assert await _events(db, ws_id) == events_before
    assert enqueuer.removals == []


# Path 5: direct SQL on the ledger ------------------------------------------------------------

LEDGER_SQL = {
    "delete": "DELETE FROM document_ledger WHERE id = :id",
    "sha256": "UPDATE document_ledger SET sha256 = :hash WHERE id = :id",
    "bare_removed": "UPDATE document_ledger SET state = 'removed' WHERE id = :id",
    "fields_sha256": "UPDATE document_ledger SET fields_sha256 = :hash WHERE id = :id",
}


def _ledger_refusal(statement: str, state: str) -> str:
    # An unsealed row has no `fields_sha256` to freeze: a hash without a seal breaks the CHECK.
    if statement == "fields_sha256" and state == "unsealed":
        return "ck_document_ledger_seal_consistent"
    return "ledger.immutable"


@pytest.mark.db
@pytest.mark.parametrize("state", STATES)
@pytest.mark.parametrize("statement", sorted(LEDGER_SQL))
@pytest.mark.parametrize("as_role", ["app", "table_owner"])
async def test_ledger_sql(
    as_role: str, statement: str, state: str, make_actor: MakeActor, db: AsyncSession,
    ws: Workspace, memory_store: MemoryObjectStore, index_conn: AsyncConnection,
    owner_conn: AsyncConnection,
) -> None:  # fmt: skip
    """The app role (what every service connects as) and the table owner (the migrations')."""
    row = await _document(state, make_actor, db, ws, memory_store, index_conn)
    before = await _snapshot(db, index_conn, memory_store, row)
    params = {"id": row.id, "hash": "e" * 64}
    with pytest.raises(DBAPIError, match=_ledger_refusal(statement, state)):
        if as_role == "app":
            await db.execute(text(LEDGER_SQL[statement]), params)
        else:
            await owner_conn.execute(text(LEDGER_SQL[statement]), params)
    await db.rollback()
    assert await _snapshot(db, index_conn, memory_store, row) == before


# Path 7: direct SQL on the index -------------------------------------------------------------

INDEX_SQL = {
    "update_values": "UPDATE document_fields SET \"values\" = '{}' WHERE ledger_id = :id",
    "unseal": (
        "UPDATE document_fields SET sealed_at = NULL, fields_sha256 = NULL WHERE ledger_id = :id"
    ),
    "delete": "DELETE FROM document_fields WHERE ledger_id = :id",
}


@pytest.mark.db
@pytest.mark.parametrize(
    ("state", "statement"),
    [("sealed", s) for s in sorted(INDEX_SQL)] + [("unsealed", "delete")],
)
async def test_index_sql_without_the_purge_gate(
    state: str, statement: str, make_actor: MakeActor, db: AsyncSession, ws: Workspace,
    memory_store: MemoryObjectStore, index_conn: AsyncConnection,
) -> None:  # fmt: skip
    """As the index database's superuser, without `sijil.purge`. An unsealed row's UPDATE is
    the review edit (path 2), so only its DELETE is in the matrix."""
    row = await _document(state, make_actor, db, ws, memory_store, index_conn)
    before = await _snapshot(db, index_conn, memory_store, row)
    with pytest.raises(DBAPIError, match="retention.locked"):
        await index_conn.execute(text(INDEX_SQL[statement]), {"id": row.id})
    assert await _snapshot(db, index_conn, memory_store, row) == before


# Path 4: Paperless, the tenant service token (live tenant t1) --------------------------------


@pytest.mark.integration
@pytest.mark.parametrize("state", STATES)
async def test_paperless_service_token_delete_refused(state: str) -> None:
    """`DELETE /api/documents/{id}/` with the service token answers 403 and the document
    stays, sealed (its `sijil_sealed` custom field set, as the mirror sets it) or not."""
    from sijil_worker.mirror import SEALED_FIELD
    from tests.support.t1 import T1_URL, Harness

    data = (Path(__file__).parents[2] / "fixtures" / "invoices" / "sample-01-fr.pdf").read_bytes()
    data += f"\n%sijil-lock-{uuid.uuid4()}\n".encode()  # Paperless refuses duplicate checksums
    harness = Harness()
    await harness.prepare_paperless()
    try:
        ws = await harness.workspace()
        row = await harness.ingest(await harness.ledger(ws, data, FileKind.pdf))
        doc_id = row.paperless_doc_id
        assert doc_id is not None, row.ingest_state
        headers = {"Authorization": f"Token {harness.token}"}
        async with httpx.AsyncClient(base_url=T1_URL, headers=headers, timeout=30) as http:
            if state == "sealed":
                from sijil_core.paperless import PaperlessClient

                async with PaperlessClient(T1_URL, token=harness.token) as svc:
                    field = await svc.ensure_custom_field(SEALED_FIELD, "boolean")
                    await svc.set_custom_fields(doc_id, {field: True})
            r = await http.delete(f"/api/documents/{doc_id}/")
            assert r.status_code == 403, r.text
            r = await http.get(f"/api/documents/{doc_id}/")
            assert r.status_code == 200, r.text
        await harness.forget_in_paperless(data)  # the admin may; this is the test's cleanup
    finally:
        await harness.close()


# Path 6: the object store (RustFS) -----------------------------------------------------------


def _store(prefix: str | None) -> S3ObjectStore:
    settings = StorageSettings()  # type: ignore[call-arg]
    if prefix is not None:
        settings.objectstore_access_key = SecretStr(os.environ[f"{prefix}_ACCESS_KEY"])
        settings.objectstore_secret_key = SecretStr(os.environ[f"{prefix}_SECRET_KEY"])
    return S3ObjectStore(settings)


KEYS = {
    "app": "SIJIL_OBJECTSTORE_APP",
    "seal": "SIJIL_OBJECTSTORE_SEAL",
    "removal": "SIJIL_OBJECTSTORE_REMOVAL",
}


@pytest.mark.integration
@pytest.mark.parametrize(
    ("state", "key"),
    [("unsealed", "app"), ("unsealed", "seal"), ("sealed", "app"), ("sealed", "seal"),
     ("sealed", "removal")],
)  # fmt: skip
async def test_object_store_delete_refused(state: str, key: str, tmp_path: Path) -> None:
    """A versioned delete, with and without the GOVERNANCE bypass, and the delete marker. The
    removal key deleting an unsealed original is the Owner's removal itself (path 1, carried
    out by worker-removal), so only its sealed case is here."""
    from botocore.exceptions import ClientError

    root = _store(None)
    app = _store(KEYS["app"])
    path = tmp_path / "original"
    path.write_bytes(b"SAMPLE lock " + uuid.uuid4().bytes)
    ref = await app.put_original(
        f"it-lock-{uuid.uuid4().hex}/{hashlib.sha256(path.read_bytes()).hexdigest()}",
        path,
        date.today() + timedelta(days=1),
    )
    if state == "sealed":
        await _store(KEYS["seal"]).seal(ref, date.today() + timedelta(days=1))
    store = _store(KEYS[key])
    target = {"Bucket": store.bucket, "Key": ref.key, "VersionId": ref.version_id}
    attempts: list[Callable[[], object]] = [
        lambda: store.client.delete_object(**target),
        lambda: store.client.delete_object(**target, BypassGovernanceRetention=True),
        lambda: store.client.delete_object(Bucket=store.bucket, Key=ref.key),
    ]
    try:
        for attempt in attempts:
            with pytest.raises(ClientError) as raised:
                attempt()
            assert raised.value.response["Error"]["Code"] == "AccessDenied"
        assert await root.head(ref) is not None
        versions = root.client.list_object_versions(Bucket=root.bucket, Prefix=ref.key)
        assert [v["VersionId"] for v in versions.get("Versions", [])] == [ref.version_id]
        assert versions.get("DeleteMarkers", []) == []
    finally:
        if state == "unsealed":  # a sealed one stays until its lock ends (a day)
            await root.remove_original(ref, governance=True)
