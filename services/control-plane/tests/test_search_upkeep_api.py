"""The control plane keeps `search_doc` in step with what it changes.

A field edit, a seal and a correction write the search row in the same index transaction as the
change: a failure after the upsert rolls both back. A removal and an approved deletion mark the
row `removed` / `deleting` right after their commit (Task 2's query skips terminal rows); the
purge deletes it later.
"""

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import date
from typing import Any

import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core import index, search_index
from sijil_core.db.models import DocumentLedger, DocumentState, Membership, Role, Workspace
from sijil_core.search_index import SearchLedgerFacts, refresh_search_doc
from sijil_cp.api import review
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import INVOICE, add_document, add_index_credential, engines_on

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db

REASON = "Avoir SAMPLE reçu après le scellement"
PAST = date(2020, 12, 31)


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


async def _actor(make_actor: MakeActor, db: AsyncSession, ws: Workspace, role: Role) -> Actor:
    actor = await make_actor(f"{role.value}-{uuid.uuid4().hex[:6]}@example.com")
    db.add(Membership(user_id=actor.user_id, workspace_id=ws.id, role=role))
    await db.commit()
    return actor


@pytest.fixture
async def member(make_actor: MakeActor, db: AsyncSession, ws: Workspace) -> Actor:
    return await _actor(make_actor, db, ws, Role.member)


@pytest.fixture
async def owner(make_actor: MakeActor, db: AsyncSession, ws: Workspace) -> Actor:
    actor = await _actor(make_actor, db, ws, Role.owner)
    await actor.enable_totp()
    await actor.step_up()
    return actor


def _url(ws: Workspace, doc: DocumentLedger, suffix: str) -> str:
    return f"/v1/workspaces/{ws.id}/documents/{doc.id}/{suffix}"


def _code(r: Any) -> str:
    return str(r.json()["error"]["code"])


async def _indexed(conn: AsyncConnection, doc: DocumentLedger) -> None:
    """The search row the worker wrote at routing."""
    facts = SearchLedgerFacts(
        ledger_id=doc.id, state=doc.state, ingested_at=doc.ingested_at, region="ma-cas",
        now=doc.ingested_at,
    )  # fmt: skip
    await refresh_search_doc(conn, facts, "Facture SAMPLE Atlas Télécom")


async def _search(conn: AsyncConnection, doc: DocumentLedger) -> dict[str, Any]:
    found = await conn.execute(
        text(
            "SELECT state, total_ttc, number, corrected, content FROM search_doc "
            "WHERE ledger_id = :id"
        ),
        {"id": doc.id},
    )
    return dict(found.mappings().one())


def _fail_after_upsert(monkeypatch: pytest.MonkeyPatch, module: Any) -> None:
    """The real upsert runs in the request's index transaction, then the request fails."""
    real = search_index.refresh_search_doc

    async def upsert_then_fail(*args: Any, **kwargs: Any) -> None:
        await real(*args, **kwargs)
        raise ConnectionResetError("index connection lost after the upsert")

    monkeypatch.setattr(module, "refresh_search_doc", upsert_then_fail)


async def test_patch_and_correction_update_search_doc_in_same_transaction(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws, state=DocumentState.needs_review)
    await _indexed(index_conn, row)

    # A failure after the upsert: neither the edit nor the search row changes.
    with monkeypatch.context() as m:
        _fail_after_upsert(m, review)
        r = await member.client.patch(
            _url(ws, row, "fields"), json={"fields": {"total_ttc": "1600,00"}}
        )
    assert (r.status_code, _code(r)) == (503, "field.update_failed")
    fields = await index.read_fields(index_conn, row.id)
    assert fields is not None and fields.values["total_ttc"] == INVOICE["total_ttc"]
    assert str((await _search(index_conn, row))["total_ttc"]) == INVOICE["total_ttc"]

    # The edit: visible in search as soon as the request answers.
    r = await member.client.patch(
        _url(ws, row, "fields"), json={"fields": {"total_ttc": "1600,00"}}
    )
    assert r.status_code == 200, r.text
    found = await _search(index_conn, row)
    assert (found["state"], str(found["total_ttc"])) == ("needs_review", "1600.00")
    assert found["content"] == "Facture SAMPLE Atlas Télécom"  # the text is kept

    # The seal moves the row to `sealed`, in the seal's own index transaction.
    r = await member.client.post(_url(ws, row, "seal"))
    assert r.status_code == 200, r.text
    assert (await _search(index_conn, row))["state"] == "sealed"

    # A correction: rolled back with its note when the request fails after the upsert...
    from sijil_cp.api import corrections

    body = {"field": "number", "value": "F-2026-01001-A", "reason": REASON}
    with monkeypatch.context() as m:
        _fail_after_upsert(m, corrections)
        r = await member.client.post(_url(ws, row, "corrections"), json=body)
    assert (r.status_code, _code(r)) == (503, "field.update_failed")
    assert await index.read_corrections(index_conn, row.id) == []
    found = await _search(index_conn, row)
    assert (found["number"], found["corrected"]) == (INVOICE["number"], [])
    # ...and shown as the effective value once it applies.
    r = await member.client.post(_url(ws, row, "corrections"), json=body)
    assert r.status_code == 201, r.text
    found = await _search(index_conn, row)
    assert (found["number"], found["corrected"]) == ("F-2026-01001-A", ["number"])
    assert found["state"] == "sealed"


async def test_a_failed_search_write_fails_the_seal_and_a_retry_completes_it(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    await _indexed(index_conn, row)
    with monkeypatch.context() as m:
        _fail_after_upsert(m, review)
        r = await member.client.post(_url(ws, row, "seal"))
    assert (r.status_code, _code(r)) == (503, "seal.index_unavailable")
    fields = await index.read_fields(index_conn, row.id)
    assert fields is not None and fields.sealed_at is None  # rolled back with the search row
    assert (await _search(index_conn, row))["state"] == "filed"
    r = await member.client.post(_url(ws, row, "seal"))
    assert r.status_code == 200, r.text
    assert (await _search(index_conn, row))["state"] == "sealed"


async def test_removal_marks_search_doc_removed(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    await _indexed(index_conn, row)
    r = await owner.client.post(
        _url(ws, row, "remove"), json={"reason": "Facture déposée deux fois (SAMPLE)"}
    )
    assert r.status_code == 202, r.text
    assert (await _search(index_conn, row))["state"] == "removed"


async def test_approved_deletion_marks_search_doc_deleting(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws, retention_until=PAST)
    await _indexed(index_conn, row)
    r = await owner.client.post(
        f"/v1/workspaces/{ws.id}/retention/approve-deletion", json={"ids": [str(row.id)]}
    )
    assert r.status_code == 202, r.text
    assert (await _search(index_conn, row))["state"] == "deleting"


async def test_a_failed_search_mark_does_not_undo_the_removal(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    """The removal stands; the daily resync marks the row later."""
    from sijil_cp import removals

    async def broken(*_args: Any, **_kwargs: Any) -> None:
        raise ConnectionResetError("index down")

    monkeypatch.setattr(removals, "mark_search_state", broken)
    row = await add_document(db, memory_store, index_conn, ws)
    await _indexed(index_conn, row)
    r = await owner.client.post(
        _url(ws, row, "remove"), json={"reason": "Facture déposée deux fois (SAMPLE)"}
    )
    assert r.status_code == 202, r.text
    assert (await _search(index_conn, row))["state"] == "filed"
