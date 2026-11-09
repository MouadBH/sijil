"""The search API, saved views and tags (plan 5.1).

Each test gets its own index database: search rows cannot be deleted, so a shared one would let
another test's documents into the results.
"""

import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any

import pytest
from fastapi import FastAPI
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core import index
from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    DocumentState,
    Membership,
    Organisation,
    OrgType,
    Role,
    Workspace,
    WorkspaceState,
)
from sijil_core.search_index import SearchLedgerFacts, mark_search_state, refresh_search_doc
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import INVOICE, add_document, add_index_credential, engines_on

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db

CONTENT = "Facture SAMPLE Atlas Télécom — abonnement fibre"


@pytest.fixture
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


async def _indexed(
    db: AsyncSession, store: MemoryObjectStore, conn: AsyncConnection, ws: Workspace,
    state: DocumentState = DocumentState.filed, content: str = CONTENT,
    **values: str | None,
) -> DocumentLedger:  # fmt: skip
    """A ledger row, its fields and its search row (as the worker writes it at routing)."""
    row = await add_document(db, store, conn, ws, state=state, values=INVOICE | values)
    facts = SearchLedgerFacts(
        ledger_id=row.id, state=state, ingested_at=row.ingested_at, region="ma-cas",
        now=row.ingested_at,
    )  # fmt: skip
    await refresh_search_doc(conn, facts, content)
    return row


def _search_url(ws: Workspace) -> str:
    return f"/v1/workspaces/{ws.id}/search"


def _code(r: Any) -> str:
    return str(r.json()["error"]["code"])


async def _events(db: AsyncSession, ws: Workspace, action: str) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id, AuditEvent.action == action)
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def _ids(actor: Actor, ws: Workspace, **filters: Any) -> list[str]:
    r = await actor.client.post(_search_url(ws), json={"filters": filters, "sort": "date_desc"})
    assert r.status_code == 200, r.text
    return [item["id"] for item in r.json()["items"]]


# Search ---------------------------------------------------------------------------------------


async def test_search_returns_live_documents_with_a_snippet(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    doc = await _indexed(db, memory_store, index_conn, ws)
    gone = await _indexed(db, memory_store, index_conn, ws)
    await mark_search_state(index_conn, gone.id, DocumentState.removed)
    r = await member.client.post(_search_url(ws), json={"filters": {"q": "telecom"}})
    assert r.status_code == 200, r.text
    assert r.headers["server-timing"].startswith("app;dur=")
    body = r.json()
    assert body["next_cursor"] is None
    [item] = body["items"]
    assert item | {"snippet": None} == {
        "id": str(doc.id), "state": "filed", "document_type": "invoice",
        "basis_date": INVOICE["date"], "supplier_name": INVOICE["supplier_name"],
        "number": INVOICE["number"], "total_ttc": "1500.00", "currency": "MAD", "tags": [],
        "corrected": [], "snippet": None,
    }  # fmt: skip
    assert "«" in item["snippet"] and len(item["snippet"]) <= 200


async def test_search_refuses_bad_bodies(member: Actor, ws: Workspace) -> None:
    for body in (
        {"filters": {"text": "x"}},
        {"filters": {"q": "x" * 201}},
        {"filters": {"status": ["removed"]}},
        {"filters": {"amount_min": "abc"}},
        {"sort": "name"},
        {"limit": 101},
        {"cursor": "garbage"},
    ):
        r = await member.client.post(_search_url(ws), json=body)
        assert (r.status_code, _code(r)) == (422, "search.query_invalid"), body


async def test_search_paging_through_the_api(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    docs = {str((await _indexed(db, memory_store, index_conn, ws)).id) for _ in range(3)}
    seen: list[str] = []
    cursor = None
    while True:
        body = {"sort": "amount_desc", "limit": 2} | ({"cursor": cursor} if cursor else {})
        r = await member.client.post(_search_url(ws), json=body)
        assert r.status_code == 200, r.text
        seen += [item["id"] for item in r.json()["items"]]
        cursor = r.json()["next_cursor"]
        if cursor is None:
            break
    assert sorted(seen) == sorted(docs)


async def test_search_audit_has_no_query_text(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    await _indexed(db, memory_store, index_conn, ws)
    secret_q, secret_supplier = "abonnement fibre", "Atlas Télécom"
    r = await member.client.post(
        _search_url(ws),
        json={"filters": {"q": secret_q, "supplier": secret_supplier, "amount_max": "2000.00",
                          "tags": []}},
    )  # fmt: skip
    assert r.status_code == 200, r.text
    [event] = await _events(db, ws, "search.performed")
    assert event.data == {"result_count": 1, "filters": "q,supplier,amount_max"}
    assert event.actor_id == str(member.user_id)
    dumped = json.dumps(event.data, ensure_ascii=False).casefold()
    for secret in (secret_q, secret_supplier, "2000"):
        assert secret.casefold() not in dumped


async def test_search_region_asserted(
    make_actor: MakeActor, db: AsyncSession, cp_app: FastAPI
) -> None:
    elsewhere = Organisation(name="Paris", type=OrgType.business, country="FR", region="eu-fra")
    db.add(elsewhere)
    await db.flush()
    slug = f"ws-{uuid.uuid4().hex[:8]}"
    foreign = Workspace(
        org_id=elsewhere.id, name="Paris SARL", slug=slug, state=WorkspaceState.active,
        db_name=f"paperless_{slug}", index_db_name=f"index_{slug}", kms_key_id=f"kek-{slug}",
    )  # fmt: skip
    db.add(foreign)
    await db.commit()
    actor = await _actor(make_actor, db, foreign, Role.owner)
    for method, path in (
        ("POST", _search_url(foreign)),
        ("GET", f"{_search_url(foreign)}/views"),
        ("PUT", f"/v1/workspaces/{foreign.id}/documents/{uuid.uuid4()}/tags"),
    ):
        body: dict[str, Any] = {"tags": []} if method == "PUT" else {}
        r = await actor.client.request(method, path, json=body)
        assert (r.status_code, _code(r)) == (421, "region.mismatch"), path


# Saved views ----------------------------------------------------------------------------------


VIEW = {
    "name": "Factures télécom SAMPLE",
    "filters": {"q": "telecom", "amount_min": "100.00", "period_from": "2026-01-01"},
    "sort": "amount_desc",
}


async def test_saved_view_roundtrip(member: Actor, ws: Workspace, db: AsyncSession) -> None:
    views = f"{_search_url(ws)}/views"
    r = await member.client.post(views, json=VIEW)
    assert r.status_code == 201, r.text
    created = r.json()
    view_id = created["id"]
    assert created["name"] == VIEW["name"] and created["sort"] == "amount_desc"
    assert created["filters"]["amount_min"] == "100.00"
    assert created["filters"]["period_from"] == "2026-01-01"
    assert created["created_by"] == str(member.user_id)

    r = await member.client.get(views)
    assert r.status_code == 200
    assert [v["id"] for v in r.json()["items"]] == [view_id]

    changed = VIEW | {"name": "  Télécom > 100  ", "sort": "date_desc"}
    r = await member.client.put(f"{views}/{view_id}", json=changed)
    assert r.status_code == 200, r.text
    assert (r.json()["name"], r.json()["sort"]) == ("Télécom > 100", "date_desc")
    [listed] = (await member.client.get(views)).json()["items"]
    assert listed["name"] == "Télécom > 100"

    r = await member.client.delete(f"{views}/{view_id}")
    assert r.status_code == 204
    assert (await member.client.get(views)).json()["items"] == []
    for method in ("PUT", "DELETE"):
        r = await member.client.request(method, f"{views}/{view_id}", json=VIEW)
        assert (r.status_code, _code(r)) == (404, "search.view_not_found")

    ops = [e.data for e in await _events(db, ws, "search.view_changed")]
    assert ops == [{"op": "create"}, {"op": "update"}, {"op": "delete"}]

    for bad in ({**VIEW, "name": " "}, {**VIEW, "name": "x" * 81},
                {**VIEW, "filters": {"status": ["deleted"]}}, {**VIEW, "sort": "x"}):  # fmt: skip
        r = await member.client.post(views, json=bad)
        assert (r.status_code, _code(r)) == (422, "search.query_invalid"), bad


async def test_saved_view_edit_by_other_member_forbidden(
    make_actor: MakeActor, member: Actor, ws: Workspace, db: AsyncSession
) -> None:
    views = f"{_search_url(ws)}/views"
    view_id = (await member.client.post(views, json=VIEW)).json()["id"]
    other = await _actor(make_actor, db, ws, Role.member)
    reader = await _actor(make_actor, db, ws, Role.readonly)
    admin = await _actor(make_actor, db, ws, Role.admin)
    owner = await _actor(make_actor, db, ws, Role.owner)
    # Everyone who reads documents sees the workspace's views and may save their own.
    assert len((await reader.client.get(views)).json()["items"]) == 1
    assert (await reader.client.post(views, json=VIEW)).status_code == 201
    for actor in (other, reader):
        r = await actor.client.put(f"{views}/{view_id}", json=VIEW | {"name": "pris"})
        assert (r.status_code, _code(r)) == (403, "forbidden")
        r = await actor.client.delete(f"{views}/{view_id}")
        assert (r.status_code, _code(r)) == (403, "forbidden")
    r = await admin.client.put(f"{views}/{view_id}", json=VIEW | {"name": "Revue admin"})
    assert r.status_code == 200, r.text
    assert r.json()["created_by"] == str(member.user_id)
    assert (await owner.client.delete(f"{views}/{view_id}")).status_code == 204


# Tags -----------------------------------------------------------------------------------------


def _tags_url(ws: Workspace, doc: DocumentLedger) -> str:
    return f"/v1/workspaces/{ws.id}/documents/{doc.id}/tags"


async def test_tags_replace_and_filter(
    make_actor: MakeActor, member: Actor, ws: Workspace, db: AsyncSession,
    memory_store: MemoryObjectStore, index_conn: AsyncConnection,
) -> None:  # fmt: skip
    doc = await _indexed(db, memory_store, index_conn, ws)
    other = await _indexed(db, memory_store, index_conn, ws)
    r = await member.client.put(_tags_url(ws, doc), json={"tags": [" Urgent ", "BUREAU", "urgent"]})
    assert r.status_code == 200, r.text
    assert r.json() == {"tags": ["bureau", "urgent"]}
    assert await _ids(member, ws, tags=["urgent"]) == [str(doc.id)]
    assert await _ids(member, ws, tags=["urgent", "bureau"]) == [str(doc.id)]

    r = await member.client.put(_tags_url(ws, doc), json={"tags": ["bureau"]})
    assert r.json() == {"tags": ["bureau"]}
    assert await _ids(member, ws, tags=["urgent"]) == []
    assert set(await _ids(member, ws)) == {str(doc.id), str(other.id)}

    for bad in (["x" * 51], [""], ["   "], [f"t{i}" for i in range(21)], ["a\u0000b"], "x"):
        r = await member.client.put(_tags_url(ws, doc), json={"tags": bad})
        assert (r.status_code, _code(r)) == (422, "tag.invalid"), bad
    events = await _events(db, ws, "document.tagged")
    assert [e.data for e in events] == [{"count": 2}, {"count": 1}]
    assert all(e.target_id == str(doc.id) for e in events)

    reader = await _actor(make_actor, db, ws, Role.readonly)
    r = await reader.client.put(_tags_url(ws, doc), json={"tags": []})
    assert (r.status_code, _code(r)) == (403, "forbidden")
    r = await member.client.put(f"/v1/workspaces/{ws.id}/documents/{uuid.uuid4()}/tags",
                                json={"tags": []})  # fmt: skip
    assert (r.status_code, _code(r)) == (404, "document.not_found")


async def test_tags_on_sealed_allowed_fields_untouched(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    doc = await _indexed(db, memory_store, index_conn, ws)
    r = await member.client.post(f"/v1/workspaces/{ws.id}/documents/{doc.id}/seal")
    assert r.status_code == 200, r.text
    before = await index.read_fields(index_conn, doc.id)
    assert before is not None and before.sealed_at is not None

    r = await member.client.put(_tags_url(ws, doc), json={"tags": ["contrôle 2026"]})
    assert r.status_code == 200, r.text
    r = await member.client.put(_tags_url(ws, doc), json={"tags": []})
    assert r.status_code == 200, r.text
    assert await index.read_fields(index_conn, doc.id) == before
    await db.refresh(doc)
    assert doc.fields_sha256 == before.fields_sha256 and doc.state is DocumentState.sealed
    assert await _ids(member, ws, status=["sealed"]) == [str(doc.id)]

    removed = await _indexed(db, memory_store, index_conn, ws)
    await db.execute(
        text(
            "UPDATE document_ledger SET state = 'removed', removed_at = now(), removed_by = :u,"
            " removal_reason = 'Doublon SAMPLE par erreur' WHERE id = :id"
        ),
        {"id": removed.id, "u": member.user_id},
    )
    await db.commit()
    r = await member.client.put(_tags_url(ws, removed), json={"tags": ["x"]})
    assert (r.status_code, _code(r)) == (409, "document.not_reviewable")
