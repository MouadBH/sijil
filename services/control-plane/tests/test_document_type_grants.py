"""Document-type grants (plan 6.1, ADM-1): a Member or Read-only user limited to some types sees
only documents of those types, and their own uploads not yet classified.

Each test gets its own index database (search rows cannot be deleted). The type is the search
row's (`search_doc.document_type`), written here as the worker writes it at routing. SAMPLE
values only.
"""

import base64
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core import ask_store, index
from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    DocumentState,
    Membership,
    Role,
    Workspace,
)
from sijil_core.search_index import SearchLedgerFacts, refresh_search_doc
from sijil_cp import permissions
from sijil_cp.api import review
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import INVOICE, add_document, add_index_credential, engines_on

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db

PAYSLIP: dict[str, str | None] = INVOICE | {
    "document_type": "payslip",
    "supplier_name": "Paie SAMPLE SARL",
    "supplier_ice": "002905837000042",
    "number": "P-2026-09",
}
UNCLASSIFIED: dict[str, str | None] = INVOICE | {"document_type": None, "number": "U-2026-01"}


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


async def _actor(
    make_actor: MakeActor, db: AsyncSession, ws: Workspace, role: Role,
    types: list[str] | None = None,
) -> Actor:  # fmt: skip
    actor = await make_actor(f"{role.value}-{uuid.uuid4().hex[:6]}@example.com")
    db.add(Membership(user_id=actor.user_id, workspace_id=ws.id, role=role, document_types=types))
    await db.commit()
    return actor


async def _doc(
    db: AsyncSession, store: MemoryObjectStore, conn: AsyncConnection, ws: Workspace,
    values: dict[str, str | None], *, state: DocumentState = DocumentState.filed,
    uploaded_by: uuid.UUID | None = None,
) -> DocumentLedger:  # fmt: skip
    """A ledger row, its fields and its search row (as the worker writes it at routing)."""
    row = await add_document(
        db, store, conn, ws, state=state, values=values,
        document_type=values.get("document_type"), uploaded_by=uploaded_by,
    )  # fmt: skip
    facts = SearchLedgerFacts(
        ledger_id=row.id, state=state, ingested_at=row.ingested_at, region="ma-cas",
        now=row.ingested_at,
    )  # fmt: skip
    await refresh_search_doc(conn, facts, "SAMPLE document")
    return row


@pytest.fixture
async def invoice(
    db: AsyncSession, memory_store: MemoryObjectStore, index_conn: AsyncConnection, ws: Workspace
) -> DocumentLedger:
    return await _doc(db, memory_store, index_conn, ws, INVOICE)


@pytest.fixture
async def payslip(
    db: AsyncSession, memory_store: MemoryObjectStore, index_conn: AsyncConnection, ws: Workspace
) -> DocumentLedger:
    return await _doc(db, memory_store, index_conn, ws, PAYSLIP)


def _code(r: Any) -> str:
    return str(r.json()["error"]["code"])


async def _listed(actor: Actor, ws: Workspace, **params: Any) -> list[str]:
    r = await actor.client.get(f"/v1/workspaces/{ws.id}/documents", params=params)
    assert r.status_code == 200, r.text
    return [item["id"] for item in r.json()["items"]]


async def _found(actor: Actor, ws: Workspace, **filters: Any) -> list[str]:
    r = await actor.client.post(
        f"/v1/workspaces/{ws.id}/search", json={"filters": filters, "sort": "date_desc"}
    )
    assert r.status_code == 200, r.text
    return [item["id"] for item in r.json()["items"]]


# The members API ------------------------------------------------------------------------------


async def _org(actor: Actor) -> str:
    r = await actor.client.post(
        "/v1/orgs", json={"name": "Atlas SAMPLE SARL", "type": "business", "country": "MA"}
    )
    assert r.status_code == 202, r.text
    return str(r.json()["org"]["id"])


async def test_grant_rejects_types_for_admin_and_unknown_types(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    await make_actor("carol@example.com")
    org_id = await _org(alice)
    await alice.enable_totp()
    await alice.step_up()
    members = f"/v1/orgs/{org_id}/members"

    r = await alice.client.post(
        members, json={"email": "carol@example.com", "role": "admin", "document_types": ["invoice"]}
    )
    assert (r.status_code, _code(r)) == (422, "members.types_not_allowed_for_role")
    for bad in (["invoice", "recipe"], [], ["invoice", "invoice"]):
        r = await alice.client.post(
            members, json={"email": "bob@example.com", "role": "member", "document_types": bad}
        )
        assert (r.status_code, _code(r)) == (422, "members.types_invalid"), bad

    r = await alice.client.post(
        members, json={"email": "bob@example.com", "role": "member", "document_types": ["invoice"]}
    )
    assert r.status_code == 201, r.text
    assert r.json()["document_types"] == ["invoice"]
    listed = {m["email"]: m["document_types"] for m in (await alice.client.get(members)).json()}
    assert listed == {"alice@example.com": None, "bob@example.com": ["invoice"]}

    bob_url = f"{members}/{bob.user_id}"
    r = await alice.client.patch(bob_url, json={"document_types": ["payslip", "nope"]})
    assert (r.status_code, _code(r)) == (422, "members.types_invalid")
    r = await alice.client.patch(bob_url, json={"role": "admin", "document_types": ["invoice"]})
    assert (r.status_code, _code(r)) == (422, "members.types_not_allowed_for_role")
    r = await alice.client.patch(bob_url, json={"document_types": ["payslip", "invoice"]})
    assert r.status_code == 200, r.text
    assert r.json()["document_types"] == ["invoice", "payslip"]
    # Promoted to admin: the grant goes, an admin sees every type.
    r = await alice.client.patch(bob_url, json={"role": "admin"})
    assert r.status_code == 200, r.text
    assert r.json()["document_types"] is None
    db.expire_all()
    stored = await db.scalar(
        select(Membership.document_types).where(Membership.user_id == bob.user_id)
    )
    assert stored is None

    events = list(
        await db.scalars(
            select(AuditEvent)
            .where(AuditEvent.action == "member.types_changed")
            .order_by(AuditEvent.seq)
        )
    )
    assert [e.data for e in events] == [{"count": 1}, {"count": 2}, {"count": None}]
    assert all(e.target_id == str(bob.user_id) for e in events)


# Visibility ----------------------------------------------------------------------------------


async def test_member_limited_to_invoices_lists_only_invoices(
    make_actor: MakeActor, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, ws: Workspace,
) -> None:  # fmt: skip
    invoices = [await _doc(db, memory_store, index_conn, ws, INVOICE) for _ in range(3)]
    payslips = [await _doc(db, memory_store, index_conn, ws, PAYSLIP) for _ in range(3)]
    review = await _doc(db, memory_store, index_conn, ws, PAYSLIP, state=DocumentState.needs_review)
    review_invoice = await _doc(
        db, memory_store, index_conn, ws, INVOICE, state=DocumentState.needs_review
    )
    clerk = await _actor(make_actor, db, ws, Role.member, ["invoice"])
    reader = await _actor(make_actor, db, ws, Role.readonly, ["invoice"])
    everyone = await _actor(make_actor, db, ws, Role.member)

    wanted = {str(d.id) for d in [*invoices, review_invoice]}
    for actor in (clerk, reader):
        assert set(await _listed(actor, ws)) == wanted
    assert set(await _listed(everyone, ws)) == wanted | {str(d.id) for d in [*payslips, review]}

    # Paging walks past the hidden rows: one page at a time, every invoice once.
    seen: list[str] = []
    cursor: str | None = None
    while True:
        params: dict[str, Any] = {"limit": 1} | ({"cursor": cursor} if cursor else {})
        r = await clerk.client.get(f"/v1/workspaces/{ws.id}/documents", params=params)
        assert r.status_code == 200
        seen += [i["id"] for i in r.json()["items"]]
        cursor = r.json()["next_cursor"]
        if cursor is None:
            break
    assert sorted(seen) == sorted(wanted)

    r = await clerk.client.get(f"/v1/workspaces/{ws.id}/review")
    assert r.status_code == 200
    assert [i["id"] for i in r.json()["items"]] == [str(review_invoice.id)]
    listed = (
        await db.scalars(
            select(AuditEvent.data)
            .where(AuditEvent.workspace_id == ws.id, AuditEvent.action == "documents.listed")
            .order_by(AuditEvent.seq)
        )
    ).first()
    assert listed == {"count": len(wanted)}


BY_ID: list[tuple[str, str, dict[str, Any] | None]] = [
    ("GET", "", None),
    ("GET", "/fields", None),
    ("PATCH", "/fields", {"fields": {"number": "F-SAMPLE-9"}}),
    ("POST", "/seal", None),
    ("PUT", "/tags", {"tags": ["SAMPLE"]}),
    ("POST", "/corrections", {"field": "number", "value": "F-9", "reason": "SAMPLE typo fix"}),
]


@pytest.mark.parametrize(("method", "suffix", "body"), BY_ID)
async def test_hidden_document_by_id_is_404_on_every_route(
    method: str, suffix: str, body: dict[str, Any] | None,
    make_actor: MakeActor, db: AsyncSession, ws: Workspace,
    invoice: DocumentLedger, payslip: DocumentLedger, index_conn: AsyncConnection,
) -> None:  # fmt: skip
    payslip_id, invoice_id = payslip.id, invoice.id
    clerk = await _actor(make_actor, db, ws, Role.member, ["invoice"])
    base = f"/v1/workspaces/{ws.id}/documents"
    hidden = await clerk.client.request(method, f"{base}/{payslip_id}{suffix}", json=body)
    unknown = await clerk.client.request(method, f"{base}/{uuid.uuid4()}{suffix}", json=body)
    assert hidden.status_code == 404, hidden.text
    # Indistinguishable from a document that does not exist.
    assert hidden.json() == unknown.json()
    db.expire_all()
    still = await db.get(DocumentLedger, payslip_id)
    assert still is not None and still.state is DocumentState.filed
    tags = await index_conn.scalar(
        text("SELECT tags FROM search_doc WHERE ledger_id = :id"), {"id": payslip_id}
    )
    assert tags == []
    if method == "GET":  # the invoice itself still answers
        shown = await clerk.client.get(f"{base}/{invoice_id}{suffix}")
        assert shown.status_code == 200, shown.text


async def test_unclassified_document_visible_only_to_its_uploader(
    make_actor: MakeActor, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, ws: Workspace, invoice: DocumentLedger,
) -> None:  # fmt: skip
    uploader = await _actor(make_actor, db, ws, Role.member, ["invoice"])
    colleague = await _actor(make_actor, db, ws, Role.member, ["invoice"])
    everyone = await _actor(make_actor, db, ws, Role.readonly)
    mine = await _doc(db, memory_store, index_conn, ws, UNCLASSIFIED, uploaded_by=uploader.user_id)
    # No search row at all yet (just received): unclassified too.
    fresh = await add_document(
        db, memory_store, index_conn, ws, state=DocumentState.received, values=None,
        uploaded_by=uploader.user_id,
    )  # fmt: skip
    theirs = await _doc(
        db, memory_store, index_conn, ws, UNCLASSIFIED | {"number": "U-2"},
        uploaded_by=everyone.user_id,
    )  # fmt: skip

    assert set(await _listed(uploader, ws)) == {str(invoice.id), str(mine.id), str(fresh.id)}
    assert set(await _found(uploader, ws)) == {str(invoice.id), str(mine.id)}
    for doc in (mine, fresh):
        r = await uploader.client.get(f"/v1/workspaces/{ws.id}/documents/{doc.id}")
        assert r.status_code == 200, r.text

    assert set(await _listed(colleague, ws)) == {str(invoice.id)}
    assert set(await _found(colleague, ws)) == {str(invoice.id)}
    for doc in (mine, fresh, theirs):
        r = await colleague.client.get(f"/v1/workspaces/{ws.id}/documents/{doc.id}")
        assert r.status_code == 404
    assert len(await _listed(everyone, ws)) == 4


async def test_search_counts_and_saved_views_respect_grants(
    make_actor: MakeActor, db: AsyncSession, ws: Workspace,
    invoice: DocumentLedger, payslip: DocumentLedger,
) -> None:  # fmt: skip
    clerk = await _actor(make_actor, db, ws, Role.member, ["invoice"])
    admin = await _actor(make_actor, db, ws, Role.admin)
    assert await _found(clerk, ws) == [str(invoice.id)]
    assert set(await _found(admin, ws)) == {str(invoice.id), str(payslip.id)}
    counts = list(
        await db.scalars(
            select(AuditEvent.data)
            .where(AuditEvent.workspace_id == ws.id, AuditEvent.action == "search.performed")
            .order_by(AuditEvent.seq)
        )
    )
    assert [c["result_count"] for c in counts] == [1, 2]

    # A view the admin saved for payslips shows the clerk nothing when run.
    r = await admin.client.post(
        f"/v1/workspaces/{ws.id}/search/views",
        json={"name": "Paie", "filters": {"document_type": ["payslip"]}, "sort": "date_desc"},
    )
    assert r.status_code == 201, r.text
    views = (await clerk.client.get(f"/v1/workspaces/{ws.id}/search/views")).json()["items"]
    [view] = [v for v in views if v["name"] == "Paie"]
    assert await _found(clerk, ws, **view["filters"]) == []
    assert await _found(admin, ws, **view["filters"]) == [str(payslip.id)]
    # A view the clerk saves cannot carry a grant either.
    r = await clerk.client.post(
        f"/v1/workspaces/{ws.id}/search/views",
        json={"name": "Mine", "filters": {"allowed_types": ["payslip"]}},
    )
    assert (r.status_code, _code(r)) == (422, "search.query_invalid")


async def test_restricted_member_cannot_widen_search_with_document_type_filter(
    make_actor: MakeActor, db: AsyncSession, ws: Workspace,
    invoice: DocumentLedger, payslip: DocumentLedger,
) -> None:  # fmt: skip
    clerk = await _actor(make_actor, db, ws, Role.member, ["invoice"])
    assert await _found(clerk, ws, document_type=["payslip"]) == []
    assert await _found(clerk, ws, document_type=["invoice", "payslip"]) == [str(invoice.id)]
    assert await _found(clerk, ws, q="P-2026-09") == []
    r = await clerk.client.post(
        f"/v1/workspaces/{ws.id}/search", json={"filters": {"allowed_types": ["payslip"]}}
    )
    assert (r.status_code, _code(r)) == (422, "search.query_invalid")


async def test_union_of_org_and_workspace_memberships(
    make_actor: MakeActor, db: AsyncSession, ws: Workspace,
    invoice: DocumentLedger, payslip: DocumentLedger,
) -> None:  # fmt: skip
    both = await _actor(make_actor, db, ws, Role.readonly, ["payslip"])
    db.add(
        Membership(user_id=both.user_id, org_id=ws.org_id, role=Role.member,
                   document_types=["invoice"])
    )  # fmt: skip
    await db.commit()
    assert await permissions.visible_types(db, both.user_id, ws) == frozenset(
        {"invoice", "payslip"}
    )
    assert set(await _listed(both, ws)) == {str(invoice.id), str(payslip.id)}

    # Any unrestricted membership, or an admin's, means every type.
    open_ws = await _actor(make_actor, db, ws, Role.readonly)
    db.add(
        Membership(user_id=open_ws.user_id, org_id=ws.org_id, role=Role.member,
                   document_types=["invoice"])
    )  # fmt: skip
    admin = await make_actor("admin@example.com")
    db.add(Membership(user_id=admin.user_id, org_id=ws.org_id, role=Role.admin))
    db.add(
        Membership(user_id=admin.user_id, workspace_id=ws.id, role=Role.readonly,
                   document_types=["invoice"])
    )  # fmt: skip
    await db.commit()
    assert await permissions.visible_types(db, open_ws.user_id, ws) is None
    assert await permissions.visible_types(db, admin.user_id, ws) is None
    assert set(await _listed(open_ws, ws)) == {str(invoice.id), str(payslip.id)}


async def test_restricted_ask_stores_the_askers_grant(
    make_actor: MakeActor, db: AsyncSession, ws: Workspace, index_conn: AsyncConnection,
) -> None:  # fmt: skip
    clerk = await _actor(make_actor, db, ws, Role.member, ["payslip", "invoice"])
    everyone = await _actor(make_actor, db, ws, Role.member)
    stored = {}
    asked = {}
    for actor in (clerk, everyone):
        r = await actor.client.post(
            f"/v1/workspaces/{ws.id}/ask",
            json={"question": "Combien de factures ?", "locale": "fr"},
        )
        assert r.status_code == 202, r.text
        asked[actor] = r.json()["id"]
        record = await ask_store.read_ask(index_conn, uuid.UUID(asked[actor]))
        assert record is not None
        stored[actor] = record.document_types
    assert stored == {clerk: ["invoice", "payslip"], everyone: None}

    mine = f"/v1/workspaces/{ws.id}/ask/{asked[clerk]}"
    assert (await clerk.client.get(mine)).status_code == 200
    # Narrowed since: the answer, computed over payslips too, is no longer the clerk's to read.
    await db.execute(
        update(Membership)
        .where(Membership.user_id == clerk.user_id)
        .values(document_types=["invoice"])
    )
    await db.commit()
    r = await clerk.client.get(mine)
    assert (r.status_code, _code(r)) == (404, "ask.not_found")


def _cursor_id(cursor: str) -> str:
    """The ledger id a seal-batch cursor carries (plain base64 of `ingested_at|id`)."""
    raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
    return raw.split("|")[1]


async def test_seal_batch_cursor_never_names_a_hidden_document(
    make_actor: MakeActor, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, ws: Workspace, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    monkeypatch.setattr(review, "SEAL_BATCH_LIMIT", 2)
    order: list[tuple[str, dict[str, str | None]]] = [
        ("i1", INVOICE), ("p1", PAYSLIP), ("i2", INVOICE | {"number": "F-2"}),
        ("p2", PAYSLIP | {"number": "P-2"}), ("i3", INVOICE | {"number": "F-3"}),
        ("p3", PAYSLIP | {"number": "P-3"}),
    ]  # fmt: skip
    docs: dict[str, DocumentLedger] = {}
    for day, (name, values) in enumerate(order, start=2):
        row = await add_document(
            db, memory_store, index_conn, ws, values=values,
            document_type=values["document_type"], ingested_at=datetime(2026, 10, day, tzinfo=UTC),
        )  # fmt: skip
        facts = SearchLedgerFacts(
            ledger_id=row.id, state=DocumentState.filed, ingested_at=row.ingested_at,
            region="ma-cas", now=row.ingested_at,
        )  # fmt: skip
        await refresh_search_doc(index_conn, facts, "SAMPLE document")
        docs[name] = row
    hidden = {str(docs[n].id) for n in ("p1", "p2", "p3")}
    invoices = {str(docs[n].id) for n in ("i1", "i2", "i3")}
    clerk = await _actor(make_actor, db, ws, Role.member, ["invoice"])
    url = f"/v1/workspaces/{ws.id}/seal-batch"

    pages = []
    body: dict[str, Any] = {"month": "2026-10"}
    while True:
        r = await clerk.client.post(url, json=body)
        assert r.status_code == 200, r.text
        page = r.json()
        pages.append(page)
        assert page["skipped"] == []
        if page["next_cursor"] is None:
            break
        assert _cursor_id(page["next_cursor"]) not in hidden
        body = {"month": "2026-10", "cursor": page["next_cursor"]}
        assert len(pages) < 5
    assert [(p["sealed"], p["remaining"]) for p in pages] == [(2, 1), (1, 0)]
    ws_id = ws.id
    db.expire_all()
    rows = await db.scalars(select(DocumentLedger).where(DocumentLedger.workspace_id == ws_id))
    states = {str(r.id): r.state for r in rows}
    assert {i for i, s in states.items() if s is DocumentState.sealed} == invoices
    assert all(states[h] is DocumentState.filed for h in hidden)


PDF = (Path(__file__).resolve().parents[3] / "fixtures" / "formats" / "sample.pdf").read_bytes()


async def test_duplicate_upload_names_the_existing_document_only_when_visible(
    make_actor: MakeActor, db: AsyncSession, index_conn: AsyncConnection, ws: Workspace,
) -> None:  # fmt: skip
    admin = await _actor(make_actor, db, ws, Role.admin)
    clerk = await _actor(make_actor, db, ws, Role.member, ["invoice"])
    url = f"/v1/workspaces/{ws.id}/documents"

    async def upload(actor: Actor) -> Any:
        return await actor.client.post(url, files={"file": ("f.pdf", PDF, "application/pdf")})

    first = await upload(admin)
    assert first.status_code == 202, first.text
    existing = first.json()["document_id"]
    # Unclassified and the admin's: hidden from the clerk, whose 409 does not name it.
    r = await upload(clerk)
    assert r.status_code == 409
    assert r.json() == {"error": {"code": "document.duplicate"}}
    # The admin still gets the id.
    r = await upload(admin)
    assert r.json() == {"error": {"code": "document.duplicate"}, "document_id": existing}
    # Classified as an invoice, it is the clerk's to see: the id comes back.
    when = datetime(2026, 10, 1, tzinfo=UTC)
    await index.write_extraction(
        index_conn,
        index.FieldsRow(
            ledger_id=uuid.UUID(existing), document_type="invoice", values=INVOICE,
            updated_at=when,
        ),
    )  # fmt: skip
    facts = SearchLedgerFacts(
        ledger_id=uuid.UUID(existing), state=DocumentState.received, ingested_at=when,
        region="ma-cas", now=when,
    )  # fmt: skip
    await refresh_search_doc(index_conn, facts, "SAMPLE document")
    r = await upload(clerk)
    assert r.status_code == 409
    assert r.json() == {"error": {"code": "document.duplicate"}, "document_id": existing}
