"""The Ask API (plan 5.3): a question is queued in the workspace's index database and sent to
the Ask worker; its answer is read back by the asker, an Owner or an Admin.

Each test gets its own index database (search rows cannot be deleted). The worker is not run:
its answer is stored as it stores one (`ask_store.store_answer`).
"""

import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core import ask_store, index
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

NOW = datetime(2026, 11, 9, 10, 0, tzinfo=UTC)
QUESTION = "Quelle TVA avons-nous payée à Atlas Télécom en 2026 ?"


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


def _url(ws: Workspace) -> str:
    return f"/v1/workspaces/{ws.id}/ask"


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


async def _indexed(
    db: AsyncSession, store: MemoryObjectStore, conn: AsyncConnection, ws: Workspace
) -> DocumentLedger:
    row = await add_document(db, store, conn, ws, state=DocumentState.filed, values=INVOICE)
    facts = SearchLedgerFacts(
        ledger_id=row.id, state=DocumentState.filed, ingested_at=row.ingested_at,
        region="ma-cas", now=row.ingested_at,
    )  # fmt: skip
    await refresh_search_doc(conn, facts, "Facture SAMPLE Atlas Télécom")
    return row


async def _asked(actor: Actor, ws: Workspace, locale: str = "fr") -> str:
    r = await actor.client.post(_url(ws), json={"question": QUESTION, "locale": locale})
    assert r.status_code == 202, r.text
    return str(r.json()["id"])


async def _store(conn: AsyncConnection, ask_id: str, cited: list[uuid.UUID]) -> None:
    answer = ask_store.Answer(
        intent={"kind": "sum"}, answer_text=None, answer_code="ask.answer.sum",
        figures=[{"measure": "vat", "value": "250.00", "currency": "MAD", "document_count": 1}],
        citations=cited, counted=cited, excluded={"needs_review": 2},
    )  # fmt: skip
    assert await ask_store.store_answer(conn, uuid.UUID(ask_id), answer, now=NOW)


async def test_ask_returns_202_then_answer(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    doc = await _indexed(db, memory_store, index_conn, ws)
    r = await member.client.post(_url(ws), json={"question": f"  {QUESTION} ", "locale": "fr"})
    assert r.status_code == 202, r.text
    assert r.headers["server-timing"].startswith("app;dur=")
    ask_id = r.json()["id"]
    assert r.json() == {"id": ask_id, "state": "queued"}
    # Queued in the workspace's index database, then sent to the Ask worker.
    record = await ask_store.read_ask(index_conn, uuid.UUID(ask_id))
    assert record is not None
    assert (record.question, record.locale, record.state) == (QUESTION, "fr", "queued")
    assert record.created_by == member.user_id
    assert enqueuer.asks == [(uuid.UUID(ask_id), ws.id)]

    r = await member.client.get(f"{_url(ws)}/{ask_id}")
    assert r.status_code == 200, r.text
    assert r.json() == {
        "id": ask_id, "state": "queued", "locale": "fr", "answer_text": None,
        "answer_code": None, "figures": [], "excluded": {"needs_review": 0}, "citations": [],
        "error_code": None,
    }  # fmt: skip

    await _store(index_conn, ask_id, [doc.id])
    r = await member.client.get(f"{_url(ws)}/{ask_id}")
    assert r.status_code == 200, r.text
    assert r.json() == {
        "id": ask_id, "state": "answered", "locale": "fr", "answer_text": None,
        "answer_code": "ask.answer.sum",
        "figures": [
            {"measure": "vat", "value": "250.00", "currency": "MAD", "document_count": 1}
        ],
        "excluded": {"needs_review": 2},
        "citations": [
            {"id": str(doc.id), "supplier_name": INVOICE["supplier_name"],
             "basis_date": INVOICE["date"], "total_ttc": "1500.00", "currency": "MAD"}
        ],
        "error_code": None,
    }  # fmt: skip

    # A cited document removed (its purge will delete the answer): the answer is gone already.
    await mark_search_state(index_conn, doc.id, DocumentState.removed)
    r = await member.client.get(f"{_url(ws)}/{ask_id}")
    assert (r.status_code, _code(r)) == (404, "ask.not_found")


async def test_ask_read_by_other_member_404(
    member: Actor, ws: Workspace, db: AsyncSession, make_actor: MakeActor
) -> None:
    ask_id = await _asked(member, ws)
    other = await _actor(make_actor, db, ws, Role.member)
    r = await other.client.get(f"{_url(ws)}/{ask_id}")
    assert (r.status_code, _code(r)) == (404, "ask.not_found")
    for role in (Role.owner, Role.admin):
        reader = await _actor(make_actor, db, ws, role)
        r = await reader.client.get(f"{_url(ws)}/{ask_id}")
        assert r.status_code == 200, (role, r.text)
    r = await member.client.get(f"{_url(ws)}/{uuid.uuid4()}")
    assert (r.status_code, _code(r)) == (404, "ask.not_found")


async def test_ask_rate_limited(member: Actor, ws: Workspace, enqueuer: Any) -> None:
    for _ in range(20):
        await _asked(member, ws)
    r = await member.client.post(_url(ws), json={"question": QUESTION, "locale": "ar"})
    assert (r.status_code, _code(r)) == (429, "ask.rate_limited")
    assert len(enqueuer.asks) == 20


async def test_ask_audit_has_no_question_text(
    member: Actor, ws: Workspace, db: AsyncSession
) -> None:
    ask_id = await _asked(member, ws, "ar")
    [event] = await _events(db, ws, "ask.asked")
    assert event.data == {"locale": "ar"}
    assert (event.target_type, event.target_id) == ("ask", ask_id)
    assert event.actor_id == str(member.user_id)
    dumped = json.dumps(event.data, ensure_ascii=False).casefold()
    for secret in ("atlas", "tva", "2026"):
        assert secret not in dumped


async def test_ask_refuses_bad_bodies(member: Actor, ws: Workspace, enqueuer: Any) -> None:
    for body in (
        {"question": "ab", "locale": "fr"},
        {"question": "   ab   ", "locale": "fr"},
        {"question": "x" * 501, "locale": "fr"},
        {"question": QUESTION, "locale": "de"},
        {"question": QUESTION},
        {"question": QUESTION, "locale": "fr", "supplier": "Atlas"},
        {"question": 42, "locale": "fr"},
    ):
        r = await member.client.post(_url(ws), json=body)
        assert (r.status_code, _code(r)) == (422, "ask.question_invalid"), body
    assert enqueuer.asks == []


async def test_ask_needs_documents_read(
    make_actor: MakeActor, db: AsyncSession, ws: Workspace
) -> None:
    outsider = await make_actor(f"outsider-{uuid.uuid4().hex[:6]}@example.com")
    r = await outsider.client.post(_url(ws), json={"question": QUESTION, "locale": "fr"})
    assert r.status_code in (403, 404)


async def test_ask_region_asserted(
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
    r = await actor.client.post(_url(foreign), json={"question": QUESTION, "locale": "fr"})
    assert (r.status_code, _code(r)) == (421, "region.mismatch")
    r = await actor.client.get(f"{_url(foreign)}/{uuid.uuid4()}")
    assert (r.status_code, _code(r)) == (421, "region.mismatch")
