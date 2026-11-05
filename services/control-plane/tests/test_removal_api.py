"""The Owner's pre-seal removal (plan 4.3, design §5.4 "Uploaded by mistake").

`POST …/documents/{id}/remove` needs the Owner, a TOTP step-up within 10 minutes and a written
reason; it turns the ledger row into a tombstone and, after the commit, sends
`sijil.remove_original`. Every refusal to a signed-in caller is audited. The sealed paths are in
the lock suite (tests/retention/test_removal_paths.py).
"""

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import timedelta
from typing import Any

import pytest
from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core import index
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    DocumentState,
    Membership,
    Role,
    Workspace,
)
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import add_document, add_index_credential, engines_on

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db

REASON = "Facture déposée deux fois par erreur (SAMPLE)"


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


async def _actor(
    make_actor: MakeActor, db: AsyncSession, ws: Workspace, role: Role, *, step_up: bool = True
) -> Actor:
    actor = await make_actor(f"{role.value}-{uuid.uuid4().hex[:6]}@example.com")
    db.add(Membership(user_id=actor.user_id, workspace_id=ws.id, role=role))
    await db.commit()
    if step_up:
        await actor.enable_totp()
        await actor.step_up()
    return actor


@pytest.fixture
async def owner(make_actor: MakeActor, db: AsyncSession, ws: Workspace) -> Actor:
    return await _actor(make_actor, db, ws, Role.owner)


def _remove_url(ws: Workspace, doc_id: uuid.UUID) -> str:
    return f"/v1/workspaces/{ws.id}/documents/{doc_id}/remove"


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


async def _ledger(db: AsyncSession, doc_id: uuid.UUID) -> DocumentLedger:
    found = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == doc_id)
        .execution_options(populate_existing=True)
    )
    assert found is not None
    return found


async def _refusals(db: AsyncSession, ws: Workspace) -> list[tuple[str | None, object]]:
    return [(e.target_id, e.data) for e in await _events(db, ws, "document.removal_refused")]


@pytest.mark.parametrize(
    "state", [DocumentState.received, DocumentState.needs_review, DocumentState.filed]
)
async def test_owner_can_remove_unsealed_with_reason(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any, redis: Redis, state: DocumentState,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws, state=state)
    r = await owner.client.post(_remove_url(ws, row.id), json={"reason": f"  {REASON}\n"})
    assert r.status_code == 202, r.text
    assert r.json() == {"id": str(row.id), "state": "removed"}

    ledger = await _ledger(db, row.id)
    assert ledger.state is DocumentState.removed
    assert ledger.removed_by == owner.user_id
    assert ledger.removed_at == owner.clock.now
    assert ledger.removal_reason == REASON  # trimmed
    [event] = await _events(db, ws, "document.removed")
    assert (event.target_type, event.target_id) == ("document", str(row.id))
    assert event.actor_id == str(owner.user_id)
    # The audit names the reason's length only, never its text.
    assert event.data == {"reason_length": len(REASON)}
    assert enqueuer.removals == [(row.id, ws.id)]
    # Claimed before the send, so the `requeue_unremoved` sweep never sends it again meanwhile.
    assert await redis.exists(f"remove:pending:{row.id}")
    assert 0 < await redis.ttl(f"remove:pending:{row.id}") <= 3600


@pytest.mark.parametrize("role", [Role.admin, Role.member, Role.readonly])
async def test_admin_cannot_remove(
    make_actor: MakeActor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any, role: Role,
) -> None:  # fmt: skip
    """Admin, Member and ReadOnly, even stepped up, get 403 `permission.denied`: audited."""
    actor = await _actor(make_actor, db, ws, role)
    row = await add_document(db, memory_store, index_conn, ws)
    r = await actor.client.post(_remove_url(ws, row.id), json={"reason": REASON})
    assert (r.status_code, _code(r)) == (403, "permission.denied")
    assert (await _ledger(db, row.id)).state is DocumentState.filed
    assert await _refusals(db, ws) == [(str(row.id), {"code": "permission.denied"})]
    [refused] = await _events(db, ws, "document.removal_refused")
    assert refused.actor_id == str(actor.user_id)
    assert await _events(db, ws, "document.removed") == []
    assert enqueuer.removals == []


async def test_remove_requires_step_up(
    make_actor: MakeActor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    url = _remove_url(ws, row.id)
    # An Owner without TOTP can never step up.
    plain = await _actor(make_actor, db, ws, Role.owner, step_up=False)
    r = await plain.client.post(url, json={"reason": REASON})
    assert (r.status_code, _code(r)) == (403, "auth.step_up_required")
    # A step-up older than 10 minutes no longer counts.
    owner = await _actor(make_actor, db, ws, Role.owner)
    owner.clock.advance(timedelta(minutes=11))
    r = await owner.client.post(url, json={"reason": REASON})
    assert (r.status_code, _code(r)) == (403, "auth.step_up_required")
    assert await _refusals(db, ws) == [
        (str(row.id), {"code": "auth.step_up_required"}),
        (str(row.id), {"code": "auth.step_up_required"}),
    ]
    assert (await _ledger(db, row.id)).state is DocumentState.filed
    assert enqueuer.removals == []
    await owner.step_up()
    assert (await owner.client.post(url, json={"reason": REASON})).status_code == 202


@pytest.mark.parametrize(
    "body",
    [{}, {"reason": None}, {"reason": ""}, {"reason": "  9 chars  "}, {"reason": "x" * 1001}],
)
async def test_remove_requires_reason(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any, body: dict[str, Any],
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    r = await owner.client.post(_remove_url(ws, row.id), json=body)
    assert (r.status_code, _code(r)) == (422, "removal.reason_required")
    assert (await _ledger(db, row.id)).state is DocumentState.filed
    assert await _refusals(db, ws) == [(str(row.id), {"code": "removal.reason_required"})]
    assert enqueuer.removals == []


@pytest.mark.parametrize("reason", ["  ten chars! ", "y" * 1000])
async def test_reason_bounds_are_ten_and_a_thousand_after_trimming(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, reason: str,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    r = await owner.client.post(_remove_url(ws, row.id), json={"reason": reason})
    assert r.status_code == 202, r.text
    assert (await _ledger(db, row.id)).removal_reason == reason.strip()


async def test_remove_twice_is_not_removable(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    url = _remove_url(ws, row.id)
    assert (await owner.client.post(url, json={"reason": REASON})).status_code == 202
    r = await owner.client.post(url, json={"reason": "Une autre raison (SAMPLE)"})
    assert (r.status_code, _code(r)) == (409, "document.not_removable")
    assert (await _ledger(db, row.id)).removal_reason == REASON
    assert await _refusals(db, ws) == [(str(row.id), {"code": "document.not_removable"})]
    assert len(await _events(db, ws, "document.removed")) == 1
    assert enqueuer.removals == [(row.id, ws.id)]


async def test_remove_of_an_unknown_document_is_404_and_audited(
    owner: Actor, ws: Workspace, db: AsyncSession
) -> None:
    unknown = uuid.uuid4()
    r = await owner.client.post(_remove_url(ws, unknown), json={"reason": REASON})
    assert (r.status_code, _code(r)) == (404, "document.not_found")
    assert await _refusals(db, ws) == [(str(unknown), {"code": "document.not_found"})]


class _CheckingEnqueuer:
    """Reads the ledger from its own connection when the removal is sent: it must be committed
    `removed` by then (the worker would otherwise find nothing to do)."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.seen: list[DocumentState] = []

    def send_remove(self, ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        async def read() -> DocumentState:
            engine = make_engine(self.url)
            try:
                async with make_sessionmaker(engine)() as session:
                    state = await session.scalar(
                        select(DocumentLedger.state).where(DocumentLedger.id == ledger_id)
                    )
                    assert state is not None
                    return state
            finally:
                await engine.dispose()

        # The enqueuer runs in a worker thread (asyncio.to_thread): no loop of its own yet.
        self.seen.append(asyncio.run(read()))


async def test_remove_enqueues_after_commit(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, cp_app: FastAPI, migrated_db: Any,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    checking = _CheckingEnqueuer(migrated_db.app_url)
    cp_app.state.enqueuer = checking
    r = await owner.client.post(_remove_url(ws, row.id), json={"reason": REASON})
    assert r.status_code == 202, r.text
    assert checking.seen == [DocumentState.removed]


async def test_a_failed_send_releases_the_claim_for_the_sweep(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, cp_app: FastAPI, redis: Redis,
) -> None:  # fmt: skip
    """The removal stands; the broker being down only leaves it to `requeue_unremoved`."""

    class Down:
        def send_remove(self, ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
            raise ConnectionError("broker down")

    cp_app.state.enqueuer = Down()
    row = await add_document(db, memory_store, index_conn, ws)
    r = await owner.client.post(_remove_url(ws, row.id), json={"reason": REASON})
    assert r.status_code == 202, r.text
    assert (await _ledger(db, row.id)).state is DocumentState.removed
    assert not await redis.exists(f"remove:pending:{row.id}")


async def test_removed_is_hidden_from_lists_and_review(
    owner: Actor, make_actor: MakeActor, ws: Workspace, db: AsyncSession,
    memory_store: MemoryObjectStore, index_conn: AsyncConnection,
) -> None:  # fmt: skip
    kept = await add_document(db, memory_store, index_conn, ws, state=DocumentState.needs_review)
    gone = await add_document(db, memory_store, index_conn, ws, state=DocumentState.needs_review)
    r = await owner.client.post(_remove_url(ws, gone.id), json={"reason": REASON})
    assert r.status_code == 202, r.text

    listed = (await owner.client.get(f"/v1/workspaces/{ws.id}/documents")).json()
    assert [item["id"] for item in listed["items"]] == [str(kept.id)]
    review = (await owner.client.get(f"/v1/workspaces/{ws.id}/review")).json()
    assert [item["id"] for item in review["items"]] == [str(kept.id)]

    # The document itself answers with its tombstone; the reason for Owners only.
    r = await owner.client.get(f"/v1/workspaces/{ws.id}/documents/{gone.id}")
    assert r.status_code == 200, r.text
    assert r.json() == {
        "id": str(gone.id),
        "state": "removed",
        "sha256": gone.sha256,
        "removed_at": owner.clock.now.isoformat().replace("+00:00", "Z"),
        "deleted_at": None,
        "removal_reason": REASON,
    }
    reader = await _actor(make_actor, db, ws, Role.readonly, step_up=False)
    body = (await reader.client.get(f"/v1/workspaces/{ws.id}/documents/{gone.id}")).json()
    assert "removal_reason" not in body
    assert (body["state"], body["sha256"]) == ("removed", gone.sha256)
