"""The end of retention (plan 4.4, design §5.4): nothing is purged automatically; the Owner
approves the deletion of eligible documents in batches, with a TOTP step-up.

`GET …/retention/eligible` lists them; `POST …/retention/approve-deletion` `{ids}` moves them
all to `deleting` or none, and after the commit sends `sijil.remove_original` for each. Every
refusal to a signed-in caller is audited `document.removal_refused` `{code}`. The worker's side
is in the lock suite (tests/retention/test_deletion_paths.py).
"""

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import date, timedelta
from typing import Any

import pytest
from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core import index
from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    DocumentState,
    Membership,
    Role,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.retention import region_local_date
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import INVOICE, add_document, add_index_credential, engines_on

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db

PAST = date(2020, 12, 31)
OLDER = date(2019, 12, 31)
REASON = "Doublon déposé par erreur (SAMPLE)"


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


def _approve_url(ws: Workspace) -> str:
    return f"/v1/workspaces/{ws.id}/retention/approve-deletion"


def _eligible_url(ws: Workspace) -> str:
    return f"/v1/workspaces/{ws.id}/retention/eligible"


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


async def _refusals(db: AsyncSession, ws: Workspace) -> list[tuple[str | None, object]]:
    return [(e.target_id, e.data) for e in await _events(db, ws, "document.removal_refused")]


async def _ledger(db: AsyncSession, doc_id: uuid.UUID) -> DocumentLedger:
    found = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == doc_id)
        .execution_options(populate_existing=True)
    )
    assert found is not None
    return found


async def _expired(
    db: AsyncSession,
    store: MemoryObjectStore,
    conn: AsyncConnection,
    ws: Workspace,
    retention: date = PAST,
    **kwargs: Any,
) -> DocumentLedger:
    return await add_document(db, store, conn, ws, retention_until=retention, **kwargs)


async def _removed(db: AsyncSession, row: DocumentLedger, user_id: uuid.UUID) -> None:
    await db.execute(
        text(
            "UPDATE document_ledger SET state = 'removed', removed_at = now(),"
            " removed_by = :u, removal_reason = :reason WHERE id = :id"
        ),
        {"id": row.id, "u": user_id, "reason": REASON},
    )
    await db.commit()


async def _other_workspace(db: AsyncSession, ws: Workspace) -> Workspace:
    slug = f"other-{uuid.uuid4().hex[:8]}"
    other = Workspace(
        org_id=ws.org_id, name="Other", slug=slug, state=WorkspaceState.active,
        db_name=f"paperless_{slug}", index_db_name=f"index_{slug}", kms_key_id=f"kek-{slug}",
    )  # fmt: skip
    db.add(other)
    await db.commit()
    return other


@pytest.mark.parametrize("role", [Role.admin, Role.member, Role.readonly])
async def test_approve_deletion_owner_only_with_step_up(
    make_actor: MakeActor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any, role: Role,
) -> None:  # fmt: skip
    """Admin, Member and ReadOnly, even stepped up, get 403 `forbidden`; an Owner without a
    step-up within 10 minutes gets `auth.step_up_required`. Each refusal is audited once, with
    no target (the body is not read before the role and the step-up)."""
    row = await _expired(db, memory_store, index_conn, ws)
    body = {"ids": [str(row.id)]}
    actor = await _actor(make_actor, db, ws, role)
    r = await actor.client.post(_approve_url(ws), json=body)
    assert (r.status_code, _code(r)) == (403, "forbidden")
    [refused] = await _events(db, ws, "document.removal_refused")
    assert (refused.target_type, refused.target_id) == (None, None)
    assert (refused.actor_id, refused.data) == (str(actor.user_id), {"code": "forbidden"})

    plain = await _actor(make_actor, db, ws, Role.owner, step_up=False)
    r = await plain.client.post(_approve_url(ws), json=body)
    assert (r.status_code, _code(r)) == (403, "auth.step_up_required")
    owner = await _actor(make_actor, db, ws, Role.owner)
    owner.clock.advance(timedelta(minutes=11))
    r = await owner.client.post(_approve_url(ws), json=body)
    assert (r.status_code, _code(r)) == (403, "auth.step_up_required")
    assert await _refusals(db, ws) == [
        (None, {"code": "forbidden"}),
        (None, {"code": "auth.step_up_required"}),
        (None, {"code": "auth.step_up_required"}),
    ]
    assert (await _ledger(db, row.id)).state is DocumentState.filed
    assert await _events(db, ws, "document.deletion_approved") == []
    assert enqueuer.removals == []

    await owner.step_up()
    r = await owner.client.post(_approve_url(ws), json=body)
    assert (r.status_code, r.json()) == (202, {"approved": 1})


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"ids": []},
        {"ids": ["not-a-uuid"]},
        {"ids": [str(uuid.UUID(int=i + 1)) for i in range(501)]},
    ],
)
async def test_approve_deletion_validates_the_body(
    owner: Actor, ws: Workspace, db: AsyncSession, enqueuer: Any, body: dict[str, Any]
) -> None:
    """1 to 500 ids, each a UUID: anything else is 422 `validation`, audited."""
    r = await owner.client.post(_approve_url(ws), json=body)
    assert (r.status_code, _code(r)) == (422, "validation")
    assert await _refusals(db, ws) == [(None, {"code": "validation"})]
    assert enqueuer.removals == []


async def test_approve_deletion_all_or_nothing(
    owner: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any, redis: Redis, user: User,
) -> None:  # fmt: skip
    filed = await _expired(db, memory_store, index_conn, ws)
    needs_review = await _expired(
        db, memory_store, index_conn, ws, state=DocumentState.needs_review
    )
    # Not over by the region-local date: retention runs to the end of `retention_until`.
    today = region_local_date(owner.clock.now, "ma-cas")
    running = await _expired(db, memory_store, index_conn, ws, today)
    removed = await _expired(db, memory_store, index_conn, ws)
    await _removed(db, removed, user.id)
    elsewhere = await _expired(db, memory_store, index_conn, await _other_workspace(db, ws))
    unknown = uuid.uuid4()
    offending = sorted([running.id, removed.id, elsewhere.id, unknown])

    ids = [filed.id, running.id, needs_review.id, removed.id, elsewhere.id, unknown]
    r = await owner.client.post(_approve_url(ws), json={"ids": [str(i) for i in ids]})
    assert (r.status_code, _code(r)) == (409, "retention.not_eligible")
    assert r.json()["ids"] == [str(i) for i in offending]
    # Nothing changed: not even the eligible ones in the same batch.
    for row, state in ((filed, DocumentState.filed), (needs_review, DocumentState.needs_review)):
        fresh = await _ledger(db, row.id)
        assert (fresh.state, fresh.deletion_approved_at) == (state, None)
    assert await _refusals(db, ws) == [
        (str(i), {"code": "retention.not_eligible"}) for i in offending
    ]
    assert await _events(db, ws, "document.deletion_approved") == []
    assert await _events(db, ws, "retention.deletion_batch") == []
    assert enqueuer.removals == []

    # The eligible ones alone (a repeated id counts once).
    ids = [needs_review.id, filed.id, filed.id]
    r = await owner.client.post(_approve_url(ws), json={"ids": [str(i) for i in ids]})
    assert (r.status_code, r.json()) == (202, {"approved": 2})
    for row in (filed, needs_review):
        fresh = await _ledger(db, row.id)
        assert fresh.state is DocumentState.deleting
        assert (fresh.deletion_approved_at, fresh.deletion_approved_by) == (
            owner.clock.now,
            owner.user_id,
        )
        assert await redis.exists(f"remove:pending:{row.id}")
        assert 0 < await redis.ttl(f"remove:pending:{row.id}") <= 3600
    approved = await _events(db, ws, "document.deletion_approved")
    assert sorted((e.target_type, e.target_id) for e in approved) == sorted(
        ("document", str(i)) for i in (filed.id, needs_review.id)
    )
    assert {e.actor_id for e in approved} == {str(owner.user_id)}
    [batch] = await _events(db, ws, "retention.deletion_batch")
    assert batch.data == {"count": 2}
    assert batch.seq > max(e.seq for e in approved)
    assert sorted(enqueuer.removals) == sorted([(filed.id, ws.id), (needs_review.id, ws.id)])

    # Approved already: a second approval is refused like any other terminal row.
    r = await owner.client.post(_approve_url(ws), json={"ids": [str(filed.id)]})
    assert (r.status_code, r.json()["ids"]) == (409, [str(filed.id)])
    assert len(enqueuer.removals) == 2


async def test_eligible_lists_oldest_retention_first(
    make_actor: MakeActor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, user: User,
) -> None:  # fmt: skip
    admin = await _actor(make_actor, db, ws, Role.admin)  # `retention_manage`
    newer = await _expired(db, memory_store, index_conn, ws)
    older = await _expired(
        db, memory_store, index_conn, ws, OLDER, values={**INVOICE, "document_type": "receipt"},
        document_type="receipt", state=DocumentState.needs_review,
    )  # fmt: skip
    await _expired(db, memory_store, index_conn, ws, date(2036, 12, 31))  # running
    removed = await _expired(db, memory_store, index_conn, ws)
    await _removed(db, removed, user.id)
    await _expired(db, memory_store, index_conn, await _other_workspace(db, ws))

    r = await admin.client.get(_eligible_url(ws), params={"limit": 1})
    assert r.status_code == 200, r.text
    page = r.json()
    assert page["items"] == [
        {
            "id": str(older.id),
            "retention_until": "2019-12-31",
            "state": "needs_review",
            "document_type": "receipt",
        }
    ]
    assert page["engine"] == "ok"
    r = await admin.client.get(
        _eligible_url(ws), params={"limit": 1, "cursor": page["next_cursor"]}
    )
    page = r.json()
    assert [i["id"] for i in page["items"]] == [str(newer.id)]
    assert page["items"][0]["document_type"] == "invoice"
    assert page["next_cursor"] is None

    member = await _actor(make_actor, db, ws, Role.member)
    r = await member.client.get(_eligible_url(ws))
    assert (r.status_code, _code(r)) == (403, "forbidden")
    r = await admin.client.get(_eligible_url(ws), params={"cursor": "nope"})
    assert (r.status_code, _code(r)) == (422, "validation")
