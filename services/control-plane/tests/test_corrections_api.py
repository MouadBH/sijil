"""Correction notes on sealed documents (design §5.4): audited, shown beside the original,
the sealed values untouched."""

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import timedelta
from typing import Any

import pytest
from fastapi import FastAPI
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core import index
from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    Membership,
    Role,
    Workspace,
)
from sijil_core.fields import fields_digest
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import INVOICE, add_document, add_index_credential, engines_on

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db

REASON = "The supplier reissued the total after a credit note."


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
    actor = await make_actor(f"{role.value}-{uuid.uuid4().hex[:6]}@example.com")
    db.add(Membership(user_id=actor.user_id, workspace_id=ws.id, role=role))
    await db.commit()
    return actor


@pytest.fixture
async def member(make_actor: MakeActor, db: AsyncSession, ws: Workspace) -> Actor:
    return await _member(make_actor, db, ws, Role.member)


@pytest.fixture
async def reader(make_actor: MakeActor, db: AsyncSession, ws: Workspace) -> Actor:
    return await _member(make_actor, db, ws, Role.readonly)


def _url(ws: Workspace, doc: DocumentLedger, suffix: str) -> str:
    return f"/v1/workspaces/{ws.id}/documents/{doc.id}/{suffix}"


def _code(r: Any) -> str:
    return str(r.json()["error"]["code"])


async def _sealed(
    member: Actor, ws: Workspace, db: AsyncSession, store: MemoryObjectStore, conn: AsyncConnection
) -> DocumentLedger:
    row = await add_document(db, store, conn, ws)
    r = await member.client.post(f"/v1/workspaces/{ws.id}/documents/{row.id}/seal")
    assert r.status_code == 200, r.text
    return row


async def _ledger(db: AsyncSession, row: DocumentLedger) -> DocumentLedger:
    found = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == row.id)
        .execution_options(populate_existing=True)
    )
    assert found is not None
    return found


async def _events(db: AsyncSession, ws: Workspace, action: str) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id, AuditEvent.action == action)
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def test_correction_keeps_original_value(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await _sealed(member, ws, db, memory_store, index_conn)
    before = await _ledger(db, row)
    snapshot = (before.state, before.fields_sha256, before.sealed_at, before.retention_until)
    r = await member.client.post(
        _url(ws, row, "corrections"),
        json={"field": "total_ttc", "value": "1200,00", "reason": REASON},
    )
    assert r.status_code == 201, r.text
    body = (await member.client.get(_url(ws, row, "fields"))).json()
    assert body["fields"]["total_ttc"]["value"] == INVOICE["total_ttc"]
    assert body["fields_sha256"] == snapshot[1] == fields_digest(INVOICE)
    [note] = body["corrections"]
    assert (note["field"], note["value"], note["reason"]) == ("total_ttc", "1200.00", REASON)
    assert note["created_by"] == str(member.user_id)
    frozen = await index.read_fields(index_conn, row.id)
    assert frozen is not None
    assert dict(frozen.values) == INVOICE
    assert fields_digest(frozen.values) == snapshot[1]
    after = await _ledger(db, row)
    assert (after.state, after.fields_sha256, after.sealed_at, after.retention_until) == snapshot


async def test_correction_audited(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await _sealed(member, ws, db, memory_store, index_conn)
    r = await member.client.post(
        _url(ws, row, "corrections"),
        json={"field": "total_ttc", "value": "1200.00", "reason": REASON},
    )
    assert r.status_code == 201, r.text
    [event] = await _events(db, ws, "document.corrected")
    assert event.target_id == str(row.id)
    assert event.data == {"field": "total_ttc"}


async def test_correction_on_unsealed_is_409(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    body = {"field": "total_ttc", "value": "1200.00", "reason": REASON}
    r = await member.client.post(_url(ws, row, "corrections"), json=body)
    assert (r.status_code, _code(r)) == (409, "correction.not_sealed")
    assert await _events(db, ws, "document.corrected") == []
    gone = await add_document(db, memory_store, index_conn, ws)
    await db.execute(
        text(
            "UPDATE document_ledger SET state = 'removed', removed_at = now(), "
            "removed_by = :u, removal_reason = 'a duplicate upload' WHERE id = :id"
        ),
        {"u": member.user_id, "id": gone.id},
    )
    await db.commit()
    r = await member.client.post(_url(ws, gone, "corrections"), json=body)
    assert (r.status_code, _code(r)) == (409, "document.not_reviewable")


async def test_correction_value_validated(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await _sealed(member, ws, db, memory_store, index_conn)

    async def post(**body: Any) -> Any:
        return await member.client.post(_url(ws, row, "corrections"), json=body)

    r = await post(field="nope", value="x", reason=REASON)
    assert (r.status_code, _code(r)) == (422, "field.unknown")
    r = await post(field="total_ttc", value="abc", reason=REASON)
    assert (r.status_code, _code(r)) == (422, "field.invalid")
    for reason in ("", "   short  ", "x" * 1001):
        r = await post(field="total_ttc", value="1.00", reason=reason)
        assert (r.status_code, _code(r)) == (422, "correction.reason_required"), reason
    r = await post(field="total_ttc", value="1.00")
    assert (r.status_code, _code(r)) == (422, "correction.reason_required")
    assert await index.read_corrections(index_conn, row.id) == []
    r = await post(field="supplier_name", value=None, reason=REASON)
    assert r.status_code == 201, r.text
    assert len(await index.read_corrections(index_conn, row.id)) == 1


async def test_correction_needs_review_permission(
    member: Actor, reader: Actor, ws: Workspace, db: AsyncSession,
    memory_store: MemoryObjectStore, index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await _sealed(member, ws, db, memory_store, index_conn)
    r = await reader.client.post(
        _url(ws, row, "corrections"),
        json={"field": "total_ttc", "value": "1.00", "reason": REASON},
    )
    assert r.status_code == 403


async def test_latest_correction_wins(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await _sealed(member, ws, db, memory_store, index_conn)
    for value in ("10.00", "20.00"):
        member.clock.advance(timedelta(seconds=1))
        r = await member.client.post(
            _url(ws, row, "corrections"),
            json={"field": "total_ttc", "value": value, "reason": REASON},
        )
        assert r.status_code == 201, r.text
    notes = (await member.client.get(_url(ws, row, "fields"))).json()["corrections"]
    assert [n["value"] for n in notes] == ["20.00", "10.00"]
    frozen = await index.read_fields(index_conn, row.id)
    assert frozen is not None
    effective = index.effective_fields(frozen, await index.read_corrections(index_conn, row.id))
    assert effective["total_ttc"] == ("20.00", True)
    assert effective["number"] == (INVOICE["number"], False)


async def test_correction_sends_the_embedding_after_the_index_write(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any, redis: Any,
) -> None:  # fmt: skip
    """The corrected value is in `search_doc` before the message leaves. While the seal's own
    embedding is still queued (its key held), it will read the correction: none is added."""
    row = await _sealed(member, ws, db, memory_store, index_conn)
    assert enqueuer.embeds == [(row.id, ws.id)]
    body = {"field": "total_ttc", "value": "1200.00", "reason": REASON}
    r = await member.client.post(_url(ws, row, "corrections"), json=body)
    assert r.status_code == 201, r.text
    assert enqueuer.embeds == [(row.id, ws.id)]

    seen: list[Any] = []

    def send(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        seen.append((ledger_id, workspace_id))

    enqueuer.send_embed = send
    await redis.delete(f"embed:pending:{row.id}")  # the seal's embedding has started
    r = await member.client.post(
        _url(ws, row, "corrections"), json=body | {"field": "vat", "value": "200.00"}
    )
    assert r.status_code == 201, r.text
    assert seen == [(row.id, ws.id)]
    found = (
        await index_conn.execute(
            text("SELECT total_ttc, vat FROM search_doc WHERE ledger_id = :id"), {"id": row.id}
        )
    ).one()
    assert tuple(str(v) for v in found) == ("1200.00", "200.00")


async def test_an_index_failure_after_the_audit_is_503(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch, enqueuer: Any, redis: Any,
) -> None:  # fmt: skip
    row = await _sealed(member, ws, db, memory_store, index_conn)
    await redis.delete(f"embed:pending:{row.id}")
    sent_before = list(enqueuer.embeds)

    async def boom(*_a: Any, **_k: Any) -> None:
        raise OSError("index down")

    monkeypatch.setattr(index, "add_correction", boom)
    r = await member.client.post(
        _url(ws, row, "corrections"),
        json={"field": "total_ttc", "value": "1.00", "reason": REASON},
    )
    assert (r.status_code, _code(r)) == (503, "field.update_failed")
    assert len(await _events(db, ws, "document.corrected")) == 1
    assert enqueuer.embeds == sent_before  # nothing changed in the index: nothing to embed


async def test_fields_of_a_removed_document_carry_no_corrections(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    gone = await add_document(db, memory_store, index_conn, ws)
    await db.execute(
        text(
            "UPDATE document_ledger SET state = 'removed', removed_at = now(), "
            "removed_by = :u, removal_reason = 'a duplicate upload' WHERE id = :id"
        ),
        {"u": member.user_id, "id": gone.id},
    )
    await db.commit()
    r = await member.client.get(_url(ws, gone, "fields"))
    assert r.status_code == 200
    assert "corrections" not in r.json()
