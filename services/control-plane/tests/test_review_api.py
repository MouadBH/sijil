"""Review queue, field edits and sealing (REC-4, design §5.4).

The index is a throw-away database reached through a real `IndexEngines`; the object store is
the in-memory double with the bucket's lock semantics.
"""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core import index
from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    DocumentState,
    Membership,
    Role,
    Workspace,
)
from sijil_core.fields import FIELD_NAMES, REQUIRED_BY_TYPE, fields_digest
from sijil_cp.api import review
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import (
    ARRIVAL,
    INVOICE,
    PROVISIONAL,
    add_document,
    add_index_credential,
    engines_on,
    stored,
)

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db


@pytest.fixture(scope="module")
def index_db_url() -> Iterator[str]:
    # Shared by the module: rows cannot be deleted, so every test uses fresh ledger ids.
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


def _url(ws: Workspace, suffix: str) -> str:
    return f"/v1/workspaces/{ws.id}{suffix}"


async def _events(db: AsyncSession, ws: Workspace, action: str | None = None) -> list[AuditEvent]:
    query = select(AuditEvent).where(AuditEvent.workspace_id == ws.id)
    if action is not None:
        query = query.where(AuditEvent.action == action)
    rows = await db.scalars(
        query.order_by(AuditEvent.seq).execution_options(populate_existing=True)
    )
    return list(rows)


async def _ledger(db: AsyncSession, row: DocumentLedger) -> DocumentLedger:
    found = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == row.id)
        .execution_options(populate_existing=True)
    )
    assert found is not None
    return found


def _code(r: Any) -> str:
    return str(r.json()["error"]["code"])


# Review queue --------------------------------------------------------------------------------


async def test_review_lists_needs_review_only(
    member: Actor,
    ws: Workspace,
    db: AsyncSession,
    memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:
    later = datetime(2026, 10, 3, tzinfo=UTC)
    second = await add_document(
        db, memory_store, index_conn, ws, state=DocumentState.needs_review, ingested_at=later,
        values={**INVOICE, "total_ttc": None}, flags=("low_confidence",),
    )  # fmt: skip
    first = await add_document(
        db, memory_store, index_conn, ws, state=DocumentState.needs_review, ingested_at=ARRIVAL
    )
    await add_document(db, memory_store, index_conn, ws, state=DocumentState.filed)
    await add_document(db, memory_store, index_conn, ws, state=DocumentState.received)

    r = await member.client.get(_url(ws, "/review"), params={"limit": 1})
    assert r.status_code == 200, r.text
    page = r.json()
    assert [i["id"] for i in page["items"]] == [str(first.id)]  # oldest first
    assert page["next_cursor"]
    r = await member.client.get(_url(ws, "/review"), params={"cursor": page["next_cursor"]})
    rest = r.json()
    assert [i["id"] for i in rest["items"]] == [str(second.id)]
    assert rest["next_cursor"] is None

    item = rest["items"][0]
    assert set(item) == {"id", "state", "document_type", "fields", "flags", "required"}
    assert item["state"] == "needs_review"
    assert item["document_type"] == "invoice"
    assert item["flags"] == ["low_confidence"]
    assert item["required"] == sorted(REQUIRED_BY_TYPE["invoice"])
    assert set(item["fields"]) == set(FIELD_NAMES)
    assert item["fields"]["total_ht"] == {
        "value": "1250.00",
        "confidence": 0.95,
        "source": "extraction",
    }
    assert item["fields"]["total_ttc"]["value"] is None

    listed = await _events(db, ws, "review.listed")
    assert [e.data for e in listed] == [{"count": 1}, {"count": 1}]
    assert listed[0].actor_id == str(member.user_id)


async def test_review_and_edits_need_the_review_permission(
    reader: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws, state=DocumentState.needs_review)
    assert (await reader.client.get(_url(ws, "/review"))).status_code == 403
    r = await reader.client.patch(
        _url(ws, f"/documents/{row.id}/fields"), json={"fields": {"number": "X"}}
    )
    assert r.status_code == 403
    # Reading the fields is a plain read.
    r = await reader.client.get(_url(ws, f"/documents/{row.id}/fields"))
    assert r.status_code == 200


async def test_get_fields_has_seal_columns_and_is_audited(
    reader: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    r = await reader.client.get(_url(ws, f"/documents/{row.id}/fields"))
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["sealed_at"], body["fields_sha256"]) == (None, None)
    assert body["fields"]["number"]["value"] == INVOICE["number"]
    [viewed] = await _events(db, ws, "document.viewed")
    assert (viewed.target_type, viewed.target_id) == ("document", str(row.id))
    assert viewed.data == {}
    r = await reader.client.get(_url(ws, f"/documents/{uuid.uuid4()}/fields"))
    assert (r.status_code, _code(r)) == (404, "document.not_found")


# Field edits ---------------------------------------------------------------------------------


async def test_patch_audit_names_only(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws, state=DocumentState.needs_review)
    r = await member.client.patch(
        _url(ws, f"/documents/{row.id}/fields"),
        json={"fields": {"total_ttc": "1 600,00 DH", "number": "F-SAMPLE-9", "vat_rate": None}},
    )
    assert r.status_code == 200, r.text
    fields = r.json()["fields"]
    assert fields["total_ttc"] == {"value": "1600.00", "confidence": 1.0, "source": "user"}
    assert fields["number"]["value"] == "F-SAMPLE-9"
    assert fields["vat_rate"] == {"value": None, "confidence": 1.0, "source": "user"}
    assert fields["total_ht"]["source"] == "extraction"
    stored_row = await index.read_fields(index_conn, row.id)
    assert stored_row is not None
    assert stored_row.values["total_ttc"] == "1600.00"
    assert stored_row.updated_by == member.user_id

    [edited] = await _events(db, ws, "document.fields_edited")
    assert edited.data == {"fields": "number,total_ttc,vat_rate"}
    assert (edited.target_type, edited.target_id) == ("document", str(row.id))
    dumped = json.dumps([e.data for e in await _events(db, ws)])
    for value in ("1600", "1 600", "F-SAMPLE-9"):
        assert value not in dumped


async def test_patch_invalid_amount_422_field_invalid(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws, state=DocumentState.needs_review)
    url = _url(ws, f"/documents/{row.id}/fields")
    r = await member.client.patch(url, json={"fields": {"number": "F-2", "total_ttc": "abc"}})
    assert r.status_code == 422
    assert r.json() == {"error": {"code": "field.invalid"}, "field": "total_ttc"}
    r = await member.client.patch(url, json={"fields": {"colour": "red"}})
    assert r.status_code == 422
    assert r.json() == {"error": {"code": "field.unknown"}, "field": "colour"}
    r = await member.client.patch(url, json={"fields": {"document_type": "poem"}})
    assert r.json() == {"error": {"code": "field.invalid"}, "field": "document_type"}
    # Nothing was written, nothing audited.
    stored_row = await index.read_fields(index_conn, row.id)
    assert stored_row is not None
    assert stored_row.values["number"] == INVOICE["number"]
    assert await _events(db, ws, "document.fields_edited") == []


async def test_patch_document_type_changes_the_required_set(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws, state=DocumentState.needs_review)
    r = await member.client.patch(
        _url(ws, f"/documents/{row.id}/fields"), json={"fields": {"document_type": "contract"}}
    )
    assert r.status_code == 200, r.text
    assert r.json()["document_type"] == "contract"
    assert r.json()["required"] == sorted(REQUIRED_BY_TYPE["contract"])


async def test_patch_received_409_not_reviewable(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws, state=DocumentState.received)
    r = await member.client.patch(
        _url(ws, f"/documents/{row.id}/fields"), json={"fields": {"number": "F-2"}}
    )
    assert (r.status_code, _code(r)) == (409, "document.not_reviewable")


async def test_patch_after_seal_409_retention_locked(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    assert (await member.client.post(_url(ws, f"/documents/{row.id}/seal"))).status_code == 200
    r = await member.client.patch(
        _url(ws, f"/documents/{row.id}/fields"), json={"fields": {"total_ttc": "1.00"}}
    )
    assert (r.status_code, _code(r)) == (409, "retention.locked")
    stored_row = await index.read_fields(index_conn, row.id)
    assert stored_row is not None
    assert stored_row.values["total_ttc"] == INVOICE["total_ttc"]
    assert await _events(db, ws, "document.fields_edited") == []


async def test_patch_refused_by_the_index_trigger_is_409(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    """The index row is frozen but the ledger not yet (a seal that failed half-way): the
    trigger's refusal is answered as `retention.locked` too."""
    row = await add_document(db, memory_store, index_conn, ws)
    await index.seal_fields(
        index_conn, row.id, fields_sha256=fields_digest(INVOICE), sealed_at=ARRIVAL
    )
    r = await member.client.patch(
        _url(ws, f"/documents/{row.id}/fields"), json={"fields": {"number": "F-2"}}
    )
    assert (r.status_code, _code(r)) == (409, "retention.locked")


# Sealing -------------------------------------------------------------------------------------


async def test_seal_freezes_fields(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 200, r.text
    body = r.json()
    digest = fields_digest(INVOICE)
    assert body["state"] == "sealed"
    assert body["fields_sha256"] == digest
    assert body["sealed_at"] is not None

    frozen = await index.read_fields(index_conn, row.id)
    assert frozen is not None
    assert frozen.fields_sha256 == digest
    ledger = await _ledger(db, row)
    assert ledger.state is DocumentState.sealed
    assert ledger.fields_sha256 == digest
    assert ledger.sealed_at == frozen.sealed_at
    assert ledger.sealed_by == member.user_id
    assert ledger.retention_until == PROVISIONAL
    assert ledger.object_locked_at is None
    # The control plane holds no key that can switch a lock: the seal worker does it after
    # the commit, so the original is still GOVERNANCE at its provisional date here.
    assert stored(memory_store, row).mode == "GOVERNANCE"

    [sealed] = await _events(db, ws, "document.sealed")
    assert sealed.data == {"fields_sha256": digest, "retention_until": "2036-12-31"}
    assert (sealed.target_id, sealed.actor_id) == (str(row.id), str(member.user_id))
    assert enqueuer.locks == [(row.id, ws.id)]
    assert enqueuer.mirrors == [(row.id, ws.id)]

    # No re-seal.
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert (r.status_code, _code(r)) == (409, "retention.locked")
    assert len(await _events(db, ws, "document.sealed")) == 1


async def test_seal_needs_review_document(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws, state=DocumentState.needs_review)
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 200, r.text
    assert (await _ledger(db, row)).state is DocumentState.sealed


async def test_seal_received_409_not_reviewable(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws, state=DocumentState.received)
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert (r.status_code, _code(r)) == (409, "document.not_reviewable")
    assert stored(memory_store, row).mode == "GOVERNANCE"


async def test_readonly_cannot_seal(
    reader: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    r = await reader.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert (r.status_code, _code(r)) == (403, "forbidden")
    r = await reader.client.post(_url(ws, "/seal-batch"), json={"month": "2026-10"})
    assert r.status_code == 403
    assert (await _ledger(db, row)).state is DocumentState.filed
    assert stored(memory_store, row).mode == "GOVERNANCE"


async def test_member_can_seal(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 200, r.text
    assert (await _ledger(db, row)).sealed_by == member.user_id


async def test_seal_requires_required_fields(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(
        db, memory_store, index_conn, ws,
        values={**INVOICE, "supplier_ice": None, "total_ttc": None, "vat_rate": None},
    )  # fmt: skip
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 422
    # vat_rate is optional: it never blocks.
    assert r.json() == {
        "error": {"code": "seal.fields_incomplete"},
        "fields": ["supplier_ice", "total_ttc"],
    }
    untyped = await add_document(
        db, memory_store, index_conn, ws, values={"date": "2026-09-30"}, document_type=None
    )
    r = await member.client.post(_url(ws, f"/documents/{untyped.id}/seal"))
    assert r.status_code == 422
    assert r.json()["fields"] == sorted(REQUIRED_BY_TYPE["invoice"] - {"date"})
    # Nothing moved anywhere.
    for doc in (row, untyped):
        assert (await _ledger(db, doc)).state is DocumentState.filed
        assert stored(memory_store, doc).mode == "GOVERNANCE"
        fields = await index.read_fields(index_conn, doc.id)
        assert fields is not None and fields.sealed_at is None
    assert await _events(db, ws, "document.sealed") == []


async def test_seal_other_type_needs_only_type_and_date(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    row = await add_document(
        db, memory_store, index_conn, ws,
        values={"document_type": "contract", "date": "2026-09-30"}, document_type="contract",
    )  # fmt: skip
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 200, r.text
    assert r.json()["document_type"] == "contract"


@pytest.mark.parametrize(
    ("provisional", "doc_date", "final"),
    [
        # The document's fiscal year raises it.
        (date(2035, 12, 31), "2026-09-30", date(2036, 12, 31)),
        # An earlier fiscal year never lowers it (ruling P1).
        (PROVISIONAL, "2024-03-15", PROVISIONAL),
        # A longer existing retention stays.
        (date(2040, 12, 31), "2026-09-30", date(2040, 12, 31)),
    ],
)
async def test_seal_retention_is_max_of_provisional_and_doc_date(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, provisional: date, doc_date: str, final: date,
) -> None:  # fmt: skip
    row = await add_document(
        db, memory_store, index_conn, ws, retention_until=provisional,
        values={**INVOICE, "date": doc_date},
    )  # fmt: skip
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 200, r.text
    assert (await _ledger(db, row)).retention_until == final
    [sealed] = await _events(db, ws, "document.sealed")
    assert sealed.data["retention_until"] == final.isoformat()


async def test_seal_future_date_never_adds_years_of_lock(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, clock: Any,
) -> None:  # fmt: skip
    """A COMPLIANCE lock can never be shortened: an implausible year counts as the arrival
    date, the same basis as routing (`sijil_core.retention.retention_basis`)."""
    row = await add_document(
        db, memory_store, index_conn, ws, values={**INVOICE, "date": "2099-01-01"}
    )
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 200, r.text
    assert (await _ledger(db, row)).retention_until == PROVISIONAL  # arrival 2026 + 10


async def test_seal_retry_after_partial_failure(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    """Review Focus 4: the index is frozen, then the ledger commit fails. Calling seal again
    completes it exactly once; the lock is only ever sent after a commit."""
    row = await add_document(db, memory_store, index_conn, ws, retention_until=date(2035, 12, 31))
    real_commit = AsyncSession.commit
    failed: list[bool] = []

    async def commit_failing_once(self: AsyncSession) -> None:
        # The ledger row is flushed by then (the audit append autoflushes): look at what the
        # session holds, not at what is still dirty.
        sealing = any(
            isinstance(o, DocumentLedger) and o.state is DocumentState.sealed
            for o in self.identity_map.values()
        )
        if sealing and not failed:
            failed.append(True)
            raise ConnectionResetError("injected: the ledger commit fails")
        await real_commit(self)

    monkeypatch.setattr(AsyncSession, "commit", commit_failing_once)
    with pytest.raises(ConnectionResetError):
        await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert failed == [True]
    # Half-way: the index frozen, the ledger not sealed, the original untouched.
    assert stored(memory_store, row).mode == "GOVERNANCE"
    frozen = await index.read_fields(index_conn, row.id)
    assert frozen is not None and frozen.sealed_at is not None
    assert (await _ledger(db, row)).state is DocumentState.filed
    assert await _events(db, ws, "document.sealed") == []
    assert (enqueuer.locks, enqueuer.mirrors) == ([], [])

    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 200, r.text
    ledger = await _ledger(db, row)
    assert ledger.state is DocumentState.sealed
    assert ledger.sealed_at == frozen.sealed_at  # the index's stored value is reused
    assert ledger.fields_sha256 == frozen.fields_sha256
    assert ledger.retention_until == date(2036, 12, 31)
    assert len(await _events(db, ws, "document.sealed")) == 1
    assert enqueuer.locks == [(row.id, ws.id)]
    assert enqueuer.mirrors == [(row.id, ws.id)]


async def test_seal_enqueues_mirror_after_commit(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any, migrated_db: Any,
) -> None:  # fmt: skip
    """When the mirror task is sent, the sealed ledger row is already committed (a separate
    connection sees it)."""
    row = await add_document(db, memory_store, index_conn, ws)
    loop = asyncio.get_running_loop()
    observer = create_async_engine(migrated_db.app_url)
    seen: list[str] = []

    async def state_now() -> str:
        async with observer.connect() as conn:
            found = await conn.scalar(
                select(DocumentLedger.state).where(DocumentLedger.id == row.id)
            )
            return str(found)

    def observing(sent: list[tuple[uuid.UUID, uuid.UUID]]) -> Any:
        def send(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
            # Runs in a thread (asyncio.to_thread): ask the event loop to look at the database.
            seen.append(asyncio.run_coroutine_threadsafe(state_now(), loop).result(timeout=10))
            sent.append((ledger_id, workspace_id))

        return send

    enqueuer.send_lock = observing(enqueuer.locks)
    enqueuer.send_mirror = observing(enqueuer.mirrors)
    try:
        r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    finally:
        await observer.dispose()
    assert r.status_code == 200, r.text
    assert seen == ["sealed", "sealed"]  # the lock, then the mirror: both after the commit
    assert enqueuer.locks == enqueuer.mirrors == [(row.id, ws.id)]


async def test_seal_survives_a_failed_mirror_enqueue(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    """The seal is committed: a broker that is down only delays Paperless's copy."""
    row = await add_document(db, memory_store, index_conn, ws)

    def broken(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        raise ConnectionRefusedError("broker down")

    enqueuer.send_mirror = broken
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 200, r.text
    assert (await _ledger(db, row)).state is DocumentState.sealed


# Batch seal ----------------------------------------------------------------------------------


async def test_batch_seal_one_audit_event_per_document(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any,
) -> None:  # fmt: skip
    # The month is Casablanca's civil month, whatever its UTC offset then (tzdata decides).
    local = ZoneInfo("Africa/Casablanca")
    october = [
        datetime(2026, 10, 1, 0, 30, tzinfo=local).astimezone(UTC),
        datetime(2026, 10, 10, 9, 0, tzinfo=local).astimezone(UTC),
        datetime(2026, 10, 31, 23, 30, tzinfo=local).astimezone(UTC),
    ]
    filed = [await add_document(db, memory_store, index_conn, ws, ingested_at=at) for at in october]
    incomplete = await add_document(
        db, memory_store, index_conn, ws, ingested_at=datetime(2026, 10, 12, tzinfo=UTC),
        values={**INVOICE, "number": None},
    )  # fmt: skip
    review_row = await add_document(
        db, memory_store, index_conn, ws, state=DocumentState.needs_review,
        ingested_at=datetime(2026, 10, 11, tzinfo=UTC),
    )  # fmt: skip
    september = await add_document(
        db, memory_store, index_conn, ws,
        ingested_at=datetime(2026, 9, 30, 23, 30, tzinfo=local).astimezone(UTC),
    )  # fmt: skip
    await add_document(
        db, memory_store, index_conn, ws,
        ingested_at=datetime(2026, 11, 1, 0, 30, tzinfo=local).astimezone(UTC),
    )  # fmt: skip

    r = await member.client.post(_url(ws, "/seal-batch"), json={"month": "2026-10"})
    assert r.status_code == 200, r.text
    assert r.json() == {
        "sealed": 3,
        "skipped": [{"id": str(incomplete.id), "code": "seal.fields_incomplete"}],
        "remaining": 0,
        "next_cursor": None,
    }
    sealed_events = await _events(db, ws, "document.sealed")
    assert sorted(e.target_id or "" for e in sealed_events) == sorted(str(d.id) for d in filed)
    assert [e.target_id for e in sealed_events] == [str(d.id) for d in filed]  # oldest first
    for doc in filed:
        assert (await _ledger(db, doc)).state is DocumentState.sealed
    for doc in (incomplete, review_row, september):
        assert (await _ledger(db, doc)).state is not DocumentState.sealed
    assert sorted(enqueuer.locks) == sorted((d.id, ws.id) for d in filed)
    assert sorted(enqueuer.mirrors) == sorted((d.id, ws.id) for d in filed)

    # Again: nothing left to seal, nothing audited twice.
    r = await member.client.post(_url(ws, "/seal-batch"), json={"month": "2026-10"})
    assert r.json()["sealed"] == 0
    assert len(await _events(db, ws, "document.sealed")) == 3


async def test_batch_seal_caps_each_call_and_moves_past_skipped_rows(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    """The cursor carries the client past a row that keeps failing: the oldest document can
    never wedge every later call."""
    monkeypatch.setattr(review, "SEAL_BATCH_LIMIT", 2)
    stuck = await add_document(
        db, memory_store, index_conn, ws, ingested_at=datetime(2026, 10, 3, tzinfo=UTC),
        values={**INVOICE, "total_ttc": None},
    )  # fmt: skip
    rest = [
        await add_document(
            db, memory_store, index_conn, ws, ingested_at=datetime(2026, 10, day, tzinfo=UTC)
        )
        for day in (4, 5)
    ]
    url = _url(ws, "/seal-batch")
    first = (await member.client.post(url, json={"month": "2026-10"})).json()
    assert first["sealed"] == 1
    assert first["skipped"] == [{"id": str(stuck.id), "code": "seal.fields_incomplete"}]
    assert first["remaining"] == 1
    assert first["next_cursor"]
    second = (
        await member.client.post(url, json={"month": "2026-10", "cursor": first["next_cursor"]})
    ).json()
    assert second == {"sealed": 1, "skipped": [], "remaining": 0, "next_cursor": None}
    for doc in rest:
        assert (await _ledger(db, doc)).state is DocumentState.sealed
    # Without the cursor the client starts over: the stuck row is skipped again, nothing else.
    again = (await member.client.post(url, json={"month": "2026-10"})).json()
    assert (again["sealed"], again["remaining"]) == (0, 0)


async def test_batch_seal_reports_a_failing_document_and_seals_the_others(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    docs = [
        await add_document(
            db, memory_store, index_conn, ws, ingested_at=datetime(2026, 10, day, tzinfo=UTC)
        )
        for day in (6, 7, 8)
    ]
    broken = docs[1].id
    real = index.seal_fields

    async def flaky(conn: Any, ledger_id: uuid.UUID, **kw: Any) -> Any:
        if ledger_id == broken:
            raise ConnectionResetError("index connection lost")
        return await real(conn, ledger_id, **kw)

    monkeypatch.setattr(index, "seal_fields", flaky)
    r = await member.client.post(_url(ws, "/seal-batch"), json={"month": "2026-10"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["sealed"] == 2
    assert body["skipped"] == [{"id": str(broken), "code": "seal.index_unavailable"}]
    states = [(await _ledger(db, d)).state for d in docs]
    assert states == [DocumentState.sealed, DocumentState.filed, DocumentState.sealed]


async def test_batch_rejects_a_bad_cursor(member: Actor, ws: Workspace) -> None:
    r = await member.client.post(
        _url(ws, "/seal-batch"), json={"month": "2026-10", "cursor": "not-a-cursor"}
    )
    assert (r.status_code, _code(r)) == (422, "validation")


# The hash and the values it freezes ----------------------------------------------------------


async def test_seal_of_values_that_changed_after_they_were_read_is_409(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch, enqueuer: Any,
) -> None:  # fmt: skip
    """The seal read the values, an edit changed them before the freeze: the index recomputes
    the digest from the row it locks, so the stale hash is refused, never stamped on the new
    values."""
    row = await add_document(db, memory_store, index_conn, ws)
    real_read = index.read_fields

    async def read_then_edit(conn: Any, ledger_id: uuid.UUID) -> Any:
        stale = await real_read(conn, ledger_id)
        await index.update_fields(
            index_conn, ledger_id, {"total_ttc": "1600.00"}, source="user", updated_by=None,
            now=ARRIVAL,
        )  # fmt: skip
        return stale

    monkeypatch.setattr(index, "read_fields", read_then_edit)
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert (r.status_code, _code(r)) == (409, "seal.fields_changed")
    current = await index.read_fields(index_conn, row.id)
    assert current is not None and current.sealed_at is None
    assert (await _ledger(db, row)).state is DocumentState.filed
    assert await _events(db, ws, "document.sealed") == []
    assert enqueuer.locks == []
    # Sealing again hashes what is there now.
    monkeypatch.setattr(index, "read_fields", real_read)
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 200, r.text
    frozen = await index.read_fields(index_conn, row.id)
    assert frozen is not None
    assert frozen.values["total_ttc"] == "1600.00"
    assert r.json()["fields_sha256"] == frozen.fields_sha256 == fields_digest(frozen.values)


async def test_patch_audits_the_edit_before_it_applies_it(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    """Controller ruling: the `document.fields_edited` event commits first, the index edit
    after it, so an applied edit always has its audit event."""
    row = await add_document(db, memory_store, index_conn, ws, state=DocumentState.needs_review)
    real_commit = AsyncSession.commit
    seen: list[str | None] = []

    async def observing_commit(self: AsyncSession) -> None:
        # The edit's audit row is flushed in this (not yet committed) transaction.
        editing = await self.scalar(
            select(AuditEvent.id).where(
                AuditEvent.workspace_id == ws.id, AuditEvent.action == "document.fields_edited"
            )
        )
        if editing is not None and not seen:
            current = await index.read_fields(index_conn, row.id)  # another connection
            seen.append(current.values["total_ttc"] if current else None)
        await real_commit(self)

    monkeypatch.setattr(AsyncSession, "commit", observing_commit)
    r = await member.client.patch(
        _url(ws, f"/documents/{row.id}/fields"), json={"fields": {"total_ttc": "1600.00"}}
    )
    assert r.status_code == 200, r.text
    assert seen == [INVOICE["total_ttc"]]  # not applied yet when the audit committed
    applied = await index.read_fields(index_conn, row.id)
    assert applied is not None and applied.values["total_ttc"] == "1600.00"


async def test_index_failure_after_the_audit_is_field_update_failed(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    """A logged edit attempt that did not apply is acceptable; the values stay as they were."""
    row = await add_document(db, memory_store, index_conn, ws, state=DocumentState.needs_review)

    async def failing(trans: Any) -> None:
        await trans.rollback()
        raise ConnectionResetError("index connection lost at commit")

    monkeypatch.setattr(review, "_commit_index", failing)
    r = await member.client.patch(
        _url(ws, f"/documents/{row.id}/fields"), json={"fields": {"total_ttc": "1600.00"}}
    )
    assert (r.status_code, _code(r)) == (503, "field.update_failed")
    [edited] = await _events(db, ws, "document.fields_edited")
    assert edited.data == {"fields": "total_ttc"}
    unchanged = await index.read_fields(index_conn, row.id)
    assert unchanged is not None and unchanged.values["total_ttc"] == INVOICE["total_ttc"]


async def test_a_seal_racing_a_patch_never_stores_a_mismatched_digest(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    """The PATCH has committed its audit (the ledger lock is free) but not yet its index edit
    when a seal starts: the seal either seals the values it locked, with their digest, or
    answers 409 `seal.fields_changed`. Never a digest of other values."""
    row = await add_document(db, memory_store, index_conn, ws)
    real_commit_index = review._commit_index
    outcome: list[Any] = []

    async def pause_then_commit(trans: Any) -> None:
        # The seal runs now, between the PATCH's audit commit and its index commit. It blocks
        # on the index row the PATCH holds; let it get there, then commit the edit.
        seal = asyncio.create_task(member.client.post(_url(ws, f"/documents/{row.id}/seal")))
        await asyncio.sleep(0.5)
        await real_commit_index(trans)
        outcome.append(await seal)

    monkeypatch.setattr(review, "_commit_index", pause_then_commit)
    r = await member.client.patch(
        _url(ws, f"/documents/{row.id}/fields"), json={"fields": {"total_ttc": "1600.00"}}
    )
    assert r.status_code == 200, r.text
    [sealed] = outcome
    frozen = await index.read_fields(index_conn, row.id)
    assert frozen is not None
    if sealed.status_code == 200:
        digest = fields_digest(frozen.values)
        assert frozen.fields_sha256 == digest == sealed.json()["fields_sha256"]
        assert (await _ledger(db, row)).fields_sha256 == fields_digest(frozen.values)
    else:
        assert (sealed.status_code, _code(sealed)) == (409, "seal.fields_changed")
        assert frozen.sealed_at is None
        assert frozen.values["total_ttc"] == "1600.00"


@pytest.mark.parametrize("month", ["2026-13", "2026-1", "october", "2026-10-01"])
async def test_batch_seal_rejects_a_bad_month(member: Actor, ws: Workspace, month: str) -> None:
    r = await member.client.post(_url(ws, "/seal-batch"), json={"month": month})
    assert (r.status_code, _code(r)) == (422, "validation")


def test_mirror_is_sent_to_the_main_workers_default_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    from sijil_cp.enqueue import CeleryEnqueuer

    sent: list[tuple[str, dict[str, str], str]] = []
    celery = CeleryEnqueuer("redis://127.0.0.1:1/0")
    monkeypatch.setattr(
        celery._app,  # noqa: SLF001
        "send_task",
        lambda name, kwargs, queue: sent.append((name, kwargs, queue)),
    )
    ledger_id, workspace_id = uuid.uuid4(), uuid.uuid4()
    celery.send_mirror(ledger_id, workspace_id)
    celery.send_lock(ledger_id, workspace_id)
    kwargs = {"ledger_id": str(ledger_id), "workspace_id": str(workspace_id)}
    assert sent == [
        ("sijil.mirror_fields", kwargs, "default"),
        ("sijil.lock_sealed", kwargs, "seal"),  # the seal worker, never the control plane
    ]


async def test_the_seal_claims_the_lock_pending_key_before_sending(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any, redis: Any,
) -> None:  # fmt: skip
    """The control plane's lock message and a sweep re-send never run together: both claim
    `lock:pending:<id>` first."""
    row = await add_document(db, memory_store, index_conn, ws)
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 200, r.text
    assert await redis.exists(f"lock:pending:{row.id}")
    assert 0 < await redis.ttl(f"lock:pending:{row.id}") <= 3600
    assert enqueuer.locks == [(row.id, ws.id)]
    # Already in flight (a sweep sent it): the control plane does not send a second one.
    other = await add_document(db, memory_store, index_conn, ws)
    await redis.set(f"lock:pending:{other.id}", "1", ex=3600)
    r = await member.client.post(_url(ws, f"/documents/{other.id}/seal"))
    assert r.status_code == 200, r.text
    assert enqueuer.locks == [(row.id, ws.id)]
    assert enqueuer.mirrors == [(row.id, ws.id), (other.id, ws.id)]


async def test_a_failed_lock_send_releases_its_claim(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, enqueuer: Any, redis: Any,
) -> None:  # fmt: skip
    row = await add_document(db, memory_store, index_conn, ws)

    def broken(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        raise ConnectionRefusedError("broker down")

    enqueuer.send_lock = broken
    r = await member.client.post(_url(ws, f"/documents/{row.id}/seal"))
    assert r.status_code == 200, r.text
    assert not await redis.exists(f"lock:pending:{row.id}")  # the sweep may send it


async def test_batch_tells_a_database_failure_from_an_index_failure(
    member: Actor, ws: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    from sqlalchemy.exc import OperationalError

    docs = [
        await add_document(
            db, memory_store, index_conn, ws, ingested_at=datetime(2026, 10, day, tzinfo=UTC)
        )
        for day in (13, 14, 15)
    ]
    db_broken, index_broken = docs[0].id, docs[1].id
    real_locked, real_read = review._locked_ledger, index.read_fields

    async def locked(session: Any, workspace: Any, doc_id: uuid.UUID) -> Any:
        if doc_id == db_broken:
            raise OperationalError("SELECT", {}, ConnectionResetError("cp db lost"))
        return await real_locked(session, workspace, doc_id)

    async def read(conn: Any, ledger_id: uuid.UUID) -> Any:
        if ledger_id == index_broken:
            raise OperationalError("SELECT", {}, ConnectionResetError("index db lost"))
        return await real_read(conn, ledger_id)

    monkeypatch.setattr(review, "_locked_ledger", locked)
    monkeypatch.setattr(index, "read_fields", read)
    body = (await member.client.post(_url(ws, "/seal-batch"), json={"month": "2026-10"})).json()
    assert body["sealed"] == 1
    assert body["skipped"] == [
        {"id": str(db_broken), "code": "seal.db_unavailable"},
        {"id": str(index_broken), "code": "seal.index_unavailable"},
    ]
