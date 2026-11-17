"""Account closure (plan 6.4, ADM-5, design §7 item 11): the request with its 30-day grace
period and full exports, the cancel, the take-over statement, and what a hibernated workspace
still serves (its Owner's reads) and refuses (capture, writes, every other role).

The daily closure job runs in-process (`sijil_worker.closure.closure_daily`) with a recording
runtime, as the main worker would with Docker. SAMPLE data only.
"""

import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core.crypto import seal
from sijil_core.db.models import (
    AuditEvent,
    Closure,
    ClosureState,
    CredentialKind,
    ExportJob,
    ExportState,
    Membership,
    Organisation,
    OrgStatus,
    Role,
    Subscription,
    SubscriptionStatus,
    TakeoverStatement,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.settings import CoreSettings
from sijil_core.storage import ObjectRef
from sijil_core.tenancy import tenant_ref
from sijil_cp.api import documents
from sijil_worker.closure import closure_daily
from tests.support.billing_states import make_late, workspace_writes
from tests.support.exports import run_export
from tests.support.memory_store import MemoryExportStore, MemoryObjectStore, StoredObject

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db

PDF = b"%PDF-1.7\n% SAMPLE take-over statement\n%%EOF\n"
SETTINGS = CoreSettings.model_construct(region="ma-cas", environment="test")
FAR = date(2036, 12, 31)
UNUSED_INDEX = "postgresql+asyncpg://unused:unused@127.0.0.1:1/unused"


class RecordingRuntime:
    """The tenant runtime of the closure job: remembers what it was asked to do."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def stop(self, ref: str) -> None:
        self.calls.append(("stop", ref))

    async def destroy(self, ref: str) -> None:
        self.calls.append(("destroy", ref))


class NoLocks:
    """No original of these tests is COMPLIANCE-locked beyond its `retention_until`."""

    async def retention(self, _ref: ObjectRef) -> tuple[str, date] | None:
        return None


def _code(r: httpx.Response) -> str:
    return str(r.json()["error"]["code"])


@dataclass(frozen=True)
class Ws:
    """A workspace's id, kept apart from the ORM row (a rollback expires that)."""

    id: UUID


async def _org(make_actor: MakeActor, db: AsyncSession) -> tuple[Actor, UUID, Ws]:
    """An Owner (TOTP enrolled, stepped up) of a new org whose workspace is active."""
    owner = await make_actor(f"owner-{uuid4().hex[:6]}@example.com")
    r = await owner.client.post(
        "/v1/orgs", json={"name": "Atlas SAMPLE SARL", "type": "business", "country": "MA"}
    )
    assert r.status_code == 202, r.text
    org_id = UUID(r.json()["org"]["id"])
    workspace = await db.scalar(select(Workspace).where(Workspace.org_id == org_id))
    assert workspace is not None
    workspace.state = WorkspaceState.active
    workspace.paperless_url = "http://paperless-sample.invalid:8000"
    ws = Ws(workspace.id)
    await db.commit()
    await owner.enable_totp()
    await owner.step_up()
    return owner, org_id, ws


async def _with_role(
    make_actor: MakeActor, db: AsyncSession, org_id: UUID, role: Role, *, step_up: bool = True
) -> Actor:
    actor = await make_actor(f"{role.value}-{uuid4().hex[:6]}@example.com")
    db.add(Membership(user_id=actor.user_id, org_id=org_id, role=role))
    await db.commit()
    if step_up:
        await actor.enable_totp()
        await actor.step_up()
    return actor


def _url(org_id: UUID, suffix: str = "") -> str:
    return f"/v1/orgs/{org_id}/closure{suffix}"


async def _actions(db: AsyncSession, workspace_id: UUID, prefix: str) -> list[tuple[str, Any]]:
    rows = await db.execute(
        select(AuditEvent.action, AuditEvent.data)
        .where(AuditEvent.workspace_id == workspace_id, AuditEvent.action.like(f"{prefix}%"))
        .order_by(AuditEvent.seq)
    )
    found = [(a, d) for a, d in rows]
    await db.rollback()
    return found


async def _org_row(db: AsyncSession, org_id: UUID) -> Organisation:
    org = await db.get(Organisation, org_id, populate_existing=True)
    assert org is not None
    return org


async def _document(
    db: AsyncSession, store: MemoryObjectStore, workspace: Ws, user_id: UUID,
    retain: date = FAR,
) -> UUID:  # fmt: skip
    """A SAMPLE filed document, its original in the store, retained until `retain`."""
    data = b"%PDF-1.7 SAMPLE " + uuid4().bytes
    sha = hashlib.sha256(data).hexdigest()
    doc_id = uuid4()
    await db.execute(
        text(
            """
            INSERT INTO document_ledger (id, workspace_id, sha256, md5, size_bytes, content_kind,
                source, object_key, object_version_id, state, ingest_state, ingest_attempts,
                retention_until, ingested_at, uploaded_by, paperless_doc_id)
            VALUES (:id, :ws, :sha, :md5, :size, 'pdf', 'upload', :key, 'v1', 'filed',
                'consumed', 0, :retain, now(), :user, 7)
            """
        ),
        {"id": doc_id, "ws": workspace.id, "sha": sha, "md5": "0" * 32,
         "size": len(data), "key": f"t/{sha}",
         "retain": retain, "user": user_id},
    )  # fmt: skip
    await db.commit()
    store.objects[ObjectRef(f"t/{sha}", "v1")] = StoredObject(
        data=data, mode="GOVERNANCE", retain_until=retain
    )
    return doc_id


# The request and its rules -----------------------------------------------------------------


async def test_closure_requires_owner_and_step_up(
    make_actor: MakeActor, db: AsyncSession, enqueuer: Any, clock: Any
) -> None:
    owner, org_id, workspace = await _org(make_actor, db)
    admin = await _with_role(make_actor, db, org_id, Role.admin)
    member = await _with_role(make_actor, db, org_id, Role.member)
    stranger = await make_actor(f"stranger-{uuid4().hex[:6]}@example.com")
    for actor, status, code in (
        (admin, 403, "forbidden"),
        (member, 403, "forbidden"),
        (stranger, 404, "not_found"),
    ):
        for method in ("POST", "GET", "DELETE"):
            r = await actor.client.request(method, _url(org_id))
            assert (r.status_code, _code(r)) == (status, code), (method, r.text)
        r = await actor.client.post(
            _url(org_id, "/takeover"),
            files={"file": ("statement.pdf", PDF, "application/pdf")},
            data={"signer_name": "SAMPLE", "signer_capacity": "Gérant"},
        )
        assert (r.status_code, _code(r)) == (status, code)
    # The Owner, but without a recent step-up.
    clock.advance(timedelta(minutes=11))
    r = await owner.client.post(_url(org_id))
    assert (r.status_code, _code(r)) == (403, "auth.step_up_required")
    assert (await owner.client.get(_url(org_id))).status_code == 404  # nothing requested
    assert enqueuer.exports == []
    assert (await _org_row(db, org_id)).status is OrgStatus.active

    await owner.step_up()
    requested_at = clock.now
    r = await owner.client.post(_url(org_id))
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["state"] == "grace"
    assert datetime.fromisoformat(body["requested_at"]) == requested_at
    assert datetime.fromisoformat(body["grace_ends_at"]) == requested_at + timedelta(days=30)
    assert (await _org_row(db, org_id)).status is OrgStatus.closing
    # A full export of the workspace is queued and sent.
    [job] = await db.scalars(select(ExportJob).where(ExportJob.workspace_id == workspace.id))
    assert (job.period, job.state, job.requested_by) == ("all", ExportState.queued, owner.user_id)
    assert body["export_job_id"] == str(job.id)
    assert enqueuer.exports == [job.id]
    closure_id = body["id"]
    [(action, data)] = await _actions(db, workspace.id, "closure.")
    assert (action, data["closure_id"]) == ("closure.requested", closure_id)
    assert datetime.fromisoformat(data["grace_ends_at"]) == requested_at + timedelta(days=30)
    assert await _actions(db, workspace.id, "export.requested") == [
        ("export.requested", {"period": "all"})
    ]
    assert (await owner.client.get(_url(org_id))).json()["id"] == closure_id
    # Once only.
    r = await owner.client.post(_url(org_id))
    assert (r.status_code, _code(r)) == (409, "closure.already_requested")
    # Cancel needs the step-up too.
    clock.advance(timedelta(minutes=11))
    r = await owner.client.delete(_url(org_id))
    assert (r.status_code, _code(r)) == (403, "auth.step_up_required")


async def test_grace_period_cancel(
    make_actor: MakeActor, db: AsyncSession, enqueuer: Any, clock: Any,
    memory_store: MemoryObjectStore,
) -> None:  # fmt: skip
    owner, org_id, workspace = await _org(make_actor, db)
    r = await owner.client.delete(_url(org_id))
    assert (r.status_code, _code(r)) == (404, "closure.not_requested")
    first = (await owner.client.post(_url(org_id))).json()

    # During the grace period the workspace keeps working, capture and writes included.
    r = await owner.client.post(
        f"/v1/workspaces/{workspace.id}/documents",
        files={"file": ("facture.pdf", PDF, "application/pdf")},
    )
    assert r.status_code == 202, r.text
    r = await owner.client.put(
        f"/v1/workspaces/{workspace.id}/retention/policies/invoice", json={"years": 12}
    )
    assert r.status_code == 200, r.text

    clock.advance(timedelta(days=10))
    await owner.step_up()
    r = await owner.client.delete(_url(org_id))
    assert r.status_code == 200, r.text
    assert (r.json()["id"], r.json()["state"]) == (first["id"], "cancelled")
    assert (await _org_row(db, org_id)).status is OrgStatus.active
    row = await db.get(Closure, UUID(first["id"]), populate_existing=True)
    assert row is not None
    assert (row.state, row.cancelled_at, row.cancelled_by) == (
        ClosureState.cancelled, clock.now, owner.user_id,
    )  # fmt: skip
    assert [a for a, _ in await _actions(db, workspace.id, "closure.")] == [
        "closure.requested", "closure.cancelled",
    ]  # fmt: skip
    r = await owner.client.delete(_url(org_id))
    assert (r.status_code, _code(r)) == (404, "closure.not_requested")

    # A second request starts a new grace period.
    r = await owner.client.post(_url(org_id))
    assert r.status_code == 201, r.text
    second = r.json()
    assert second["id"] != first["id"]
    assert datetime.fromisoformat(second["grace_ends_at"]) == clock.now + timedelta(days=30)
    assert len(enqueuer.exports) == 2

    # The grace period runs to the end of its last day (region-local), then cannot be cancelled.
    ends = datetime.fromisoformat(second["grace_ends_at"])
    clock.now = ends + timedelta(hours=1)
    await owner.step_up()
    r = await owner.client.get(_url(org_id))
    assert r.json()["state"] == "grace"
    clock.now = ends + timedelta(days=1, hours=1)
    await owner.step_up()
    r = await owner.client.delete(_url(org_id))
    assert (r.status_code, _code(r)) == (409, "closure.grace_over")
    assert (await _org_row(db, org_id)).status is OrgStatus.closing


async def test_closure_routes_are_never_billing_gated(
    make_actor: MakeActor, db: AsyncSession, clock: Any
) -> None:
    """Ruling R4: a read-only org can still close (and cancel)."""
    owner, org_id, _workspace = await _org(make_actor, db)
    await make_late(db, org_id, clock.now, SubscriptionStatus.read_only)
    assert (await owner.client.post(_url(org_id))).status_code == 201
    assert (await owner.client.delete(_url(org_id))).status_code == 200


# The take-over statement --------------------------------------------------------------------


async def test_takeover_requires_ready_full_export(
    make_actor: MakeActor, db: AsyncSession, clock: Any, cp_app: FastAPI,
    memory_store: MemoryObjectStore, export_store: MemoryExportStore,
) -> None:  # fmt: skip
    owner, org_id, workspace = await _org(make_actor, db)

    async def takeover(
        content: bytes = PDF, name: str = "Samira SAMPLE", capacity: str = "Gérante"
    ) -> httpx.Response:
        r: httpx.Response = await owner.client.post(
            _url(org_id, "/takeover"),
            files={"file": ("declaration.pdf", content, "application/pdf")},
            data={"signer_name": name, "signer_capacity": capacity},
        )
        return r

    r = await takeover()
    assert (r.status_code, _code(r)) == (404, "closure.not_requested")
    closure = (await owner.client.post(_url(org_id))).json()
    # The full export is still queued: not yet downloadable, so no statement.
    r = await takeover()
    assert (r.status_code, _code(r)) == (422, "takeover.invalid")
    # An older full export, from before the request, does not count either.
    assert await run_export(
        UUID(closure["export_job_id"]), sessions=cp_app.state.sessionmaker,
        index_db_url=UNUSED_INDEX, store=memory_store, exports=export_store, now=clock.now,
    )  # fmt: skip
    for bad in (
        {"content": b"not a pdf"},
        {"content": b""},
        {"name": "  "},
        {"capacity": ""},
        {"name": "x" * 201},
    ):
        r = await takeover(**bad)
        assert r.status_code == 422, (bad, r.text)
    assert memory_store.puts == []

    clock.advance(timedelta(minutes=5))
    r = await takeover()
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["sha256"] == hashlib.sha256(PDF).hexdigest()
    assert (body["signer_name"], body["signer_capacity"]) == ("Samira SAMPLE", "Gérante")
    row = await db.get(TakeoverStatement, UUID(body["id"]))
    assert row is not None
    assert (row.closure_id, row.org_id, row.signed_by) == (
        UUID(closure["id"]), org_id, owner.user_id,
    )  # fmt: skip
    # Write-once in the object store, under its own COMPLIANCE lock (legal evidence: no key can
    # shorten or delete it before its date), for at least ten years.
    stored = memory_store.objects[ObjectRef(row.object_key, row.object_version_id)]
    assert stored.data == PDF
    assert stored.mode == "COMPLIANCE"
    assert stored.retain_until >= date(clock.now.year + 10, 1, 1)
    assert row.object_key.startswith(f"takeover/{org_id}/")
    assert await _actions(db, workspace.id, "closure.takeover_signed") == [
        ("closure.takeover_signed", {"statement_id": body["id"], "sha256": body["sha256"]})
    ]
    assert (await owner.client.get(_url(org_id))).json()["takeover"]["id"] == body["id"]
    # Once per closure; the row cannot be changed or deleted by the app role.
    r = await takeover()
    assert (r.status_code, _code(r)) == (422, "takeover.invalid")
    with pytest.raises(Exception, match="permission denied"):
        await db.execute(text("UPDATE takeover_statement SET sha256 = repeat('0', 64)"))
    await db.rollback()
    with pytest.raises(Exception, match="permission denied"):
        await db.execute(text("DELETE FROM takeover_statement"))
    await db.rollback()


async def test_takeover_refused_once_the_export_has_expired_and_needs_a_new_one(
    make_actor: MakeActor, db: AsyncSession, clock: Any, cp_app: FastAPI,
    memory_store: MemoryObjectStore, export_store: MemoryExportStore,
) -> None:  # fmt: skip
    """An expired full export is no longer downloadable; a new full export, once ready, is."""
    owner, org_id, workspace = await _org(make_actor, db)
    closure = (await owner.client.post(_url(org_id))).json()
    await db.execute(
        text(
            "UPDATE export_job SET state = 'expired', object_key = 'k', sha256 = repeat('a', 64),"
            " size_bytes = 1, document_count = 0, ready_at = now() WHERE id = :id"
        ),
        {"id": UUID(closure["export_job_id"])},
    )
    await db.commit()
    files = {"file": ("declaration.pdf", PDF, "application/pdf")}
    data = {"signer_name": "Samira SAMPLE", "signer_capacity": "Gérante"}
    r = await owner.client.post(_url(org_id, "/takeover"), files=files, data=data)
    assert (r.status_code, _code(r)) == (422, "takeover.invalid")
    r = await owner.client.post(f"/v1/workspaces/{workspace.id}/exports", json={"period": "all"})
    assert r.status_code == 202, r.text
    assert await run_export(
        UUID(r.json()["id"]), sessions=cp_app.state.sessionmaker, index_db_url=UNUSED_INDEX,
        store=memory_store, exports=export_store, now=clock.now,
    )  # fmt: skip
    r = await owner.client.post(_url(org_id, "/takeover"), files=files, data=data)
    assert r.status_code == 201, r.text


async def test_a_takeover_does_not_outlive_its_cancelled_closure(
    make_actor: MakeActor, db: AsyncSession, clock: Any, cp_app: FastAPI,
    memory_store: MemoryObjectStore, export_store: MemoryExportStore,
) -> None:  # fmt: skip
    """Take-over, cancel, request again: the new closure carries no statement, so after its
    grace period a workspace holding a document under retention hibernates; nothing is
    destroyed early."""
    owner, org_id, workspace = await _org(make_actor, db)
    await _document(db, memory_store, workspace, owner.user_id)
    first = (await owner.client.post(_url(org_id))).json()
    await db.execute(
        text(
            "UPDATE export_job SET state = 'ready', object_key = 'k', sha256 = repeat('a', 64),"
            " size_bytes = 1, document_count = 1, ready_at = now() WHERE id = :id"
        ),
        {"id": UUID(first["export_job_id"])},
    )
    await db.commit()
    r = await owner.client.post(
        _url(org_id, "/takeover"),
        files={"file": ("declaration.pdf", PDF, "application/pdf")},
        data={"signer_name": "Samira SAMPLE", "signer_capacity": "Gérante"},
    )
    assert r.status_code == 201, r.text
    assert (await owner.client.delete(_url(org_id))).json()["state"] == "cancelled"
    second = (await owner.client.post(_url(org_id))).json()
    assert second["id"] != first["id"]
    assert second["takeover"] is None
    assert (await owner.client.get(_url(org_id))).json()["takeover"] is None

    runtime = RecordingRuntime()

    async def send_removal(ledger_id: UUID, workspace_id: UUID) -> None:
        raise AssertionError("nothing is past retention")

    await db.execute(
        text(
            "UPDATE closure SET requested_at = requested_at - interval '31 days',"
            " grace_ends_at = grace_ends_at - interval '31 days' WHERE id = :id"
        ),
        {"id": UUID(second["id"])},
    )
    await db.commit()
    await closure_daily(
        async_sessionmaker(db.bind, expire_on_commit=False), runtime=runtime, locks=NoLocks(),
        settings=SETTINGS, send_removal=send_removal, now=datetime.now(UTC),
    )  # fmt: skip
    assert runtime.calls == [("stop", tenant_ref(workspace.id))]
    ws = await db.get(Workspace, workspace.id, populate_existing=True)
    assert ws is not None and ws.state is WorkspaceState.hibernated
    row = await db.get(Closure, UUID(second["id"]), populate_existing=True)
    assert row is not None and row.state is ClosureState.hibernated


# Hibernation --------------------------------------------------------------------------------


async def _service_token(db: AsyncSession, app: FastAPI, workspace: Ws) -> None:
    """The tenant's service token, so the documents routes would reach Paperless if let."""
    kind = CredentialKind.service_token
    row = await db.get(Workspace, workspace.id)
    assert row is not None
    envelope = seal(b"SAMPLE-token", app.state.keys, row.kms_key_id, f"{workspace.id}:{kind}")
    db.add(TenantCredential(workspace_id=workspace.id, kind=kind, envelope=envelope.to_bytes()))
    await db.commit()


def _paperless_forbidden(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Records every Paperless client made (the documents routes swallow its errors)."""
    made: list[str] = []

    def refuse(url: str, *_args: Any, **_kwargs: Any) -> None:
        made.append(url)
        raise AssertionError("Paperless was called for a hibernated workspace")

    monkeypatch.setattr(documents, "PaperlessClient", refuse)
    return made


async def _hibernated(
    owner: Actor, db: AsyncSession, cp_app: FastAPI, org_id: UUID, clock: Any
) -> RecordingRuntime:
    """Requests the closure, then runs the daily job once the grace period is over."""
    r = await owner.client.post(_url(org_id))
    assert r.status_code == 201, r.text
    runtime = RecordingRuntime()
    removals: list[tuple[UUID, UUID]] = []

    async def send_removal(ledger_id: UUID, workspace_id: UUID) -> None:
        removals.append((ledger_id, workspace_id))

    await db.execute(
        text(
            "UPDATE closure SET requested_at = requested_at - interval '31 days',"
            " grace_ends_at = grace_ends_at - interval '31 days' WHERE org_id = :org"
        ),
        {"org": org_id},
    )
    await db.commit()
    sessions = async_sessionmaker(db.bind, expire_on_commit=False)
    await closure_daily(
        sessions, runtime=runtime, locks=NoLocks(), settings=SETTINGS,
        send_removal=send_removal, now=datetime.now(UTC),
    )  # fmt: skip
    assert removals == []
    return runtime


async def test_closure_keeps_retained_documents(
    make_actor: MakeActor, db: AsyncSession, clock: Any, cp_app: FastAPI,
    memory_store: MemoryObjectStore, export_store: MemoryExportStore,
    monkeypatch: pytest.MonkeyPatch, enqueuer: Any,
) -> None:  # fmt: skip
    """A document still under retention: the workspace hibernates. Its Owner lists it, reads
    it, downloads the original from our object store and exports; Paperless is never called;
    every other role gets 404."""
    owner, org_id, workspace = await _org(make_actor, db)
    admin = await _with_role(make_actor, db, org_id, Role.admin)
    member = await _with_role(make_actor, db, org_id, Role.member, step_up=False)
    doc = await _document(db, memory_store, workspace, owner.user_id)
    await _service_token(db, cp_app, workspace)
    paperless = _paperless_forbidden(monkeypatch)

    runtime = await _hibernated(owner, db, cp_app, org_id, clock)

    assert runtime.calls == [("stop", tenant_ref(workspace.id))]
    ws = await db.get(Workspace, workspace.id, populate_existing=True)
    assert ws is not None and ws.state is WorkspaceState.hibernated
    closure = await db.scalar(select(Closure).where(Closure.org_id == org_id))
    assert closure is not None and closure.state is ClosureState.hibernated
    sub = await db.scalar(select(Subscription).where(Subscription.org_id == org_id))
    assert sub is not None and sub.status is SubscriptionStatus.closed
    assert (await _org_row(db, org_id)).status is OrgStatus.closing
    closure_id = str(closure.id)
    [(_action, data)] = await _actions(db, workspace.id, "closure.hibernated")
    assert data == {"closure_id": closure_id, "retained": "1"}
    await db.rollback()

    base = f"/v1/workspaces/{workspace.id}"
    await owner.step_up()
    r = await owner.client.get(f"{base}/documents")
    assert r.status_code == 200, r.text
    assert ([i["id"] for i in r.json()["items"]], r.json()["engine"]) == (
        [str(doc)], "unavailable",
    )  # fmt: skip
    r = await owner.client.get(f"{base}/documents/{doc}")
    assert r.status_code == 200, r.text
    assert paperless == []
    r = await owner.client.get(f"{base}/documents/{doc}/file")
    assert r.status_code == 200, r.text
    stored = next(o for ref, o in memory_store.objects.items() if ref.key.startswith("t/"))
    assert r.content == stored.data
    assert r.headers["content-type"] == "application/pdf"
    r = await owner.client.post(f"{base}/exports", json={"period": "all"})
    assert r.status_code == 202, r.text
    r = await owner.client.get(f"{base}/exports/{r.json()['id']}")
    assert r.status_code == 200, r.text
    for actor in (admin, member):
        for path in ("/documents", f"/documents/{doc}", f"/documents/{doc}/file"):
            r = await actor.client.get(base + path)
            assert (r.status_code, _code(r)) == (404, "not_found"), (actor.email, path)
    # The original and its lock are untouched.
    assert stored.mode == "GOVERNANCE" and stored.retain_until == FAR
    assert enqueuer.removals == []


async def test_hibernated_workspace_refuses_capture_and_writes(
    make_actor: MakeActor, db: AsyncSession, clock: Any, cp_app: FastAPI,
    memory_store: MemoryObjectStore, monkeypatch: pytest.MonkeyPatch, enqueuer: Any,
) -> None:  # fmt: skip
    """Rulings R5 and R3: every write of the Owner answers `billing.read_only` (a missing
    document still 404 first); a new export is the one write left (exports stay open); capture
    is refused; every other role gets 404 on writes too."""
    owner, org_id, workspace = await _org(make_actor, db)
    member = await _with_role(make_actor, db, org_id, Role.member)
    doc = await _document(db, memory_store, workspace, owner.user_id)
    _paperless_forbidden(monkeypatch)
    await _hibernated(owner, db, cp_app, org_id, clock)
    await owner.step_up()
    before = await db.scalar(
        text("SELECT jsonb_agg(to_jsonb(l) ORDER BY id) FROM document_ledger l")
    )
    await db.rollback()

    # Every document past retention was approved for deletion by the closure itself (none here),
    # so an approval by hand finds nothing eligible, before any billing check (ruling R3).
    writes = workspace_writes(workspace.id, doc, doc)
    approve = [w for w in writes if w[1].endswith("/approve-deletion")]
    writes = [w for w in writes if w not in approve and not _is_export(w)]
    for method, url, body in approve:
        r = await owner.client.request(method, url, json=body)
        assert (r.status_code, _code(r)) == (409, "retention.not_eligible"), r.text
        r = await member.client.request(method, url, json=body)
        assert (r.status_code, _code(r)) == (404, "not_found"), r.text
    for method, url, body in writes:
        r = await owner.client.request(method, url, json=body)
        assert (r.status_code, _code(r)) == (402, "billing.read_only"), (method, url, r.text)
        r = await member.client.request(method, url, json=body)
        assert (r.status_code, _code(r)) == (404, "not_found"), (method, url, r.text)
    missing = f"/v1/workspaces/{workspace.id}/documents/{uuid4()}/fields"
    r = await owner.client.patch(missing, json={"fields": {"total_ttc": "1.00"}})
    assert (r.status_code, _code(r)) == (404, "document.not_found")
    r = await owner.client.post(
        f"/v1/workspaces/{workspace.id}/documents",
        files={"file": ("facture.pdf", PDF, "application/pdf")},
    )
    assert (r.status_code, _code(r)) == (409, "workspace.not_ready")
    assert enqueuer.ingests == []
    assert enqueuer.removals == [] and enqueuer.locks == [] and enqueuer.asks == []
    after = await db.scalar(
        text("SELECT jsonb_agg(to_jsonb(l) ORDER BY id) FROM document_ledger l")
    )
    assert after == before


def _is_export(write: tuple[str, str, Any]) -> bool:
    return write[1].endswith("/exports")
