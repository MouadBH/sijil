"""The billing gate (ADM-4, design §5.7a): overdue stops exports (and AI, and new users);
read-only refuses every state-changing workspace route but capture; reads and capture are never
gated; and no state-changing route can forget the gate."""

from collections.abc import Awaitable, Callable, Iterator
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.dependencies.models import Dependant
from fastapi.routing import APIRoute, iter_route_contexts
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import SubscriptionStatus, User, Workspace
from sijil_cp.permissions import BILLING_GATES, GATED_IN_HANDLER
from tests.support.billing_states import make_late, workspace_writes

MakeActor = Callable[[str], Awaitable[Any]]
FORMATS = Path(__file__).resolve().parents[3] / "fixtures" / "formats"
PDF = (FORMATS / "sample.pdf").read_bytes()
UNSAFE = {"POST", "PUT", "PATCH", "DELETE"}

# Every state-changing route outside /v1/auth/ applies the billing gate, unless it is here,
# with the reason.
NOT_GATED = {
    ("POST", "/v1/workspaces/{workspace_id}/documents"): "upload: capture is never gated",
    ("POST", "/v1/workspaces/{workspace_id}/search"): "a read sent as POST: reads are never gated",
    ("PUT", "/v1/orgs/{org_id}/subscription"): "org billing: choosing a plan",
    ("PATCH", "/v1/orgs/{org_id}"): "org billing: the ICE an invoice needs (and the org's name)",
    ("POST", "/v1/orgs"): "a new organisation has no standing yet",
    ("POST", "/v1/workspaces"): "refuses every request today (one workspace per business)",
    ("POST", "/v1/orgs/{org_id}/members"): "the seat check refuses a new user with the 402",
    ("PATCH", "/v1/orgs/{org_id}/members/{user_id}"): "access control is never blocked",
    ("DELETE", "/v1/orgs/{org_id}/members/{user_id}"): "revoking access is never blocked",
    ("POST", "/v1/staff/invoices/{invoice_id}/mark-paid"): "staff: a payment lifts the gate",
    ("POST", "/v1/orgs/{org_id}/closure"): "closure: an account can always be closed (R4)",
    ("DELETE", "/v1/orgs/{org_id}/closure"): "closure: cancelling it, during the grace period",
    ("POST", "/v1/orgs/{org_id}/closure/takeover"): "closure: the take-over statement",
} | {
    (method, path): "staff: the catch-all hiding /v1/staff, always 404"
    for method in UNSAFE
    for path in ("/v1/staff", "/v1/staff/{rest:path}")
}


def _calls(dependant: Dependant) -> Iterator[object]:
    for dep in dependant.dependencies:
        yield dep.call
        yield from _calls(dep)


def test_every_mutating_route_applies_the_billing_gate(make_app: Callable[..., FastAPI]) -> None:
    seen: set[tuple[str, str]] = set()
    for context in iter_route_contexts(make_app().routes):
        route = context.original_route
        if not isinstance(route, APIRoute) or context.path is None:
            continue
        for method in (context.methods or set()) & UNSAFE:
            key = (method, context.path)
            seen.add(key)
            if context.path.startswith("/v1/auth/") or key in NOT_GATED:
                continue
            gated = route.endpoint in GATED_IN_HANDLER or any(
                call in BILLING_GATES for call in _calls(route.dependant)
            )
            assert gated, f"{method} {context.path} has no billing gate"
    assert len(seen) > len(NOT_GATED)
    assert set(NOT_GATED) <= seen, "a route on the list no longer exists"


async def _owner(make_actor: MakeActor, db: AsyncSession) -> tuple[Any, str, Workspace]:
    alice = await make_actor("alice@example.com")
    r = await alice.client.post(
        "/v1/orgs", json={"name": "Atlas SARL", "type": "business", "country": "MA"}
    )
    assert r.status_code == 202, r.text
    org_id: str = r.json()["org"]["id"]
    await alice.enable_totp()
    await alice.step_up()
    workspace = await db.scalar(select(Workspace).where(Workspace.org_id == UUID(org_id)))
    assert workspace is not None
    return alice, org_id, workspace


async def _document(
    db: AsyncSession, workspace: Workspace, user_id: UUID, retain: date = date(2036, 12, 31)
) -> UUID:
    """A SAMPLE filed ledger row, retained until `retain`."""
    doc_id = uuid4()
    await db.execute(
        text(
            """
            INSERT INTO document_ledger (id, workspace_id, sha256, md5, size_bytes, content_kind,
                source, object_key, object_version_id, state, ingest_state, ingest_attempts,
                retention_until, ingested_at, uploaded_by)
            VALUES (:id, :ws, md5(:n) || md5(:n || 'b'), md5(:n), 10, 'pdf', 'upload', 'k/' || :n,
                'v1', 'filed', 'consumed', 0, :retain, now(), :user)
            """
        ),
        {"id": doc_id, "ws": workspace.id, "n": str(doc_id), "retain": retain,
         "user": user_id},
    )  # fmt: skip
    await db.commit()
    return doc_id


def error(r: Any) -> str:
    code: str = r.json()["error"]["code"]
    return code


async def _upload(alice: Any, workspace: Workspace, enqueuer: Any) -> None:
    r = await alice.client.post(
        f"/v1/workspaces/{workspace.id}/documents",
        files={"file": ("facture.pdf", PDF, "application/pdf")},
    )
    assert r.status_code == 202, r.text
    assert [w for _, w in enqueuer.ingests] == [workspace.id]


async def test_read_only_refuses_writes_but_not_upload_or_reads(
    make_actor: MakeActor, db: AsyncSession, enqueuer: Any
) -> None:
    alice, org_id, workspace = await _owner(make_actor, db)
    doc = await _document(db, workspace, alice.user_id)
    expired = await _document(db, workspace, alice.user_id, retain=date(2020, 12, 31))
    await make_late(db, UUID(org_id), alice.clock.now, SubscriptionStatus.read_only)
    for method, url, body in workspace_writes(workspace.id, doc, expired):
        r = await alice.client.request(method, url, json=body)
        assert (r.status_code, error(r)) == (402, "billing.read_only"), (method, url, r.text)
    # Rule R3: a document that does not exist (or is hidden) answers 404, never the 402.
    missing = f"/v1/workspaces/{workspace.id}/documents/{uuid4()}/fields"
    r = await alice.client.patch(missing, json={"fields": {"total_ttc": "1.00"}})
    assert (r.status_code, error(r)) == (404, "document.not_found")
    # Reads, capture and the org's billing stay open.
    assert (await alice.client.get(f"/v1/workspaces/{workspace.id}/documents")).status_code == 200
    assert (await alice.client.get(f"/v1/orgs/{org_id}/subscription")).json()["status"] == (
        "read_only"
    )
    assert (await alice.client.get(f"/v1/orgs/{org_id}/invoices")).status_code == 200
    r = await alice.client.patch(f"/v1/orgs/{org_id}", json={"name": "Atlas SARL"})
    assert r.status_code == 200, r.text
    await _upload(alice, workspace, enqueuer)
    # Nothing was refused into the ledger: the document is untouched.
    state = await db.scalar(text("SELECT state FROM document_ledger WHERE id = :id"), {"id": doc})
    assert state == "filed"


async def test_overdue_restricts_exports_not_capture(
    make_actor: MakeActor, db: AsyncSession, enqueuer: Any
) -> None:
    alice, org_id, workspace = await _owner(make_actor, db)
    await make_late(db, UUID(org_id), alice.clock.now, SubscriptionStatus.overdue)
    r = await alice.client.post(
        f"/v1/workspaces/{workspace.id}/exports", json={"period": "2026-11"}
    )
    assert (r.status_code, error(r)) == (402, "billing.overdue")
    # No new user while overdue: 402 with the billing code, not the quota's 409.
    bob = User(email="bob@example.com", name="SAMPLE", password_hash="x")
    db.add(bob)
    await db.commit()
    r = await alice.client.post(
        f"/v1/orgs/{org_id}/members", json={"email": "bob@example.com", "role": "member"}
    )
    assert (r.status_code, error(r)) == (402, "billing.overdue")
    # Other writes go on while merely overdue: the gate lets them through to the handler.
    r = await alice.client.put(
        f"/v1/workspaces/{workspace.id}/retention/policies/invoice", json={"years": 12}
    )
    assert r.status_code == 200, r.text
    await _upload(alice, workspace, enqueuer)


async def test_a_missing_document_answers_before_the_billing_gate_in_handlers(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    """Ruling R3 in the routes that check in their handler: on a read-only workspace, removing
    a document that does not exist is 404 `document.not_found`, and approving its deletion is
    the usual `retention.not_eligible` naming it, never the 402."""
    alice, org_id, workspace = await _owner(make_actor, db)
    await make_late(db, UUID(org_id), alice.clock.now, SubscriptionStatus.read_only)
    missing = uuid4()
    base = f"/v1/workspaces/{workspace.id}"
    r = await alice.client.post(
        f"{base}/documents/{missing}/remove", json={"reason": "SAMPLE: uploaded by mistake"}
    )
    assert (r.status_code, error(r)) == (404, "document.not_found")
    r = await alice.client.post(f"{base}/retention/approve-deletion", json={"ids": [str(missing)]})
    assert (r.status_code, error(r), r.json()["ids"]) == (
        409, "retention.not_eligible", [str(missing)]
    )  # fmt: skip


@pytest.mark.parametrize("status", [SubscriptionStatus.overdue, SubscriptionStatus.read_only])
async def test_a_role_refusal_comes_before_the_billing_one(
    make_actor: MakeActor, db: AsyncSession, status: SubscriptionStatus
) -> None:
    """Ruling R3: a Read-only member asking to seal gets 403, whatever the billing state."""
    alice, org_id, workspace = await _owner(make_actor, db)
    doc = await _document(db, workspace, alice.user_id)
    carol = await make_actor("carol@example.com")
    r = await alice.client.post(
        f"/v1/orgs/{org_id}/members", json={"email": "carol@example.com", "role": "readonly"}
    )
    assert r.status_code == 201, r.text
    await make_late(db, UUID(org_id), alice.clock.now, status)
    r = await carol.client.post(f"/v1/workspaces/{workspace.id}/documents/{doc}/seal")
    assert (r.status_code, error(r)) == (403, "forbidden")
