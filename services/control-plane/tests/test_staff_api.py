"""The staff API (ADM-4): platform staff record a bank transfer as paid. Hidden from everyone
else (404 `staff.forbidden` on any `/v1/staff/*` path or method), TOTP for every staff route
and a fresh step-up for `mark-paid`, and only the orgs of the service's region."""

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit, billing
from sijil_core.db.models import (
    ActorType,
    AuditEvent,
    Invoice,
    InvoiceState,
    Subscription,
    SubscriptionStatus,
    Workspace,
)
from sijil_core.retention import region_local_date
from sijil_cp import deps
from tests.support.billing_states import make_late

pytestmark = pytest.mark.db

MakeActor = Callable[[str], Awaitable[Any]]
REFERENCE = "VIR-2026-000123"  # SAMPLE: a fictitious transfer reference


def error(r: Any) -> str:
    code: str = r.json()["error"]["code"]
    return code


async def late_org(
    make_actor: MakeActor, db: AsyncSession, status: SubscriptionStatus
) -> tuple[Any, UUID, Invoice]:
    """Alice's org, its first annual invoice unpaid long enough to be `status`."""
    alice = await make_actor("alice@example.com")
    r = await alice.client.post(
        "/v1/orgs", json={"name": "Atlas SARL", "type": "business", "country": "MA"}
    )
    assert r.status_code == 202, r.text
    org_id = UUID(r.json()["org"]["id"])
    await alice.enable_totp()
    await alice.step_up()
    await make_late(db, org_id, alice.clock.now, status)
    invoice = await db.scalar(select(Invoice).where(Invoice.org_id == org_id))
    assert invoice is not None
    return alice, org_id, invoice


async def make_staff(db: AsyncSession, user_id: UUID) -> None:
    await db.execute(
        text("UPDATE user_account SET is_staff = true WHERE id = :id"), {"id": user_id}
    )
    await db.commit()


async def staff_member(make_actor: MakeActor, db: AsyncSession) -> Any:
    """Sam, staff, with TOTP and a fresh step-up."""
    sam = await make_actor("sam@example.com")
    await make_staff(db, sam.user_id)
    await sam.enable_totp()
    await sam.step_up()
    return sam


def today(actor: Any) -> str:
    return region_local_date(actor.clock.now, "ma-cas").isoformat()


def routes(invoice_id: UUID, org_id: UUID) -> list[tuple[str, str, Any]]:
    return [
        ("GET", "/v1/staff/invoices", None),
        ("GET", "/v1/staff/invoices?state=overdue", None),
        ("POST", f"/v1/staff/invoices/{invoice_id}/mark-paid",
         {"payment_reference": REFERENCE, "paid_on": "2026-11-01"}),
        # An invalid body never reveals the route by a 422.
        ("POST", f"/v1/staff/invoices/{invoice_id}/mark-paid", {"nonsense": True}),
        ("GET", f"/v1/staff/orgs/{org_id}/subscription", None),
        # Unknown sub-paths and wrong methods answer the same.
        ("GET", "/v1/staff", None),
        ("GET", "/v1/staff/", None),
        ("GET", "/v1/staff/users", None),
        ("DELETE", "/v1/staff/invoices", None),
        ("PUT", f"/v1/staff/orgs/{org_id}/subscription", {"plan_code": "pro"}),
        ("GET", f"/v1/staff/invoices/{invoice_id}/mark-paid", None),
    ]  # fmt: skip


async def test_non_staff_gets_404_on_staff_routes(
    make_actor: MakeActor,
    db: AsyncSession,
    cp_app: FastAPI,
    csrf_client_for: Callable[..., httpx.AsyncClient],
) -> None:
    # Alice is an Owner with TOTP and a fresh step-up: being one is not being staff.
    alice, org_id, invoice = await late_org(make_actor, db, SubscriptionStatus.overdue)
    async with csrf_client_for(cp_app) as anonymous:
        for client in (alice.client, anonymous):
            for method, url, body in routes(invoice.id, org_id):
                r = await client.request(method, url, json=body)
                assert (r.status_code, error(r)) == (404, "staff.forbidden"), (method, url)
        # Nor does the API's own description list them.
        paths = (await anonymous.get("/v1/openapi.json")).json()["paths"]
        assert not [p for p in paths if p.startswith("/v1/staff")]
    # Nothing moved.
    await db.refresh(invoice)
    assert invoice.state is InvoiceState.overdue
    # A staff member sees the routes: an unknown one is a plain 404.
    sam = await staff_member(make_actor, db)
    r = await sam.client.get("/v1/staff/users")
    assert (r.status_code, error(r)) == (404, "not_found")
    assert (await sam.client.get("/v1/staff/invoices")).status_code == 200


async def test_mark_paid_requires_totp_and_step_up(make_actor: MakeActor, db: AsyncSession) -> None:
    _, _, invoice = await late_org(make_actor, db, SubscriptionStatus.overdue)
    sam = await make_actor("sam@example.com")
    await make_staff(db, sam.user_id)
    paid = f"/v1/staff/invoices/{invoice.id}/mark-paid"
    body = {"payment_reference": REFERENCE, "paid_on": today(sam)}
    # No TOTP: no staff route at all.
    for r in (await sam.client.get("/v1/staff/invoices"), await sam.client.post(paid, json=body)):
        assert (r.status_code, error(r)) == (403, "auth.totp_enrolment_required")
    # TOTP enrolled, this session never verified a code.
    await sam.enable_totp()
    r = await sam.client.get("/v1/staff/invoices")
    assert (r.status_code, error(r)) == (403, "auth.step_up_required")
    # Verified, but long ago: reads work, recording a payment does not.
    await sam.step_up()
    sam.clock.advance(timedelta(minutes=11))
    assert (await sam.client.get("/v1/staff/invoices")).status_code == 200
    r = await sam.client.post(paid, json=body)
    assert (r.status_code, error(r)) == (403, "auth.step_up_required")
    await db.refresh(invoice)
    assert (invoice.state, invoice.paid_at) == (InvoiceState.overdue, None)
    await sam.step_up()
    r = await sam.client.post(paid, json=body)
    assert r.status_code == 200, r.text
    assert r.json()["invoice"]["state"] == "paid"


@pytest.mark.parametrize("status", [SubscriptionStatus.overdue, SubscriptionStatus.read_only])
async def test_mark_paid_lifts_overdue_and_read_only_at_once(
    make_actor: MakeActor, db: AsyncSession, status: SubscriptionStatus
) -> None:
    alice, org_id, invoice = await late_org(make_actor, db, status)
    workspace = await db.scalar(select(Workspace).where(Workspace.org_id == org_id))
    assert workspace is not None
    exports = f"/v1/workspaces/{workspace.id}/exports"
    r = await alice.client.post(exports, json={"period": "2026-10"})
    assert (r.status_code, error(r)) == (402, f"billing.{status.value}")

    sam = await staff_member(make_actor, db)
    r = await sam.client.get("/v1/staff/invoices?state=overdue")
    assert r.status_code == 200, r.text
    listed = r.json()
    assert [i["number"] for i in listed] == [invoice.number]
    assert listed[0]["org_id"] == str(org_id)
    assert listed[0]["amount_ttc"] == f"{invoice.amount_ttc:.2f}"
    r = await sam.client.get(f"/v1/staff/orgs/{org_id}/subscription")
    assert r.json()["status"] == status.value

    r = await sam.client.post(
        f"/v1/staff/invoices/{invoice.id}/mark-paid",
        json={"payment_reference": REFERENCE, "paid_on": today(sam)},
    )
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["invoice"]["state"] == "paid"
    assert out["invoice"]["payment_reference"] == REFERENCE
    assert out["invoice"]["amount_ttc"] == f"{invoice.amount_ttc:.2f}"
    assert out["subscription"]["status"] == "active"

    # Stored, not only derived: the subscription row is active again.
    paid = await db.get(Invoice, invoice.id, populate_existing=True)
    assert paid is not None and paid.state is InvoiceState.paid
    assert paid.payment_reference == REFERENCE and paid.paid_at is not None
    sub = await db.scalar(
        select(Subscription)
        .where(Subscription.org_id == org_id)
        .execution_options(populate_existing=True)
    )
    assert sub is not None and sub.status is SubscriptionStatus.active
    standing = await billing.standing(db, org_id, sam.clock.now)
    assert standing.writes_allowed and standing.export_allowed and standing.ai_allowed
    assert standing.billing_code is None

    # Every restriction lifts at once, for the customer too.
    assert (await alice.client.get(f"/v1/orgs/{org_id}/subscription")).json()["status"] == (
        "active"
    )
    r = await alice.client.post(exports, json={"period": "2026-10"})
    assert r.status_code not in (402,), r.text
    r = await alice.client.put(
        f"/v1/workspaces/{workspace.id}/retention/policies/invoice", json={"years": 12}
    )
    assert r.status_code == 200, r.text
    assert (await sam.client.get("/v1/staff/invoices?state=overdue")).json() == []
    r = await sam.client.get("/v1/staff/invoices?state=paid")
    assert [i["number"] for i in r.json()] == [invoice.number]

    # Audited on the org's chain, the staff member as actor, amounts as strings.
    events = list(
        await db.scalars(
            select(AuditEvent).where(
                AuditEvent.workspace_id == workspace.id,
                AuditEvent.action == "billing.invoice_paid",
            )
        )
    )
    assert len(events) == 1
    event = events[0]
    assert (event.actor_type, event.actor_id) == (ActorType.user, str(sam.user_id))
    assert event.data == {"number": invoice.number, "amount_ttc": f"{invoice.amount_ttc:.2f}"}
    assert (await audit.verify(db, workspace.id)).ok


async def test_mark_paid_idempotent_and_conflict_on_other_reference(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    _, org_id, invoice = await late_org(make_actor, db, SubscriptionStatus.overdue)
    sam = await staff_member(make_actor, db)
    paid = f"/v1/staff/invoices/{invoice.id}/mark-paid"
    body = {"payment_reference": REFERENCE, "paid_on": today(sam)}
    first = await sam.client.post(paid, json=body)
    assert first.status_code == 200, first.text
    again = await sam.client.post(paid, json=body)
    assert again.status_code == 200, again.text
    assert again.json()["invoice"] == first.json()["invoice"]
    r = await sam.client.post(paid, json=body | {"payment_reference": "VIR-2026-000999"})
    assert (r.status_code, error(r)) == (409, "invoice.already_paid")
    row = await db.get(Invoice, invoice.id, populate_existing=True)
    assert row is not None and row.payment_reference == REFERENCE
    count = await db.scalar(
        text("SELECT count(*) FROM audit_event WHERE action = 'billing.invoice_paid'")
    )
    assert count == 1
    # An unknown invoice.
    r = await sam.client.post(f"/v1/staff/invoices/{uuid4()}/mark-paid", json=body)
    assert (r.status_code, error(r)) == (404, "invoice.not_found")


async def test_mark_paid_validates_reference_and_date(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    _, _, invoice = await late_org(make_actor, db, SubscriptionStatus.overdue)
    sam = await staff_member(make_actor, db)
    paid = f"/v1/staff/invoices/{invoice.id}/mark-paid"
    now = region_local_date(sam.clock.now, "ma-cas")
    issued = region_local_date(invoice.issued_at, "ma-cas")
    bad = [
        {"payment_reference": "", "paid_on": now.isoformat()},
        {"payment_reference": "x" * 65, "paid_on": now.isoformat()},
        {"payment_reference": "VIR<script>", "paid_on": now.isoformat()},
        {"payment_reference": " VIR-1", "paid_on": now.isoformat()},
        {"payment_reference": "VIR\n1", "paid_on": now.isoformat()},
        {"payment_reference": REFERENCE, "paid_on": "not-a-date"},
        {"payment_reference": REFERENCE, "paid_on": (now + timedelta(days=1)).isoformat()},
        {"payment_reference": REFERENCE, "paid_on": (issued - timedelta(days=1)).isoformat()},
    ]
    for body in bad:
        r = await sam.client.post(paid, json=body)
        assert (r.status_code, error(r)) == (422, "validation"), body
    row = await db.get(Invoice, invoice.id, populate_existing=True)
    assert row is not None and row.state is InvoiceState.overdue
    # The issue date itself is accepted, and the instant recorded never precedes the issue.
    r = await sam.client.post(
        paid, json={"payment_reference": "VIR 2026/11.000123", "paid_on": issued.isoformat()}
    )
    assert r.status_code == 200, r.text
    row = await db.get(Invoice, invoice.id, populate_existing=True)
    assert row is not None and row.paid_at is not None
    assert invoice.issued_at <= row.paid_at <= sam.clock.now


async def test_staff_cannot_reach_another_regions_org(
    make_actor: MakeActor,
    db: AsyncSession,
    make_db_app: Callable[..., FastAPI],
    csrf_client_for: Callable[..., httpx.AsyncClient],
) -> None:
    _, org_id, invoice = await late_org(make_actor, db, SubscriptionStatus.overdue)
    sam = await staff_member(make_actor, db)
    # The same staff session, on a control plane of another region.
    elsewhere = make_db_app(region="fr-par")
    elsewhere.dependency_overrides[deps.get_now] = lambda: sam.clock.now
    async with csrf_client_for(elsewhere) as other:
        other.cookies.update(sam.client.cookies)
        r = await other.get("/v1/staff/invoices")
        assert r.status_code == 200, r.text
        assert r.json() == []
        r = await other.get("/v1/staff/invoices?state=overdue")
        assert r.json() == []
        r = await other.post(
            f"/v1/staff/invoices/{invoice.id}/mark-paid",
            json={"payment_reference": REFERENCE, "paid_on": today(sam)},
        )
        assert (r.status_code, error(r)) == (404, "invoice.not_found")
        r = await other.get(f"/v1/staff/orgs/{org_id}/subscription")
        assert (r.status_code, error(r)) == (404, "not_found")
    row = await db.get(Invoice, invoice.id, populate_existing=True)
    assert row is not None and row.state is InvoiceState.overdue
    # In its own region the org is there.
    r = await sam.client.get(f"/v1/staff/orgs/{org_id}/subscription")
    assert (r.status_code, r.json()["status"]) == (200, "overdue")
    assert [i["id"] for i in (await sam.client.get("/v1/staff/invoices")).json()] == [
        str(invoice.id)
    ]
