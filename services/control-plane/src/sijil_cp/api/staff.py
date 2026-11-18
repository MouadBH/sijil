"""The staff API (ADM-4): platform staff record a bank transfer as paid.

- **Hidden.** Anyone who is not staff (signed out, mid-login, or a user without
  `User.is_staff`) gets 404 `staff.forbidden` on every `/v1/staff` path and method, known or
  not, before the body is even read; the routes are left out of the OpenAPI document. Staff
  is set only by `sijil-cp staff grant|revoke`.
- **Second factor.** Staff need TOTP enrolled (403 `auth.totp_enrolment_required`) and a
  session that verified a code (403 `auth.step_up_required`); `mark-paid` needs that
  verification to be fresh (`deps.ensure_step_up`).
- **Region.** Only the orgs of this service's region are listed or acted on (`assert_region`
  per org): another region's invoice is 404 `invoice.not_found`, its org 404 `not_found`.

Routes:

- `GET /staff/invoices?state=issued|overdue|paid`: oldest due first. The state is the one the
  clock gives: an issued invoice past its due date is `overdue` even before the daily job marks
  it. Amounts are strings.
- `POST /staff/invoices/{id}/mark-paid {payment_reference, paid_on}`: the reference is 1-64
  characters, letters and digits with ` . _ / : # -` inside; `paid_on` is a date in the
  region's calendar, from the invoice's issue date to today (else 422 `validation`). It sets
  `state` `paid`, `payment_reference` and `paid_at` (the start of `paid_on` in the region,
  kept between the issue and now), audits `billing.invoice_paid` `{number, amount_ttc}` on the
  org's chain with the staff member as actor, then stores the subscription's status by
  `billing.advance`: with no other invoice unpaid past due it is `active` at once, every
  restriction lifted. The same reference again answers the paid invoice and changes nothing;
  another reference is 409 `invoice.already_paid`.
- `GET /staff/orgs/{id}/subscription`: the org's subscription, its status by the clock.
"""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Final
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field, field_serializer
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer

from sijil_core import billing
from sijil_core.db.models import (
    Invoice,
    InvoiceKind,
    InvoiceState,
    Organisation,
    User,
    UserSession,
)
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.retention import region_day_start, region_local_date
from sijil_core.settings import CoreSettings
from sijil_cp.api.billing import SubscriptionOut, subscription_out
from sijil_cp.auth import sessions
from sijil_cp.deps import ensure_step_up, get_db, get_now
from sijil_cp.errors import ApiError
from sijil_cp.org_audit import audit_org_event

FORBIDDEN: Final = "staff.forbidden"
# Letters and digits, with a few separators inside (never first or last): what a bank
# transfer's reference holds, and nothing that renders or breaks a line.
REFERENCE_PATTERN: Final = r"^[A-Za-z0-9](?:[A-Za-z0-9 ._/:#-]{0,62}[A-Za-z0-9])?$"
ALL_METHODS: Final = ["GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"]


@dataclass(frozen=True)
class Staff:
    user: User
    session: UserSession


Db = Annotated[AsyncSession, Depends(get_db)]
Now = Annotated[datetime, Depends(get_now)]


async def require_staff(request: Request, db: Db, now: Now) -> Staff:
    """The staff gate of every `/v1/staff` route (see the module docstring)."""
    raw = request.cookies.get(sessions.COOKIE)
    session = await sessions.resolve(db, raw, now) if raw else None
    user = await db.get(User, session.user_id) if session is not None else None
    if session is None or user is None or not user.is_staff:
        raise ApiError(FORBIDDEN, 404)
    if not user.totp_enabled:
        raise ApiError("auth.totp_enrolment_required", 403)
    if session.totp_verified_at is None:
        raise ApiError("auth.step_up_required", 403)
    return Staff(user=user, session=session)


StaffDep = Annotated[Staff, Depends(require_staff)]

router = APIRouter(
    prefix="/staff",
    tags=["staff"],
    dependencies=[Depends(require_staff)],
    include_in_schema=False,
)


class StaffInvoiceOut(BaseModel):
    id: UUID
    org_id: UUID
    org_name: str
    number: str
    kind: InvoiceKind
    state: InvoiceState
    plan_code: str
    amount_ht: Decimal
    amount_vat: Decimal
    amount_ttc: Decimal
    currency: str
    issued_at: datetime
    due_at: datetime
    paid_at: datetime | None
    payment_reference: str | None

    @field_serializer("amount_ht", "amount_vat", "amount_ttc")
    def _money(self, value: Decimal) -> str:
        return f"{value:.2f}"


class MarkPaid(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_reference: str = Field(pattern=REFERENCE_PATTERN)
    paid_on: date


class MarkPaidOut(BaseModel):
    invoice: StaffInvoiceOut
    subscription: SubscriptionOut


def _settings(request: Request) -> CoreSettings:
    settings: CoreSettings = request.app.state.settings
    return settings


def _in_region(org: Organisation | None, settings: CoreSettings, code: str) -> Organisation:
    """The org when it is of this region; otherwise 404 `code`, as for one that does not
    exist (never the 421, which would say it exists elsewhere)."""
    if org is None:
        raise ApiError(code, 404)
    try:
        assert_region(org.region, settings)
    except RegionMismatch:
        raise ApiError(code, 404) from None
    return org


def _effective_state(invoice: Invoice, now: datetime) -> InvoiceState:
    if invoice.state is InvoiceState.issued and invoice.due_at <= now:
        return InvoiceState.overdue
    return invoice.state


def _invoice_out(invoice: Invoice, org: Organisation, now: datetime) -> StaffInvoiceOut:
    return StaffInvoiceOut(
        id=invoice.id,
        org_id=org.id,
        org_name=org.name,
        number=invoice.number,
        kind=invoice.kind,
        state=_effective_state(invoice, now),
        plan_code=invoice.plan_code,
        amount_ht=invoice.amount_ht,
        amount_vat=invoice.amount_vat,
        amount_ttc=invoice.amount_ttc,
        currency=invoice.currency,
        issued_at=invoice.issued_at,
        due_at=invoice.due_at,
        paid_at=invoice.paid_at,
        payment_reference=invoice.payment_reference,
    )


@router.get("/invoices")
async def list_invoices(
    request: Request, db: Db, now: Now, state: InvoiceState | None = None
) -> list[StaffInvoiceOut]:
    settings = _settings(request)
    query = (
        select(Invoice, Organisation)
        .join(Organisation, Organisation.id == Invoice.org_id)
        .where(Organisation.region == settings.region)
        .options(defer(Invoice.pdf))
        .order_by(Invoice.due_at, Invoice.number)
    )
    past_due = and_(Invoice.state == InvoiceState.issued, Invoice.due_at <= now)
    if state is InvoiceState.overdue:
        query = query.where(or_(Invoice.state == InvoiceState.overdue, past_due))
    elif state is InvoiceState.issued:
        query = query.where(Invoice.state == InvoiceState.issued, Invoice.due_at > now)
    elif state is InvoiceState.paid:
        query = query.where(Invoice.state == InvoiceState.paid)
    rows = (await db.execute(query)).all()
    return [_invoice_out(inv, _in_region(org, settings, "not_found"), now) for inv, org in rows]


def _reject_paid_on() -> RequestValidationError:
    return RequestValidationError(
        [{"loc": ("body", "paid_on"), "msg": "out of range", "type": "value_error"}]
    )


@router.post("/invoices/{invoice_id}/mark-paid")
async def mark_paid(
    invoice_id: UUID, body: MarkPaid, request: Request, staff: StaffDep, db: Db, now: Now
) -> MarkPaidOut:
    ensure_step_up(staff.session, staff.user, now)
    settings = request.app.state.settings
    org_id = await db.scalar(select(Invoice.org_id).where(Invoice.id == invoice_id))
    if org_id is None:
        raise ApiError("invoice.not_found", 404)
    _in_region(await db.get(Organisation, org_id), settings, "invoice.not_found")
    # The lock order of `billing.advance` (org, subscription, then invoices): no deadlock
    # with the daily job.
    org, _ = await billing.locked_subscription(db, org_id)
    invoice = await db.scalar(
        select(Invoice)
        .where(Invoice.id == invoice_id)
        .options(defer(Invoice.pdf))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if invoice is None:  # unreachable: an invoice is never deleted
        raise ApiError("invoice.not_found", 404)
    issued_on = region_local_date(invoice.issued_at, org.region)
    if not issued_on <= body.paid_on <= region_local_date(now, org.region):
        raise _reject_paid_on()
    if invoice.state is InvoiceState.paid:
        if invoice.payment_reference != body.payment_reference:
            raise ApiError("invoice.already_paid", 409)
    else:
        paid_at = max(region_day_start(body.paid_on, org.region), invoice.issued_at)
        invoice.state = InvoiceState.paid
        invoice.paid_at = min(paid_at, now)
        invoice.payment_reference = body.payment_reference
        await db.flush()
        await audit_org_event(
            db,
            request,
            org_id=org.id,
            actor=staff.user,
            action="billing.invoice_paid",
            target_type="invoice",
            target_id=str(invoice.id),
            data={"number": invoice.number, "amount_ttc": f"{invoice.amount_ttc:.2f}"},
            now=now,
        )
        # The status by the clock and the invoices, stored: with nothing else unpaid past due
        # the subscription is active again, every restriction lifted at once.
        await billing.advance(db, org.id, now, seller=settings)
    await db.commit()
    sub = await billing.subscription_of(db, org.id)
    return MarkPaidOut(
        invoice=_invoice_out(invoice, org, now), subscription=await subscription_out(db, sub, now)
    )


@router.get("/orgs/{org_id}/subscription")
async def get_subscription(org_id: UUID, request: Request, db: Db, now: Now) -> SubscriptionOut:
    org = _in_region(await db.get(Organisation, org_id), _settings(request), "not_found")
    return await subscription_out(db, await billing.subscription_of(db, org.id), now)


async def _unknown() -> None:
    """Staff only (the router's gate runs first): any other path or method."""
    raise ApiError("not_found", 404)


# Last, so the routes above match first; a wrong method on one of them lands here too.
router.add_api_route("", _unknown, methods=ALL_METHODS)
router.add_api_route("/{rest:path}", _unknown, methods=ALL_METHODS)
