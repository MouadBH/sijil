"""Plans, the organisation's subscription and its usage meter (ADM-3).

- `GET /plans`: the active catalogue, prices as strings (MAD, excluding VAT).
- `GET /orgs/{id}/usage`: the meter (`sijil_core.billing.meter`), per quota `used`, `limit`,
  `percent` and `level`. Reading it audits a quota warning met for the first time
  (`billing.check`). Owners and Admins.
- `GET /orgs/{id}/subscription`: Owners and Admins. Its `status` is the one the clock gives
  (`billing.phase`: a trial whose end has passed is over, an invoice unpaid past due makes it
  `overdue`, then `read_only`), with `status_reason` (`org.ice_required`).
- `PUT /orgs/{id}/subscription {plan_code}`: the Owner's, with step-up. An unknown or retired
  plan is 422 `billing.plan_unknown`; an org without an ICE is 422 `org.ice_required`. The
  stored state is first brought up to date (`billing.advance`, the daily job's transition), so
  a trial that ended is never treated as running.
  - In the trial, the plan is the one the first year starts on (the trial keeps
    `essentiel`'s quotas): effective `trial_end`.
  - Otherwise a dearer plan applies at once (effective `now`) with an `upgrade` invoice for
    the price difference times the share of the billing year left (`invoicing.prorata`), a
    cheaper one waits for the renewal as `pending_plan_code` (effective `renewal`, no credit
    note), and the current plan clears a pending change.
  - A change is audited `billing.plan_chosen` `{from, to, effective}`.
- `GET /orgs/{id}/invoices`, `GET /orgs/{id}/invoices/{inv}` and `GET …/{inv}/pdf` (the PDF
  stored at issue, never rendered again): Owners and Admins; another org's invoice, or none,
  is 404 `invoice.not_found`. Amounts are strings.
"""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, Field, field_serializer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer

from sijil_core import billing, invoicing
from sijil_core.db.models import (
    Invoice,
    InvoiceKind,
    InvoiceState,
    Locale,
    Plan,
    Subscription,
    SubscriptionStatus,
    User,
    UserSession,
)
from sijil_cp.deps import (
    current_session,
    ensure_step_up,
    get_db,
    get_now,
    require_totp_compliance,
)
from sijil_cp.errors import ApiError
from sijil_cp.org_audit import audit_org_event
from sijil_cp.permissions import Action, authorize_org

router = APIRouter(tags=["billing"], dependencies=[Depends(require_totp_compliance)])

Db = Annotated[AsyncSession, Depends(get_db)]
CurrentUser = Annotated[User, Depends(require_totp_compliance)]
Session = Annotated[UserSession, Depends(current_session())]
Now = Annotated[datetime, Depends(get_now)]


class PlanOut(BaseModel):
    code: str
    price_ht: Decimal
    max_users: int
    max_documents_year: int
    max_storage_bytes: int
    max_ai_pages_year: int

    @field_serializer("price_ht")
    def _money(self, value: Decimal) -> str:
        return f"{value:.2f}"


class SubscriptionOut(BaseModel):
    plan_code: str
    status: SubscriptionStatus
    status_reason: str | None
    trial_ends_at: datetime
    period_start: datetime
    period_end: datetime
    pending_plan_code: str | None
    plan: PlanOut


class QuotaOut(BaseModel):
    used: int
    limit: int
    percent: int
    level: billing.QuotaLevel


class UsageOut(BaseModel):
    plan_code: str
    status: SubscriptionStatus
    period_start: datetime
    period_end: datetime
    quotas: dict[str, QuotaOut]


class InvoiceOut(BaseModel):
    id: UUID
    number: str
    kind: InvoiceKind
    state: InvoiceState
    plan_code: str
    period_start: datetime
    period_end: datetime
    amount_ht: Decimal
    vat_rate: Decimal
    amount_vat: Decimal
    amount_ttc: Decimal
    currency: str
    issued_at: datetime
    due_at: datetime
    paid_at: datetime | None
    locale: Locale
    buyer: dict[str, Any]
    seller: dict[str, Any]
    pdf_sha256: str

    @field_serializer("amount_ht", "vat_rate", "amount_vat", "amount_ttc")
    def _money(self, value: Decimal) -> str:
        return f"{value:.2f}"


class PlanChoice(BaseModel):
    plan_code: str = Field(min_length=1, max_length=32)


def _plan_out(plan: Plan) -> PlanOut:
    return PlanOut(
        code=plan.code,
        price_ht=plan.price_ht,
        max_users=plan.max_users,
        max_documents_year=plan.max_documents_year,
        max_storage_bytes=plan.max_storage_bytes,
        max_ai_pages_year=plan.max_ai_pages_year,
    )


async def _subscription_out(db: AsyncSession, sub: Subscription, now: datetime) -> SubscriptionOut:
    plan = await db.get(Plan, sub.plan_code)
    if plan is None:  # unreachable: the foreign key guarantees the plan
        raise ApiError("not_found", 404)
    current = await billing.phase_of(db, sub, now)
    return SubscriptionOut(
        plan_code=sub.plan_code,
        status=current.status,
        status_reason=current.reason,
        trial_ends_at=sub.trial_ends_at,
        period_start=sub.period_start,
        period_end=sub.period_end,
        pending_plan_code=sub.pending_plan_code,
        plan=_plan_out(plan),
    )


@router.get("/plans")
async def list_plans(db: Db, _: CurrentUser) -> list[PlanOut]:
    plans = await db.scalars(select(Plan).where(Plan.active).order_by(Plan.price_ht))
    return [_plan_out(p) for p in plans]


@router.get("/orgs/{org_id}/usage")
async def get_usage(
    org_id: UUID, request: Request, user: CurrentUser, db: Db, now: Now
) -> UsageOut:
    await authorize_org(db, user.id, org_id, Action.billing_read, request.app.state.settings)
    meter = await billing.check(db, org_id, now)
    await db.commit()
    return UsageOut(
        plan_code=meter.plan_code,
        status=meter.status,
        period_start=meter.period_start,
        period_end=meter.period_end,
        quotas={
            kind: QuotaOut(used=q.used, limit=q.limit, percent=q.percent, level=q.level)
            for kind, q in meter.quotas.items()
        },
    )


@router.get("/orgs/{org_id}/subscription")
async def get_subscription(
    org_id: UUID, request: Request, user: CurrentUser, db: Db, now: Now
) -> SubscriptionOut:
    await authorize_org(db, user.id, org_id, Action.billing_read, request.app.state.settings)
    return await _subscription_out(db, await billing.subscription_of(db, org_id), now)


@router.put("/orgs/{org_id}/subscription")
async def choose_plan(
    org_id: UUID,
    body: PlanChoice,
    request: Request,
    user: CurrentUser,
    db: Db,
    session: Session,
    now: Now,
) -> SubscriptionOut:
    org, _ = await authorize_org(
        db, user.id, org_id, Action.billing_manage, request.app.state.settings
    )
    plan = await db.get(Plan, body.plan_code)
    if plan is None or not plan.active:
        raise ApiError("billing.plan_unknown", 422)
    if org.ice is None:
        raise ApiError("org.ice_required", 422)
    ensure_step_up(session, user, now)
    settings = request.app.state.settings
    current_phase = await billing.advance(db, org_id, now, seller=settings)
    org, sub = await billing.locked_subscription(db, org_id)
    previous = sub.plan_code
    if current_phase.status is SubscriptionStatus.trial:
        changed, effective = plan.code != sub.plan_code, "trial_end"
        sub.plan_code = plan.code
    else:
        current = await db.get(Plan, sub.plan_code)
        if current is None:  # unreachable: the foreign key guarantees the plan
            raise ApiError("not_found", 404)
        if plan.code == current.code:
            changed, effective = sub.pending_plan_code is not None, "now"
            sub.pending_plan_code = None
        elif plan.price_ht > current.price_ht:
            changed, effective = True, "now"
            await invoicing.issue(
                db,
                org,
                InvoiceKind.upgrade,
                plan,
                (now, sub.period_end),
                now,
                seller=settings,
                price_ht=plan.price_ht - current.price_ht,
                fraction=invoicing.prorata(sub.period_start, sub.period_end, now, org.region),
            )
            sub.plan_code, sub.pending_plan_code = plan.code, None
        else:
            changed, effective = plan.code != sub.pending_plan_code, "renewal"
            sub.pending_plan_code = plan.code
    if changed:
        await audit_org_event(
            db,
            request,
            org_id=org_id,
            actor=user,
            action="billing.plan_chosen",
            target_type="org",
            target_id=str(org_id),
            data={"from": previous, "to": plan.code, "effective": effective},
            now=now,
        )
    await db.commit()
    return await _subscription_out(db, sub, now)


def _invoice_out(row: Invoice) -> InvoiceOut:
    return InvoiceOut(
        id=row.id,
        number=row.number,
        kind=row.kind,
        state=row.state,
        plan_code=row.plan_code,
        period_start=row.period_start,
        period_end=row.period_end,
        amount_ht=row.amount_ht,
        vat_rate=row.vat_rate,
        amount_vat=row.amount_vat,
        amount_ttc=row.amount_ttc,
        currency=row.currency,
        issued_at=row.issued_at,
        due_at=row.due_at,
        paid_at=row.paid_at,
        locale=row.locale,
        buyer=row.buyer,
        seller=row.seller,
        pdf_sha256=row.pdf_sha256,
    )


async def _invoice(db: AsyncSession, org_id: UUID, invoice_id: UUID, *, pdf: bool) -> Invoice:
    query = select(Invoice).where(Invoice.id == invoice_id, Invoice.org_id == org_id)
    if not pdf:
        query = query.options(defer(Invoice.pdf))
    row = await db.scalar(query)
    if row is None:
        raise ApiError("invoice.not_found", 404)
    return row


@router.get("/orgs/{org_id}/invoices")
async def list_invoices(
    org_id: UUID, request: Request, user: CurrentUser, db: Db
) -> list[InvoiceOut]:
    await authorize_org(db, user.id, org_id, Action.billing_read, request.app.state.settings)
    rows = await db.scalars(
        select(Invoice)
        .where(Invoice.org_id == org_id)
        .options(defer(Invoice.pdf))
        .order_by(Invoice.issued_at.desc(), Invoice.number.desc())
    )
    return [_invoice_out(r) for r in rows]


@router.get("/orgs/{org_id}/invoices/{invoice_id}")
async def get_invoice(
    org_id: UUID, invoice_id: UUID, request: Request, user: CurrentUser, db: Db
) -> InvoiceOut:
    await authorize_org(db, user.id, org_id, Action.billing_read, request.app.state.settings)
    return _invoice_out(await _invoice(db, org_id, invoice_id, pdf=False))


@router.get("/orgs/{org_id}/invoices/{invoice_id}/pdf")
async def get_invoice_pdf(
    org_id: UUID, invoice_id: UUID, request: Request, user: CurrentUser, db: Db
) -> Response:
    await authorize_org(db, user.id, org_id, Action.billing_read, request.app.state.settings)
    row = await _invoice(db, org_id, invoice_id, pdf=True)
    return Response(
        content=row.pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{row.number}.pdf"'},
    )
