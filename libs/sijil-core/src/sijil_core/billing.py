"""Plans, subscriptions, the usage meter and what an organisation may do (ADM-3, design §5.7a).

Billing is per organisation, in the control-plane database of its region.

- **Subscription.** A new organisation starts a TRIAL_DAYS trial on TRIAL_PLAN's quotas
  (`new_trial`). The billing year is `[period_start, period_end)`: during the trial it is the
  trial itself, and the first year starts at `trial_ends_at` (`billing_window`, which also
  rolls a stale period forward a year at a time until the daily job moves it). An
  organisation with no row yet (one created before its subscription was) has an implied
  trial from its creation (`subscription_of`).
- **Meter** (`meter`), each quota with `used`, `limit`, `percent` (integer, floor) and `level`
  (`ok` below 80%, `warn_80` from 80%, `over_100` from 100%):
  - `users`: the distinct users with a membership in the org or in one of its workspaces;
  - `documents`: ledger rows of its workspaces ingested in the billing year (a rejected
    duplicate never makes a row);
  - `storage`: the bytes of its originals not yet purged (`original_removed_at IS NULL`);
  - `ai_pages`: the `UsageRecord` pages of the billing year.
- **Phase** (`phase`): the subscription's status by the clock and the invoices, never by a
  stored status alone, so the daily job and every request agree even before the job has run:
  - `trial` until `trial_ends_at` (a trial whose end has passed counts as ended everywhere);
  - a trial that ended with no ICE to invoice: `overdue`, reason `org.ice_required`, and
    `read_only` OVERDUE_GRACE after the trial's end;
  - an invoice unpaid past its `due_at`: `overdue`, and `read_only` OVERDUE_GRACE after it;
  - otherwise `active`.
- **Advance** (`advance`: the daily job `sijil.billing_daily`, and the request paths that need
  the stored state current): issues the first annual invoice once the trial has ended (with an
  ICE), the renewal RENEWAL_NOTICE before `period_end` (on `pending_plan_code` when there is
  one), rolls the billing year on at `period_end` (the plan the new year is invoiced on then
  applies: its renewal's, or an upgrade's issued after it), marks
  unpaid invoices past due `overdue`, and stores the phase. While the org is closing or closed
  (ADM-5, ruling R4) no invoice is issued, neither the first annual one nor a renewal, and the
  billing year does not roll: a cancel then issues what was skipped. Each
  transition is audited once:
  `billing.invoice_issued`, `billing.overdue` (`{number}` for an invoice, `{reason}` for the
  missing ICE) and `billing.read_only`.
- **Standing** (`standing`): what the organisation may do. Over quota (any of users,
  documents and storage beyond its limit, or no AI page left) AI extraction stops and no user
  may be added; at the user limit only new users stop. `overdue` also stops exports
  (`billing.overdue`), and `read_only` every state-changing workspace route but capture
  (`billing.read_only`); a document that skips AI for either is flagged `billing_overdue`.
  Capture and reads are never gated here or anywhere (§5.7a). A `closed` subscription (a
  hibernated account, ADM-5, ruling R5) refuses every write (`billing.read_only`), AI and new
  users, and keeps the Owner's exports open.
- **Warnings** (`check`): the first time a quota reaches 80% or 100% in a billing year,
  `quota.warning_80` / `quota.warning_100` `{kind}` is audited on every workspace of the org.
  A `quota_warning` row, unique per (org, kind, threshold, period start), makes it once
  under any concurrency: the audit is appended only by the transaction whose insert won.
- **AI pages** (`record_ai_pages`): written by the extraction worker once per document,
  before it calls the model; unique per (ledger_id, kind), so a retry adds nothing.

Money is `Decimal` (`Plan.price_ht`, numeric(14,2)); no float anywhere.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Final
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import invoicing
from sijil_core.closure import open_closure
from sijil_core.db.models import (
    DocumentLedger,
    Invoice,
    InvoiceKind,
    InvoiceState,
    Membership,
    Organisation,
    OrgStatus,
    Plan,
    QuotaWarning,
    Subscription,
    SubscriptionStatus,
    UsageRecord,
    Workspace,
)
from sijil_core.settings import SellerSettings

TRIAL_DAYS: Final = 30
TRIAL_PLAN: Final = "essentiel"
AI_PAGES: Final = "ai_pages"  # the one `UsageRecord.kind`
QUOTA_KINDS: Final = ("users", "documents", "storage", "ai_pages")
WARN_PERCENT: Final = 80
# The `reason_code` of a standing refused by a quota, and the flag of a document that skipped
# AI for it; `billing_overdue` when it was an unpaid invoice (or the missing ICE).
QUOTA_EXCEEDED: Final = "quota_exceeded"
BILLING_OVERDUE: Final = "billing_overdue"
# The API codes (402) of a standing refused by non-payment.
OVERDUE_CODE: Final = "billing.overdue"
READ_ONLY_CODE: Final = "billing.read_only"
# Why a trial that ended is overdue with no invoice (`Subscription.status_reason`).
ICE_REQUIRED: Final = "org.ice_required"
# Overdue for this long, the organisation becomes read-only (plus capture).
OVERDUE_GRACE: Final = timedelta(days=30)
# The renewal invoice is issued this long before the billing year ends.
RENEWAL_NOTICE: Final = timedelta(days=30)
UNPAID: Final = (InvoiceState.issued, InvoiceState.overdue)


class QuotaLevel(StrEnum):
    ok = "ok"
    warn_80 = "warn_80"
    over_100 = "over_100"


@dataclass(frozen=True)
class Quota:
    kind: str
    used: int
    limit: int
    percent: int
    level: QuotaLevel


@dataclass(frozen=True)
class Phase:
    status: SubscriptionStatus
    reason: str | None = None  # ICE_REQUIRED
    invoice: str | None = None  # the number of the oldest invoice unpaid past due


@dataclass(frozen=True)
class Meter:
    org_id: UUID
    plan_code: str  # the plan whose quotas apply now (the trial's during the trial)
    status: SubscriptionStatus  # by the clock (`phase`)
    period_start: datetime
    period_end: datetime
    quotas: dict[str, Quota]
    status_reason: str | None = None


@dataclass(frozen=True)
class Standing:
    ai_allowed: bool
    new_users_allowed: bool
    export_allowed: bool
    writes_allowed: bool
    reason_code: str | None
    # OVERDUE_CODE or READ_ONLY_CODE when non-payment refuses something (402).
    billing_code: str | None = None


def quota(kind: str, used: int, limit: int) -> Quota:
    """Integer arithmetic only: `percent` is floored, the levels compare exact products."""
    if used >= limit:
        level = QuotaLevel.over_100
    elif used * 100 >= WARN_PERCENT * limit:
        level = QuotaLevel.warn_80
    else:
        level = QuotaLevel.ok
    return Quota(kind=kind, used=used, limit=limit, percent=used * 100 // limit, level=level)


def new_trial(org_id: UUID, now: datetime) -> Subscription:
    """The subscription of a new organisation: a trial on TRIAL_PLAN, from `now`."""
    ends = now + timedelta(days=TRIAL_DAYS)
    return Subscription(
        org_id=org_id,
        plan_code=TRIAL_PLAN,
        status=SubscriptionStatus.trial,
        trial_ends_at=ends,
        period_start=now,
        period_end=ends,
        created_at=now,
    )


def add_year(at: datetime) -> datetime:
    """The same instant a year later; 29 February becomes 28 February."""
    try:
        return at.replace(year=at.year + 1)
    except ValueError:
        return at.replace(year=at.year + 1, day=28)


def billing_window(sub: Subscription, now: datetime) -> tuple[datetime, datetime]:
    """The billing year holding `now`: the stored period, rolled forward a year at a time once
    it is over (the first year starts at the end of the trial)."""
    start, end = sub.period_start, sub.period_end
    while now >= end:
        start, end = end, add_year(end)
    return start, end


def quota_plan(sub: Subscription, now: datetime) -> str:
    """The plan whose quotas apply: the trial's during the trial, else the chosen one."""
    if sub.status is SubscriptionStatus.trial and now < sub.trial_ends_at:
        return TRIAL_PLAN
    return sub.plan_code


async def subscription_of(db: AsyncSession, org_id: UUID) -> Subscription:
    """The org's subscription, or (none stored yet) an implied trial from its creation that
    is not added to the session."""
    found = await db.scalar(select(Subscription).where(Subscription.org_id == org_id))
    if found is not None:
        return found
    org = await db.get(Organisation, org_id)
    if org is None:
        raise LookupError("organisation not found")
    return new_trial(org_id, org.created_at)


def phase(
    sub: Subscription,
    *,
    has_ice: bool,
    unpaid: tuple[str, datetime] | None,
    now: datetime,
) -> Phase:
    """The status by the clock (see the module docstring). `unpaid` is the number and due date
    of the org's oldest unpaid invoice."""
    if sub.status is SubscriptionStatus.closed:
        return Phase(SubscriptionStatus.closed)
    if sub.status is SubscriptionStatus.trial and now < sub.trial_ends_at:
        return Phase(SubscriptionStatus.trial)

    def late(since: datetime) -> SubscriptionStatus:
        if now >= since + OVERDUE_GRACE:
            return SubscriptionStatus.read_only
        return SubscriptionStatus.overdue

    first_year_due = sub.period_end <= sub.trial_ends_at <= now
    if first_year_due and not has_ice:
        return Phase(late(sub.trial_ends_at), reason=ICE_REQUIRED)
    if unpaid is not None and now >= unpaid[1]:
        return Phase(late(unpaid[1]), invoice=unpaid[0])
    return Phase(SubscriptionStatus.active)


async def _oldest_unpaid(db: AsyncSession, org_id: UUID) -> tuple[str, datetime] | None:
    row = (
        await db.execute(
            select(Invoice.number, Invoice.due_at)
            .where(Invoice.org_id == org_id, Invoice.state.in_(UNPAID))
            .order_by(Invoice.due_at, Invoice.number)
            .limit(1)
        )
    ).first()
    return None if row is None else (row.number, row.due_at)


async def phase_of(db: AsyncSession, sub: Subscription, now: datetime) -> Phase:
    """`phase` with the org's ICE and oldest unpaid invoice read from the database."""
    ice = await db.scalar(select(Organisation.ice).where(Organisation.id == sub.org_id))
    unpaid = await _oldest_unpaid(db, sub.org_id)
    return phase(sub, has_ice=ice is not None, unpaid=unpaid, now=now)


async def _plan(db: AsyncSession, code: str) -> Plan:
    plan = await db.get(Plan, code)
    if plan is None:
        raise LookupError(f"plan {code} not found")
    return plan


async def meter(db: AsyncSession, org_id: UUID, now: datetime) -> Meter:
    """The usage of every quota in the billing year holding `now`. Reads only."""
    sub = await subscription_of(db, org_id)
    current = await phase_of(db, sub, now)
    start, end = billing_window(sub, now)
    plan = await _plan(db, quota_plan(sub, now))
    workspaces = select(Workspace.id).where(Workspace.org_id == org_id)
    users = await db.scalar(
        select(func.count(func.distinct(Membership.user_id))).where(
            or_(Membership.org_id == org_id, Membership.workspace_id.in_(workspaces))
        )
    )
    documents = await db.scalar(
        select(func.count()).where(
            DocumentLedger.workspace_id.in_(workspaces),
            DocumentLedger.ingested_at >= start,
            DocumentLedger.ingested_at < end,
        )
    )
    storage = await db.scalar(
        select(func.coalesce(func.sum(DocumentLedger.size_bytes), 0)).where(
            DocumentLedger.workspace_id.in_(workspaces),
            DocumentLedger.original_removed_at.is_(None),
        )
    )
    pages = await db.scalar(
        select(func.coalesce(func.sum(UsageRecord.quantity), 0)).where(
            UsageRecord.org_id == org_id,
            UsageRecord.kind == AI_PAGES,
            UsageRecord.at >= start,
            UsageRecord.at < end,
        )
    )
    quotas = [
        quota("users", int(users or 0), plan.max_users),
        quota("documents", int(documents or 0), plan.max_documents_year),
        quota("storage", int(storage or 0), plan.max_storage_bytes),
        quota("ai_pages", int(pages or 0), plan.max_ai_pages_year),
    ]
    return Meter(
        org_id=org_id,
        plan_code=plan.code,
        status=current.status,
        period_start=start,
        period_end=end,
        quotas={q.kind: q for q in quotas},
        status_reason=current.reason,
    )


def exceeded(m: Meter) -> list[str]:
    """The quotas that stop AI and new users: users, documents or storage beyond their limit,
    or no AI page left."""
    found = [k for k in ("users", "documents", "storage") if m.quotas[k].used > m.quotas[k].limit]
    pages = m.quotas["ai_pages"]
    if pages.used >= pages.limit:
        found.append("ai_pages")
    return found


def standing_of(m: Meter) -> Standing:
    if m.status is SubscriptionStatus.closed:
        return Standing(
            ai_allowed=False,
            new_users_allowed=False,
            export_allowed=True,
            writes_allowed=False,
            reason_code=None,
            billing_code=READ_ONLY_CODE,
        )
    if m.status in (SubscriptionStatus.overdue, SubscriptionStatus.read_only):
        read_only = m.status is SubscriptionStatus.read_only
        return Standing(
            ai_allowed=False,
            new_users_allowed=False,
            export_allowed=False,
            writes_allowed=not read_only,
            reason_code=BILLING_OVERDUE,
            billing_code=READ_ONLY_CODE if read_only else OVERDUE_CODE,
        )
    over = bool(exceeded(m))
    users = m.quotas["users"]
    new_users = not over and users.used < users.limit
    return Standing(
        ai_allowed=not over,
        new_users_allowed=new_users,
        export_allowed=True,
        writes_allowed=True,
        reason_code=QUOTA_EXCEEDED if over or not new_users else None,
    )


async def standing(db: AsyncSession, org_id: UUID, now: datetime) -> Standing:
    """What the organisation may do now (see the module docstring). Reads only."""
    return standing_of(await meter(db, org_id, now))


async def _audit_org(
    db: AsyncSession, org_id: UUID, action: str, data: dict[str, str], now: datetime
) -> None:
    await invoicing.audit_org(db, org_id, action, data, now)


async def record_warnings(db: AsyncSession, m: Meter, now: datetime) -> list[tuple[str, int]]:
    """Audits each warning the meter reaches for the first time in its billing year, in the
    caller's transaction (the caller commits); returns the (kind, threshold) given now."""
    given: list[tuple[str, int]] = []
    for kind in QUOTA_KINDS:
        q = m.quotas[kind]
        reached = [t for t in (WARN_PERCENT, 100) if q.used * 100 >= t * q.limit]
        for threshold in reached:
            won = await db.scalar(
                insert(QuotaWarning)
                .values(
                    org_id=m.org_id,
                    kind=kind,
                    threshold=threshold,
                    period_start=m.period_start,
                    at=now,
                )
                .on_conflict_do_nothing(constraint="uq_quota_warning_once")
                .returning(QuotaWarning.id)
            )
            if won is not None:
                await _audit_org(db, m.org_id, f"quota.warning_{threshold}", {"kind": kind}, now)
                given.append((kind, threshold))
    return given


async def check(db: AsyncSession, org_id: UUID, now: datetime) -> Meter:
    """The meter, with its first-time warnings audited in the caller's transaction."""
    m = await meter(db, org_id, now)
    await record_warnings(db, m, now)
    return m


async def ai_pages_recorded(db: AsyncSession, ledger_id: UUID) -> bool:
    found = await db.scalar(
        select(UsageRecord.id).where(
            UsageRecord.ledger_id == ledger_id, UsageRecord.kind == AI_PAGES
        )
    )
    return found is not None


async def record_ai_pages(
    db: AsyncSession,
    *,
    org_id: UUID,
    workspace_id: UUID,
    ledger_id: UUID,
    pages: int,
    now: datetime,
) -> bool:
    """Records the document's AI pages (at least 1), once: False when it already has them.
    In the caller's transaction."""
    won = await db.scalar(
        insert(UsageRecord)
        .values(
            org_id=org_id,
            workspace_id=workspace_id,
            ledger_id=ledger_id,
            kind=AI_PAGES,
            quantity=max(1, pages),
            at=now,
        )
        .on_conflict_do_nothing(constraint="uq_usage_record_ledger_kind")
        .returning(UsageRecord.id)
    )
    return won is not None


# Transitions ----------------------------------------------------------------------------------


async def locked_subscription(db: AsyncSession, org_id: UUID) -> tuple[Organisation, Subscription]:
    """The org and its subscription, both FOR UPDATE until the caller's commit; the implied
    trial is stored if the org had no row yet."""
    org = await db.scalar(
        select(Organisation)
        .where(Organisation.id == org_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if org is None:
        raise LookupError("organisation not found")
    sub = await db.scalar(
        select(Subscription)
        .where(Subscription.org_id == org_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if sub is None:
        sub = new_trial(org.id, org.created_at)
        db.add(sub)
        await db.flush()
    return org, sub


async def _billed_plan(db: AsyncSession, org_id: UUID, start: datetime) -> str | None:
    """The plan the billing year starting at `start` is invoiced on: that of its last invoice
    (its renewal, or an upgrade issued after it), or None when nothing is issued for it."""
    found: str | None = await db.scalar(
        select(Invoice.plan_code)
        .where(Invoice.org_id == org_id, Invoice.period_start == start)
        .order_by(Invoice.issued_at.desc(), Invoice.number.desc())
        .limit(1)
    )
    return found


async def _annual_for(db: AsyncSession, org_id: UUID, start: datetime) -> Invoice | None:
    """The annual invoice of the billing year starting at `start`, if issued."""
    found: Invoice | None = await db.scalar(
        select(Invoice).where(
            Invoice.org_id == org_id,
            Invoice.kind == InvoiceKind.annual,
            Invoice.period_start == start,
        )
    )
    return found


async def advance(
    db: AsyncSession, org_id: UUID, now: datetime, *, seller: SellerSettings
) -> Phase:
    """Brings the org's stored billing state up to `now` (see the module docstring), in the
    caller's transaction (the caller commits). Idempotent: a second run at the same time
    issues, marks and audits nothing more. Returns the phase it stored."""
    org, sub = await locked_subscription(db, org_id)
    if sub.status is SubscriptionStatus.closed:
        return Phase(SubscriptionStatus.closed)
    # Ruling R4: an account being closed is never invoiced again.
    invoicing_open = org.status is OrgStatus.active
    if invoicing_open and org.ice is not None and sub.period_end <= sub.trial_ends_at <= now:
        start, end = sub.trial_ends_at, add_year(sub.trial_ends_at)
        if await _annual_for(db, org.id, start) is None:
            plan = await _plan(db, sub.plan_code)
            await invoicing.issue(
                db, org, InvoiceKind.annual, plan, (start, end), now, seller=seller
            )
        sub.period_start, sub.period_end = start, end
    while sub.period_end > sub.trial_ends_at:  # the first billing year has started
        renewal = await _annual_for(db, org.id, sub.period_end)
        due = now >= sub.period_end - RENEWAL_NOTICE
        if renewal is None and invoicing_open and org.ice is not None and due:
            plan = await _plan(db, sub.pending_plan_code or sub.plan_code)
            period = (sub.period_end, add_year(sub.period_end))
            renewal = await invoicing.issue(
                db, org, InvoiceKind.annual, plan, period, now, seller=seller
            )
        # Nor does the year roll while the org is closing: a cancel then issues the renewal it
        # skipped, and the year rolls onto it (an unrolled year is never left unbilled).
        if now < sub.period_end or not invoicing_open:
            break
        # The new year runs on the plan it is invoiced on (an upgrade issued after its renewal
        # included). A pending downgrade that this renewal already carries is done; one chosen
        # after it was issued waits for the following renewal.
        billed = await _billed_plan(db, org.id, sub.period_end)
        if billed is not None:
            sub.plan_code = billed
            if sub.pending_plan_code == billed:
                sub.pending_plan_code = None
        elif sub.pending_plan_code is not None:
            sub.plan_code, sub.pending_plan_code = sub.pending_plan_code, None
        sub.period_start, sub.period_end = sub.period_end, add_year(sub.period_end)
    late = await db.scalars(
        select(Invoice)
        .where(
            Invoice.org_id == org.id,
            Invoice.state == InvoiceState.issued,
            Invoice.due_at <= now,
        )
        .order_by(Invoice.due_at, Invoice.number)
        .with_for_update()
    )
    for invoice in list(late):
        invoice.state = InvoiceState.overdue
        await _audit_org(db, org.id, "billing.overdue", {"number": invoice.number}, now)
    await db.flush()
    current = await phase_of(db, sub, now)
    if current.status is not sub.status:
        why = {"reason": current.reason} if current.reason else {"number": current.invoice or ""}
        if current.status is SubscriptionStatus.read_only:
            await _audit_org(db, org.id, "billing.read_only", why, now)
        elif current.status is SubscriptionStatus.overdue and current.reason is not None:
            await _audit_org(db, org.id, "billing.overdue", why, now)
        sub.status = current.status
    sub.status_reason = current.reason
    await db.flush()
    return current


class ClosureInProgress(Exception):  # noqa: N818 - named for the machine code it maps to
    """The org has an open closure: no plan may be chosen (`closure.in_progress`)."""


@dataclass(frozen=True)
class PlanChange:
    previous: str  # the plan in force before
    changed: bool
    # `trial_end`, `now`, `renewal` (the next renewal invoice is on the new plan) or
    # `following_renewal` (the next year is already invoiced: the renewal after it).
    effective: str


async def change_plan(
    db: AsyncSession, org_id: UUID, plan: Plan, now: datetime, *, seller: SellerSettings
) -> PlanChange:
    """The Owner's choice of plan (`PUT /orgs/{id}/subscription`), after `advance`, in the
    caller's transaction (the caller audits and commits).

    Raises `ClosureInProgress`, having changed nothing, while the org has an open closure
    (grace, hibernated or destroyed; ruling I2): an account being closed is never invoiced
    (R4), and its stored period may no longer hold `now`. Checked under the org row lock,
    which a closure request takes too, so the two never interleave; a cancelled closure
    restores the choice."""
    await locked_subscription(db, org_id)
    if await open_closure(db, org_id) is not None:
        raise ClosureInProgress
    current_phase = await advance(db, org_id, now, seller=seller)
    org, sub = await locked_subscription(db, org_id)
    previous = sub.plan_code
    if current_phase.status is SubscriptionStatus.trial:
        sub.plan_code = plan.code
        return PlanChange(previous, plan.code != previous, "trial_end")
    current = await _plan(db, sub.plan_code)

    async def bill_next_year() -> bool:
        """When the next year is already invoiced (its renewal is issued: the last
        RENEWAL_NOTICE of the year) on a plan cheaper than the one chosen, invoices that
        year's difference in full, so it is billed, and then runs, on the chosen plan. An
        issued invoice is never altered. Returns whether the next year is now billed on it."""
        billed_code = await _billed_plan(db, org.id, sub.period_end)
        if billed_code is None:
            return False
        billed = await _plan(db, billed_code)
        if plan.price_ht > billed.price_ht:
            await invoicing.issue(
                db,
                org,
                InvoiceKind.upgrade,
                plan,
                (sub.period_end, add_year(sub.period_end)),
                now,
                seller=seller,
                price_ht=plan.price_ht - billed.price_ht,
            )
            return True
        return billed.code == plan.code

    if plan.code == current.code:
        # Keeping the plan in force (or cancelling a pending downgrade): it stays after the
        # roll, the next year billed at its price.
        corrected = await bill_next_year()
        changed = sub.pending_plan_code is not None or corrected
        sub.pending_plan_code = None
        return PlanChange(previous, changed, "now")
    if plan.price_ht > current.price_ht:
        await invoicing.issue(
            db,
            org,
            InvoiceKind.upgrade,
            plan,
            (now, sub.period_end),
            now,
            seller=seller,
            price_ht=plan.price_ht - current.price_ht,
            fraction=invoicing.prorata(sub.period_start, sub.period_end, now, org.region),
        )
        await bill_next_year()
        sub.plan_code, sub.pending_plan_code = plan.code, None
        return PlanChange(previous, True, "now")
    # A cheaper plan waits: no credit note. It applies at the coming roll when the next year is
    # (now) billed on it, or has no invoice yet; otherwise at the renewal after it.
    issued = await _billed_plan(db, org.id, sub.period_end) is not None
    at_roll = await bill_next_year() or not issued
    changed = plan.code != sub.pending_plan_code
    sub.pending_plan_code = plan.code
    return PlanChange(previous, changed, "renewal" if at_roll else "following_renewal")
