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
- **Standing** (`standing`): what the organisation may do. Over quota (any of users,
  documents and storage beyond its limit, or no AI page left) AI extraction stops and no user
  may be added; at the user limit only new users stop. Capture and reads are never gated
  here or anywhere (§5.7a). This task covers `trial` and `active` only (ruling R1): Task 3
  maps `overdue` and `read_only`.
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

from sijil_core import audit
from sijil_core.db.models import (
    ActorType,
    DocumentLedger,
    Membership,
    Organisation,
    Plan,
    QuotaWarning,
    Subscription,
    SubscriptionStatus,
    UsageRecord,
    Workspace,
)

TRIAL_DAYS: Final = 30
TRIAL_PLAN: Final = "essentiel"
AI_PAGES: Final = "ai_pages"  # the one `UsageRecord.kind`
QUOTA_KINDS: Final = ("users", "documents", "storage", "ai_pages")
WARN_PERCENT: Final = 80
# The `reason_code` of a standing refused by a quota, and the flag of a document that skipped
# AI for it (Task 3 adds `billing_overdue`).
QUOTA_EXCEEDED: Final = "quota_exceeded"


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
class Meter:
    org_id: UUID
    plan_code: str  # the plan whose quotas apply now (the trial's during the trial)
    status: SubscriptionStatus
    period_start: datetime
    period_end: datetime
    quotas: dict[str, Quota]


@dataclass(frozen=True)
class Standing:
    ai_allowed: bool
    new_users_allowed: bool
    export_allowed: bool
    writes_allowed: bool
    reason_code: str | None


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


async def _plan(db: AsyncSession, code: str) -> Plan:
    plan = await db.get(Plan, code)
    if plan is None:
        raise LookupError(f"plan {code} not found")
    return plan


async def meter(db: AsyncSession, org_id: UUID, now: datetime) -> Meter:
    """The usage of every quota in the billing year holding `now`. Reads only."""
    sub = await subscription_of(db, org_id)
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
        status=sub.status,
        period_start=start,
        period_end=end,
        quotas={q.kind: q for q in quotas},
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
    ids = await db.scalars(
        select(Workspace.id).where(Workspace.org_id == org_id).order_by(Workspace.id)
    )
    for workspace_id in list(ids):
        await audit.append(
            db,
            workspace_id=workspace_id,
            action=action,
            actor_type=ActorType.system,
            actor_id=None,
            target_type="org",
            target_id=str(org_id),
            data=data,
            now=now,
        )


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
