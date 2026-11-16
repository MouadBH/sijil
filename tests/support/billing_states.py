"""Put an organisation in a non-payment state the way production gets there: its trial ended in
the past, `billing.advance` issued the first annual invoice then, and nobody paid it.

- `overdue`: the invoice is 15 days past due at `now`;
- `read_only`: 45 days past due (read-only comes 30 days after the due date).
"""

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import billing
from sijil_core.db.models import Organisation, Subscription, SubscriptionStatus
from tests.support.seller import sample_seller

SAMPLE_ORG_ICE = "002999888000011"  # SAMPLE: a fictitious company's ICE
_PAST_DUE = {SubscriptionStatus.overdue: 15, SubscriptionStatus.read_only: 45}


async def make_paid_up(db: AsyncSession, org_id: UUID, start: datetime) -> None:
    """An active subscription, its first year from `start`, nothing owed: for tests that run
    their clock past the trial's end. Commits."""
    sub = await db.scalar(select(Subscription).where(Subscription.org_id == org_id))
    if sub is None:
        sub = billing.new_trial(org_id, start - timedelta(days=billing.TRIAL_DAYS))
        db.add(sub)
    sub.status, sub.trial_ends_at = SubscriptionStatus.active, start
    sub.period_start, sub.period_end = start, billing.add_year(start)
    await db.commit()


async def make_late(
    db: AsyncSession, org_id: UUID, now: datetime, status: SubscriptionStatus
) -> None:
    """See the module docstring. Commits."""
    org = await db.get(Organisation, org_id, populate_existing=True)
    assert org is not None
    if org.ice is None:
        org.ice = SAMPLE_ORG_ICE
    issued_at = now - timedelta(days=30 + _PAST_DUE[status])
    sub = await db.scalar(select(Subscription).where(Subscription.org_id == org_id))
    if sub is None:
        sub = billing.new_trial(org_id, issued_at - timedelta(days=billing.TRIAL_DAYS))
        db.add(sub)
    sub.status, sub.plan_code, sub.pending_plan_code = SubscriptionStatus.trial, "essentiel", None
    sub.trial_ends_at = issued_at
    sub.period_start, sub.period_end = issued_at - timedelta(days=billing.TRIAL_DAYS), issued_at
    await db.flush()
    seller = sample_seller()
    await billing.advance(db, org_id, issued_at, seller=seller)
    assert (await billing.advance(db, org_id, now, seller=seller)).status is status
    await db.commit()
