"""Put an organisation in a non-payment state the way production gets there: its trial ended in
the past, `billing.advance` issued the first annual invoice then, and nobody paid it.

- `overdue`: the invoice is 15 days past due at `now`;
- `read_only`: 45 days past due (read-only comes 30 days after the due date).
"""

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

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


def workspace_writes(ws: UUID, doc: UUID, expired: UUID) -> list[tuple[str, str, Any]]:
    """Every state-changing workspace route the billing gate covers, as (method, url, body):
    on document `doc`, approving the deletion of `expired`."""
    base = f"/v1/workspaces/{ws}"
    reason = "SAMPLE: uploaded by mistake"
    return [
        ("PATCH", f"{base}/documents/{doc}/fields", {"fields": {"total_ttc": "1.00"}}),
        ("POST", f"{base}/documents/{doc}/seal", None),
        ("POST", f"{base}/seal-batch", {"month": "2026-11"}),
        ("PUT", f"{base}/documents/{doc}/tags", {"tags": ["SAMPLE"]}),
        ("POST", f"{base}/documents/{doc}/corrections",
         {"field": "total_ttc", "value": "1.00", "reason": reason}),
        ("PUT", f"{base}/retention/policies/invoice", {"years": 12}),
        # A document past retention: eligible, so only the billing gate refuses it.
        ("POST", f"{base}/retention/approve-deletion", {"ids": [str(expired)]}),
        ("POST", f"{base}/documents/{doc}/remove", {"reason": reason}),
        ("POST", f"{base}/search/views", {"name": "SAMPLE"}),
        ("PUT", f"{base}/search/views/{uuid4()}", {"name": "SAMPLE"}),
        ("DELETE", f"{base}/search/views/{uuid4()}", None),
        ("POST", f"{base}/ask", {"question": "Total TTC de novembre ?", "locale": "fr"}),
        ("POST", f"{base}/exports", {"period": "2026-11"}),
    ]  # fmt: skip
