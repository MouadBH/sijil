"""`make seed` keeps the SAMPLE organisation paid up (`keep_paid_up`), so its AI never stops."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import billing
from sijil_core.db.models import (
    AuditEvent,
    Invoice,
    InvoiceState,
    Organisation,
    Subscription,
    SubscriptionStatus,
    Workspace,
)
from tests.support.seed import SAMPLE_ORG_ICE, keep_paid_up
from tests.support.seller import sample_seller

pytestmark = pytest.mark.db


async def _with_ice(db: AsyncSession, org: Organisation) -> None:
    org.ice = SAMPLE_ORG_ICE
    await db.commit()


async def test_in_the_trial_it_chooses_the_plan_and_pays_nothing(
    db: AsyncSession, org: Organisation, workspace: Workspace
) -> None:
    await _with_ice(db, org)
    paid = await keep_paid_up(db, org.id, datetime.now(UTC), seller=sample_seller())
    assert paid == []
    sub = await db.scalar(select(Subscription).where(Subscription.org_id == org.id))
    assert sub is not None
    assert (sub.status, sub.plan_code) == (SubscriptionStatus.trial, "essentiel")
    assert await db.scalar(select(Invoice).where(Invoice.org_id == org.id)) is None


async def test_an_overdue_org_is_paid_up_once(
    db: AsyncSession, org: Organisation, workspace: Workspace
) -> None:
    await _with_ice(db, org)
    now = datetime.now(UTC)
    trial_end = now - timedelta(days=45)
    db.add(billing.new_trial(org.id, trial_end - timedelta(days=billing.TRIAL_DAYS)))
    await db.commit()
    # The daily job at the trial's end issued the first annual invoice; nobody paid it.
    await billing.advance(db, org.id, trial_end, seller=sample_seller())
    assert (await billing.advance(db, org.id, now, seller=sample_seller())).status is (
        SubscriptionStatus.overdue
    )
    await db.commit()

    paid = await keep_paid_up(db, org.id, now, seller=sample_seller())

    invoice = await db.scalar(
        select(Invoice).where(Invoice.org_id == org.id).execution_options(populate_existing=True)
    )
    assert invoice is not None and paid == [invoice.number]
    assert invoice.state is InvoiceState.paid
    assert invoice.payment_reference == f"SAMPLE-SEED-{invoice.number}"
    assert (await billing.standing(db, org.id, now)).ai_allowed
    sub = await db.scalar(
        select(Subscription)
        .where(Subscription.org_id == org.id)
        .execution_options(populate_existing=True)
    )
    assert sub is not None and sub.status is SubscriptionStatus.active
    actions = list(
        await db.scalars(select(AuditEvent.action).where(AuditEvent.workspace_id == workspace.id))
    )
    assert actions.count("billing.invoice_paid") == 1
    # Again: nothing more to pay, nothing more audited.
    assert await keep_paid_up(db, org.id, now, seller=sample_seller()) == []
    actions = list(
        await db.scalars(select(AuditEvent.action).where(AuditEvent.workspace_id == workspace.id))
    )
    assert actions.count("billing.invoice_paid") == 1
