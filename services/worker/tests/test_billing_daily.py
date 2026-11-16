"""The daily billing transitions (`sijil.billing_daily`, ADM-4, design §5.7a): the first annual
invoice at the trial's end, renewals, overdue and read-only, each audited once."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from celery.schedules import crontab
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import billing, invoicing
from sijil_core.db.models import (
    AuditEvent,
    Invoice,
    InvoiceKind,
    InvoiceState,
    Membership,
    Organisation,
    OrgType,
    Plan,
    Role,
    Subscription,
    SubscriptionStatus,
    User,
    Workspace,
)
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.billing import billing_daily
from sijil_worker.settings import BillingSettings, BrokerSettings
from tests.support.seller import SAMPLE_SELLER, sample_seller

pytestmark = pytest.mark.db

START = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)
TRIAL_END = START + timedelta(days=30)
ICE = "002999888000011"  # SAMPLE: a fictitious company's ICE
SELLER = sample_seller()


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


async def _subscribed(db: AsyncSession, org: Organisation, user: User, ice: str | None) -> None:
    org.ice = ice
    db.add(Membership(user_id=user.id, org_id=org.id, role=Role.owner))
    db.add(billing.new_trial(org.id, START))
    await db.commit()


async def _sub(db: AsyncSession, org: Organisation) -> Subscription:
    sub = await db.scalar(
        select(Subscription)
        .where(Subscription.org_id == org.id)
        .execution_options(populate_existing=True)
    )
    assert sub is not None
    return sub


async def _events(db: AsyncSession, workspace: Workspace) -> list[tuple[str, object]]:
    rows = await db.execute(
        select(AuditEvent.action, AuditEvent.data)
        .where(AuditEvent.workspace_id == workspace.id, AuditEvent.action.like("billing.%"))
        .order_by(AuditEvent.seq)
    )
    return [(a, d) for a, d in rows]


async def _invoices(db: AsyncSession, org: Organisation) -> list[Invoice]:
    rows = await db.scalars(
        select(Invoice)
        .where(Invoice.org_id == org.id)
        .order_by(Invoice.issued_at, Invoice.number)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def _twice(sessions: async_sessionmaker[AsyncSession], now: datetime) -> None:
    for _ in range(2):
        await billing_daily(sessions, settings=SELLER, now=now)


def test_billing_daily_runs_once_a_day_on_the_main_worker() -> None:
    settings = BrokerSettings(region="ma-cas", environment="test", redis_url="redis://r:6379/0")
    entry = make_celery(settings).conf.beat_schedule["billing-daily"]
    # No queue option: the main worker's `default` queue.
    assert entry == {"task": "sijil.billing_daily", "schedule": crontab(hour=1, minute=0)}
    assert tasks.billing_daily_task.name == "sijil.billing_daily"


def test_billing_settings_require_the_seller(monkeypatch: pytest.MonkeyPatch) -> None:
    base = {"region": "ma-cas", "environment": "test", "redis_url": "redis://r:6379/0"}
    base |= {"database_url": "postgresql+asyncpg://x:y@127.0.0.1:1/z"}
    for name in SAMPLE_SELLER:
        monkeypatch.delenv(f"SIJIL_{name.upper()}", raising=False)
    with pytest.raises(ValidationError):
        BillingSettings.model_validate(base)
    settings = BillingSettings.model_validate(base | SAMPLE_SELLER)
    assert settings.seller()["ice"] == SAMPLE_SELLER["seller_ice"]


async def test_daily_transitions_are_audited_once(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    org: Organisation,
    user: User,
    workspace: Workspace,
) -> None:
    await _subscribed(db, org, user, ICE)
    # Still in the trial: nothing.
    await _twice(sessions, TRIAL_END - timedelta(hours=1))
    assert (await _sub(db, org)).status is SubscriptionStatus.trial
    assert await _invoices(db, org) == []

    # The trial ends: active, and the first annual invoice, for the year after the trial.
    issued_at = TRIAL_END + timedelta(hours=2)
    await _twice(sessions, issued_at)
    sub = await _sub(db, org)
    assert (sub.status, sub.status_reason) == (SubscriptionStatus.active, None)
    year_end = TRIAL_END.replace(year=TRIAL_END.year + 1)
    assert (sub.period_start, sub.period_end) == (TRIAL_END, year_end)
    (first,) = await _invoices(db, org)
    assert (first.kind, first.plan_code, first.state) == (
        InvoiceKind.annual, "essentiel", InvoiceState.issued
    )  # fmt: skip
    assert (first.period_start, first.period_end) == (TRIAL_END, year_end)
    assert (first.amount_ttc, first.due_at) == (Decimal("2880.00"), issued_at + timedelta(days=30))
    issued = ("billing.invoice_issued", {
        "number": first.number, "kind": "annual", "plan": "essentiel", "amount_ttc": "2880.00",
    })  # fmt: skip
    assert await _events(db, workspace) == [issued]

    # Unpaid past due: the invoice and the subscription are overdue; exports stop, AI too.
    await _twice(sessions, first.due_at + timedelta(minutes=1))
    assert (await _sub(db, org)).status is SubscriptionStatus.overdue
    assert (await _invoices(db, org))[0].state is InvoiceState.overdue
    overdue = ("billing.overdue", {"number": first.number})
    assert await _events(db, workspace) == [issued, overdue]
    standing = await billing.standing(db, org.id, first.due_at + timedelta(minutes=1))
    assert (standing.ai_allowed, standing.export_allowed, standing.new_users_allowed) == (
        False, False, False
    )  # fmt: skip
    assert (standing.writes_allowed, standing.billing_code, standing.reason_code) == (
        True, "billing.overdue", "billing_overdue"
    )  # fmt: skip

    # 30 days after the due date: read-only (plus capture).
    late = first.due_at + timedelta(days=30, minutes=1)
    await _twice(sessions, late)
    assert (await _sub(db, org)).status is SubscriptionStatus.read_only
    read_only = ("billing.read_only", {"number": first.number})
    assert await _events(db, workspace) == [issued, overdue, read_only]
    standing = await billing.standing(db, org.id, late)
    assert (standing.writes_allowed, standing.billing_code) == (False, "billing.read_only")

    # Paid (Task 4 records it): everything lifts at once, with no further event here.
    async with sessions() as session:
        paid = await session.get(Invoice, first.id)
        assert paid is not None
        paid.state, paid.paid_at, paid.payment_reference = InvoiceState.paid, late, "VIR-SAMPLE-1"
        await session.commit()
    assert (await billing.standing(db, org.id, late)).billing_code is None
    await _twice(sessions, late)
    assert (await _sub(db, org)).status is SubscriptionStatus.active
    assert len(await _events(db, workspace)) == 3

    # A downgrade waits for the renewal: the renewal invoice, 30 days before the year ends, is
    # on the pending plan, which applies at the renewal itself.
    async with sessions() as session:
        row = await session.scalar(select(Subscription).where(Subscription.org_id == org.id))
        assert row is not None
        row.plan_code, row.pending_plan_code = "pro", "essentiel"
        await session.commit()
    await _twice(sessions, year_end - timedelta(days=31))
    assert len(await _invoices(db, org)) == 1
    notice = year_end - timedelta(days=30) + timedelta(hours=1)
    await _twice(sessions, notice)
    renewal = (await _invoices(db, org))[1]
    assert (renewal.kind, renewal.plan_code, renewal.amount_ht) == (
        InvoiceKind.annual, "essentiel", Decimal("2400.00")
    )  # fmt: skip
    assert renewal.period_start == year_end
    sub = await _sub(db, org)
    assert (sub.plan_code, sub.pending_plan_code) == ("pro", "essentiel")
    await _twice(sessions, year_end + timedelta(hours=1))
    sub = await _sub(db, org)
    assert (sub.plan_code, sub.pending_plan_code) == ("essentiel", None)
    assert (sub.period_start, sub.period_end) == (year_end, year_end.replace(year=2028))
    assert len(await _invoices(db, org)) == 2
    actions = [a for a, _ in await _events(db, workspace)]
    assert actions.count("billing.invoice_issued") == 2


async def test_trial_end_without_ice_restricts(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    org: Organisation,
    user: User,
    workspace: Workspace,
) -> None:
    await _subscribed(db, org, user, None)
    # Ended by the clock before the daily job has run: every reader already sees it.
    just_after = TRIAL_END + timedelta(minutes=1)
    standing = await billing.standing(db, org.id, just_after)
    assert (standing.ai_allowed, standing.export_allowed, standing.billing_code) == (
        False, False, "billing.overdue"
    )  # fmt: skip
    meter = await billing.meter(db, org.id, just_after)
    assert (meter.status, meter.status_reason) == (SubscriptionStatus.overdue, "org.ice_required")

    await _twice(sessions, just_after)
    sub = await _sub(db, org)
    assert (sub.status, sub.status_reason) == (SubscriptionStatus.overdue, "org.ice_required")
    assert await _invoices(db, org) == []
    reason = {"reason": "org.ice_required"}
    assert await _events(db, workspace) == [("billing.overdue", reason)]

    # 30 days on, still no ICE: read-only.
    await _twice(sessions, TRIAL_END + timedelta(days=30, minutes=1))
    assert (await _sub(db, org)).status is SubscriptionStatus.read_only
    assert await _events(db, workspace) == [
        ("billing.overdue", reason),
        ("billing.read_only", reason),
    ]

    # The ICE given: the first annual invoice is issued, due 30 days on, and it lifts.
    async with sessions() as session:
        row = await session.get(Organisation, org.id)
        assert row is not None
        row.ice = ICE
        await session.commit()
    given = TRIAL_END + timedelta(days=31)
    await _twice(sessions, given)
    sub = await _sub(db, org)
    assert (sub.status, sub.status_reason) == (SubscriptionStatus.active, None)
    (invoice,) = await _invoices(db, org)
    assert (invoice.period_start, invoice.due_at) == (TRIAL_END, given + timedelta(days=30))


async def test_daily_covers_this_region_only(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], user: User
) -> None:
    abroad = Organisation(
        name="Abroad", type=OrgType.business, country="FR", region="eu-fra", ice=ICE
    )
    db.add(abroad)
    await db.flush()
    db.add(billing.new_trial(abroad.id, START))
    await db.commit()
    await billing_daily(sessions, settings=SELLER, now=TRIAL_END + timedelta(days=1))
    assert await _invoices(db, abroad) == []
    assert (await _sub(db, abroad)).status is SubscriptionStatus.trial


# Fix round 1: a plan change inside the renewal window (the last 30 days) ---------------------

YEAR_END = TRIAL_END.replace(year=TRIAL_END.year + 1)


async def _pay_all(sessions: async_sessionmaker[AsyncSession], org: Organisation) -> None:
    async with sessions() as session:
        rows = await session.scalars(
            select(Invoice).where(Invoice.org_id == org.id, Invoice.state != InvoiceState.paid)
        )
        for row in rows:
            row.state, row.paid_at, row.payment_reference = (
                InvoiceState.paid, row.issued_at, f"VIR-SAMPLE-{row.number}"
            )  # fmt: skip
        await session.commit()


async def _renewal_issued(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], org: Organisation, plan: str
) -> None:
    """The first year started on `plan` and paid; the renewal on it issued, 30 days before the
    year ends."""
    async with sessions() as session:
        row = await session.scalar(select(Subscription).where(Subscription.org_id == org.id))
        assert row is not None
        row.plan_code = plan
        await session.commit()
    await billing_daily(sessions, settings=SELLER, now=TRIAL_END + timedelta(hours=2))
    await _pay_all(sessions, org)
    await billing_daily(sessions, settings=SELLER, now=YEAR_END - timedelta(days=29))
    renewal = (await _invoices(db, org))[-1]
    assert (renewal.period_start, renewal.plan_code) == (YEAR_END, plan)


async def _change(
    sessions: async_sessionmaker[AsyncSession], org: Organisation, plan: str, now: datetime
) -> billing.PlanChange:
    async with sessions() as session:
        row = await session.get(Plan, plan)
        assert row is not None
        change = await billing.change_plan(session, org.id, row, now, seller=SELLER)
        await session.commit()
    return change


@pytest.mark.db
async def test_upgrade_in_the_renewal_window_is_billed_and_kept_after_the_roll(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    org: Organisation,
    user: User,
    workspace: Workspace,
) -> None:
    await _subscribed(db, org, user, ICE)
    await _renewal_issued(db, sessions, org, "essentiel")
    change = await _change(sessions, org, "pro", YEAR_END - timedelta(days=10))
    assert (change.previous, change.changed, change.effective) == ("essentiel", True, "now")
    invoices = await _invoices(db, org)
    # The 10 days left of this year, prorated; then the next year's difference in full, since
    # its renewal was already issued on essentiel (never altered).
    days = invoicing.prorata(
        TRIAL_END, YEAR_END, YEAR_END - timedelta(days=10), "ma-cas"
    )  # fmt: skip
    prorated, corrective = invoices[-2:]
    assert (prorated.kind, prorated.plan_code, prorated.period_end) == (
        InvoiceKind.upgrade, "pro", YEAR_END
    )  # fmt: skip
    assert prorated.amount_ht == invoicing.amounts(Decimal("3600.00"), days).ht
    assert (corrective.kind, corrective.plan_code, corrective.amount_ht) == (
        InvoiceKind.upgrade, "pro", Decimal("3600.00")
    )  # fmt: skip
    assert (corrective.period_start, corrective.period_end) == (
        YEAR_END, YEAR_END.replace(year=YEAR_END.year + 1)
    )  # fmt: skip
    renewal = invoices[-3]
    assert (renewal.plan_code, renewal.amount_ht) == ("essentiel", Decimal("2400.00"))
    await _pay_all(sessions, org)
    # The roll keeps pro in force: the next year is billed 2 400 + 3 600 = pro's 6 000 HT.
    await billing_daily(sessions, settings=SELLER, now=YEAR_END + timedelta(hours=1))
    sub = await _sub(db, org)
    assert (sub.plan_code, sub.pending_plan_code, sub.period_start) == ("pro", None, YEAR_END)
    assert (sub.status, len(await _invoices(db, org))) == (SubscriptionStatus.active, 4)
    # The year after is renewed on pro.
    year_2 = YEAR_END.replace(year=YEAR_END.year + 1)
    await billing_daily(sessions, settings=SELLER, now=year_2 - timedelta(days=29))
    last = (await _invoices(db, org))[-1]
    assert (last.kind, last.plan_code, last.period_start) == (InvoiceKind.annual, "pro", year_2)


@pytest.mark.db
async def test_downgrade_in_the_renewal_window_applies_at_the_following_renewal(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    org: Organisation,
    user: User,
    workspace: Workspace,
) -> None:
    await _subscribed(db, org, user, ICE)
    await _renewal_issued(db, sessions, org, "pro")
    before = len(await _invoices(db, org))
    change = await _change(sessions, org, "essentiel", YEAR_END - timedelta(days=10))
    # The next year is already invoiced on pro: the downgrade waits for the renewal after it.
    assert (change.changed, change.effective) == (True, "following_renewal")
    assert len(await _invoices(db, org)) == before
    await _pay_all(sessions, org)
    await billing_daily(sessions, settings=SELLER, now=YEAR_END + timedelta(hours=1))
    sub = await _sub(db, org)
    assert (sub.plan_code, sub.pending_plan_code) == ("pro", "essentiel")
    year_2 = YEAR_END.replace(year=YEAR_END.year + 1)
    await billing_daily(sessions, settings=SELLER, now=year_2 - timedelta(days=29))
    renewal = (await _invoices(db, org))[-1]
    assert (renewal.plan_code, renewal.amount_ht, renewal.period_start) == (
        "essentiel", Decimal("2400.00"), year_2
    )  # fmt: skip
    await _pay_all(sessions, org)
    await billing_daily(sessions, settings=SELLER, now=year_2 + timedelta(hours=1))
    sub = await _sub(db, org)
    assert (sub.plan_code, sub.pending_plan_code) == ("essentiel", None)


@pytest.mark.db
async def test_downgrade_before_the_renewal_is_invoiced_applies_at_that_renewal(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    org: Organisation,
    user: User,
    workspace: Workspace,
) -> None:
    await _subscribed(db, org, user, ICE)
    async with sessions() as session:
        row = await session.scalar(select(Subscription).where(Subscription.org_id == org.id))
        assert row is not None
        row.plan_code = "pro"
        await session.commit()
    await billing_daily(sessions, settings=SELLER, now=TRIAL_END + timedelta(hours=2))
    await _pay_all(sessions, org)
    change = await _change(sessions, org, "essentiel", YEAR_END - timedelta(days=60))
    assert change.effective == "renewal"
    await billing_daily(sessions, settings=SELLER, now=YEAR_END - timedelta(days=29))
    assert (await _invoices(db, org))[-1].plan_code == "essentiel"
    await _pay_all(sessions, org)
    await billing_daily(sessions, settings=SELLER, now=YEAR_END + timedelta(hours=1))
    sub = await _sub(db, org)
    assert (sub.plan_code, sub.pending_plan_code) == ("essentiel", None)


async def _downgrade_renewed(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], org: Organisation, plan: str
) -> None:
    """The first year on `plan`, paid; a downgrade to essentiel chosen 60 days before the year
    ends; so its renewal, issued 30 days before, is on essentiel."""
    async with sessions() as session:
        row = await session.scalar(select(Subscription).where(Subscription.org_id == org.id))
        assert row is not None
        row.plan_code = plan
        await session.commit()
    await billing_daily(sessions, settings=SELLER, now=TRIAL_END + timedelta(hours=2))
    await _pay_all(sessions, org)
    assert (await _change(sessions, org, "essentiel", YEAR_END - timedelta(days=60))).effective == (
        "renewal"
    )
    await billing_daily(sessions, settings=SELLER, now=YEAR_END - timedelta(days=29))
    renewal = (await _invoices(db, org))[-1]
    assert (renewal.period_start, renewal.plan_code, renewal.amount_ht) == (
        YEAR_END, "essentiel", Decimal("2400.00")
    )  # fmt: skip


async def _rolled(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], org: Organisation
) -> Subscription:
    await _pay_all(sessions, org)
    await billing_daily(sessions, settings=SELLER, now=YEAR_END + timedelta(hours=1))
    return await _sub(db, org)


@pytest.mark.db
async def test_cancelled_downgrade_after_its_renewal_keeps_the_plan_after_the_roll(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    org: Organisation,
    user: User,
    workspace: Workspace,
) -> None:
    """pro, a downgrade to essentiel renewed on essentiel, then pro chosen again on day −10:
    the next year's difference is invoiced (the renewal is never altered) and pro stays."""
    await _subscribed(db, org, user, ICE)
    await _downgrade_renewed(db, sessions, org, "pro")
    before = len(await _invoices(db, org))
    change = await _change(sessions, org, "pro", YEAR_END - timedelta(days=10))
    assert (change.changed, change.effective) == (True, "now")
    invoices = await _invoices(db, org)
    assert len(invoices) == before + 1
    correction = invoices[-1]
    assert (correction.kind, correction.plan_code, correction.amount_ht, correction.amount_ttc) == (
        InvoiceKind.upgrade, "pro", Decimal("3600.00"), Decimal("4320.00")
    )  # fmt: skip
    assert correction.period_start == YEAR_END
    assert invoices[-2].amount_ht == Decimal("2400.00")  # the renewal, untouched
    sub = await _rolled(db, sessions, org)
    assert (sub.plan_code, sub.pending_plan_code, sub.period_start) == ("pro", None, YEAR_END)


@pytest.mark.db
async def test_a_mid_plan_above_a_cheaper_issued_renewal_is_billed_and_applies_at_the_roll(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    org: Organisation,
    user: User,
    workspace: Workspace,
) -> None:
    """entreprise, a downgrade to essentiel renewed on essentiel, then pro chosen on day −10:
    still a downgrade from entreprise, so nothing changes this year, but the next year is
    billed pro (2 400 + 3 600) and runs on pro."""
    await _subscribed(db, org, user, ICE)
    await _downgrade_renewed(db, sessions, org, "entreprise")
    before = len(await _invoices(db, org))
    change = await _change(sessions, org, "pro", YEAR_END - timedelta(days=10))
    assert (change.changed, change.effective) == (True, "renewal")
    invoices = await _invoices(db, org)
    assert len(invoices) == before + 1
    correction = invoices[-1]
    assert (correction.kind, correction.plan_code, correction.amount_ht) == (
        InvoiceKind.upgrade, "pro", Decimal("3600.00")
    )  # fmt: skip
    assert (correction.period_start, correction.period_end) == (
        YEAR_END, YEAR_END.replace(year=YEAR_END.year + 1)
    )  # fmt: skip
    assert (await _sub(db, org)).plan_code == "entreprise"
    sub = await _rolled(db, sessions, org)
    assert (sub.plan_code, sub.pending_plan_code) == ("pro", None)
    year_2 = YEAR_END.replace(year=YEAR_END.year + 1)
    await billing_daily(sessions, settings=SELLER, now=year_2 - timedelta(days=29))
    assert (await _invoices(db, org))[-1].plan_code == "pro"
