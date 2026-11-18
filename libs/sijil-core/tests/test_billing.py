"""Plans, subscriptions, the usage meter, the standing and the quota warnings (ADM-3)."""

import asyncio
import hashlib
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import billing
from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    Membership,
    Organisation,
    Plan,
    Role,
    Subscription,
    SubscriptionStatus,
    User,
    Workspace,
)

pytestmark = pytest.mark.db

NOW = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)
GB = 1024**3


def _user(n: int) -> User:
    return User(email=f"u{n}-{uuid.uuid4().hex[:6]}@example.com", name="SAMPLE", password_hash="x")


def _ledger(ws: Workspace, *, size: int, at: datetime, removed: bool = False) -> DocumentLedger:
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    return DocumentLedger(
        workspace_id=ws.id,
        sha256=sha,
        md5="0" * 32,
        size_bytes=size,
        content_kind="pdf",
        source=DocumentSource.upload,
        object_key=f"k/{sha}",
        object_version_id="v1",
        state=DocumentState.received,
        retention_until=date(2036, 12, 31),
        ingested_at=at,
    )


async def _trial(db: AsyncSession, org: Organisation, start: datetime = NOW) -> Subscription:
    sub = billing.new_trial(org.id, start)
    db.add(sub)
    await db.commit()
    return sub


async def test_catalogue_is_seeded(db: AsyncSession) -> None:
    plans = {p.code: p for p in await db.scalars(select(Plan))}
    assert set(plans) == {"essentiel", "pro", "entreprise"}
    rows = {
        code: (p.price_ht, p.max_users, p.max_documents_year, p.max_storage_bytes,
               p.max_ai_pages_year, p.active)
        for code, p in plans.items()
    }  # fmt: skip
    assert rows == {
        "essentiel": (Decimal("2400.00"), 3, 2_000, 5 * GB, 2_000, True),
        "pro": (Decimal("6000.00"), 10, 10_000, 25 * GB, 10_000, True),
        "entreprise": (Decimal("14400.00"), 30, 40_000, 100 * GB, 40_000, True),
    }


def test_new_trial_is_30_days_on_essentiel() -> None:
    org_id = uuid.uuid4()
    sub = billing.new_trial(org_id, NOW)
    assert (sub.org_id, sub.plan_code, sub.status) == (org_id, "essentiel", "trial")
    assert sub.trial_ends_at == NOW + timedelta(days=30)
    assert (sub.period_start, sub.period_end) == (NOW, NOW + timedelta(days=30))


def test_billing_year_starts_at_the_end_of_the_trial() -> None:
    sub = billing.new_trial(uuid.uuid4(), NOW)
    end = NOW + timedelta(days=30)
    assert billing.billing_window(sub, NOW + timedelta(days=3)) == (NOW, end)
    # The trial is over but the daily job has not moved the period yet: the first year.
    first = billing.billing_window(sub, end + timedelta(days=1))
    assert first == (end, datetime(2027, 12, 16, 10, 0, tzinfo=UTC))
    second = (first[1], datetime(2028, 12, 16, 10, 0, tzinfo=UTC))
    assert billing.billing_window(sub, first[1]) == second


def test_levels_without_floats() -> None:
    assert billing.quota("documents", 0, 2000).level == "ok"
    assert billing.quota("documents", 1599, 2000).level == "ok"
    assert billing.quota("documents", 1600, 2000).level == "warn_80"
    assert billing.quota("documents", 1999, 2000).level == "warn_80"
    assert billing.quota("documents", 2000, 2000).level == "over_100"
    assert billing.quota("documents", 3000, 2000).percent == 150
    assert billing.quota("documents", 1999, 2000).percent == 99


async def test_meter_counts_users_documents_storage_ai_pages(
    db: AsyncSession, org: Organisation, workspace: Workspace
) -> None:
    await _trial(db, org, NOW - timedelta(days=5))
    # Users: distinct users with an org-level or a workspace-level membership.
    a, b, c, outsider = _user(1), _user(2), _user(3), _user(4)
    db.add_all([a, b, c, outsider])
    await db.flush()
    db.add_all([
        Membership(user_id=a.id, org_id=org.id, role=Role.owner),
        Membership(user_id=b.id, org_id=org.id, role=Role.member),
        Membership(user_id=b.id, workspace_id=workspace.id, role=Role.member),
        Membership(user_id=c.id, workspace_id=workspace.id, role=Role.readonly),
    ])  # fmt: skip
    # Documents: ingested in the window (here the trial); storage: originals not purged.
    inside = [_ledger(workspace, size=1000, at=NOW - timedelta(days=n)) for n in range(3)]
    before = _ledger(workspace, size=500, at=NOW - timedelta(days=40))
    db.add_all([*inside, before])
    await db.flush()
    await billing.record_ai_pages(
        db, org_id=org.id, workspace_id=workspace.id, ledger_id=inside[0].id, pages=3, now=NOW
    )
    await billing.record_ai_pages(
        db, org_id=org.id, workspace_id=workspace.id, ledger_id=inside[1].id, pages=1, now=NOW
    )
    await db.commit()

    meter = await billing.meter(db, org.id, NOW)
    assert (meter.plan_code, meter.status) == ("essentiel", SubscriptionStatus.trial)
    got = {k: (q.used, q.limit) for k, q in meter.quotas.items()}
    assert got == {
        "users": (3, 3),
        "documents": (3, 2_000),
        "storage": (3_500, 5 * GB),
        "ai_pages": (4, 2_000),
    }
    assert meter.quotas["users"].level == "over_100"


async def test_meter_without_subscription_row_is_an_implied_trial(
    db: AsyncSession, org: Organisation, workspace: Workspace
) -> None:
    meter = await billing.meter(db, org.id, NOW)
    assert (meter.plan_code, meter.status) == ("essentiel", SubscriptionStatus.trial)
    assert meter.period_start == org.created_at


async def test_ai_pages_recorded_once_per_document(
    db: AsyncSession, org: Organisation, workspace: Workspace
) -> None:
    await _trial(db, org, NOW - timedelta(days=1))
    row = _ledger(workspace, size=10, at=NOW)
    db.add(row)
    await db.flush()

    async def record(pages: int) -> bool:
        return await billing.record_ai_pages(
            db, org_id=org.id, workspace_id=workspace.id, ledger_id=row.id, pages=pages, now=NOW
        )

    assert await record(2) is True
    assert await record(5) is False
    assert await record(0) is False
    await db.commit()
    assert (await billing.meter(db, org.id, NOW)).quotas["ai_pages"].used == 2


async def test_standing_over_quota_stops_ai_and_new_users(
    db: AsyncSession, org: Organisation, workspace: Workspace
) -> None:
    await _trial(db, org, NOW - timedelta(days=1))
    fine = await billing.standing(db, org.id, NOW)
    assert (fine.ai_allowed, fine.new_users_allowed, fine.reason_code) == (True, True, None)
    assert (fine.export_allowed, fine.writes_allowed) == (True, True)
    # Storage beyond its limit: AI and new users stop, the rest stays.
    db.add(_ledger(workspace, size=5 * GB + 1, at=NOW))
    await db.commit()
    over = await billing.standing(db, org.id, NOW)
    assert (over.ai_allowed, over.new_users_allowed) == (False, False)
    assert over.reason_code == "quota_exceeded"
    assert (over.export_allowed, over.writes_allowed) == (True, True)


async def test_users_at_their_limit_refuse_only_new_users(
    db: AsyncSession, org: Organisation, workspace: Workspace
) -> None:
    await _trial(db, org, NOW - timedelta(days=1))
    users = [_user(n) for n in range(3)]
    db.add_all(users)
    await db.flush()
    db.add_all([Membership(user_id=u.id, org_id=org.id, role=Role.member) for u in users])
    await db.commit()
    standing = await billing.standing(db, org.id, NOW)
    assert (standing.ai_allowed, standing.new_users_allowed) == (True, False)


async def _warnings(db: AsyncSession, ws: Workspace) -> list[tuple[str, dict[str, object]]]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id, AuditEvent.action.like("quota.%"))
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return [(e.action, e.data) for e in rows]


async def test_warning_at_80_and_100(
    db: AsyncSession, org: Organisation, workspace: Workspace
) -> None:
    """Audited once each per billing year, on every workspace of the org, however many times
    the meter is read."""
    await _trial(db, org, NOW - timedelta(days=1))
    db.add(_ledger(workspace, size=4 * GB, at=NOW))  # 80% of 5 GB
    await db.commit()
    for _ in range(2):
        await billing.check(db, org.id, NOW)
        await db.commit()
    assert await _warnings(db, workspace) == [("quota.warning_80", {"kind": "storage"})]
    db.add(_ledger(workspace, size=GB, at=NOW))  # 100%
    await db.commit()
    for _ in range(2):
        meter = await billing.check(db, org.id, NOW)
        await db.commit()
    assert meter.quotas["storage"].level == "over_100"
    assert await _warnings(db, workspace) == [
        ("quota.warning_80", {"kind": "storage"}),
        ("quota.warning_100", {"kind": "storage"}),
    ]
    # The next billing year warns again.
    later = NOW + timedelta(days=40)
    await billing.check(db, org.id, later)
    await db.commit()
    assert [a for a, _ in await _warnings(db, workspace)] == [
        "quota.warning_80", "quota.warning_100", "quota.warning_80", "quota.warning_100",
    ]  # fmt: skip


async def test_warning_once_under_concurrency(
    db: AsyncSession, org: Organisation, workspace: Workspace
) -> None:
    await _trial(db, org, NOW - timedelta(days=1))
    db.add(_ledger(workspace, size=4 * GB, at=NOW))
    await db.commit()
    sessions = async_sessionmaker(db.bind, expire_on_commit=False)

    async def one() -> None:
        async with sessions() as session:
            await billing.check(session, org.id, NOW)
            await session.commit()

    await asyncio.gather(*(one() for _ in range(5)))
    assert await _warnings(db, workspace) == [("quota.warning_80", {"kind": "storage"})]
    count = await db.scalar(select(func.count()).select_from(AuditEvent))
    assert count == 1
