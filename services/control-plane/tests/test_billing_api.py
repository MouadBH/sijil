"""Plans, the subscription, the usage meter, the user quota and the organisation's ICE (ADM-3)."""

from collections.abc import Awaitable, Callable
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import (
    AuditEvent,
    Membership,
    Organisation,
    Role,
    Subscription,
    SubscriptionStatus,
    User,
    Workspace,
)

pytestmark = pytest.mark.db

MakeActor = Callable[[str], Awaitable[Any]]
FORMATS = Path(__file__).resolve().parents[3] / "fixtures" / "formats"
PDF = (FORMATS / "sample.pdf").read_bytes()
GB = 1024**3
ICE = "002999888000011"  # SAMPLE: a fictitious company's ICE


def error(r: Any) -> str:
    code: str = r.json()["error"]["code"]
    return code


async def create_org(actor: Any) -> str:
    r = await actor.client.post(
        "/v1/orgs", json={"name": "Atlas SARL", "type": "business", "country": "MA"}
    )
    assert r.status_code == 202, r.text
    org_id: str = r.json()["org"]["id"]
    return org_id


async def stepped_up_owner(make_actor: MakeActor) -> tuple[Any, str]:
    alice = await make_actor("alice@example.com")
    org_id = await create_org(alice)
    await alice.enable_totp()
    await alice.step_up()
    return alice, org_id


async def add(db: AsyncSession, org_id: str, user_id: UUID, role: Role) -> None:
    db.add(Membership(user_id=user_id, org_id=UUID(org_id), role=role))
    await db.commit()


async def new_user(db: AsyncSession, email: str) -> User:
    user = User(email=email, name="SAMPLE", password_hash="x")
    db.add(user)
    await db.commit()
    return user


async def test_new_org_starts_trial_on_essentiel(
    make_actor: MakeActor, db: AsyncSession, clock: Any
) -> None:
    alice = await make_actor("alice@example.com")
    org_id = await create_org(alice)
    sub = await db.scalar(select(Subscription).where(Subscription.org_id == UUID(org_id)))
    assert sub is not None
    assert (sub.plan_code, sub.status) == ("essentiel", SubscriptionStatus.trial)
    assert sub.trial_ends_at == clock.now + timedelta(days=30)
    assert (sub.period_start, sub.period_end) == (clock.now, sub.trial_ends_at)
    r = await alice.client.get(f"/v1/orgs/{org_id}/subscription")
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["plan_code"], body["status"], body["pending_plan_code"]) == (
        "essentiel",
        "trial",
        None,
    )
    assert body["plan"] == {
        "code": "essentiel",
        "price_ht": "2400.00",
        "max_users": 3,
        "max_documents_year": 2000,
        "max_storage_bytes": 5 * GB,
        "max_ai_pages_year": 2000,
    }
    plans = await alice.client.get("/v1/plans")
    assert plans.status_code == 200
    assert [(p["code"], p["price_ht"]) for p in plans.json()] == [
        ("essentiel", "2400.00"),
        ("pro", "6000.00"),
        ("entreprise", "14400.00"),
    ]


async def test_usage_reports_each_quota_and_its_level(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    alice = await make_actor("alice@example.com")
    org_id = await create_org(alice)
    r = await alice.client.get(f"/v1/orgs/{org_id}/usage")
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["plan_code"], body["status"]) == ("essentiel", "trial")
    assert body["quotas"]["users"] == {"used": 1, "limit": 3, "percent": 33, "level": "ok"}
    assert body["quotas"]["documents"]["level"] == "ok"
    assert set(body["quotas"]) == {"users", "documents", "storage", "ai_pages"}


async def test_usage_and_subscription_are_for_owners_and_admins(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    carol = await make_actor("carol@example.com")
    org_id = await create_org(alice)
    await add(db, org_id, bob.user_id, Role.admin)
    await add(db, org_id, carol.user_id, Role.member)
    for path in ("usage", "subscription"):
        assert (await bob.client.get(f"/v1/orgs/{org_id}/{path}")).status_code == 200
        assert (await carol.client.get(f"/v1/orgs/{org_id}/{path}")).status_code == 403
    await bob.enable_totp()
    await bob.step_up()
    r = await bob.client.put(f"/v1/orgs/{org_id}/subscription", json={"plan_code": "pro"})
    assert (r.status_code, error(r)) == (403, "forbidden")


async def test_new_member_refused_over_user_quota(make_actor: MakeActor, db: AsyncSession) -> None:
    alice, org_id = await stepped_up_owner(make_actor)
    for n in range(2):
        user = await new_user(db, f"member{n}@example.com")
        r = await alice.client.post(
            f"/v1/orgs/{org_id}/members", json={"email": user.email, "role": "member"}
        )
        assert r.status_code == 201, r.text
    # Three users on essentiel: the fourth is refused, and nothing is written.
    await new_user(db, "fourth@example.com")
    r = await alice.client.post(
        f"/v1/orgs/{org_id}/members", json={"email": "fourth@example.com", "role": "member"}
    )
    assert (r.status_code, error(r)) == (409, "quota.users_exceeded")
    db.expire_all()
    members = await db.scalars(select(Membership).where(Membership.org_id == UUID(org_id)))
    assert len(list(members)) == 3
    # The third user reached 100%: audited once.
    warnings = await db.scalars(
        select(AuditEvent.data).where(AuditEvent.action == "quota.warning_100")
    )
    assert list(warnings) == [{"kind": "users"}]


async def test_a_workspace_member_added_to_the_org_is_no_new_user(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    alice, org_id = await stepped_up_owner(make_actor)
    workspace = await db.scalar(select(Workspace).where(Workspace.org_id == UUID(org_id)))
    assert workspace is not None
    for n in range(2):
        user = await new_user(db, f"ws{n}@example.com")
        db.add(Membership(user_id=user.id, workspace_id=workspace.id, role=Role.member))
    await db.commit()
    r = await alice.client.post(
        f"/v1/orgs/{org_id}/members", json={"email": "ws0@example.com", "role": "member"}
    )
    assert r.status_code == 201, r.text


async def test_org_ice_is_15_digits(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    org_id = await create_org(alice)
    for bad in ("12345", "00299988800001X", "٠٠٢٩٩٩٨٨٨٠٠٠٠١١", " " + ICE, None, 2999888000011):
        r = await alice.client.patch(f"/v1/orgs/{org_id}", json={"ice": bad})
        assert (r.status_code, error(r)) == (422, "org.ice_invalid"), bad
    r = await alice.client.patch(f"/v1/orgs/{org_id}", json={"ice": ICE})
    assert r.status_code == 200, r.text
    assert r.json()["ice"] == ICE
    org = await db.get(Organisation, UUID(org_id), populate_existing=True)
    assert org is not None and org.ice == ICE
    events = await db.scalars(select(AuditEvent.data).where(AuditEvent.action == "org.ice_changed"))
    assert list(events) == [{"ice": ICE}]


async def test_choose_plan_requires_org_ice(make_actor: MakeActor, db: AsyncSession) -> None:
    alice, org_id = await stepped_up_owner(make_actor)
    url = f"/v1/orgs/{org_id}/subscription"
    r = await alice.client.put(url, json={"plan_code": "pro"})
    assert (r.status_code, error(r)) == (422, "org.ice_required")
    assert (await alice.client.patch(f"/v1/orgs/{org_id}", json={"ice": ICE})).status_code == 200
    r = await alice.client.put(url, json={"plan_code": "gratuit"})
    assert (r.status_code, error(r)) == (422, "billing.plan_unknown")
    r = await alice.client.put(url, json={"plan_code": "pro"})
    assert r.status_code == 200, r.text
    # During the trial the plan is chosen for the first year; the quotas stay the trial's.
    assert (r.json()["plan_code"], r.json()["status"]) == ("pro", "trial")
    usage = (await alice.client.get(f"/v1/orgs/{org_id}/usage")).json()
    assert usage["plan_code"] == "essentiel"
    assert usage["quotas"]["users"]["limit"] == 3
    events = await db.scalars(
        select(AuditEvent.data).where(AuditEvent.action == "billing.plan_chosen")
    )
    assert list(events) == [{"from": "essentiel", "to": "pro", "effective": "trial_end"}]


async def test_choose_plan_needs_step_up(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    org_id = await create_org(alice)
    assert (await alice.client.patch(f"/v1/orgs/{org_id}", json={"ice": ICE})).status_code == 200
    r = await alice.client.put(f"/v1/orgs/{org_id}/subscription", json={"plan_code": "pro"})
    assert (r.status_code, error(r)) == (403, "auth.step_up_required")


async def test_active_upgrade_applies_now_and_downgrade_at_renewal(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    alice, org_id = await stepped_up_owner(make_actor)
    assert (await alice.client.patch(f"/v1/orgs/{org_id}", json={"ice": ICE})).status_code == 200
    sub = await db.scalar(select(Subscription).where(Subscription.org_id == UUID(org_id)))
    assert sub is not None
    sub.status = SubscriptionStatus.active
    await db.commit()
    url = f"/v1/orgs/{org_id}/subscription"
    r = await alice.client.put(url, json={"plan_code": "entreprise"})
    assert (r.json()["plan_code"], r.json()["pending_plan_code"]) == ("entreprise", None)
    r = await alice.client.put(url, json={"plan_code": "essentiel"})
    assert (r.json()["plan_code"], r.json()["pending_plan_code"]) == ("entreprise", "essentiel")
    r = await alice.client.put(url, json={"plan_code": "entreprise"})  # the downgrade undone
    assert (r.json()["plan_code"], r.json()["pending_plan_code"]) == ("entreprise", None)


async def _fill(db: AsyncSession, workspace_id: UUID, documents: int, big_bytes: int) -> None:
    """`documents` SAMPLE ledger rows, ingested now, one of them `big_bytes` long."""
    await db.execute(
        text(
            """
            INSERT INTO document_ledger (id, workspace_id, sha256, md5, size_bytes, content_kind,
                source, object_key, object_version_id, state, ingest_state, ingest_attempts,
                retention_until, ingested_at)
            SELECT gen_random_uuid(), :ws, md5(n::text || 'a') || md5(n::text || 'b'),
                md5(n::text), CASE WHEN n = 1 THEN CAST(:big AS bigint) ELSE 10 END, 'pdf',
                'upload',
                'k/' || n, 'v1', 'filed', 'consumed', 0, DATE '2036-12-31', now()
            FROM generate_series(1, :count) AS n
            """
        ),
        {"ws": workspace_id, "big": big_bytes, "count": documents},
    )
    await db.commit()


async def test_capture_never_blocked_over_quota(
    make_actor: MakeActor, db: AsyncSession, enqueuer: Any
) -> None:
    """At 150% of documents and storage, an upload is still accepted (202) and ingested."""
    alice = await make_actor("alice@example.com")
    org_id = await create_org(alice)
    workspace = await db.scalar(select(Workspace).where(Workspace.org_id == UUID(org_id)))
    assert workspace is not None
    await _fill(db, workspace.id, 3_000, 15 * GB // 2)
    usage = (await alice.client.get(f"/v1/orgs/{org_id}/usage")).json()["quotas"]
    assert (usage["documents"]["percent"], usage["documents"]["level"]) == (150, "over_100")
    assert (usage["storage"]["percent"], usage["storage"]["level"]) == (150, "over_100")
    r = await alice.client.post(
        f"/v1/workspaces/{workspace.id}/documents",
        files={"file": ("facture.pdf", PDF, "application/pdf")},
    )
    assert r.status_code == 202, r.text
    assert [w for _, w in enqueuer.ingests] == [workspace.id]
