from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

import pytest
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import (
    Job,
    JobKind,
    JobStatus,
    Membership,
    Organisation,
    OrgType,
    Role,
    Workspace,
    WorkspaceState,
)
from sijil_cp.api import orgs
from sijil_cp.enqueue import CeleryEnqueuer

pytestmark = pytest.mark.db

# `Any` stands for the Actor/RecordingEnqueuer test helpers defined in conftest.
MakeActor = Callable[[str], Awaitable[Any]]


def error(r: Any) -> str:
    code: str = r.json()["error"]["code"]
    return code


async def create_org(actor: Any, **over: Any) -> dict[str, Any]:
    body = {"name": "Atlas SARL", "type": "business", "country": "MA"} | over
    r = await actor.client.post("/v1/orgs", json=body)
    assert r.status_code == 202, r.text
    data: dict[str, Any] = r.json()
    return data


async def test_create_business_org_creates_workspace_and_job(
    make_actor: MakeActor, enqueuer: Any, db: AsyncSession
) -> None:
    alice = await make_actor("alice@example.com")
    data = await create_org(alice)
    org, ws = data["org"], data["workspace"]
    assert org["region"] == "ma-cas"
    assert org["type"] == "business"
    assert ws["state"] == "provisioning"
    assert ws["role"] == "owner"

    row = await db.scalar(select(Workspace).where(Workspace.id == UUID(ws["id"])))
    assert row is not None
    assert row.slug.startswith("atlas-sarl-")
    assert len(row.slug.rsplit("-", 1)[1]) == 6
    slug_ = row.slug.replace("-", "_")
    assert row.db_name == f"paperless_{slug_}"
    assert row.index_db_name == f"index_{slug_}"
    assert row.kms_key_id == f"ws-{row.id}"
    assert row.paperless_url is None

    member = await db.scalar(select(Membership).where(Membership.org_id == UUID(org["id"])))
    assert member is not None
    assert member.role == Role.owner
    assert member.user_id == alice.user_id

    job = await db.scalar(select(Job).where(Job.id == UUID(data["job_id"])))
    assert job is not None
    assert job.status == JobStatus.queued
    assert job.kind == JobKind.provision_workspace
    assert job.workspace_id == row.id
    assert enqueuer.calls == [(JobKind.provision_workspace, job.id, row.id)]


async def test_create_org_requires_login(
    cp_app: FastAPI, csrf_client_for: Callable[..., Any]
) -> None:
    async with csrf_client_for(cp_app) as c:
        r = await c.post("/v1/orgs", json={"name": "A", "type": "business", "country": "MA"})
    assert r.status_code == 401


async def test_default_enqueuer_is_celery(make_app: Callable[..., FastAPI]) -> None:
    assert isinstance(make_app().state.enqueuer, CeleryEnqueuer)


async def test_firm_org_rejected(make_actor: MakeActor, enqueuer: Any) -> None:
    alice = await make_actor("alice@example.com")
    r = await alice.client.post("/v1/orgs", json={"name": "Fid", "type": "firm", "country": "MA"})
    assert r.status_code == 422
    assert error(r) == "org.type_unsupported"
    assert enqueuer.calls == []


async def test_second_workspace_rejected_for_business(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    alice = await make_actor("alice@example.com")
    org = (await create_org(alice))["org"]
    r = await alice.client.post("/v1/workspaces", json={"org_id": org["id"], "name": "Second"})
    assert r.status_code == 409
    assert error(r) == "org.workspace_limit"
    assert len((await db.scalars(select(Workspace))).all()) == 1


async def test_org_in_other_region_refused(
    make_actor: MakeActor, monkeypatch: pytest.MonkeyPatch, enqueuer: Any, db: AsyncSession
) -> None:
    monkeypatch.setitem(orgs.COUNTRY_REGIONS, "FR", "eu-fra")
    alice = await make_actor("alice@example.com")
    r = await alice.client.post(
        "/v1/orgs", json={"name": "Paris", "type": "business", "country": "FR"}
    )
    assert r.status_code == 421
    assert error(r) == "region.mismatch"
    assert (await db.scalars(select(Organisation))).all() == []
    assert enqueuer.calls == []


async def test_unknown_country_rejected(make_actor: MakeActor) -> None:
    alice = await make_actor("alice@example.com")
    r = await alice.client.post("/v1/orgs", json={"name": "X", "type": "business", "country": "ZZ"})
    assert r.status_code == 422


async def test_org_region_cannot_change_via_api(make_actor: MakeActor) -> None:
    alice = await make_actor("alice@example.com")
    org = (await create_org(alice))["org"]
    r = await alice.client.patch(f"/v1/orgs/{org['id']}", json={"region": "eu-fra"})
    assert r.status_code == 409
    assert error(r) == "org.region_immutable"
    got = await alice.client.get(f"/v1/orgs/{org['id']}")
    assert got.json()["region"] == "ma-cas"


async def test_patch_org_name_owner_only(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    org = (await create_org(alice))["org"]
    db.add(Membership(user_id=bob.user_id, org_id=UUID(org["id"]), role=Role.admin))
    await db.commit()
    r = await bob.client.patch(f"/v1/orgs/{org['id']}", json={"name": "Hijack"})
    assert r.status_code == 403
    assert error(r) == "forbidden"
    r = await alice.client.patch(f"/v1/orgs/{org['id']}", json={"name": "Renamed"})
    assert r.status_code == 200
    assert r.json()["name"] == "Renamed"


async def test_require_totp_toggle_needs_step_up(make_actor: MakeActor) -> None:
    alice = await make_actor("alice@example.com")
    org = (await create_org(alice))["org"]
    url = f"/v1/orgs/{org['id']}"
    r = await alice.client.patch(url, json={"require_totp": True})
    assert r.status_code == 403
    assert error(r) == "auth.step_up_required"
    await alice.enable_totp()
    await alice.step_up()
    r = await alice.client.patch(url, json={"require_totp": True})
    assert r.status_code == 200
    assert r.json()["require_totp"] is True


async def test_totp_enrolment_required_on_org_workspace_and_job_routes(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    alice = await make_actor("alice@example.com")
    data = await create_org(alice)
    org_id, ws_id = data["org"]["id"], data["workspace"]["id"]
    org = await db.get(Organisation, UUID(org_id))
    assert org is not None
    org.require_totp = True
    await db.commit()
    for path in (f"/v1/orgs/{org_id}", f"/v1/workspaces/{ws_id}", f"/v1/jobs/{data['job_id']}"):
        r = await alice.client.get(path)
        assert r.status_code == 403, path
        assert error(r) == "auth.totp_enrolment_required"


async def test_org_read_hides_existence(make_actor: MakeActor) -> None:
    alice = await make_actor("alice@example.com")
    mallory = await make_actor("mallory@example.com")
    org = (await create_org(alice))["org"]
    r = await mallory.client.get(f"/v1/orgs/{org['id']}")
    assert r.status_code == 404
    assert error(r) == "not_found"
    r = await mallory.client.post("/v1/workspaces", json={"org_id": org["id"], "name": "Mine"})
    assert r.status_code == 404


async def test_cross_workspace_access_404(make_actor: MakeActor) -> None:
    alice = await make_actor("alice@example.com")
    mallory = await make_actor("mallory@example.com")
    ws = (await create_org(alice))["workspace"]
    r = await mallory.client.get(f"/v1/workspaces/{ws['id']}")
    assert r.status_code == 404
    assert error(r) == "not_found"
    missing = await mallory.client.get("/v1/workspaces/00000000-0000-0000-0000-000000000000")
    assert missing.status_code == 404
    assert missing.json() == r.json()


async def test_cross_region_workspace_refused(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    mallory = await make_actor("mallory@example.com")
    org = Organisation(name="Paris", type=OrgType.business, country="FR", region="eu-fra")
    db.add(org)
    await db.flush()
    ws = Workspace(
        org_id=org.id,
        name="Paris",
        slug="paris-abc123",
        state=WorkspaceState.active,
        db_name="paperless_paris_abc123",
        index_db_name="index_paris_abc123",
        kms_key_id="ws-x",
    )
    db.add(ws)
    await db.flush()
    db.add(Membership(user_id=alice.user_id, org_id=org.id, role=Role.owner))
    await db.commit()
    r = await alice.client.get(f"/v1/workspaces/{ws.id}")
    assert r.status_code == 421
    assert error(r) == "region.mismatch"
    # A non-member still learns nothing about it.
    assert (await mallory.client.get(f"/v1/workspaces/{ws.id}")).status_code == 404


async def test_workspace_out_never_exposes_paperless_url(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    alice = await make_actor("alice@example.com")
    ws = (await create_org(alice))["workspace"]
    row = await db.get(Workspace, UUID(ws["id"]))
    assert row is not None
    row.paperless_url = "http://tenant-secret:8000"
    await db.commit()
    r = await alice.client.get(f"/v1/workspaces/{ws['id']}")
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"id", "org_id", "name", "slug", "state", "role"}
    assert "tenant-secret" not in r.text
    assert body["role"] == "owner"


async def test_workspace_role_is_higher_of_org_and_workspace(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    data = await create_org(alice)
    ws_id = UUID(data["workspace"]["id"])
    db.add(Membership(user_id=bob.user_id, workspace_id=ws_id, role=Role.readonly))
    db.add(Membership(user_id=bob.user_id, org_id=UUID(data["org"]["id"]), role=Role.member))
    await db.commit()
    r = await bob.client.get(f"/v1/workspaces/{ws_id}")
    assert r.json()["role"] == "member"


async def test_job_visible_only_to_members(make_actor: MakeActor) -> None:
    alice = await make_actor("alice@example.com")
    mallory = await make_actor("mallory@example.com")
    data = await create_org(alice)
    r = await alice.client.get(f"/v1/jobs/{data['job_id']}")
    assert r.status_code == 200
    assert r.json()["status"] == "queued"
    assert r.json()["kind"] == "provision_workspace"
    r = await mallory.client.get(f"/v1/jobs/{data['job_id']}")
    assert r.status_code == 404
    assert error(r) == "not_found"
