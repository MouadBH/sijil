from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import Membership, Role

pytestmark = pytest.mark.db

MakeActor = Callable[[str], Awaitable[Any]]


def error(r: Any) -> str:
    code: str = r.json()["error"]["code"]
    return code


async def create_org(actor: Any) -> str:
    r = await actor.client.post(
        "/v1/orgs", json={"name": "Atlas SARL", "type": "business", "country": "MA"}
    )
    assert r.status_code == 202
    org_id: str = r.json()["org"]["id"]
    return org_id


async def add_member(db: AsyncSession, org_id: str, actor: Any, role: Role) -> None:
    db.add(Membership(user_id=actor.user_id, org_id=UUID(org_id), role=role))
    await db.commit()


async def role_of(db: AsyncSession, org_id: str, actor: Any) -> Role | None:
    db.expire_all()
    return await db.scalar(
        select(Membership.role).where(
            Membership.org_id == UUID(org_id), Membership.user_id == actor.user_id
        )
    )


async def test_owner_lists_and_adds_members(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    org_id = await create_org(alice)
    await alice.enable_totp()
    await alice.step_up()
    r = await alice.client.post(
        f"/v1/orgs/{org_id}/members", json={"email": " Bob@Example.COM ", "role": "member"}
    )
    assert r.status_code == 201, r.text
    assert r.json()["role"] == "member"
    assert await role_of(db, org_id, bob) == Role.member
    listed = await alice.client.get(f"/v1/orgs/{org_id}/members")
    assert listed.status_code == 200
    assert {m["email"]: m["role"] for m in listed.json()} == {
        "alice@example.com": "owner",
        "bob@example.com": "member",
    }


async def test_add_member_unknown_user(make_actor: MakeActor) -> None:
    alice = await make_actor("alice@example.com")
    org_id = await create_org(alice)
    await alice.enable_totp()
    await alice.step_up()
    r = await alice.client.post(
        f"/v1/orgs/{org_id}/members", json={"email": "ghost@example.com", "role": "member"}
    )
    assert r.status_code == 404
    assert error(r) == "member.user_not_found"


async def test_add_member_needs_step_up(make_actor: MakeActor) -> None:
    alice = await make_actor("alice@example.com")
    await make_actor("bob@example.com")
    org_id = await create_org(alice)
    r = await alice.client.post(
        f"/v1/orgs/{org_id}/members", json={"email": "bob@example.com", "role": "member"}
    )
    assert r.status_code == 403
    assert error(r) == "auth.step_up_required"


async def test_admin_cannot_grant_owner(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    carol = await make_actor("carol@example.com")
    org_id = await create_org(alice)
    await add_member(db, org_id, bob, Role.admin)
    await add_member(db, org_id, carol, Role.member)
    await bob.enable_totp()
    await bob.step_up()
    r = await bob.client.post(
        f"/v1/orgs/{org_id}/members", json={"email": "carol@example.com", "role": "owner"}
    )
    # carol is already a member, but the refusal must come first and be the role one.
    assert r.status_code == 403
    assert error(r) == "member.forbidden_role"
    r = await bob.client.patch(f"/v1/orgs/{org_id}/members/{carol.user_id}", json={"role": "owner"})
    assert r.status_code == 403
    assert error(r) == "member.forbidden_role"
    assert await role_of(db, org_id, carol) == Role.member


async def test_admin_cannot_touch_an_owner(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    carol = await make_actor("carol@example.com")
    org_id = await create_org(alice)
    await add_member(db, org_id, bob, Role.admin)
    await add_member(db, org_id, carol, Role.owner)
    await bob.enable_totp()
    await bob.step_up()
    r = await bob.client.patch(
        f"/v1/orgs/{org_id}/members/{carol.user_id}", json={"role": "member"}
    )
    assert (r.status_code, error(r)) == (403, "member.forbidden_role")
    r = await bob.client.delete(f"/v1/orgs/{org_id}/members/{carol.user_id}")
    assert (r.status_code, error(r)) == (403, "member.forbidden_role")
    assert await role_of(db, org_id, carol) == Role.owner


async def test_member_cannot_manage_members(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    org_id = await create_org(alice)
    await add_member(db, org_id, bob, Role.member)
    r = await bob.client.get(f"/v1/orgs/{org_id}/members")
    assert (r.status_code, error(r)) == (403, "forbidden")


async def test_non_member_sees_404_on_member_routes(make_actor: MakeActor) -> None:
    alice = await make_actor("alice@example.com")
    eve = await make_actor("eve@example.com")
    org_id = await create_org(alice)
    assert (await eve.client.get(f"/v1/orgs/{org_id}/members")).status_code == 404
    r = await eve.client.delete(f"/v1/orgs/{org_id}/members/{alice.user_id}")
    assert (r.status_code, error(r)) == (404, "not_found")


async def test_cannot_remove_last_owner(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    org_id = await create_org(alice)
    await alice.enable_totp()
    await alice.step_up()
    r = await alice.client.delete(f"/v1/orgs/{org_id}/members/{alice.user_id}")
    assert (r.status_code, error(r)) == (409, "org.last_owner")
    r = await alice.client.patch(
        f"/v1/orgs/{org_id}/members/{alice.user_id}", json={"role": "admin"}
    )
    assert (r.status_code, error(r)) == (409, "org.last_owner")
    assert await role_of(db, org_id, alice) == Role.owner


async def test_owner_can_step_down_when_another_owner_exists(
    make_actor: MakeActor, db: AsyncSession
) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    org_id = await create_org(alice)
    await add_member(db, org_id, bob, Role.owner)
    await alice.enable_totp()
    await alice.step_up()
    r = await alice.client.patch(
        f"/v1/orgs/{org_id}/members/{alice.user_id}", json={"role": "admin"}
    )
    assert r.status_code == 200
    await bob.enable_totp()
    await bob.step_up()
    r = await bob.client.delete(f"/v1/orgs/{org_id}/members/{bob.user_id}")
    assert (r.status_code, error(r)) == (409, "org.last_owner")
    assert await role_of(db, org_id, alice) == Role.admin


async def test_step_up_required_for_role_change(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    org_id = await create_org(alice)
    await add_member(db, org_id, bob, Role.member)
    r = await alice.client.patch(f"/v1/orgs/{org_id}/members/{bob.user_id}", json={"role": "admin"})
    assert (r.status_code, error(r)) == (403, "auth.step_up_required")
    r = await alice.client.delete(f"/v1/orgs/{org_id}/members/{bob.user_id}")
    assert (r.status_code, error(r)) == (403, "auth.step_up_required")
    assert await role_of(db, org_id, bob) == Role.member
    await alice.enable_totp()
    await alice.step_up()
    r = await alice.client.patch(f"/v1/orgs/{org_id}/members/{bob.user_id}", json={"role": "admin"})
    assert r.status_code == 200
    assert await role_of(db, org_id, bob) == Role.admin
    r = await alice.client.delete(f"/v1/orgs/{org_id}/members/{bob.user_id}")
    assert r.status_code == 204
    assert await role_of(db, org_id, bob) is None


async def test_patch_unknown_member_404(make_actor: MakeActor) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    org_id = await create_org(alice)
    await alice.enable_totp()
    await alice.step_up()
    r = await alice.client.patch(f"/v1/orgs/{org_id}/members/{bob.user_id}", json={"role": "admin"})
    assert (r.status_code, error(r)) == (404, "not_found")
