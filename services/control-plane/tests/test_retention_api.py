"""Retention policies per document type (ARC-3): read, raise, never below the locked preset."""

import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import AuditEvent, Membership, RetentionPolicy, Role
from sijil_core.fields import DOCUMENT_TYPES

pytestmark = pytest.mark.db

MakeActor = Callable[[str], Awaitable[Any]]


def _code(r: Any) -> str:
    return str(r.json()["error"]["code"])


async def _create(actor: Any) -> str:
    r = await actor.client.post(
        "/v1/orgs", json={"name": "Atlas SARL", "type": "business", "country": "MA"}
    )
    assert r.status_code == 202, r.text
    return str(r.json()["workspace"]["id"])


async def _owner(make_actor: MakeActor) -> tuple[Any, str]:
    alice = await make_actor("alice@example.com")
    workspace_id = await _create(alice)
    await alice.enable_totp()
    await alice.step_up()
    return alice, workspace_id


async def _changes(db: AsyncSession, workspace_id: str) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(
            AuditEvent.workspace_id == uuid.UUID(workspace_id),
            AuditEvent.action == "retention.policy_changed",
        )
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def test_new_workspace_gets_locked_presets(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    workspace_id = await _create(alice)
    rows = list(
        await db.scalars(
            select(RetentionPolicy).where(RetentionPolicy.workspace_id == uuid.UUID(workspace_id))
        )
    )
    assert sorted(r.document_type for r in rows) == sorted(DOCUMENT_TYPES)
    assert {(r.years, r.preset_years, r.locked) for r in rows} == {(10, 10, True)}


async def test_list_policies(make_actor: MakeActor) -> None:
    alice, workspace_id = await _owner(make_actor)
    r = await alice.client.get(f"/v1/workspaces/{workspace_id}/retention/policies")
    assert r.status_code == 200, r.text
    assert [p["document_type"] for p in r.json()] == list(DOCUMENT_TYPES)
    assert {(p["years"], p["preset_years"], p["locked"]) for p in r.json()} == {(10, 10, True)}
    assert all(set(p) == {"document_type", "years", "preset_years", "locked"} for p in r.json())


async def test_put_policy_raises_years_and_audits(make_actor: MakeActor, db: AsyncSession) -> None:
    alice, workspace_id = await _owner(make_actor)
    url = f"/v1/workspaces/{workspace_id}/retention/policies/invoice"
    r = await alice.client.put(url, json={"years": 12})
    assert r.status_code == 200, r.text
    assert r.json() == {"document_type": "invoice", "years": 12, "preset_years": 10, "locked": True}
    listed = await alice.client.get(f"/v1/workspaces/{workspace_id}/retention/policies")
    assert {p["document_type"]: p["years"] for p in listed.json()}["invoice"] == 12
    [event] = await _changes(db, workspace_id)
    assert event.data == {"document_type": "invoice", "years": 12, "previous": 10}
    assert event.actor_id == str(alice.user_id)
    # Back down to the preset is allowed, and audited with what it replaced.
    assert (await alice.client.put(url, json={"years": 10})).status_code == 200
    assert (await _changes(db, workspace_id))[-1].data == {
        "document_type": "invoice",
        "years": 10,
        "previous": 12,
    }


async def test_put_policy_below_preset_is_422(make_actor: MakeActor, db: AsyncSession) -> None:
    alice, workspace_id = await _owner(make_actor)
    base = f"/v1/workspaces/{workspace_id}/retention/policies"
    r = await alice.client.put(f"{base}/receipt", json={"years": 9})
    assert r.status_code == 422
    assert _code(r) == "retention.policy_below_preset"
    assert await _changes(db, workspace_id) == []
    listed = await alice.client.get(base)
    assert {p["document_type"]: p["years"] for p in listed.json()}["receipt"] == 10
    # Outside 1..50 is refused too, and an unknown type is not found.
    assert (await alice.client.put(f"{base}/receipt", json={"years": 51})).status_code == 422
    assert (await alice.client.put(f"{base}/nope", json={"years": 12})).status_code == 404


async def test_put_policy_requires_step_up(make_actor: MakeActor, clock: Any) -> None:
    alice, workspace_id = await _owner(make_actor)
    clock.advance(timedelta(minutes=11))
    base = f"/v1/workspaces/{workspace_id}/retention/policies"
    r = await alice.client.put(f"{base}/invoice", json={"years": 12})
    assert r.status_code == 403
    assert _code(r) == "auth.step_up_required"
    # Reading needs no step-up.
    assert (await alice.client.get(base)).status_code == 200


async def test_member_cannot_change_policy(make_actor: MakeActor, db: AsyncSession) -> None:
    _, workspace_id = await _owner(make_actor)
    bob = await make_actor("bob@example.com")
    db.add(Membership(user_id=bob.user_id, workspace_id=uuid.UUID(workspace_id), role=Role.member))
    await db.commit()
    base = f"/v1/workspaces/{workspace_id}/retention/policies"
    r = await bob.client.put(f"{base}/invoice", json={"years": 12})
    assert r.status_code == 403
    assert _code(r) == "forbidden"
    assert (await bob.client.get(base)).status_code == 403
