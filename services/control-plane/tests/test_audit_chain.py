import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from sijil_core import audit
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import ActorType, AuditEvent, Workspace
from sijil_cp.cli import main

pytestmark = pytest.mark.db

MakeActor = Callable[[str], Awaitable[Any]]
NOW = datetime(2026, 11, 1, 10, 0, 0, 123456, tzinfo=UTC)
TRIGGER = "audit_event_append_only"


async def _append(db: AsyncSession, ws: uuid.UUID, n: int = 1) -> list[AuditEvent]:
    out = []
    for i in range(n):
        out.append(
            await audit.append(
                db,
                workspace_id=ws,
                action="document.viewed",
                actor_type=ActorType.system,
                actor_id=None,
                data={"i": i, "نص": "عربي"},
                now=NOW,
            )
        )
    await db.commit()
    return out


async def _owner_edit(conn: AsyncConnection, sql: str) -> None:
    """Test-only: the owner bypasses the append-only trigger to simulate tampering."""
    await conn.execute(text(f"ALTER TABLE audit_event DISABLE TRIGGER {TRIGGER}"))
    try:
        await conn.execute(text(sql))
    finally:
        await conn.execute(text(f"ALTER TABLE audit_event ENABLE TRIGGER {TRIGGER}"))


async def test_chain_links(db: AsyncSession, workspace: Workspace) -> None:
    ws_id = workspace.id
    events = await _append(db, ws_id, 3)
    assert [e.seq for e in events] == [1, 2, 3]
    assert events[0].prev_hash == "0" * 64
    assert events[1].prev_hash == events[0].hash
    assert events[2].prev_hash == events[1].hash
    db.expire_all()
    result = await audit.verify(db, ws_id)
    assert (result.ok, result.count, result.first_bad_seq) == (True, 3, None)


async def test_tamper_detected_by_verify(
    db: AsyncSession, workspace: Workspace, owner_conn: AsyncConnection
) -> None:
    ws_id = workspace.id
    await _append(db, ws_id, 3)
    await _owner_edit(owner_conn, "UPDATE audit_event SET data = '{\"i\": 99}' WHERE seq = 2")
    db.expire_all()
    result = await audit.verify(db, ws_id)
    assert (result.ok, result.first_bad_seq) == (False, 2)


async def test_gap_detected(
    db: AsyncSession, workspace: Workspace, owner_conn: AsyncConnection
) -> None:
    ws_id = workspace.id
    await _append(db, ws_id, 3)
    await _owner_edit(owner_conn, "DELETE FROM audit_event WHERE seq = 2")
    db.expire_all()
    result = await audit.verify(db, ws_id)
    assert (result.ok, result.first_bad_seq) == (False, 3)


async def test_broken_link_detected(
    db: AsyncSession, workspace: Workspace, owner_conn: AsyncConnection
) -> None:
    ws_id = workspace.id
    await _append(db, ws_id, 3)
    await _owner_edit(
        owner_conn, "UPDATE audit_event SET prev_hash = repeat('f', 64) WHERE seq = 3"
    )
    db.expire_all()
    result = await audit.verify(db, ws_id)
    assert (result.ok, result.first_bad_seq) == (False, 3)


async def test_parallel_appends_keep_single_chain(
    db: AsyncSession, workspace: Workspace, migrated_db: Any
) -> None:
    engine = make_engine(migrated_db.app_url)
    maker = make_sessionmaker(engine)

    async def one(i: int) -> None:
        async with maker() as s:
            await audit.append(
                s,
                workspace_id=workspace.id,
                action="document.viewed",
                actor_type=ActorType.system,
                actor_id=None,
                data={"i": i},
                now=NOW,
            )
            await s.commit()

    try:
        await asyncio.gather(*(one(i) for i in range(20)))
    finally:
        await engine.dispose()
    seqs = list(await db.scalars(select(AuditEvent.seq).order_by(AuditEvent.seq)))
    assert seqs == list(range(1, 21))
    result = await audit.verify(db, workspace.id)
    assert (result.ok, result.count) == (True, 20)


async def test_member_change_is_audited(make_actor: MakeActor, db: AsyncSession) -> None:
    alice = await make_actor("alice@example.com")
    bob = await make_actor("bob@example.com")
    r = await alice.client.post(
        "/v1/orgs", json={"name": "Atlas SARL", "type": "business", "country": "MA"}
    )
    org_id = r.json()["org"]["id"]
    ws_id = UUID(r.json()["workspace"]["id"])
    await alice.enable_totp()
    await alice.step_up()
    r = await alice.client.post(
        f"/v1/orgs/{org_id}/members", json={"email": bob.email, "role": "member"}
    )
    assert r.status_code == 201
    r = await alice.client.patch(f"/v1/orgs/{org_id}/members/{bob.user_id}", json={"role": "admin"})
    assert r.status_code == 200
    r = await alice.client.patch(f"/v1/orgs/{org_id}", json={"require_totp": True})
    assert r.status_code == 200
    r = await alice.client.delete(f"/v1/orgs/{org_id}/members/{bob.user_id}")
    assert r.status_code == 204
    db.expire_all()
    events = list(
        await db.scalars(
            select(AuditEvent).where(AuditEvent.workspace_id == ws_id).order_by(AuditEvent.seq)
        )
    )
    assert [e.action for e in events] == [
        "member.added",
        "member.role_changed",
        "org.require_totp_changed",
        "member.removed",
    ]
    assert all(e.actor_type == ActorType.user and e.actor_id == str(alice.user_id) for e in events)
    assert events[0].target_id == str(bob.user_id)
    assert (await audit.verify(db, ws_id)).ok


async def test_cli_verify_exit_codes(
    db: AsyncSession,
    workspace: Workspace,
    owner_conn: AsyncConnection,
    migrated_db: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    await _append(db, workspace.id, 2)
    monkeypatch.setenv("SIJIL_DATABASE_URL", migrated_db.app_url)
    monkeypatch.setenv("SIJIL_REGION", "ma-cas")
    monkeypatch.setenv("SIJIL_ENVIRONMENT", "test")
    monkeypatch.setenv("SIJIL_MASTER_KEY", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=")
    monkeypatch.setenv("SIJIL_REDIS_URL", "redis://127.0.0.1:1/0")
    monkeypatch.setenv("SIJIL_COOKIE_SECURE", "false")
    monkeypatch.setenv("SIJIL_INBOUND_DOMAIN", "in.sijil.local")
    monkeypatch.setenv("SIJIL_OBJECTSTORE_ENDPOINT", "http://127.0.0.1:1")
    monkeypatch.setenv("SIJIL_OBJECTSTORE_ACCESS_KEY", "test-access")
    monkeypatch.setenv("SIJIL_OBJECTSTORE_SECRET_KEY", "test-secret")
    monkeypatch.setenv("SIJIL_OBJECTSTORE_BUCKET", "sijil-originals-test")
    loop = asyncio.get_running_loop()

    def run() -> int:
        try:
            main(["audit", "verify", "--workspace", str(workspace.id)])
        except SystemExit as exc:
            return int(exc.code or 0)
        return 0

    assert await loop.run_in_executor(None, run) == 0
    assert capsys.readouterr().out.strip() == "ok 2"
    await _owner_edit(owner_conn, "UPDATE audit_event SET action = 'x' WHERE seq = 2")
    assert await loop.run_in_executor(None, run) == 1
    assert capsys.readouterr().out.strip() == "broken at seq 2"


@pytest.mark.parametrize("bad", [1e20, 1.5, float("nan"), float("inf"), {"a": [0.1]}])
async def test_float_data_rejected_and_nothing_written(
    db: AsyncSession, workspace: Workspace, bad: Any
) -> None:
    with pytest.raises(TypeError):
        await audit.append(
            db,
            workspace_id=workspace.id,
            action="document.viewed",
            actor_type=ActorType.system,
            actor_id=None,
            data={"x": bad},
            now=NOW,
        )
    await db.rollback()
    assert list(await db.scalars(select(AuditEvent))) == []


async def test_naive_now_rejected(db: AsyncSession, workspace: Workspace) -> None:
    with pytest.raises(ValueError, match="timezone"):
        await audit.append(
            db,
            workspace_id=workspace.id,
            action="document.viewed",
            actor_type=ActorType.system,
            actor_id=None,
            now=datetime(2026, 11, 1, 10, 0, 0),  # noqa: DTZ001
        )
    await db.rollback()
    assert list(await db.scalars(select(AuditEvent))) == []


async def test_roundtrip_through_jsonb_verifies(
    db: AsyncSession, workspace: Workspace, migrated_db: Any
) -> None:
    await audit.append(
        db,
        workspace_id=workspace.id,
        action="document.viewed",
        actor_type=ActorType.user,
        actor_id="u",
        data={
            "big": 10**20,
            "neg": -(10**25),
            "nested": {"ب": [1, {"t": True, "f": False, "n": None}], "نص": "عربي"},
        },
        now=NOW,
    )
    await db.commit()
    engine = make_engine(migrated_db.app_url)
    try:
        async with make_sessionmaker(engine)() as fresh:
            result = await audit.verify(fresh, workspace.id)
    finally:
        await engine.dispose()
    assert (result.ok, result.count) == (True, 1)
