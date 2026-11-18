"""`sijil-cp audit verify` (a workspace's chain, or `--platform`): 0 intact, 1 broken, 2 when it
could not verify at all."""

import asyncio
import base64
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from sijil_core import audit
from sijil_core.db.models import ActorType, Workspace
from sijil_cp import cli
from sijil_cp.settings import CpSettings
from tests.support.export_keys import TEST_PUBLIC_KEY_B64
from tests.support.seller import SAMPLE_SELLER

pytestmark = pytest.mark.db

NOW = datetime(2026, 11, 1, 12, tzinfo=UTC)
CLOSED_DB = "postgresql+asyncpg://sijil:sijil@127.0.0.1:1/sijil_cp"  # nothing listens


def _settings(url: str) -> CpSettings:
    return CpSettings.model_validate(
        {
            "region": "ma-cas",
            "environment": "test",
            "database_url": url,
            "redis_url": "redis://127.0.0.1:1/0",
            "master_key": base64.b64encode(bytes(range(32))).decode(),
            "cookie_secure": "false",
            "inbound_domain": "in.sijil.local",
            "objectstore_endpoint": "http://127.0.0.1:1",
            "objectstore_access_key": "test-access",
            "objectstore_secret_key": "test-secret",
            "objectstore_bucket": "sijil-originals-test",
            "export_public_key": TEST_PUBLIC_KEY_B64,
        }
        | SAMPLE_SELLER
    )


async def _append(db: AsyncSession, workspace: Workspace, n: int) -> None:
    for i in range(n):
        await audit.append(
            db,
            workspace_id=workspace.id,
            action="test.event",
            actor_type=ActorType.system,
            actor_id=None,
            data={"i": i},
            now=NOW,
        )
    await db.commit()


async def _verify(workspace_id: uuid.UUID, settings: CpSettings) -> int:
    # The CLI runs its own event loop.
    return await asyncio.to_thread(cli.audit_verify, workspace_id, settings)


async def test_intact_chain_exits_0(
    db: AsyncSession,
    workspace: Workspace,
    migrated_db: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    await _append(db, workspace, 3)
    assert await _verify(workspace.id, _settings(migrated_db.app_url)) == 0
    assert capsys.readouterr().out.strip() == "ok 3"


async def test_broken_chain_exits_1(
    db: AsyncSession,
    workspace: Workspace,
    migrated_db: Any,
    owner_conn: AsyncConnection,
    capsys: pytest.CaptureFixture[str],
) -> None:
    await _append(db, workspace, 3)
    await owner_conn.execute(
        text("ALTER TABLE audit_event DISABLE TRIGGER audit_event_append_only")
    )
    try:
        await owner_conn.execute(text("UPDATE audit_event SET data = '{\"i\": 9}' WHERE seq = 2"))
    finally:
        await owner_conn.execute(
            text("ALTER TABLE audit_event ENABLE TRIGGER audit_event_append_only")
        )
    assert await _verify(workspace.id, _settings(migrated_db.app_url)) == 1
    assert capsys.readouterr().out.strip() == "broken at seq 2"


async def test_unknown_workspace_exits_2(
    db: AsyncSession, migrated_db: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    # Not "ok 0": an empty chain for a mistyped id would read as a clean bill of health.
    assert await _verify(uuid.uuid4(), _settings(migrated_db.app_url)) == 2
    captured = capsys.readouterr()
    assert "unknown workspace" in captured.err
    assert not captured.out.startswith("ok")


async def test_database_unreachable_exits_2(capsys: pytest.CaptureFixture[str]) -> None:
    assert await _verify(uuid.uuid4(), _settings(CLOSED_DB)) == 2
    captured = capsys.readouterr()
    assert "database" in captured.err
    assert "sijil:sijil" not in captured.err  # never the connection string


# `sijil-cp audit verify --platform` ---------------------------------------------------------


async def _append_platform(db: AsyncSession, n: int) -> None:
    for i in range(n):
        await audit.append_platform(
            db, action="test.event", actor_type=ActorType.platform, actor_id=None,
            data={"i": i}, now=NOW,
        )  # fmt: skip
    await db.commit()


async def _verify_platform(settings: CpSettings) -> int:
    return await asyncio.to_thread(cli.audit_verify_platform, settings)


async def test_platform_chain_intact_exits_0(
    db: AsyncSession, migrated_db: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    await _append_platform(db, 3)
    assert await _verify_platform(_settings(migrated_db.app_url)) == 0
    assert capsys.readouterr().out.strip() == "ok 3"


async def test_platform_chain_broken_exits_1(
    db: AsyncSession, migrated_db: Any, owner_conn: AsyncConnection,
    capsys: pytest.CaptureFixture[str],
) -> None:  # fmt: skip
    await _append_platform(db, 3)
    table, trigger = "platform_audit_event", "platform_audit_event_append_only"
    await owner_conn.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER {trigger}"))
    try:
        await owner_conn.execute(
            text("UPDATE platform_audit_event SET data = '{\"i\": 9}' WHERE seq = 2")
        )
    finally:
        await owner_conn.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER {trigger}"))
    assert await _verify_platform(_settings(migrated_db.app_url)) == 1
    assert capsys.readouterr().out.strip() == "broken at seq 2"


async def test_platform_chain_database_unreachable_exits_2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert await _verify_platform(_settings(CLOSED_DB)) == 2
    captured = capsys.readouterr()
    assert "database" in captured.err
    assert "sijil:sijil" not in captured.err  # never the connection string
    assert not captured.out.startswith("ok")


def test_audit_verify_commands_are_wired(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[object] = []

    def workspace(workspace_id: uuid.UUID) -> int:
        seen.append(workspace_id)
        return 1

    def platform() -> int:
        seen.append("platform")
        return 0

    monkeypatch.setattr(cli, "audit_verify", workspace)
    monkeypatch.setattr(cli, "audit_verify_platform", platform)
    target = uuid.uuid4()
    with pytest.raises(SystemExit) as exited:
        cli.main(["audit", "verify", "--platform"])
    assert (exited.value.code, seen) == (0, ["platform"])
    with pytest.raises(SystemExit) as exited:
        cli.main(["audit", "verify", "--workspace", str(target)])
    assert (exited.value.code, seen) == (1, ["platform", target])
    # One chain or the other: never both, never neither.
    both = ["audit", "verify", "--platform", "--workspace", str(target)]
    for argv in (["audit", "verify"], both):
        with pytest.raises(SystemExit) as exited:
            cli.main(argv)
        assert exited.value.code == 2


# `sijil-cp search backfill` ----------------------------------------------------------------


async def _backfill(workspace_id: uuid.UUID, settings: CpSettings, enqueuer: Any) -> int:
    return await asyncio.to_thread(cli.search_backfill, workspace_id, settings, enqueuer)


async def test_search_backfill_enqueues_the_workspace(
    db: AsyncSession, workspace: Workspace, migrated_db: Any, enqueuer: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:  # fmt: skip
    assert await _backfill(workspace.id, _settings(migrated_db.app_url), enqueuer) == 0
    assert enqueuer.backfills == [workspace.id]
    assert capsys.readouterr().out.strip() == f"queued {workspace.id}"


async def test_search_backfill_of_an_unknown_workspace_exits_2(
    db: AsyncSession, migrated_db: Any, enqueuer: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    assert await _backfill(uuid.uuid4(), _settings(migrated_db.app_url), enqueuer) == 2
    assert enqueuer.backfills == []
    assert "unknown workspace" in capsys.readouterr().err


async def test_search_backfill_of_another_region_exits_2(
    db: AsyncSession, workspace: Workspace, migrated_db: Any, enqueuer: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:  # fmt: skip
    elsewhere = _settings(migrated_db.app_url).model_copy(update={"region": "fr-par"})
    assert await _backfill(workspace.id, elsewhere, enqueuer) == 2
    assert enqueuer.backfills == []
    assert "region" in capsys.readouterr().err


def test_search_backfill_command_is_wired(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[uuid.UUID] = []

    def fake(workspace_id: uuid.UUID) -> int:
        seen.append(workspace_id)
        return 0

    monkeypatch.setattr(cli, "search_backfill", fake)
    target = uuid.uuid4()
    with pytest.raises(SystemExit) as exited:
        cli.main(["search", "backfill", str(target)])
    assert (exited.value.code, seen) == (0, [target])


async def _staff(command: str, email: str, settings: CpSettings) -> int:
    fn = cli.staff_grant if command == "grant" else cli.staff_revoke
    return await asyncio.to_thread(fn, email, settings)


async def test_cli_grant_and_revoke(
    db: AsyncSession, migrated_db: Any, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    from sqlalchemy import select

    from sijil_core.db.models import PlatformAuditEvent, User

    settings = _settings(migrated_db.app_url)
    user = User(email="sam@example.com", name="SAMPLE", password_hash="x")
    db.add(user)
    await db.commit()

    async def is_staff() -> bool:
        await db.refresh(user)
        return user.is_staff

    user_id = user.id
    assert not await is_staff()
    # The email is matched as stored: case and spaces do not matter.
    assert await _staff("grant", " Sam@Example.com ", settings) == 0
    assert capsys.readouterr().out.strip() == f"granted {user.id}"
    assert await is_staff()
    # Granting again changes and audits nothing.
    assert await _staff("grant", "sam@example.com", settings) == 0
    assert capsys.readouterr().out.strip() == f"unchanged {user.id}"
    assert await _staff("revoke", "sam@example.com", settings) == 0
    assert capsys.readouterr().out.strip() == f"revoked {user.id}"
    assert not await is_staff()
    # An unknown email, or no database: exit code 2, nothing written.
    assert await _staff("grant", "nobody@example.com", settings) == 2
    assert "unknown user" in capsys.readouterr().err
    assert await _staff("revoke", "sam@example.com", _settings(CLOSED_DB)) == 2
    assert "database error" in capsys.readouterr().err

    # Audited on the platform's own chain (no org), by the platform operator; ids, no email.
    db.expire_all()
    events = list(await db.scalars(select(PlatformAuditEvent).order_by(PlatformAuditEvent.seq)))
    assert [(e.action, e.actor_type, e.actor_id) for e in events] == [
        ("staff.granted", ActorType.platform, None),
        ("staff.revoked", ActorType.platform, None),
    ]
    assert all((e.target_type, e.target_id, e.data) == ("user", str(user_id), {}) for e in events)
    assert (await audit.verify_platform(db)).ok

    # The commands are wired.
    seen: list[tuple[str, str]] = []

    def recorder(command: str) -> Callable[[str], int]:
        def fake(email: str) -> int:
            seen.append((command, email))
            return 0

        return fake

    monkeypatch.setattr(cli, "staff_grant", recorder("grant"))
    monkeypatch.setattr(cli, "staff_revoke", recorder("revoke"))
    for command in ("grant", "revoke"):
        with pytest.raises(SystemExit) as exited:
            cli.main(["staff", command, "sam@example.com"])
        assert exited.value.code == 0
    assert seen == [("grant", "sam@example.com"), ("revoke", "sam@example.com")]
