"""`sijil-cp audit verify`: 0 intact, 1 broken, 2 when it could not verify at all."""

import asyncio
import base64
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from sijil_core import audit
from sijil_core.db.models import ActorType, Workspace
from sijil_cp import cli
from sijil_cp.settings import CpSettings

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
        }
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
