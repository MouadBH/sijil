"""Shared test fixtures: a throw-away Postgres database migrated as in production, and Redis.

The database mirrors infra/compose/postgres/init.sh: an owner role owns it and runs the
migrations, an app role gets DML only. Set SIJIL_TEST_DATABASE_URL to a superuser URL to
enable `db`-marked tests; they are skipped otherwise.
"""

import asyncio
import os
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core.db.base import Base
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    Organisation,
    OrgType,
    User,
    Workspace,
    WorkspaceState,
)

ROOT = Path(__file__).parent
CP_DIR = ROOT / "services" / "control-plane"
OWNER_ROLE = "sijil_test_owner"
APP_ROLE = "sijil_test_app"
# Throw-away roles on a local test cluster; not secrets.
OWNER_PASSWORD = "test-owner"  # noqa: S105
APP_PASSWORD = "test-app"  # noqa: S105
SKIP_REASON = "set SIJIL_TEST_DATABASE_URL"


@dataclass(frozen=True)
class TestDb:
    __test__ = False  # not a pytest class

    app_url: str
    owner_url: str


@dataclass(frozen=True)
class Fixtures:
    user: User
    org: Organisation
    workspace: Workspace


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    if os.environ.get("SIJIL_TEST_DATABASE_URL"):
        return
    skip = pytest.mark.skip(reason=SKIP_REASON)
    for item in items:
        if item.get_closest_marker("db"):
            item.add_marker(skip)


def _url_for(base: URL, user: str, password: str, database: str) -> str:
    return base.set(username=user, password=password, database=database).render_as_string(
        hide_password=False
    )


async def _admin(admin_url: str, statements: list[str], database: str | None = None) -> None:
    url = make_url(admin_url)
    if database:
        url = url.set(database=database)
    engine = create_async_engine(url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            for statement in statements:
                await conn.execute(text(statement))
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def migrated_db() -> Iterator[TestDb]:
    raw = os.environ.get("SIJIL_TEST_DATABASE_URL")
    if not raw:
        pytest.skip(SKIP_REASON)
    base = make_url(raw)
    name = f"sijil_test_{os.getpid()}"
    app_url = _url_for(base, APP_ROLE, APP_PASSWORD, name)
    owner_url = _url_for(base, OWNER_ROLE, OWNER_PASSWORD, name)

    def create_role(role: str, password: str) -> str:
        # Roles are cluster-wide and shared by concurrent runs: create if missing.
        return (
            f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN "
            f"CREATE ROLE {role} LOGIN PASSWORD '{password}'; END IF; END $$"
        )

    asyncio.run(
        _admin(
            raw,
            [
                create_role(OWNER_ROLE, OWNER_PASSWORD),
                create_role(APP_ROLE, APP_PASSWORD),
                f"DROP DATABASE IF EXISTS {name} WITH (FORCE)",
                f"CREATE DATABASE {name} OWNER {OWNER_ROLE}",
                f"REVOKE ALL ON DATABASE {name} FROM PUBLIC",
                f"GRANT CONNECT ON DATABASE {name} TO {APP_ROLE}",
            ],
        )
    )
    asyncio.run(
        _admin(
            raw,
            [
                "REVOKE CREATE ON SCHEMA public FROM PUBLIC",
                f"GRANT USAGE ON SCHEMA public TO {APP_ROLE}",
                f"ALTER DEFAULT PRIVILEGES FOR ROLE {OWNER_ROLE} IN SCHEMA public "
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {APP_ROLE}",
                f"ALTER DEFAULT PRIVILEGES FOR ROLE {OWNER_ROLE} IN SCHEMA public "
                f"GRANT USAGE, SELECT ON SEQUENCES TO {APP_ROLE}",
            ],
            database=name,
        )
    )
    try:
        subprocess.run(  # noqa: S603
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=CP_DIR,
            env={
                **os.environ,
                "SIJIL_MIGRATION_DATABASE_URL": owner_url,
                "SIJIL_REGION": "ma-cas",
                "SIJIL_ENVIRONMENT": "test",
            },
            check=True,
        )
        yield TestDb(app_url=app_url, owner_url=owner_url)
    finally:
        asyncio.run(_admin(raw, [f"DROP DATABASE IF EXISTS {name} WITH (FORCE)"]))


@pytest.fixture
async def owner_conn(migrated_db: TestDb) -> AsyncIterator[AsyncConnection]:
    # AUTOCOMMIT: no transaction left holding locks that would block the table cleanup.
    engine = create_async_engine(migrated_db.owner_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as conn:
        yield conn
    await engine.dispose()


@pytest.fixture
async def db(migrated_db: TestDb) -> AsyncIterator[AsyncSession]:
    engine = make_engine(migrated_db.app_url)
    owner = create_async_engine(migrated_db.owner_url, isolation_level="AUTOCOMMIT")
    async with make_sessionmaker(engine)() as session:
        yield session
        await session.rollback()
    tables = ", ".join(f'"{t.name}"' for t in Base.metadata.sorted_tables)
    async with owner.connect() as conn:
        await conn.execute(text(f"TRUNCATE {tables} CASCADE"))
    await owner.dispose()
    await engine.dispose()


@pytest.fixture
async def redis() -> AsyncIterator[Redis]:
    url = os.environ.get("SIJIL_TEST_REDIS_URL")
    if not url:
        pytest.skip("set SIJIL_TEST_REDIS_URL")
    client = Redis.from_url(url)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest.fixture
async def user(db: AsyncSession) -> User:
    row = User(email=f"{uuid.uuid4().hex[:8]}@example.com", name="Test", password_hash="x")
    db.add(row)
    await db.commit()
    return row


@pytest.fixture
async def org(db: AsyncSession) -> Organisation:
    row = Organisation(name="Atlas", type=OrgType.business, country="MA", region="ma-cas")
    db.add(row)
    await db.commit()
    return row


@pytest.fixture
async def workspace(db: AsyncSession, org: Organisation) -> Workspace:
    slug = f"ws-{uuid.uuid4().hex[:8]}"
    row = Workspace(
        org_id=org.id,
        name="Atlas SARL",
        slug=slug,
        state=WorkspaceState.active,
        db_name=f"paperless_{slug}",
        index_db_name=f"index_{slug}",
        kms_key_id=f"kek-{slug}",
    )
    db.add(row)
    await db.commit()
    return row


@pytest.fixture
async def fixtures(user: User, org: Organisation, workspace: Workspace) -> Fixtures:
    return Fixtures(user=user, org=org, workspace=workspace)
