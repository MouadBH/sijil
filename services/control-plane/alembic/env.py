import asyncio

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from sijil_core.db import models  # noqa: F401  (registers the tables on Base.metadata)
from sijil_core.db.base import Base
from sijil_cp.settings import MigrationSettings

target_metadata = Base.metadata

# Migrations run as the schema owner, never as the app role the API uses.
url = str(MigrationSettings().migration_database_url)


def _run(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def _run_online() -> None:
    engine = create_async_engine(url)
    async with engine.connect() as connection:
        await connection.run_sync(_run)
    await engine.dispose()


if context.is_offline_mode():
    context.configure(url=url, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(_run_online())
