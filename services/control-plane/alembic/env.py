import asyncio

from alembic import context
from sqlalchemy.engine import Connection

from sijil_cp.db import make_engine
from sijil_cp.settings import CpSettings

# Models register their tables on this metadata from M1 onwards.
target_metadata = None


def _run(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def _run_online() -> None:
    engine = make_engine(CpSettings())
    async with engine.connect() as connection:
        await connection.run_sync(_run)
    await engine.dispose()


if context.is_offline_mode():
    context.configure(url=str(CpSettings().database_url), target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(_run_online())
