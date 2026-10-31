from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from sijil_cp.settings import CpSettings

CONNECT_TIMEOUT_S = 2


def make_engine(settings: CpSettings) -> AsyncEngine:
    return create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
        connect_args={"timeout": CONNECT_TIMEOUT_S},
    )


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker:  # type: ignore[type-arg]
    return async_sessionmaker(engine, expire_on_commit=False)
