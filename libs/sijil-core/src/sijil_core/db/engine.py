from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

CONNECT_TIMEOUT_S = 2


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(
        url,
        pool_pre_ping=True,
        connect_args={"timeout": CONNECT_TIMEOUT_S},
    )


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
