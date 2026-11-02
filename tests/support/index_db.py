"""Throw-away per-workspace index databases for `db`-marked tests.

Each call creates an empty database on the test cluster (SIJIL_TEST_DATABASE_URL, a superuser)
and drops it afterwards. The index tables refuse DELETE and TRUNCATE, so tests sharing one
database use fresh ledger ids instead of cleaning up.
"""

import asyncio
import os
import uuid
from collections.abc import Iterator

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine


def _admin_run(raw: str, statement: str) -> None:
    async def go() -> None:
        engine = create_async_engine(raw, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as c:
                await c.execute(text(statement))
        finally:
            await engine.dispose()

    asyncio.run(go())


def throwaway_index_db() -> Iterator[str]:
    """Yields the URL of a new, empty database (superuser credentials); drops it afterwards."""
    raw = os.environ.get("SIJIL_TEST_DATABASE_URL")
    if not raw:
        pytest.skip("set SIJIL_TEST_DATABASE_URL")
    name = f"sijil_idx_{uuid.uuid4().hex[:12]}"
    _admin_run(raw, f"CREATE DATABASE {name}")
    try:
        yield make_url(raw).set(database=name).render_as_string(hide_password=False)
    finally:
        _admin_run(raw, f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
