"""Teardown for integration tests: every workspace a test creates ends `closing`.

Ledger rows cannot be deleted (the retention lock), so a test's workspace stays in the database.
Left `active`, its `received` rows would keep feeding the live extraction worker (and its sweep)
long after the tenant is gone; a `closing` workspace never extracts. Plain UPDATE as the app
role (`SIJIL_DATABASE_URL`, which the compose `tests` runner carries): `workspace` is not a
guarded table.
"""

import os
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy import update

from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import Workspace, WorkspaceState


async def mark_closing(workspace_ids: Iterable[UUID], *, database_url: str | None = None) -> None:
    ids = list(workspace_ids)
    if not ids:
        return
    engine = make_engine(database_url or os.environ["SIJIL_DATABASE_URL"])
    try:
        async with make_sessionmaker(engine)() as db:
            await db.execute(
                update(Workspace).where(Workspace.id.in_(ids)).values(state=WorkspaceState.closing)
            )
            await db.commit()
    finally:
        await engine.dispose()


@asynccontextmanager
async def closing_at_exit() -> AsyncIterator[list[UUID]]:
    """Yields a list; every workspace id appended to it is marked `closing` on exit, even when
    the body fails."""
    created: list[UUID] = []
    try:
        yield created
    finally:
        await mark_closing(created)
