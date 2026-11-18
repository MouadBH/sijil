"""Teardown for integration tests: every workspace a test creates ends `closing`, its tenant
stopped.

Ledger rows cannot be deleted (the retention lock), so a test's workspace stays in the database.
Left `active`, its `received` rows would keep feeding the live extraction worker (and its sweep)
long after the test; a `closing` workspace never extracts. Plain UPDATE as the app role
(`SIJIL_DATABASE_URL`, which the compose `tests` runner carries): `workspace` is not a guarded
table.

Its tenant (Paperless and its Redis) is then stopped through `TenantRuntime.stop`, never
removed: a run no longer leaves Paperless running (its memory is what ran Ollama out of it in the
M5 acceptance run). Its containers, volumes and databases stay, as for a hibernated workspace;
a test that wants them gone destroys them itself.
"""

import contextlib
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from contextlib import asynccontextmanager
from typing import Protocol
from uuid import UUID

from sqlalchemy import update

from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import Workspace, WorkspaceState
from sijil_core.tenancy import tenant_ref


class Stopper(Protocol):
    """What the teardown needs of a `TenantRuntime`."""

    async def stop(self, ref: str) -> None: ...


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


async def stop_tenants(workspace_ids: Iterable[UUID], runtime: Stopper) -> None:
    """Stops each workspace's tenant (one already stopped or gone is a no-op); every one is
    tried, then the first failure is raised."""
    failures: list[Exception] = []
    for workspace_id in workspace_ids:
        try:
            await runtime.stop(tenant_ref(workspace_id))
        except Exception as exc:  # noqa: BLE001 - the others are still stopped
            failures.append(exc)
    if failures:
        raise failures[0]


@asynccontextmanager
async def closing_at_exit(
    runtime: Stopper | None = None,
    *,
    mark: Callable[[Iterable[UUID]], Awaitable[None]] = mark_closing,
) -> AsyncIterator[list[UUID]]:
    """Yields a list; every workspace id appended to it is marked `closing` on exit, even when
    the body fails, and then, given a `runtime`, its tenant is stopped. A failed body's error
    is the one raised; a failed stop is raised only after a body that passed."""
    created: list[UUID] = []
    try:
        yield created
    except BaseException:
        with contextlib.suppress(Exception):
            await mark(created)
        if runtime is not None:
            with contextlib.suppress(Exception):
                await stop_tenants(created, runtime)
        raise
    try:
        await mark(created)
    finally:
        if runtime is not None:
            await stop_tenants(created, runtime)
