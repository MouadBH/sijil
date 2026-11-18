"""Account closure after the grace period (`sijil.closure_daily`, ADM-5, design §7 item 11).

Daily (beat, 02:15 in the region's time zone, after `sijil.mark_eligible`), on the main worker
(it has the Docker socket), for every open closure of this worker's region (`grace` once its
grace period is over, region-local: `sijil_core.closure.grace_over`; `hibernated`; and
`destroyed` ones whose take-over left documents under retention), workspace by workspace:

0. **Freeze.** Under the workspace row lock, a workspace not yet hibernated or closed becomes
   `hibernated` first, so capture stops before anything is counted: upload, inbound mail and
   ingest accept only `provisioning` or `active`, and `sijil_core.ingest.accept_file`
   re-checks the state under the same row (FOR SHARE) when it writes the ledger row. A
   document either landed before the freeze, and is counted below, or is refused.

1. **Retention end.** Each live document (not `removed`, `deleting` or `deleted`) whose
   `retention_until` is before today (region-local) goes to `deleting`, approved by the closure
   request itself: `deletion_approved_by` is the Owner who asked for the closure, audited
   `closure.deletion_approved` `{closure_id}` on the document. A sealed one whose original is
   still COMPLIANCE-locked in the store (until today or later, UTC) waits for its lock and
   counts as retained. The ledger refuses `deleting` before retention ends anyway (layer 3).
   After the commit `sijil.remove_original` is sent for each (the removal worker, the only
   holder of the delete key, which also refuses an unexpired lock), which then sends the
   purge; the rows stay as tombstones.
2. **Destroyed** when no live document is left under retention, or when the Owner signed a
   take-over statement and the workspace's newest built full export since the closure holds
   every live document (`sijil_core.closure.export_coverage`, on the export's recorded
   document set; ruling I1): `TenantRuntime.destroy` (its containers, volumes and network, its
   Paperless database and its index database), then the workspace is `closed` (ruling R6),
   audited `closure.destroyed` `{closure_id, retained}`. After a take-over the originals still
   under retention stay in the store, locked, never deleted early: step 1 approves each one
   once its retention has passed, with no further approval.

   A take-over whose export misses a live document (one captured during the grace period
   after it) is deferred: the workspace hibernates as in step 3, so its Owner keeps reading
   that document, audited once per export `closure.takeover_deferred` `{closure_id, code:
   takeover.export_incomplete, export_id, uncovered}`; a later run destroys it once a newer
   full export (the Owner may request one while hibernated) holds everything.
3. **Hibernated** otherwise (once): `TenantRuntime.stop`, then the workspace `hibernated` and
   the org's subscription `closed` (every write then answers `billing.read_only`, ruling R5),
   audited `closure.hibernated` `{closure_id, retained}`. Its Owners keep their reads, from the
   ledger, the index and our object store, never Paperless; every other role gets 404.

Then the closure is `destroyed` (with the org `closed`) once every workspace is, else
`hibernated`. One closure's failure is logged and leaves the others to run; every step is
idempotent, so the next run resumes where one stopped.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Final, Protocol
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit, billing
from sijil_core.closure import (
    EXPORT_INCOMPLETE,
    OPEN_STATES,
    TAKEOVER_DEFERRED,
    Coverage,
    export_coverage,
    grace_over,
)
from sijil_core.db.models import (
    TERMINAL_STATES,
    ActorType,
    AuditEvent,
    Closure,
    ClosureState,
    DocumentLedger,
    DocumentState,
    Organisation,
    OrgStatus,
    SubscriptionStatus,
    TakeoverStatement,
    Workspace,
    WorkspaceState,
)
from sijil_core.ingest import Enqueue
from sijil_core.region import assert_region
from sijil_core.retention import region_local_date
from sijil_core.settings import CoreSettings
from sijil_core.storage import ObjectRef
from sijil_core.tenancy import tenant_ref

log = logging.getLogger(__name__)

COMPLIANCE: Final = "COMPLIANCE"
_FROZEN: Final = (WorkspaceState.hibernated, WorkspaceState.closed)


class ClosureRuntime(Protocol):
    """What closing needs of a tenant runtime (`DockerRuntime` satisfies it)."""

    async def stop(self, ref: str) -> None: ...

    async def destroy(self, ref: str) -> None: ...


class LockReader(Protocol):
    """An original's lock in the store (`ObjectStore` satisfies it)."""

    async def retention(self, ref: ObjectRef) -> tuple[str, date] | None: ...


@dataclass
class ClosureRun:
    approved: int = 0
    hibernated: int = 0
    destroyed: int = 0


@dataclass(frozen=True)
class _Closure:
    id: UUID
    org_id: UUID
    requested_by: UUID
    requested_at: datetime
    takeover: bool


async def _still_locked(locks: LockReader, row: DocumentLedger, now: datetime) -> bool:
    if row.object_locked_at is None:
        return False
    found = await locks.retention(ObjectRef(row.object_key, row.object_version_id))
    return found is not None and found[0] == COMPLIANCE and found[1] >= now.astimezone(UTC).date()


async def _approve_expired(
    sessions: async_sessionmaker[AsyncSession],
    closure: _Closure,
    workspace_id: UUID,
    locks: LockReader,
    today: date,
    now: datetime,
) -> tuple[list[UUID], int]:
    """Step 1 of the module docstring, in one transaction. Returns the ids approved now and
    how many live documents are still under retention."""
    live = (
        DocumentLedger.workspace_id == workspace_id,
        DocumentLedger.state.not_in(TERMINAL_STATES),
    )
    async with sessions() as session:
        expired = list(
            await session.scalars(
                select(DocumentLedger).where(*live, DocumentLedger.retention_until < today)
            )
        )
        # Read without a lock: the store calls never run while a ledger row is held.
        waiting = {row.id for row in expired if await _still_locked(locks, row, now)}
        ids = sorted(row.id for row in expired if row.id not in waiting)
        await session.rollback()
        rows = list(
            await session.scalars(
                select(DocumentLedger)
                .where(*live, DocumentLedger.retention_until < today, DocumentLedger.id.in_(ids))
                .order_by(DocumentLedger.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        for row in rows:
            row.state = DocumentState.deleting
            row.deletion_approved_at = now
            row.deletion_approved_by = closure.requested_by
        # The ledger rows first, then the audit chain's lock (as everywhere else).
        await session.flush()
        for row in rows:
            await audit.append(
                session,
                workspace_id=workspace_id,
                action="closure.deletion_approved",
                actor_type=ActorType.system,
                actor_id=None,
                target_type="document",
                target_id=str(row.id),
                data={"closure_id": str(closure.id)},
                now=now,
            )
        retained = await session.scalar(
            select(func.count()).select_from(DocumentLedger).where(*live)
        )
        await session.commit()
    return [row.id for row in rows], int(retained or 0)


async def _destroy(
    sessions: async_sessionmaker[AsyncSession],
    runtime: ClosureRuntime,
    closure: _Closure,
    workspace_id: UUID,
    retained: int,
    now: datetime,
) -> None:
    await runtime.destroy(tenant_ref(workspace_id))
    async with sessions() as session:
        workspace = await session.get(Workspace, workspace_id, with_for_update=True)
        if workspace is None or workspace.state is WorkspaceState.closed:
            return
        workspace.state = WorkspaceState.closed
        await session.flush()
        await audit.append(
            session,
            workspace_id=workspace_id,
            action="closure.destroyed",
            actor_type=ActorType.system,
            actor_id=None,
            target_type="closure",
            target_id=str(closure.id),
            data={"closure_id": str(closure.id), "retained": str(retained)},
            now=now,
        )
        await session.commit()


async def _coverage(
    sessions: async_sessionmaker[AsyncSession], closure: _Closure, workspace_id: UUID
) -> Coverage:
    async with sessions() as session:
        found = await export_coverage(session, workspace_id, closure.requested_at)
        await session.rollback()
    return found


async def _defer_takeover(
    sessions: async_sessionmaker[AsyncSession],
    closure: _Closure,
    workspace_id: UUID,
    coverage: Coverage,
    now: datetime,
) -> None:
    """Audits, once per closure, workspace and export, that the take-over waits (ruling I1)."""
    export_id = "" if coverage.export_id is None else str(coverage.export_id)
    async with sessions() as session:
        done = await session.scalar(
            select(AuditEvent.id).where(
                AuditEvent.workspace_id == workspace_id,
                AuditEvent.action == TAKEOVER_DEFERRED,
                AuditEvent.target_id == str(closure.id),
                AuditEvent.data["export_id"].astext == export_id,
            )
        )
        if done is None:
            await audit.append(
                session,
                workspace_id=workspace_id,
                action=TAKEOVER_DEFERRED,
                actor_type=ActorType.system,
                actor_id=None,
                target_type="closure",
                target_id=str(closure.id),
                data={
                    "closure_id": str(closure.id),
                    "code": EXPORT_INCOMPLETE,
                    "export_id": export_id,
                    "uncovered": str(coverage.uncovered),
                },
                now=now,
            )
        await session.commit()


async def _close_subscription(session: AsyncSession, org_id: UUID) -> None:
    _org, sub = await billing.locked_subscription(session, org_id)
    sub.status = SubscriptionStatus.closed
    sub.status_reason = None


async def _freeze(sessions: async_sessionmaker[AsyncSession], workspace_id: UUID) -> None:
    """Stops capture before anything is counted: under the workspace row lock, a workspace not
    yet hibernated or closed becomes `hibernated` (upload, inbound mail and ingest accept only
    `provisioning` or `active`; `sijil_core.ingest.accept_file` re-checks under this row), so
    no document can land after the count."""
    async with sessions() as session:
        workspace = await session.get(
            Workspace, workspace_id, with_for_update=True, populate_existing=True
        )
        if workspace is not None and workspace.state not in _FROZEN:
            workspace.state = WorkspaceState.hibernated
        await session.commit()


async def _hibernate(
    sessions: async_sessionmaker[AsyncSession],
    runtime: ClosureRuntime,
    closure: _Closure,
    workspace_id: UUID,
    retained: int,
    now: datetime,
) -> bool:
    """The tenant stopped, the subscription closed, audited once. True when this run did it."""
    await runtime.stop(tenant_ref(workspace_id))
    async with sessions() as session:
        # The org and its subscription first (as billing does), then the workspace.
        await _close_subscription(session, closure.org_id)
        workspace = await session.get(Workspace, workspace_id, with_for_update=True)
        done = await session.scalar(
            select(AuditEvent.id).where(
                AuditEvent.workspace_id == workspace_id,
                AuditEvent.action == "closure.hibernated",
                AuditEvent.target_id == str(closure.id),
            )
        )
        if workspace is None or workspace.state is not WorkspaceState.hibernated or done:
            await session.commit()
            return False
        await audit.append(
            session,
            workspace_id=workspace_id,
            action="closure.hibernated",
            actor_type=ActorType.system,
            actor_id=None,
            target_type="closure",
            target_id=str(closure.id),
            data={"closure_id": str(closure.id), "retained": str(retained)},
            now=now,
        )
        await session.commit()
    return True


async def _settle(
    sessions: async_sessionmaker[AsyncSession], closure: _Closure, now: datetime
) -> None:
    """The closure's own state, from its workspaces'."""
    async with sessions() as session:
        await _close_subscription(session, closure.org_id)  # locks the org first
        org = await session.get(Organisation, closure.org_id)
        row = await session.get(Closure, closure.id, with_for_update=True, populate_existing=True)
        if org is None or row is None:
            return
        states = set(
            await session.scalars(select(Workspace.state).where(Workspace.org_id == closure.org_id))
        )
        if states <= {WorkspaceState.closed}:
            if row.state is not ClosureState.destroyed:
                row.state, row.destroyed_at = ClosureState.destroyed, now
            org.status = OrgStatus.closed
        elif row.state is ClosureState.grace:
            row.state, row.hibernated_at = ClosureState.hibernated, now
        await session.commit()


async def _run(
    sessions: async_sessionmaker[AsyncSession],
    closure: _Closure,
    *,
    runtime: ClosureRuntime,
    locks: LockReader,
    send_removal: Enqueue,
    today: date,
    now: datetime,
    run: ClosureRun,
) -> None:
    async with sessions() as session:
        workspaces = list(
            (
                await session.execute(
                    select(Workspace.id, Workspace.state)
                    .where(Workspace.org_id == closure.org_id)
                    .order_by(Workspace.created_at, Workspace.id)
                )
            ).all()
        )
    for workspace_id, state in workspaces:
        if state is not WorkspaceState.closed:
            await _freeze(sessions, workspace_id)
        # Counted only once capture is stopped (`_freeze`): the count holds until the end.
        approved, retained = await _approve_expired(
            sessions, closure, workspace_id, locks, today, now
        )
        run.approved += len(approved)
        deferred: Coverage | None = None
        if state is not WorkspaceState.closed and retained and closure.takeover:
            # Counted after the freeze too: no document can land in between.
            coverage = await _coverage(sessions, closure, workspace_id)
            deferred = coverage if coverage.uncovered else None
        if (
            state is not WorkspaceState.closed
            and deferred is None
            and (closure.takeover or retained == 0)
        ):
            await _destroy(sessions, runtime, closure, workspace_id, retained, now)
            run.destroyed += 1
        elif state is not WorkspaceState.closed:
            run.hibernated += await _hibernate(
                sessions, runtime, closure, workspace_id, retained, now
            )
            if deferred is not None:
                await _defer_takeover(sessions, closure, workspace_id, deferred, now)
        for ledger_id in approved:
            try:
                await send_removal(ledger_id, workspace_id)
            except Exception as exc:
                # Committed already: the removal sweep sends it again.
                log.error("removal of document %s not sent: %s", ledger_id, type(exc).__name__)
    await _settle(sessions, closure, now)


async def closure_daily(
    sessions: async_sessionmaker[AsyncSession],
    *,
    runtime: ClosureRuntime,
    locks: LockReader,
    settings: CoreSettings,
    send_removal: Enqueue,
    now: datetime,
) -> ClosureRun:
    """See the module docstring."""
    today = region_local_date(now, settings.region)
    async with sessions() as session:
        rows = (
            await session.execute(
                select(Closure, Organisation.region)
                .join(Organisation, Organisation.id == Closure.org_id)
                .where(Organisation.region == settings.region, Closure.state.in_(OPEN_STATES))
                .order_by(Closure.requested_at, Closure.id)
            )
        ).all()
        due: list[_Closure] = []
        for row, region in rows:
            assert_region(region, settings)
            if row.state is ClosureState.grace and not grace_over(row, now, region):
                continue
            signed = await session.scalar(
                select(TakeoverStatement.id).where(TakeoverStatement.closure_id == row.id)
            )
            due.append(
                _Closure(row.id, row.org_id, row.requested_by, row.requested_at, signed is not None)
            )
    run = ClosureRun()
    for closure in due:
        try:
            await _run(
                sessions, closure, runtime=runtime, locks=locks, send_removal=send_removal,
                today=today, now=now, run=run,
            )  # fmt: skip
        except Exception:
            log.exception("closure %s failed; the next run resumes it", closure.id)
    return run
