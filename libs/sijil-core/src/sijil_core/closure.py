"""Account closure (ADM-5, design §7 item 11): what the control plane and the closure job share.

An Owner's request opens a `Closure` in `grace` for GRACE_DAYS. The grace period runs to the
end of the region-local day `grace_ends_at` falls on (`grace_over`), like retention: the daily
job (`sijil_worker.closure`) then hibernates each workspace that still holds a document under
retention, or destroys it; a take-over statement ends the hosting anyway, the retained
originals staying locked in the store until their retention passes.
"""

from datetime import date, datetime, timedelta
from typing import Final
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import Closure, ClosureState
from sijil_core.retention import region_local_date

GRACE_DAYS: Final = 30
# A take-over statement is kept at least this long (write-once, its own object lock).
TAKEOVER_RETENTION_YEARS: Final = 10
# A closure that is not `cancelled` is open: at most one per org (`uq_closure_org_open`).
OPEN_STATES: Final = (ClosureState.grace, ClosureState.hibernated, ClosureState.destroyed)

ALREADY_REQUESTED: Final = "closure.already_requested"
NOT_REQUESTED: Final = "closure.not_requested"
GRACE_OVER: Final = "closure.grace_over"
TAKEOVER_INVALID: Final = "takeover.invalid"


def grace_ends(requested_at: datetime) -> datetime:
    return requested_at + timedelta(days=GRACE_DAYS)


def grace_last_day(closure: Closure, region: str) -> date:
    """The last region-local day of the grace period."""
    return region_local_date(closure.grace_ends_at, region)


def grace_over(closure: Closure, now: datetime, region: str) -> bool:
    """Whether the grace period has ended at `now`: its last day is over, region-local."""
    return region_local_date(now, region) > grace_last_day(closure, region)


async def open_closure(db: AsyncSession, org_id: UUID, *, lock: bool = False) -> Closure | None:
    """The org's open closure, FOR UPDATE with `lock`."""
    query = select(Closure).where(Closure.org_id == org_id, Closure.state.in_(OPEN_STATES))
    if lock:
        query = query.with_for_update().execution_options(populate_existing=True)
    found: Closure | None = await db.scalar(query)
    return found


def takeover_retain_until(today: date, latest_retention: date | None) -> date:
    """How long a take-over statement is locked: TAKEOVER_RETENTION_YEARS from `today`, or the
    org's latest `retention_until` when that is later (31 December, like retention)."""
    floor = date(today.year + TAKEOVER_RETENTION_YEARS, 12, 31)
    return max(floor, latest_retention) if latest_retention is not None else floor
