"""The end of retention marked, never acted on (plan 4.4, design §5.4).

Nightly (`sijil.mark_eligible`, 02:00 in the region's time zone), for every workspace of this
worker's region, whatever its state: the rows whose `retention_until` is before today in the
region (the same region-local rule as the ledger guard `sijil_region_today()`), that are not
`removed`, `deleting` or `deleted` and not yet marked, get `deletion_eligible_at = now`, and the
workspace gets one `retention.eligible` `{count}` event when the count is above 0. Nothing else
changes: no state, no original, no Paperless copy, no index row. A deletion happens only when
an Owner approves it (`POST …/retention/approve-deletion`).

`deletion_eligible_at` is a marker (set once, 0011), not a guard: the ledger checks
`retention_until` itself before it allows `deleting`.
"""

from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core.db.models import (
    TERMINAL_STATES,
    ActorType,
    DocumentLedger,
    Organisation,
    Workspace,
)
from sijil_core.region import assert_region
from sijil_core.retention import region_local_date
from sijil_core.settings import CoreSettings


async def mark_eligible(
    sessions: async_sessionmaker[AsyncSession], *, settings: CoreSettings, now: datetime
) -> int:
    """See the module docstring. One transaction per workspace (its rows, then its audit
    chain). Returns how many rows were marked in all."""
    today = region_local_date(now, settings.region)
    async with sessions() as session:
        workspaces = (
            await session.execute(
                select(Workspace.id, Organisation.region)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(Organisation.region == settings.region)
                .order_by(Workspace.id)
            )
        ).all()
    total = 0
    for workspace_id, region in workspaces:
        assert_region(region, settings)
        async with sessions() as session:
            marked = (
                await session.scalars(
                    update(DocumentLedger)
                    .where(
                        DocumentLedger.workspace_id == workspace_id,
                        DocumentLedger.retention_until < today,
                        DocumentLedger.state.not_in(TERMINAL_STATES),
                        DocumentLedger.deletion_eligible_at.is_(None),
                    )
                    .values(deletion_eligible_at=now)
                    .returning(DocumentLedger.id)
                    .execution_options(synchronize_session=False)
                )
            ).all()
            if marked:
                await audit.append(
                    session,
                    workspace_id=workspace_id,
                    action="retention.eligible",
                    actor_type=ActorType.system,
                    actor_id=None,
                    data={"count": len(marked)},
                    now=now,
                )
            await session.commit()
        total += len(marked)
    return total
