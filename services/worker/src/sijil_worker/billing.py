"""The daily billing transitions (`sijil.billing_daily`, ADM-4, design §5.7a).

Once a day (beat, 01:00 in the region's time zone), for every organisation of this worker's
region: `sijil_core.billing.advance`, the same transition function the control plane runs when
a request needs the stored state current. It issues the first annual invoice at the trial's end
(or marks the subscription overdue, `org.ice_required`, with no ICE), the renewal 30 days before
the billing year ends (applying a pending downgrade), marks unpaid invoices past due overdue,
and moves the subscription to `overdue`, `read_only` or back to `active`. One transaction per
organisation, so one failure leaves the others done; a second run the same day changes nothing.
"""

import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import billing
from sijil_core.db.models import Organisation
from sijil_core.region import assert_region
from sijil_core.settings import SellerSettings

log = logging.getLogger(__name__)


async def billing_daily(
    sessions: async_sessionmaker[AsyncSession], *, settings: SellerSettings, now: datetime
) -> int:
    """See the module docstring. Returns how many organisations were brought up to date."""
    async with sessions() as session:
        orgs = (
            await session.execute(
                select(Organisation.id, Organisation.region)
                .where(Organisation.region == settings.region)
                .order_by(Organisation.id)
            )
        ).all()
    done = 0
    for org_id, region in orgs:
        assert_region(region, settings)
        try:
            async with sessions() as session:
                await billing.advance(session, org_id, now, seller=settings)
                await session.commit()
        except Exception:
            # The next run tries again; the other organisations go on.
            log.exception("billing transitions failed for org %s", org_id)
            continue
        done += 1
    return done
