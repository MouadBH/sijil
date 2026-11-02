"""Wait for the extraction worker to be idle before a timing-sensitive integration test.

Idle: the `extraction` queue in the broker Redis is empty and `worker-extraction` runs no task
(Celery inspect). For the compose `tests` runner, which carries the worker's environment.
"""

import asyncio
import os
import time
from collections.abc import Awaitable, Callable

from celery import Celery
from redis.asyncio import Redis

EXTRACTION_QUEUE = "extraction"
WORKER_PREFIX = "extraction@"
IDLE_BUDGET_S = 600
POLL_S = 5

Probe = Callable[[], Awaitable[tuple[int, int]]]


class ExtractionBusy(AssertionError):
    """The extraction worker was still busy when the budget ran out."""


def _running() -> int:
    """Tasks the extraction worker is running now (0 if it does not answer)."""
    app = Celery(broker=os.environ["SIJIL_REDIS_URL"])
    try:
        active = app.control.inspect(timeout=2.0).active() or {}
    finally:
        app.close()
    return sum(len(tasks) for name, tasks in active.items() if name.startswith(WORKER_PREFIX))


async def broker_probe() -> tuple[int, int]:
    """(messages waiting on the `extraction` queue, tasks the extraction worker runs)."""
    redis = Redis.from_url(os.environ["SIJIL_REDIS_URL"])
    try:
        queued = int(await redis.llen(EXTRACTION_QUEUE))
    finally:
        await redis.aclose()
    return queued, await asyncio.to_thread(_running)


async def wait_extraction_idle(
    *, budget_s: float = IDLE_BUDGET_S, poll_s: float = POLL_S, probe: Probe = broker_probe
) -> float:
    """Waits until nothing is queued for or running on the extraction worker; returns the
    seconds waited. Raises ExtractionBusy after `budget_s`."""
    started = time.monotonic()
    while True:
        queued, running = await probe()
        if queued == 0 and running == 0:
            return time.monotonic() - started
        if time.monotonic() - started >= budget_s:
            raise ExtractionBusy(
                f"extraction worker still busy after {budget_s:.0f} s: "
                f"{queued} queued, {running} running"
            )
        await asyncio.sleep(poll_s)
