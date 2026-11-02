"""One `sijil.extract_document` (or `sijil.lock_sealed`, prefix `lock`) in flight per document.

Every sender claims `extract:pending:<ledger_id>` on the broker Redis (`SET NX EX`) first and
skips the send when the key exists. The task clears it when it finishes (routed, final failure
or no-op) and refreshes it on every Celery retry, so a retry chain keeps it. A key that outlives
a lost message expires (1 h), and the sweep may send again.
"""

from collections.abc import Awaitable, Callable
from typing import Final
from uuid import UUID

from redis.asyncio import Redis

from sijil_core.ingest import Enqueue

PENDING_TTL_S: Final = 3600


def pending_key(ledger_id: UUID, prefix: str = "extract") -> str:
    return f"{prefix}:pending:{ledger_id}"


class PendingExtractions:
    def __init__(
        self, redis: Redis, *, ttl_s: int = PENDING_TTL_S, prefix: str = "extract"
    ) -> None:
        self._redis = redis
        self._ttl = ttl_s
        self._prefix = prefix

    async def claim(self, ledger_id: UUID) -> bool:
        """True when no extraction was in flight for the document (this caller may send)."""
        return bool(
            await self._redis.set(pending_key(ledger_id, self._prefix), "1", nx=True, ex=self._ttl)
        )

    async def clear(self, ledger_id: UUID) -> None:
        await self._redis.delete(pending_key(ledger_id, self._prefix))

    async def refresh(self, ledger_id: UUID, ttl_s: int) -> None:
        await self._redis.set(pending_key(ledger_id, self._prefix), "1", ex=ttl_s)

    async def send(self, send: Enqueue, ledger_id: UUID, workspace_id: UUID) -> bool:
        """Claims, then sends; a failed send releases the claim. False when skipped."""
        if not await self.claim(ledger_id):
            return False
        try:
            await send(ledger_id, workspace_id)
        except BaseException:
            await self.clear(ledger_id)
            raise
        return True

    def deduplicated(self, send: Enqueue) -> Enqueue:
        """`send`, skipped while an extraction of the document is in flight."""

        async def enqueue(ledger_id: UUID, workspace_id: UUID) -> None:
            await self.send(send, ledger_id, workspace_id)

        return enqueue


PendingOp = Callable[[PendingExtractions], Awaitable[None]]
