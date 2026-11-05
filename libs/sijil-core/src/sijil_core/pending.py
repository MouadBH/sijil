"""Keys that mark a task in flight for one document on the broker Redis (`SET NX EX`).

Shared by the worker (extraction, the compliance lock and the original's removal) and the
control plane (which claims the lock key before sending its own `sijil.lock_sealed`), so a
sweep re-send and the first message never run together.
"""

from typing import Final
from uuid import UUID

PENDING_TTL_S: Final = 3600
LOCK_PREFIX: Final = "lock"
EXTRACT_PREFIX: Final = "extract"
REMOVE_PREFIX: Final = "remove"


def pending_key(ledger_id: UUID, prefix: str = EXTRACT_PREFIX) -> str:
    return f"{prefix}:pending:{ledger_id}"
