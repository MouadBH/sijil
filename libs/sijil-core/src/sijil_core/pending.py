"""Keys that mark a task in flight for one document on the broker Redis (`SET NX EX`).

Shared by the worker (extraction, the compliance lock, the original's removal and the purge
after it) and the control plane (which claims the lock and removal keys before sending its own
`sijil.lock_sealed` and `sijil.remove_original`), so a sweep re-send and the first message
never run together.
"""

from typing import Final
from uuid import UUID

PENDING_TTL_S: Final = 3600
LOCK_PREFIX: Final = "lock"
EXTRACT_PREFIX: Final = "extract"
REMOVE_PREFIX: Final = "remove"
PURGE_PREFIX: Final = "purge"
# The control plane claims it before sending `sijil.embed_document` (seal, correction); the
# worker does after routing.
EMBED_PREFIX: Final = "embed"
# Keyed by export job id, not a document: `export:pending:<job_id>`.
EXPORT_PREFIX: Final = "export"


def pending_key(ledger_id: UUID, prefix: str = EXTRACT_PREFIX) -> str:
    return f"{prefix}:pending:{ledger_id}"
