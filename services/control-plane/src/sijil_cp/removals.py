"""What the two routes that delete an original share: the Owner's pre-seal removal
(sijil_cp.api.removal) and the Owner's approved deletion after retention
(sijil_cp.api.retention).

- Every refusal to a signed-in member of the workspace is audited `document.removal_refused`
  `{code}` (design §5.5: "every refused deletion is logged"), in a transaction of its own once
  the request's own has been rolled back, so a refused request changes nothing else.
- The body is parsed in the handler, after the role and the step-up, so that a malformed one
  is audited too (`validation`) and tells an Admin nothing.
- After the commit `remove:pending:<id>` is claimed, then `sijil.remove_original` is sent to the
  removal worker; a lost message is re-sent by the `requeue_unremoved` sweep.
"""

import asyncio
import logging
from collections.abc import Sequence
from datetime import datetime
from typing import Final
from uuid import UUID

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit
from sijil_core.db.models import ActorType
from sijil_core.pending import PENDING_TTL_S, REMOVE_PREFIX, pending_key
from sijil_cp.enqueue import get_enqueuer

log = logging.getLogger(__name__)

REFUSED: Final = "document.removal_refused"
INVALID_BODY: Final = "validation"
STEP_UP_MINUTES: Final = 10


def client_ip(request: Request) -> str | None:
    return request.client.host if request.client else None


def parse_body[M: BaseModel](model: type[M], raw: bytes) -> M:
    """The JSON body, or the same 422 `validation` FastAPI would have answered."""
    try:
        return model.model_validate_json(raw)
    except ValidationError as exc:
        raise RequestValidationError(
            [{**error, "loc": ("body", *error["loc"])} for error in exc.errors()]
        ) from None


async def audit_refusal(
    db: AsyncSession,
    request: Request,
    workspace_id: UUID,
    user_id: UUID,
    targets: Sequence[UUID | None],
    code: str,
    now: datetime,
) -> None:
    """One `document.removal_refused` `{code}` per target (None: an event with no target), in
    a transaction of its own: whatever the refused request had started is rolled back first
    (and its ledger locks with it)."""
    await db.rollback()
    for target in targets:
        await audit.append(
            db,
            workspace_id=workspace_id,
            action=REFUSED,
            actor_type=ActorType.user,
            actor_id=str(user_id),
            target_type=None if target is None else "document",
            target_id=None if target is None else str(target),
            ip=client_ip(request),
            data={"code": code},
            now=now,
        )
    await db.commit()


async def send_removal(request: Request, ledger_id: UUID, workspace_id: UUID) -> None:
    """After the commit; the decision stands whatever happens here. `remove:pending:<id>` is
    claimed first, the key the removal worker clears and the sweep honours, so this send and a
    sweep re-send never both run."""
    redis = request.app.state.redis
    key = pending_key(ledger_id, REMOVE_PREFIX)
    try:
        claimed = bool(await redis.set(key, "1", nx=True, ex=PENDING_TTL_S))
    except Exception as exc:  # noqa: BLE001 - the worker is idempotent: send anyway
        log.warning("removal claim of document %s failed: %s", ledger_id, type(exc).__name__)
        claimed = True
    if not claimed:
        return
    try:
        await asyncio.to_thread(get_enqueuer(request).send_remove, ledger_id, workspace_id)
    except Exception as exc:  # noqa: BLE001 - the broker being down must not undo a decision
        log.warning("removal enqueue of document %s failed: %s", ledger_id, type(exc).__name__)
        try:
            await redis.delete(key)
        except Exception as release_exc:  # noqa: BLE001 - it expires anyway
            log.warning(
                "removal release of document %s failed: %s",
                ledger_id, type(release_exc).__name__,
            )  # fmt: skip
