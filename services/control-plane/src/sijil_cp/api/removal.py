"""The Owner's pre-seal removal of a document uploaded by mistake (plan 4.3, design §5.4).

`POST …/documents/{id}/remove` `{reason}` needs `Action.documents_remove` (the Owner only), a
TOTP step-up on this session within 10 minutes and a reason of 10 to 1000 characters after
trimming. In one transaction, in this order (the ledger lock before the audit chain's, as in
sijil_cp.api.review):
1. the ledger row is locked FOR UPDATE and refused if it is in a terminal state
   (`document.not_removable`) or sealed (`document.sealed_cannot_be_removed`);
2. its index row is read: one frozen while the ledger is not sealed (a seal half done,
   `seal_incomplete`) holds values a person confirmed, so it is refused as sealed too. A seal
   racing this removal holds the same ledger lock while it freezes the index row, so one of
   the two always sees the other's commit;
3. the row becomes the tombstone (`state = removed`, `removed_at`, `removed_by`,
   `removal_reason`; the hash and the object reference never change) and `document.removed`
   `{reason_length}` is audited (never the reason's text).
After the commit `remove:pending:<id>` is claimed and `sijil.remove_original` sent to the
removal worker, which deletes the original and then has the main worker purge the Paperless
copy and the index rows. A lost message is re-sent by the `requeue_unremoved` sweep.

Every refusal to a signed-in member of the workspace (design §5.5: "every refused deletion is
logged") is audited `document.removal_refused` `{code}`, in its own transaction once the
request's own has been rolled back, so a refused request changes nothing else. That is why
the permission and step-up checks run here, in the handler, and not as route dependencies, and
why the body is parsed here too (a malformed one is audited with the code `validation`, after
the role and the step-up, so it tells an Admin nothing). A role refusal is the API's usual 403
`forbidden`. A non-member gets the 404 of every workspace route, unaudited (no existence leak).
"""

import asyncio
import logging
from datetime import datetime
from typing import Annotated, Final
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ValidationError
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit
from sijil_core import index as index_db
from sijil_core.db.models import (
    TERMINAL_STATES,
    ActorType,
    DocumentLedger,
    DocumentState,
    User,
    UserSession,
    Workspace,
)
from sijil_core.index import IndexEngines
from sijil_core.pending import PENDING_TTL_S, REMOVE_PREFIX, pending_key
from sijil_cp.deps import (
    current_session,
    current_user,
    get_db,
    get_now,
    require_recent_totp,
    require_totp_compliance,
)
from sijil_cp.enqueue import get_enqueuer
from sijil_cp.errors import ApiError
from sijil_cp.permissions import Action, authorize

log = logging.getLogger(__name__)

router = APIRouter(tags=["removal"])

Db = Annotated[AsyncSession, Depends(get_db)]
Session = Annotated[UserSession, Depends(current_session())]
CurrentUser = Annotated[User, Depends(current_user)]
Now = Annotated[datetime, Depends(get_now)]

REFUSED: Final = "document.removal_refused"
INVALID_BODY: Final = "validation"
SEALED: Final = "document.sealed_cannot_be_removed"
NOT_REMOVABLE: Final = "document.not_removable"
REASON_REQUIRED: Final = "removal.reason_required"
INDEX_UNAVAILABLE: Final = "removal.index_unavailable"
REASON_MIN: Final = 10
REASON_MAX: Final = 1000
STEP_UP_MINUTES: Final = 10

# Called from the handler, so its refusal is audited like the others.
_step_up = require_recent_totp(STEP_UP_MINUTES)


class RemovalIn(BaseModel):
    # Optional here so that a missing reason answers `removal.reason_required` too.
    reason: str | None = None


class RemovalOut(BaseModel):
    id: UUID
    state: DocumentState


def _ip(request: Request) -> str | None:
    return request.client.host if request.client else None


def _body(raw: bytes) -> RemovalIn:
    """The JSON body, or the same 422 `validation` FastAPI would have answered."""
    try:
        return RemovalIn.model_validate_json(raw)
    except ValidationError as exc:
        raise RequestValidationError(
            [{**error, "loc": ("body", *error["loc"])} for error in exc.errors()]
        ) from None


def _reason(body: RemovalIn) -> str:
    reason = (body.reason or "").strip()
    if not REASON_MIN <= len(reason) <= REASON_MAX:
        raise ApiError(REASON_REQUIRED, 422)
    return reason


async def _locked_ledger(db: AsyncSession, workspace: Workspace, doc_id: UUID) -> DocumentLedger:
    ledger = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == doc_id, DocumentLedger.workspace_id == workspace.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if ledger is None:
        raise ApiError("document.not_found", 404)
    return ledger


def _refuse_unless_removable(ledger: DocumentLedger) -> None:
    if ledger.state in TERMINAL_STATES:
        raise ApiError(NOT_REMOVABLE, 409)
    if ledger.state is DocumentState.sealed or ledger.sealed_at is not None:
        raise ApiError(SEALED, 409)


async def _refuse_if_half_sealed(
    request: Request, workspace: Workspace, ledger: DocumentLedger
) -> None:
    """A frozen index row means a person confirmed these values: the document is sealed for
    this purpose even though the seal did not finish (sealing again completes it)."""
    engines: IndexEngines = request.app.state.index
    try:
        async with (await engines.engine(workspace)).connect() as conn:
            row = await index_db.read_fields(conn, ledger.id)
    except (LookupError, SQLAlchemyError, OSError) as exc:
        # Without the index, a half-done seal cannot be ruled out: refuse.
        log.warning("index of workspace %s unavailable: %s", workspace.id, type(exc).__name__)
        raise ApiError(INDEX_UNAVAILABLE, 503) from None
    if row is not None and row.sealed_at is not None:
        raise ApiError(SEALED, 409)


async def _audit_refusal(
    db: AsyncSession,
    request: Request,
    workspace_id: UUID,
    user_id: UUID,
    doc_id: UUID,
    code: str,
    now: datetime,
) -> None:
    """`document.removal_refused` `{code}` in a transaction of its own: whatever the refused
    request had started is rolled back first (and the ledger lock with it)."""
    await db.rollback()
    await audit.append(
        db,
        workspace_id=workspace_id,
        action=REFUSED,
        actor_type=ActorType.user,
        actor_id=str(user_id),
        target_type="document",
        target_id=str(doc_id),
        ip=_ip(request),
        data={"code": code},
        now=now,
    )
    await db.commit()


@router.post(
    "/workspaces/{workspace_id}/documents/{doc_id}/remove",
    status_code=202,
    response_model=RemovalOut,
    # The body is read in the handler (see the module docstring); documented here.
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": RemovalIn.model_json_schema()}},
        }
    },
)
async def remove_document(
    workspace_id: UUID,
    doc_id: UUID,
    request: Request,
    session: Session,
    user: CurrentUser,
    db: Db,
    now: Now,
) -> RemovalOut:
    workspace = await db.get(Workspace, workspace_id)
    if workspace is None:
        raise ApiError("not_found", 404)
    # Kept apart: the rollback before a refusal's audit expires the loaded rows.
    ws_id, user_id = workspace.id, user.id
    try:
        # Non-members get 404 and another region 421, unaudited; then the role (`forbidden`).
        await authorize(db, user.id, workspace, Action.documents_remove, request.app.state.settings)
        await require_totp_compliance(session, user, db)
        await _step_up(session, user, now)
        reason = _reason(_body(await request.body()))
        ledger = await _locked_ledger(db, workspace, doc_id)
        _refuse_unless_removable(ledger)
        await _refuse_if_half_sealed(request, workspace, ledger)
    except RequestValidationError:
        await _audit_refusal(db, request, ws_id, user_id, doc_id, INVALID_BODY, now)
        raise
    except ApiError as exc:
        if exc.code != "not_found":
            await _audit_refusal(db, request, ws_id, user_id, doc_id, exc.code, now)
        raise
    ledger.state = DocumentState.removed
    ledger.removed_at = now
    ledger.removed_by = user_id
    ledger.removal_reason = reason
    await audit.append(
        db,
        workspace_id=ws_id,
        action="document.removed",
        actor_type=ActorType.user,
        actor_id=str(user_id),
        target_type="document",
        target_id=str(doc_id),
        ip=_ip(request),
        data={"reason_length": len(reason)},
        now=now,
    )
    await db.commit()
    await _after_commit(request, doc_id, ws_id)
    return RemovalOut(id=doc_id, state=DocumentState.removed)


async def _after_commit(request: Request, ledger_id: UUID, workspace_id: UUID) -> None:
    """The removal stands whatever happens here: a lost message is re-sent by the
    `requeue_unremoved` sweep. `remove:pending:<id>` is claimed first, the key the removal
    worker clears and the sweep honours, so this send and a sweep re-send never both run."""
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
    except Exception as exc:  # noqa: BLE001 - the broker being down must not undo a removal
        log.warning("removal enqueue of document %s failed: %s", ledger_id, type(exc).__name__)
        try:
            await redis.delete(key)
        except Exception as release_exc:  # noqa: BLE001 - it expires anyway
            log.warning(
                "removal release of document %s failed: %s",
                ledger_id, type(release_exc).__name__,
            )  # fmt: skip
