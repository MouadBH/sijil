"""Correction notes on sealed documents (design §5.4).

A sealed document is never edited. A correction is a note beside the original value: appended
to the index's `correction_note` (append-only), audited as `document.corrected` naming the field
and never the value or the reason (ruling P5). The sealed row, `fields_sha256` and the ledger
stay as they were; exports use `index.effective_fields` and flag the corrected values.

Like a field edit, it holds the ledger lock, commits its audit event first and writes the index
after: an applied note always has its event, and an index failure answers 503
`field.update_failed`. The note and the search row's effective values (`search_doc`) are written
in one index transaction, so search shows the corrected value as soon as the request answers;
then the document's embedding is sent (`sijil.embed_document`), which reads that row, and, for a
note on the supplier's name or ICE, `sijil.learn_from_document` (REC-5).
"""

import logging
from typing import Final
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict

from sijil_core import audit
from sijil_core import index as index_db
from sijil_core.db.models import TERMINAL_STATES, ActorType
from sijil_core.search_index import refresh_search_doc
from sijil_cp.api.review import (
    NOT_REVIEWABLE,
    SUPPLIER_FIELDS,
    UPDATE_FAILED,
    CorrectionOut,
    CurrentUser,
    Db,
    Now,
    Reviewing,
    _engines,
    _ip,
    _locked_ledger,
    _parsed,
    _search_facts,
    send_embedding,
    send_learning,
)
from sijil_cp.deps import require_totp_compliance
from sijil_cp.errors import ApiError

log = logging.getLogger(__name__)

router = APIRouter(tags=["review"], dependencies=[Depends(require_totp_compliance)])

NOT_SEALED: Final = "correction.not_sealed"
REASON_REQUIRED: Final = "correction.reason_required"
REASON_MIN: Final = 10
REASON_MAX: Final = 1000


class CorrectionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    value: str | None
    # Optional here so that a missing reason answers `correction.reason_required` too.
    reason: str | None = None


@router.post(
    "/workspaces/{workspace_id}/documents/{doc_id}/corrections",
    response_model=CorrectionOut,
    status_code=201,
)
async def add_correction(
    doc_id: UUID,
    body: CorrectionIn,
    request: Request,
    ctx: Reviewing,
    user: CurrentUser,
    db: Db,
    now: Now,
) -> CorrectionOut:
    workspace = ctx.workspace
    value = _parsed({body.field: body.value})[body.field]
    reason = (body.reason or "").strip()
    if not REASON_MIN <= len(reason) <= REASON_MAX:
        raise ApiError(REASON_REQUIRED, 422)
    ledger = await _locked_ledger(db, workspace, doc_id)
    if ledger.state in TERMINAL_STATES:
        raise ApiError(NOT_REVIEWABLE, 409)
    if ledger.sealed_at is None:
        raise ApiError(NOT_SEALED, 409)
    await audit.append(
        db,
        workspace_id=workspace.id,
        action="document.corrected",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        target_type="document",
        target_id=str(ledger.id),
        ip=_ip(request),
        data={"field": body.field},
        now=now,
    )
    await db.commit()
    try:
        async with (await _engines(request).engine(workspace)).begin() as conn:
            note = await index_db.add_correction(
                conn, ledger.id, body.field, value, reason, created_by=user.id, now=now
            )
            await refresh_search_doc(conn, _search_facts(request, ledger, ledger.state, now))
    except Exception as exc:
        # A logged correction that did not apply: acceptable, and said so.
        log.warning("correction of document %s not applied: %s", doc_id, type(exc).__name__)
        raise ApiError(UPDATE_FAILED, 503) from None
    await send_embedding(request, ledger.id, workspace.id)
    if body.field in SUPPLIER_FIELDS:
        await send_learning(request, ledger.id, workspace.id)
    return CorrectionOut.model_validate(note, from_attributes=True)
