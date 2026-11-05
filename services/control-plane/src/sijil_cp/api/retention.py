"""Retention: the policy per document type, and the end of retention (plan 4.4, design §5.4).

A policy only ever applies to retention computed from then on: nothing already set is lowered
(the ledger trigger refuses it anyway).

Nothing is ever purged automatically. A document whose `retention_until` is before today in the
region (`sijil_core.retention.region_local_date`, the rule of the ledger guard
`sijil_region_today()`) and that is not `removed`, `deleting` or `deleted` is eligible for
deletion (`GET …/retention/eligible`); the nightly `sijil.mark_eligible` only marks it.
`POST …/retention/approve-deletion` `{ids}` (1 to 500) needs `Action.retention_approve_deletion`
(the Owner only) and a TOTP step-up within 10 minutes. It is all or nothing: the rows are
locked FOR UPDATE in id order and, if any is not eligible (retention not over, terminal
already, another workspace's or unknown), 409 `retention.not_eligible` names the offending
`ids` and nothing changes. Otherwise, in one transaction, each row goes to `deleting` with
`deletion_approved_at` and `deletion_approved_by` and is audited `document.deletion_approved`,
then `retention.deletion_batch` `{count}`; after the commit `sijil.remove_original` is sent for
each (sijil_cp.removals). The removal worker deletes an original only once its lock is over.

Every refusal of an approval to a signed-in member is audited `document.removal_refused`
`{code}` (sijil_cp.removals): one per offending id for `retention.not_eligible`, else one with
no target. That is why its checks run in the handler, and why the TOTP compliance dependency is
on each route rather than on the router (the approval checks it in the handler).
"""

import base64
import binascii
import logging
from datetime import date, datetime
from typing import Annotated, Any, Final, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field
from sqlalchemy import select, tuple_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit
from sijil_core import index as index_db
from sijil_core.db.models import (
    TERMINAL_STATES,
    ActorType,
    DocumentLedger,
    DocumentState,
    Organisation,
    RetentionPolicy,
    User,
    UserSession,
    Workspace,
)
from sijil_core.fields import DOCUMENT_TYPES
from sijil_core.index import IndexEngines
from sijil_core.retention import preset_policies, region_local_date
from sijil_cp.deps import (
    current_session,
    current_user,
    get_db,
    get_now,
    require_recent_totp,
    require_totp_compliance,
)
from sijil_cp.errors import ApiError
from sijil_cp.permissions import Action, WorkspaceContext, authorize, require_workspace
from sijil_cp.removals import (
    INVALID_BODY,
    STEP_UP_MINUTES,
    audit_refusal,
    client_ip,
    parse_body,
    send_removal,
)

log = logging.getLogger(__name__)

router = APIRouter(tags=["retention"])
COMPLIANT = [Depends(require_totp_compliance)]

BELOW_PRESET = "retention.policy_below_preset"


class PolicyOut(BaseModel):
    document_type: str
    years: int
    preset_years: int
    locked: bool


class PolicyIn(BaseModel):
    years: int = Field(ge=1, le=50)


def _out(row: RetentionPolicy) -> PolicyOut:
    return PolicyOut(
        document_type=row.document_type,
        years=row.years,
        preset_years=row.preset_years,
        locked=row.locked,
    )


async def _policies(
    db: AsyncSession, ctx: WorkspaceContext, *, lock: bool = False
) -> dict[str, RetentionPolicy]:
    """The workspace's rows by type, with any type that has none (a workspace created before
    its policies were seeded) added from its country's preset."""
    query = select(RetentionPolicy).where(RetentionPolicy.workspace_id == ctx.workspace.id)
    if lock:
        query = query.with_for_update()
    rows = {r.document_type: r for r in await db.scalars(query)}
    missing = [t for t in DOCUMENT_TYPES if t not in rows]
    if missing:
        org = await db.get(Organisation, ctx.workspace.org_id)
        if org is None:  # authorize already checked; keeps the type narrow
            raise ApiError("not_found", 404)
        for row in preset_policies(ctx.workspace.id, org.country):
            if row.document_type in missing:
                db.add(row)
                rows[row.document_type] = row
        await db.flush()
    return rows


@router.get("/workspaces/{workspace_id}/retention/policies", dependencies=COMPLIANT)
async def list_policies(
    ctx: Annotated[WorkspaceContext, Depends(require_workspace(Action.retention_manage))],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> list[PolicyOut]:
    rows = await _policies(db, ctx)
    await db.commit()
    return [_out(rows[t]) for t in DOCUMENT_TYPES]


@router.put("/workspaces/{workspace_id}/retention/policies/{document_type}", dependencies=COMPLIANT)
async def put_policy(
    document_type: str,
    body: PolicyIn,
    request: Request,
    ctx: Annotated[WorkspaceContext, Depends(require_workspace(Action.retention_manage))],
    _step_up: Annotated[object, Depends(require_recent_totp(10))],
    user: Annotated[User, Depends(require_totp_compliance)],
    db: Annotated[AsyncSession, Depends(get_db)],
    now: Annotated[datetime, Depends(get_now)],
) -> PolicyOut:
    if document_type not in DOCUMENT_TYPES:
        raise ApiError("not_found", 404)
    row = (await _policies(db, ctx, lock=True))[document_type]
    if row.locked and body.years < row.preset_years:
        raise ApiError(
            BELOW_PRESET, 422, {"document_type": document_type, "minimum": row.preset_years}
        )
    previous = row.years
    row.years = body.years
    await audit.append(
        db,
        workspace_id=ctx.workspace.id,
        action="retention.policy_changed",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        target_type="retention_policy",
        target_id=document_type,
        ip=request.client.host if request.client else None,
        data={"document_type": document_type, "years": body.years, "previous": previous},
        now=now,
    )
    await db.commit()
    return _out(row)


# The end of retention ----------------------------------------------------------------------

NOT_ELIGIBLE: Final = "retention.not_eligible"
BATCH_MAX: Final = 500
ELIGIBLE_DEFAULT_LIMIT: Final = 50
ELIGIBLE_MAX_LIMIT: Final = 200

# Called from the handler, so its refusal is audited like the others.
_step_up = require_recent_totp(STEP_UP_MINUTES)


class EligibleOut(BaseModel):
    id: UUID
    retention_until: date
    state: DocumentState
    # From the workspace's index; None without a fields row or when the index cannot answer.
    document_type: str | None


class EligiblePage(BaseModel):
    items: list[EligibleOut]
    next_cursor: str | None
    engine: Literal["ok", "unavailable"]


class ApproveIn(BaseModel):
    ids: list[UUID] = Field(min_length=1, max_length=BATCH_MAX)


class ApproveOut(BaseModel):
    approved: int


class NotEligible(ApiError):
    """409 `retention.not_eligible` with the offending `ids`."""

    def __init__(self, ids: list[UUID]) -> None:
        super().__init__(NOT_ELIGIBLE, 409, {"ids": [str(i) for i in ids]})
        self.ids = ids


def _eligible(workspace_id: UUID, today: date) -> list[Any]:
    """The workspace's rows eligible for deletion on the region-local date `today`."""
    return [
        DocumentLedger.workspace_id == workspace_id,
        DocumentLedger.retention_until < today,
        DocumentLedger.state.not_in(TERMINAL_STATES),
    ]


def _encode_cursor(row: DocumentLedger) -> str:
    raw = f"{row.retention_until.isoformat()}|{row.id}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str) -> tuple[date, UUID]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        day, id_text = raw.split("|")
        return date.fromisoformat(day), UUID(id_text)
    except (binascii.Error, ValueError, UnicodeDecodeError):
        raise RequestValidationError(
            [{"loc": ("query", "cursor"), "msg": "invalid cursor", "type": "value_error"}]
        ) from None


async def _document_types(
    request: Request, workspace: Workspace, ids: list[UUID]
) -> dict[UUID, str | None] | None:
    """Each document's type from the workspace's index, or None when it cannot answer."""
    engines: IndexEngines = request.app.state.index
    try:
        async with (await engines.engine(workspace)).connect() as conn:
            rows = {i: await index_db.read_fields(conn, i) for i in ids}
    except (LookupError, SQLAlchemyError, OSError) as exc:
        log.warning("index of workspace %s unavailable: %s", workspace.id, type(exc).__name__)
        return None
    return {i: row.document_type if row else None for i, row in rows.items()}


@router.get(
    "/workspaces/{workspace_id}/retention/eligible",
    response_model=EligiblePage,
    dependencies=COMPLIANT,
)
async def list_eligible(
    request: Request,
    ctx: Annotated[WorkspaceContext, Depends(require_workspace(Action.retention_manage))],
    db: Annotated[AsyncSession, Depends(get_db)],
    now: Annotated[datetime, Depends(get_now)],
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=ELIGIBLE_MAX_LIMIT)] = ELIGIBLE_DEFAULT_LIMIT,
) -> EligiblePage:
    """Oldest `retention_until` first, then by id."""
    today = region_local_date(now, request.app.state.settings.region)
    query = select(DocumentLedger).where(*_eligible(ctx.workspace.id, today))
    if cursor is not None:
        after = _decode_cursor(cursor)
        query = query.where(tuple_(DocumentLedger.retention_until, DocumentLedger.id) > after)
    query = query.order_by(DocumentLedger.retention_until, DocumentLedger.id).limit(limit + 1)
    rows = list((await db.scalars(query)).all())
    await db.commit()
    more = len(rows) > limit
    rows = rows[:limit]
    types = await _document_types(request, ctx.workspace, [r.id for r in rows])
    return EligiblePage(
        items=[
            EligibleOut(
                id=r.id,
                retention_until=r.retention_until,
                state=r.state,
                document_type=types.get(r.id) if types else None,
            )
            for r in rows
        ],
        next_cursor=_encode_cursor(rows[-1]) if more else None,
        engine="ok" if types is not None else "unavailable",
    )


async def _locked_eligible(
    db: AsyncSession, workspace: Workspace, ids: list[UUID], today: date
) -> list[DocumentLedger]:
    """The workspace's rows `ids` (sorted), locked FOR UPDATE in id order; `NotEligible`
    naming every one that is not eligible on `today` or not this workspace's."""
    found = {
        row.id: row
        for row in await db.scalars(
            select(DocumentLedger)
            .where(DocumentLedger.workspace_id == workspace.id, DocumentLedger.id.in_(ids))
            .order_by(DocumentLedger.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    }
    offending = [
        i
        for i in ids
        if (row := found.get(i)) is None
        or row.state in TERMINAL_STATES
        or row.retention_until >= today
    ]
    if offending:
        raise NotEligible(offending)
    return [found[i] for i in ids]


@router.post(
    "/workspaces/{workspace_id}/retention/approve-deletion",
    status_code=202,
    response_model=ApproveOut,
    # The body is read in the handler (see the module docstring); documented here.
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": ApproveIn.model_json_schema()}},
        }
    },
)
async def approve_deletion(
    workspace_id: UUID,
    request: Request,
    session: Annotated[UserSession, Depends(current_session())],
    user: Annotated[User, Depends(current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    now: Annotated[datetime, Depends(get_now)],
) -> ApproveOut:
    workspace = await db.get(Workspace, workspace_id)
    if workspace is None:
        raise ApiError("not_found", 404)
    # Kept apart: the rollback before a refusal's audit expires the loaded rows.
    ws_id, user_id = workspace.id, user.id
    settings = request.app.state.settings
    try:
        # Non-members get 404 and another region 421, unaudited; then the role (`forbidden`).
        await authorize(db, user_id, workspace, Action.retention_approve_deletion, settings)
        await require_totp_compliance(session, user, db)
        await _step_up(session, user, now)
        ids = sorted(set(parse_body(ApproveIn, await request.body()).ids))
        today = region_local_date(now, settings.region)
        rows = await _locked_eligible(db, workspace, ids, today)
    except RequestValidationError:
        await audit_refusal(db, request, ws_id, user_id, [None], INVALID_BODY, now)
        raise
    except ApiError as exc:
        if exc.code != "not_found":
            targets: list[UUID | None] = list(exc.ids) if isinstance(exc, NotEligible) else [None]
            await audit_refusal(db, request, ws_id, user_id, targets, exc.code, now)
        raise
    for row in rows:
        row.state = DocumentState.deleting
        row.deletion_approved_at = now
        row.deletion_approved_by = user_id
    # The ledger rows first, then the audit chain's lock (as everywhere else).
    await db.flush()
    for row in rows:
        await audit.append(
            db,
            workspace_id=ws_id,
            action="document.deletion_approved",
            actor_type=ActorType.user,
            actor_id=str(user_id),
            target_type="document",
            target_id=str(row.id),
            ip=client_ip(request),
            now=now,
        )
    await audit.append(
        db,
        workspace_id=ws_id,
        action="retention.deletion_batch",
        actor_type=ActorType.user,
        actor_id=str(user_id),
        ip=client_ip(request),
        data={"count": len(rows)},
        now=now,
    )
    await db.commit()
    for ledger_id in ids:
        await send_removal(request, ledger_id, ws_id)
    return ApproveOut(approved=len(ids))
