"""Retention policy per document type. A policy only ever applies to retention computed from
then on: nothing already set is lowered (the ledger trigger refuses it anyway)."""

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit
from sijil_core.db.models import ActorType, Organisation, RetentionPolicy, User
from sijil_core.fields import DOCUMENT_TYPES
from sijil_core.retention import preset_policies
from sijil_cp.deps import get_db, get_now, require_recent_totp, require_totp_compliance
from sijil_cp.errors import ApiError
from sijil_cp.permissions import Action, WorkspaceContext, require_workspace

router = APIRouter(tags=["retention"], dependencies=[Depends(require_totp_compliance)])

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


@router.get("/workspaces/{workspace_id}/retention/policies")
async def list_policies(
    ctx: Annotated[WorkspaceContext, Depends(require_workspace(Action.retention_manage))],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> list[PolicyOut]:
    rows = await _policies(db, ctx)
    await db.commit()
    return [_out(rows[t]) for t in DOCUMENT_TYPES]


@router.put("/workspaces/{workspace_id}/retention/policies/{document_type}")
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
