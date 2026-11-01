"""Job status, visible to members of the job's workspace."""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import Job, JobKind, JobStatus, User, Workspace
from sijil_cp.deps import get_db, require_totp_compliance
from sijil_cp.errors import ApiError
from sijil_cp.permissions import Action, authorize

router = APIRouter(tags=["jobs"], dependencies=[Depends(require_totp_compliance)])


class JobOut(BaseModel):
    id: UUID
    kind: JobKind
    status: JobStatus
    workspace_id: UUID
    error_code: str | None
    created_at: datetime
    updated_at: datetime


@router.get("/jobs/{job_id}")
async def get_job(
    job_id: UUID,
    request: Request,
    user: Annotated[User, Depends(require_totp_compliance)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> JobOut:
    job = await db.get(Job, job_id)
    workspace = await db.get(Workspace, job.workspace_id) if job and job.workspace_id else None
    if job is None or workspace is None:
        raise ApiError("not_found", 404)
    await authorize(db, user.id, workspace, Action.workspace_read, request.app.state.settings)
    return JobOut(
        id=job.id,
        kind=job.kind,
        status=job.status,
        workspace_id=workspace.id,
        error_code=job.error_code,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )
