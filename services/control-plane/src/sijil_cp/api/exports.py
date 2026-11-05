"""Signed period exports (plan 4.6, ARC-5, design §5.6).

`POST …/exports` `{period}` (`YYYY` or `YYYY-MM`) needs `Action.documents_export` and a TOTP
step-up on this session within 10 minutes. A malformed period, a future one or one ending
before the workspace was created (region-local dates) is 422 `export.period_invalid`. It
creates the job (`queued`), audits `export.requested` `{period}`, commits, then sends
`sijil.build_export` to the export worker, the only holder of the region's signing seed; 202
`{id, state}`.

`GET …/exports/{id}` is the job's state. `GET …/exports/{id}/download` (same permission and
step-up) streams a `ready` export's ZIP from the exports bucket with the app key, which only
reads it, and audits `export.downloaded` `{period}`; any other state is 409 `export.not_ready`.

`GET /v1/regions/{region}/export-key` (no sign-in) publishes the region's public key and its
`key_id`, which `sijil-cp export verify` and any third party check a report's signature with.
"""

import base64
import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Annotated, Final
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit
from sijil_core.db.models import ActorType, ExportJob, ExportState, User, UserSession, Workspace
from sijil_core.export_report import key_id, period_bounds
from sijil_core.region import assert_region
from sijil_core.retention import region_local_date
from sijil_core.storage import ExportStore
from sijil_cp.deps import (
    current_user,
    get_db,
    get_now,
    require_recent_totp,
    require_totp_compliance,
)
from sijil_cp.enqueue import Enqueuer, get_enqueuer
from sijil_cp.errors import ApiError
from sijil_cp.permissions import Action, WorkspaceContext, require_workspace
from sijil_cp.settings import CpSettings

log = logging.getLogger(__name__)

router = APIRouter(tags=["exports"], dependencies=[Depends(require_totp_compliance)])
regions = APIRouter(tags=["exports"])

PERIOD_INVALID: Final = "export.period_invalid"
NOT_READY: Final = "export.not_ready"
NOT_FOUND: Final = "export.not_found"
UNAVAILABLE: Final = "export.unavailable"
STEP_UP_MINUTES: Final = 10

Db = Annotated[AsyncSession, Depends(get_db)]
Now = Annotated[datetime, Depends(get_now)]
CurrentUser = Annotated[User, Depends(current_user)]
# Declared in this order on every route: the role first (403 `forbidden`), then the step-up.
Exporting = Annotated[WorkspaceContext, Depends(require_workspace(Action.documents_export))]
SteppedUp = Annotated[UserSession, Depends(require_recent_totp(STEP_UP_MINUTES))]
Queue = Annotated[Enqueuer, Depends(get_enqueuer)]


class ExportIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Optional here so that a missing period answers `export.period_invalid` too.
    period: str | None = None


class ExportCreated(BaseModel):
    id: UUID
    state: ExportState


class ExportOut(BaseModel):
    id: UUID
    period: str
    state: ExportState
    document_count: int | None
    size_bytes: int | None
    sha256: str | None
    error_code: str | None
    created_at: datetime
    ready_at: datetime | None


class ExportKey(BaseModel):
    region: str
    key_id: str
    public_key: str


def _ip(request: Request) -> str | None:
    return request.client.host if request.client else None


def _checked_period(raw: str | None, workspace: Workspace, region: str, now: datetime) -> str:
    period = raw or ""
    try:
        start, end = period_bounds(period)
    except ValueError:
        raise ApiError(PERIOD_INVALID, 422) from None
    if start > region_local_date(now, region):
        raise ApiError(PERIOD_INVALID, 422)
    if end < region_local_date(workspace.created_at, region):
        raise ApiError(PERIOD_INVALID, 422)
    return period


async def _job(db: AsyncSession, workspace: Workspace, export_id: UUID) -> ExportJob:
    job = await db.scalar(
        select(ExportJob).where(ExportJob.id == export_id, ExportJob.workspace_id == workspace.id)
    )
    if job is None:
        raise ApiError(NOT_FOUND, 404)
    return job


def _out(job: ExportJob) -> ExportOut:
    return ExportOut.model_validate(job, from_attributes=True)


@router.post("/workspaces/{workspace_id}/exports", status_code=202, response_model=ExportCreated)
async def request_export(
    body: ExportIn,
    request: Request,
    ctx: Exporting,
    _step_up: SteppedUp,
    user: CurrentUser,
    db: Db,
    now: Now,
    enqueuer: Queue,
) -> ExportCreated:
    settings: CpSettings = request.app.state.settings
    workspace = ctx.workspace
    period = _checked_period(body.period, workspace, settings.region, now)
    job = ExportJob(workspace_id=workspace.id, period=period, requested_by=user.id, created_at=now)
    db.add(job)
    await db.flush()
    await audit.append(
        db,
        workspace_id=workspace.id,
        action="export.requested",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        target_type="export",
        target_id=str(job.id),
        ip=_ip(request),
        data={"period": period},
        now=now,
    )
    await db.commit()
    try:
        enqueuer.send_export(job.id)
    except Exception as exc:
        # The job is committed and stays `queued`; asking again makes a new one.
        log.error("export %s not sent: %s", job.id, type(exc).__name__)
    return ExportCreated(id=job.id, state=job.state)


@router.get("/workspaces/{workspace_id}/exports/{export_id}", response_model=ExportOut)
async def get_export(export_id: UUID, ctx: Exporting, db: Db) -> ExportOut:
    return _out(await _job(db, ctx.workspace, export_id))


async def _chunks(first: bytes, rest: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    yield first
    async for chunk in rest:
        yield chunk


@router.get("/workspaces/{workspace_id}/exports/{export_id}/download")
async def download_export(
    export_id: UUID,
    request: Request,
    ctx: Exporting,
    _step_up: SteppedUp,
    user: CurrentUser,
    db: Db,
    now: Now,
) -> StreamingResponse:
    job = await _job(db, ctx.workspace, export_id)
    if job.state is not ExportState.ready or job.object_key is None:
        raise ApiError(NOT_READY, 409)
    store: ExportStore = request.app.state.exports
    rest = store.open_export(job.object_key)
    try:
        # The first chunk before the 200: a missing object answers an error, not a cut stream.
        first = await anext(rest)
    except (FileNotFoundError, ClientError, BotoCoreError, StopAsyncIteration) as exc:
        log.error("export %s unreadable: %s", job.id, type(exc).__name__)
        raise ApiError(UNAVAILABLE, 503) from None
    await audit.append(
        db,
        workspace_id=job.workspace_id,
        action="export.downloaded",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        target_type="export",
        target_id=str(job.id),
        ip=_ip(request),
        data={"period": job.period},
        now=now,
    )
    await db.commit()
    headers = {
        "Content-Disposition": f'attachment; filename="sijil-export-{job.period}-{job.id}.zip"'
    }
    if job.size_bytes is not None:
        headers["Content-Length"] = str(job.size_bytes)
    return StreamingResponse(_chunks(first, rest), media_type="application/zip", headers=headers)


@regions.get("/regions/{region}/export-key", response_model=ExportKey)
async def export_key(region: str, request: Request) -> ExportKey:
    settings: CpSettings = request.app.state.settings
    assert_region(region, settings)
    public = settings.export_public_key_bytes()
    return ExportKey(
        region=region, key_id=key_id(public), public_key=base64.b64encode(public).decode()
    )
