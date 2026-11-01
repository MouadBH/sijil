"""Organisations. Creating one also creates its workspace and queues its provisioning."""

import asyncio
import logging
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field, field_validator
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import (
    Job,
    JobKind,
    Membership,
    Organisation,
    OrgStatus,
    OrgType,
    Role,
    User,
    UserSession,
)
from sijil_core.region import assert_region
from sijil_cp.api.workspaces import WorkspaceOut, new_workspace, workspace_out
from sijil_cp.auth import ratelimit
from sijil_cp.deps import (
    current_session,
    ensure_step_up,
    get_db,
    get_now,
    get_redis,
    require_totp_compliance,
)
from sijil_cp.enqueue import Enqueuer, get_enqueuer
from sijil_cp.errors import ApiError
from sijil_cp.permissions import Action, authorize_org

logger = logging.getLogger(__name__)

router = APIRouter(tags=["orgs"], dependencies=[Depends(require_totp_compliance)])

# Country → region. A resource's region is fixed when it is created.
COUNTRY_REGIONS: dict[str, str] = {"MA": "ma-cas"}

Db = Annotated[AsyncSession, Depends(get_db)]
CurrentUser = Annotated[User, Depends(require_totp_compliance)]


class OrgOut(BaseModel):
    id: UUID
    name: str
    type: OrgType
    country: str
    region: str
    status: OrgStatus
    require_totp: bool
    role: Role


class OrgIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    type: OrgType
    country: str = Field(min_length=2, max_length=2)

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("empty name")
        return value

    @field_validator("country")
    @classmethod
    def _country(cls, value: str) -> str:
        return value.upper()


class OrgPatch(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    require_totp: bool | None = None
    # Accepted only to be refused: the region never changes after creation.
    region: Any = None

    @field_validator("name")
    @classmethod
    def _name(cls, value: str | None) -> str | None:
        if value is not None:
            value = value.strip()
            if not value:
                raise ValueError("empty name")
        return value


class OrgCreated(BaseModel):
    org: OrgOut
    workspace: WorkspaceOut
    job_id: UUID


def org_out(org: Organisation, role: Role) -> OrgOut:
    return OrgOut(
        id=org.id,
        name=org.name,
        type=org.type,
        country=org.country,
        region=org.region,
        status=org.status,
        require_totp=org.require_totp,
        role=role,
    )


@router.post("/orgs", status_code=202)
async def create_org(
    body: OrgIn,
    request: Request,
    user: CurrentUser,
    db: Db,
    enqueuer: Annotated[Enqueuer, Depends(get_enqueuer)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> OrgCreated:
    ip = request.client.host if request.client else ""
    key = ratelimit.ip_key("org_create", ip)
    if not await ratelimit.hit(
        redis, key, ratelimit.ORG_CREATE_LIMIT, ratelimit.ORG_CREATE_WINDOW_S
    ):
        raise ApiError("auth.rate_limited", 429)
    if body.type != OrgType.business:
        raise ApiError("org.type_unsupported", 422)
    region = COUNTRY_REGIONS.get(body.country)
    if region is None:
        raise ApiError("org.country_unsupported", 422)
    # Before anything is written: this service only creates organisations of its own region.
    assert_region(region, request.app.state.settings)

    # The user's row lock serialises their concurrent creations, so the cap holds under races.
    await db.execute(select(User.id).where(User.id == user.id).with_for_update())
    owned = await db.scalar(
        select(func.count())
        .select_from(Membership)
        .where(
            Membership.user_id == user.id,
            Membership.role == Role.owner,
            Membership.org_id.is_not(None),
        )
    )
    if (owned or 0) >= ratelimit.MAX_OWNED_ORGS:
        raise ApiError("org.limit_reached", 409)

    org = Organisation(name=body.name, type=body.type, country=body.country, region=region)
    db.add(org)
    await db.flush()
    db.add(Membership(user_id=user.id, org_id=org.id, role=Role.owner))
    workspace = new_workspace(org.id, body.name)
    db.add(workspace)
    await db.flush()
    job = Job(kind=JobKind.provision_workspace, workspace_id=workspace.id)
    db.add(job)
    await db.commit()

    try:
        await asyncio.to_thread(enqueuer.send, job.kind, job.id, workspace.id)
    except Exception:
        # The job stays `queued`; a failed hand-off must not hide the created organisation.
        logger.exception("could not enqueue job %s", job.id)
    return OrgCreated(
        org=org_out(org, Role.owner),
        workspace=workspace_out(workspace, Role.owner),
        job_id=job.id,
    )


@router.get("/orgs/{org_id}")
async def get_org(org_id: UUID, request: Request, user: CurrentUser, db: Db) -> OrgOut:
    org, role = await authorize_org(
        db, user.id, org_id, Action.org_read, request.app.state.settings
    )
    return org_out(org, role)


@router.patch("/orgs/{org_id}")
async def patch_org(
    org_id: UUID,
    body: OrgPatch,
    request: Request,
    user: CurrentUser,
    db: Db,
    session: Annotated[UserSession, Depends(current_session())],
    now: Annotated[datetime, Depends(get_now)],
) -> OrgOut:
    org, role = await authorize_org(
        db, user.id, org_id, Action.org_update, request.app.state.settings
    )
    if "region" in body.model_fields_set:
        raise ApiError("org.region_immutable", 409)
    if body.require_totp is not None:
        ensure_step_up(session, user, now)
        org.require_totp = body.require_totp
    if body.name is not None:
        org.name = body.name
    await db.commit()
    return org_out(org, role)
