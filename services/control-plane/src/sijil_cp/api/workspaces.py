"""Workspaces. Responses never carry the Paperless URL or any credential."""

import re
import secrets
import unicodedata
import uuid
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import OrgType, Role, User, Workspace, WorkspaceState
from sijil_core.tenancy import tenant_db_names
from sijil_cp.deps import get_db, require_totp_compliance
from sijil_cp.errors import ApiError
from sijil_cp.permissions import Action, WorkspaceContext, authorize_org, require_workspace

router = APIRouter(tags=["workspaces"], dependencies=[Depends(require_totp_compliance)])

_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"
_SLUG_BASE_MAX = 40


class WorkspaceOut(BaseModel):
    id: UUID
    org_id: UUID
    name: str
    slug: str
    state: WorkspaceState
    role: Role


class WorkspaceIn(BaseModel):
    org_id: UUID
    name: str = Field(min_length=1, max_length=200)


def workspace_out(workspace: Workspace, role: Role) -> WorkspaceOut:
    return WorkspaceOut(
        id=workspace.id,
        org_id=workspace.org_id,
        name=workspace.name,
        slug=workspace.slug,
        state=workspace.state,
        role=role,
    )


def make_slug(name: str) -> str:
    folded = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    base = re.sub(r"[^a-z0-9]+", "-", folded.lower()).strip("-")[:_SLUG_BASE_MAX].strip("-")
    suffix = "".join(secrets.choice(_ALPHABET) for _ in range(6))
    return f"{base or 'ws'}-{suffix}"


def new_workspace(org_id: UUID, name: str) -> Workspace:
    """A workspace in `provisioning`, with the names its tenant stack will use."""
    slug = make_slug(name)
    db_name, index_db_name = tenant_db_names(slug)
    # The key id embeds the primary key, so it is assigned here rather than at flush.
    workspace_id = uuid.uuid4()
    return Workspace(
        id=workspace_id,
        org_id=org_id,
        name=name,
        slug=slug,
        state=WorkspaceState.provisioning,
        db_name=db_name,
        index_db_name=index_db_name,
        kms_key_id=f"ws-{workspace_id}",
    )


@router.post("/workspaces", status_code=201)
async def create_workspace(
    body: WorkspaceIn,
    request: Request,
    user: Annotated[User, Depends(require_totp_compliance)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> WorkspaceOut:
    org, _ = await authorize_org(
        db, user.id, body.org_id, Action.org_manage_members, request.app.state.settings
    )
    # Business organisations are one workspace each; firms are not accepted yet either.
    if org.type == OrgType.business:
        raise ApiError("org.workspace_limit", 409)
    raise ApiError("org.type_unsupported", 422)


@router.get("/workspaces/{workspace_id}")
async def get_workspace(
    ctx: Annotated[WorkspaceContext, Depends(require_workspace(Action.workspace_read))],
) -> WorkspaceOut:
    return workspace_out(ctx.workspace, ctx.role)
