"""Who may do what. Roles come from memberships; the matrix maps roles to actions."""

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, Request
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import Membership, Organisation, Role, User, Workspace
from sijil_core.region import assert_region
from sijil_core.settings import CoreSettings
from sijil_cp.deps import current_user, get_db
from sijil_cp.errors import ApiError


class Action(StrEnum):
    org_read = "org_read"
    org_update = "org_update"
    org_manage_members = "org_manage_members"
    workspace_read = "workspace_read"
    workspace_update = "workspace_update"
    workspace_manage_members = "workspace_manage_members"
    documents_read = "documents_read"
    documents_upload = "documents_upload"
    documents_review = "documents_review"
    documents_seal = "documents_seal"
    documents_export = "documents_export"
    retention_manage = "retention_manage"
    audit_read = "audit_read"


MATRIX: dict[Role, frozenset[Action]] = {
    Role.owner: frozenset(Action),
    Role.admin: frozenset(Action) - {Action.org_update},
    Role.member: frozenset(
        {
            Action.workspace_read,
            Action.documents_read,
            Action.documents_upload,
            Action.documents_review,
            Action.documents_seal,
        }
    ),
    Role.readonly: frozenset({Action.workspace_read, Action.documents_read}),
}


def allows(role: Role, action: Action) -> bool:
    return action in MATRIX[role]


def highest(roles: list[Role]) -> Role | None:
    return max(roles, key=lambda r: r.rank, default=None)


async def org_role(db: AsyncSession, user_id: UUID, org_id: UUID) -> Role | None:
    """The user's organisation-level role (a workspace-only member has none here)."""
    roles = await db.scalars(
        select(Membership.role).where(Membership.user_id == user_id, Membership.org_id == org_id)
    )
    return highest(list(roles))


async def effective_role(db: AsyncSession, user_id: UUID, workspace: Workspace) -> Role | None:
    """The higher of the user's org-level and workspace-level membership."""
    roles = await db.scalars(
        select(Membership.role).where(
            Membership.user_id == user_id,
            or_(Membership.org_id == workspace.org_id, Membership.workspace_id == workspace.id),
        )
    )
    return highest(list(roles))


async def authorize(
    db: AsyncSession,
    user_id: UUID,
    workspace: Workspace,
    action: Action,
    settings: CoreSettings,
) -> Role:
    """The user's role if it allows `action` on the workspace; otherwise an ApiError.

    Non-members get 404 (no existence leak), then the region is checked before anything of the
    workspace is served, then an insufficient role gets 403.
    """
    role = await effective_role(db, user_id, workspace)
    if role is None:
        raise ApiError("not_found", 404)
    org = await db.get(Organisation, workspace.org_id)
    if org is None:
        raise ApiError("not_found", 404)
    assert_region(org.region, settings)
    if not allows(role, action):
        raise ApiError("forbidden", 403)
    return role


@dataclass(frozen=True)
class WorkspaceContext:
    workspace: Workspace
    role: Role


def require_workspace(action: Action) -> Callable[..., Coroutine[Any, Any, WorkspaceContext]]:
    """A dependency for routes with a `{workspace_id}` path parameter."""

    async def dependency(
        workspace_id: UUID,
        request: Request,
        user: Annotated[User, Depends(current_user)],
        db: Annotated[AsyncSession, Depends(get_db)],
    ) -> WorkspaceContext:
        workspace = await db.get(Workspace, workspace_id)
        if workspace is None:
            raise ApiError("not_found", 404)
        role = await authorize(db, user.id, workspace, action, request.app.state.settings)
        return WorkspaceContext(workspace, role)

    return dependency


async def authorize_org(
    db: AsyncSession, user_id: UUID, org_id: UUID, action: Action, settings: CoreSettings
) -> tuple[Organisation, Role]:
    """Like `authorize`, for organisation-level actions on the user's org-level role."""
    org = await db.get(Organisation, org_id)
    role = await org_role(db, user_id, org_id) if org is not None else None
    if org is None or role is None:
        raise ApiError("not_found", 404)
    assert_region(org.region, settings)
    if not allows(role, action):
        raise ApiError("forbidden", 403)
    return org, role
