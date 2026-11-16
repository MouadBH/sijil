"""Who may do what. Roles come from memberships; the matrix maps roles to actions.

Document-type grants (ADM-1): a Member or Read-only membership may name the document types it
sees (`Membership.document_types`, NULL for all). A user's set in a workspace is the union over
their org-level and workspace-level memberships; any unrestricted one, or a role of Admin or
higher, means every type (`visible_types`). A restricted user sees a document only when its
search row's type is in the set, or when it is unclassified and they uploaded it
(`search_query.grant_allows`). A hidden document answers exactly as one that does not exist
(`ensure_visible`), after the role check (ruling R3), and lists leave it out (`hidden_ids`).
"""

from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, Request
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import DocumentLedger, Membership, Organisation, Role, User, Workspace
from sijil_core.index import IndexEngines
from sijil_core.region import assert_region
from sijil_core.search_query import document_types, grant_allows
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
    # The pre-seal removal of a document uploaded by mistake (design §5.4): the Owner's only.
    documents_remove = "documents_remove"
    documents_export = "documents_export"
    retention_manage = "retention_manage"
    # Approving the deletion of documents past retention (design §5.4): the Owner's only.
    retention_approve_deletion = "retention_approve_deletion"
    audit_read = "audit_read"


MATRIX: dict[Role, frozenset[Action]] = {
    Role.owner: frozenset(Action),
    Role.admin: frozenset(Action)
    - {Action.org_update, Action.documents_remove, Action.retention_approve_deletion},
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


async def visible_types(
    db: AsyncSession, user_id: UUID, workspace: Workspace
) -> frozenset[str] | None:
    """The document types the user sees in the workspace; None for every type. The union over
    their memberships: an unrestricted one, or a role of Admin or higher, means every type. No
    membership: none."""
    rows = (
        await db.execute(
            select(Membership.role, Membership.document_types).where(
                Membership.user_id == user_id,
                or_(Membership.org_id == workspace.org_id, Membership.workspace_id == workspace.id),
            )
        )
    ).all()
    types: set[str] = set()
    for role, granted in rows:
        if granted is None or role.rank >= Role.admin.rank:
            return None
        types.update(granted)
    return frozenset(types)


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
    # The caller's document-type grant (`visible_types`); None: every type.
    document_types: frozenset[str] | None = None
    user_id: UUID | None = None


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
        types = await visible_types(db, user.id, workspace)
        return WorkspaceContext(workspace, role, types, user.id)

    return dependency


async def hidden_ids(
    request: Request, ctx: WorkspaceContext, docs: Sequence[tuple[UUID, UUID | None]]
) -> set[UUID]:
    """The documents of `docs` (ledger id, uploader) the caller's grant hides. Unrestricted
    callers never touch the index database."""
    if ctx.document_types is None or not docs:
        return set()
    engines: IndexEngines = request.app.state.index
    async with (await engines.engine(ctx.workspace)).connect() as conn:
        found = await document_types(conn, [doc_id for doc_id, _ in docs])
    return {
        doc_id
        for doc_id, uploaded_by in docs
        if not grant_allows(found.get(doc_id), uploaded_by, ctx.user_id, ctx.document_types)
    }


async def ensure_visible(request: Request, ctx: WorkspaceContext, ledger: DocumentLedger) -> None:
    """404 `document.not_found`, as for a document that does not exist, when the caller's grant
    hides it."""
    if await hidden_ids(request, ctx, [(ledger.id, ledger.uploaded_by)]):
        raise ApiError("document.not_found", 404)


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
