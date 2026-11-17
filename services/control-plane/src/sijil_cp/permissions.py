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
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import Depends, Request
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import billing
from sijil_core.db.models import (
    DocumentLedger,
    Membership,
    Organisation,
    Role,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.index import IndexEngines
from sijil_core.region import assert_region
from sijil_core.search_query import document_types, grant_allows
from sijil_core.settings import CoreSettings
from sijil_cp.deps import current_user, get_db, get_now
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
    # The plan, the subscription and the usage meter (ADM-3): Owners and Admins read them, and
    # only the Owner chooses the plan.
    billing_read = "billing_read"
    billing_manage = "billing_manage"
    # Closing the account, its take-over statement (ADM-5): the Owner's only.
    org_close = "org_close"


MATRIX: dict[Role, frozenset[Action]] = {
    Role.owner: frozenset(Action),
    Role.admin: frozenset(Action)
    - {
        Action.org_update,
        Action.documents_remove,
        Action.retention_approve_deletion,
        Action.billing_manage,
        Action.org_close,
    },
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
    workspace is served, then an insufficient role gets 403. A closed account's workspace
    (ADM-5) answers as one that does not exist: a hibernated one to everyone but its Owners
    (who keep their reads; its writes meet the billing gate, `billing.read_only`), a destroyed
    one (`closed`) to everyone.
    """
    role = await effective_role(db, user_id, workspace)
    if role is None:
        raise ApiError("not_found", 404)
    org = await db.get(Organisation, workspace.org_id)
    if org is None:
        raise ApiError("not_found", 404)
    assert_region(org.region, settings)
    if workspace.state is WorkspaceState.closed or (
        workspace.state is WorkspaceState.hibernated and role is not Role.owner
    ):
        raise ApiError("not_found", 404)
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


# The billing gate (ADM-4, design §5.7a) ------------------------------------------------------

Gate = Literal["writes", "export"]
WorkspaceDep = Callable[..., Coroutine[Any, Any, WorkspaceContext]]
# The dependencies that apply the gate, and the endpoints that apply it in their own body (to
# audit the refusal): every state-changing route is one or the other, or on the allow-list of
# services/control-plane/tests/test_billing_gate.py.
BILLING_GATES: set[object] = set()
GATED_IN_HANDLER: set[object] = set()
_GATED: dict[tuple[Action, Gate], WorkspaceDep] = {}


async def ensure_billing(
    db: AsyncSession, org_id: UUID, now: datetime, gate: Gate = "writes"
) -> None:
    """402 `billing.read_only` (any write) or `billing.overdue` (an export) when the org's
    standing refuses it. Capture and reads never come here."""
    standing = await billing.standing(db, org_id, now)
    allowed = standing.export_allowed if gate == "export" else standing.writes_allowed
    if not allowed:
        raise ApiError(standing.billing_code or billing.READ_ONLY_CODE, 402)


def gated_in_handler[F](endpoint: F) -> F:
    """Marks an endpoint that calls `ensure_billing` itself, after its own checks."""
    GATED_IN_HANDLER.add(endpoint)
    return endpoint


def require_writable(action: Action, gate: Gate = "writes") -> WorkspaceDep:
    """`require_workspace` for a state-changing route, then the billing gate. The order is
    ruling R3: authenticated, region, role (`authorize`), then a `{doc_id}` of the path that
    is missing or hidden by the caller's grant answers 404 `document.not_found` (a hidden
    document never reveals the org's billing state), then 402 (`ensure_billing`)."""
    key = (action, gate)
    if key in _GATED:
        return _GATED[key]
    context = require_workspace(action)

    async def dependency(
        request: Request,
        ctx: Annotated[WorkspaceContext, Depends(context)],
        db: Annotated[AsyncSession, Depends(get_db)],
        now: Annotated[datetime, Depends(get_now)],
    ) -> WorkspaceContext:
        raw = request.path_params.get("doc_id")
        if raw is not None:
            try:
                doc_id = UUID(str(raw))
            except ValueError:
                raise ApiError("document.not_found", 404) from None
            row = (
                await db.execute(
                    select(DocumentLedger.id, DocumentLedger.uploaded_by).where(
                        DocumentLedger.id == doc_id,
                        DocumentLedger.workspace_id == ctx.workspace.id,
                    )
                )
            ).first()
            if row is None or await hidden_ids(request, ctx, [(row.id, row.uploaded_by)]):
                raise ApiError("document.not_found", 404)
        await ensure_billing(db, ctx.workspace.org_id, now, gate)
        return ctx

    BILLING_GATES.add(dependency)
    _GATED[key] = dependency
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
