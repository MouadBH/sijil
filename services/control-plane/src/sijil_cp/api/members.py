"""Organisation members. Every change is a step-up action; owners are protected.

A Member or Read-only user may be limited to some document types (`document_types`, ADM-1; null
for every type): at least one, each from the taxonomy (`sijil_core.fields.DOCUMENT_TYPES`), no
duplicates, else 422 `members.types_invalid`. An Owner or Admin sees every type: a grant for
one is 422 `members.types_not_allowed_for_role`, and a change of role to either clears it. A
change of grant is audited `member.types_changed` `{count}` (null: every type), never the
types themselves.

A new user (one with no membership in the org or its workspaces yet) is refused with 409
`quota.users_exceeded` when the org's standing allows no new user (`sijil_core.billing`: the
user quota is full, or a quota is exceeded), and with 402 `billing.overdue` /
`billing.read_only` while an invoice is unpaid past due. The org row is locked first, so
concurrent additions cannot both take the last seat. An addition that reaches a quota warning
audits it. Changing a role or removing a member is never gated by billing: access control
must always work.
"""

from datetime import datetime
from typing import Annotated, Final
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import billing
from sijil_core.db.models import Membership, Organisation, Role, User, UserSession, Workspace
from sijil_core.fields import DOCUMENT_TYPES
from sijil_cp.deps import (
    current_session,
    ensure_step_up,
    get_db,
    get_now,
    require_totp_compliance,
)
from sijil_cp.errors import ApiError
from sijil_cp.org_audit import audit_org_event
from sijil_cp.permissions import Action, authorize_org

router = APIRouter(tags=["members"], dependencies=[Depends(require_totp_compliance)])

Db = Annotated[AsyncSession, Depends(get_db)]
CurrentUser = Annotated[User, Depends(require_totp_compliance)]
Session = Annotated[UserSession, Depends(current_session())]
Now = Annotated[datetime, Depends(get_now)]

TYPES_INVALID: Final = "members.types_invalid"
TYPES_NOT_ALLOWED: Final = "members.types_not_allowed_for_role"
# The roles a grant may restrict; any other sees every type.
RESTRICTABLE: Final = frozenset({Role.member, Role.readonly})


class MemberOut(BaseModel):
    user_id: UUID
    email: str
    name: str
    role: Role
    document_types: list[str] | None


class MemberIn(BaseModel):
    email: str = Field(max_length=320)
    role: Role
    document_types: list[str] | None = None

    @field_validator("email")
    @classmethod
    def _email(cls, value: str) -> str:
        return value.strip().lower()


class RoleIn(BaseModel):
    """A new role, a new grant, or both. A grant left out is kept (cleared for an Owner or
    Admin); null means every type."""

    role: Role | None = None
    document_types: list[str] | None = None

    @model_validator(mode="after")
    def _role_or_types(self) -> "RoleIn":
        if self.role is None and "document_types" not in self.model_fields_set:
            raise ValueError("a role or document_types is required")
        return self


def _member_out(user: User, role: Role, types: list[str] | None) -> MemberOut:
    return MemberOut(
        user_id=user.id, email=user.email, name=user.name, role=role, document_types=types
    )


def _grant(role: Role, types: list[str] | None) -> list[str] | None:
    """The grant to store, sorted; ApiError 422 when `types` is not one for `role`."""
    if types is None:
        return None
    if not types or len(set(types)) != len(types) or not set(types) <= set(DOCUMENT_TYPES):
        raise ApiError(TYPES_INVALID, 422)
    if role not in RESTRICTABLE:
        raise ApiError(TYPES_NOT_ALLOWED, 422)
    return sorted(types)


async def _audit_grant(
    db: AsyncSession, request: Request, org_id: UUID, actor: User, target: UUID,
    types: list[str] | None, now: datetime,
) -> None:  # fmt: skip
    await audit_org_event(
        db,
        request,
        org_id=org_id,
        actor=actor,
        action="member.types_changed",
        target_type="user",
        target_id=str(target),
        data={"count": None if types is None else len(types)},
        now=now,
    )


async def _ensure_seat(db: AsyncSession, org_id: UUID, user_id: UUID, now: datetime) -> None:
    """409 `quota.users_exceeded` (or 402 with the billing code, overdue or read-only) when
    `user_id` would be a new user the standing refuses.
    Locks the org row, so the check holds until the caller's commit."""
    await db.execute(select(Organisation.id).where(Organisation.id == org_id).with_for_update())
    workspaces = select(Workspace.id).where(Workspace.org_id == org_id)
    counted = await db.scalar(
        select(Membership.id)
        .where(
            Membership.user_id == user_id,
            or_(Membership.org_id == org_id, Membership.workspace_id.in_(workspaces)),
        )
        .limit(1)
    )
    if counted is None:
        standing = await billing.standing(db, org_id, now)
        if not standing.new_users_allowed:
            # Non-payment (`billing.overdue` / `billing.read_only`) says so; a quota is 409.
            if standing.billing_code is not None:
                raise ApiError(standing.billing_code, 402)
            raise ApiError("quota.users_exceeded", 409)


async def _lock_owners(db: AsyncSession, org_id: UUID) -> list[Membership]:
    """The org's owner memberships, row-locked so concurrent demotions are serialised."""
    rows = await db.scalars(
        select(Membership)
        .where(Membership.org_id == org_id, Membership.role == Role.owner)
        .order_by(Membership.id)
        .with_for_update()
    )
    return list(rows)


async def _target(db: AsyncSession, org_id: UUID, user_id: UUID) -> Membership:
    membership = await db.scalar(
        select(Membership)
        .where(Membership.org_id == org_id, Membership.user_id == user_id)
        .with_for_update()
    )
    if membership is None:
        raise ApiError("not_found", 404)
    return membership


@router.get("/orgs/{org_id}/members")
async def list_members(
    org_id: UUID, request: Request, user: CurrentUser, db: Db
) -> list[MemberOut]:
    await authorize_org(db, user.id, org_id, Action.org_manage_members, request.app.state.settings)
    rows = await db.execute(
        select(User, Membership.role, Membership.document_types)
        .join(Membership, Membership.user_id == User.id)
        .where(Membership.org_id == org_id)
        .order_by(User.email)
    )
    return [_member_out(u, role, types) for u, role, types in rows]


@router.post("/orgs/{org_id}/members", status_code=201)
async def add_member(
    org_id: UUID,
    body: MemberIn,
    request: Request,
    user: CurrentUser,
    db: Db,
    session: Session,
    now: Now,
) -> MemberOut:
    _, caller_role = await authorize_org(
        db, user.id, org_id, Action.org_manage_members, request.app.state.settings
    )
    if body.role == Role.owner and caller_role != Role.owner:
        raise ApiError("member.forbidden_role", 403)
    types = _grant(body.role, body.document_types)
    ensure_step_up(session, user, now)
    target = await db.scalar(select(User).where(User.email == body.email))
    if target is None:
        raise ApiError("member.user_not_found", 404)
    await _ensure_seat(db, org_id, target.id, now)
    db.add(Membership(user_id=target.id, org_id=org_id, role=body.role, document_types=types))
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        raise ApiError("member.exists", 409) from None
    await billing.check(db, org_id, now)
    await audit_org_event(
        db,
        request,
        org_id=org_id,
        actor=user,
        action="member.added",
        target_type="user",
        target_id=str(target.id),
        data={"role": body.role.value},
        now=now,
    )
    if types is not None:
        await _audit_grant(db, request, org_id, user, target.id, types, now)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise ApiError("member.exists", 409) from None
    return _member_out(target, body.role, types)


@router.patch("/orgs/{org_id}/members/{user_id}")
async def change_role(
    org_id: UUID,
    user_id: UUID,
    body: RoleIn,
    request: Request,
    user: CurrentUser,
    db: Db,
    session: Session,
    now: Now,
) -> MemberOut:
    _, caller_role = await authorize_org(
        db, user.id, org_id, Action.org_manage_members, request.app.state.settings
    )
    membership = await _target(db, org_id, user_id)
    role = body.role or membership.role
    # Only owners may create owners or change one: an admin can never escalate or demote them.
    if caller_role != Role.owner and Role.owner in (role, membership.role):
        raise ApiError("member.forbidden_role", 403)
    if "document_types" in body.model_fields_set:
        types = _grant(role, body.document_types)
    else:  # kept, unless the new role sees every type
        types = membership.document_types if role in RESTRICTABLE else None
    ensure_step_up(session, user, now)
    if membership.role == Role.owner and role != Role.owner:
        owners = await _lock_owners(db, org_id)
        if len(owners) <= 1:
            raise ApiError("org.last_owner", 409)
    previous, previous_types = membership.role, membership.document_types
    membership.role = role
    membership.document_types = types
    if body.role is not None:
        await audit_org_event(
            db,
            request,
            org_id=org_id,
            actor=user,
            action="member.role_changed",
            target_type="user",
            target_id=str(user_id),
            data={"from": previous.value, "to": role.value},
            now=now,
        )
    if types != previous_types:
        await _audit_grant(db, request, org_id, user, user_id, types, now)
    await db.commit()
    target = await db.get(User, user_id)
    if target is None:  # unreachable: the membership's foreign key guarantees the user
        raise ApiError("not_found", 404)
    return _member_out(target, role, types)


@router.delete("/orgs/{org_id}/members/{user_id}", status_code=204)
async def remove_member(
    org_id: UUID,
    user_id: UUID,
    request: Request,
    user: CurrentUser,
    db: Db,
    session: Session,
    now: Now,
) -> Response:
    _, caller_role = await authorize_org(
        db, user.id, org_id, Action.org_manage_members, request.app.state.settings
    )
    membership = await _target(db, org_id, user_id)
    if caller_role != Role.owner and membership.role == Role.owner:
        raise ApiError("member.forbidden_role", 403)
    ensure_step_up(session, user, now)
    if membership.role == Role.owner:
        owners = await _lock_owners(db, org_id)
        if len(owners) <= 1:
            raise ApiError("org.last_owner", 409)
    removed_role = membership.role
    await db.delete(membership)
    await audit_org_event(
        db,
        request,
        org_id=org_id,
        actor=user,
        action="member.removed",
        target_type="user",
        target_id=str(user_id),
        data={"role": removed_role.value},
        now=now,
    )
    await db.commit()
    return Response(status_code=204)
