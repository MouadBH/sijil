"""Organisation members. Every change is a step-up action; owners are protected."""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import Membership, Role, User, UserSession
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


class MemberOut(BaseModel):
    user_id: UUID
    email: str
    name: str
    role: Role


class MemberIn(BaseModel):
    email: str = Field(max_length=320)
    role: Role

    @field_validator("email")
    @classmethod
    def _email(cls, value: str) -> str:
        return value.strip().lower()


class RoleIn(BaseModel):
    role: Role


def _member_out(user: User, role: Role) -> MemberOut:
    return MemberOut(user_id=user.id, email=user.email, name=user.name, role=role)


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
        select(User, Membership.role)
        .join(Membership, Membership.user_id == User.id)
        .where(Membership.org_id == org_id)
        .order_by(User.email)
    )
    return [_member_out(u, role) for u, role in rows]


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
    ensure_step_up(session, user, now)
    target = await db.scalar(select(User).where(User.email == body.email))
    if target is None:
        raise ApiError("member.user_not_found", 404)
    db.add(Membership(user_id=target.id, org_id=org_id, role=body.role))
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        raise ApiError("member.exists", 409) from None
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
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise ApiError("member.exists", 409) from None
    return _member_out(target, body.role)


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
    # Only owners may create owners or change one: an admin can never escalate or demote them.
    if caller_role != Role.owner and Role.owner in (body.role, membership.role):
        raise ApiError("member.forbidden_role", 403)
    ensure_step_up(session, user, now)
    if membership.role == Role.owner and body.role != Role.owner:
        owners = await _lock_owners(db, org_id)
        if len(owners) <= 1:
            raise ApiError("org.last_owner", 409)
    previous = membership.role
    membership.role = body.role
    await audit_org_event(
        db,
        request,
        org_id=org_id,
        actor=user,
        action="member.role_changed",
        target_type="user",
        target_id=str(user_id),
        data={"from": previous.value, "to": body.role.value},
        now=now,
    )
    await db.commit()
    target = await db.get(User, user_id)
    if target is None:  # unreachable: the membership's foreign key guarantees the user
        raise ApiError("not_found", 404)
    return _member_out(target, body.role)


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
