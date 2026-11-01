from collections.abc import Callable

import pytest
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import Membership, Organisation, Role, User, Workspace
from sijil_cp import permissions
from sijil_cp.errors import ApiError
from sijil_cp.permissions import MATRIX, Action

pytestmark = pytest.mark.db

ALL = frozenset(Action)
MEMBER = frozenset(
    {
        Action.workspace_read,
        Action.documents_read,
        Action.documents_upload,
        Action.documents_review,
        Action.documents_seal,
    }
)
READONLY = frozenset({Action.workspace_read, Action.documents_read})


def test_matrix_shape() -> None:
    assert MATRIX[Role.owner] == ALL
    assert MATRIX[Role.admin] == ALL - {Action.org_update}
    assert MATRIX[Role.member] == MEMBER
    assert MATRIX[Role.readonly] == READONLY
    assert len(Action) == 13


@pytest.mark.parametrize("role", list(Role))
@pytest.mark.parametrize("action", list(Action))
async def test_role_matrix(
    role: Role,
    action: Action,
    db: AsyncSession,
    user: User,
    org: Organisation,
    workspace: Workspace,
    make_app: Callable[..., FastAPI],
) -> None:
    settings = make_app().state.settings
    db.add(Membership(user_id=user.id, org_id=org.id, role=role))
    await db.commit()
    if action in MATRIX[role]:
        assert await permissions.authorize(db, user.id, workspace, action, settings) == role
    else:
        with pytest.raises(ApiError) as exc:
            await permissions.authorize(db, user.id, workspace, action, settings)
        assert (exc.value.code, exc.value.status) == ("forbidden", 403)


async def test_non_member_gets_404_for_every_action(
    db: AsyncSession,
    user: User,
    workspace: Workspace,
    make_app: Callable[..., FastAPI],
) -> None:
    settings = make_app().state.settings
    for action in Action:
        with pytest.raises(ApiError) as exc:
            await permissions.authorize(db, user.id, workspace, action, settings)
        assert (exc.value.code, exc.value.status) == ("not_found", 404)


async def test_effective_role_is_higher_of_org_and_workspace(
    db: AsyncSession, user: User, org: Organisation, workspace: Workspace
) -> None:
    assert await permissions.effective_role(db, user.id, workspace) is None
    db.add(Membership(user_id=user.id, workspace_id=workspace.id, role=Role.admin))
    await db.commit()
    db.add(Membership(user_id=user.id, org_id=org.id, role=Role.readonly))
    await db.commit()
    assert await permissions.effective_role(db, user.id, workspace) == Role.admin
    org_membership = await db.scalar(select(Membership).where(Membership.org_id == org.id))
    assert org_membership is not None
    org_membership.role = Role.owner
    await db.commit()
    assert await permissions.effective_role(db, user.id, workspace) == Role.owner
