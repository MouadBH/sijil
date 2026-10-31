import pytest
from sqlalchemy import text, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from sijil_core.db.models import Membership, Organisation, OrgType, Role, User, Workspace

pytestmark = pytest.mark.db


async def test_org_region_cannot_change(db: AsyncSession, owner_conn: AsyncConnection) -> None:
    org = Organisation(name="Atlas", type=OrgType.business, country="MA", region="ma-cas")
    db.add(org)
    await db.commit()
    with pytest.raises(DBAPIError, match="org.region_immutable"):
        await db.execute(
            update(Organisation).where(Organisation.id == org.id).values(region="eu-fra")
        )
        await db.commit()
    await db.rollback()
    # The trigger binds the owner too: it is not just a grant.
    with pytest.raises(DBAPIError, match="org.region_immutable"):
        await owner_conn.execute(text("UPDATE organisation SET region = 'eu-fra'"))


async def test_email_unique_case_insensitive(db: AsyncSession) -> None:
    db.add(User(email="mouad@example.com", name="M", password_hash="x"))
    await db.commit()
    db.add(User(email="MOUAD@example.com", name="M2", password_hash="x"))
    with pytest.raises(IntegrityError):
        await db.commit()


async def test_email_is_normalised(db: AsyncSession) -> None:
    user = User(email="  Mouad@Example.COM ", name="M", password_hash="x")
    assert user.email == "mouad@example.com"


async def test_membership_scope_exactly_one(
    db: AsyncSession, user: User, org: Organisation, workspace: Workspace
) -> None:
    db.add(Membership(user_id=user.id, org_id=org.id, workspace_id=workspace.id, role=Role.member))
    with pytest.raises(IntegrityError):
        await db.commit()


async def test_membership_scope_neither(db: AsyncSession, user: User) -> None:
    db.add(Membership(user_id=user.id, role=Role.member))
    with pytest.raises(IntegrityError):
        await db.commit()


async def test_membership_unique_per_org(db: AsyncSession, user: User, org: Organisation) -> None:
    db.add(Membership(user_id=user.id, org_id=org.id, role=Role.member))
    await db.commit()
    db.add(Membership(user_id=user.id, org_id=org.id, role=Role.admin))
    with pytest.raises(IntegrityError):
        await db.commit()


def test_role_rank() -> None:
    assert [r.rank for r in (Role.owner, Role.admin, Role.member, Role.readonly)] == [4, 3, 2, 1]


async def test_app_role_cannot_alter_schema(db: AsyncSession) -> None:
    with pytest.raises(DBAPIError):
        await db.execute(text("ALTER TABLE organisation DROP COLUMN region"))
