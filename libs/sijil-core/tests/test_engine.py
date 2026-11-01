import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import User

pytestmark = pytest.mark.db


async def test_errors_never_carry_bound_parameters(db: AsyncSession) -> None:
    """Emails and password hashes must not reach logs through a database error's message."""
    email = "leak-check@example.com"
    secret_hash = "$argon2id$v=19$m=65536,t=3,p=4$c2VjcmV0LWhhc2g"
    db.add(User(email=email, name="A", password_hash=secret_hash))
    await db.commit()
    db.add(User(email=email, name="B", password_hash=secret_hash))
    with pytest.raises(IntegrityError) as exc:
        await db.commit()
    await db.rollback()
    message = str(exc.value)
    assert "uq_user_account_email" in message
    assert secret_hash not in message
    assert "'B'" not in message
    assert email not in message
