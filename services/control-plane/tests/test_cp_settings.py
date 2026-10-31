import pytest
from pydantic import ValidationError

from sijil_cp.settings import MigrationSettings


def test_migrations_use_their_own_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIJIL_REGION", "ma-cas")
    monkeypatch.setenv("SIJIL_ENVIRONMENT", "test")
    monkeypatch.setenv("SIJIL_DATABASE_URL", "postgresql+asyncpg://app:x@db/sijil_cp")
    monkeypatch.setenv("SIJIL_MIGRATION_DATABASE_URL", "postgresql+asyncpg://owner:y@db/sijil_cp")
    assert str(MigrationSettings().migration_database_url).startswith("postgresql+asyncpg://owner:")


def test_migrations_never_fall_back_to_the_app_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIJIL_REGION", "ma-cas")
    monkeypatch.setenv("SIJIL_ENVIRONMENT", "test")
    monkeypatch.setenv("SIJIL_DATABASE_URL", "postgresql+asyncpg://app:x@db/sijil_cp")
    monkeypatch.delenv("SIJIL_MIGRATION_DATABASE_URL", raising=False)
    with pytest.raises(ValidationError):
        MigrationSettings()
