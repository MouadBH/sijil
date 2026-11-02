import pytest
from pydantic import ValidationError

from sijil_cp.main import create_app
from sijil_cp.settings import CpSettings, MigrationSettings


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


_CP_ENV = {
    "region": "ma-cas",
    "environment": "test",
    "database_url": "postgresql+asyncpg://app:x@db/sijil_cp",
    "redis_url": "redis://127.0.0.1:1/0",
    "master_key": "a" * 43 + "=",
    "cookie_secure": "false",
    "inbound_domain": "in.sijil.local",
    "objectstore_endpoint": "http://objectstore:9000",
    "objectstore_access_key": "app-access",
    "objectstore_secret_key": "app-secret",
    "objectstore_bucket": "sijil-originals-ma-cas",
}


def test_the_control_plane_holds_no_sealing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Product-owner ruling: only the seal worker can switch a lock. The control plane builds,
    and `sijil-cp audit verify` runs, without any SIJIL_OBJECTSTORE_SEAL_* setting."""
    monkeypatch.setenv("SIJIL_OBJECTSTORE_SEAL_ACCESS_KEY", "seal-access")
    settings = CpSettings.model_validate(_CP_ENV)
    assert not [name for name in CpSettings.model_fields if "seal" in name]
    assert not hasattr(settings, "seal_storage")
    app = create_app(settings)
    assert not hasattr(app.state, "seal_store")
    assert app.state.store.client is not None
