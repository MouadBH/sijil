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


def test_the_sealing_key_is_required() -> None:
    with pytest.raises(ValidationError) as missing:
        CpSettings.model_validate(_CP_ENV)
    assert {e["loc"][0] for e in missing.value.errors()} == {
        "objectstore_seal_access_key",
        "objectstore_seal_secret_key",
    }


def test_the_seal_store_is_the_same_bucket_through_the_sealing_key() -> None:
    settings = CpSettings.model_validate(
        _CP_ENV
        | {"objectstore_seal_access_key": "seal-access", "objectstore_seal_secret_key": "seal-s"}
    )
    seal = settings.seal_storage()
    assert seal.objectstore_access_key.get_secret_value() == "seal-access"
    assert seal.objectstore_secret_key.get_secret_value() == "seal-s"
    assert (seal.objectstore_endpoint, seal.objectstore_bucket, seal.region) == (
        settings.objectstore_endpoint,
        settings.objectstore_bucket,
        "ma-cas",
    )
    # The app key is untouched.
    assert settings.objectstore_access_key.get_secret_value() == "app-access"
    app = create_app(settings)
    assert app.state.seal_store is not app.state.store
    assert app.state.store.client is not app.state.seal_store.client
