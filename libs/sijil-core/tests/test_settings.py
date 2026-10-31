import pytest
from pydantic import ValidationError

from sijil_core.settings import CoreSettings


def test_settings_require_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SIJIL_REGION", raising=False)
    with pytest.raises(ValidationError):
        CoreSettings()


def test_settings_read_region_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIJIL_REGION", "ma-cas")
    monkeypatch.setenv("SIJIL_ENVIRONMENT", "test")
    settings = CoreSettings()
    assert settings.region == "ma-cas"
    assert settings.environment == "test"


def test_settings_reject_unknown_environment() -> None:
    with pytest.raises(ValidationError):
        CoreSettings(region="ma-cas", environment="staging")
