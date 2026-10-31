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


@pytest.mark.parametrize("region", ["", "   ", "MA-CAS", "morocco", "ma_cas"])
def test_settings_reject_malformed_region(region: str) -> None:
    with pytest.raises(ValidationError):
        CoreSettings(region=region, environment="test")


def test_settings_require_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SIJIL_ENVIRONMENT", raising=False)
    with pytest.raises(ValidationError):
        CoreSettings(region="ma-cas")


def test_settings_reject_unknown_environment() -> None:
    with pytest.raises(ValidationError):
        CoreSettings(region="ma-cas", environment="staging")
