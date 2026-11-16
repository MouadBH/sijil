import pytest
from pydantic import ValidationError

from sijil_core.settings import CoreSettings, SellerSettings
from tests.support.seller import SAMPLE_SELLER, sample_seller


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


@pytest.mark.parametrize("missing", sorted(SAMPLE_SELLER))
def test_seller_settings_are_each_required(missing: str, monkeypatch: pytest.MonkeyPatch) -> None:
    for name in SAMPLE_SELLER:
        monkeypatch.delenv(f"SIJIL_{name.upper()}", raising=False)
    values = {k: v for k, v in SAMPLE_SELLER.items() if k != missing}
    with pytest.raises(ValidationError):
        SellerSettings.model_validate({"region": "ma-cas", "environment": "test"} | values)


def test_seller_settings_read_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in SAMPLE_SELLER.items():
        monkeypatch.setenv(f"SIJIL_{name.upper()}", value)
    settings = SellerSettings(region="ma-cas", environment="test")
    assert settings.seller() == {
        "name": "SAMPLE Sijil SARL",
        "address": "SAMPLE 1 rue Exemple, Casablanca",
        "ice": "009999999000099",
        "if": "99999999",
        "rc": "999999",
        "tp": "99999999",
        "rib": "999 780 0000000000000000 99",
        "bank": "SAMPLE Banque",
    }


@pytest.mark.parametrize("ice", ["12345", "00999999900009X", ""])
def test_seller_ice_is_15_digits(ice: str) -> None:
    with pytest.raises(ValidationError):
        sample_seller(seller_ice=ice)
