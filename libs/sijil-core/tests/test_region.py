import pytest
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.settings import CoreSettings

SETTINGS = CoreSettings(region="ma-cas", environment="test")


def test_assert_region_passes_same_region() -> None:
    assert_region("ma-cas", SETTINGS)


def test_assert_region_refuses_other_region() -> None:
    with pytest.raises(RegionMismatch) as e:
        assert_region("eu-fra", SETTINGS)
    assert e.value.code == "region.mismatch"


def test_assert_region_is_exact_match() -> None:
    with pytest.raises(RegionMismatch):
        assert_region("MA-CAS", SETTINGS)
