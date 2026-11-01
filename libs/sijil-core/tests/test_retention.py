from datetime import date

import pytest

from sijil_core.retention import provisional_retain_until, retention_years


def test_retention_years_ma_10() -> None:
    assert retention_years("MA") == 10


def test_provisional_retain_until_end_of_year_plus_10() -> None:
    assert provisional_retain_until(date(2026, 10, 31), "MA") == date(2036, 12, 31)


def test_unknown_country_raises() -> None:
    with pytest.raises(KeyError):
        retention_years("ZZ")
