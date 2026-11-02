from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from sijil_core.retention import (
    REGION_TZ,
    provisional_retain_until,
    region_local_date,
    retention_years,
)


def test_retention_years_ma_10() -> None:
    assert retention_years("MA") == 10


def test_provisional_retain_until_end_of_year_plus_10() -> None:
    assert provisional_retain_until(date(2026, 10, 31), "MA") == date(2036, 12, 31)


def test_unknown_country_raises() -> None:
    with pytest.raises(KeyError):
        retention_years("ZZ")


def test_region_tz_map() -> None:
    assert REGION_TZ == {"ma-cas": "Africa/Casablanca"}


def test_region_local_date_crosses_new_year_in_casablanca() -> None:
    # Casablanca was UTC+1 on 31 December 2025 (tzdata), so 23:30 UTC is already 00:30 on
    # 1 January 2026 there. (tzdata 2026e+ has Morocco on UTC+0 from 2026-09-20, so the same
    # wall-clock instant a year later does not cross the year.)
    now = datetime(2025, 12, 31, 23, 30, tzinfo=UTC)
    assert now.date() == date(2025, 12, 31)
    assert region_local_date(now, "ma-cas") == date(2026, 1, 1)
    assert provisional_retain_until(region_local_date(now, "ma-cas"), "MA") == date(2036, 12, 31)


def test_region_local_date_accepts_any_aware_datetime() -> None:
    now = datetime(2026, 1, 1, 1, 30, tzinfo=timezone(timedelta(hours=2)))  # 2025-12-31 23:30Z
    assert region_local_date(now, "ma-cas") == date(2026, 1, 1)
    assert region_local_date(datetime(2026, 6, 1, 12, tzinfo=UTC), "ma-cas") == date(2026, 6, 1)


def test_region_local_date_unknown_region_raises() -> None:
    with pytest.raises(KeyError):
        region_local_date(datetime(2026, 1, 1, tzinfo=UTC), "zz-zzz")


def test_region_local_date_refuses_naive() -> None:
    with pytest.raises(ValueError):
        region_local_date(datetime(2026, 1, 1), "ma-cas")  # noqa: DTZ001
