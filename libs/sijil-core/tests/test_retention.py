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


def test_fiscal_retain_until_ma() -> None:
    from sijil_core.retention import fiscal_retain_until

    # Fiscal year = calendar year: kept to 31 December, ten years after the document's year.
    assert fiscal_retain_until(date(2024, 3, 15), "MA") == date(2034, 12, 31)
    assert fiscal_retain_until(date(2027, 1, 2), "MA") == date(2037, 12, 31)
    assert fiscal_retain_until(date(2026, 12, 31), "MA") == date(2036, 12, 31)
    with pytest.raises(KeyError):
        fiscal_retain_until(date(2026, 1, 1), "XX")


ARRIVED = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
NOW_R = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("doc_date", "basis"),
    [
        ("2026-09-30", date(2026, 9, 30)),  # a plausible date
        ("2026-11-03", date(2026, 11, 2)),  # one day ahead: plausible, capped at today
        ("2099-01-01", date(2026, 10, 1)),  # implausible: the arrival date
        ("2005-01-01", date(2026, 10, 1)),  # more than 15 years back: the arrival date
        (None, date(2026, 10, 1)),
        ("not a date", date(2026, 10, 1)),
    ],
)
def test_retention_basis_is_shared_by_routing_and_seal(doc_date: str | None, basis: date) -> None:
    from sijil_core.retention import retention_basis

    assert retention_basis(doc_date, ARRIVED, "ma-cas", NOW_R) == basis
