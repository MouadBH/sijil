from datetime import UTC, date, datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import Organisation, RetentionPolicy, Workspace
from sijil_core.fields import DOCUMENT_TYPES
from sijil_core.retention import (
    PRESETS,
    REGION_TZ,
    fiscal_retain_until,
    policy_years,
    provisional_retain_until,
    region_local_date,
)


def test_ma_preset_10_years() -> None:
    assert PRESETS["MA"] == dict.fromkeys(DOCUMENT_TYPES, 10)


def test_every_preset_covers_every_type() -> None:
    assert set(PRESETS) == {"MA", "FR", "DE", "AE", "SA"}
    for country, years in PRESETS.items():
        assert set(years) == set(DOCUMENT_TYPES), country
        assert all(1 <= y <= 50 for y in years.values()), country
    assert PRESETS["DE"]["invoice"] == PRESETS["DE"]["receipt"] == 8
    assert PRESETS["DE"]["bank_statement"] == 8
    assert PRESETS["DE"]["contract"] == PRESETS["DE"]["other"] == 10
    assert set(PRESETS["AE"].values()) == {5}
    assert set(PRESETS["SA"].values()) == {6}


def test_provisional_retain_until_end_of_year_plus_years() -> None:
    assert provisional_retain_until(date(2026, 10, 31), 10) == date(2036, 12, 31)


def test_region_tz_map() -> None:
    assert REGION_TZ == {"ma-cas": "Africa/Casablanca"}


def test_region_local_date_crosses_new_year_in_casablanca() -> None:
    # Casablanca was UTC+1 on 31 December 2025 (tzdata), so 23:30 UTC is already 00:30 on
    # 1 January 2026 there. (tzdata 2026e+ has Morocco on UTC+0 from 2026-09-20, so the same
    # wall-clock instant a year later does not cross the year.)
    now = datetime(2025, 12, 31, 23, 30, tzinfo=UTC)
    assert now.date() == date(2025, 12, 31)
    assert region_local_date(now, "ma-cas") == date(2026, 1, 1)
    assert provisional_retain_until(region_local_date(now, "ma-cas"), 10) == date(2036, 12, 31)


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


def test_retention_until_end_of_fiscal_year_plus_years() -> None:
    # Fiscal year = calendar year: kept to 31 December, `years` after the document's year.
    assert fiscal_retain_until(date(2026, 3, 15), 10) == date(2036, 12, 31)
    assert fiscal_retain_until(date(2026, 12, 31), 8) == date(2034, 12, 31)
    assert fiscal_retain_until(date(2026, 1, 1), 5) == date(2031, 12, 31)


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


@pytest.mark.parametrize(
    ("doc_date", "bases"),
    [
        # Dated after its arrival: the day before the date (cut to it), then the date itself.
        ("2026-10-20", {date(2026, 10, 1), date(2026, 10, 19), date(2026, 10, 20)}),
        ("2026-09-30", {date(2026, 9, 30)}),  # plausible on every day since the arrival
        ("2026-11-03", {date(2026, 10, 1), date(2026, 11, 2)}),  # still ahead of today
        # Plausible at the arrival, no longer today (over 15 years old): either judgement.
        ("2011-10-15", {date(2011, 10, 15), date(2026, 10, 1)}),
        (None, {date(2026, 10, 1)}),
    ],
)
def test_retention_bases_are_every_judgement_since_the_arrival(
    doc_date: str | None, bases: set[date]
) -> None:
    """A search row keeps the basis judged on the day it was written: `retention_bases` is
    every value `retention_basis` gave on some day from the arrival to now."""
    from sijil_core.retention import retention_bases

    assert retention_bases(doc_date, ARRIVED, "ma-cas", NOW_R) == bases


@pytest.mark.db
async def test_policy_years_falls_back_to_other_and_country(
    db: AsyncSession, org: Organisation, workspace: Workspace
) -> None:
    # No row: the workspace's country preset, unknown and missing types read `other`.
    assert await policy_years(db, workspace.id, "invoice") == 10
    org.country = "DE"
    await db.commit()
    assert await policy_years(db, workspace.id, "invoice") == 8
    assert await policy_years(db, workspace.id, "contract") == 10
    assert await policy_years(db, workspace.id, None) == 10
    assert await policy_years(db, workspace.id, "no-such-type") == 10
    # Rows win, and an unknown or missing type reads the `other` row.
    db.add_all(
        [
            RetentionPolicy(
                workspace_id=workspace.id, document_type=t, years=y, preset_years=10, locked=True
            )
            for t, y in (("invoice", 12), ("other", 11))
        ]
    )
    await db.commit()
    assert await policy_years(db, workspace.id, "invoice") == 12
    assert await policy_years(db, workspace.id, None) == 11
    assert await policy_years(db, workspace.id, "no-such-type") == 11
    # A type without a row (a partly seeded workspace) falls back to the preset, not to `other`.
    assert await policy_years(db, workspace.id, "contract") == 10


@pytest.mark.db
async def test_lowest_policy_years_across_types(
    db: AsyncSession, org: Organisation, workspace: Workspace
) -> None:
    from sijil_core.retention import lowest_policy_years

    assert await lowest_policy_years(db, workspace.id) == 10  # MA: every type 10
    org.country = "DE"
    await db.commit()
    assert await lowest_policy_years(db, workspace.id) == 8  # invoice, receipt, bank_statement
    # Raising `other` (or any type) never raises the lowest; rows win over the preset.
    db.add_all(
        [
            RetentionPolicy(
                workspace_id=workspace.id, document_type=t, years=y, preset_years=p, locked=True
            )
            for t, y, p in (("other", 15, 10), ("invoice", 9, 8))
        ]
    )
    await db.commit()
    assert await lowest_policy_years(db, workspace.id) == 8  # receipt still on its preset
    for t in ("receipt", "bank_statement"):
        db.add(
            RetentionPolicy(
                workspace_id=workspace.id, document_type=t, years=12, preset_years=8, locked=True
            )
        )
    await db.commit()
    assert await lowest_policy_years(db, workspace.id) == 9
