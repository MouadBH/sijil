"""Retention periods by country. Provisional until a document is dated (see design §5.2)."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

# Statutory retention in whole years, by ISO country code.
_RETENTION_YEARS = {"MA": 10}

# The civil time zone of each Sijil region: arrival dates are local, not UTC. Zone data comes
# from the `tzdata` package when the OS has none, so it never depends on the image.
REGION_TZ = {"ma-cas": "Africa/Casablanca"}


def retention_years(country: str) -> int:
    """Years a document must be kept in `country`. Unknown country raises KeyError."""
    return _RETENTION_YEARS[country]


def region_local_date(now: datetime, region: str) -> date:
    """The calendar date of the aware instant `now` in `region`. Unknown region: KeyError."""
    if now.tzinfo is None:
        raise ValueError("region_local_date needs a timezone-aware datetime")
    return now.astimezone(ZoneInfo(REGION_TZ[region])).date()


def provisional_retain_until(arrival: date, country: str) -> date:
    """Retain until 31 December, `retention_years` after the year of arrival."""
    return date(arrival.year + retention_years(country), 12, 31)
