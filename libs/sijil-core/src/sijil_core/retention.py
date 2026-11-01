"""Retention periods by country. Provisional until a document is dated (see design §5.2)."""

from datetime import date

# Statutory retention in whole years, by ISO country code.
_RETENTION_YEARS = {"MA": 10}


def retention_years(country: str) -> int:
    """Years a document must be kept in `country`. Unknown country raises KeyError."""
    return _RETENTION_YEARS[country]


def provisional_retain_until(arrival: date, country: str) -> date:
    """Retain until 31 December, `retention_years` after the year of arrival."""
    return date(arrival.year + retention_years(country), 12, 31)
