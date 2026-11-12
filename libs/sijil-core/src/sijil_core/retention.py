"""Retention periods by country and document type. Provisional until a document is dated
(see design §5.2)."""

from collections.abc import Mapping
from datetime import date, datetime, timedelta
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.confidence import plausible_date
from sijil_core.db.models import Organisation, RetentionPolicy, Workspace
from sijil_core.fields import DOCUMENT_TYPES

# Working values, in whole years, by ISO country code and document type: none is confirmed by an
# accountant yet. A workspace starts from its country's row and may only raise it
# (`retention_policy`).
PRESETS: Mapping[str, Mapping[str, int]] = {
    "MA": dict.fromkeys(DOCUMENT_TYPES, 10),
    "FR": dict.fromkeys(DOCUMENT_TYPES, 10),
    "DE": {t: 8 if t in ("invoice", "receipt", "bank_statement") else 10 for t in DOCUMENT_TYPES},
    "AE": dict.fromkeys(DOCUMENT_TYPES, 5),
    "SA": dict.fromkeys(DOCUMENT_TYPES, 6),
}

# The civil time zone of each Sijil region: arrival dates are local, not UTC. Zone data comes
# from the `tzdata` package when the OS has none, so it never depends on the image.
REGION_TZ = {"ma-cas": "Africa/Casablanca"}


def region_local_date(now: datetime, region: str) -> date:
    """The calendar date of the aware instant `now` in `region`. Unknown region: KeyError."""
    if now.tzinfo is None:
        raise ValueError("region_local_date needs a timezone-aware datetime")
    return now.astimezone(ZoneInfo(REGION_TZ[region])).date()


def provisional_retain_until(arrival: date, years: int) -> date:
    """Retain until 31 December, `years` after the year of arrival."""
    return date(arrival.year + years, 12, 31)


def fiscal_retain_until(doc_date: date, years: int) -> date:
    """Retain until 31 December, `years` after the fiscal (= calendar) year of the document's
    date (design §5.4)."""
    return date(doc_date.year + years, 12, 31)


async def policy_years(db: AsyncSession, workspace_id: UUID, document_type: str | None) -> int:
    """The workspace's retention in years for `document_type`; None and unknown types read
    `other`. A type with no row falls back to the preset of the workspace's org country."""
    kind = document_type if document_type in DOCUMENT_TYPES else "other"
    years = await db.scalar(
        select(RetentionPolicy.years).where(
            RetentionPolicy.workspace_id == workspace_id, RetentionPolicy.document_type == kind
        )
    )
    if years is not None:
        return years
    country = await db.scalar(
        select(Organisation.country)
        .join(Workspace, Workspace.org_id == Organisation.id)
        .where(Workspace.id == workspace_id)
    )
    if country is None:
        raise LookupError(f"workspace {workspace_id} not found")
    return PRESETS[country][kind]


async def lowest_policy_years(db: AsyncSession, workspace_id: UUID) -> int:
    """The lowest of the workspace's retention years across every document type (`policy_years`
    of each: its row, else its country's preset). The provisional retention at arrival, when
    the type is not known yet: routing and sealing raise it to the type's own policy, and a
    lock is only ever extended, so it never keeps a type longer than its policy (final review
    I2), and raising one type's policy never raises another's."""
    found = await db.execute(
        select(RetentionPolicy.document_type, RetentionPolicy.years).where(
            RetentionPolicy.workspace_id == workspace_id
        )
    )
    rows: dict[str, int] = {name: years for name, years in found}
    country = await db.scalar(
        select(Organisation.country)
        .join(Workspace, Workspace.org_id == Organisation.id)
        .where(Workspace.id == workspace_id)
    )
    if country is None:
        raise LookupError(f"workspace {workspace_id} not found")
    return min(rows.get(t, PRESETS[country][t]) for t in DOCUMENT_TYPES)


def preset_policies(workspace_id: UUID, country: str) -> list[RetentionPolicy]:
    """One locked row per document type from `country`'s preset, for a new workspace."""
    return [
        RetentionPolicy(
            workspace_id=workspace_id, document_type=t, years=y, preset_years=y, locked=True
        )
        for t, y in PRESETS[country].items()
    ]


def retention_basis(doc_date: str | None, arrival: datetime, region: str, now: datetime) -> date:
    """The date whose fiscal year sets retention, at routing and at seal alike: the document's
    date when plausible (`confidence.plausible_date` against the region-local today), else the
    region-local arrival date, and never after today. An object lock is never shortened, so a
    misread or mistyped year must never lock an original for longer."""
    return _basis_on(doc_date, region_local_date(arrival, region), region_local_date(now, region))


def _basis_on(doc_date: str | None, arrival: date, today: date) -> date:
    return min(plausible_date(doc_date, today) or arrival, today)


def retention_bases(
    doc_date: str | None, arrival: datetime, region: str, now: datetime
) -> frozenset[date]:
    """Every value `retention_basis` gave on some day from the arrival to `now`: its judgement
    depends on the day (a date ahead of it is cut to it, one over 15 years old is no longer
    plausible), so a value judged earlier may still be right. The result changes only on the
    first and last days and around the date itself, so those are the days tried."""
    first, last = region_local_date(arrival, region), region_local_date(now, region)
    days = {first, last}
    try:
        day = date.fromisoformat(doc_date) if doc_date is not None else None
    except ValueError:
        day = None
    if day is not None:
        days |= {d for d in (day - timedelta(days=1), day) if first <= d <= last}
    return frozenset(_basis_on(doc_date, first, today) for today in days)
