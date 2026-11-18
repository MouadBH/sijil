"""MAD invoices: amounts, proration, the total in words, gapless numbers and frozen snapshots
(ADM-4, plan 6.3)."""

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import delete, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from sijil_core import invoicing
from sijil_core.db.models import (
    AuditEvent,
    Invoice,
    InvoiceKind,
    InvoiceState,
    Locale,
    Membership,
    Organisation,
    Plan,
    Role,
    User,
    Workspace,
)
from tests.support.seller import sample_seller

CASA = ZoneInfo("Africa/Casablanca")
NOW = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)
YEAR = (datetime(2026, 12, 16, tzinfo=CASA), datetime(2027, 12, 16, tzinfo=CASA))
BUYER_ICE = "002999888000011"  # SAMPLE: a fictitious company's ICE


def test_invoice_totals_mad_with_vat_20() -> None:
    assert invoicing.amounts(Decimal("2400.00"), None) == invoicing.Amounts(
        ht=Decimal("2400.00"), vat=Decimal("480.00"), ttc=Decimal("2880.00")
    )
    assert invoicing.amounts(Decimal("6000.00"), None).ttc == Decimal("7200.00")
    # Half a cent rounds up (ROUND_HALF_UP), never to even, and never through a float.
    half = invoicing.amounts(Decimal("2.05"), Fraction(1, 2))  # HT 1.025
    assert (half.ht, half.vat, half.ttc) == (Decimal("1.03"), Decimal("0.21"), Decimal("1.24"))
    half = invoicing.amounts(Decimal("1.01"), Fraction(1, 2))  # HT 0.505
    assert (half.ht, half.vat, half.ttc) == (Decimal("0.51"), Decimal("0.10"), Decimal("0.61"))
    vat_half = invoicing.amounts(Decimal("0.03"), None)  # VAT 0.006
    assert (vat_half.vat, vat_half.ttc) == (Decimal("0.01"), Decimal("0.04"))
    assert invoicing.amounts(Decimal("0.02"), None).vat == Decimal("0.00")  # VAT 0.004
    for price in ("2400.00", "0.01", "9999.99", "14400.00"):
        for fraction in (None, Fraction(1, 365), Fraction(200, 366), Fraction(1, 3)):
            a = invoicing.amounts(Decimal(price), fraction)
            assert a.ttc == a.ht + a.vat
            assert all(v.as_tuple().exponent == -2 for v in (a.ht, a.vat, a.ttc))


def test_upgrade_prorated_first_and_last_day() -> None:
    start, end = datetime(2027, 1, 1, tzinfo=CASA), datetime(2028, 1, 1, tzinfo=CASA)
    first = datetime(2027, 1, 1, 9, 30, tzinfo=CASA)
    last = datetime(2027, 12, 31, 23, 59, tzinfo=CASA)
    assert invoicing.prorata(start, end, first, "ma-cas") == Fraction(1)
    assert invoicing.prorata(start, end, last, "ma-cas") == Fraction(1, 365)
    # The region's calendar decides the day, whatever zone the instant is given in.
    leap = (datetime(2028, 1, 1, tzinfo=CASA), datetime(2029, 1, 1, tzinfo=CASA))
    new_year = datetime(2028, 1, 1, 0, 30, tzinfo=CASA).astimezone(UTC)
    assert invoicing.prorata(*leap, new_year, "ma-cas") == 1
    assert invoicing.prorata(*leap, datetime(2028, 12, 31, 12, tzinfo=CASA), "ma-cas") == (
        Fraction(1, 366)
    )
    # pro (6 000.00) over essentiel (2 400.00): 3 600.00 for the whole year, a 365th on the last.
    full = invoicing.amounts(Decimal("3600.00"), Fraction(1))
    assert (full.ht, full.vat, full.ttc) == (
        Decimal("3600.00"), Decimal("720.00"), Decimal("4320.00")
    )  # fmt: skip
    day = invoicing.amounts(Decimal("3600.00"), Fraction(1, 365))  # 9.8630...
    assert (day.ht, day.vat, day.ttc) == (Decimal("9.86"), Decimal("1.97"), Decimal("11.83"))


@pytest.mark.parametrize(
    ("amount", "locale", "words"),
    [
        ("2880.00", "fr", "deux mille huit cent quatre-vingts dirhams"),
        ("2880.50", "fr", "deux mille huit cent quatre-vingts dirhams et cinquante centimes"),
        ("1.01", "fr", "un dirham et un centime"),
        ("1000000.00", "fr", "un million de dirhams"),
        ("17280.00", "fr", "dix-sept mille deux cent quatre-vingts dirhams"),
        ("2880.00", "ar", "ألفان وثمانمائة وثمانون درهما"),
        ("2880.50", "ar", "ألفان وثمانمائة وثمانون درهما وخمسون سنتيما"),
        ("1.01", "ar", "درهم واحد وسنتيم واحد"),
        ("2.02", "ar", "درهمان وسنتيمان"),
        ("3.10", "ar", "ثلاثة دراهم وعشرة سنتيمات"),
        ("7200.00", "ar", "سبعة آلاف ومئتا درهم"),
        ("17280.00", "ar", "سبعة عشر ألفاً ومئتان وثمانون درهما"),
        ("11.83", "ar", "أحد عشر درهما وثلاثة وثمانون سنتيما"),
    ],
)
def test_total_in_words_matches_the_digits(amount: str, locale: str, words: str) -> None:
    assert invoicing.amount_words(Decimal(amount), invoicing.messages(locale)) == words


def test_invoice_messages_fall_back_to_french() -> None:
    assert invoicing.invoice_locale(Locale.en) is Locale.fr
    assert invoicing.invoice_locale(Locale.ar) is Locale.ar
    assert invoicing.messages("fr")["title"] == "Facture"


async def _owned_org(db: AsyncSession, org: Organisation, user: User, locale: Locale) -> None:
    user.locale = locale
    org.ice = BUYER_ICE
    db.add(Membership(user_id=user.id, org_id=org.id, role=Role.owner))
    await db.commit()


async def _plan(db: AsyncSession, code: str) -> Plan:
    plan = await db.get(Plan, code)
    assert plan is not None
    return plan


@pytest.mark.db
async def test_issue_freezes_amounts_snapshots_and_pdf(
    db: AsyncSession, org: Organisation, user: User, workspace: Workspace
) -> None:
    await _owned_org(db, org, user, Locale.ar)
    invoice = await invoicing.issue(
        db, org, InvoiceKind.annual, await _plan(db, "essentiel"), YEAR, NOW,
        seller=sample_seller(),
    )  # fmt: skip
    await db.commit()
    assert invoice.number == "SIJ-2026-000001"
    assert invoice.year == 2026
    assert (invoice.amount_ht, invoice.vat_rate, invoice.amount_vat, invoice.amount_ttc) == (
        Decimal("2400.00"), Decimal("20.00"), Decimal("480.00"), Decimal("2880.00")
    )  # fmt: skip
    assert (invoice.currency, invoice.state, invoice.locale) == ("MAD", InvoiceState.issued, "ar")
    assert invoice.due_at == NOW + timedelta(days=30)
    assert invoice.buyer == {"name": "Atlas", "ice": BUYER_ICE}
    assert invoice.seller == sample_seller().seller()
    assert invoice.pdf.startswith(b"%PDF")
    assert invoice.pdf_sha256 == hashlib.sha256(invoice.pdf).hexdigest()
    events = list(
        await db.scalars(select(AuditEvent).where(AuditEvent.action == "billing.invoice_issued"))
    )
    assert [(e.workspace_id, e.target_type, e.target_id) for e in events] == [
        (workspace.id, "invoice", str(invoice.id))
    ]
    assert events[0].data == {
        "number": "SIJ-2026-000001",
        "kind": "annual",
        "plan": "essentiel",
        "amount_ttc": "2880.00",
    }


@pytest.mark.db
async def test_invoice_numbers_gapless_under_concurrency(
    db: AsyncSession, org: Organisation, user: User
) -> None:
    await _owned_org(db, org, user, Locale.fr)
    plan = await _plan(db, "essentiel")
    sessions = async_sessionmaker(db.bind, expire_on_commit=False)
    held = asyncio.Event()

    async def issue(commit: bool, hold: asyncio.Event | None = None) -> str:
        async with sessions() as session:
            row = await session.get(Organisation, org.id)
            assert row is not None
            invoice = await invoicing.issue(
                session, row, InvoiceKind.annual, plan, YEAR, NOW, seller=sample_seller()
            )
            if hold is not None:
                held.set()
                await hold.wait()
            if commit:
                await session.commit()
            else:
                await session.rollback()
            return invoice.number

    # The first holds its number, the second waits for it; the first rolls back, so the
    # second gets that same number: no gap.
    release = asyncio.Event()
    first = asyncio.create_task(issue(False, release))
    await held.wait()
    second = asyncio.create_task(issue(True))
    await asyncio.sleep(0.2)
    assert not second.done()  # blocked on the counter row
    release.set()
    assert await first == "SIJ-2026-000001"
    assert await second == "SIJ-2026-000001"
    # Two at the same moment (a renewal and an upgrade): two numbers, in sequence.
    numbers = await asyncio.gather(issue(True), issue(True))
    assert sorted(numbers) == ["SIJ-2026-000002", "SIJ-2026-000003"]
    stored = await db.scalars(select(Invoice.number).order_by(Invoice.number))
    assert list(stored) == ["SIJ-2026-000001", "SIJ-2026-000002", "SIJ-2026-000003"]
    # A new calendar year (of the region) starts again at 1.
    async with sessions() as session:
        row = await session.get(Organisation, org.id)
        assert row is not None
        new_year = datetime(2027, 1, 1, 0, 30, tzinfo=CASA).astimezone(UTC)
        invoice = await invoicing.issue(
            session, row, InvoiceKind.annual, plan, YEAR, new_year, seller=sample_seller()
        )
        await session.commit()
    assert (invoice.number, invoice.year) == ("SIJ-2027-000001", 2027)


@pytest.mark.db
async def test_issued_invoice_unchanged_by_price_or_seller_change(
    db: AsyncSession, org: Organisation, user: User, owner_conn: AsyncConnection
) -> None:
    await _owned_org(db, org, user, Locale.fr)
    invoice = await invoicing.issue(
        db, org, InvoiceKind.annual, await _plan(db, "essentiel"), YEAR, NOW,
        seller=sample_seller(),
    )  # fmt: skip
    await db.commit()
    before = (invoice.amount_ht, invoice.amount_ttc, invoice.buyer, invoice.seller)
    pdf_sha, invoice_id = invoice.pdf_sha256, invoice.id
    # The catalogue, the org and the seller all change afterwards.
    await owner_conn.execute(text("UPDATE plan SET price_ht = 9999.00 WHERE code = 'essentiel'"))
    try:
        org.name, org.ice = "Atlas Renamed", "002999888000099"
        await db.commit()
        issued_later = await invoicing.issue(
            db, org, InvoiceKind.annual, await _plan(db, "essentiel"), YEAR, NOW,
            seller=sample_seller(seller_name="SAMPLE Other SARL"),
        )  # fmt: skip
        await db.commit()
        stored = await db.get(Invoice, invoice_id, populate_existing=True)
        assert stored is not None
        assert (stored.amount_ht, stored.amount_ttc, stored.buyer, stored.seller) == before
        assert stored.pdf_sha256 == pdf_sha == hashlib.sha256(stored.pdf).hexdigest()
        assert issued_later.amount_ht == Decimal("9999.00")
        assert issued_later.seller["name"] == "SAMPLE Other SARL"
        assert issued_later.buyer["name"] == "Atlas Renamed"
    finally:
        await owner_conn.execute(
            text("UPDATE plan SET price_ht = 2400.00 WHERE code = 'essentiel'")
        )
    # The database refuses any change but the payment's, and any delete, the owner's included.
    with pytest.raises(DBAPIError, match="invoice.immutable"):
        await db.execute(
            update(Invoice).where(Invoice.id == invoice_id).values(amount_ht=Decimal("1.00"))
        )
    await db.rollback()
    with pytest.raises(DBAPIError, match="permission denied"):
        await db.execute(delete(Invoice).where(Invoice.id == invoice_id))
    await db.rollback()
    with pytest.raises(DBAPIError, match="invoice.immutable"):
        await owner_conn.execute(text("UPDATE invoice SET seller = '{}'::jsonb"))
    with pytest.raises(DBAPIError, match="invoice.immutable"):
        await owner_conn.execute(text("DELETE FROM invoice"))
    await db.execute(
        update(Invoice).where(Invoice.id == invoice_id).values(state=InvoiceState.overdue)
    )
    await db.commit()
