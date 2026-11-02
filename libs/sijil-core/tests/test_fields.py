import hashlib
from datetime import date
from decimal import Decimal

import pytest

from sijil_core.audit import canonical_json
from sijil_core.fields import (
    DOCUMENT_TYPES,
    FIELD_NAMES,
    REQUIRED_BY_TYPE,
    fields_digest,
    normalise_ice,
    parse_amount,
    parse_currency,
    parse_date,
    parse_field,
    parse_vat_rate,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("١٢٠٫٠٠", Decimal("120.00")),
        ("1 200,00", Decimal("1200.00")),
        ("1.200,00", Decimal("1200.00")),
        ("1200.00", Decimal("1200.00")),
        ("1,200.00", Decimal("1200.00")),
        ("1 200,00 MAD", Decimal("1200.00")),
        ("٦١٥٫٨٠ درهم", Decimal("615.80")),
        ("\u202b\u202a ٦١٥٫٨٠\u202c" + "درهم\u202c", Decimal("615.80")),
        ("3.275,50", Decimal("3275.50")),
        ("1.200", Decimal("1200.00")),
        ("12,5", Decimal("12.50")),
        ("1\u00a0200,00 DH", Decimal("1200.00")),
        ("abc", None),
        ("", None),
        (None, None),
        ("-5,00", None),
    ],
)
def test_parse_amount_variants(raw: str | None, expected: Decimal | None) -> None:
    assert parse_amount(raw) == expected


def test_parse_amount_arabic_marks_are_unambiguous() -> None:
    assert parse_amount("١٫٢٠٠") is None
    assert parse_amount("١٬٢٠٠٫٥٠") == Decimal("1200.50")
    assert parse_amount("١٬٢٠٠") == Decimal("1200.00")


def test_parse_amount_never_float() -> None:
    r = parse_amount("12,5")
    assert type(r) is Decimal


def test_parse_date_forms() -> None:
    d = date(2026, 9, 30)
    assert parse_date("2026-09-30") == d
    assert parse_date("30/09/2026") == d
    assert parse_date("30-09-2026") == d
    assert parse_date("30.09.2026") == d
    assert parse_date("30 septembre 2026") == d
    assert parse_date("30 aout 2026") == date(2026, 8, 30)
    assert parse_date("1 août 2026") == date(2026, 8, 1)
    assert parse_date("03/04/2026") == date(2026, 4, 3)
    assert parse_date("1er octobre 2026") == date(2026, 10, 1)
    assert parse_date("31/02/2026") is None
    assert parse_date("12/31/2026") is None
    assert parse_date(None) is None
    assert parse_date("n'importe quoi") is None


def test_parse_date_arabic_month_and_digits() -> None:
    assert parse_date("٥ أكتوبر ٢٠٢٦") == date(2026, 10, 5)
    assert parse_date("31 غشت 2026") == date(2026, 8, 31)
    assert parse_date("٣٠/٠٩/٢٠٢٦") == date(2026, 9, 30)
    assert parse_date("5 تشرين الأول 2026") == date(2026, 10, 5)
    assert parse_date("5 كانون الثاني 2026") == date(2026, 1, 5)
    assert parse_date("5 أيلول 2026") == date(2026, 9, 5)


def test_ice_must_be_15_digits() -> None:
    assert normalise_ice("001905837000041") == "001905837000041"
    assert normalise_ice("001 905 837 000 041") == "001905837000041"
    assert normalise_ice("٠٠١٩٠٥٨٣٧٠٠٠٠٤١") == "001905837000041"
    assert normalise_ice("00190583700004") is None
    assert normalise_ice("0019058370000411") is None
    assert normalise_ice("00190583700004A") is None
    assert normalise_ice(None) is None


def test_currency_and_vat_rate() -> None:
    for raw in ("DH", "dhs", "درهم", "د.م.", "mad"):
        assert parse_currency(raw) == "MAD"
    assert parse_currency("€") == "EUR"
    assert parse_currency("eur") == "EUR"
    assert parse_currency("$") == "USD"
    assert parse_currency("GBP") is None
    assert parse_currency(None) is None
    assert parse_vat_rate("20 %") == Decimal("20")
    assert parse_vat_rate("5,5%") == Decimal("5.5")
    assert parse_vat_rate("abc") is None
    assert parse_vat_rate("101") is None


def test_constants() -> None:
    assert "invoice" in DOCUMENT_TYPES
    assert len(FIELD_NAMES) == 10
    assert REQUIRED_BY_TYPE["receipt"] == REQUIRED_BY_TYPE["invoice"] - {"supplier_ice"}
    assert REQUIRED_BY_TYPE["payslip"] == frozenset({"document_type", "date"})


def test_parse_field_canonical_strings() -> None:
    assert parse_field("total_ttc", "1 200,5 MAD") == "1200.50"
    assert parse_field("total_ht", "١٢٠٫٠٠") == "120.00"
    assert parse_field("date", "30 septembre 2026") == "2026-09-30"
    assert parse_field("currency", "DH") == "MAD"
    assert parse_field("vat_rate", "20,00 %") == "20"
    assert parse_field("vat_rate", "7%") == "7"
    assert parse_field("vat_rate", "5,5") == "5.5"
    assert parse_field("supplier_ice", "001 905 837 000 041") == "001905837000041"
    assert parse_field("document_type", " Invoice ") == "invoice"
    assert parse_field("document_type", "‫invoice‬ ") == "invoice"
    assert parse_field("document_type", "facture") is None
    assert parse_field("supplier_name", "  ACME\u00a0 SARL ") == "ACME SARL"
    assert parse_field("number", "   ") is None
    assert parse_field("number", None) is None
    with pytest.raises(KeyError):
        parse_field("nope", "x")


def test_fields_digest_is_canonical_json_of_every_field_and_the_type() -> None:
    values = {"number": "F-1", "total_ttc": "1500.00", "unknown": "ignored"}
    expected = hashlib.sha256(
        canonical_json(
            {
                "fields": {name: values.get(name) for name in FIELD_NAMES},
                "document_type": "invoice",
            }
        )
    ).hexdigest()
    assert fields_digest(values, document_type="invoice") == expected
    # A missing name is a null: absent and explicit null hash the same.
    assert fields_digest({**values, "vat": None}, document_type="invoice") == expected
    assert fields_digest(values, document_type="receipt") != expected
    assert fields_digest({**values, "number": "F-2"}, document_type="invoice") != expected
