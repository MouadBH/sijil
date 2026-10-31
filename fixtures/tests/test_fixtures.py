"""Sample data must be internally consistent: it is what extraction is scored against."""

import json
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
SUPPLIERS = json.loads((ROOT / "suppliers.json").read_text())
INVOICES = json.loads((ROOT / "invoices" / "manifest.json").read_text())
BY_ICE = {s["ice"]: s for s in SUPPLIERS}
MOROCCAN_VAT_RATES = {20, 14, 10, 7}


def test_eight_suppliers_half_with_arabic_names() -> None:
    assert len(SUPPLIERS) == 8
    assert all(s["name"] and s["name_ar"] for s in SUPPLIERS)


@pytest.mark.parametrize("supplier", SUPPLIERS, ids=lambda s: s["name"])
def test_supplier_ice_is_15_digits_and_rate_is_moroccan(supplier: dict[str, object]) -> None:
    ice = str(supplier["ice"])
    assert len(ice) == 15 and ice.isdigit()
    assert supplier["vat_rate"] in MOROCCAN_VAT_RATES


def test_twelve_invoices_six_per_language() -> None:
    assert len(INVOICES) == 12
    assert sum(i["lang"] == "fr" for i in INVOICES) == 6
    assert sum(i["lang"] == "ar" for i in INVOICES) == 6


@pytest.mark.parametrize("inv", INVOICES, ids=lambda i: i["file"])
def test_invoice_totals_add_up(inv: dict[str, str]) -> None:
    ht, tva, ttc = (Decimal(inv[k]) for k in ("ht", "tva", "ttc"))
    rate = Decimal(BY_ICE[inv["supplier_ice"]]["vat_rate"])
    assert inv["currency"] == "MAD"
    assert ht + tva == ttc
    assert tva == (ht * rate / 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def test_exactly_two_invoices_print_a_wrong_total_one_per_language() -> None:
    broken = [i for i in INVOICES if "printed_ttc" in i]
    assert sorted(i["lang"] for i in broken) == ["ar", "fr"]
    assert all(Decimal(i["printed_ttc"]) != Decimal(i["ttc"]) for i in broken)


def test_one_arabic_invoice_uses_arabic_indic_digits() -> None:
    assert [i["digits"] for i in INVOICES if i.get("digits") == "arab"] == ["arab"]


@pytest.mark.parametrize("inv", INVOICES, ids=lambda i: i["file"])
def test_pdf_exists_and_is_marked_sample(inv: dict[str, str]) -> None:
    pdf = ROOT / "invoices" / inv["file"]
    assert pdf.exists()
    text = "".join(page.extract_text() or "" for page in PdfReader(pdf).pages)
    assert "SAMPLE" in text
