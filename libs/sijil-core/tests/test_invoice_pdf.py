"""The invoice PDF: A4, one page, fr or ar, the mandatory mentions, deterministic bytes."""

import unicodedata
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import arabic_reshaper
import pypdfium2 as pdfium
import pytest
from bidi.algorithm import get_display

from sijil_core import invoice_pdf, invoicing
from sijil_core.db.models import Invoice, InvoiceKind, InvoiceState, Locale
from tests.support.seller import sample_seller

NOW = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)
BUYER_ICE = "002999888000011"  # SAMPLE: a fictitious company's ICE


def _invoice(locale: Locale, buyer_name: str = "Atlas SARL") -> Invoice:
    amounts = invoicing.amounts(Decimal("2400.00"), None)
    return Invoice(
        id=uuid.uuid4(),
        org_id=uuid.uuid4(),
        number="SIJ-2026-000042",
        year=2026,
        kind=InvoiceKind.annual,
        plan_code="essentiel",
        period_start=NOW + timedelta(days=30),
        period_end=NOW + timedelta(days=395),
        amount_ht=amounts.ht,
        vat_rate=invoicing.VAT_RATE,
        amount_vat=amounts.vat,
        amount_ttc=amounts.ttc,
        currency="MAD",
        issued_at=NOW,
        due_at=NOW + timedelta(days=30),
        state=InvoiceState.issued,
        buyer={"name": buyer_name, "ice": BUYER_ICE},
        seller=sample_seller().seller(),
        locale=locale,
    )


def _text(pdf: bytes) -> tuple[int, str, tuple[float, float]]:
    """The page count, the text as drawn and the page size. The text is read glyph by glyph
    from its position, line by line and left to right (pdfium's own extraction reorders mixed
    Arabic and Latin lines), folded with NFKC and without whitespace (pdfium synthesises some
    spaces off the line)."""
    doc = pdfium.PdfDocument(pdf)
    try:
        page = doc[0]
        textpage = page.get_textpage()
        glyphs = []
        for i in range(textpage.count_chars()):
            ch = textpage.get_text_range(i, 1)
            if ch.isspace():
                continue
            left, bottom, _, top = textpage.get_charbox(i, loose=True)
            glyphs.append(((bottom + top) / 2, left, ch))
        lines: list[tuple[float, list[tuple[float, str]]]] = []
        for y, x, ch in sorted(glyphs, key=lambda g: -g[0]):
            if lines and abs(lines[-1][0] - y) < 5:
                lines[-1][1].append((x, ch))
            else:
                lines.append((y, [(x, ch)]))
        text = "\n".join("".join(c for _, c in sorted(line)) for _, line in lines)
        return len(doc), unicodedata.normalize("NFKC", text), page.get_size()
    finally:
        doc.close()


def _visual(text: str) -> str:
    """Arabic as drawn: shaped, in visual order, folded back to plain letters, no spaces."""
    drawn = unicodedata.normalize("NFKC", get_display(arabic_reshaper.reshape(text)))
    return "".join(drawn.split())


def _bare(text: str) -> str:
    return "".join(text.split())


@pytest.mark.parametrize("locale", [Locale.fr, Locale.ar])
def test_pdf_fr_and_ar_carry_mandatory_mentions(locale: Locale) -> None:
    invoice = _invoice(locale)
    messages = invoicing.messages(locale)
    pages, text, size = _text(invoice_pdf.render(invoice, messages, region="ma-cas"))
    assert pages == 1
    assert (round(size[0]), round(size[1])) == (595, 842)  # A4
    flat = text
    seller = sample_seller()
    for mention in (
        BUYER_ICE,
        seller.seller_ice,
        seller.seller_if,
        seller.seller_rc,
        seller.seller_tp,
        seller.seller_rib,
        "SIJ-2026-000042",  # the number, and the transfer reference to quote
        "16/11/2026",  # issued
        "16/12/2026",  # due
        "Atlas SARL",
        "SAMPLE Sijil SARL",
    ):
        assert _bare(mention) in flat, mention
    assert flat.count("SIJ-2026-000042") >= 2
    words = invoicing.amount_words(Decimal("2880.00"), messages)
    if locale is Locale.fr:
        for mention in ("2 400,00", "480,00", "2 880,00", "TVA 20 %", "Total HT", "Total TTC"):
            assert _bare(mention) in flat, mention
        assert _bare("deux mille huit cent quatre-vingts dirhams") in flat
    else:
        for mention in ("2.400,00", "480,00", "2.880,00"):
            assert mention in flat, mention
        assert words == "ألفان وثمانمائة وثمانون درهما"
        assert _visual(words) in flat
        assert _visual(messages["amount_ttc"]) in flat
        assert _visual(messages["ice"]) in flat


def test_pdf_draws_an_arabic_buyer_name_in_a_latin_invoice() -> None:
    invoice = _invoice(Locale.fr, buyer_name="شركة النور")
    _, text, _ = _text(invoice_pdf.render(invoice, invoicing.messages(Locale.fr), region="ma-cas"))
    assert _visual("شركة النور") in text


def test_pdf_bytes_deterministic() -> None:
    for locale in (Locale.fr, Locale.ar):
        invoice = _invoice(locale)
        messages = invoicing.messages(locale)
        first = invoice_pdf.render(invoice, messages, region="ma-cas")
        assert invoice_pdf.render(invoice, messages, region="ma-cas") == first
        # No timestamp: another clock gives the same bytes.
        assert b"CreationDate" not in first or b"D:20000101000000" in first
