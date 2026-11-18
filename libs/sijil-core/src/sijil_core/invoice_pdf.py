"""The invoice PDF (ADM-4): A4, one page, in French or Arabic (right to left).

Rendered once, at issue (`sijil_core.invoicing.issue`), from the invoice row alone (its buyer
and seller snapshots, amounts and dates), with the `invoice.*` labels of its locale. It carries
both parties with their ICE (and the seller's IF, RC and TP), the number, the issue and due
dates, the plan and the period, HT, VAT 20% and TTC, the total in words, the bank, the RIB and
the reference to quote on the transfer (the number).

- Arabic text is reshaped (`arabic_reshaper`) and put in visual order (`python-bidi`), as in
  `fixtures/generate.py`. Each line is then drawn in runs: Arabic letters in Noto Naskh Arabic,
  everything else (Latin, digits, punctuation) in Noto Sans, both embedded (OFL, in
  `sijil_core/fonts`). A Latin value (an ICE, a RIB, a number) is drawn on its own, so the
  bidi algorithm never reorders its groups.
- Amounts are printed from their `Decimal` value as text; no float is ever involved.
- The bytes are deterministic: reportlab's `invariant` mode writes no timestamp or random id,
  so the stored SHA-256 can be checked against a new rendering.
"""

import io
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final

import arabic_reshaper
from bidi.algorithm import get_display
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen.canvas import Canvas

from sijil_core.db.models import Invoice, Locale
from sijil_core.invoicing import Messages, amount_words
from sijil_core.retention import region_local_date

FONTS: Final = Path(__file__).parent / "fonts"
LATIN: Final = "SijilNotoSans"
ARABIC: Final = "SijilNotoNaskhArabic"
pdfmetrics.registerFont(TTFont(LATIN, str(FONTS / "NotoSans-Regular.ttf")))
pdfmetrics.registerFont(TTFont(ARABIC, str(FONTS / "NotoNaskhArabic-Regular.ttf")))

WIDTH, HEIGHT = A4
MARGIN: Final = 50.0
GAP: Final = 6.0
GREY: Final = 0.4
_ARABIC_RANGES: Final = ((0x0600, 0x06FF), (0x0750, 0x077F), (0xFB50, 0xFDFF), (0xFE70, 0xFEFF))


def _is_arabic(ch: str) -> bool:
    code = ord(ch)
    return any(lo <= code <= hi for lo, hi in _ARABIC_RANGES)


def _has_arabic(text: str) -> bool:
    return any(_is_arabic(ch) for ch in text)


def _runs(visual: str) -> list[tuple[str, str]]:
    """`visual` cut into (font, text) runs: Arabic letters in Naskh, the rest in Noto Sans."""
    runs: list[tuple[str, str]] = []
    for ch in visual:
        font = ARABIC if _is_arabic(ch) else LATIN
        if runs and runs[-1][0] == font:
            runs[-1] = (font, runs[-1][1] + ch)
        else:
            runs.append((font, ch))
    return runs


def _visual(text: str, rtl: bool) -> str:
    """The text as drawn left to right: Arabic shaped and bidi-ordered (base direction right to
    left on an Arabic invoice); Latin-only text as is."""
    if not _has_arabic(text):
        return text
    return str(get_display(arabic_reshaper.reshape(text), base_dir="R" if rtl else "L"))


def money(amount: Decimal, locale: Locale) -> str:
    """2 880,00 (fr) or 2.880,00 (ar: a space would let the bidi algorithm split the figure)."""
    whole, cents = f"{amount:.2f}".split(".")
    grouped = f"{int(whole):,}".replace(",", "." if locale is Locale.ar else " ")
    return f"{grouped},{cents}"


def _date(at: datetime, region: str) -> str:
    return region_local_date(at, region).strftime("%d/%m/%Y")


class _Page:
    def __init__(self, canvas: Canvas, rtl: bool) -> None:
        self.c = canvas
        self.rtl = rtl

    def width(self, text: str, size: float) -> float:
        visual = _visual(text, self.rtl)
        return float(sum(pdfmetrics.stringWidth(t, f, size) for f, t in _runs(visual)))

    def draw(self, text: str, x: float, y: float, size: float, *, right: bool = False) -> float:
        """Draws `text` with its left edge (or its right edge, `right`) at x; returns its
        width."""
        visual = _visual(text, self.rtl)
        runs = _runs(visual)
        total = float(sum(pdfmetrics.stringWidth(t, f, size) for f, t in runs))
        cursor = x - total if right else x
        for font, part in runs:
            self.c.setFont(font, size)
            self.c.drawString(cursor, y, part)
            cursor += pdfmetrics.stringWidth(part, font, size)
        return total

    def start(self, text: str, y: float, size: float, *, indent: float = 0.0) -> float:
        """At the start edge of the page (right on an Arabic invoice); returns the width."""
        if self.rtl:
            return self.draw(text, WIDTH - MARGIN - indent, y, size, right=True)
        return self.draw(text, MARGIN + indent, y, size)

    def end(self, text: str, y: float, size: float) -> None:
        """At the end edge of the page (left on an Arabic invoice)."""
        if self.rtl:
            self.draw(text, MARGIN, y, size)
        else:
            self.draw(text, WIDTH - MARGIN, y, size, right=True)

    def field(self, label: str, value: str, y: float, size: float, *, indent: float = 0.0) -> None:
        """`label : value` from the start edge; the value drawn apart, in its own direction."""
        sep = ":" if self.rtl else " :"
        drawn = self.start(f"{label}{sep}", y, size, indent=indent) + GAP
        if self.rtl:
            self.draw(value, WIDTH - MARGIN - indent - drawn, y, size, right=True)
        else:
            self.draw(value, MARGIN + indent + drawn, y, size)

    def wrapped(self, text: str, y: float, size: float, leading: float) -> float:
        """Text broken on spaces to the page width, from the start edge; returns the next y."""
        line = ""
        for word in text.split(" "):
            candidate = f"{line} {word}" if line else word
            if line and self.width(candidate, size) > WIDTH - 2 * MARGIN:
                self.start(line, y, size)
                y -= leading
                line = word
            else:
                line = candidate
        if line:
            self.start(line, y, size)
            y -= leading
        return y

    def rule(self, y: float) -> None:
        self.c.setStrokeGray(0.75)
        self.c.line(MARGIN, y, WIDTH - MARGIN, y)


def _fill(template: str, **values: str) -> str:
    for key, value in values.items():
        template = template.replace("{" + key + "}", value)
    return template


def render(invoice: Invoice, messages: Messages, *, region: str) -> bytes:
    """The invoice's one-page A4 PDF in the locale of `messages` (`invoicing.messages`), its
    dates in the calendar of `region`."""
    locale = messages.locale
    rtl = locale is Locale.ar
    labels = messages
    buf = io.BytesIO()
    canvas = Canvas(buf, pagesize=A4, invariant=True, pageCompression=1)
    canvas.setTitle(f"{labels['title']} {invoice.number}")
    canvas.setCreator("Sijil")
    page = _Page(canvas, rtl)
    seller, buyer = invoice.seller, invoice.buyer
    currency = str(labels["currency"])

    y = HEIGHT - MARGIN - 20
    page.start(str(labels["title"]), y, 22)
    page.end(f"{labels['number']} {invoice.number}" if not rtl else invoice.number, y, 13)
    if rtl:
        number_width = page.width(invoice.number, 13)
        page.draw(str(labels["number"]), MARGIN + number_width + GAP, y, 13)
    y -= 40

    canvas.setFillGray(GREY)
    page.start(str(labels["seller"]), y, 9)
    canvas.setFillGray(0)
    y -= 16
    page.start(str(seller["name"]), y, 12)
    y -= 15
    page.start(str(seller["address"]), y, 10)
    y -= 14
    for key in ("ice", "if", "rc", "tp"):
        page.field(str(labels[key]), str(seller[key]), y, 10)
        y -= 14
    y -= 10

    canvas.setFillGray(GREY)
    page.start(str(labels["buyer"]), y, 9)
    canvas.setFillGray(0)
    y -= 16
    page.start(str(buyer["name"]), y, 12)
    y -= 15
    if buyer.get("address"):
        page.start(str(buyer["address"]), y, 10)
        y -= 14
    page.field(str(labels["ice"]), str(buyer["ice"]), y, 10)
    y -= 24

    page.field(str(labels["issued_on"]), _date(invoice.issued_at, region), y, 10)
    y -= 14
    page.field(str(labels["due_on"]), _date(invoice.due_at, region), y, 10)
    y -= 20
    page.rule(y)
    y -= 22

    plan = str(labels["plan"].get(invoice.plan_code, invoice.plan_code))
    page.field(str(labels["description"]), _fill(labels["kind"][invoice.kind.value], plan=plan),
               y, 10)  # fmt: skip
    y -= 14
    last_day = invoice.period_end - timedelta(microseconds=1)
    period = _fill(
        labels["period_range"],
        start=_date(invoice.period_start, region),
        end=_date(last_day, region),
    )
    page.start(f"{labels['period']}{':' if rtl else ' :'} {period}", y, 10)
    y -= 22
    page.rule(y)
    y -= 22

    rate = f"{invoice.vat_rate.normalize():f}"
    rows = (
        (str(labels["amount_ht"]), invoice.amount_ht, 11),
        (_fill(labels["vat"], rate=rate), invoice.amount_vat, 11),
        (str(labels["amount_ttc"]), invoice.amount_ttc, 13),
    )
    for label, amount, size in rows:
        page.start(label, y, size)
        page.end(f"{money(amount, locale)} {currency}", y, size)
        y -= size + 8
    y -= 6
    words = amount_words(invoice.amount_ttc, labels)
    y = page.wrapped(_fill(labels["in_words"], words=words), y, 10, 14)
    y -= 12
    page.rule(y)
    y -= 22

    page.start(str(labels["payment"]), y, 11)
    y -= 16
    page.field(str(labels["bank"]), str(seller["bank"]), y, 10)
    y -= 14
    page.field(str(labels["rib"]), str(seller["rib"]), y, 10)
    y -= 14
    page.field(str(labels["reference"]), invoice.number, y, 10)

    canvas.showPage()
    canvas.save()
    return buf.getvalue()
