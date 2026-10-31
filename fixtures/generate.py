"""Render fixtures/invoices/manifest.json into SAMPLE invoice PDFs.

Run: uv run python fixtures/generate.py
Every page carries a SAMPLE / ÉCHANTILLON / عينة mark. Companies and ICE numbers are fictitious.
"""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import arabic_reshaper
from bidi.algorithm import get_display
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen.canvas import Canvas

ROOT = Path(__file__).resolve().parent
ARABIC_FONT = "NotoNaskhArabic"
pdfmetrics.registerFont(TTFont(ARABIC_FONT, str(ROOT / "fonts" / "NotoNaskhArabic-Regular.ttf")))

WIDTH, HEIGHT = A4
MARGIN = 56
ARABIC_INDIC = str.maketrans("0123456789.,", "٠١٢٣٤٥٦٧٨٩٫٬")

CLIENT = {
    "fr": "Client : Entreprise Exemple SARL — Casablanca",
    "ar": "الزبون: شركة نموذجية، الدار البيضاء",
}
# Noto Naskh Arabic has no Latin letters, hyphen or "%": Latin values are drawn in Helvetica.
MONTHS_AR = ["يناير", "فبراير", "مارس", "أبريل", "ماي", "يونيو",
             "يوليوز", "غشت", "شتنبر", "أكتوبر", "نونبر", "دجنبر"]  # fmt: skip
LABELS = {
    "fr": {
        "invoice": "FACTURE N°",
        "date": "Date",
        "ice": "ICE",
        "item": "Désignation",
        "ht": "Total HT",
        "tva": "TVA",
        "ttc": "Total TTC",
    },
    "ar": {
        "invoice": "فاتورة رقم",
        "date": "التاريخ",
        "ice": "التعريف الموحد للمقاولة",
        "item": "البيان",
        "ht": "المجموع دون احتساب الرسوم",
        "tva": "الضريبة على القيمة المضافة",
        "ttc": "المجموع مع احتساب الرسوم",
    },
}


def money(amount: str, lang: str, digits: str) -> str:
    whole, cents = f"{Decimal(amount):.2f}".split(".")
    grouped = f"{int(whole):,}"
    if lang == "fr":
        return f"{grouped.replace(',', ' ')},{cents} MAD"
    # A space would let the bidi algorithm split the figure in an RTL line.
    if digits == "arab":
        whole_ar = grouped.replace(",", "٬").translate(ARABIC_INDIC)
        return f"{whole_ar}٫{cents.translate(ARABIC_INDIC)} درهم"
    return f"{grouped.replace(',', '.')},{cents} درهم"


def ar(text: str) -> str:
    return str(get_display(arabic_reshaper.reshape(text)))


def watermark(c: Canvas) -> None:
    c.saveState()
    c.setFillGray(0.85)
    c.setFont("Helvetica-Bold", 54)
    c.translate(WIDTH / 2, HEIGHT / 2)
    c.rotate(35)
    c.drawCentredString(0, 20, "SAMPLE / ÉCHANTILLON")
    c.setFont(ARABIC_FONT, 54)
    c.drawCentredString(0, -50, ar("عينة"))
    c.restoreState()
    c.setFont("Helvetica", 8)
    c.drawString(MARGIN, 24, "SAMPLE DATA — fictitious company and ICE, not a real document.")


def render(inv: dict[str, Any], supplier: dict[str, Any]) -> None:
    lang, digits = inv["lang"], inv.get("digits", "latn")
    labels = LABELS[lang]
    # invariant: byte-identical output on every run, so the PDFs (and their hashes) stay stable.
    c = Canvas(str(ROOT / "invoices" / inv["file"]), pagesize=A4, invariant=True)
    c.setTitle(f"SAMPLE {inv['number']}")
    watermark(c)

    def line(y: float, text: str, size: int = 11, bold: bool = False, latin: str = "") -> None:
        if lang == "ar":
            shaped = ar(text)
            c.setFont(ARABIC_FONT, size)
            c.drawRightString(WIDTH - MARGIN, y, shaped)
            if latin:
                start = WIDTH - MARGIN - pdfmetrics.stringWidth(shaped, ARABIC_FONT, size) - 6
                c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
                c.drawRightString(start, y, latin)
        else:
            c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
            c.drawString(MARGIN, y, text)

    def row(y: float, label: str, value: str, bold: bool = False) -> None:
        if lang == "ar":
            c.setFont(ARABIC_FONT, 12)
            c.drawRightString(WIDTH - MARGIN, y, ar(label))
            c.drawString(MARGIN, y, ar(value))
        else:
            c.setFont("Helvetica-Bold" if bold else "Helvetica", 12)
            c.drawString(MARGIN, y, label)
            c.drawRightString(WIDTH - MARGIN, y, value)

    number, date = inv["number"], inv["date"]
    if lang == "fr":
        line(HEIGHT - 80, supplier["name"], 18, bold=True)
        line(HEIGHT - 100, supplier["city"])
        line(HEIGHT - 118, f"{labels['ice']} : {supplier['ice']}")
        line(HEIGHT - 170, f"{labels['invoice']} {number}", 14, bold=True)
        line(HEIGHT - 190, f"{labels['date']} : {date}")
        line(HEIGHT - 222, CLIENT[lang])
        line(HEIGHT - 280, f"{labels['item']} : {inv['item']}")
        rate = f"{supplier['vat_rate']} %"
    else:
        line(HEIGHT - 80, supplier["name_ar"], 18, bold=True)
        line(HEIGHT - 100, "", latin=supplier["city"])
        if digits == "arab":
            year, month, day = date.split("-")
            words = f"{int(day)} {MONTHS_AR[int(month) - 1]} {year}".translate(ARABIC_INDIC)
            line(HEIGHT - 118, f"{labels['ice']}: {supplier['ice'].translate(ARABIC_INDIC)}")
            line(HEIGHT - 190, f"{labels['date']}: {words}")
            rate = f"{supplier['vat_rate']}٪".translate(ARABIC_INDIC)
        else:
            line(HEIGHT - 118, f"{labels['ice']}:", latin=supplier["ice"])
            line(HEIGHT - 190, f"{labels['date']}:", latin=date)
            rate = f"{supplier['vat_rate']}٪"
        line(HEIGHT - 170, labels["invoice"], 14, bold=True, latin=number)
        line(HEIGHT - 222, CLIENT[lang])
        line(HEIGHT - 280, inv["item"])
    printed_ttc = inv.get("printed_ttc", inv["ttc"])
    row(HEIGHT - 330, labels["ht"], money(inv["ht"], lang, digits))
    row(HEIGHT - 352, f"{labels['tva']} {rate}", money(inv["tva"], lang, digits))
    row(HEIGHT - 380, labels["ttc"], money(printed_ttc, lang, digits), bold=True)
    c.showPage()
    c.save()


def main() -> None:
    suppliers = {s["ice"]: s for s in json.loads((ROOT / "suppliers.json").read_text())}
    for inv in json.loads((ROOT / "invoices" / "manifest.json").read_text()):
        render(inv, suppliers[inv["supplier_ice"]])
        print(f"wrote invoices/{inv['file']}")


if __name__ == "__main__":
    main()
