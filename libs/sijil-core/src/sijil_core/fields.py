"""Parsers that turn model-copied field values into canonical strings. Pure functions."""

import hashlib
import re
import unicodedata
from collections.abc import Callable, Mapping
from datetime import date
from decimal import Decimal, InvalidOperation

from sijil_core.audit import canonical_json
from sijil_core.text import normalise

DOCUMENT_TYPES: tuple[str, ...] = (
    "invoice",
    "receipt",
    "contract",
    "bank_statement",
    "payslip",
    "tax_notice",
    "other",
)

FIELD_NAMES: tuple[str, ...] = (
    "document_type",
    "supplier_name",
    "supplier_ice",
    "date",
    "number",
    "total_ht",
    "vat",
    "vat_rate",
    "total_ttc",
    "currency",
)

_INVOICE_REQUIRED = frozenset(
    {
        "document_type",
        "supplier_name",
        "supplier_ice",
        "date",
        "number",
        "total_ht",
        "vat",
        "total_ttc",
        "currency",
    }
)
_OTHER_REQUIRED = frozenset({"document_type", "date"})

REQUIRED_BY_TYPE: dict[str, frozenset[str]] = {
    "invoice": _INVOICE_REQUIRED,
    "receipt": _INVOICE_REQUIRED - {"supplier_ice"},
    **{t: _OTHER_REQUIRED for t in DOCUMENT_TYPES if t not in ("invoice", "receipt")},
}

_DIGITS = {
    **{chr(0x0660 + i): str(i) for i in range(10)},
    **{chr(0x06F0 + i): str(i) for i in range(10)},
}
_DIGIT_TABLE = str.maketrans(_DIGITS)
_ARABIC_DECIMAL = "٫"
_ARABIC_GROUPING = "٬"
_ARABIC_MARKS = str.maketrans({_ARABIC_DECIMAL: ".", _ARABIC_GROUPING: ","})


def _ascii_digits(s: str) -> str:
    return s.translate(_DIGIT_TABLE)


# --- currency -----------------------------------------------------------------------------

_CURRENCY_MAP = {
    "dh": "MAD",
    "dhs": "MAD",
    "درهم": "MAD",
    "د.م.": "MAD",
    "mad": "MAD",
    "€": "EUR",
    "eur": "EUR",
    "$": "USD",
    "usd": "USD",
}
# Longest first so DHS is stripped before DH.
_CURRENCY_STRIP = re.compile(
    "|".join(re.escape(t) for t in sorted(_CURRENCY_MAP, key=len, reverse=True)),
    re.IGNORECASE,
)


def parse_currency(s: str | None) -> str | None:
    if s is None:
        return None
    return _CURRENCY_MAP.get(normalise(s).strip().lower())


# --- amounts ------------------------------------------------------------------------------

_AMOUNT_OK = re.compile(r"^\d+(\.\d{1,2})?$")
_CENT = Decimal("0.01")


def _is_decimal_mark(s: str, mark: str) -> bool:
    return 1 <= len(s) - s.rindex(mark) - 1 <= 2


def parse_amount(s: str | None) -> Decimal | None:
    if s is None:
        return None
    t = _ascii_digits(normalise(s))
    t = _CURRENCY_STRIP.sub("", t)
    t = re.sub(r"\s+", "", t)
    arabic = _ARABIC_DECIMAL in t
    if arabic:
        # The Arabic decimal mark is unambiguous: 1-2 digits after it, U+066C is grouping.
        head, _, tail = t.partition(_ARABIC_DECIMAL)
        if not re.fullmatch(r"\d{1,2}", tail):
            return None
        t = head.replace(_ARABIC_GROUPING, "") + "." + tail
    else:
        t = t.replace(_ARABIC_GROUPING, "")
    has_dot, has_comma = "." in t, "," in t
    if arabic:
        pass  # heuristic applies only when no Arabic decimal mark was present
    elif has_dot and has_comma:
        dec = "." if t.rindex(".") > t.rindex(",") else ","
        grp = "," if dec == "." else "."
        head, _, tail = t.rpartition(dec)
        t = head.replace(grp, "") + "." + tail
    elif has_dot or has_comma:
        mark = "." if has_dot else ","
        if _is_decimal_mark(t, mark):
            head, _, tail = t.rpartition(mark)
            t = head + "." + tail
        else:
            t = t.replace(mark, "")
    if not _AMOUNT_OK.match(t):
        return None
    return Decimal(t).quantize(_CENT)


# --- VAT rate -----------------------------------------------------------------------------

_RATE_OK = re.compile(r"^\d+(\.\d+)?$")


def parse_vat_rate(s: str | None) -> Decimal | None:
    if s is None:
        return None
    t = _ascii_digits(normalise(s)).translate(_ARABIC_MARKS)
    t = re.sub(r"[\s%]+", "", t).replace(",", ".")
    if not _RATE_OK.match(t):
        return None
    try:
        rate = Decimal(t)
    except InvalidOperation:  # pragma: no cover - guarded by the regex
        return None
    return rate if rate <= 100 else None


# --- dates --------------------------------------------------------------------------------

_MONTHS: dict[str, int] = {
    # French, accents stripped
    "janvier": 1,
    "fevrier": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "aout": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "decembre": 12,
    # Moroccan Arabic
    "يناير": 1,
    "فبراير": 2,
    "مارس": 3,
    "أبريل": 4,
    "ابريل": 4,
    "ماي": 5,
    "مايو": 5,
    "يونيو": 6,
    "يوليوز": 7,
    "يوليو": 7,
    "غشت": 8,
    "أغسطس": 8,
    "شتنبر": 9,
    "سبتمبر": 9,
    "أكتوبر": 10,
    "اكتوبر": 10,
    "نونبر": 11,
    "نوفمبر": 11,
    "دجنبر": 12,
    "ديسمبر": 12,
    # Mashriq Arabic
    "كانون الثاني": 1,
    "شباط": 2,
    "آذار": 3,
    "نيسان": 4,
    "أيار": 5,
    "حزيران": 6,
    "تموز": 7,
    "آب": 8,
    "أيلول": 9,
    "تشرين الأول": 10,
    "تشرين الثاني": 11,
    "كانون الأول": 12,
}

_ISO = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$")
_NUMERIC = re.compile(r"^(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{4})$")
_NAMED = re.compile(r"^(\d{1,2})(?:er)?\s+(\D+?)\s+(\d{4})$")


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if not unicodedata.combining(c))


def _month(name: str) -> int | None:
    name = re.sub(r"\s+", " ", name.strip().lower())
    return _MONTHS.get(name) or _MONTHS.get(_strip_accents(name))


def _make(y: int, m: int, d: int) -> date | None:
    try:
        return date(y, m, d)
    except ValueError:
        return None


def parse_date(s: str | None) -> date | None:
    if s is None:
        return None
    t = _ascii_digits(normalise(s)).strip()
    if m := _ISO.match(t):
        return _make(int(m[1]), int(m[2]), int(m[3]))
    if m := _NUMERIC.match(t):
        return _make(int(m[3]), int(m[2]), int(m[1]))
    if m := _NAMED.match(t):
        month = _month(m[2])
        return _make(int(m[3]), month, int(m[1])) if month else None
    return None


# --- ICE ----------------------------------------------------------------------------------

_ICE_OK = re.compile(r"^[0-9]{15}$")


def normalise_ice(s: str | None) -> str | None:
    if s is None:
        return None
    t = re.sub(r"\s+", "", _ascii_digits(normalise(s)))
    return t if _ICE_OK.match(t) else None


# --- canonical field strings --------------------------------------------------------------


def _text(raw: str | None) -> str | None:
    if raw is None:
        return None
    return normalise(raw).strip() or None


def _amount_field(raw: str | None) -> str | None:
    v = parse_amount(raw)
    return None if v is None else f"{v:.2f}"


def _date_field(raw: str | None) -> str | None:
    d = parse_date(raw)
    return None if d is None else d.isoformat()


def _rate_field(raw: str | None) -> str | None:
    v = parse_vat_rate(raw)
    return None if v is None else format(v.normalize(), "f")


def _type_field(raw: str | None) -> str | None:
    if raw is None:
        return None
    t = normalise(raw).lower().strip()
    return t if t in DOCUMENT_TYPES else None


_PARSERS: dict[str, Callable[[str | None], str | None]] = {
    "document_type": _type_field,
    "supplier_name": _text,
    "supplier_ice": normalise_ice,
    "date": _date_field,
    "number": _text,
    "total_ht": _amount_field,
    "vat": _amount_field,
    "vat_rate": _rate_field,
    "total_ttc": _amount_field,
    "currency": parse_currency,
}


def parse_field(name: str, raw: str | None) -> str | None:
    """Canonical string for storage, or None. Raises KeyError for an unknown field name."""
    return _PARSERS[name](raw)


# --- the sealed hash ----------------------------------------------------------------------


def fields_digest(values: Mapping[str, str | None], *, document_type: str | None) -> str:
    """`fields_sha256`: SHA-256 of the canonical JSON of every field (a missing one is null) and
    the document type. Sealing stores it on the ledger; exports recompute it to prove the
    values are the sealed ones. Names outside FIELD_NAMES are ignored."""
    payload = {
        "fields": {name: values.get(name) for name in FIELD_NAMES},
        "document_type": document_type,
    }
    return hashlib.sha256(canonical_json(payload)).hexdigest()
