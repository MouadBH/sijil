"""Field confidence from checkable signals, and the filing/review decision (REC-3, design §5.3).

A score is never the model's own rating: it is the weighted share of a field's applicable
signals that hold. A signal that is None is not applicable and counts in neither the numerator
nor the denominator; a field with no applicable signal scores 0. A hard signal that fails caps
the field at 0.5, below the filing threshold whatever else holds.

Every value here is a canonical string from `sijil_core.fields`. Amounts are compared as
`Decimal`, never as floats.
"""

import re
from collections.abc import Collection, Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Final, Literal

from sijil_core.fields import FIELD_NAMES, REQUIRED_BY_TYPE
from sijil_core.text import normalise, search_normalise, trigram_similarity

Signals = dict[str, bool | None]
Values = Mapping[str, str | None]
Decision = Literal["filed", "needs_review"]

THRESHOLD: Final = 0.90
HARD_CAP: Final = 0.5
# A supplier record for the kept ICE whose name (or alias) neither run gave: the name is the
# record's, but it may be another company's ICE (a client's), so it goes to review.
SUPPLIER_MISMATCH: Final = "supplier_mismatch"
MISMATCH_CAP: Final = 0.85
# A run's name matches a record's name or alias at this trigram similarity, both folded.
NAME_MATCH: Final = 0.6
BLOCKING_FLAGS: Final = frozenset(
    {
        "possible_duplicate",
        "no_text",
        "ingest.unsupported_format",
        "extraction_failed",
        "retention.extend_failed",  # the object lock could not be extended at filing
        SUPPLIER_MISMATCH,
    }
)
VAT_RATES: Final = (Decimal(20), Decimal(14), Decimal(10), Decimal(7))
VAT_RATE_TOLERANCE: Final = Decimal("0.1")
TOTALS_TOLERANCE: Final = Decimal("0.02")
CURRENCIES: Final = frozenset({"MAD", "EUR", "USD"})
PLAUSIBLE_YEARS_BACK: Final = 15

_AMOUNT_WEIGHTS: Final = {"runs_agree": 0.5, "totals_consistent": 0.5}

WEIGHTS: Final[Mapping[str, Mapping[str, float]]] = {
    "document_type": {"runs_agree": 0.6, "paperless_agrees": 0.4},
    "supplier_name": {"runs_agree": 0.5, "in_text": 0.2, "supplier_known": 0.3},
    "supplier_ice": {"runs_agree": 0.4, "ice_15_digits": 0.3, "supplier_known": 0.3},
    "date": {"runs_agree": 0.6, "date_plausible": 0.4},
    "number": {"runs_agree": 0.6, "in_text": 0.4},
    "total_ht": _AMOUNT_WEIGHTS,
    "vat": {**_AMOUNT_WEIGHTS, "vat_rate_valid": 0.5},
    "vat_rate": {"runs_agree": 0.5, "vat_rate_valid": 0.5},
    "total_ttc": _AMOUNT_WEIGHTS,
    "currency": {"runs_agree": 0.6, "currency_valid": 0.4},
}

_AMOUNT_HARD: Final = frozenset({"present", "totals_consistent"})

HARD: Final[Mapping[str, frozenset[str]]] = {
    "document_type": frozenset(),
    "supplier_name": frozenset({"present"}),
    "supplier_ice": frozenset({"present", "ice_15_digits"}),
    "date": frozenset({"present", "date_plausible"}),
    "number": frozenset({"present"}),
    "total_ht": _AMOUNT_HARD,
    "vat": _AMOUNT_HARD | {"vat_rate_valid"},
    "vat_rate": frozenset(),
    "total_ttc": _AMOUNT_HARD,
    "currency": frozenset({"present", "currency_valid"}),
}

_ICE: Final = re.compile(r"^[0-9]{15}$")


def kept(values_a: Values | None, values_b: Values | None) -> dict[str, str | None]:
    """The value kept per field: run A's, or run B's where run A has none."""
    a, b = values_a or {}, values_b or {}
    return {name: a.get(name) or b.get(name) for name in FIELD_NAMES}


def _fold(s: str) -> str:
    return normalise(s).strip().casefold()


def _amount(value: str | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _years_back(today: date, years: int) -> date:
    try:
        return today.replace(year=today.year - years)
    except ValueError:  # 29 February
        return today.replace(year=today.year - years, day=28)


def plausible_date(value: str | None, today: date) -> date | None:
    """The ISO date `value` when it lies between 15 years before `today` and the day after it
    (both inclusive), else None. `today` is the region-local date."""
    if value is None:
        return None
    try:
        day = date.fromisoformat(value)
    except ValueError:
        return None
    if _years_back(today, PLAUSIBLE_YEARS_BACK) <= day <= date.fromordinal(today.toordinal() + 1):
        return day
    return None


def _matches_a_rate(percent: Decimal) -> bool:
    return any(abs(percent - rate) <= VAT_RATE_TOLERANCE for rate in VAT_RATES)


def _vat_valid(ht: Decimal | None, vat: Decimal | None) -> bool | None:
    """`vat / ht * 100` within 0.1 of a Moroccan rate. A zero VAT is invalid (no exemptions
    in M3). None when either amount is missing."""
    if ht is None or vat is None:
        return None
    if ht <= 0 or vat <= 0:
        return False
    return _matches_a_rate(vat / ht * 100)


def _rate_valid(rate: str | None, ht: Decimal | None, vat: Decimal | None) -> bool | None:
    """The printed rate is a Moroccan one and, when both amounts are known, agrees with them."""
    printed = _amount(rate)
    if printed is None:
        return None
    if not any(printed == r for r in VAT_RATES):
        return False
    if ht is None or vat is None or ht <= 0:
        return True
    return abs(vat / ht * 100 - printed) <= VAT_RATE_TOLERANCE


def compute_signals(
    values_a: Values | None,
    values_b: Values | None,
    content: str,
    *,
    paperless_type: str | None,
    known_supplier: str | None,
    today: date,
    supplier_record: Sequence[str] | None = None,
) -> dict[str, dict[str, bool | None]]:
    """Every field's signals, from the two runs' canonical values (None for a run that gave
    nothing usable), the normalised content, Paperless's suggested type, the supplier name
    already sealed for the kept ICE, and the region-local date.

    `supplier_record` is the workspace's supplier record for the kept ICE (`sijil_core.
    suppliers`, REC-5): its name, then its aliases. The name replaces the kept one. It is
    known only when a run's own name matches the name or an alias (`names_match`), and the
    runs agree on it only when both gave that ICE and a matching name; else `supplier_known`
    is false, which caps the score at MISMATCH_CAP (the caller flags SUPPLIER_MISMATCH)."""
    a, b = values_a or {}, values_b or {}
    value = kept(values_a, values_b)
    record_known: bool | None = None
    record_agree = False
    if supplier_record:
        value["supplier_name"] = supplier_record[0]
        known_supplier = None
        matched = [names_match(r.get("supplier_name"), supplier_record) for r in (a, b)]
        record_known = any(matched)
        ice_a, ice_b = a.get("supplier_ice"), b.get("supplier_ice")
        record_agree = all(matched) and ice_a is not None and ice_a == ice_b
    folded = _fold(content)
    signals: dict[str, Signals] = {}
    for name in FIELD_NAMES:
        va, vb = a.get(name), b.get(name)
        signals[name] = {
            "runs_agree": va is not None and va == vb,
            "present": value[name] is not None,
        }

    def in_text(name: str) -> bool | None:
        v = value[name]
        return None if v is None else _fold(v) in folded

    supplier = value["supplier_name"]
    known: bool | None = record_known
    if known_supplier is not None:
        known = supplier is not None and _fold(supplier) == _fold(known_supplier)
    if supplier_record:
        signals["supplier_name"]["runs_agree"] = record_agree
    ht, vat, ttc = (_amount(value[n]) for n in ("total_ht", "vat", "total_ttc"))
    totals: bool | None = None
    if ht is not None and vat is not None and ttc is not None:
        totals = abs(ht + vat - ttc) <= TOTALS_TOLERANCE
    ice = value["supplier_ice"]

    signals["document_type"]["paperless_agrees"] = (
        None if paperless_type is None else paperless_type == value["document_type"]
    )
    signals["supplier_name"].update(in_text=in_text("supplier_name"), supplier_known=known)
    signals["supplier_ice"].update(
        ice_15_digits=ice is not None and _ICE.match(ice) is not None, supplier_known=known
    )
    signals["date"]["date_plausible"] = plausible_date(value["date"], today) is not None
    signals["number"]["in_text"] = in_text("number")
    for name in ("total_ht", "vat", "total_ttc"):
        signals[name]["totals_consistent"] = totals
    signals["vat"]["vat_rate_valid"] = _vat_valid(ht, vat)
    signals["vat_rate"]["vat_rate_valid"] = _rate_valid(value["vat_rate"], ht, vat)
    signals["currency"]["currency_valid"] = value["currency"] in CURRENCIES
    return signals


def names_match(name: str | None, names: Sequence[str]) -> bool:
    """Whether `name` is one of `names` after `search_normalise`, at trigram similarity of at
    least NAME_MATCH."""
    if not name:
        return False
    folded = search_normalise(name).strip()
    return any(
        trigram_similarity(folded, search_normalise(other).strip()) >= NAME_MATCH for other in names
    )


def score(field: str, signals: Mapping[str, bool | None]) -> float:
    """The weighted share of applicable signals that hold, capped at 0.5 when a hard signal
    fails. Raises KeyError for an unknown field."""
    held = applicable = 0.0
    for signal, weight in WEIGHTS[field].items():
        state = signals.get(signal)
        if state is None:
            continue
        applicable += weight
        if state:
            held += weight
    if not applicable:
        result = 0.0
    elif held == applicable:  # exact, whatever the float sum
        result = 1.0
    else:
        result = held / applicable
    if any(signals.get(signal) is False for signal in HARD[field]):
        result = min(result, HARD_CAP)
    if field == "supplier_name" and signals.get("supplier_known") is False:
        result = min(result, MISMATCH_CAP)
    return result


def scores(signals: Mapping[str, Mapping[str, bool | None]]) -> dict[str, float]:
    """Every field's score; a field without signals scores 0."""
    return {name: score(name, signals.get(name, {})) for name in FIELD_NAMES}


def _required(document_type: str | None) -> frozenset[str]:
    return REQUIRED_BY_TYPE.get(document_type or "invoice", REQUIRED_BY_TYPE["invoice"])


def route(
    scores: Mapping[str, float], flags: Collection[str], document_type: str | None
) -> Decision:
    """`filed` iff every required field (type None: the invoice set) scores at least 0.90 and
    no flag blocks; otherwise `needs_review`. Optional fields never block."""
    if any(flag in BLOCKING_FLAGS for flag in flags):
        return "needs_review"
    if all(scores.get(name, 0.0) >= THRESHOLD for name in _required(document_type)):
        return "filed"
    return "needs_review"


def min_confidence_pct(scores: Mapping[str, float], document_type: str | None) -> int:
    """The lowest required field's score, in whole percent rounded down (0 when none)."""
    lowest = min((scores.get(name, 0.0) for name in _required(document_type)), default=0.0)
    return int(Decimal(str(lowest)) * 100)
