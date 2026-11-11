"""What an Ask question asks for (plan 5.3, ARC-2): the JSON the model must fill, and its
validation.

The model reads the question and fills `intent_schema()`; Ollama constrains the output to it,
but nothing the model says is trusted. `validate_intent` turns any output, malformed or hostile,
into an `Intent` Ask can run:
- anything but an object is a bare lookup (no filters);
- unknown keys are dropped, and a value of the wrong type or outside its enum is null;
- `supplier` and `keywords` are stripped and cut (MAX_SUPPLIER, MAX_KEYWORDS); blank is null;
- a date that is not `YYYY-MM-DD` is a null bound; so are both bounds of an inverted period or
  of one wholly after today. A bound the question asked for that cannot be honoured makes the
  intent a lookup: a sum or a count over every year would answer another question. An end
  after today is null (nothing is dated after today), and the intent keeps its kind;
- a `sum` needs a `measure`, else it is a lookup; any other kind has no measure.

The figures are never the model's: Ask computes them in SQL from the intent's filters.
"""

import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Final, Literal, get_args

from sijil_core.fields import DOCUMENT_TYPES

Kind = Literal["sum", "count", "lookup"]
Measure = Literal["vat", "total_ttc", "total_ht"]
KINDS: Final[tuple[str, ...]] = get_args(Kind)
MEASURES: Final[tuple[str, ...]] = get_args(Measure)
MAX_KEYWORDS: Final = 200
MAX_SUPPLIER: Final = 200

_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


@dataclass(frozen=True, kw_only=True)
class Intent:
    kind: Kind
    measure: Measure | None = None
    supplier: str | None = None
    document_type: str | None = None
    period_from: date | None = None
    period_to: date | None = None
    keywords: str | None = None

    def as_json(self) -> dict[str, str | None]:
        """The intent as stored (`ask_query.intent`), dates as ISO strings."""
        return {
            "kind": self.kind,
            "measure": self.measure,
            "supplier": self.supplier,
            "document_type": self.document_type,
            "period_from": None if self.period_from is None else self.period_from.isoformat(),
            "period_to": None if self.period_to is None else self.period_to.isoformat(),
            "keywords": self.keywords,
        }


def intent_schema() -> dict[str, Any]:
    """The JSON schema the model's answer is constrained to (Ollama `format`)."""
    nullable_text = {"type": ["string", "null"]}
    nullable_date = {"type": ["string", "null"], "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"}
    properties: dict[str, Any] = {
        "kind": {"type": "string", "enum": list(KINDS)},
        "measure": {"type": ["string", "null"], "enum": [*MEASURES, None]},
        "supplier": nullable_text,
        "document_type": {"type": ["string", "null"], "enum": [*DOCUMENT_TYPES, None]},
        "period_from": nullable_date,
        "period_to": nullable_date,
        "keywords": nullable_text,
    }
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def _text(value: Any, limit: int) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split())
    return cleaned[:limit] or None


def _date(value: Any) -> date | None:
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def validate_intent(raw: Any, today: date) -> Intent:
    """See the module docstring. `today` is the region-local date."""
    if not isinstance(raw, dict) or raw.get("kind") not in KINDS:
        return Intent(kind="lookup")
    kind: Kind = raw["kind"]
    measure: Measure | None = raw.get("measure") if raw.get("measure") in MEASURES else None
    document_type = raw.get("document_type")
    if document_type not in DOCUMENT_TYPES:
        document_type = None

    start, end = _date(raw.get("period_from")), _date(raw.get("period_to"))
    dropped = (raw.get("period_from") is not None and start is None) or (
        raw.get("period_to") is not None and end is None
    )
    if (start is not None and end is not None and start > end) or (
        start is not None and start > today
    ):
        start = end = None
        dropped = True
    if end is not None and end > today:
        end = None

    if kind == "sum" and measure is None:
        kind = "lookup"
    if dropped:
        kind = "lookup"
    return Intent(
        kind=kind,
        measure=measure if kind == "sum" else None,
        supplier=_text(raw.get("supplier"), MAX_SUPPLIER),
        document_type=document_type,
        period_from=start,
        period_to=end,
        keywords=_text(raw.get("keywords"), MAX_KEYWORDS),
    )
