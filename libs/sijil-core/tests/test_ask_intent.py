"""Ask's question intent (`sijil_core.ask_intent`): whatever the model answers, the intent is
one Ask can run, and a doubtful one is narrowed (a lookup), never widened into another sum."""

from datetime import date
from typing import Any

import pytest

from sijil_core.ask_intent import (
    MAX_KEYWORDS,
    MAX_SUPPLIER,
    Intent,
    intent_schema,
    validate_intent,
)
from sijil_core.fields import DOCUMENT_TYPES

TODAY = date(2026, 11, 11)
SUM_2026: dict[str, Any] = {
    "kind": "sum",
    "measure": "vat",
    "supplier": "Atlas Télécom",
    "document_type": "invoice",
    "period_from": "2026-01-01",
    "period_to": "2026-10-31",
    "keywords": "TVA",
}


def test_schema_names_every_key_and_closes_the_enums() -> None:
    schema = intent_schema()
    keys = {"kind", "measure", "supplier", "document_type", "period_from", "period_to", "keywords"}
    assert set(schema["properties"]) == keys
    assert set(schema["required"]) == keys
    assert schema["additionalProperties"] is False
    assert schema["properties"]["kind"]["enum"] == ["sum", "count", "lookup"]
    assert schema["properties"]["measure"]["enum"] == ["vat", "total_ttc", "total_ht", None]
    assert schema["properties"]["document_type"]["enum"] == [*DOCUMENT_TYPES, None]


def test_a_good_intent_is_kept() -> None:
    assert validate_intent(SUM_2026, TODAY) == Intent(
        kind="sum",
        measure="vat",
        supplier="Atlas Télécom",
        document_type="invoice",
        period_from=date(2026, 1, 1),
        period_to=date(2026, 10, 31),
        keywords="TVA",
    )


LOOKUP = Intent(kind="lookup")


@pytest.mark.parametrize(
    "raw",
    [None, "sum", 42, [SUM_2026], {}, {"kind": "delete"}, {"kind": None}, {"kind": ["sum"]}],
)
def test_malformed_output_is_a_bare_lookup(raw: Any) -> None:
    assert validate_intent(raw, TODAY) == LOOKUP


def test_validate_intent_degrades_bad_output() -> None:
    # Unknown keys are dropped.
    extra = validate_intent(SUM_2026 | {"sql": "DROP TABLE search_doc", "answer": 999999}, TODAY)
    assert extra == validate_intent(SUM_2026, TODAY)
    # A sum needs a measure: without one (or with an unknown one) it is a lookup.
    for measure in (None, "profit", 3):
        got = validate_intent(SUM_2026 | {"measure": measure}, TODAY)
        assert (got.kind, got.measure) == ("lookup", None), measure
    # A measure means nothing outside a sum.
    assert validate_intent(SUM_2026 | {"kind": "count"}, TODAY).measure is None
    # Bad dates are null bounds; so is an inverted period, and the sum it bounded becomes a
    # lookup (a sum over every year would answer another question).
    for bad in ("2026-13-01", "31/01/2026", "2026-1-1", "", 20260101, "2026-01-01T00:00:00"):
        got = validate_intent(SUM_2026 | {"period_from": bad}, TODAY)
        assert (got.kind, got.period_from) == ("lookup", None), bad
        assert got.period_to == date(2026, 10, 31)
    inverted = validate_intent(
        SUM_2026 | {"period_from": "2026-10-01", "period_to": "2026-01-31"}, TODAY
    )
    assert (inverted.kind, inverted.period_from, inverted.period_to) == ("lookup", None, None)
    # A period wholly in the future: null bounds, and no sum.
    future = validate_intent(
        SUM_2026 | {"period_from": "2027-01-01", "period_to": "2027-12-31"}, TODAY
    )
    assert (future.kind, future.period_from, future.period_to) == ("lookup", None, None)
    # A period running past today keeps its start: nothing is dated after today.
    running = validate_intent(SUM_2026 | {"period_to": "2026-12-31"}, TODAY)
    assert (running.kind, running.period_from, running.period_to) == (
        "sum", date(2026, 1, 1), None,
    )  # fmt: skip
    # Keywords are cut to 200 characters; blank text is null.
    long = validate_intent(SUM_2026 | {"keywords": "x" * 500}, TODAY)
    assert long.keywords == "x" * MAX_KEYWORDS
    assert validate_intent(SUM_2026 | {"keywords": "   "}, TODAY).keywords is None
    assert validate_intent(SUM_2026 | {"supplier": " \n"}, TODAY).supplier is None
    assert validate_intent(SUM_2026 | {"supplier": "y" * 900}, TODAY).supplier == (
        "y" * MAX_SUPPLIER
    )
    # Wrong types and unknown document types are null.
    assert validate_intent(SUM_2026 | {"supplier": ["Atlas"]}, TODAY).supplier is None
    assert validate_intent(SUM_2026 | {"document_type": "passport"}, TODAY).document_type is None
    assert validate_intent(SUM_2026 | {"keywords": {"q": 1}}, TODAY).keywords is None
