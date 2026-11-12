"""Signal-based field confidence and the filing/review routing (REC-3, design §5.3)."""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from sijil_core import confidence
from sijil_core.confidence import (
    BLOCKING_FLAGS,
    HARD,
    THRESHOLD,
    WEIGHTS,
    compute_signals,
    min_confidence_pct,
    route,
    score,
)
from sijil_core.fields import FIELD_NAMES, REQUIRED_BY_TYPE

FIXTURES = Path(__file__).parents[3] / "fixtures"
MANIFEST = {i["file"]: i for i in json.loads((FIXTURES / "invoices" / "manifest.json").read_text())}
SUPPLIERS = {s["ice"]: s for s in json.loads((FIXTURES / "suppliers.json").read_text())}
TODAY = date(2026, 10, 20)


def _values(sample: str, **override: str | None) -> dict[str, str | None]:
    """SAMPLE: canonical values of a fixture invoice, as both runs would read them."""
    item = MANIFEST[sample]
    supplier = SUPPLIERS[item["supplier_ice"]]
    values: dict[str, str | None] = {
        "document_type": "invoice",
        "supplier_name": supplier["name"],
        "supplier_ice": item["supplier_ice"],
        "date": item["date"],
        "number": item["number"],
        "total_ht": item["ht"],
        "vat": item["tva"],
        "vat_rate": str(supplier["vat_rate"]),
        "total_ttc": item["ttc"],
        "currency": item["currency"],
    }
    values.update(override)
    return values


def _content(values: dict[str, str | None]) -> str:
    return (
        f"SAMPLE\n{values['supplier_name']}\nICE {values['supplier_ice']}\n"
        f"FACTURE N° {values['number']}\n"
    )


def _scores(
    values: dict[str, str | None],
    *,
    other: dict[str, str | None] | None = None,
    paperless_type: str | None = "invoice",
    known: str | None = None,
) -> dict[str, float]:
    signals = compute_signals(
        values,
        values if other is None else other,
        _content(values),
        paperless_type=paperless_type,
        known_supplier=known,
        today=TODAY,
    )
    return {name: score(name, signals[name]) for name in FIELD_NAMES}


def test_weights_table_is_the_design_one() -> None:
    assert WEIGHTS["document_type"] == {"runs_agree": 0.6, "paperless_agrees": 0.4}
    assert WEIGHTS["supplier_name"] == {"runs_agree": 0.5, "in_text": 0.2, "supplier_known": 0.3}
    assert WEIGHTS["supplier_ice"] == {
        "runs_agree": 0.4,
        "ice_15_digits": 0.3,
        "supplier_known": 0.3,
    }
    assert WEIGHTS["date"] == {"runs_agree": 0.6, "date_plausible": 0.4}
    assert WEIGHTS["number"] == {"runs_agree": 0.6, "in_text": 0.4}
    for name in ("total_ht", "total_ttc"):
        assert WEIGHTS[name] == {"runs_agree": 0.5, "totals_consistent": 0.5}
    assert WEIGHTS["vat"] == {"runs_agree": 0.5, "totals_consistent": 0.5, "vat_rate_valid": 0.5}
    assert WEIGHTS["vat_rate"] == {"runs_agree": 0.5, "vat_rate_valid": 0.5}
    assert WEIGHTS["currency"] == {"runs_agree": 0.6, "currency_valid": 0.4}
    assert HARD["document_type"] == frozenset()
    assert HARD["supplier_name"] == {"present"}
    assert HARD["supplier_ice"] == {"present", "ice_15_digits"}
    assert HARD["date"] == {"present", "date_plausible"}
    assert HARD["number"] == {"present"}
    assert HARD["total_ht"] == HARD["total_ttc"] == {"present", "totals_consistent"}
    assert HARD["vat"] == {"present", "totals_consistent", "vat_rate_valid"}
    assert HARD["vat_rate"] == frozenset()
    assert HARD["currency"] == {"present", "currency_valid"}
    assert set(WEIGHTS) == set(HARD) == set(FIELD_NAMES)
    assert THRESHOLD == 0.90
    assert BLOCKING_FLAGS == {
        "possible_duplicate",
        "no_text",
        "ingest.unsupported_format",
        "extraction_failed",
        "retention.extend_failed",
        "supplier_mismatch",
        "supplier_own_ice",
    }


def test_totals_mismatch_lowers_confidence_below_090() -> None:
    # sample-05: HT 2340.00 + TVA 468.00 = 2808.00, but the printed TTC is 2808.50.
    item = MANIFEST["sample-05-fr.pdf"]
    values = _values("sample-05-fr.pdf", total_ttc=item["printed_ttc"])
    assert (values["total_ht"], values["vat"], values["total_ttc"]) == (
        "2340.00",
        "468.00",
        "2808.50",
    )
    scores = _scores(values)
    assert scores["total_ttc"] < 0.90
    assert scores["total_ht"] < 0.90
    assert route(scores, (), "invoice") == "needs_review"


def test_all_checks_pass_scores_at_least_090() -> None:
    for sample in MANIFEST:
        item = MANIFEST[sample]
        if "printed_ttc" in item:
            continue
        scores = _scores(_values(sample))
        assert all(scores[name] >= 0.90 for name in FIELD_NAMES), (sample, scores)
        assert route(scores, (), "invoice") == "filed", sample


def test_invalid_vat_rate_flags_vat() -> None:
    # 15 % is no Moroccan rate: HT 1000.00, TVA 150.00, TTC 1150.00 add up but the rate is wrong.
    values = _values(
        "sample-01-fr.pdf", total_ht="1000.00", vat="150.00", total_ttc="1150.00", vat_rate="15"
    )
    signals = compute_signals(
        values, values, _content(values), paperless_type=None, known_supplier=None, today=TODAY
    )
    assert signals["vat"]["totals_consistent"] is True
    assert signals["vat"]["vat_rate_valid"] is False
    assert signals["vat_rate"]["vat_rate_valid"] is False
    assert score("vat", signals["vat"]) < 0.90
    assert score("total_ttc", signals["total_ttc"]) >= 0.90


@pytest.mark.parametrize(
    ("ht", "vat", "valid"),
    [
        ("1000.00", "200.00", True),
        ("1000.00", "140.00", True),
        ("1000.00", "100.00", True),
        ("1000.00", "70.00", True),
        ("1000.00", "200.99", True),  # 20.099 %: within 0.1
        ("1000.00", "201.01", False),  # 20.101 %
        ("1000.00", "0.00", False),  # no exemptions in M3
        ("432.50", "30.28", True),  # sample-03, 7.001 %
    ],
)
def test_vat_rate_rule(ht: str, vat: str, valid: bool) -> None:
    ttc = str(Decimal(ht) + Decimal(vat))
    values = _values("sample-01-fr.pdf", total_ht=ht, vat=vat, total_ttc=ttc, vat_rate=None)
    signals = compute_signals(
        values, values, "", paperless_type=None, known_supplier=None, today=TODAY
    )
    assert signals["vat"]["vat_rate_valid"] is valid


def test_totals_tolerance_is_two_cents() -> None:
    def consistent(ttc: str) -> bool | None:
        values = _values("sample-01-fr.pdf", total_ht="1250.00", vat="250.00", total_ttc=ttc)
        signals = compute_signals(
            values, values, "", paperless_type=None, known_supplier=None, today=TODAY
        )
        return signals["total_ttc"]["totals_consistent"]

    assert consistent("1500.02") is True
    assert consistent("1499.98") is True
    assert consistent("1500.03") is False


def test_routing_threshold_boundary() -> None:
    required = REQUIRED_BY_TYPE["invoice"]
    scores = dict.fromkeys(required, 1.0)
    assert route({**scores, "total_ttc": 0.8999}, (), "invoice") == "needs_review"
    assert route({**scores, "total_ttc": 0.90}, (), "invoice") == "filed"
    # A missing required score counts as 0.
    assert route({k: v for k, v in scores.items() if k != "date"}, (), "invoice") == (
        "needs_review"
    )


def test_blocking_flags_route_to_review() -> None:
    scores = dict.fromkeys(FIELD_NAMES, 1.0)
    for flag in BLOCKING_FLAGS:
        assert route(scores, (flag,), "invoice") == "needs_review", flag
    assert route(scores, ("extraction_failed", "extraction.ai_bad_output"), "invoice") == (
        "needs_review"
    )


def test_type_none_uses_the_invoice_set() -> None:
    scores = dict.fromkeys(REQUIRED_BY_TYPE["receipt"], 1.0)  # no supplier_ice
    assert route(scores, (), "receipt") == "filed"
    assert route(scores, (), None) == "needs_review"
    assert route({**scores, "supplier_ice": 0.95}, (), None) == "filed"


def test_not_applicable_signal_ignored() -> None:
    # Without Paperless's opinion, agreeing runs alone make the type certain.
    assert score("document_type", {"runs_agree": True, "paperless_agrees": None}) == 1.0
    assert score("document_type", {"runs_agree": True}) == 1.0
    assert score("document_type", {"runs_agree": True, "paperless_agrees": False}) == (
        pytest.approx(0.6)
    )
    # No known supplier: the name rests on the runs and the text.
    assert score(
        "supplier_name",
        {"present": True, "runs_agree": True, "in_text": True, "supplier_known": None},
    ) == pytest.approx(1.0)
    # Nothing applicable: 0.
    assert score("document_type", {"paperless_agrees": None}) == 0.0
    assert score("document_type", {}) == 0.0


def test_failed_hard_signal_caps_at_half() -> None:
    signals = {"present": True, "runs_agree": True, "ice_15_digits": False, "supplier_known": True}
    assert score("supplier_ice", signals) == 0.5
    # A hard signal that is not applicable caps nothing.
    assert score("total_ttc", {"present": True, "runs_agree": True, "totals_consistent": None}) == (
        1.0
    )
    assert score("supplier_name", {"present": False, "runs_agree": False}) == 0.0


def test_optional_field_low_does_not_block() -> None:
    values = _values("sample-01-fr.pdf")
    scores = _scores(values, other={**values, "vat_rate": "14"})  # the runs disagree on it
    assert scores["vat_rate"] < 0.90
    assert "vat_rate" not in REQUIRED_BY_TYPE["invoice"]
    assert route(scores, (), "invoice") == "filed"
    # For a contract, amounts are optional too.
    contract = {"document_type": 1.0, "date": 0.95, "total_ttc": 0.0, "supplier_ice": 0.1}
    assert route(contract, (), "contract") == "filed"


def test_in_text_is_normalised_and_case_folded() -> None:
    values = _values("sample-01-fr.pdf")
    content = f"atlas  TÉLÉCOM services sarl\nfacture n° {values['number']!s}".upper()
    signals = compute_signals(
        values, values, content, paperless_type=None, known_supplier=None, today=TODAY
    )
    assert signals["supplier_name"]["in_text"] is True
    assert signals["number"]["in_text"] is True
    signals = compute_signals(
        values, values, "nothing", paperless_type=None, known_supplier=None, today=TODAY
    )
    assert signals["supplier_name"]["in_text"] is False
    assert signals["number"]["in_text"] is False


def test_supplier_known_and_paperless_agrees() -> None:
    values = _values("sample-01-fr.pdf")

    def signals(known: str | None, ptype: str | None) -> dict[str, dict[str, bool | None]]:
        return compute_signals(
            values, values, "", paperless_type=ptype, known_supplier=known, today=TODAY
        )

    assert signals(None, None)["supplier_name"]["supplier_known"] is None
    assert signals(None, None)["supplier_ice"]["supplier_known"] is None
    assert signals(None, None)["document_type"]["paperless_agrees"] is None
    same = signals("ATLAS TÉLÉCOM  SERVICES SARL", "invoice")
    assert same["supplier_name"]["supplier_known"] is True
    assert same["supplier_ice"]["supplier_known"] is True
    assert same["document_type"]["paperless_agrees"] is True
    other = signals("Autre Société SA", "receipt")
    assert other["supplier_name"]["supplier_known"] is False
    assert other["document_type"]["paperless_agrees"] is False


RECORD = SUPPLIERS[MANIFEST["sample-01-fr.pdf"]["supplier_ice"]]["name"]


def _record_signals(name_a: str | None, name_b: str | None, *aliases: str) -> dict[str, Any]:
    a = _values("sample-01-fr.pdf", supplier_name=name_a)
    b = _values("sample-01-fr.pdf", supplier_name=name_b)
    return compute_signals(
        a, b, _content(_values("sample-01-fr.pdf")), paperless_type=None,
        known_supplier="Autre Société SA", supplier_record=(RECORD, *aliases), today=TODAY,
    )  # fmt: skip


def test_a_supplier_record_is_known_when_the_runs_name_it() -> None:
    """The workspace's record for the kept ICE (REC-5) is the kept name. It is known when a
    run's own name matches it (folded, trigram >= 0.6); the runs agree when both do."""
    signals = _record_signals("ATLAS TÉLÉCOM SERVICES", "Atlas Telecom Services")
    assert signals["supplier_name"] == {
        "runs_agree": True,
        "present": True,
        "in_text": True,
        "supplier_known": True,
    }
    assert signals["supplier_ice"]["supplier_known"] is True
    assert score("supplier_name", signals["supplier_name"]) == 1.0
    one = _record_signals("ATLAS TÉLÉCOM SERVICES", "ATLAS TLC SAMPLE")
    assert (one["supplier_name"]["supplier_known"], one["supplier_name"]["runs_agree"]) == (
        True,
        False,
    )


def test_a_run_name_matching_an_alias_is_known() -> None:
    signals = _record_signals("ATS Maroc SAMPLE", "ATS MAROC SAMPLE", "ATS Maroc SAMPLE")
    assert signals["supplier_name"]["supplier_known"] is True
    assert signals["supplier_name"]["runs_agree"] is True


def test_a_record_the_runs_do_not_name_is_a_mismatch() -> None:
    """Both runs read another company's name next to this ICE (a client's ICE, say): not
    known, the runs do not agree on a matching name, and the score stays under the cap."""
    signals = _record_signals("Papeterie Al Manar SARL", "Papeterie Al Manar SARL")
    assert signals["supplier_name"]["supplier_known"] is False
    assert signals["supplier_name"]["runs_agree"] is False
    assert signals["supplier_ice"]["supplier_known"] is False
    assert score("supplier_name", signals["supplier_name"]) <= confidence.MISMATCH_CAP
    capped = {"runs_agree": True, "present": True, "in_text": True, "supplier_known": False}
    assert confidence.MISMATCH_CAP == 0.85
    assert score("supplier_name", capped) <= 0.85
    assert "supplier_mismatch" in BLOCKING_FLAGS


def test_runs_agree_and_kept_values() -> None:
    a = _values("sample-01-fr.pdf", number=None)
    b = _values("sample-01-fr.pdf", total_ttc="1050.00")
    signals = compute_signals(a, b, "", paperless_type=None, known_supplier=None, today=TODAY)
    assert signals["number"]["runs_agree"] is False
    assert signals["number"]["present"] is True  # run B's value is kept
    assert signals["total_ttc"]["runs_agree"] is False
    assert signals["total_ttc"]["totals_consistent"] is True  # run A's 1500.00 is kept
    assert signals["supplier_ice"]["runs_agree"] is True
    assert confidence.kept(a, b)["number"] == MANIFEST["sample-01-fr.pdf"]["number"]
    assert confidence.kept(a, b)["total_ttc"] == "1500.00"


def test_no_runs_means_nothing_present() -> None:
    signals = compute_signals(None, None, "", paperless_type=None, known_supplier=None, today=TODAY)
    assert set(signals) == set(FIELD_NAMES)
    scores = {name: score(name, signals[name]) for name in FIELD_NAMES}
    assert all(s == 0.0 for s in scores.values())
    assert min_confidence_pct(scores, None) == 0


@pytest.mark.parametrize(
    ("value", "plausible"),
    [
        ("2026-10-20", True),
        ("2026-10-21", True),  # one day ahead: a time zone apart
        ("2026-10-22", False),
        ("2011-10-20", True),  # 15 years back, inclusive
        ("2011-10-19", False),
    ],
)
def test_date_plausible_window(value: str, plausible: bool) -> None:
    values = _values("sample-01-fr.pdf", date=value)
    signals = compute_signals(
        values, values, "", paperless_type=None, known_supplier=None, today=TODAY
    )
    assert signals["date"]["date_plausible"] is plausible
    expected = date.fromisoformat(value) if plausible else None
    assert confidence.plausible_date(value, TODAY) == expected


def test_ice_and_currency_checks() -> None:
    values = _values("sample-01-fr.pdf", supplier_ice="12345", currency="GBP")
    signals = compute_signals(
        values, values, "", paperless_type=None, known_supplier=None, today=TODAY
    )
    assert signals["supplier_ice"]["ice_15_digits"] is False
    assert signals["currency"]["currency_valid"] is False
    assert score("supplier_ice", signals["supplier_ice"]) <= 0.5
    assert score("currency", signals["currency"]) <= 0.5


def test_min_confidence_pct_over_required_fields() -> None:
    scores = {**dict.fromkeys(FIELD_NAMES, 1.0), "vat_rate": 0.1, "number": 0.57}
    assert min_confidence_pct(scores, "invoice") == 57
    assert min_confidence_pct(scores, "contract") == 100
    assert min_confidence_pct({}, "invoice") == 0


def test_the_workspace_own_ice_as_supplier_blocks_filing() -> None:
    """Final review I2: a supplier ICE that is the workspace's own (`suppliers.own_ices`) is
    flagged and goes to review, however high every score."""
    assert confidence.OWN_ICE == "supplier_own_ice"
    perfect = dict.fromkeys(FIELD_NAMES, 1.0)
    assert route(perfect, [], "invoice") == "filed"
    assert route(perfect, [confidence.OWN_ICE], "invoice") == "needs_review"
