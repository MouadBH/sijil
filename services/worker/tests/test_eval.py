"""The extraction accuracy harness: comparison, tally, table and JSON (pure functions)."""

import json
from pathlib import Path

import pytest

from sijil_worker import eval as ev

SUPPLIERS = [
    {"name": "Atlas Télécom Services SARL", "name_ar": "أطلس لخدمات الاتصالات", "ice": "001"},
]
ENTRY = {
    "file": "x.pdf",
    "lang": "fr",
    "supplier_ice": "001",
    "number": "F-1",
    "date": "2026-09-30",
    "ht": "1250.00",
    "tva": "250.00",
    "ttc": "1500.00",
}
GOOD = {
    "supplier_name": "Atlas Télécom Services SARL",
    "supplier_ice": "001",
    "date": "2026-09-30",
    "number": "F-1",
    "total_ht": "1250.00",
    "vat": "250.00",
    "total_ttc": "1500.00",
}


def _results(
    lang: str,
    field: str,
    correct: int,
    total: int,
    source: str = "stored",
    reason: str | None = None,
) -> list[ev.EvalResult]:
    return [
        ev.EvalResult(lang, field, i < correct, source, f"{lang}-{i}.pdf", reason)
        for i in range(total)
    ]


def test_eval_report_format() -> None:
    results = (
        _results("fr", "supplier_name", 5, 6)
        + _results("ar", "supplier_name", 3, 6)
        + _results("fr", "total_ttc", 6, 6)
        + _results("ar", "total_ttc", 0, 6)
    )
    text = ev.report(ev.tally(results), "qwen3:4b")
    expected = "\n".join(
        [
            "Extraction accuracy (model: qwen3:4b)",
            "field          fr          ar         all",
            "supplier_name  5/6 (83%)   3/6 (50%)  8/12 (66%)",
            "total_ttc      6/6 (100%)  0/6 (0%)   6/12 (50%)",
            "Documents: 12 stored, 0 computed, 0 missing (counted incorrect)",
            "Targets (reported, not gated): fr ≥ 95%, ar ≥ 90% on supplier, date and total",
        ]
    )
    assert text == expected


def test_eval_uses_printed_ttc() -> None:
    entry = {**ENTRY, "printed_ttc": "1500.50"}
    assert ev.expected_values(entry, SUPPLIERS)["total_ttc"] == ("1500.50",)
    assert ev.expected_values(ENTRY, SUPPLIERS)["total_ttc"] == ("1500.00",)
    got = ev.score_entry(entry, SUPPLIERS, {**GOOD, "total_ttc": "1500.50"})
    assert got["total_ttc"] is True
    assert ev.score_entry(entry, SUPPLIERS, GOOD)["total_ttc"] is False


@pytest.mark.parametrize(
    ("lang", "value", "ok"),
    [
        ("fr", "  ATLAS  télécom services sarl ", True),
        ("fr", "أطلس لخدمات الاتصالات", False),  # compared in the document's language
        ("ar", "Atlas Télécom Services SARL", False),
        ("ar", "أطلس لخدمات الاتصالات", True),
        ("fr", "Atlas Telecom Services", False),
        ("fr", None, False),
    ],
)
def test_eval_supplier_compared_normalised(lang: str, value: str | None, ok: bool) -> None:
    got = ev.score_entry({**ENTRY, "lang": lang}, SUPPLIERS, {**GOOD, "supplier_name": value})
    assert got["supplier_name"] is ok


def test_eval_supplier_either_name_when_language_name_absent() -> None:
    suppliers = [{"name": "Atlas", "ice": "001"}]
    entry = {**ENTRY, "lang": "ar"}
    assert ev.score_entry(entry, suppliers, {**GOOD, "supplier_name": "atlas"})["supplier_name"]


def test_eval_report_shows_missing_documents() -> None:
    results = _results("fr", "date", 0, 2, "missing", "not_in_ledger") + _results(
        "ar", "date", 1, 1, "computed"
    )
    text = ev.report(ev.tally(results), "m")
    assert "Documents: 0 stored, 1 computed, 2 missing (counted incorrect)" in text
    out = json.loads(ev.to_json(ev.tally(results), "m"))
    assert out["documents"] == {"stored": 0, "computed": 1, "missing": 2}
    assert out["missing"] == [
        {"file": "fr-0.pdf", "missing_reason": "not_in_ledger"},
        {"file": "fr-1.pdf", "missing_reason": "not_in_ledger"},
    ]


def test_eval_json_shape() -> None:
    results = _results("fr", "date", 1, 2) + _results("ar", "date", 2, 2)
    out = json.loads(ev.to_json(ev.tally(results), "m"))
    assert out == {
        "fields": {
            "date": {
                "fr": {"correct": 1, "total": 2},
                "ar": {"correct": 2, "total": 2},
            }
        },
        "documents": {"stored": 4, "computed": 0, "missing": 0},
        "missing": [],
        "model": "m",
    }


def test_eval_cell_with_no_documents_reads_zero_percent() -> None:
    assert ev._cell(0, 0) == "0/0 (0%)"


def test_eval_empty_results() -> None:
    text = ev.report(ev.tally([]), "m")
    assert text.splitlines() == [
        "Extraction accuracy (model: m)",
        "field  fr  ar  all",
        "Documents: 0 stored, 0 computed, 0 missing (counted incorrect)",
        ev.TARGETS_LINE,
    ]


def test_eval_exit_2_when_set_missing(tmp_path: Path) -> None:
    code = ev.main(["extraction", "--set", str(tmp_path / "nope.json"), "--workspace", "x"])
    assert code == 2


@pytest.mark.parametrize(
    "manifest",
    [
        [{"lang": "fr"}],  # missing keys
        [{**ENTRY, "lang": "en"}],  # unknown language
        ["x.pdf"],
        {"file": "x.pdf"},
    ],
)
def test_eval_exit_2_on_malformed_set(
    tmp_path: Path, manifest: object, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "invoices").mkdir()
    (tmp_path / "suppliers.json").write_text("[]")
    path = tmp_path / "invoices" / "manifest.json"
    path.write_text(json.dumps(manifest))
    assert ev.main(["extraction", "--set", str(path), "--workspace", "x"]) == 2
    assert "malformed set" in capsys.readouterr().err
