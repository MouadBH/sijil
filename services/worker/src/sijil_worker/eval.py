"""`sijil-eval extraction`: how accurately the extraction reads the SAMPLE invoices (M3, 3.7).

For each manifest entry the ledger row is found by the SHA-256 of the fixture file within the
workspace (the ledger holds it, unique per workspace), and its stored `document_fields` are
compared with the manifest. A row without fields is extracted now with the same step
`extract_document` uses (`_extract`: two model runs, the index row written) but is neither
routed nor audited, and the ledger is not touched. Accuracy is reported, never gated: the exit
code is 0 whatever it is, and 2 when the set or the workspace is not found.

Only the table and the JSON are printed, never the content of a document.
"""

import argparse
import asyncio
import hashlib
import json
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, cast
from uuid import UUID

from sqlalchemy import select

from sijil_core import index as index_db
from sijil_core.crypto import LocalKeyService
from sijil_core.db.base import utcnow
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import DocumentLedger, Organisation, Workspace
from sijil_core.index import IndexEngines
from sijil_core.ollama import OllamaClient
from sijil_core.paperless import PaperlessClient
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.text import normalise
from sijil_worker.extract import _extract, _load
from sijil_worker.settings import ExtractionSettings

EVAL_FIELDS: Final = (
    "supplier_name",
    "supplier_ice",
    "date",
    "number",
    "total_ht",
    "vat",
    "total_ttc",
)
LANGS: Final = ("fr", "ar")
SOURCES: Final = ("stored", "extracted_now", "missing")
# CLI output for developers, not a UI string: the i18n rule (CLAUDE.md rule 6) does not apply.
TARGETS_LINE: Final = (
    "Targets (reported, not gated): fr ≥ 95%, ar ≥ 90% on supplier, date and total"
)
NOT_FOUND: Final = 2

Tally = dict[str, dict[str, dict[str, int]]]


@dataclass(frozen=True)
class EvalResult:
    """One field of one document: whether the stored value equals the expected one."""

    lang: str
    field: str
    correct: bool
    source: str  # "stored" | "extracted_now" | "missing"


def _fold(s: str) -> str:
    return normalise(s).strip().casefold()


def expected_values(
    entry: Mapping[str, Any], suppliers: Sequence[Mapping[str, Any]]
) -> dict[str, tuple[str, ...]]:
    """The accepted canonical values per field. The supplier's name is accepted in either
    language (the French or the Arabic one listed for its ICE), whatever the document's;
    `total_ttc` is the printed one when the manifest has it (it is what the document shows)."""
    names = [
        n
        for s in suppliers
        if s.get("ice") == entry["supplier_ice"]
        for n in (s.get("name"), s.get("name_ar"))
        if n
    ]
    return {
        "supplier_name": tuple(names),
        "supplier_ice": (entry["supplier_ice"],),
        "date": (entry["date"],),
        "number": (entry["number"].strip(),),
        "total_ht": (entry["ht"],),
        "vat": (entry["tva"],),
        "total_ttc": (entry.get("printed_ttc") or entry["ttc"],),
    }


def score_entry(
    entry: Mapping[str, Any],
    suppliers: Sequence[Mapping[str, Any]],
    values: Mapping[str, str | None],
) -> dict[str, bool]:
    """Per field: does the stored canonical value equal an accepted expected one?"""
    out: dict[str, bool] = {}
    for field, accepted in expected_values(entry, suppliers).items():
        got = values.get(field)
        if got is None:
            out[field] = False
        elif field == "supplier_name":
            out[field] = _fold(got) in {_fold(a) for a in accepted}
        elif field == "number":
            out[field] = got.strip() in accepted
        else:
            out[field] = got in accepted
    return out


def tally(results: Sequence[EvalResult]) -> Tally:
    """`{field: {lang: {"correct", "total"}}}`, in field order, for the languages seen."""
    out: Tally = {}
    for field in EVAL_FIELDS:
        rows = [r for r in results if r.field == field]
        if not rows:
            continue
        out[field] = {
            lang: {
                "correct": sum(1 for r in rows if r.lang == lang and r.correct),
                "total": sum(1 for r in rows if r.lang == lang),
            }
            for lang in LANGS
        }
    return out


def _cell(correct: int, total: int) -> str:
    pct = 0 if total == 0 else correct * 100 // total  # integer, floor
    return f"{correct}/{total} ({pct}%)"


def report(tallied: Tally, model: str) -> str:
    """The table: a row per field, columns `fr`, `ar` and `all`, then the targets line."""
    rows: list[tuple[str, ...]] = [("field", "fr", "ar", "all")]
    for field, langs in tallied.items():
        cells = [_cell(langs[lang]["correct"], langs[lang]["total"]) for lang in LANGS]
        total = sum(langs[lang]["total"] for lang in LANGS)
        correct = sum(langs[lang]["correct"] for lang in LANGS)
        rows.append((field, *cells, _cell(correct, total)))
    widths = [max(len(r[i]) for r in rows) + 2 for i in range(3)]
    lines = [f"Extraction accuracy (model: {model})"]
    for r in rows:
        lines.append("".join(r[i].ljust(widths[i]) for i in range(3)) + r[3])
    lines.append(TARGETS_LINE)
    return "\n".join(lines)


def to_json(tallied: Tally, model: str, results: Sequence[EvalResult]) -> str:
    # One count per document (not per field): each document yields one result per field.
    per_doc = Counter(r.source for r in results if r.field == EVAL_FIELDS[0])
    if not per_doc:
        per_doc = Counter(r.source for r in results)
    return json.dumps(
        {
            "fields": tallied,
            "model": model,
            "sources": {s: per_doc.get(s, 0) for s in SOURCES},
        },
        ensure_ascii=False,
    )


async def _workspace_of(
    sessions: Any, settings: ExtractionSettings, workspace_id: UUID
) -> Workspace | None:
    async with sessions() as session:
        found = (
            await session.execute(
                select(Workspace, Organisation.region)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(Workspace.id == workspace_id)
            )
        ).first()
    if found is None:
        return None
    workspace, region = found
    try:
        assert_region(region, settings)
    except RegionMismatch:
        return None
    return cast(Workspace, workspace)


async def _evaluate(
    settings: ExtractionSettings,
    workspace_id: UUID,
    entries: Sequence[Mapping[str, Any]],
    suppliers: Sequence[Mapping[str, Any]],
    base: Path,
) -> list[EvalResult] | None:
    engine = make_engine(settings.database_url)
    keys = LocalKeyService(settings.master_key_bytes())
    sessions = make_sessionmaker(engine)
    engines = IndexEngines(settings, keys, sessions)
    results: list[EvalResult] = []
    try:
        workspace = await _workspace_of(sessions, settings, workspace_id)
        if workspace is None:
            return None
        async with OllamaClient(settings.ollama_url, settings.ai_allowed_hosts) as ollama:
            for entry in entries:
                digest = hashlib.sha256((base / entry["file"]).read_bytes()).hexdigest()
                async with sessions() as session:
                    ledger_id = await session.scalar(
                        select(DocumentLedger.id).where(
                            DocumentLedger.workspace_id == workspace.id,
                            DocumentLedger.sha256 == digest,
                        )
                    )
                values: Mapping[str, str | None] = {}
                source = "missing"
                if ledger_id is not None:
                    async with (await engines.engine(workspace)).connect() as conn:
                        row = await index_db.read_fields(conn, ledger_id)
                    if row is not None:
                        values, source = row.values, "stored"
                    else:
                        try:
                            job = await _load(sessions, keys, settings, ledger_id, workspace.id)
                            if job is not None:
                                fresh = await _extract(
                                    job,
                                    index=engines,
                                    paperless=PaperlessClient,
                                    ollama=ollama,
                                    model=settings.extraction_model,
                                    now=utcnow(),
                                )
                                values, source = fresh.values, "extracted_now"
                        except Exception as exc:
                            print(
                                f"{entry['file']}: extraction failed ({type(exc).__name__})",
                                file=sys.stderr,
                            )
                scored = score_entry(entry, suppliers, values)
                results.extend(EvalResult(entry["lang"], f, ok, source) for f, ok in scored.items())
        return results
    finally:
        await engines.aclose()
        await engine.dispose()


def _load_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sijil-eval")
    sub = parser.add_subparsers(dest="command", required=True)
    ext = sub.add_parser("extraction", help="field accuracy of the extraction on a fixture set")
    ext.add_argument("--set", required=True, type=Path, help="manifest.json of the fixtures")
    ext.add_argument("--workspace", required=True, help="workspace id of the ingested fixtures")
    ext.add_argument("--suppliers", type=Path, help="default: suppliers.json beside the set's dir")
    ext.add_argument("--json", action="store_true", help="print JSON instead of the table")
    args = parser.parse_args(argv)

    manifest = _load_json(args.set)
    suppliers = _load_json(args.suppliers or args.set.parent.parent / "suppliers.json")
    if not isinstance(manifest, list) or not isinstance(suppliers, list):
        print("set not found", file=sys.stderr)
        return NOT_FOUND
    if not all((args.set.parent / e["file"]).is_file() for e in manifest):
        print("set not found: a fixture file is missing", file=sys.stderr)
        return NOT_FOUND
    try:
        workspace_id = UUID(args.workspace)
    except ValueError:
        print("workspace not found", file=sys.stderr)
        return NOT_FOUND

    settings = ExtractionSettings()
    results = asyncio.run(_evaluate(settings, workspace_id, manifest, suppliers, args.set.parent))
    if results is None:
        print("workspace not found", file=sys.stderr)
        return NOT_FOUND
    tallied = tally(results)
    model = settings.extraction_model
    print(to_json(tallied, model, results) if args.json else report(tallied, model))
    return 0


if __name__ == "__main__":
    sys.exit(main())
