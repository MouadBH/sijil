"""`sijil-eval extraction`: how accurately the extraction reads the SAMPLE invoices (M3, 3.7).

For each manifest entry the ledger row is found by the SHA-256 of the fixture file within the
workspace (the ledger holds it, unique per workspace), and its stored `document_fields` are
compared with the manifest. A row without fields is computed now with the same step
`extract_document` uses (`_compute`: the two model runs, nothing written) and is marked
"computed": the index, the audit log and the ledger are never written, and nothing is routed.
Accuracy is reported, never gated: the exit code is 0 whatever it is, and 2 when the set or the
workspace is not found (or the set is malformed).

Only the table and the JSON are printed, never the content of a document.
"""

import argparse
import asyncio
import hashlib
import json
import logging
import sys
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import index as index_db
from sijil_core.crypto import KeyService, LocalKeyService
from sijil_core.db.base import utcnow
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    DocumentLedger,
    DocumentState,
    IngestState,
    Organisation,
    Workspace,
)
from sijil_core.index import IndexEngines
from sijil_core.ollama import OllamaClient
from sijil_core.paperless import PaperlessClient
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.text import normalise
from sijil_worker.extract import _compute, _load
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
SOURCES: Final = ("stored", "computed", "missing")
ENTRY_KEYS: Final = ("file", "lang", "supplier_ice", "number", "date", "ht", "tva", "ttc")
log = logging.getLogger(__name__)
# CLI output for developers, not a UI string: the i18n rule (CLAUDE.md rule 6) does not apply.
TARGETS_LINE: Final = (
    "Targets (reported, not gated): fr ≥ 95%, ar ≥ 90% on supplier, date and total"
)
NOT_FOUND: Final = 2

Tally = dict[str, Any]  # {"fields": {...}, "documents": {...}, "missing": [...]}


@dataclass(frozen=True)
class EvalResult:
    """One field of one document: whether the stored or computed value equals the expected."""

    lang: str
    field: str
    correct: bool
    source: str  # "stored" | "computed" | "missing"
    doc: str = ""  # the fixture file
    reason: str | None = None  # why a document is "missing"


def _fold(s: str) -> str:
    return normalise(s).strip().casefold()


def expected_values(
    entry: Mapping[str, Any], suppliers: Sequence[Mapping[str, Any]]
) -> dict[str, tuple[str, ...]]:
    """The accepted canonical values per field. The supplier's name is the one listed for its
    ICE in the document's language (`name` for fr, `name_ar` for ar); either is accepted only
    when that language's name is absent. `total_ttc` is the printed one when the manifest has
    it (it is what the document shows)."""
    key = "name_ar" if entry["lang"] == "ar" else "name"
    listed = [s for s in suppliers if s.get("ice") == entry["supplier_ice"]]
    names = [n for s in listed if (n := s.get(key))]
    if not names:
        names = [n for s in listed for n in (s.get("name"), s.get("name_ar")) if n]
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
    """Per field and language `{"correct", "total"}`, the documents by source, and the missing
    ones with their reason."""
    fields: dict[str, dict[str, dict[str, int]]] = {}
    for field in EVAL_FIELDS:
        rows = [r for r in results if r.field == field]
        if rows:
            fields[field] = {
                lang: {
                    "correct": sum(1 for r in rows if r.lang == lang and r.correct),
                    "total": sum(1 for r in rows if r.lang == lang),
                }
                for lang in LANGS
            }
    docs = {(r.doc, r.lang): r for r in results}  # one result stands for each document
    counts = Counter(r.source for r in docs.values())
    return {
        "fields": fields,
        "documents": {s: counts.get(s, 0) for s in SOURCES},
        "missing": [
            {"file": r.doc, "missing_reason": r.reason}
            for r in docs.values()
            if r.source == "missing"
        ],
    }


def _cell(correct: int, total: int) -> str:
    pct = 0 if total == 0 else correct * 100 // total  # integer, floor; 0/0 reads 0%
    return f"{correct}/{total} ({pct}%)"


def report(tallied: Tally, model: str) -> str:
    """The table (a row per field; columns `fr`, `ar`, `all`), the documents line, then the
    targets line."""
    rows: list[tuple[str, ...]] = [("field", "fr", "ar", "all")]
    for field, langs in tallied["fields"].items():
        cells = [_cell(langs[lang]["correct"], langs[lang]["total"]) for lang in LANGS]
        total = sum(langs[lang]["total"] for lang in LANGS)
        correct = sum(langs[lang]["correct"] for lang in LANGS)
        rows.append((field, *cells, _cell(correct, total)))
    widths = [max(len(r[i]) for r in rows) + 2 for i in range(3)]
    lines = [f"Extraction accuracy (model: {model})"]
    for r in rows:
        lines.append("".join(r[i].ljust(widths[i]) for i in range(3)) + r[3])
    d = tallied["documents"]
    lines.append(
        f"Documents: {d['stored']} stored, {d['computed']} computed, "
        f"{d['missing']} missing (counted incorrect)"
    )
    lines.append(TARGETS_LINE)
    return "\n".join(lines)


def to_json(tallied: Tally, model: str) -> str:
    return json.dumps({**tallied, "model": model}, ensure_ascii=False)


def validate_manifest(manifest: Any) -> str | None:
    """A message when the set is malformed (or has a language other than fr or ar), else None."""
    if not isinstance(manifest, list):
        return "the set must be a list of entries"
    for i, entry in enumerate(manifest):
        if not isinstance(entry, dict):
            return f"entry {i} is not an object"
        missing = [k for k in ENTRY_KEYS if not isinstance(entry.get(k), str)]
        if missing:
            return f"entry {i} lacks {', '.join(missing)}"
        if entry["lang"] not in LANGS:
            return f"entry {i} has an unknown language"
    return None


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


async def evaluate(
    *,
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    settings: ExtractionSettings,
    engines: IndexEngines,
    ollama: OllamaClient,
    paperless: Callable[..., PaperlessClient],
    workspace: Workspace,
    entries: Sequence[Mapping[str, Any]],
    suppliers: Sequence[Mapping[str, Any]],
    base: Path,
) -> list[EvalResult]:
    """Stored values where the index has them, else computed now (`_compute`). Reads only: the
    index, the audit log and the ledger are never written, nothing is routed."""
    results: list[EvalResult] = []
    for entry in entries:
        digest = hashlib.sha256((base / entry["file"]).read_bytes()).hexdigest()
        values: Mapping[str, str | None] = {}
        source, reason = "missing", None
        async with sessions() as session:
            ledger = (
                await session.execute(
                    select(
                        DocumentLedger.id, DocumentLedger.state, DocumentLedger.ingest_state
                    ).where(
                        DocumentLedger.workspace_id == workspace.id,
                        DocumentLedger.sha256 == digest,
                    )
                )
            ).first()
        if ledger is None:
            reason = "not_in_ledger"
        else:
            async with (await engines.engine(workspace)).connect() as conn:
                stored = await index_db.read_fields(conn, ledger.id)
            if stored is not None:
                values, source = stored.values, "stored"
            elif ledger.state is not DocumentState.received:
                reason = "not_received"
            elif ledger.ingest_state is not IngestState.consumed:
                reason = "not_consumed"
            else:
                try:
                    job = await _load(sessions, keys, settings, ledger.id, workspace.id)
                    if job is None:
                        reason = "not_received"
                    else:
                        row, _, _ = await _compute(
                            job,
                            index=engines,
                            paperless=paperless,
                            ollama=ollama,
                            model=settings.extraction_model,
                            now=utcnow(),
                        )
                        values, source = row.values, "computed"
                except Exception as exc:
                    log.warning("%s: extraction failed (%s)", entry["file"], type(exc).__name__)
                    reason = "extraction_failed"
        scored = score_entry(entry, suppliers, values)
        results.extend(
            EvalResult(entry["lang"], f, ok, source, entry["file"], reason)
            for f, ok in scored.items()
        )
    return results


async def _run(
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
    try:
        workspace = await _workspace_of(sessions, settings, workspace_id)
        if workspace is None:
            return None
        async with OllamaClient(
            settings.ollama_url, settings.ai_allowed_hosts, num_thread=settings.ollama_num_thread
        ) as ollama:
            return await evaluate(
                sessions=sessions,
                keys=keys,
                settings=settings,
                engines=engines,
                ollama=ollama,
                paperless=PaperlessClient,
                workspace=workspace,
                entries=entries,
                suppliers=suppliers,
                base=base,
            )
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
    if manifest is None or not isinstance(suppliers, list):
        print("set not found", file=sys.stderr)
        return NOT_FOUND
    problem = validate_manifest(manifest)
    if problem is not None:
        print(f"malformed set: {problem}", file=sys.stderr)
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
    results = asyncio.run(_run(settings, workspace_id, manifest, suppliers, args.set.parent))
    if results is None:
        print("workspace not found", file=sys.stderr)
        return NOT_FOUND
    tallied = tally(results)
    model = settings.extraction_model
    print(to_json(tallied, model) if args.json else report(tallied, model))
    return 0


if __name__ == "__main__":
    sys.exit(main())
