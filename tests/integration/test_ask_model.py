"""Ask with the real in-region models (slow: `make acceptance`), in a workspace of its own.

A fresh workspace (marked `closing` at teardown through `created_workspaces`) gets the two
Atlas Télécom SAMPLE invoices of 2026 (fixtures/invoices/manifest.json, a fictitious
supplier) through the API. Their fields are written straight into the workspace's index from
the manifest, as `tests/support/sealing.py` does, before ingest finishes, so the model never
extracts them; they are routed `filed` (filed counts in sums: nothing is sealed). The search
rows and the embeddings are then rebuilt (`sijil.search_backfill`), and the test asks through
the API and the Ask worker (`qwen3:4b` reads and writes, `bge-m3` embeds) for the VAT paid to
Atlas Télécom in 2026, once in Arabic and once in French.

Each answer must carry the exact figure, computed independently here from the manifest with
Decimal, and at least one citation. Its text is either the model's, in the question's
language, or null with the templated `ask.answer.sum`. The answers, figures, citations and
timings are printed (run with -s). The shared SAMPLE workspace is never touched.
"""

import asyncio
import json
import re
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from celery import Celery
from sqlalchemy import text

from sijil_core.crypto import LocalKeyService
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import DocumentLedger, DocumentState, IngestState, Workspace
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow, IndexEngines, read_fields, write_extraction
from sijil_core.storage import S3ObjectStore, StorageSettings
from sijil_worker.extract import _route
from sijil_worker.settings import WorkerSettings
from tests.support.polling import poll_get

pytestmark = [pytest.mark.integration, pytest.mark.slow]

CP_URL = "http://control-plane:8000"
PASSWORD = "it-ask-model-Passw0rd"  # noqa: S105 - a throw-away SAMPLE account
FIXTURES = Path(__file__).parents[2] / "fixtures"
MANIFEST: list[dict[str, Any]] = json.loads((FIXTURES / "invoices" / "manifest.json").read_text())
SUPPLIERS = {s["ice"]: s for s in json.loads((FIXTURES / "suppliers.json").read_text())}
ATLAS = "001905837000041"  # SAMPLE: Atlas Télécom Services SARL (fictitious)
YEAR = 2026
QUESTIONS = {
    "ar": "كم دفعنا من الضريبة على القيمة المضافة لشركة أطلس لخدمات الاتصالات سنة 2026؟",
    "fr": "Combien de TVA avons-nous payée à Atlas Télécom Services en 2026 ?",
}
PROVISION_BUDGET_S = 240
CONSUME_BUDGET_S = 300
INDEX_BUDGET_S = 600
ANSWER_BUDGET_S = 1800
POLL_S = 3
_ARABIC = re.compile(r"[ء-ي]")
_LATIN = re.compile(r"[A-Za-z]")


def _log(line: str) -> None:
    print(line, flush=True)  # noqa: T201 - the acceptance log (-s runs)


def _csrf(http: httpx.AsyncClient) -> dict[str, str]:
    return {"X-CSRF-Token": http.cookies["sijil_csrf"]}


def _items() -> list[dict[str, Any]]:
    return [i for i in MANIFEST if i["supplier_ice"] == ATLAS and i["date"].startswith(f"{YEAR}-")]


def _expected() -> Decimal:
    """The VAT paid to Atlas in 2026, from the manifest alone."""
    return sum((Decimal(i["tva"]) for i in _items()), Decimal("0"))


def _fields(item: dict[str, Any], ledger_id: uuid.UUID, now: datetime) -> FieldsRow:
    """The invoice's fields as printed (the Arabic one names the supplier in Arabic), complete
    and confident, as an extraction that went well would leave them."""
    supplier = SUPPLIERS[item["supplier_ice"]]
    values: dict[str, str | None] = {
        "document_type": "invoice",
        "supplier_name": supplier["name_ar"] if item["lang"] == "ar" else supplier["name"],
        "supplier_ice": item["supplier_ice"],
        "date": item["date"],
        "number": item["number"],
        "total_ht": item["ht"],
        "vat": item["tva"],
        "vat_rate": str(supplier["vat_rate"]),
        "total_ttc": item["ttc"],
        "currency": item["currency"],
    }
    return FieldsRow(
        ledger_id=ledger_id,
        document_type="invoice",
        values={n: values.get(n) for n in FIELD_NAMES},
        confidence=dict.fromkeys(FIELD_NAMES, 0.95),
        signals={n: {"runs_agree": True} for n in FIELD_NAMES},
        source=dict.fromkeys(FIELD_NAMES, "extraction"),
        extracted_at=now,
        updated_at=now,
    )


async def _answered(http: httpx.AsyncClient, base: str, ask_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + ANSWER_BUDGET_S
    while True:
        r = await poll_get(http, f"{base}/ask/{ask_id}")
        assert r.status_code == 200, r.text
        body: dict[str, Any] = r.json()
        if body["state"] in ("answered", "failed"):
            return body
        assert time.monotonic() < deadline, f"no answer within {ANSWER_BUDGET_S} s: {body}"
        await asyncio.sleep(POLL_S)


async def test_vat_question_answered_with_the_computed_figure(
    worker_settings: WorkerSettings, created_workspaces: list[uuid.UUID]
) -> None:
    expected = _expected()
    assert expected == Decimal("429.80")
    run = uuid.uuid4().hex[:8]
    engine = make_engine(worker_settings.database_url)
    sessions = make_sessionmaker(engine)
    engines = IndexEngines(
        worker_settings, LocalKeyService(worker_settings.master_key_bytes()), sessions
    )
    try:
        async with httpx.AsyncClient(base_url=CP_URL, timeout=60) as http:
            # 1. A fresh workspace and its Owner.
            (await http.get("/v1/auth/csrf")).raise_for_status()
            r = await http.post(
                "/v1/auth/signup",
                json={"email": f"ask-{run}@sijil.local", "password": PASSWORD,
                      "name": "SAMPLE Ask Owner"},
                headers=_csrf(http),
            )  # fmt: skip
            assert r.status_code == 201, r.text
            r = await http.post(
                "/v1/orgs",
                json={"name": f"SAMPLE Ask Model {run}", "type": "business", "country": "MA"},
                headers=_csrf(http),
            )
            assert r.status_code == 202, r.text
            workspace_id = uuid.UUID(r.json()["workspace"]["id"])
            created_workspaces.append(workspace_id)
            base = f"/v1/workspaces/{workspace_id}"
            started = time.monotonic()
            while (state := (await poll_get(http, base)).json()["state"]) != "active":
                assert state != "error"
                assert time.monotonic() - started < PROVISION_BUDGET_S, state
                await asyncio.sleep(3)
            async with sessions() as db:
                workspace = await db.get(Workspace, workspace_id)
            assert workspace is not None
            _log(f"\nworkspace {workspace_id} active after {time.monotonic() - started:.0f} s")

            # 2. The two invoices, their fields written into the index before ingest ends.
            now = datetime.now(UTC)
            docs: dict[str, uuid.UUID] = {}
            for item in _items():
                data = (FIXTURES / "invoices" / item["file"]).read_bytes()
                r = await http.post(
                    f"{base}/documents", files={"file": (item["file"], data, "application/pdf")},
                    headers=_csrf(http),
                )  # fmt: skip
                assert r.status_code == 202, r.text
                ledger_id = uuid.UUID(r.json()["document_id"])
                docs[item["file"]] = ledger_id
                async with (await engines.engine(workspace)).begin() as conn:
                    await write_extraction(conn, _fields(item, ledger_id, now))

            async def ledger(ledger_id: uuid.UUID) -> DocumentLedger:
                async with sessions() as db:
                    row = await db.get(DocumentLedger, ledger_id, populate_existing=True)
                assert row is not None
                return row

            started = time.monotonic()
            while [
                i
                for i in docs.values()
                if (await ledger(i)).ingest_state is not IngestState.consumed
            ]:
                assert time.monotonic() - started < CONSUME_BUDGET_S, "not consumed in time"
                await asyncio.sleep(3)
            # Routed by the worker's own routing (the extraction worker may get there first).
            store = S3ObjectStore(StorageSettings())
            for ledger_id in docs.values():
                async with (await engines.engine(workspace)).connect() as conn:
                    row = await read_fields(conn, ledger_id)
                assert row is not None
                await _route(sessions, engines, store, workspace, row, datetime.now(UTC))
            states = [(await ledger(i)).state for i in docs.values()]
            assert states == [DocumentState.filed] * len(docs), states

            # 3. Search rows and embeddings rebuilt for the workspace.
            Celery("sijil-it", broker=str(worker_settings.redis_url)).send_task(
                "sijil.search_backfill", kwargs={"workspace_id": str(workspace_id)},
                queue="default",
            )  # fmt: skip
            started = time.monotonic()
            ids = list(docs.values())
            while True:
                async with (await engines.engine(workspace)).connect() as conn:
                    rows = (
                        await conn.execute(
                            text(
                                "SELECT count(*) FILTER (WHERE state = 'filed' AND vat IS NOT "
                                "NULL), (SELECT count(DISTINCT ledger_id) FROM doc_chunk "
                                "WHERE ledger_id = ANY(:ids)) FROM search_doc "
                                "WHERE ledger_id = ANY(:ids)"
                            ),
                            {"ids": ids},
                        )
                    ).one()
                if tuple(rows) == (len(ids), len(ids)):
                    break
                assert time.monotonic() - started < INDEX_BUDGET_S, f"not indexed: {rows}"
                await asyncio.sleep(3)
            _log(f"indexed and embedded after {time.monotonic() - started:.0f} s: {docs}")
            _log(f"expected VAT (manifest, Decimal): {expected}")

            # 4. The question, in Arabic and in French.
            for locale, question in QUESTIONS.items():
                started = time.monotonic()
                r = await http.post(
                    f"{base}/ask", json={"question": question, "locale": locale},
                    headers=_csrf(http),
                )  # fmt: skip
                assert r.status_code == 202, r.text
                answer = await _answered(http, base, r.json()["id"])
                took = time.monotonic() - started
                _log(f"\n[{locale}] {question}\n  took {took:.1f} s")
                _log(f"  answer_text: {answer['answer_text']!r}")
                _log(f"  answer_code: {answer['answer_code']!r}")
                _log(f"  figures: {answer['figures']}")
                _log(f"  excluded: {answer['excluded']}")
                cited = [(c["id"], c["supplier_name"]) for c in answer["citations"]]
                _log(f"  citations: {cited}")

                assert answer["state"] == "answered", answer
                vat = [f for f in answer["figures"] if f["measure"] == "vat"]
                assert vat == [
                    {"measure": "vat", "value": str(expected), "currency": "MAD",
                     "document_count": len(docs)}
                ]  # fmt: skip
                assert answer["citations"], "no citation"
                assert {c["id"] for c in answer["citations"]} <= {str(i) for i in ids}
                written = answer["answer_text"]
                if written is None:
                    assert answer["answer_code"] == "ask.answer.sum"
                    continue
                assert answer["answer_code"] is None
                arabic, latin = len(_ARABIC.findall(written)), len(_LATIN.findall(written))
                assert arabic > latin if locale == "ar" else latin > arabic, written
    finally:
        await engines.aclose()
        await engine.dispose()
