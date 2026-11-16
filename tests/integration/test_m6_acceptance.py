"""M6 acceptance: admin and billing, end to end against the compose stack, with no model
(`make itest`). Started by Task 1 (document-type grants); completed in Task 6.

Document-type grants (ADM-1): a fresh workspace (marked `closing` at teardown through
`created_workspaces`; the shared SAMPLE workspace is never touched) gets two SAMPLE files through
the API, classified directly in its index (no model), one as an invoice and one as a payslip,
then routed by the worker's own routing. Its Owner adds a Member limited to invoices through the
members API. The Member lists, searches and opens the invoice only: the payslip is absent from
the list and from search, and 404 by id, exactly as an unknown id. Ask runs the Ask worker's own
pipeline (`answer_question`) on a fake in-region model for questions stored as the API stores
the Member's (with their grant): the figure counts the invoice only, and a question naming
payslips gets the answer of no matching documents. Exporting is not the Member's (403).
"""

import asyncio
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import select

from sijil_core import ask_store
from sijil_core.crypto import LocalKeyService
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    DocumentLedger,
    DocumentState,
    IngestState,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow, IndexEngines, read_fields, write_extraction
from sijil_core.search_index import SearchLedgerFacts, refresh_search_doc
from sijil_core.storage import S3ObjectStore, StorageSettings
from sijil_core.tenancy import tenant_ref
from sijil_worker.ask import ANSWER_NO_MATCH, answer_question
from sijil_worker.extract import _route
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import AskSettings, WorkerSettings
from tests.support.acceptance import Owner, csrf, post
from tests.support.fake_ollama import FakeOllama
from tests.support.polling import poll_get

pytestmark = pytest.mark.integration

CP_URL = "http://control-plane:8000"
REGION = "ma-cas"
PASSWORD = "it-m6-acceptance-Passw0rd"  # noqa: S105 - a throw-away SAMPLE account
PROVISION_BUDGET_S = 240
CONSUME_BUDGET_S = 600
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
ATLAS = "001905837000041"  # SAMPLE: Atlas Télécom Services SARL (fictitious)
# SAMPLE: what the index holds for each file, as an extraction that went well would leave it.
INVOICE: dict[str, str | None] = {
    "document_type": "invoice",
    "supplier_name": "Atlas Télécom Services SARL",
    "supplier_ice": ATLAS,
    "date": "2026-01-15",
    "number": "M6-F-0001",
    "total_ht": "1000.00",
    "vat": "200.00",
    "vat_rate": "20",
    "total_ttc": "1200.00",
    "currency": "MAD",
}
PAYSLIP: dict[str, str | None] = INVOICE | {
    "document_type": "payslip",
    "number": "M6-P-0001",
    "total_ht": "8000.00",
    "vat": "0.00",
    "total_ttc": "8000.00",
}
FILES = {"invoice": "sample-01-fr.pdf", "payslip": "sample-02-ar.pdf"}
COUNT_ATLAS_2026 = {
    "kind": "count",
    "measure": None,
    "supplier": ATLAS,
    "document_type": None,
    "period_from": "2026-01-01",
    "period_to": "2026-12-31",
    "keywords": None,
}
ASK_SETTINGS = AskSettings.model_construct(
    region=REGION, environment="test", ask_model="qwen3:4b", embedding_model="bge-m3"
)


def _log(line: str) -> None:
    print(line, flush=True)  # noqa: T201 - the acceptance log (-s runs)


def _fields(values: dict[str, str | None], ledger_id: uuid.UUID, now: datetime) -> FieldsRow:
    return FieldsRow(
        ledger_id=ledger_id,
        document_type=values["document_type"],
        values={n: values.get(n) for n in FIELD_NAMES},
        confidence=dict.fromkeys(FIELD_NAMES, 0.95),
        signals={n: {"runs_agree": True} for n in FIELD_NAMES},
        source=dict.fromkeys(FIELD_NAMES, "extraction"),
        model="qwen3:4b",
        extracted_at=now,
        updated_at=now,
    )


def _ids(r: httpx.Response) -> list[str]:
    assert r.status_code == 200, r.text
    return [item["id"] for item in r.json()["items"]]


async def test_member_limited_to_invoices_cannot_see_payslips(
    worker_settings: WorkerSettings,
    docker_runtime: DockerRuntime,
    created_workspaces: list[uuid.UUID],
) -> None:
    run = uuid.uuid4().hex[:8]
    engine = make_engine(worker_settings.database_url)
    sessions = make_sessionmaker(engine)
    keys = LocalKeyService(worker_settings.master_key_bytes())
    engines = IndexEngines(worker_settings, keys, sessions)
    store = S3ObjectStore(StorageSettings())
    http = httpx.AsyncClient(base_url=CP_URL, timeout=60)
    member_http = httpx.AsyncClient(base_url=CP_URL, timeout=60)
    try:
        # 1. A fresh workspace and its Owner; a Member limited to invoices, through the API.
        owner = Owner(http, PASSWORD)
        await owner.sign_up(f"m6-{run}@sijil.local", "SAMPLE M6 Owner")
        r = await post(
            http, "/v1/orgs",
            {"name": f"SAMPLE M6 Acceptance {run}", "type": "business", "country": "MA"},
        )  # fmt: skip
        assert r.status_code == 202, r.text
        org_id = r.json()["org"]["id"]
        workspace_id = uuid.UUID(r.json()["workspace"]["id"])
        created_workspaces.append(workspace_id)
        base = f"/v1/workspaces/{workspace_id}"
        member_email = f"m6-member-{run}@sijil.local"
        await Owner(member_http, PASSWORD).sign_up(member_email, "SAMPLE M6 Member")
        await owner.step_up()
        r = await post(
            http, f"/v1/orgs/{org_id}/members",
            {"email": member_email, "role": "member", "document_types": ["invoice"]},
        )  # fmt: skip
        assert r.status_code == 201, r.text
        assert r.json()["document_types"] == ["invoice"]
        started = time.monotonic()
        while (state := (await poll_get(http, base)).json()["state"]) != WorkspaceState.active:
            assert state != WorkspaceState.error
            assert time.monotonic() - started < PROVISION_BUDGET_S, state
            await asyncio.sleep(3)
        _log(f"\nworkspace {workspace_id} active after {time.monotonic() - started:.0f} s")
        async with sessions() as db:
            workspace = await db.get(Workspace, workspace_id)
            member_id = await db.scalar(select(User.id).where(User.email == member_email))
        assert workspace is not None and member_id is not None

        # 2. One invoice and one payslip through the API, classified directly in the index
        # before ingest finishes (extraction then finds them and never runs the model).
        now = datetime.now(UTC)
        docs: dict[str, uuid.UUID] = {}
        values = {"invoice": INVOICE, "payslip": PAYSLIP}
        for kind, name in FILES.items():
            data = (FIXTURES / "invoices" / name).read_bytes()
            r = await http.post(
                f"{base}/documents", files={"file": (name, data, "application/pdf")},
                headers=csrf(http),
            )  # fmt: skip
            assert r.status_code == 202, r.text
            docs[kind] = uuid.UUID(r.json()["document_id"])
            async with (await engines.engine(workspace)).begin() as conn:
                await write_extraction(conn, _fields(values[kind], docs[kind], now))

        async def ledger(ledger_id: uuid.UUID) -> DocumentLedger:
            async with sessions() as db:
                row = await db.get(DocumentLedger, ledger_id, populate_existing=True)
            assert row is not None
            return row

        started = time.monotonic()
        while [i for i in docs.values() if (await ledger(i)).ingest_state != IngestState.consumed]:
            assert time.monotonic() - started < CONSUME_BUDGET_S, "not consumed in time"
            await asyncio.sleep(3)
        for kind, ledger_id in docs.items():
            async with (await engines.engine(workspace)).connect() as conn:
                row = await read_fields(conn, ledger_id)
            assert row is not None
            await _route(sessions, engines, store, workspace, row, datetime.now(UTC))
            routed = await ledger(ledger_id)
            assert routed.state is DocumentState.filed, (kind, routed.state)
            facts = SearchLedgerFacts(
                ledger_id=ledger_id, state=routed.state, ingested_at=routed.ingested_at,
                region=REGION, now=datetime.now(UTC),
            )  # fmt: skip
            async with (await engines.engine(workspace)).begin() as conn:
                content = f"SAMPLE {kind} Atlas {values[kind]['number']}"
                await refresh_search_doc(conn, facts, content)
        invoice, payslip = str(docs["invoice"]), str(docs["payslip"])

        # 3. The Owner sees both; the Member, the invoice only: list, search, by id.
        assert set(_ids(await poll_get(http, f"{base}/documents"))) == {invoice, payslip}
        assert _ids(await poll_get(member_http, f"{base}/documents")) == [invoice]
        assert _ids(await post(member_http, f"{base}/search", {"filters": {}})) == [invoice]
        r = await post(member_http, f"{base}/search", {"filters": {"document_type": ["payslip"]}})
        assert _ids(r) == []
        r = await post(member_http, f"{base}/search", {"filters": {"q": "M6-P-0001"}})
        assert _ids(r) == []
        assert (await member_http.get(f"{base}/documents/{invoice}")).status_code == 200
        hidden = await member_http.get(f"{base}/documents/{payslip}")
        unknown = await member_http.get(f"{base}/documents/{uuid.uuid4()}")
        assert hidden.status_code == 404
        assert hidden.json() == unknown.json()
        r = await member_http.get(f"{base}/documents/{payslip}/fields")
        assert r.status_code == 404

        # 4. Ask, as the API stores the Member's question (with their grant), through the Ask
        # worker's pipeline on the fake model: the payslip is neither counted nor cited, and a
        # question naming payslips gets the answer of no matching documents.
        async def ask(intent: dict[str, Any], answer: str | None) -> dict[str, Any]:
            ask_id = uuid.uuid4()
            async with (await engines.engine(workspace)).begin() as conn:
                await ask_store.create_ask(
                    conn, ask_id, question="Combien de documents Atlas en 2026 ?", locale="fr",
                    created_by=member_id, now=datetime.now(UTC), document_types=["invoice"],
                )  # fmt: skip
            ollama = FakeOllama(intent=intent, answer=answer)
            async with ollama.client() as client:
                await answer_question(
                    ask_id, sessions=sessions, index=engines, ollama=client,
                    settings=ASK_SETTINGS, workspace_id=workspace_id, now=datetime.now(UTC),
                )  # fmt: skip
            r = await poll_get(member_http, f"{base}/ask/{ask_id}")
            assert r.status_code == 200, r.text
            found: dict[str, Any] = r.json()
            return found

        counted = await ask(COUNT_ATLAS_2026, "Un document Atlas en 2026 [#1].")
        _log(f"ask: {counted['answer_text']!r} {counted['figures']}")
        assert counted["figures"] == [
            {"measure": "count", "value": "1", "currency": None, "document_count": 1}
        ]
        assert [c["id"] for c in counted["citations"]] == [invoice]
        assert Decimal(counted["citations"][0]["total_ttc"]) == Decimal("1200.00")
        named = await ask(COUNT_ATLAS_2026 | {"document_type": "payslip"}, None)
        assert (named["answer_text"], named["answer_code"]) == (None, ANSWER_NO_MATCH)
        assert named["figures"] == [] and named["citations"] == []
        assert named["excluded"] == {"needs_review": 0}

        # 5. Exporting is not a Member's.
        r = await post(member_http, f"{base}/exports", {"period": f"{now:%Y-%m}"})
        assert r.status_code == 403, r.text
    finally:
        # Each step on its own; the tenant always goes (the workspace is marked closing by the
        # `created_workspaces` fixture). Originals stay: they are under retention.
        try:
            await http.aclose()
            await member_http.aclose()
            await engines.aclose()
            await engine.dispose()
        finally:
            for created_id in created_workspaces:
                await docker_runtime.destroy(tenant_ref(created_id))
