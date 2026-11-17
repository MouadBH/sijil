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

Billing, then the closure (ADM-3/4/5), on a fresh workspace with one SAMPLE invoice (under
retention):

2. The meter shows the upload (documents and storage).
3. The Owner sets the org's ICE and chooses a plan in the trial. The control plane's clock cannot
   move, so the trial is made to have ended 45 days ago and the daily job's transition
   (`billing.advance`) runs at that moment: the first annual invoice is issued then, in the
   Owner's locale (fr). That call runs in the test runner (`sijil_core` as built into it), not
   in the deployed worker; it is the same function `sijil.billing_daily` runs. It also takes the
   next gapless number with a past issue date, so the dev stack's numbering is not
   chronological. The Owner, now reading Arabic, upgrades through the control plane: the
   prorated invoice is in ar. Both PDFs are served as stored at issue.
4. The annual invoice is unpaid past due: the org is overdue, a new export is refused
   (`billing.overdue`) and an upload still lands.
5. Staff (`sijil-cp staff grant`, TOTP) mark it paid: active again, exporting works.
6. A full export holds both documents. The closure is requested, cancelled, requested again;
   its own full export is built by the export worker.
7. The daily closure job runs in-process with its clock 31 days on and the real Docker runtime:
   the workspace hibernates and its tenant is stopped. The Owner still lists the documents (no
   Paperless: it is never called) and downloads an original from our object store; an Admin
   gets 404; an upload is refused and so is a write (`billing.read_only`).
"""

import asyncio
import hashlib
import io
import json
import time
import uuid
import zipfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import select, update

from sijil_core import ask_store, billing
from sijil_core.crypto import LocalKeyService
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    DocumentLedger,
    DocumentState,
    IngestState,
    Locale,
    Subscription,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.export_report import REPORT_NAME
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow, IndexEngines, read_fields, write_extraction
from sijil_core.retention import region_local_date
from sijil_core.search_index import SearchLedgerFacts, refresh_search_doc
from sijil_core.settings import SellerSettings
from sijil_core.storage import S3ObjectStore, StorageSettings
from sijil_core.tenancy import tenant_ref
from sijil_cp.auth.passwords import hash_password
from sijil_worker.ask import ANSWER_NO_MATCH, answer_question
from sijil_worker.closure import closure_daily
from sijil_worker.extract import _route
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import AskSettings, WorkerSettings
from tests.support.acceptance import Owner, cli, csrf, post
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


async def _db_user(sessions: Any, email: str, name: str) -> None:
    """A user created in the database: the signup rate limit is per IP and hourly."""
    async with sessions() as db:
        db.add(User(email=email, name=name, password_hash=hash_password(PASSWORD)))
        await db.commit()


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
        # Users are created in the database and sign in: the signup rate limit is per IP
        # and hourly.
        owner_email = f"m6-{run}@sijil.local"
        await _db_user(sessions, owner_email, "SAMPLE M6 Owner")
        await owner.sign_in(owner_email)
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
        await _db_user(sessions, member_email, "SAMPLE M6 Member")
        await Owner(member_http, PASSWORD).sign_in(member_email)
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


EXPORT_BUDGET_S = 300
SAMPLE_ORG_ICE = "002999888000011"  # SAMPLE: the org's own (fictitious) ICE
TRANSFER = "VIR-M6-000123"  # SAMPLE: a fictitious bank transfer reference


async def _consumed(sessions: Any, doc_id: str) -> None:
    started = time.monotonic()
    while True:
        async with sessions() as db:
            row = await db.get(DocumentLedger, uuid.UUID(doc_id))
        assert row is not None
        if row.ingest_state == IngestState.consumed:
            return
        assert time.monotonic() - started < CONSUME_BUDGET_S, row.ingest_state
        await asyncio.sleep(3)


async def _upload(http: httpx.AsyncClient, base: str, name: str, data: bytes) -> httpx.Response:
    return await http.post(
        f"{base}/documents", files={"file": (name, data, "application/pdf")}, headers=csrf(http)
    )


async def _ready(http: httpx.AsyncClient, base: str, job: str) -> dict[str, Any]:
    started = time.monotonic()
    while (r := await poll_get(http, f"{base}/exports/{job}")).json()["state"] != "ready":
        assert r.json()["state"] in ("queued", "building"), r.text
        assert time.monotonic() - started < EXPORT_BUDGET_S, r.text
        await asyncio.sleep(3)
    found: dict[str, Any] = r.json()
    return found


async def _report(owner: Owner, base: str, job: str) -> dict[str, Any]:
    """Downloads a ready export (with a step-up) and returns its signed report."""
    await owner.step_up()
    r = await owner.http.get(f"{base}/exports/{job}/download")
    assert r.status_code == 200, r.text
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        report: dict[str, Any] = json.loads(zf.read(REPORT_NAME))
    return report


async def test_billing_then_closure_end_to_end(
    worker_settings: WorkerSettings,
    docker_runtime: DockerRuntime,
    created_workspaces: list[uuid.UUID],
) -> None:
    run = uuid.uuid4().hex[:8]
    engine = make_engine(worker_settings.database_url)
    sessions = make_sessionmaker(engine)
    engines = IndexEngines(
        worker_settings, LocalKeyService(worker_settings.master_key_bytes()), sessions
    )
    http = httpx.AsyncClient(base_url=CP_URL, timeout=60)
    admin_http = httpx.AsyncClient(base_url=CP_URL, timeout=60)
    staff_http = httpx.AsyncClient(base_url=CP_URL, timeout=60)
    try:
        # 1. A fresh workspace, its Owner and an Admin; one SAMPLE invoice through the API.
        # Every user of this test is created in the database and signs in (the signup rate
        # limit is per IP and hourly: test 1 and other runs spend it).
        owner = Owner(http, PASSWORD)
        owner_email = f"m6-close-{run}@sijil.local"
        await _db_user(sessions, owner_email, "SAMPLE M6 Owner")
        await owner.sign_in(owner_email)
        r = await post(
            http, "/v1/orgs",
            {"name": f"SAMPLE M6 Closure {run}", "type": "business", "country": "MA"},
        )  # fmt: skip
        assert r.status_code == 202, r.text
        org_id = r.json()["org"]["id"]
        org = f"/v1/orgs/{org_id}"
        workspace_id = uuid.UUID(r.json()["workspace"]["id"])
        created_workspaces.append(workspace_id)
        base = f"/v1/workspaces/{workspace_id}"
        # The Admin is created in the database, added by the Owner over the API, and signs in.
        admin_email = f"m6-close-admin-{run}@sijil.local"
        await _db_user(sessions, admin_email, "SAMPLE M6 Admin")
        await owner.step_up()
        r = await post(http, f"{org}/members", {"email": admin_email, "role": "admin"})
        assert r.status_code == 201, r.text
        (await admin_http.get("/v1/auth/csrf")).raise_for_status()
        r = await post(admin_http, "/v1/auth/login", {"email": admin_email, "password": PASSWORD})
        assert r.status_code == 200, r.text
        started = time.monotonic()
        while (state := (await poll_get(http, base)).json()["state"]) != WorkspaceState.active:
            assert state != WorkspaceState.error
            assert time.monotonic() - started < PROVISION_BUDGET_S, state
            await asyncio.sleep(3)
        async with sessions() as db:
            workspace = await db.get(Workspace, workspace_id)
        assert workspace is not None

        async def classified(name: str, data: bytes, values: dict[str, str | None]) -> str:
            """Uploaded, then classified in the index before ingest finishes (no model)."""
            r = await _upload(http, base, name, data)
            assert r.status_code == 202, r.text
            found = r.json()["document_id"]
            async with (await engines.engine(workspace)).begin() as conn:
                await write_extraction(conn, _fields(values, uuid.UUID(found), datetime.now(UTC)))
            await _consumed(sessions, found)
            return str(found)

        data = (FIXTURES / "invoices" / FILES["invoice"]).read_bytes()
        doc_id = await classified("facture.pdf", data, INVOICE)

        # 2. The meter shows the upload.
        r = await poll_get(http, f"{org}/usage")
        assert r.status_code == 200, r.text
        quotas = r.json()["quotas"]
        assert (quotas["documents"]["used"], quotas["storage"]["used"]) == (1, len(data))
        _log(f"\nusage: {quotas}")

        # 3. The org's ICE, then a plan chosen in the trial (the first year starts on it).
        r = await http.patch(org, json={"ice": SAMPLE_ORG_ICE}, headers=csrf(http))
        assert r.status_code == 200, r.text
        await owner.step_up()
        r = await http.put(
            f"{org}/subscription", json={"plan_code": "essentiel"}, headers=csrf(http)
        )
        assert (r.status_code, r.json()["status"]) == (200, "trial"), r.text
        # The control plane's clock cannot move: the trial is made to have ended 45 days ago,
        # and the daily job's transition (`billing.advance`) runs at that moment, issuing the
        # first annual invoice then, in the Owner's locale (fr). Its 30 days to pay are over.
        trial_end = datetime.now(UTC) - timedelta(days=45)
        async with sessions() as db:
            sub = await db.scalar(
                select(Subscription).where(Subscription.org_id == uuid.UUID(org_id))
            )
            assert sub is not None
            sub.trial_ends_at, sub.period_end = trial_end, trial_end
            sub.period_start = trial_end - timedelta(days=billing.TRIAL_DAYS)
            await db.commit()
            await billing.advance(db, uuid.UUID(org_id), trial_end, seller=SellerSettings())
            await db.commit()
            # The Owner now reads Arabic (no route sets a locale yet): the next invoice is in ar.
            owner_id = (await http.get("/v1/me")).json()["id"]
            await db.execute(
                update(User).where(User.id == uuid.UUID(owner_id)).values(locale=Locale.ar)
            )
            await db.commit()
        await owner.step_up()
        r = await http.put(f"{org}/subscription", json={"plan_code": "pro"}, headers=csrf(http))
        assert r.status_code == 200, r.text
        assert (r.json()["plan_code"], r.json()["status"]) == ("pro", "overdue")
        r = await poll_get(http, f"{org}/invoices")
        assert r.status_code == 200, r.text
        invoices = {i["kind"]: i for i in r.json()}
        assert set(invoices) == {"annual", "upgrade"}
        annual, upgrade = invoices["annual"], invoices["upgrade"]
        assert (annual["locale"], upgrade["locale"]) == ("fr", "ar")
        assert (annual["state"], annual["plan_code"]) == ("overdue", "essentiel")
        assert (upgrade["state"], upgrade["plan_code"]) == ("issued", "pro")
        assert annual["buyer"]["ice"] == SAMPLE_ORG_ICE
        for invoice in (annual, upgrade):
            amounts = [Decimal(invoice[k]) for k in ("amount_ht", "amount_vat", "amount_ttc")]
            assert amounts[0] + amounts[1] == amounts[2] and amounts[2] > 0
            r = await http.get(f"{org}/invoices/{invoice['id']}/pdf")
            assert r.status_code == 200, r.text
            assert r.headers["content-type"] == "application/pdf"
            assert r.content.startswith(b"%PDF")
            assert hashlib.sha256(r.content).hexdigest() == invoice["pdf_sha256"]
        _log(f"invoices: {annual['number']} fr, {upgrade['number']} ar")

        # 4. Overdue: a new export is refused, capture still lands.
        r = await poll_get(http, f"{org}/subscription")
        assert r.json()["status"] == "overdue", r.text
        await owner.step_up()
        r = await post(http, f"{base}/exports", {"period": "all"})
        assert (r.status_code, r.json()["error"]["code"]) == (402, "billing.overdue"), r.text
        second = (FIXTURES / "invoices" / "sample-03-fr.pdf").read_bytes()
        second_id = await classified("facture-2.pdf", second, INVOICE | {"number": "M6-F-0002"})

        # 5. Staff (granted by `sijil-cp staff grant`, TOTP enrolled) record the transfer: the
        # subscription is active at once, and exporting works again.
        staff_email = f"m6-staff-{run}@sijil.local"
        await _db_user(sessions, staff_email, "SAMPLE M6 Staff")
        code, out = await cli("staff", "grant", staff_email)
        assert code == 0, out
        staff = Owner(staff_http, PASSWORD)
        await staff.sign_in(staff_email)
        r = await poll_get(staff_http, "/v1/staff/invoices?state=overdue")
        assert r.status_code == 200, r.text
        assert annual["id"] in [i["id"] for i in r.json()]
        paid_on = region_local_date(datetime.now(UTC), REGION).isoformat()
        r = await post(
            staff_http, f"/v1/staff/invoices/{annual['id']}/mark-paid",
            {"payment_reference": TRANSFER, "paid_on": paid_on},
        )  # fmt: skip
        assert r.status_code == 200, r.text
        assert (r.json()["invoice"]["state"], r.json()["subscription"]["status"]) == (
            "paid", "active",
        )  # fmt: skip
        r = await poll_get(http, f"{org}/subscription")
        assert r.json()["status"] == "active", r.text

        # 6. A full export of every period; then the closure: requested, cancelled, requested
        # again, its own full export built by the export worker.
        await owner.step_up()
        r = await post(http, f"{base}/exports", {"period": "all"})
        assert r.status_code == 202, r.text
        assert (await _ready(http, base, r.json()["id"]))["document_count"] == 2
        report = await _report(owner, base, r.json()["id"])
        assert report["period"] == "all"
        assert {d["id"] for d in report["documents"]} == {doc_id, second_id}
        await owner.step_up()
        r = await post(http, f"{org}/closure", None)
        assert (r.status_code, r.json()["state"]) == (201, "grace"), r.text
        first_closure = r.json()["id"]
        await owner.step_up()
        r = await http.delete(f"{org}/closure", headers=csrf(http))
        assert (r.status_code, r.json()["state"]) == (200, "cancelled"), r.text
        assert (await poll_get(http, org)).json()["status"] == "active"
        await owner.step_up()
        r = await post(http, f"{org}/closure", None)
        assert (r.status_code, r.json()["state"]) == (201, "grace"), r.text
        assert r.json()["id"] != first_closure
        job = r.json()["export_job_id"]
        assert (await _ready(http, base, job))["document_count"] == 2
        report = await _report(owner, base, job)
        assert report["period"] == "all"
        assert {d["id"] for d in report["documents"]} == {doc_id, second_id}

        # 7. The grace period over (the job's clock 31 days on): hibernated, the tenant stopped.
        removals: list[uuid.UUID] = []

        async def send_removal(ledger_id: uuid.UUID, _workspace_id: uuid.UUID) -> None:
            removals.append(ledger_id)

        outcome = await closure_daily(
            sessions, runtime=docker_runtime, locks=S3ObjectStore(StorageSettings()),
            settings=worker_settings, send_removal=send_removal,
            now=datetime.now(UTC) + timedelta(days=31),
        )  # fmt: skip
        _log(f"closure: {outcome}")
        # The job serves the whole region (closures other runs left count too): this
        # workspace's outcome is read from its own state.
        assert outcome.hibernated >= 1
        assert not {uuid.UUID(doc_id), uuid.UUID(second_id)} & set(removals)
        assert await docker_runtime.status(tenant_ref(workspace_id)) == "stopped"
        r = await poll_get(http, base)
        assert r.json()["state"] == WorkspaceState.hibernated

        # The Owner reads the retained documents; everyone else gets 404; capture and writes
        # are refused.
        r = await poll_get(http, f"{base}/documents")
        assert (set(_ids(r)), r.json()["engine"]) == ({doc_id, second_id}, "unavailable")
        r = await http.get(f"{base}/documents/{doc_id}/file")
        assert r.status_code == 200, r.text
        assert r.content == data
        for path in ("/documents", f"/documents/{doc_id}", f"/documents/{doc_id}/file"):
            assert (await admin_http.get(base + path)).status_code == 404, path
        r = await _upload(http, base, "autre.pdf", data + b"\n")
        assert r.status_code == 409, r.text
        r = await http.put(
            f"{base}/retention/policies/invoice", json={"years": 12}, headers=csrf(http)
        )
        assert (r.status_code, r.json()["error"]["code"]) == (402, "billing.read_only")
    finally:
        try:
            await http.aclose()
            await admin_http.aclose()
            await staff_http.aclose()
            await engines.aclose()
            await engine.dispose()
        finally:
            for created_id in created_workspaces:
                await docker_runtime.destroy(tenant_ref(created_id))
