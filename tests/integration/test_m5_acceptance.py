"""M5 acceptance: search, saved views, tags, Ask and learning, end to end against the compose
stack, with no model (`make itest`).

A fresh workspace (marked `closing` at teardown through `created_workspaces`; the shared SAMPLE
workspace is never touched) gets the 12 SAMPLE invoices through the API. Their fields are
written straight into its index from the manifest before ingest finishes, as M4's acceptance
does, so the model never extracts them; they are routed by the worker's own routing, and two are
sealed (here only). `sijil-cp search backfill` rebuilds the search index, text included.

Search then finds an amount range, the Arabic supplier written with a bare alef and a tatweel,
and a French word typed without its accents, each exactly the documents the manifest says; a
saved view round-trips; a sealed document is tagged (its seal unchanged) and found by its tag;
an unsealed one removed by the Owner leaves search at once and `search_doc` after its purge.

Ask runs the Ask worker's own pipeline (`answer_question`) on a fake in-region model
(tests/support/fake_ollama.py) for a question stored as the API stores it: the VAT paid to
Atlas Télécom in 2026 equals the manifest's Decimal sum and cites that supplier's invoices,
read back through the API. The real model's answers are `test_ask_model.py` (`make acceptance`).

Learning: the model misread a supplier's name on an invoice (its runs are stored as extraction
leaves them); a reviewer corrects the name; the next upload of that supplier's ICE, extracted
by the worker's pipeline on the fake model reading the same misspelling, is hinted with the
corrected name and takes it, with no `supplier_mismatch`. Last, the audit chain verifies.
"""

import asyncio
import json
import time
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import select, text

from sijil_core import ask_store, index, suppliers
from sijil_core.confidence import SUPPLIER_MISMATCH
from sijil_core.crypto import Envelope, LocalKeyService, open_envelope
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    CredentialKind,
    DocumentLedger,
    DocumentState,
    IngestState,
    TenantCredential,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow, IndexEngines, read_fields, write_extraction
from sijil_core.paperless import PaperlessClient
from sijil_core.storage import S3ObjectStore, StorageSettings
from sijil_core.tenancy import tenant_ref
from sijil_worker.ask import answer_question
from sijil_worker.extract import SUPPLIER_RECORD, _extract, _Job, _route
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import AskSettings, WorkerSettings
from tests.support.acceptance import Owner, cli, csrf, post
from tests.support.fake_ollama import FakeOllama
from tests.support.polling import poll_get

AttachRunner = Callable[[str], AbstractContextManager[None]]

pytestmark = pytest.mark.integration

CP_URL = "http://control-plane:8000"
REGION = "ma-cas"
PASSWORD = "it-m5-acceptance-Passw0rd"  # noqa: S105 - a throw-away SAMPLE account
PROVISION_BUDGET_S = 240
CONSUME_BUDGET_S = 600
INDEX_BUDGET_S = 300
PURGE_BUDGET_S = 180
LEARN_BUDGET_S = 120
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
MANIFEST: list[dict[str, Any]] = json.loads((FIXTURES / "invoices" / "manifest.json").read_text())
SUPPLIERS = {s["ice"]: s for s in json.loads((FIXTURES / "suppliers.json").read_text())}
ATLAS = "001905837000041"  # SAMPLE: Atlas Télécom Services SARL (fictitious)
HYDRO = "002118460000072"  # SAMPLE: Société Hydro Souss SA (fictitious)
SEALED = ("sample-01-fr.pdf", "sample-09-fr.pdf")
TAGGED = "sample-01-fr.pdf"
REMOVED = "sample-12-ar.pdf"
MISREAD = "sample-03-fr.pdf"  # the model misread its supplier's name
MISSPELLING = "Ste Hydro Sous"  # SAMPLE: what the model read
REMOVAL_REASON = "Facture déposée dans le mauvais espace (SAMPLE)"
TAG = "SAMPLE Audit 2026"
QUESTION = "Combien de TVA avons-nous payée à Atlas Télécom Services en 2026 ?"
VAT_ATLAS_2026 = {
    "kind": "sum",
    "measure": "vat",
    "supplier": "Atlas Télécom Services",
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


def _read(item: dict[str, Any], name: str | None = None) -> dict[str, str | None]:
    """The invoice's fields as printed (the Arabic ones name the supplier in Arabic)."""
    supplier = SUPPLIERS[item["supplier_ice"]]
    printed = supplier["name_ar"] if item["lang"] == "ar" else supplier["name"]
    return {
        "document_type": "invoice",
        "supplier_name": name or printed,
        "supplier_ice": item["supplier_ice"],
        "date": item["date"],
        "number": item["number"],
        "total_ht": item["ht"],
        "vat": item["tva"],
        "vat_rate": str(supplier["vat_rate"]),
        "total_ttc": item["ttc"],
        "currency": item["currency"],
    }


def _fields(values: dict[str, str | None], ledger_id: uuid.UUID, now: datetime) -> FieldsRow:
    """Complete and confident, as an extraction that went well would leave them."""
    return FieldsRow(
        ledger_id=ledger_id,
        document_type="invoice",
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


async def test_m5_acceptance(
    worker_settings: WorkerSettings,
    docker_runtime: DockerRuntime,
    attach_runner: AttachRunner,
    created_workspaces: list[uuid.UUID],
) -> None:
    run = uuid.uuid4().hex[:8]
    engine = make_engine(worker_settings.database_url)
    sessions = make_sessionmaker(engine)
    keys = LocalKeyService(worker_settings.master_key_bytes())
    engines = IndexEngines(worker_settings, keys, sessions)
    store = S3ObjectStore(StorageSettings())
    http = httpx.AsyncClient(base_url=CP_URL, timeout=60)
    try:
        # 1. A fresh workspace, its Owner with TOTP.
        owner = Owner(http, PASSWORD)
        email = f"m5-{run}@sijil.local"
        await owner.sign_up(email, "SAMPLE M5 Owner")
        r = await post(
            http, "/v1/orgs",
            {"name": f"SAMPLE M5 Acceptance {run}", "type": "business", "country": "MA"},
        )  # fmt: skip
        assert r.status_code == 202, r.text
        workspace_id = uuid.UUID(r.json()["workspace"]["id"])
        created_workspaces.append(workspace_id)
        base = f"/v1/workspaces/{workspace_id}"
        started = time.monotonic()
        while (state := (await poll_get(http, base)).json()["state"]) != WorkspaceState.active:
            assert state != WorkspaceState.error
            assert time.monotonic() - started < PROVISION_BUDGET_S, state
            await asyncio.sleep(3)
        _log(f"\nworkspace {workspace_id} active after {time.monotonic() - started:.0f} s")
        async with sessions() as db:
            workspace = await db.get(Workspace, workspace_id)
            owner_id = await db.scalar(select(User.id).where(User.email == email))
        assert workspace is not None and workspace.paperless_url is not None
        assert owner_id is not None

        # 2. The 12 SAMPLE invoices through the API; 3. their fields written into the index
        # before ingest finishes (extraction then finds them and never runs the model).
        now = datetime.now(UTC)
        docs: dict[str, uuid.UUID] = {}
        for item in MANIFEST:
            data = (FIXTURES / "invoices" / item["file"]).read_bytes()
            r = await http.post(
                f"{base}/documents", files={"file": (item["file"], data, "application/pdf")},
                headers=csrf(http),
            )  # fmt: skip
            assert r.status_code == 202, r.text
            ledger_id = uuid.UUID(r.json()["document_id"])
            docs[item["file"]] = ledger_id
            misread = item["file"] == MISREAD
            values = _read(item, MISSPELLING if misread else None)
            async with (await engines.engine(workspace)).begin() as conn:
                if misread:  # both runs read the misspelling, as extraction records them
                    for name in ("a", "b"):
                        await index.append_run(conn, ledger_id, name, "qwen3:4b", values, now)
                await write_extraction(conn, _fields(values, ledger_id, now))
        by_id = {str(v): k for k, v in docs.items()}

        async def ledger(ledger_id: uuid.UUID) -> DocumentLedger:
            async with sessions() as db:
                row = await db.get(DocumentLedger, ledger_id, populate_existing=True)
            assert row is not None
            return row

        async def consumed(ids: list[uuid.UUID]) -> None:
            started = time.monotonic()
            while [i for i in ids if (await ledger(i)).ingest_state is not IngestState.consumed]:
                assert time.monotonic() - started < CONSUME_BUDGET_S, "not consumed in time"
                await asyncio.sleep(3)
            _log(f"{len(ids)} consumed after {time.monotonic() - started:.0f} s")

        async def route(ledger_id: uuid.UUID) -> DocumentLedger:
            """Routed by the worker's own routing (the extraction worker may get there
            first)."""
            async with (await engines.engine(workspace)).connect() as conn:
                row = await read_fields(conn, ledger_id)
            assert row is not None
            await _route(sessions, engines, store, workspace, row, datetime.now(UTC))
            return await ledger(ledger_id)

        await consumed(list(docs.values()))
        states = {name: (await route(i)).state for name, i in docs.items()}
        assert set(states.values()) == {DocumentState.filed}, states
        for name in SEALED:
            r = await http.post(f"{base}/documents/{docs[name]}/seal", headers=csrf(http))
            assert r.status_code == 200, r.text
            assert r.json()["state"] == DocumentState.sealed

        # 4. The search index rebuilt by the operator's command.
        code, out = await cli("search", "backfill", str(workspace_id))
        assert code == 0, out
        started = time.monotonic()
        wanted = {i: (await ledger(i)).state.value for i in docs.values()}
        while True:
            async with (await engines.engine(workspace)).connect() as conn:
                rows = (
                    await conn.execute(
                        text(
                            "SELECT ledger_id, state FROM search_doc "
                            "WHERE ledger_id = ANY(:ids) AND content <> ''"
                        ),
                        {"ids": list(docs.values())},
                    )
                ).all()
            if dict(rows) == wanted:
                break
            assert time.monotonic() - started < INDEX_BUDGET_S, f"not indexed: {len(rows)}"
            await asyncio.sleep(3)
        _log(f"search index rebuilt after {time.monotonic() - started:.0f} s")

        # 5. Search: an amount range, an Arabic supplier variant, a French word without
        # accents; each returns exactly the documents the manifest says.
        low, high = Decimal("1000"), Decimal("2200")
        in_range = {docs[i["file"]] for i in MANIFEST if low <= Decimal(i["ttc"]) <= high}
        assert len(in_range) == 4
        r = await post(
            http, f"{base}/search", {"filters": {"amount_min": "1000", "amount_max": "2200"}}
        )
        assert set(_ids(r)) == {str(i) for i in in_range}, [by_id[i] for i in _ids(r)]
        # أطلس written with a bare alef and a tatweel.
        r = await post(http, f"{base}/search", {"filters": {"supplier": "اطلـس"}})
        assert _ids(r) == [str(docs["sample-02-ar.pdf"])], [by_id[i] for i in _ids(r)]
        # Riad Tensift Hôtellerie SARL, typed without its accent and in capitals.
        r = await post(http, f"{base}/search", {"filters": {"q": "HOTELLERIE"}})
        assert _ids(r) == [str(docs["sample-09-fr.pdf"])], [by_id[i] for i in _ids(r)]

        # 6. A saved view round-trips: saved, listed as saved, and its search is the same.
        filters = {"supplier": ATLAS, "period_from": "2026-01-01", "period_to": "2026-12-31"}
        r = await post(
            http, f"{base}/search/views",
            {"name": "SAMPLE Atlas 2026", "filters": filters, "sort": "amount_desc"},
        )  # fmt: skip
        assert r.status_code == 201, r.text
        view_id = r.json()["id"]
        r = await poll_get(http, f"{base}/search/views")
        assert r.status_code == 200, r.text
        [view] = [v for v in r.json()["items"] if v["id"] == view_id]
        kept = {k: v for k, v in view["filters"].items() if v not in (None, "", [])}
        assert (view["name"], kept, view["sort"]) == ("SAMPLE Atlas 2026", filters, "amount_desc")
        r = await post(http, f"{base}/search", {"filters": view["filters"], "sort": view["sort"]})
        atlas = [str(docs["sample-01-fr.pdf"]), str(docs["sample-02-ar.pdf"])]  # by amount
        assert _ids(r) == atlas

        # 7. A sealed document tagged: its seal unchanged, and found by its tag.
        tagged = docs[TAGGED]
        seal = (await ledger(tagged)).fields_sha256
        assert seal is not None
        r = await http.put(
            f"{base}/documents/{tagged}/tags", json={"tags": [TAG]}, headers=csrf(http)
        )
        assert r.status_code == 200, r.text
        assert r.json()["tags"] == [TAG.casefold()]
        assert (await ledger(tagged)).fields_sha256 == seal
        r = await post(http, f"{base}/search", {"filters": {"tags": [TAG]}})
        assert _ids(r) == [str(tagged)]

        # 8. An unsealed document removed: gone from search at once, from `search_doc` after
        # the purge.
        removed = docs[REMOVED]
        number = next(i["number"] for i in MANIFEST if i["file"] == REMOVED)
        r = await post(http, f"{base}/search", {"filters": {"q": number}})
        assert _ids(r) == [str(removed)]
        await owner.step_up()
        r = await post(http, f"{base}/documents/{removed}/remove", {"reason": REMOVAL_REASON})
        assert r.status_code == 202, r.text
        removed_at = time.monotonic()
        r = await post(http, f"{base}/search", {"filters": {"q": number}})
        assert _ids(r) == []
        while True:
            async with (await engines.engine(workspace)).connect() as conn:
                left = await conn.scalar(
                    text("SELECT count(*) FROM search_doc WHERE ledger_id = :id"), {"id": removed}
                )
            if left == 0:
                break
            assert time.monotonic() - removed_at < PURGE_BUDGET_S, "search row not purged"
            await asyncio.sleep(2)
        _log(f"search row purged {time.monotonic() - removed_at:.0f} s after the removal")

        # 9. Ask, through the Ask worker's pipeline on the fake model: the figure is the
        # manifest's sum, the citations are that supplier's documents.
        expected = sum(
            (Decimal(i["tva"]) for i in MANIFEST
             if i["supplier_ice"] == ATLAS and i["date"].startswith("2026-")),
            Decimal("0"),
        )  # fmt: skip
        assert expected == Decimal("429.80")
        written = "La TVA payée à Atlas Télécom Services en 2026 est de 429,80 MAD [#1] [#2]."
        ask_id = uuid.uuid4()
        async with (await engines.engine(workspace)).begin() as conn:
            await ask_store.create_ask(
                conn, ask_id, question=QUESTION, locale="fr", created_by=owner_id,
                now=datetime.now(UTC),
            )  # fmt: skip
        ollama = FakeOllama(intent=VAT_ATLAS_2026, answer=written)
        async with ollama.client() as client:
            await answer_question(
                ask_id, sessions=sessions, index=engines, ollama=client, settings=ASK_SETTINGS,
                workspace_id=workspace_id, now=datetime.now(UTC),
            )  # fmt: skip
        r = await poll_get(http, f"{base}/ask/{ask_id}")
        assert r.status_code == 200, r.text
        answer = r.json()
        _log(f"ask: {answer['answer_text']!r} {answer['figures']}")
        assert answer["state"] == "answered", answer
        assert answer["figures"] == [
            {"measure": "vat", "value": str(expected), "currency": "MAD", "document_count": 2}
        ]
        assert [c["id"] for c in answer["citations"]] == atlas
        assert (answer["answer_text"], answer["answer_code"]) == (written, None)

        # 10. A corrected supplier name is recognised on the next upload of the same ICE.
        misread = docs[MISREAD]
        corrected = SUPPLIERS[HYDRO]["name"]
        r = await http.patch(
            f"{base}/documents/{misread}/fields",
            json={"fields": {"supplier_name": corrected}}, headers=csrf(http),
        )  # fmt: skip
        assert r.status_code == 200, r.text
        started = time.monotonic()
        while True:
            async with (await engines.engine(workspace)).connect() as conn:
                record = await suppliers.get(conn, HYDRO)
            if record is not None and record.name == corrected and record.aliases:
                break
            assert time.monotonic() - started < LEARN_BUDGET_S, f"not learnt: {record}"
            await asyncio.sleep(2)
        assert record.aliases == (MISSPELLING,), record
        _log(f"supplier learnt after {time.monotonic() - started:.0f} s: {record.name}")

        item = next(i for i in MANIFEST if i["file"] == MISREAD)
        data = (FIXTURES / "invoices" / MISREAD).read_bytes()
        data += f"\n%sijil-m5-{uuid.uuid4()}\n".encode()  # a new upload of the same invoice
        r = await http.post(
            f"{base}/documents", files={"file": ("next-" + MISREAD, data, "application/pdf")},
            headers=csrf(http),
        )  # fmt: skip
        assert r.status_code == 202, r.text
        upload = uuid.UUID(r.json()["document_id"])
        # Extracted before its ingest ends, from the text Paperless read on the same invoice.
        source = await ledger(misread)
        assert source.paperless_doc_id is not None
        async with sessions() as db:
            credential = await db.scalar(
                select(TenantCredential).where(
                    TenantCredential.workspace_id == workspace_id,
                    TenantCredential.kind == CredentialKind.service_token,
                )
            )
        assert credential is not None
        token = open_envelope(
            Envelope.from_bytes(credential.envelope), keys,
            f"{workspace_id}:{CredentialKind.service_token}",
        ).decode()  # fmt: skip
        job = _Job(upload, workspace, REGION, source.paperless_doc_id, workspace.paperless_url,
                   token)  # fmt: skip
        reader = FakeOllama(extraction=_read(item, MISSPELLING))
        extracted_at = datetime.now(UTC)
        with attach_runner(docker_runtime.network_name(tenant_ref(workspace_id))):
            async with reader.client() as client:
                row, _ = await _extract(
                    job, index=engines, paperless=PaperlessClient, ollama=client,
                    model="qwen3:4b", now=extracted_at,
                )  # fmt: skip
        assert row.extracted_at == extracted_at, "the extraction worker got there first"
        hint = f"Known company in this workspace: {corrected} (ICE {HYDRO})"
        assert [hint in prompt for prompt in reader.system_prompts()] == [True, True]
        assert row.values["supplier_name"] == corrected
        assert row.source["supplier_name"] == SUPPLIER_RECORD
        assert row.signals["supplier_name"]["supplier_known"] is True
        assert SUPPLIER_MISMATCH not in row.flags
        await consumed([upload])
        routed = await route(upload)
        assert routed.state in (DocumentState.filed, DocumentState.needs_review)
        async with (await engines.engine(workspace)).connect() as conn:
            stored = await read_fields(conn, upload)
        assert stored is not None
        assert SUPPLIER_MISMATCH not in stored.flags, stored.flags
        _log(f"next upload routed {routed.state.value}, flags {stored.flags}")

        # 11. The audit chain verifies.
        code, out = await cli("audit", "verify", "--workspace", str(workspace_id))
        assert code == 0, out
        assert out.startswith("ok "), out
    finally:
        # Each step on its own; the tenant always goes (the workspace is marked closing by the
        # `created_workspaces` fixture). Originals stay: they are under retention.
        try:
            await http.aclose()
            await engines.aclose()
            await engine.dispose()
        finally:
            for created_id in created_workspaces:
                await docker_runtime.destroy(tenant_ref(created_id))
