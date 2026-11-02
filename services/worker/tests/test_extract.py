"""Extraction of a consumed document: Paperless content -> two Ollama runs -> the index row.

Paperless is respx, Ollama an httpx MockTransport, the index a throw-away database reached
through a real `IndexEngines` (its credential sealed like provisioning does).
"""

import hashlib
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, get_args

import httpx
import pytest
import respx
from sqlalchemy import select, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sijil_core import index
from sijil_core.crypto import LocalKeyService, seal
from sijil_core.db.models import (
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    Organisation,
    OrgType,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.fields import DOCUMENT_TYPES, FIELD_NAMES
from sijil_core.index import FieldsRow, IndexEngines
from sijil_core.ollama import OllamaClient, OllamaUnavailable
from sijil_core.paperless import PaperlessClient, PaperlessError, PaperlessUnavailable
from sijil_core.storage import ObjectRef
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.extract import (
    INGEST_DEFER_WINDOW,
    PROMPT_A,
    PROMPT_B,
    IngestInProgress,
    RawExtraction,
    extract_document,
    extraction_schema,
    record_extraction_failed,
    requeue_unextracted,
)
from sijil_worker.settings import WorkerSettings
from sijil_worker.taxonomy import RULES, ensure_taxonomy
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

PAPERLESS = "http://paperless-ws:8000"
OLLAMA = "http://ollama:11434"
TOKEN = "tok-" + "e" * 36
DOC_ID = 7
NOW = datetime(2026, 10, 2, 9, 30, tzinfo=UTC)
SETTINGS = WorkerSettings.model_construct(region="ma-cas", environment="test")
FIXTURES = Path(__file__).parents[3] / "fixtures"
MANIFEST = {i["file"]: i for i in json.loads((FIXTURES / "invoices" / "manifest.json").read_text())}
SUPPLIERS = {s["ice"]: s for s in json.loads((FIXTURES / "suppliers.json").read_text())}
SAMPLE_01 = MANIFEST["sample-01-fr.pdf"]
SUPPLIER_01 = SUPPLIERS[SAMPLE_01["supplier_ice"]]

# SAMPLE: what Paperless gives for fixtures/invoices/sample-01-fr.pdf (fictitious company).
CONTENT_01 = (
    "SAMPLE / ÉCHANTILLON\n"
    f"{SUPPLIER_01['name']}\nCasablanca\nICE : {SUPPLIER_01['ice']}\n"
    f"FACTURE N° {SAMPLE_01['number']}\nDate : {SAMPLE_01['date']}\n"
    "Client : Entreprise Exemple SARL — Casablanca\n"
    f"Désignation : {SAMPLE_01['item']}\n"
    "Total HT 1 250,00 MAD\nTVA 20 % 250,00 MAD\nTotal TTC 1 500,00 MAD\n"
)
# SAMPLE: the values a model copies from it, as printed.
RAW_01: dict[str, str | None] = {
    "document_type": "invoice",
    "supplier_name": SUPPLIER_01["name"],
    "supplier_ice": SUPPLIER_01["ice"],
    "date": SAMPLE_01["date"],
    "number": SAMPLE_01["number"],
    "total_ht": "1 250,00 MAD",
    "vat": "250,00 MAD",
    "vat_rate": "20 %",
    "total_ttc": "1 500,00 MAD",
    "currency": "MAD",
}
CANONICAL_01 = {
    "document_type": "invoice",
    "supplier_name": SUPPLIER_01["name"],
    "supplier_ice": SAMPLE_01["supplier_ice"],
    "date": SAMPLE_01["date"],
    "number": SAMPLE_01["number"],
    "total_ht": SAMPLE_01["ht"],
    "vat": SAMPLE_01["tva"],
    "vat_rate": str(SUPPLIER_01["vat_rate"]),
    "total_ttc": SAMPLE_01["ttc"],
    "currency": SAMPLE_01["currency"],
}
TYPE_IDS = {name: n + 1 for n, name in enumerate(DOCUMENT_TYPES)}
PROVISIONAL = date(2036, 12, 31)  # arrival in 2026, MA


class FakeStore:
    """Records every retention extension; `observe` runs inside the call (to see what the
    ledger holds at that moment)."""

    def __init__(self, observe: Callable[[], Awaitable[Any]] | None = None) -> None:
        self.extended: list[tuple[ObjectRef, date]] = []
        self.observed: list[Any] = []
        self.observe = observe
        self.fail: Exception | None = None

    async def extend_retention(self, ref: ObjectRef, retain_until: date) -> None:
        if self.observe is not None:
            self.observed.append(await self.observe())
        if self.fail is not None:
            raise self.fail
        self.extended.append((ref, retain_until))


# Fixtures ---------------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sijil_core.paperless._BACKOFF_S", 0)


@pytest.fixture
def keys() -> LocalKeyService:
    return LocalKeyService(b"k" * 32)


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


@pytest.fixture
def index_db_url() -> Iterator[str]:
    """One per test: duplicate detection looks at every row of the workspace's index."""
    yield from throwaway_index_db()


def _credential(ws: Workspace, keys: LocalKeyService, kind: CredentialKind, secret: str) -> Any:
    envelope = seal(secret.encode(), keys, ws.kms_key_id, f"{ws.id}:{kind}")
    return TenantCredential(workspace_id=ws.id, kind=kind, envelope=envelope.to_bytes())


@pytest.fixture
async def ready(db: AsyncSession, workspace: Workspace, keys: LocalKeyService) -> Workspace:
    """Active, with its Paperless URL, service token and index database password."""
    workspace.paperless_url = PAPERLESS
    db.add(_credential(workspace, keys, CredentialKind.service_token, TOKEN))
    db.add(_credential(workspace, keys, CredentialKind.index_db_password, "pw"))
    await db.commit()
    return workspace


@pytest.fixture
async def engines(
    index_db_url: str, keys: LocalKeyService, sessions: async_sessionmaker[AsyncSession]
) -> AsyncIterator[IndexEngines]:
    """The real engine cache, with every workspace's database being the throw-away one."""

    def factory(_url: URL) -> AsyncEngine:
        return create_async_engine(index_db_url)

    found = IndexEngines(SETTINGS, keys, sessions, engine_factory=factory)
    yield found
    await found.aclose()


async def make_ledger(
    db: AsyncSession,
    workspace: Workspace,
    *,
    state: DocumentState = DocumentState.received,
    ingest_state: IngestState = IngestState.consumed,
    age: timedelta = timedelta(0),
    ingested_at: datetime | None = None,
    retention_until: date = PROVISIONAL,
    sha: str | None = None,
) -> DocumentLedger:
    sha = sha or hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    row = DocumentLedger(
        workspace_id=workspace.id,
        sha256=sha,
        md5="0" * 32,
        size_bytes=10,
        content_kind="pdf",
        source=DocumentSource.upload,
        object_key=f"k/{sha}",
        object_version_id="v1",
        state=state,
        ingest_state=ingest_state,
        paperless_doc_id=DOC_ID if ingest_state is IngestState.consumed else None,
        retention_until=retention_until,
        ingested_at=ingested_at or NOW - age,
    )
    db.add(row)
    await db.commit()
    return row


class FakeOllama:
    """Answers by seed (1 = run A, 2 = run B); records every request."""

    def __init__(self, answers: dict[int, Any]) -> None:
        self.answers = answers
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        body = json.loads(request.content)
        answer = self.answers[body["options"]["seed"]]
        if isinstance(answer, httpx.Response):
            return answer
        content = answer if isinstance(answer, str) else json.dumps(answer)
        return httpx.Response(200, json={"message": {"role": "assistant", "content": content}})

    def client(self) -> OllamaClient:
        return OllamaClient(OLLAMA, {"ollama"}, transport=httpx.MockTransport(self))


def _document_types(request: httpx.Request) -> httpx.Response:
    name = request.url.params["name__iexact"]
    rule = RULES.get(name)
    row = {
        "id": TYPE_IDS[name],
        "name": name,
        "match": rule or "",
        "matching_algorithm": 4 if rule else 0,
        "is_insensitive": True,
    }
    return httpx.Response(200, json={"results": [row]})


def mock_paperless(
    router: respx.MockRouter,
    *,
    content: str = CONTENT_01,
    suggested: list[int] | None = None,
    patch_status: int = 200,
) -> respx.Route:
    route = router.get(f"{PAPERLESS}/api/documents/{DOC_ID}/").respond(
        200, json={"id": DOC_ID, "content": content}
    )
    router.get(f"{PAPERLESS}/api/document_types/").mock(side_effect=_document_types)
    router.get(f"{PAPERLESS}/api/documents/{DOC_ID}/suggestions/").respond(
        200,
        json={
            "correspondents": [],
            "tags": [],
            "document_types": [TYPE_IDS["invoice"]] if suggested is None else suggested,
            "storage_paths": [],
            "dates": [],
        },
    )
    router.patch(f"{PAPERLESS}/api/documents/{DOC_ID}/").respond(patch_status, json={"id": DOC_ID})
    return route


def paperless(base_url: str, **kw: Any) -> PaperlessClient:
    return PaperlessClient(base_url, **kw)


async def run(
    ledger: DocumentLedger,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ollama: FakeOllama,
    *,
    settings: WorkerSettings = SETTINGS,
    store: FakeStore | None = None,
    now: datetime = NOW,
) -> None:
    async with ollama.client() as client:
        await extract_document(
            ledger.id,
            sessions=sessions,
            keys=keys,
            index=engines,
            paperless=paperless,
            ollama=client,
            store=FakeStore() if store is None else store,
            settings=settings,
            now=now,
        )


async def fields_of(engines: IndexEngines, workspace: Workspace, ledger_id: uuid.UUID) -> Any:
    async with (await engines.engine(workspace)).connect() as conn:
        return await index.read_fields(conn, ledger_id)


async def runs_of(engines: IndexEngines, workspace: Workspace, ledger_id: uuid.UUID) -> Any:
    async with (await engines.engine(workspace)).connect() as conn:
        result = await conn.execute(
            text("SELECT run, model, raw FROM extraction_run WHERE ledger_id = :id ORDER BY id"),
            {"id": ledger_id},
        )
        return result.all()


async def events(db: AsyncSession, workspace: Workspace) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent).where(AuditEvent.workspace_id == workspace.id).order_by(AuditEvent.seq)
    )
    return list(rows)


async def actions(db: AsyncSession, workspace: Workspace) -> list[str]:
    return [e.action for e in await events(db, workspace)]


async def reload(db: AsyncSession, ledger: DocumentLedger) -> DocumentLedger:
    row = await db.get(DocumentLedger, ledger.id, populate_existing=True)
    assert row is not None
    return row


# The model contract -----------------------------------------------------------------------


def test_raw_extraction_has_every_field_as_optional_text() -> None:
    assert tuple(RawExtraction.model_fields) == FIELD_NAMES
    literal, none = get_args(RawExtraction.model_fields["document_type"].annotation)
    assert get_args(literal) == DOCUMENT_TYPES
    assert none is type(None)
    assert RawExtraction().model_dump() == dict.fromkeys(FIELD_NAMES)


def test_extraction_schema_is_nullable_strings_and_a_type_enum() -> None:
    schema = extraction_schema()
    assert schema["type"] == "object"
    assert schema["required"] == list(FIELD_NAMES)
    assert list(schema["properties"]) == list(FIELD_NAMES)
    for name in FIELD_NAMES:
        assert schema["properties"][name]["type"] == ["string", "null"]
    assert schema["properties"]["document_type"]["enum"] == [*DOCUMENT_TYPES, None]


@pytest.mark.parametrize("prompt", [PROMPT_A, PROMPT_B], ids=["a", "b"])
def test_prompts_carry_the_rules(prompt: str) -> None:
    for label in (
        "Total HT",
        "المجموع دون احتساب الرسوم",
        "TVA",
        "الضريبة على القيمة المضافة",
        "Total TTC",
        "المجموع مع احتساب الرسوم",
        "ICE",
        "التعريف الموحد للمقاولة",
        "DH",
        "درهم",
        "MAD",
        "Client",
        "الزبون",
        "null",
    ):
        assert label in prompt, label
    lowered = prompt.lower()
    assert "issued" in lowered
    assert "as printed" in lowered
    assert "order" in lowered
    for name in FIELD_NAMES:
        assert name in prompt


def test_prompts_are_worded_differently() -> None:
    assert PROMPT_A != PROMPT_B
    assert PROMPT_A.split("\n")[0] != PROMPT_B.split("\n")[0]


def test_settings_defaults_and_hosts_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    base = {
        "SIJIL_REGION": "ma-cas",
        "SIJIL_ENVIRONMENT": "test",
        "SIJIL_REDIS_URL": "redis://127.0.0.1:1/0",
        "SIJIL_MASTER_KEY": "a" * 43 + "=",
        "SIJIL_DATABASE_URL": "postgresql+asyncpg://x:y@h/z",
        "SIJIL_PROVISIONER_DATABASE_URL": "postgresql+asyncpg://x:y@h/z",
        "SIJIL_TENANT_IMAGE": "img:1",
        "SIJIL_COMPOSE_PROJECT": "sijil",
        "SIJIL_TESSDATA_VOLUME": "t",
    }
    for key, value in base.items():
        monkeypatch.setenv(key, value)
    s = WorkerSettings()
    assert (s.extraction_model, s.ollama_url, s.ai_allowed_hosts) == (
        "qwen3:4b",
        "http://ollama:11434",
        frozenset({"ollama"}),
    )
    monkeypatch.setenv("SIJIL_AI_ALLOWED_HOSTS", "ollama, ollama-2")
    assert WorkerSettings().ai_allowed_hosts == frozenset({"ollama", "ollama-2"})
    assert s.ollama_num_thread is None
    monkeypatch.setenv("SIJIL_OLLAMA_NUM_THREAD", "6")
    assert WorkerSettings().ollama_num_thread == 6


def test_extraction_worker_boots_without_provisioning_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The extraction worker gets no provisioner role or Docker settings; the main worker still
    refuses to boot without them."""
    from pydantic import ValidationError

    from sijil_worker.settings import ExtractionSettings

    for key in (
        "SIJIL_PROVISIONER_DATABASE_URL",
        "SIJIL_TENANT_IMAGE",
        "SIJIL_COMPOSE_PROJECT",
        "SIJIL_TESSDATA_VOLUME",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in {
        "SIJIL_REGION": "ma-cas",
        "SIJIL_ENVIRONMENT": "test",
        "SIJIL_REDIS_URL": "redis://127.0.0.1:1/0",
        "SIJIL_MASTER_KEY": "a" * 43 + "=",
        "SIJIL_DATABASE_URL": "postgresql+asyncpg://x:y@h/z",
    }.items():
        monkeypatch.setenv(key, value)
    s = ExtractionSettings()
    assert (s.region, s.extraction_model) == ("ma-cas", "qwen3:4b")
    assert not hasattr(s, "provisioner_database_url")
    with pytest.raises(ValidationError):
        WorkerSettings()
    from sijil_worker import main_extraction

    assert main_extraction.celery_app.conf.broker_url == "redis://127.0.0.1:1/0"


# Taxonomy ---------------------------------------------------------------------------------


async def test_ensure_taxonomy_idempotent() -> None:
    """A fake Paperless store of document types: the second run creates and changes nothing."""
    store: dict[int, dict[str, Any]] = {}

    def listing(request: httpx.Request) -> httpx.Response:
        name = request.url.params["name__iexact"].casefold()
        rows = [r for r in store.values() if r["name"].casefold() == name]
        return httpx.Response(200, json={"results": rows})

    def create(request: httpx.Request) -> httpx.Response:
        row = {"id": len(store) + 1, **json.loads(request.content)}
        store[row["id"]] = row
        return httpx.Response(201, json=row)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{PAPERLESS}/api/document_types/").mock(side_effect=listing)
        post = router.post(f"{PAPERLESS}/api/document_types/").mock(side_effect=create)
        patch = router.patch(url__regex=rf"{PAPERLESS}/api/document_types/\d+/")
        async with PaperlessClient(PAPERLESS, token=TOKEN) as client:
            first = await ensure_taxonomy(client)
            second = await ensure_taxonomy(client)
    assert first == second
    assert set(first) == set(DOCUMENT_TYPES)
    assert post.call_count == len(DOCUMENT_TYPES)
    assert not patch.called
    by_name = {r["name"]: r for r in store.values()}
    assert by_name["invoice"]["match"] == r"\b(facture|invoice)\b|فاتورة"
    assert by_name["receipt"]["match"] == r"\b(re[çc]u|receipt|ticket de caisse)\b|وصل"
    assert by_name["contract"]["match"] == r"\b(contrat|contract)\b|عقد"
    assert (
        by_name["bank_statement"]["match"]
        == r"relev[ée] (bancaire|de compte)|bank statement|كشف الحساب"
    )
    assert by_name["payslip"]["match"] == r"bulletin de (paie|salaire)|payslip|ورقة الأداء"
    assert by_name["tax_notice"]["match"] == r"avis d'imposition|tax notice|إشعار ضريبي"
    assert all(r["is_insensitive"] is True for r in store.values())
    assert {n: r["matching_algorithm"] for n, r in by_name.items()} == {
        **dict.fromkeys(DOCUMENT_TYPES, 4),
        "other": 0,
    }
    assert by_name["other"]["match"] == ""


# The pipeline -----------------------------------------------------------------------------


async def test_extraction_schema(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    """sample-01, both runs agreeing: the stored values are the manifest's canonical strings."""
    ledger = await make_ledger(db, ready)
    ollama = FakeOllama({1: RAW_01, 2: RAW_01})
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, ollama)
    row = await fields_of(engines, ready, ledger.id)
    assert isinstance(row, FieldsRow)
    assert dict(row.values) == CANONICAL_01
    assert row.document_type == "invoice"
    assert set(row.confidence) == set(FIELD_NAMES)
    assert all(row.confidence[name] >= 0.90 for name in FIELD_NAMES), row.confidence
    assert row.flags == ()
    assert row.model == "qwen3:4b"
    assert row.extracted_at == NOW
    assert row.source == dict.fromkeys(FIELD_NAMES, "extraction")
    assert all(row.signals[name]["runs_agree"] is True for name in FIELD_NAMES)
    assert row.signals["document_type"]["paperless_agrees"] is True
    # Both runs are kept as evidence, and both used the schema with their own prompt and seed.
    runs = await runs_of(engines, ready, ledger.id)
    assert [(r.run, r.model, r.raw) for r in runs] == [
        ("a", "qwen3:4b", RAW_01),
        ("b", "qwen3:4b", RAW_01),
    ]
    bodies = [json.loads(r.content) for r in ollama.requests]
    assert [b["options"]["seed"] for b in bodies] == [1, 2]
    assert [b["messages"][0]["content"] for b in bodies] == [PROMPT_A, PROMPT_B]
    assert all(b["format"] == extraction_schema() for b in bodies)
    assert all(CONTENT_01.strip() in b["messages"][1]["content"] for b in bodies)
    # Every check passes: filed, after the extraction is audited.
    assert (await reload(db, ledger)).state is DocumentState.filed
    extracted, filed = await events(db, ready)
    assert extracted.action == "document.extracted"
    assert extracted.data == {
        "model": "qwen3:4b",
        "runs_agree_count": len(FIELD_NAMES),
        "min_confidence_pct": 100,
    }
    assert filed.action == "document.filed"
    assert filed.data == {"min_confidence_pct": 100, "flags": ""}


async def test_no_external_host_called(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    ollama = FakeOllama({1: RAW_01, 2: RAW_01})
    # respx refuses any request it has no route for, so an unknown host would fail the run.
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, ollama)
        paperless_hosts = {call.request.url.host for call in router.calls}
    assert paperless_hosts == {"paperless-ws"}
    assert {r.url.host for r in ollama.requests} == {"ollama"}
    with pytest.raises(ValueError, match=r"^ai\.host_not_allowed$"):
        OllamaClient("https://api.example.com", {"ollama"})


async def test_two_runs_disagree_sets_signal_false(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    # Run B reads another TTC, writes the HT differently (same value), and misses the number.
    run_b = {**RAW_01, "total_ttc": "1 050,00 MAD", "total_ht": "1250.00", "number": None}
    # Run A misses the VAT rate: run B's is kept.
    run_a = {**RAW_01, "vat_rate": None}
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router, suggested=[TYPE_IDS["receipt"]])
        await run(ledger, sessions, keys, engines, FakeOllama({1: run_a, 2: run_b}))
    row = await fields_of(engines, ready, ledger.id)
    assert row.values["total_ttc"] == "1500.00"  # run A's value is the one kept
    assert row.values["number"] == SAMPLE_01["number"]
    assert row.values["vat_rate"] == "20"
    assert row.signals["total_ttc"]["runs_agree"] is False
    assert row.signals["number"]["runs_agree"] is False  # one side None
    assert row.signals["vat_rate"]["runs_agree"] is False
    assert row.signals["total_ht"]["runs_agree"] is True  # equal once canonical
    assert row.signals["supplier_ice"]["runs_agree"] is True
    assert row.signals["document_type"]["paperless_agrees"] is False
    event = (await events(db, ready))[0]
    assert event.data["runs_agree_count"] == len(FIELD_NAMES) - 3


async def test_paperless_suggesting_no_type_is_not_applicable(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router, suggested=[999])  # not one of ours
        await run(ledger, sessions, keys, engines, FakeOllama({1: RAW_01, 2: RAW_01}))
    row = await fields_of(engines, ready, ledger.id)
    assert row.signals["document_type"]["paperless_agrees"] is None


async def test_arabic_content_is_normalised_and_truncated(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    # Bidi controls and presentation forms as Paperless gives them, then far too much text.
    content = "‫ﻓﺎﺗﻮﺭﺓ‬  ‪F-2026-01002‬\n" + "x" * 20_000
    ollama = FakeOllama({1: RAW_01, 2: RAW_01})
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router, content=content)
        await run(ledger, sessions, keys, engines, ollama)
    sent = json.loads(ollama.requests[0].content)["messages"][1]["content"]
    assert "فاتورة F-2026-01002\n" in sent
    assert "‫" not in sent and "‪" not in sent
    body = sent.split("<<<\n", 1)[1].rsplit("\n>>>", 1)[0]
    assert len(body) == 12_000
    assert body.count("x") == 12_000 - len("فاتورة F-2026-01002\n")


async def test_empty_content_flags_no_text_without_calling_the_model(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    ollama = FakeOllama({})
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router, content=" ‫‬ \n ")
        await run(ledger, sessions, keys, engines, ollama)
    assert ollama.requests == []
    row = await fields_of(engines, ready, ledger.id)
    assert row.flags == ("no_text",)
    assert dict(row.values) == dict.fromkeys(FIELD_NAMES)
    extracted, review = await events(db, ready)
    assert extracted.action == "document.extracted"
    assert extracted.data == {"model": "qwen3:4b", "runs_agree_count": 0, "min_confidence_pct": 0}
    assert review.action == "document.needs_review"
    assert review.data == {"min_confidence_pct": 0, "flags": "no_text"}
    assert (await reload(db, ledger)).state is DocumentState.needs_review


async def test_one_malformed_run_uses_the_other(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, FakeOllama({1: "{not json", 2: RAW_01}))
    row = await fields_of(engines, ready, ledger.id)
    assert dict(row.values) == CANONICAL_01
    assert "extraction_failed" not in row.flags
    assert all(row.signals[name]["runs_agree"] is False for name in FIELD_NAMES)
    runs = await runs_of(engines, ready, ledger.id)
    assert runs[0].raw == {"error": "ai.bad_output", "content": "{not json"}


async def test_malformed_json_twice_routes_to_review(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    # A value of the wrong type is a schema mismatch too.
    wrong_type = {**RAW_01, "total_ttc": 1500}
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, FakeOllama({1: "nope", 2: wrong_type}))
    row = await fields_of(engines, ready, ledger.id)
    assert "extraction_failed" in row.flags
    assert dict(row.values) == dict.fromkeys(FIELD_NAMES)
    assert len(await runs_of(engines, ready, ledger.id)) == 2
    failed, review = await events(db, ready)
    assert failed.action == "document.extraction_failed"
    assert failed.data == {"code": "extraction.ai_bad_output"}
    assert review.action == "document.needs_review"
    assert review.data == {
        "min_confidence_pct": 0,
        "flags": "extraction_failed,extraction.ai_bad_output",
    }
    assert (await reload(db, ledger)).state is DocumentState.needs_review


async def test_ollama_unavailable_is_retried(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    """Ollama down: the run raises for Celery to retry, and nothing is recorded meanwhile."""
    ledger = await make_ledger(db, ready)
    down = FakeOllama({1: RAW_01, 2: httpx.Response(503, json={"error": "busy"})})
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        with pytest.raises(OllamaUnavailable):
            await run(ledger, sessions, keys, engines, down)
    assert (await reload(db, ledger)).state is DocumentState.received
    assert await fields_of(engines, ready, ledger.id) is None
    assert await runs_of(engines, ready, ledger.id) == []  # no half-run evidence
    assert await events(db, ready) == []
    # The retry, with Ollama back, extracts normally.
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, FakeOllama({1: RAW_01, 2: RAW_01}))
    assert (await fields_of(engines, ready, ledger.id)).values == CANONICAL_01


async def test_paperless_unavailable_is_raised_for_retry(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    ollama = FakeOllama({})
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{PAPERLESS}/api/documents/{DOC_ID}/").respond(500)
        with pytest.raises(PaperlessUnavailable):
            await run(ledger, sessions, keys, engines, ollama)
    assert ollama.requests == []
    assert await fields_of(engines, ready, ledger.id) is None


@pytest.mark.parametrize(
    ("state", "ingest_state"),
    [
        (DocumentState.received, IngestState.queued),
        (DocumentState.received, IngestState.sending),
        (DocumentState.received, IngestState.failed),
        (DocumentState.needs_review, IngestState.consumed),
        (DocumentState.needs_review, IngestState.unsupported),
        (DocumentState.filed, IngestState.consumed),
    ],
)
async def test_extract_noop_unless_received_and_consumed(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
    state: DocumentState,
    ingest_state: IngestState,
) -> None:
    ledger = await make_ledger(db, ready, state=state, ingest_state=ingest_state)
    ollama = FakeOllama({})
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, ollama)
        assert not router.calls
    assert ollama.requests == []
    assert await fields_of(engines, ready, ledger.id) is None
    assert await events(db, ready) == []


@pytest.mark.parametrize("ws_state", [WorkspaceState.hibernated, WorkspaceState.closing])
@pytest.mark.parametrize("ingest_state", [IngestState.consumed, IngestState.unsupported])
async def test_extract_noop_unless_workspace_active(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
    ws_state: WorkspaceState,
    ingest_state: IngestState,
) -> None:
    """A hibernated or closing workspace does not extract: nothing is called, written or
    audited, and nothing is raised (no retries piling up behind the model)."""
    ledger = await make_ledger(db, ready, ingest_state=ingest_state)
    ready.state = ws_state
    await db.commit()
    ollama = FakeOllama({})
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, ollama)
        assert not router.calls
    assert ollama.requests == []
    assert await fields_of(engines, ready, ledger.id) is None
    assert await events(db, ready) == []
    assert (await reload(db, ledger)).state is DocumentState.received


@pytest.mark.parametrize("ingest_state", [IngestState.queued, IngestState.sending])
async def test_extraction_waits_while_the_workspace_is_still_ingesting(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
    ingest_state: IngestState,
) -> None:
    """OCR first: while another document of the same workspace that just arrived is still
    being ingested, the model is not started (the task retries later, not as a failure)."""
    ledger = await make_ledger(db, ready)
    await make_ledger(db, ready, ingest_state=ingest_state, age=timedelta(minutes=2))
    ollama = FakeOllama({})
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        with pytest.raises(IngestInProgress):
            await run(ledger, sessions, keys, engines, ollama)
    assert ollama.requests == []
    assert await fields_of(engines, ready, ledger.id) is None
    assert await events(db, ready) == []


async def test_old_stuck_ingest_does_not_hold_extraction_back(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
    org: Organisation,
) -> None:
    """A row queued for longer than the window (a delayed ingest), or one of another
    workspace, never holds this workspace's extraction back."""
    ledger = await make_ledger(db, ready)
    await make_ledger(
        db, ready, ingest_state=IngestState.queued, age=INGEST_DEFER_WINDOW + timedelta(1)
    )
    slug = f"other-{uuid.uuid4().hex[:8]}"
    other = Workspace(
        org_id=org.id,
        name="Other",
        slug=slug,
        state=WorkspaceState.active,
        db_name=f"paperless_{slug}",
        index_db_name=f"index_{slug}",
        kms_key_id=f"kek-{slug}",
    )
    db.add(other)
    await db.commit()
    await make_ledger(db, other, ingest_state=IngestState.queued)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, FakeOllama({1: RAW_01, 2: RAW_01}))
    assert (await reload(db, ledger)).state is DocumentState.filed


async def test_already_extracted_is_noop(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    """A redelivery (or the sweep) never re-runs the model over an existing row."""
    ledger = await make_ledger(db, ready)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, FakeOllama({1: RAW_01, 2: RAW_01}))
    again = FakeOllama({})
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, again)
        assert not router.calls
    assert again.requests == []
    assert len(await runs_of(engines, ready, ledger.id)) == 2
    assert await actions(db, ready) == ["document.extracted", "document.filed"]


async def test_foreign_region_row_is_left_alone(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
    caplog: pytest.LogCaptureFixture,
) -> None:
    ledger = await make_ledger(db, ready)
    ollama = FakeOllama({})
    other_region = WorkerSettings.model_construct(region="eu-fra", environment="test")
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, ollama, settings=other_region)
        assert not router.calls
    assert ollama.requests == []
    assert await fields_of(engines, ready, ledger.id) is None
    assert await events(db, ready) == []
    assert "region" in caplog.text


async def test_audit_has_no_field_values(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, FakeOllama({1: RAW_01, 2: RAW_01}))
    extracted, filed = await events(db, ready)
    for event in (extracted, filed):
        dumped = json.dumps(event.data, ensure_ascii=False)
        for value in (*RAW_01.values(), *CANONICAL_01.values(), "1250", "1500", "Atlas"):
            if value not in (None, "invoice", "MAD", "20"):
                assert str(value) not in dumped, value
        assert all(isinstance(v, str | int) for v in event.data.values())
    assert set(extracted.data) == {"model", "runs_agree_count", "min_confidence_pct"}
    assert set(filed.data) == {"min_confidence_pct", "flags"}


# Scoring and routing ----------------------------------------------------------------------


def _patches(router: respx.MockRouter) -> list[dict[str, Any]]:
    return [
        json.loads(c.request.content)
        for c in router.calls
        if c.request.method == "PATCH" and c.request.url.path == f"/api/documents/{DOC_ID}/"
    ]


def _broken_store() -> FakeStore:
    store = FakeStore()
    store.fail = ConnectionError("store down")
    return store


async def test_filed_document_gets_its_title_and_type_in_paperless(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, FakeOllama({1: RAW_01, 2: RAW_01}))
        patches = _patches(router)
    title = f"{SAMPLE_01['date']} {SUPPLIER_01['name']} {SAMPLE_01['number']}"
    assert patches == [{"title": title, "document_type": TYPE_IDS["invoice"]}]


async def test_paperless_title_failure_does_not_change_routing(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
    caplog: pytest.LogCaptureFixture,
) -> None:
    ledger = await make_ledger(db, ready)
    raw = {**RAW_01, "number": None}  # a missing part is left out of the title
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router, patch_status=500)
        await run(ledger, sessions, keys, engines, FakeOllama({1: raw, 2: raw}))
        title = _patches(router)[0]["title"]
    assert title == f"{SAMPLE_01['date']} {SUPPLIER_01['name']}"
    assert (await reload(db, ledger)).state is DocumentState.needs_review  # no number
    assert await actions(db, ready) == ["document.extracted", "document.needs_review"]
    assert "title" in caplog.text
    assert SUPPLIER_01["name"] not in caplog.text  # logged by code only


async def test_totals_mismatch_routes_to_review(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    # SAMPLE: sample-05's amounts, whose printed TTC is 0.50 off HT + TVA.
    raw = {**RAW_01, "total_ht": "2 340,00", "vat": "468,00", "total_ttc": "2 808,50"}
    ledger = await make_ledger(db, ready)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, FakeOllama({1: raw, 2: raw}))
    row = await fields_of(engines, ready, ledger.id)
    assert row.signals["total_ttc"]["totals_consistent"] is False
    assert row.confidence["total_ttc"] < 0.90
    assert (await reload(db, ledger)).state is DocumentState.needs_review
    review = (await events(db, ready))[-1]
    assert review.action == "document.needs_review"
    assert review.data == {"min_confidence_pct": 50, "flags": ""}


async def test_possible_duplicate_flag(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    first = await make_ledger(db, ready)
    second = await make_ledger(db, ready)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(first, sessions, keys, engines, FakeOllama({1: RAW_01, 2: RAW_01}))
        await run(second, sessions, keys, engines, FakeOllama({1: RAW_01, 2: RAW_01}))
    assert (await reload(db, first)).state is DocumentState.filed
    assert (await reload(db, second)).state is DocumentState.needs_review
    assert (await fields_of(engines, ready, first.id)).flags == ()
    row = await fields_of(engines, ready, second.id)
    assert row.flags == ("possible_duplicate",)
    assert all(row.confidence[name] >= 0.90 for name in FIELD_NAMES)  # only the flag blocks
    review = (await events(db, ready))[-1]
    assert (review.target_id, review.action) == (str(second.id), "document.needs_review")
    assert review.data == {"min_confidence_pct": 100, "flags": "possible_duplicate"}


async def test_unsupported_ingest_routes_to_review(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    """No Paperless document and no extraction: straight to review, with the ingest flag."""
    ledger = await make_ledger(db, ready, ingest_state=IngestState.unsupported)
    ollama = FakeOllama({})
    store = FakeStore()
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, ollama, store=store)
        await run(ledger, sessions, keys, engines, ollama, store=store)  # once only
        assert not router.calls
    assert ollama.requests == []
    assert await runs_of(engines, ready, ledger.id) == []
    row = await fields_of(engines, ready, ledger.id)
    assert row.flags == ("ingest.unsupported_format",)
    assert dict(row.values) == dict.fromkeys(FIELD_NAMES)
    assert (await reload(db, ledger)).state is DocumentState.needs_review
    (review,) = await events(db, ready)
    assert review.action == "document.needs_review"
    assert review.data == {"min_confidence_pct": 0, "flags": "ingest.unsupported_format"}
    assert store.extended == []  # arrival in 2026: the provisional date already holds


async def test_filing_never_lowers_retention(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    # Arrival in 2026 (kept to 2036-12-31), dated 2024 (only 2034-12-31 would be needed).
    ledger = await make_ledger(db, ready)
    raw = {**RAW_01, "date": "2024-05-10"}
    store = FakeStore()
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, FakeOllama({1: raw, 2: raw}), store=store)
    row = await reload(db, ledger)
    assert row.state is DocumentState.filed
    assert row.retention_until == date(2036, 12, 31)
    assert store.extended == []


async def test_filing_extends_retention_and_object_lock(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    arrival = datetime(2026, 1, 5, 10, 0, tzinfo=UTC)
    ledger = await make_ledger(db, ready, ingested_at=arrival, retention_until=date(2036, 12, 31))
    raw = {**RAW_01, "date": "2027-01-02"}

    async def committed() -> tuple[DocumentState, date]:
        # A fresh session sees only what is committed: the ledger must not have moved yet.
        async with sessions() as other:
            found = await other.get(DocumentLedger, ledger.id)
            assert found is not None
            return found.state, found.retention_until

    store = FakeStore(observe=committed)
    later = datetime(2027, 1, 2, 12, 0, tzinfo=UTC)  # the date is plausible by then
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        ollama = FakeOllama({1: raw, 2: raw})
        await run(ledger, sessions, keys, engines, ollama, store=store, now=later)
    assert store.extended == [(ObjectRef(ledger.object_key, "v1"), date(2037, 12, 31))]
    assert store.observed == [(DocumentState.received, date(2036, 12, 31))]
    row = await reload(db, ledger)
    assert (row.state, row.retention_until) == (DocumentState.filed, date(2037, 12, 31))


async def test_implausible_date_never_extends_retention(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    """A misread year (2031, in 2026) must not lock the original for years more: an object
    lock can never be shortened. The arrival date stands in."""
    ledger = await make_ledger(db, ready)
    raw = {**RAW_01, "date": "2031-01-01"}
    store = FakeStore()
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, FakeOllama({1: raw, 2: raw}), store=store)
    row = await reload(db, ledger)
    assert (row.state, row.retention_until) == (DocumentState.needs_review, date(2036, 12, 31))
    assert store.extended == []


async def test_store_failure_keeps_the_ledger_and_the_retry_routes_the_stored_row(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    """The object lock is extended before the ledger moves: if that fails nothing moves, and
    the next run routes the row already extracted (no model call)."""
    ledger = await make_ledger(db, ready, retention_until=date(2036, 6, 30))
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        ollama = FakeOllama({1: RAW_01, 2: RAW_01})
        with pytest.raises(ConnectionError):
            await run(ledger, sessions, keys, engines, ollama, store=_broken_store())
        assert _patches(router) == []
    row = await reload(db, ledger)
    assert (row.state, row.retention_until) == (DocumentState.received, date(2036, 6, 30))
    assert await actions(db, ready) == ["document.extracted"]
    assert await fields_of(engines, ready, ledger.id) is not None
    again = FakeOllama({})
    store = FakeStore()
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, again, store=store)
    assert again.requests == []
    assert store.extended == [(ObjectRef(ledger.object_key, "v1"), date(2036, 12, 31))]
    row = await reload(db, ledger)
    assert (row.state, row.retention_until) == (DocumentState.filed, date(2036, 12, 31))
    assert await actions(db, ready) == ["document.extracted", "document.filed"]


async def test_concurrent_routes_write_one_event(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    import asyncio

    ledger = await make_ledger(db, ready, retention_until=date(2036, 6, 30))
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        ollama = FakeOllama({1: RAW_01, 2: RAW_01})
        with pytest.raises(ConnectionError):
            await run(ledger, sessions, keys, engines, ollama, store=_broken_store())
    store = FakeStore()
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await asyncio.gather(
            *(run(ledger, sessions, keys, engines, FakeOllama({}), store=store) for _ in range(4))
        )
        assert len(_patches(router)) == 1
    assert await actions(db, ready) == ["document.extracted", "document.filed"]
    assert len(store.extended) == 1
    assert (await reload(db, ledger)).state is DocumentState.filed


async def test_permanent_store_failure_routes_to_review(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    """The store refuses every extension: the task's final path (`record_extraction_failed`)
    sends the document to review with a blocking flag instead of leaving it `received` for
    ever. The retention stays what the ledger holds."""
    from botocore.exceptions import ClientError

    ledger = await make_ledger(db, ready, retention_until=date(2036, 6, 30))
    store = FakeStore()
    store.fail = ClientError({"Error": {"Code": "AccessDenied"}}, "PutObjectRetention")
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        with pytest.raises(ClientError):
            await run(
                ledger, sessions, keys, engines, FakeOllama({1: RAW_01, 2: RAW_01}), store=store
            )
        assert not tasks.is_extract_transient(store.fail)  # so the task goes final at once

        async def final() -> None:
            await record_extraction_failed(
                ledger.id, "extraction.error", sessions=sessions, index=engines, store=store,
                settings=SETTINGS, now=NOW,
            )  # fmt: skip

        await final()
        await final()  # once only
    row = await reload(db, ledger)
    assert (row.state, row.retention_until) == (DocumentState.needs_review, date(2036, 6, 30))
    assert store.extended == []
    fields = await fields_of(engines, ready, ledger.id)
    assert fields.flags == ("retention.extend_failed",)
    assert await actions(db, ready) == ["document.extracted", "document.needs_review"]
    review = (await events(db, ready))[-1]
    assert review.data == {"min_confidence_pct": 100, "flags": "retention.extend_failed"}


async def test_date_one_day_ahead_never_adds_a_year_of_lock(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    """On 31 December (local), a document dated 1 January next year is plausible (a time zone
    apart) but its retention counts from today's year."""
    eve = datetime(2026, 12, 31, 12, 0, tzinfo=UTC)
    ledger = await make_ledger(db, ready, ingested_at=eve)
    raw = {**RAW_01, "date": "2027-01-01"}
    store = FakeStore()
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(
            ledger, sessions, keys, engines, FakeOllama({1: raw, 2: raw}), store=store, now=eve
        )
    row = await reload(db, ledger)
    assert (row.state, row.retention_until) == (DocumentState.filed, date(2036, 12, 31))
    assert store.extended == []


# Retries running out ----------------------------------------------------------------------


async def test_exhausted_retries_record_extraction_failed(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    await record_extraction_failed(
        ledger.id,
        "extraction.ai_unavailable",
        sessions=sessions,
        index=engines,
        store=FakeStore(),
        settings=SETTINGS,
        now=NOW,
    )
    row = await fields_of(engines, ready, ledger.id)
    # The code rides along in the flags, so a lost audit can be rebuilt from the row.
    assert row.flags == ("extraction_failed", "extraction.ai_unavailable")
    failed, review = await events(db, ready)
    assert failed.action == "document.extraction_failed"
    assert failed.data == {"code": "extraction.ai_unavailable"}
    assert review.action == "document.needs_review"
    assert review.data == {
        "min_confidence_pct": 0,
        "flags": "extraction_failed,extraction.ai_unavailable",
    }
    assert (await reload(db, ledger)).state is DocumentState.needs_review
    # Never twice, and never over a row an extraction wrote.
    await record_extraction_failed(
        ledger.id,
        "extraction.ai_unavailable",
        sessions=sessions,
        index=engines,
        store=FakeStore(),
        settings=SETTINGS,
        now=NOW,
    )
    assert len(await events(db, ready)) == 2


async def test_record_failed_leaves_an_extracted_row_alone(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, FakeOllama({1: RAW_01, 2: RAW_01}))
    await record_extraction_failed(
        ledger.id,
        "extraction.error",
        sessions=sessions,
        index=engines,
        store=FakeStore(),
        settings=SETTINGS,
        now=NOW,
    )
    row = await fields_of(engines, ready, ledger.id)
    assert row.flags == ()
    assert await actions(db, ready) == ["document.extracted", "document.filed"]


async def test_record_failed_skips_a_row_of_another_workspace(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    ledger = await make_ledger(db, ready)
    await record_extraction_failed(
        ledger.id,
        "extraction.error",
        sessions=sessions,
        index=engines,
        store=FakeStore(),
        settings=SETTINGS,
        now=NOW,
        workspace_id=uuid.uuid4(),
    )
    assert await fields_of(engines, ready, ledger.id) is None
    assert await events(db, ready) == []


def _audit_fails_once(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    from sijil_worker import extract as module

    real = module._append_event
    calls: list[str] = []

    async def flaky(session: AsyncSession, **kw: Any) -> None:
        calls.append(kw["action"])
        if len(calls) == 1:
            raise ConnectionError("database gone mid-commit")
        await real(session, **kw)

    monkeypatch.setattr(module, "_append_event", flaky)
    return calls


async def test_lost_extracted_audit_is_rebuilt_on_the_next_run(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = await make_ledger(db, ready)
    calls = _audit_fails_once(monkeypatch)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        with pytest.raises(ConnectionError):
            await run(ledger, sessions, keys, engines, FakeOllama({1: RAW_01, 2: RAW_01}))
    assert await fields_of(engines, ready, ledger.id) is not None  # the row committed
    assert await events(db, ready) == []
    # The retry finds the row, runs no model, and writes the missing audit from it.
    again = FakeOllama({})
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(ledger, sessions, keys, engines, again)
        await run(ledger, sessions, keys, engines, again)  # and only once
    assert again.requests == []
    extracted, filed = await events(db, ready)
    assert extracted.action == "document.extracted"
    assert extracted.data == {
        "model": "qwen3:4b",
        "runs_agree_count": len(FIELD_NAMES),
        "min_confidence_pct": 100,
    }
    assert filed.action == "document.filed"
    assert calls == ["document.extracted", "document.extracted", "document.filed"]


async def test_lost_failure_audit_is_rebuilt_on_the_next_run(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
    ready: Workspace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = await make_ledger(db, ready)
    _audit_fails_once(monkeypatch)

    async def record() -> None:
        await record_extraction_failed(
            ledger.id,
            "extraction.ai_unavailable",
            sessions=sessions,
            index=engines,
            store=FakeStore(),
            settings=SETTINGS,
            now=NOW,
        )

    with pytest.raises(ConnectionError):
        await record()
    assert await events(db, ready) == []
    await record()
    await record()
    failed, review = await events(db, ready)
    assert failed.action == "document.extraction_failed"
    assert failed.data == {"code": "extraction.ai_unavailable"}
    assert review.action == "document.needs_review"


async def test_lost_bad_output_audit_is_rebuilt_by_the_next_extraction(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = await make_ledger(db, ready)
    _audit_fails_once(monkeypatch)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        with pytest.raises(ConnectionError):
            await run(ledger, sessions, keys, engines, FakeOllama({1: "x", 2: "y"}))
        await run(ledger, sessions, keys, engines, FakeOllama({}))
    failed, review = await events(db, ready)
    assert failed.action == "document.extraction_failed"
    assert failed.data == {"code": "extraction.ai_bad_output"}
    assert review.action == "document.needs_review"


async def test_concurrent_reaudits_write_one_event(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
    ready: Workspace,
) -> None:
    import asyncio

    ledger = await make_ledger(db, ready)

    async def record() -> None:
        await record_extraction_failed(
            ledger.id,
            "extraction.error",
            sessions=sessions,
            index=engines,
            store=FakeStore(),
            settings=SETTINGS,
            now=NOW,
        )

    await asyncio.gather(*(record() for _ in range(4)))
    assert await actions(db, ready) == ["document.extraction_failed", "document.needs_review"]


ARGS = {"ledger_id": "00000000-0000-0000-0000-000000000001", "workspace_id": None}


def _settings() -> WorkerSettings:
    return WorkerSettings(
        region="ma-cas",
        environment="test",
        redis_url="redis://127.0.0.1:1/0",
        master_key="a" * 43 + "=",
        database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
        provisioner_database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
        tenant_image="img:1",
        compose_project="sijil",
        tessdata_volume="t",
    )


@pytest.fixture
def eager_app() -> None:
    app = make_celery(_settings())
    app.conf.task_always_eager = True
    app.set_current()


def _recording(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> tuple[list[str], list[tuple[str, str]]]:
    attempts: list[str] = []
    failed: list[tuple[str, str]] = []

    async def extract(ledger_id: str, workspace_id: str | None) -> None:
        attempts.append(ledger_id)
        raise error

    async def record(ledger_id: str, workspace_id: str | None, code: str) -> None:
        assert workspace_id == ARGS["workspace_id"]
        failed.append((ledger_id, code))

    monkeypatch.setattr(tasks, "_extract", extract)
    monkeypatch.setattr(tasks, "_extract_failed", record)
    return attempts, failed


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (OllamaUnavailable("ai.unavailable: HTTP 503", 503), "extraction.ai_unavailable"),
        (PaperlessUnavailable("down"), "extraction.paperless_unavailable"),
    ],
)
def test_transient_failures_retry_then_record_extraction_failed(
    eager_app: None, monkeypatch: pytest.MonkeyPatch, error: Exception, code: str
) -> None:
    attempts, failed = _recording(monkeypatch, error)
    result = tasks.extract_document_task.apply(kwargs=ARGS)
    assert isinstance(result.result, type(error))
    assert len(attempts) == tasks.EXTRACT_MAX_RETRIES + 1 == 9
    assert failed == [(ARGS["ledger_id"], code)]  # once, when nothing will retry


def test_final_failure_records_extraction_failed_at_once(
    eager_app: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempts, failed = _recording(monkeypatch, PaperlessError("HTTP 404", 404))
    tasks.extract_document_task.apply(kwargs=ARGS)
    assert len(attempts) == 1
    assert failed == [(ARGS["ledger_id"], "extraction.paperless_error")]


@pytest.mark.parametrize(
    ("code", "status", "transient"),
    [
        ("SlowDown", 503, True),
        ("Throttling", 400, True),
        ("ThrottlingException", 400, True),
        ("RequestTimeout", 400, True),
        ("InternalError", 500, True),
        ("ServiceUnavailable", 503, True),
        ("SomethingNew", 502, True),  # any 5xx
        ("NoSuchKey", 404, False),
        ("AccessDenied", 403, False),
        ("InvalidRequest", 400, False),
        ("InvalidArgument", 400, False),
    ],
)
def test_object_store_error_classification(code: str, status: int, transient: bool) -> None:
    """Filing extends the object lock: a store that is down or throttling is retried; a refusal
    that retrying cannot change is final (the document then goes to review)."""
    from botocore.exceptions import ClientError

    response: Any = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}
    error = ClientError(response, "PutObjectRetention")
    assert tasks.is_extract_transient(error) is transient


def test_store_connection_errors_are_transient_and_shortening_is_final() -> None:
    from botocore.exceptions import EndpointConnectionError

    assert tasks.is_extract_transient(EndpointConnectionError(endpoint_url="http://s3"))
    assert not tasks.is_extract_transient(ValueError("retention can only be extended"))


def test_extract_backoff_numbers() -> None:
    assert (tasks.EXTRACT_BACKOFF_FACTOR_S, tasks.EXTRACT_BACKOFF_MAX_S) == (30, 900)
    assert tasks.EXTRACT_MAX_RETRIES == 8


@pytest.mark.parametrize(
    ("retries", "low", "high"),
    [(0, 15, 30), (1, 30, 60), (4, 240, 480), (5, 450, 900), (7, 450, 900)],
)
def test_extract_countdown_is_equal_jitter(retries: int, low: float, high: float) -> None:
    """Half fixed, half random: never the near-zero waits full jitter allows."""
    assert tasks.extract_countdown(retries, rand=lambda: 0.0) == low
    assert tasks.extract_countdown(retries, rand=lambda: 1.0) == high
    assert low <= tasks.extract_countdown(retries) <= high


def test_eight_retries_always_outlast_half_an_hour() -> None:
    shortest = sum(tasks.extract_countdown(r, rand=lambda: 0.0) for r in range(8))
    assert shortest >= 30 * 60


def test_extract_task_is_routed_to_the_extraction_queue() -> None:
    assert tasks.extract_document_task.name == "sijil.extract_document"
    assert getattr(tasks.extract_document_task, "queue", None) == "extraction"


def test_beat_requeues_unextracted_every_ten_minutes() -> None:
    entry = make_celery(_settings()).conf.beat_schedule["requeue-unextracted"]
    assert entry == {"task": "sijil.requeue_unextracted", "schedule": 600}
    assert tasks.requeue_unextracted_task.name == "sijil.requeue_unextracted"


# The sweep --------------------------------------------------------------------------------


async def test_requeue_unextracted_sends_old_received_unrouted_rows_of_this_region(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], ready: Workspace
) -> None:
    old = timedelta(minutes=30)
    due = await make_ledger(db, ready, age=old)
    await make_ledger(db, ready)  # just consumed: its own task is on the way
    await make_ledger(db, ready, age=old, state=DocumentState.needs_review)
    await make_ledger(db, ready, age=old, ingest_state=IngestState.queued)
    await make_ledger(db, ready, age=old, ingest_state=IngestState.failed)
    # Unsupported: never extracted, but still to be routed to review.
    unsupported = await make_ledger(
        db, ready, age=old + timedelta(minutes=1), ingest_state=IngestState.unsupported
    )
    foreign_org = Organisation(name="F", type=OrgType.business, country="FR", region="eu-fra")
    db.add(foreign_org)
    await db.flush()
    foreign = Workspace(
        org_id=foreign_org.id,
        name="F",
        slug=f"f-{uuid.uuid4().hex[:8]}",
        state=WorkspaceState.active,
        db_name="paperless_f",
        index_db_name="index_f",
        kms_key_id="kek-f",
    )
    db.add(foreign)
    await db.commit()
    await make_ledger(db, foreign, age=old)
    sent: list[tuple[uuid.UUID, uuid.UUID]] = []

    async def enqueue(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        sent.append((ledger_id, workspace_id))

    count = await requeue_unextracted(sessions, enqueue, region="ma-cas", now=NOW)
    assert count == 2
    assert sent == [(unsupported.id, ready.id), (due.id, ready.id)]


@pytest.mark.parametrize("ws_state", [WorkspaceState.hibernated, WorkspaceState.closing])
async def test_requeue_unextracted_only_sweeps_active_workspaces(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    ready: Workspace,
    org: Organisation,
    ws_state: WorkspaceState,
) -> None:
    old = timedelta(minutes=30)
    due = await make_ledger(db, ready, age=old)
    slug = f"idle-{uuid.uuid4().hex[:8]}"
    idle = Workspace(
        org_id=org.id,
        name="Idle",
        slug=slug,
        state=WorkspaceState.active,
        db_name=f"paperless_{slug}",
        index_db_name=f"index_{slug}",
        kms_key_id=f"kek-{slug}",
    )
    db.add(idle)
    await db.commit()
    # Older than `due`, so it would come first if the state were ignored.
    await make_ledger(db, idle, age=old + timedelta(hours=1))
    await make_ledger(db, idle, age=old + timedelta(hours=1), ingest_state=IngestState.unsupported)
    idle.state = ws_state
    await db.commit()
    sent: list[tuple[uuid.UUID, uuid.UUID]] = []

    async def enqueue(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        sent.append((ledger_id, workspace_id))

    assert await requeue_unextracted(sessions, enqueue, region="ma-cas", now=NOW, limit=1) == 1
    assert sent == [(due.id, ready.id)]


async def test_requeue_unextracted_survives_a_failed_enqueue(
    db: AsyncSession, sessions: async_sessionmaker[AsyncSession], ready: Workspace
) -> None:
    for _ in range(2):
        await make_ledger(db, ready, age=timedelta(hours=1))
    calls: list[uuid.UUID] = []

    async def flaky(ledger_id: uuid.UUID, workspace_id: uuid.UUID) -> None:
        calls.append(ledger_id)
        if len(calls) == 1:
            raise ConnectionError("broker down")

    assert await requeue_unextracted(sessions, flaky, region="ma-cas", now=NOW) == 1
    assert len(calls) == 2


async def test_eval_computes_without_writing_anything(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    engines: IndexEngines,
    ready: Workspace,
    tmp_path: Path,
) -> None:
    from sijil_worker import eval as ev

    pdf = tmp_path / "f.pdf"
    pdf.write_bytes(b"SAMPLE bytes")
    ledger = await make_ledger(db, ready, sha=hashlib.sha256(pdf.read_bytes()).hexdigest())
    absent = {**SAMPLE_01, "file": "absent.pdf"}
    (tmp_path / "absent.pdf").write_bytes(b"not archived")

    async def counts() -> tuple[Any, ...]:
        async with (await engines.engine(ready)).connect() as conn:
            n = [
                (await conn.execute(text(q))).scalar_one()
                for q in (
                    "SELECT count(*) FROM document_fields",
                    "SELECT count(*) FROM extraction_run",
                )
            ]
        return (*n, await actions(db, ready), (await reload(db, ledger)).state)

    before = await counts()
    ollama = FakeOllama({1: RAW_01, 2: RAW_01})
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        async with ollama.client() as client:
            results = await ev.evaluate(
                sessions=sessions,
                keys=keys,
                settings=SETTINGS,
                engines=engines,
                ollama=client,
                paperless=paperless,
                workspace=ready,
                entries=[{**SAMPLE_01, "file": "f.pdf"}, absent],
                suppliers=list(SUPPLIERS.values()),
                base=tmp_path,
            )
        assert not _patches(router)
    assert await counts() == before
    assert {(r.doc, r.source) for r in results} == {
        ("f.pdf", "computed"),
        ("absent.pdf", "missing"),
    }
    assert {r.reason for r in results if r.doc == "absent.pdf"} == {"not_in_ledger"}
    assert all(r.correct for r in results if r.doc == "f.pdf")
