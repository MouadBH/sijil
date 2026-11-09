"""Performance budget: search answers in under 500 ms at p95 over 100k documents (plan 5.1),
measured server-side (`Server-Timing: app;dur=…`, sijil_cp.timing) on the dev stack.

Runs in the compose `tests` runner (`make acceptance`: integration and slow) against the real
control plane. The index database is a scratch one made for this test (role and database
`index_perf…`, cloned from `template_index` like a tenant's) and dropped afterwards: never a
tenant's. Its workspace is written straight into the control-plane database (no Paperless) and
marked `closing` at the end. The 100k `search_doc` rows are SAMPLE text, generated with a fixed
seed and loaded with COPY.
"""

import os
import random
import statistics
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import asyncpg
import httpx
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from sijil_core import index
from sijil_core.crypto import LocalKeyService, seal
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    CredentialKind,
    Membership,
    Organisation,
    OrgType,
    Role,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.search_index import MAX_CONTENT_CHARS
from sijil_core.text import search_normalise
from sijil_worker.settings import WorkerSettings
from tests.support.workspaces import closing_at_exit

pytestmark = [pytest.mark.integration, pytest.mark.slow]

CP_URL = os.environ.get("SIJIL_IT_CP_URL", "http://control-plane:8000")
PASSWORD = "it-search-Passw0rd"
DOCS = 100_000
QUERIES = 200
BUDGET_MS = 500.0
SEED = 20261109

FR_WORDS = (
    "facture avoir reçu montant total TVA hors taxes règlement échéance prestation livraison "
    "fourniture bureau électricité téléphone abonnement fibre location véhicule carburant "
    "maintenance réparation honoraires conseil transport marchandises remise acompte solde "
    "commande référence client fournisseur adresse Casablanca Rabat Tanger Marrakech Agadir"
).split()
AR_WORDS = (
    "فاتورة المبلغ الإجمالي الضريبة على القيمة المضافة أداء التسليم خدمات الكهرباء الهاتف "
    "اشتراك كراء سيارة وقود صيانة إصلاح أتعاب استشارة نقل بضائع تخفيض تسبيق رصيد طلبية مرجع "
    "زبون مورد عنوان الدار البيضاء الرباط طنجة مراكش أكادير شركة مؤسسة"
).split()
SUPPLIER_WORDS = (
    "Atlas Sahara Oasis Medina Argan Cèdre Zellige Riad Kasbah Souk Andalous Rif Tafilalet "
    "Doukkala Chaouia Tensift Sebou Moulouya Ifrane Toubkal Saïss Gharb Haouz Souss Draa"
).split()
AR_SUPPLIER = "أطلس الصحراء الواحة المدينة الأركان الأرز الزليج الرياض القصبة السوق الأندلس".split()
TRADES = "Télécom Transport Fournitures Bâtiment Conseil Énergie Imprimerie Informatique".split()
TAGS = "urgent bureau audit 2026 litige trimestre banque direction stock export".split()
TYPES = ("invoice",) * 7 + ("receipt",) * 2 + ("credit_note",)
STATES = (
    ("sealed",) * 50 + ("filed",) * 25 + ("needs_review",) * 12 + ("received",) * 8
    + ("removed", "deleting", "deleted") + ("removed", "sealed")
)  # fmt: skip
COLUMNS = (
    "ledger_id", "state", "ingested_at", "basis_date", "document_type", "supplier_name",
    "supplier_ice", "number", "total_ht", "vat", "total_ttc", "currency", "corrected", "tags",
    "content", "supplier_norm", "content_norm", "updated_at",
)  # fmt: skip


@dataclass(frozen=True)
class Supplier:
    name: str
    ice: str


def _suppliers(rng: random.Random) -> list[Supplier]:
    out = []
    for i in range(600):
        if i % 4 == 0:
            name = f"شركة {rng.choice(AR_SUPPLIER)} {rng.choice(AR_SUPPLIER)} SAMPLE"
        else:
            name = f"{rng.choice(SUPPLIER_WORDS)} {rng.choice(TRADES)} SAMPLE SARL"
        out.append(Supplier(name, f"{rng.randrange(10**14):015d}"))
    return out


def _vocabulary(rng: random.Random) -> list[str]:
    """Rare words, as an OCR text has: references, names, codes."""
    syllables = "ka ra mo li za ti fa bu ne so da gu wa ri ho be".split()
    return ["".join(rng.choices(syllables, k=rng.randint(2, 4))) for _ in range(20_000)]


def _content(
    rng: random.Random, rare: list[str], supplier: Supplier, number: str, ttc: Decimal | None
) -> str:
    common = AR_WORDS if "شركة" in supplier.name else FR_WORDS
    words = rng.choices(common, k=rng.randint(40, 90)) + rng.choices(rare, k=rng.randint(5, 20))
    rng.shuffle(words)
    return f"{supplier.name} ICE {supplier.ice} N° {number} {' '.join(words)} TTC {ttc}"


@dataclass(frozen=True)
class Corpus:
    rows: list[tuple[Any, ...]]
    suppliers: list[Supplier]
    rare: list[str]


def _corpus(seed: int) -> Corpus:
    rng = random.Random(seed)  # noqa: S311 - SAMPLE text, reproducible
    suppliers = _suppliers(rng)
    rare = _vocabulary(rng)
    start = date(2019, 1, 1)
    now = datetime(2026, 11, 9, tzinfo=UTC)
    rows = []
    for i in range(DOCS):
        supplier = rng.choice(suppliers)
        number = f"F-{2019 + i % 8}-{i:06d}"
        ttc = None if rng.random() < 0.02 else Decimal(rng.randrange(1000, 10_000_000)) / 100
        ht = None if ttc is None else (ttc / Decimal("1.2")).quantize(Decimal("0.01"))
        content = _content(rng, rare, supplier, number, ttc)[:MAX_CONTENT_CHARS]
        basis = start + timedelta(days=rng.randrange(2870))
        tags = sorted(set(rng.sample(TAGS, rng.randint(1, 2)))) if rng.random() < 0.2 else []
        rows.append(
            (
                uuid.UUID(int=rng.getrandbits(128), version=4), rng.choice(STATES),
                datetime.combine(basis, datetime.min.time(), UTC), basis, rng.choice(TYPES),
                supplier.name, supplier.ice, number, ht, None if ttc is None else ttc - ht, ttc,
                "MAD", [], tags, content, search_normalise(supplier.name),
                search_normalise(content), now,
            )
        )  # fmt: skip
    return Corpus(rows, suppliers, rare)


def _queries(seed: int, corpus: Corpus) -> list[dict[str, Any]]:
    """200 mixed searches: text, text and filters, filters only; relevance and date sorts."""
    rng = random.Random(seed + 1)  # noqa: S311 - SAMPLE queries, reproducible
    bodies: list[dict[str, Any]] = []
    for i in range(QUERIES):
        supplier = rng.choice(corpus.suppliers)
        year = rng.randint(2019, 2026)
        period = {"period_from": f"{year}-01-01", "period_to": f"{year}-06-30"}
        amounts = {"amount_min": "1000.00", "amount_max": f"{rng.randint(2, 50) * 1000}.00"}
        texts = [
            rng.choice(FR_WORDS),  # common
            rng.choice(corpus.rare),  # rare
            rng.choice(AR_WORDS).replace("أ", "ا").replace("ة", "ه"),  # Arabic, folded spelling
            supplier.name.split()[0 if "SARL" in supplier.name else 1],  # part of a name
            supplier.ice,
            f"F-{rng.randint(2019, 2026)}-{rng.randrange(DOCS):06d}",
            f"{rng.choice(FR_WORDS)} {rng.choice(corpus.rare)}",
        ]
        kind = i % 3
        filters: dict[str, Any]
        if kind == 0:
            filters = {"q": rng.choice(texts)}
        elif kind == 1:
            filters = {"q": rng.choice(texts)} | rng.choice(
                [period, amounts, {"status": ["sealed", "filed"]}, {"tags": [rng.choice(TAGS)]},
                 {"document_type": ["invoice"]} | period]
            )  # fmt: skip
        else:
            filters = rng.choice(
                [period, period | amounts, {"supplier": supplier.name.split()[0]},
                 {"supplier": supplier.ice}, {"tags": [rng.choice(TAGS)]} | period,
                 {"document_type": ["receipt"], "status": ["needs_review"]}]
            )  # fmt: skip
        sort = "date_desc" if kind == 2 or i % 2 else "relevance"
        bodies.append({"filters": filters, "sort": sort, "limit": 50})
    return bodies


def _plain(url: str) -> str:
    return make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)


@dataclass(frozen=True)
class Scratch:
    name: str
    password: str


@pytest.fixture
async def scratch_index() -> AsyncIterator[Scratch]:
    """A role and database `index_perf…`, as provisioning makes a tenant's; dropped after."""
    admin = _plain(os.environ["SIJIL_TEST_DATABASE_URL"])
    scratch = Scratch(f"index_perf{uuid.uuid4().hex[:12]}", uuid.uuid4().hex)
    conn = await asyncpg.connect(admin)
    try:
        await conn.execute(f"CREATE ROLE {scratch.name} LOGIN PASSWORD '{scratch.password}'")
        await conn.execute(
            f"CREATE DATABASE {scratch.name} OWNER {scratch.name} TEMPLATE template_index"
        )
        await conn.execute(f"REVOKE ALL ON DATABASE {scratch.name} FROM PUBLIC")
    finally:
        await conn.close()
    try:
        yield scratch
    finally:
        conn = await asyncpg.connect(admin)
        try:
            await conn.execute(f"DROP DATABASE IF EXISTS {scratch.name} WITH (FORCE)")
            await conn.execute(f"DROP ROLE IF EXISTS {scratch.name}")
        finally:
            await conn.close()


async def _seed(scratch: Scratch) -> Corpus:
    url = make_url(os.environ["SIJIL_TEST_DATABASE_URL"]).set(
        username=scratch.name, password=scratch.password, database=scratch.name
    )
    engine = create_async_engine(url)
    try:
        async with engine.begin() as c:
            await index.ensure_schema(c)
    finally:
        await engine.dispose()
    corpus = _corpus(SEED)
    conn = await asyncpg.connect(_plain(url.render_as_string(hide_password=False)))
    try:
        started = time.monotonic()
        await conn.copy_records_to_table("search_doc", records=corpus.rows, columns=COLUMNS)
        await conn.execute("VACUUM ANALYZE search_doc")
        print(f"seeded {DOCS} search_doc rows in {time.monotonic() - started:.1f}s")  # noqa: T201
    finally:
        await conn.close()
    return corpus


async def _member_workspace(http: httpx.AsyncClient, scratch: Scratch) -> uuid.UUID:
    """Sign up over HTTP; write the workspace (its index database the scratch one) in the DB."""
    (await http.get("/v1/auth/csrf")).raise_for_status()
    email = f"it-search-{uuid.uuid4().hex[:10]}@example.com"
    r = await http.post(
        "/v1/auth/signup",
        json={"email": email, "password": PASSWORD, "name": "IT"},
        headers={"X-CSRF-Token": http.cookies["sijil_csrf"]},
    )
    assert r.status_code == 201, r.text
    user_id = uuid.UUID(r.json()["user"]["id"])
    settings = WorkerSettings()
    keys = LocalKeyService(settings.master_key_bytes())
    engine = make_engine(settings.database_url)
    try:
        async with make_sessionmaker(engine)() as db:
            org = Organisation(
                name="IT Search", type=OrgType.business, country="MA", region=settings.region
            )
            db.add(org)
            await db.flush()
            workspace_id = uuid.uuid4()
            kms_key_id = f"ws-{workspace_id}"
            db.add(
                Workspace(
                    id=workspace_id, org_id=org.id, name="IT Search",
                    slug=f"it-search-{uuid.uuid4().hex[:8]}", state=WorkspaceState.active,
                    db_name=f"paperless_perf{uuid.uuid4().hex[:8]}", index_db_name=scratch.name,
                    kms_key_id=kms_key_id,
                )
            )  # fmt: skip
            await db.flush()
            kind = CredentialKind.index_db_password
            envelope = seal(
                scratch.password.encode(), keys, kms_key_id, index.credential_context(
                    workspace_id, kind)
            )  # fmt: skip
            db.add(
                TenantCredential(workspace_id=workspace_id, kind=kind, envelope=envelope.to_bytes())
            )
            db.add(Membership(user_id=user_id, workspace_id=workspace_id, role=Role.member))
            await db.commit()
    finally:
        await engine.dispose()
    return workspace_id


def _server_ms(response: httpx.Response) -> float:
    timing = response.headers["server-timing"]
    assert timing.startswith("app;dur="), timing
    return float(timing.removeprefix("app;dur="))


async def test_search_p95_under_500ms_100k_docs(scratch_index: Scratch) -> None:
    corpus = await _seed(scratch_index)
    async with (
        closing_at_exit() as created,
        httpx.AsyncClient(base_url=CP_URL, timeout=60) as http,
    ):
        workspace_id = await _member_workspace(http, scratch_index)
        created.append(workspace_id)
        url = f"/v1/workspaces/{workspace_id}/search"
        headers = {"X-CSRF-Token": http.cookies["sijil_csrf"]}
        # Warm-up: the engine, the schema check and the first plans.
        r = await http.post(url, json={"filters": {"q": "facture"}}, headers=headers)
        assert r.status_code == 200, r.text
        durations: list[float] = []
        slowest: list[tuple[float, dict[str, Any]]] = []
        hits = 0
        for body in _queries(SEED, corpus):
            r = await http.post(url, json=body, headers=headers)
            assert r.status_code == 200, (body, r.text)
            assert all(item["state"] not in ("removed", "deleting", "deleted")
                       for item in r.json()["items"])  # fmt: skip
            hits += bool(r.json()["items"])
            ms = _server_ms(r)
            durations.append(ms)
            slowest = sorted([*slowest, (ms, body)], key=lambda x: -x[0])[:5]
    cuts = statistics.quantiles(durations, n=100, method="inclusive")
    p50, p95, p99 = cuts[49], cuts[94], cuts[98]
    print(  # noqa: T201
        f"search over {DOCS} docs, {QUERIES} queries ({hits} with results), server-side: "
        f"p50 {p50:.1f} ms, p95 {p95:.1f} ms, p99 {p99:.1f} ms, max {max(durations):.1f} ms"
    )
    for ms, body in slowest:
        print(f"  {ms:.1f} ms {body}")  # noqa: T201
    assert hits > QUERIES // 2, "the queries must find documents to measure anything"
    assert p95 < BUDGET_MS
