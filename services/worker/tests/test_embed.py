"""Embeddings of a routed, sealed or corrected document (`sijil.embed_document`).

Ollama is an httpx MockTransport that answers a deterministic vector per text; the index is a
throw-away database reached through a real `IndexEngines`. The isolation test provisions two
real tenant index databases (role and database per workspace, as `ensure_tenant_databases`
does) and connects as each tenant's own role.
"""

import hashlib
import json
import secrets
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime
from typing import Any

import asyncpg
import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import URL, make_url
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
    TenantCredential,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.fields import FIELD_NAMES, fields_digest
from sijil_core.index import EMBEDDING_DIM, FieldsRow, IndexEngines
from sijil_core.ollama import OllamaClient, OllamaUnavailable
from sijil_core.search_index import SearchLedgerFacts, mark_search_state, refresh_search_doc
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.embed import (
    MAX_WINDOWS,
    OVERLAP_CHARS,
    WINDOW_CHARS,
    chunk_input,
    chunk_texts,
    content_windows,
    embed_document,
    fields_line,
    record_embedding_failed,
    source_digest,
)
from sijil_worker.runtime.base import TenantSpec
from sijil_worker.runtime.postgres import drop_tenant_databases, ensure_tenant_databases
from sijil_worker.settings import WorkerSettings
from tests.support.index_db import throwaway_index_db

pytestmark = pytest.mark.db

OLLAMA = "http://ollama:11434"
NOW = datetime(2026, 11, 9, 10, 0, tzinfo=UTC)
ARRIVAL = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
SETTINGS = WorkerSettings.model_construct(
    region="ma-cas", environment="test", embedding_model="bge-m3"
)
# SAMPLE values (fictitious supplier and ICE).
VALUES: dict[str, str | None] = dict.fromkeys(FIELD_NAMES) | {
    "document_type": "invoice",
    "supplier_name": "Atlas Télécom SAMPLE SARL",
    "supplier_ice": "001905837000041",
    "date": "2026-09-30",
    "number": "F-2026-01001",
    "total_ht": "1250.00",
    "vat": "250.00",
    "total_ttc": "1500.00",
    "currency": "MAD",
}
CONTENT = "SAMPLE / ÉCHANTILLON Atlas Télécom SAMPLE SARL Facture F-2026-01001 Total TTC 1 500,00"


def _vector(text_: str) -> list[float]:
    """A deterministic unit-ish vector per text (what a real model would vary by meaning)."""
    digest = hashlib.sha256(text_.encode()).digest()
    return [digest[i % 32] / 255 + 0.001 for i in range(EMBEDDING_DIM)]


class FakeEmbedder:
    def __init__(self, response: httpx.Response | None = None) -> None:
        self.inputs: list[list[str]] = []
        self.hosts: list[str] = []
        self.response = response

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.hosts.append(request.url.host)
        body = json.loads(request.content)
        assert request.url.path == "/api/embed"
        assert body["model"] == "bge-m3"
        self.inputs.append(body["input"])
        if self.response is not None:
            return self.response
        return httpx.Response(200, json={"embeddings": [_vector(t) for t in body["input"]]})

    def client(self) -> OllamaClient:
        return OllamaClient(OLLAMA, {"ollama"}, transport=httpx.MockTransport(self))


# Chunks ------------------------------------------------------------------------------------


def test_chunks_fields_line_and_windows() -> None:
    line = fields_line(VALUES)
    assert line == (
        "invoice | Atlas Télécom SAMPLE SARL | 001905837000041 | F-2026-01001 | 2026-09-30 | "
        "1250.00 / 250.00 / 1500.00 | MAD"
    )
    assert fields_line(dict.fromkeys(FIELD_NAMES)) == ""

    words = " ".join(f"mot{i:04d}" for i in range(2000))  # 15,999 characters
    windows = content_windows(words)
    assert 1 < len(windows) <= MAX_WINDOWS
    assert all(len(w) <= WINDOW_CHARS for w in windows)
    assert all(len(w) >= WINDOW_CHARS - 200 for w in windows[:-1])  # about 800
    for first, second in zip(windows, windows[1:], strict=False):
        # Consecutive windows share about 100 characters, and no word is cut.
        tail = first[-OVERLAP_CHARS:]
        assert second.split()[1] in tail
        assert all(w.startswith("mot") and len(w) == 7 for w in second.split())
    # At most 30 windows: a long document keeps its first ~21,000 characters.
    assert len(content_windows(words * 3)) == MAX_WINDOWS
    assert content_windows("") == content_windows("   \n ") == []
    assert content_windows("Facture courte") == ["Facture courte"]

    texts = chunk_texts(VALUES, words)
    assert texts[0] == line
    assert texts[1:] == windows
    assert chunk_texts(dict.fromkeys(FIELD_NAMES), "") == []

    # The digest of the chunk input: any change to a chunk, or to how the text is cut, moves it.
    digest = source_digest(texts)
    assert len(digest) == 64 and digest == source_digest(list(texts))
    assert source_digest(["a b", "c"]) != source_digest(["a", "b c"])
    assert source_digest([*texts[:-1], texts[-1] + "."]) != digest


# Fixtures ----------------------------------------------------------------------------------


@pytest.fixture
def keys() -> LocalKeyService:
    return LocalKeyService(b"k" * 32)


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


@pytest.fixture
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


def _credential(ws: Workspace, keys: LocalKeyService, kind: CredentialKind, secret: str) -> Any:
    envelope = seal(secret.encode(), keys, ws.kms_key_id, f"{ws.id}:{kind}")
    return TenantCredential(workspace_id=ws.id, kind=kind, envelope=envelope.to_bytes())


@pytest.fixture
async def ready(db: AsyncSession, workspace: Workspace, keys: LocalKeyService) -> Workspace:
    db.add(_credential(workspace, keys, CredentialKind.index_db_password, "pw"))
    await db.commit()
    return workspace


@pytest.fixture
async def engines(
    index_db_url: str, keys: LocalKeyService, sessions: async_sessionmaker[AsyncSession]
) -> AsyncIterator[IndexEngines]:
    def factory(_url: URL) -> AsyncEngine:
        return create_async_engine(index_db_url)

    found = IndexEngines(SETTINGS, keys, sessions, engine_factory=factory)
    yield found
    await found.aclose()


async def _ledger(
    db: AsyncSession,
    ws: Workspace,
    state: DocumentState = DocumentState.filed,
    retention_until: date = date(2036, 12, 31),
) -> DocumentLedger:
    """A `filed` or `sealed` row (SAMPLE content). A past `retention_until` lets an approved
    deletion move it to `deleting`."""
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    sealed = state is DocumentState.sealed
    by = await _actor(db) if sealed else None
    row = DocumentLedger(
        workspace_id=ws.id, sha256=sha, md5="0" * 32, size_bytes=6, content_kind="pdf",
        source=DocumentSource.upload, object_key=f"k/{sha}", object_version_id="v1",
        state=state, ingest_state=IngestState.consumed, paperless_doc_id=7,
        retention_until=retention_until, ingested_at=ARRIVAL,
        fields_sha256=fields_digest(VALUES) if sealed else None,
        sealed_at=NOW if sealed else None, sealed_by=by, object_locked_at=NOW if sealed else None,
    )  # fmt: skip
    db.add(row)
    await db.commit()
    return row


async def _actor(session: AsyncSession) -> uuid.UUID:
    user = User(email=f"{uuid.uuid4().hex[:8]}@example.com", name="SAMPLE", password_hash="x")
    session.add(user)
    await session.commit()
    return user.id


async def _move(session: AsyncSession, ledger_id: uuid.UUID, state: DocumentState) -> None:
    """`filed` -> `removed`, or `sealed` -> `deleting`, as the ledger's transitions allow."""
    sql = (
        "UPDATE document_ledger SET state = 'removed', removed_at = now(), removed_by = :u, "
        "removal_reason = 'Doublon déposé par erreur (SAMPLE)' WHERE id = :id"
        if state is DocumentState.removed
        else "UPDATE document_ledger SET state = 'deleting', deletion_approved_at = now(), "
        "deletion_approved_by = :u WHERE id = :id"
    )
    await session.execute(text(sql), {"id": ledger_id, "u": await _actor(session)})
    await session.commit()


async def _indexed(
    engine: AsyncEngine, ledger_id: uuid.UUID, state: str = DocumentState.filed, *, sealed: bool
) -> None:
    """The document's fields row and its live search row, as routing (or a seal) leaves them."""
    async with engine.begin() as conn:
        await index.write_extraction(
            conn,
            FieldsRow(
                ledger_id=ledger_id, document_type="invoice", values=VALUES,
                source=dict.fromkeys(VALUES, "extraction"), updated_at=ARRIVAL,
            ),
        )  # fmt: skip
        if sealed:
            await index.seal_fields(
                conn, ledger_id, fields_sha256=fields_digest(VALUES), sealed_at=NOW
            )
        facts = SearchLedgerFacts(
            ledger_id=ledger_id, state=state, ingested_at=ARRIVAL, region="ma-cas", now=NOW
        )
        await refresh_search_doc(conn, facts, CONTENT)


async def _chunks(engine: AsyncEngine, ledger_id: uuid.UUID) -> list[tuple[int, str, str]]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT n, text, model, vector_dims(embedding) FROM doc_chunk "
                "WHERE ledger_id = :id ORDER BY n"
            ),
            {"id": ledger_id},
        )
        found = rows.all()
    assert all(dims == EMBEDDING_DIM for *_, dims in found)
    return [(n, t, m) for n, t, m, _ in found]


async def _embed(
    ledger_id: uuid.UUID,
    ws: Workspace,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
    ollama: FakeEmbedder,
) -> bool:
    async with ollama.client() as client:
        return await embed_document(
            ledger_id,
            sessions=sessions,
            index=engines,
            ollama=client,
            settings=SETTINGS,
            workspace_id=ws.id,
        )


# Embedding ---------------------------------------------------------------------------------


async def test_embed_writes_vectors_into_workspace_index_db(
    db: AsyncSession,
    ready: Workspace,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
) -> None:
    row = await _ledger(db, ready)
    engine = await engines.engine(ready)
    await _indexed(engine, row.id, sealed=False)
    ollama = FakeEmbedder()
    assert await _embed(row.id, ready, sessions, engines, ollama)
    expected = chunk_texts(VALUES, CONTENT)
    assert ollama.inputs == [expected]  # one request, every chunk
    assert await _chunks(engine, row.id) == [(n, t, "bge-m3") for n, t in enumerate(expected)]
    async with engine.connect() as conn:
        # Every chunk carries the digest of the whole chunk input, which the resync compares.
        sources = await conn.scalars(
            text("SELECT DISTINCT source_sha256 FROM doc_chunk WHERE ledger_id = :id"),
            {"id": row.id},
        )
        assert list(sources) == [source_digest(expected)]
        assert await chunk_input(conn, row.id) == expected
        nearest = await conn.scalar(
            text(
                "SELECT ledger_id FROM doc_chunk ORDER BY embedding <=> CAST(:q AS vector) LIMIT 1"
            ),
            {"q": str(_vector(expected[1]))},
        )
    assert nearest == row.id


async def test_embed_reads_the_effective_values(
    db: AsyncSession,
    ready: Workspace,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
) -> None:
    """A correction note's value replaces the sealed one in the fields line."""
    row = await _ledger(db, ready, DocumentState.sealed)
    engine = await engines.engine(ready)
    await _indexed(engine, row.id, DocumentState.sealed, sealed=True)
    async with engine.begin() as conn:
        await index.add_correction(
            conn, row.id, "number", "F-2026-01002", "Numéro mal lu (SAMPLE)",
            created_by=uuid.uuid4(), now=NOW,
        )  # fmt: skip
    ollama = FakeEmbedder()
    assert await _embed(row.id, ready, sessions, engines, ollama)
    (first, *_), *_ = ollama.inputs
    assert "F-2026-01002" in first and "F-2026-01001" not in first


async def test_reembed_replaces_chunks(
    db: AsyncSession,
    ready: Workspace,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
) -> None:
    row = await _ledger(db, ready)
    engine = await engines.engine(ready)
    await _indexed(engine, row.id, sealed=False)
    assert await _embed(row.id, ready, sessions, engines, FakeEmbedder())
    before = await _chunks(engine, row.id)
    assert len(before) == 2
    # The fields change (a review edit), then the document is embedded again.
    async with engine.begin() as conn:
        await index.update_fields(
            conn, row.id, {"supplier_name": "Rif Imprimerie SAMPLE"}, source="user",
            updated_by=uuid.uuid4(), now=NOW,
        )  # fmt: skip
        facts = SearchLedgerFacts(
            ledger_id=row.id, state="filed", ingested_at=ARRIVAL, region="ma-cas", now=NOW
        )
        await refresh_search_doc(conn, facts, "Texte court SAMPLE")
    assert await _embed(row.id, ready, sessions, engines, FakeEmbedder())
    after = await _chunks(engine, row.id)
    assert [t for _, t, _ in after] == chunk_texts(
        VALUES | {"supplier_name": "Rif Imprimerie SAMPLE"}, "Texte court SAMPLE"
    )
    assert after != before


@pytest.mark.parametrize("state", [DocumentState.removed, DocumentState.deleting])
async def test_embed_skips_terminal_document(
    db: AsyncSession,
    ready: Workspace,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
    state: DocumentState,
) -> None:
    """Nothing is read, sent to the model or written for a removed or deleting document."""
    sealed = state is DocumentState.deleting
    if sealed:
        row = await _ledger(db, ready, DocumentState.sealed, retention_until=date(2016, 12, 31))
    else:
        row = await _ledger(db, ready)
    engine = await engines.engine(ready)
    await _indexed(engine, row.id, row.state, sealed=sealed)
    await _move(db, row.id, state)
    ollama = FakeEmbedder()
    assert not await _embed(row.id, ready, sessions, engines, ollama)
    assert ollama.inputs == []
    assert await _chunks(engine, row.id) == []


async def test_embed_skips_a_document_marked_terminal_while_the_model_ran(
    db: AsyncSession,
    ready: Workspace,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = await _ledger(db, ready)
    engine = await engines.engine(ready)
    await _indexed(engine, row.id, sealed=False)
    real = OllamaClient.embed

    async def removed_meanwhile(self: OllamaClient, model: str, inputs: list[str]) -> Any:
        found = await real(self, model, inputs)
        async with sessions() as session:
            await _move(session, row.id, DocumentState.removed)
        async with engine.begin() as conn:
            await mark_search_state(conn, row.id, DocumentState.removed)
        return found

    monkeypatch.setattr(OllamaClient, "embed", removed_meanwhile)
    assert not await _embed(row.id, ready, sessions, engines, FakeEmbedder())
    assert await _chunks(engine, row.id) == []


@pytest.mark.parametrize("ws_state", [WorkspaceState.hibernated, WorkspaceState.closing])
async def test_embed_skips_a_workspace_that_is_not_active(
    db: AsyncSession,
    ready: Workspace,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
    ws_state: WorkspaceState,
) -> None:
    row = await _ledger(db, ready)
    engine = await engines.engine(ready)
    await _indexed(engine, row.id, sealed=False)
    ready.state = ws_state
    await db.commit()
    ollama = FakeEmbedder()
    assert not await _embed(row.id, ready, sessions, engines, ollama)
    assert ollama.inputs == []


async def test_embed_skips_another_region_and_another_workspace(
    db: AsyncSession,
    ready: Workspace,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
) -> None:
    row = await _ledger(db, ready)
    await _indexed(await engines.engine(ready), row.id, sealed=False)
    ollama = FakeEmbedder()
    async with ollama.client() as client:
        elsewhere = SETTINGS.model_copy(update={"region": "eu-par"})
        assert not await embed_document(
            row.id, sessions=sessions, index=engines, ollama=client, settings=elsewhere,
            workspace_id=ready.id,
        )  # fmt: skip
        assert not await embed_document(
            row.id, sessions=sessions, index=engines, ollama=client, settings=SETTINGS,
            workspace_id=uuid.uuid4(),
        )  # fmt: skip
    assert ollama.inputs == []


async def test_embed_with_the_wrong_dimensions_is_bad_output(
    db: AsyncSession,
    ready: Workspace,
    sessions: async_sessionmaker[AsyncSession],
    engines: IndexEngines,
) -> None:
    from sijil_core.ollama import OllamaBadOutput

    row = await _ledger(db, ready)
    engine = await engines.engine(ready)
    await _indexed(engine, row.id, sealed=False)
    short = httpx.Response(200, json={"embeddings": [[0.1] * 768, [0.2] * 768]})
    with pytest.raises(OllamaBadOutput):
        await _embed(row.id, ready, sessions, engines, FakeEmbedder(short))
    assert await _chunks(engine, row.id) == []


async def test_record_embedding_failed_audits_the_code_only(
    db: AsyncSession, ready: Workspace, sessions: async_sessionmaker[AsyncSession]
) -> None:
    row = await _ledger(db, ready)
    await record_embedding_failed(
        row.id, "embedding.ai_unavailable", sessions=sessions, settings=SETTINGS, now=NOW,
        workspace_id=ready.id,
    )  # fmt: skip
    events = (
        await db.scalars(
            select(AuditEvent).where(
                AuditEvent.workspace_id == ready.id, AuditEvent.target_id == str(row.id)
            )
        )
    ).all()
    assert [(e.action, e.data) for e in events] == [
        ("document.embedding_failed", {"code": "embedding.ai_unavailable"})
    ]


# Isolation ---------------------------------------------------------------------------------


@pytest.fixture
def tenants(db: AsyncSession) -> Iterator[tuple[TenantSpec, TenantSpec]]:
    """Two workspaces' index databases, each owned by its own role, PUBLIC revoked, cloned from
    `template_index` (pgvector), as provisioning makes them."""
    import asyncio
    import os

    admin = os.environ["SIJIL_TEST_DATABASE_URL"]
    specs = []
    for _ in range(2):
        ref = uuid.uuid4().hex[:10]
        specs.append(
            TenantSpec(
                workspace_id=uuid.uuid4(),
                ref=ref,
                db_name=f"paperless_{ref}",
                db_password=secrets.token_urlsafe(16),
                index_db_name=f"index_{ref}",
                index_db_password=secrets.token_urlsafe(16),
                secret_key="unused",
                admin_password="unused",
            )  # fmt: skip
        )
    for spec in specs:
        asyncio.run(ensure_tenant_databases(admin, spec))
    try:
        yield specs[0], specs[1]
    finally:
        for spec in specs:
            asyncio.run(drop_tenant_databases(admin, spec.db_name, spec.index_db_name))


def _as(role: str, password: str, database: str) -> str:
    import os

    base = make_url(os.environ["SIJIL_TEST_DATABASE_URL"])
    url = base.set(username=role, password=password, database=database)
    return url.render_as_string(hide_password=False).replace("postgresql+asyncpg", "postgresql")


async def test_embeddings_stay_in_workspace_index_db(
    db: AsyncSession,
    workspace: Workspace,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    tenants: tuple[TenantSpec, TenantSpec],
    migrated_db: Any,
) -> None:
    a, b = tenants
    workspace.index_db_name = a.index_db_name
    db.add(_credential(workspace, keys, CredentialKind.index_db_password, a.index_db_password))
    await db.commit()

    def as_role_a(url: URL) -> AsyncEngine:
        # The engine IndexEngines builds (role A, database A), on the test cluster's host.
        test = make_url(_as(a.index_db_name, a.index_db_password, a.index_db_name))
        assert (url.username, url.database) == (a.index_db_name, a.index_db_name)
        return create_async_engine(test.set(drivername="postgresql+asyncpg"))

    engines = IndexEngines(SETTINGS, keys, sessions, engine_factory=as_role_a)
    try:
        row = await _ledger(db, workspace)
        engine = await engines.engine(workspace)
        await _indexed(engine, row.id, sealed=False)
        ollama = FakeEmbedder()
        assert await _embed(row.id, workspace, sessions, engines, ollama)
        assert len(await _chunks(engine, row.id)) == 2
        assert set(ollama.hosts) == {"ollama"}  # the only host the client may call
    finally:
        await engines.aclose()

    # B's role cannot open A's database; B's own database holds none of A's vectors.
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await asyncpg.connect(_as(b.index_db_name, b.index_db_password, a.index_db_name))
    own = await asyncpg.connect(_as(b.index_db_name, b.index_db_password, b.index_db_name))
    try:
        assert not await own.fetchval("SELECT to_regclass('doc_chunk') IS NOT NULL")
    finally:
        await own.close()
    # The control plane's app role cannot open A's database, and its own has no vector at all.
    app = make_url(migrated_db.app_url)
    assert app.username is not None and app.password is not None
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await asyncpg.connect(_as(app.username, app.password, a.index_db_name))
    async with sessions() as session:
        vectors = await session.scalar(
            text(
                "SELECT count(*) FROM pg_attribute att JOIN pg_type t ON t.oid = att.atttypid "
                "WHERE t.typname = 'vector' AND att.attnum > 0"
            )
        )
        assert vectors == 0
        assert await session.scalar(text("SELECT to_regclass('doc_chunk')")) is None

    # Document content goes to the allow-listed Ollama only.
    for host in ("http://api.example.com", "http://10.0.0.5:11434", "http://ollama.evil:11434"):
        with pytest.raises(ValueError, match=r"^ai\.host_not_allowed$"):
            OllamaClient(host, SETTINGS.ai_allowed_hosts)


# Task --------------------------------------------------------------------------------------

ARGS = {
    "ledger_id": "00000000-0000-0000-0000-000000000001",
    "workspace_id": "00000000-0000-0000-0000-000000000002",
}


@pytest.fixture
def eager_app() -> None:
    settings = WorkerSettings(
        region="ma-cas", environment="test", redis_url="redis://127.0.0.1:1/0",
        master_key="a" * 43 + "=", database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
        provisioner_database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
        tenant_image="img:1", compose_project="sijil", tessdata_volume="t",
    )  # fmt: skip
    app = make_celery(settings)
    app.conf.task_always_eager = True
    app.set_current()


def _recording(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> tuple[list[str], list[tuple[str, str]]]:
    attempts: list[str] = []
    failed: list[tuple[str, str]] = []

    async def embed(ledger_id: str, workspace_id: str) -> None:
        attempts.append(ledger_id)
        raise error

    async def record(ledger_id: str, workspace_id: str, code: str) -> None:
        assert workspace_id == ARGS["workspace_id"]
        failed.append((ledger_id, code))

    monkeypatch.setattr(tasks, "_embed", embed)
    monkeypatch.setattr(tasks, "_embed_failed", record)
    monkeypatch.setattr(tasks, "_pending_embed_quietly", lambda _op: None)
    return attempts, failed


def test_embed_task_retries_transient_then_records_failure(
    eager_app: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempts, failed = _recording(monkeypatch, OllamaUnavailable("ai.unavailable: HTTP 503"))
    result = tasks.embed_document_task.apply(kwargs=ARGS)
    assert isinstance(result.result, OllamaUnavailable)
    assert len(attempts) == tasks.EXTRACT_MAX_RETRIES + 1
    assert failed == [(ARGS["ledger_id"], "embedding.ai_unavailable")]


def test_embed_task_final_error_records_failure_at_once(
    eager_app: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from sijil_core.ollama import OllamaBadOutput

    attempts, failed = _recording(monkeypatch, OllamaBadOutput("ai.bad_output: not a vector"))
    tasks.embed_document_task.apply(kwargs=ARGS)
    assert len(attempts) == 1
    assert failed == [(ARGS["ledger_id"], "embedding.ai_bad_output")]


def test_embed_task_is_on_the_embedding_queue() -> None:
    assert tasks.EMBED_TASK == ("sijil.embed_document", "embedding")
    assert tasks.embed_document_task.queue == "embedding"  # type: ignore[attr-defined]
