"""REC-2/REC-3 with the real in-region model: sample-01 through t1's Paperless and Ollama.

Runs in the compose `tests` runner (`make itest`). It needs SIJIL_EXTRACTION_MODEL pulled into
the `ollama` volume (`make models`) and enough memory for Docker to load it: it is SKIPPED, with
the reason, when the model is not pulled or Ollama cannot load it. That is an environment
limit, not a failure.
"""

import json
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from sijil_core import index as index_db
from sijil_core.crypto import seal
from sijil_core.db.base import utcnow
from sijil_core.db.models import CredentialKind, DocumentState, IngestState, TenantCredential
from sijil_core.fields import FIELD_NAMES
from sijil_core.filetypes import FileKind
from sijil_core.index import IndexEngines
from sijil_core.ollama import OllamaClient, OllamaError, OllamaUnavailable
from sijil_core.paperless import PaperlessClient
from sijil_worker.extract import extract_document
from tests.support.index_db import throwaway_index_db
from tests.support.t1 import Harness

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).parents[2] / "fixtures"
SAMPLE_01 = next(
    i
    for i in json.loads((FIXTURES / "invoices" / "manifest.json").read_text())
    if i["file"] == "sample-01-fr.pdf"
)


@pytest.fixture
def index_url() -> Iterator[str]:
    yield from throwaway_index_db()


async def test_extracts_sample_01_with_real_ollama(harness: Harness, index_url: str) -> None:
    settings = harness.settings
    model = settings.extraction_model
    async with OllamaClient(
        settings.ollama_url, settings.ai_allowed_hosts, num_thread=settings.ollama_num_thread
    ) as probe:
        try:
            pulled = await probe.has_model(model)
        except OllamaError as exc:
            pytest.skip(f"Ollama not reachable at {settings.ollama_url}: {exc}")
    if not pulled:
        pytest.skip(f"model {model} is not pulled: run `make models`")

    data = (FIXTURES / "invoices" / "sample-01-fr.pdf").read_bytes()
    await harness.forget_in_paperless(data)
    workspace = await harness.workspace()
    async with harness.sessions() as db:
        kind = CredentialKind.index_db_password
        envelope = seal(b"unused", harness.keys, workspace.kms_key_id, f"{workspace.id}:{kind}")
        db.add(TenantCredential(workspace_id=workspace.id, kind=kind, envelope=envelope.to_bytes()))
        await db.commit()
    row = await harness.ingest(await harness.ledger(workspace, data, FileKind.pdf))
    assert row.ingest_state is IngestState.consumed

    def factory(_url: URL) -> AsyncEngine:
        return create_async_engine(index_url)

    engines = IndexEngines(settings, harness.keys, harness.sessions, engine_factory=factory)
    started = time.monotonic()
    try:
        async with OllamaClient(
            settings.ollama_url, settings.ai_allowed_hosts, num_thread=settings.ollama_num_thread
        ) as ollama:
            try:
                await extract_document(
                    row.id,
                    sessions=harness.sessions,
                    keys=harness.keys,
                    index=engines,
                    paperless=PaperlessClient,
                    ollama=ollama,
                    store=harness.store,
                    settings=settings,
                    now=utcnow(),
                )
            except OllamaUnavailable as exc:
                if exc.model_load_failed:
                    pytest.skip(f"Ollama cannot load {model} (memory too small?): {exc}")
                raise
        elapsed = time.monotonic() - started
        print(f"EXTRACTION-SECONDS {model} two runs {elapsed:.1f}s")
        async with (await engines.engine(workspace)).connect() as conn:
            fields = await index_db.read_fields(conn, row.id)
    finally:
        await engines.aclose()
    assert fields is not None
    print(f"EXTRACTION-VALUES {dict(fields.values)} flags={fields.flags}")
    assert fields.values["total_ttc"] == SAMPLE_01["ttc"] == "1500.00"
    assert fields.values["supplier_ice"] == SAMPLE_01["supplier_ice"] == "001905837000041"
    # Scored from signals and routed: no longer `received`, retention never lowered.
    print(f"EXTRACTION-CONFIDENCE {dict(fields.confidence)}")
    assert set(fields.confidence) == set(FIELD_NAMES)
    routed = await harness.reload(row)
    assert routed.state in (DocumentState.filed, DocumentState.needs_review)
    assert routed.retention_until >= row.retention_until
    print(f"EXTRACTION-ROUTE {routed.state}")
