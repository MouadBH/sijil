"""Embeddings with the real in-region model (slow: `make acceptance`).

The 12 SAMPLE invoices (fixtures/invoices/manifest.json, fictitious suppliers) are chunked like
`sijil.embed_document` chunks them, embedded by the compose `ollama` with SIJIL_EMBEDDING_MODEL
(bge-m3) and stored through `index.replace_chunks` in a throw-away index database (schema v5,
HNSW cosine). A French and an Arabic sample have 1024 dimensions; a question worded unlike the
invoice (another phrasing, no shared number) ranks its document first by cosine distance.

SKIPPED, with the reason, when Ollama is unreachable or the model is not pulled (`make models`).
"""

import json
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from sijil_core import index
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import EMBEDDING_DIM, FieldsRow
from sijil_core.ollama import OllamaClient, OllamaError
from sijil_core.search_index import SearchLedgerFacts, refresh_search_doc
from sijil_worker.embed import chunk_texts
from sijil_worker.settings import WorkerSettings
from tests.support.index_db import throwaway_index_db

pytestmark = [pytest.mark.integration, pytest.mark.slow]

FIXTURES = Path(__file__).parents[2] / "fixtures"
MANIFEST: list[dict[str, Any]] = json.loads((FIXTURES / "invoices" / "manifest.json").read_text())
SUPPLIERS = {s["ice"]: s for s in json.loads((FIXTURES / "suppliers.json").read_text())}
NOW = datetime(2026, 11, 9, 10, 0, tzinfo=UTC)
# Each question is worded unlike its invoice and shares no number with it.
QUESTIONS = {
    "sample-09-fr.pdf": "combien a coûté l'hôtel des participants au séminaire ?",
    "sample-10-ar.pdf": "كم دفعنا لفاتورة الكهرباء في شهر شتنبر؟",
}


def _text(item: dict[str, Any]) -> tuple[dict[str, str | None], str]:
    """The SAMPLE invoice's values and the text Paperless would give for it."""
    supplier = SUPPLIERS.get(item["supplier_ice"], {"name": "SAMPLE SARL", "name_ar": "SAMPLE"})
    values: dict[str, str | None] = dict.fromkeys(FIELD_NAMES) | {
        "document_type": "invoice",
        "supplier_name": supplier["name"],
        "supplier_ice": item["supplier_ice"],
        "date": item["date"],
        "number": item["number"],
        "total_ht": item["ht"],
        "vat": item["tva"],
        "total_ttc": item["ttc"],
        "currency": item["currency"],
    }
    if item["lang"] == "ar":
        content = (
            f"SAMPLE {supplier['name_ar']} التعريف الموحد للمقاولة {item['supplier_ice']} "
            f"فاتورة رقم {item['number']} التاريخ {item['date']} البيان {item['item']} "
            f"المجموع دون احتساب الرسوم {item['ht']} درهم الضريبة على القيمة المضافة "
            f"{item['tva']} المجموع مع احتساب الرسوم {item['ttc']} درهم"
        )
    else:
        content = (
            f"SAMPLE / ÉCHANTILLON {supplier['name']} ICE : {item['supplier_ice']} "
            f"FACTURE N° {item['number']} Date : {item['date']} Désignation : {item['item']} "
            f"Total HT {item['ht']} MAD TVA {item['tva']} MAD Total TTC {item['ttc']} MAD"
        )
    return values, content


@pytest.fixture
def index_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
async def conn(index_url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(index_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as connection:
        await index.ensure_schema(connection)
        yield connection
    await engine.dispose()


async def test_real_bge_m3_embeddings_rank_the_similar_document_first(
    worker_settings: WorkerSettings, conn: AsyncConnection
) -> None:
    settings = worker_settings
    model = settings.embedding_model
    async with OllamaClient(
        settings.ollama_url, settings.ai_allowed_hosts, num_thread=settings.ollama_num_thread
    ) as ollama:
        try:
            pulled = await ollama.has_model(model) or await ollama.has_model(f"{model}:latest")
        except OllamaError as exc:
            pytest.skip(f"Ollama not reachable at {settings.ollama_url}: {exc}")
        if not pulled:
            pytest.skip(f"model {model} is not pulled: run `make models`")

        ids: dict[str, uuid.UUID] = {}
        started = time.monotonic()
        for item in MANIFEST:
            ledger_id = ids[item["file"]] = uuid.uuid4()
            values, content = _text(item)
            await index.write_extraction(
                conn,
                FieldsRow(
                    ledger_id=ledger_id, document_type="invoice", values=values,
                    source=dict.fromkeys(values, "extraction"), updated_at=NOW,
                ),
            )  # fmt: skip
            facts = SearchLedgerFacts(
                ledger_id=ledger_id, state="filed", ingested_at=NOW, region="ma-cas", now=NOW
            )
            await refresh_search_doc(conn, facts, content)
            texts = chunk_texts(values, content)
            vectors = await ollama.embed(model, texts)
            if item["file"] in ("sample-01-fr.pdf", "sample-02-ar.pdf"):
                dims = {len(v) for v in vectors}
                print(f"EMBEDDING-DIMS {item['file']} {sorted(dims)}")
                assert dims == {EMBEDDING_DIM} == {1024}
            chunks = list(zip(texts, vectors, strict=True))
            assert await index.replace_chunks(conn, ledger_id, model, chunks)
        print(f"EMBEDDING-SECONDS {len(MANIFEST)} documents {time.monotonic() - started:.1f}s")

        for file, question in QUESTIONS.items():
            [query] = await ollama.embed(model, [question])
            ranked = (
                await conn.execute(
                    text(
                        "SELECT ledger_id, min(embedding <=> CAST(:q AS vector)) AS distance "
                        "FROM doc_chunk GROUP BY ledger_id ORDER BY distance"
                    ),
                    {"q": str(query)},
                )
            ).all()
            names = {v: k for k, v in ids.items()}
            order = [(names[lid], round(float(d), 4)) for lid, d in ranked]
            rank = [name for name, _ in order].index(file) + 1
            print(f"EMBEDDING-RANK {file} rank={rank} top3={order[:3]}")
            assert rank == 1
