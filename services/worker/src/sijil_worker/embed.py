"""A document's chunks embedded by the in-region model, for Ask's retrieval by meaning.

`embed_document` (`sijil.embed_document`, queue `embedding`, on the extraction worker: the
same Ollama, one request at a time) runs after a document is routed `filed` or `needs_review`,
sealed or corrected. Only in this worker's region (`assert_region`), only in an `active`
workspace and only for a document that is not terminal, as extraction; anything else is a
no-op. It:
1. reads the document's effective field values (`index.effective_fields`: a correction note
   wins) and its normalised text (`search_doc.content`) from the workspace's index database,
   never from Paperless;
2. cuts them into chunks (`chunk_texts`): the fields line, then windows of the text;
3. embeds them all in one `/api/embed` call (`SIJIL_EMBEDDING_MODEL`, bge-m3, 1024
   dimensions); no lock is held meanwhile;
4. replaces the document's chunks (`index.replace_chunks`, gate `sijil.reembed`) with the
   ledger row held FOR SHARE, as the search writes do: a removal or a purge waits for the
   write or happens before it, and the schema function writes only for a live search row, so
   a removed document never gets chunks back.

The text and the vectors go only to the workspace's own index database and the allow-listed
Ollama. Transient failures raise for the task to retry (the M3 rules); a final one is audited
`document.embedding_failed` `{code}` (`record_embedding_failed`).
"""

import logging
from collections.abc import Mapping
from datetime import datetime
from typing import Final
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core import index as index_db
from sijil_core.db.models import (
    TERMINAL_STATES,
    ActorType,
    DocumentLedger,
    Organisation,
    Workspace,
    WorkspaceState,
)
from sijil_core.index import EMBEDDING_DIM, IndexEngines
from sijil_core.ollama import OllamaBadOutput, OllamaClient
from sijil_core.region import RegionMismatch, assert_region
from sijil_worker.settings import ExtractionSettings

log = logging.getLogger(__name__)

EMBEDDING_FAILED: Final = "document.embedding_failed"
WINDOW_CHARS: Final = 800
OVERLAP_CHARS: Final = 100
MAX_WINDOWS: Final = 30
# A window ends at the last space after this many characters (no word cut in two), else at
# WINDOW_CHARS.
_MIN_CUT: Final = WINDOW_CHARS - 200
_LINE_FIELDS: Final = ("document_type", "supplier_name", "supplier_ice", "number", "date")
_TOTALS: Final = ("total_ht", "vat", "total_ttc")


def fields_line(values: Mapping[str, str | None]) -> str:
    """`type | supplier | ICE | number | date | HT / VAT / TTC | currency`, "" when every
    value is empty."""
    parts = [values.get(name) or "" for name in _LINE_FIELDS]
    totals = [values.get(name) or "" for name in _TOTALS]
    parts.append(" / ".join(totals) if any(totals) else "")
    parts.append(values.get("currency") or "")
    return " | ".join(parts) if any(parts) else ""


def content_windows(content: str) -> list[str]:
    """Windows of about WINDOW_CHARS, each sharing OVERLAP_CHARS with the next, cut at a space
    when there is one; at most MAX_WINDOWS (the rest of a long text is left out)."""
    windows: list[str] = []
    start = 0
    while start < len(content) and len(windows) < MAX_WINDOWS:
        end = min(start + WINDOW_CHARS, len(content))
        if end < len(content):
            space = content.rfind(" ", start + _MIN_CUT, end + 1)
            if space > 0:
                end = space
        window = content[start:end].strip()
        if window:
            windows.append(window)
        if end >= len(content):
            break
        start = end - OVERLAP_CHARS
        # The overlap starts on a word too: after the first space of the shared part.
        space = content.find(" ", start, end)
        if space >= 0:
            start = space + 1
    return windows


def chunk_texts(values: Mapping[str, str | None], content: str) -> list[str]:
    """The fields line (when any value is set), then the text's windows: chunk 0, 1, ..."""
    line = fields_line(values)
    return ([line] if line else []) + content_windows(content)


async def _job(
    sessions: async_sessionmaker[AsyncSession],
    settings: ExtractionSettings,
    ledger_id: UUID,
    workspace_id: UUID,
) -> Workspace | None:
    """The workspace when there is something to embed here, else None."""
    async with sessions() as session:
        found = (
            await session.execute(
                select(DocumentLedger.state, Workspace, Organisation.region)
                .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(DocumentLedger.id == ledger_id, Workspace.id == workspace_id)
            )
        ).first()
    if found is None:
        return None
    state, workspace, region = found
    try:
        assert_region(region, settings)
    except RegionMismatch:
        log.warning("embedding of document %s skipped: region %s not here", ledger_id, region)
        return None
    if state in TERMINAL_STATES or workspace.state is not WorkspaceState.active:
        return None
    return workspace


async def embed_document(
    ledger_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    index: IndexEngines,
    ollama: OllamaClient,
    settings: ExtractionSettings,
    workspace_id: UUID,
) -> bool:
    """See the module docstring. True when the chunks were replaced. Raises OllamaUnavailable
    (transient), OllamaBadOutput / OllamaError (final) and database errors."""
    workspace = await _job(sessions, settings, ledger_id, workspace_id)
    if workspace is None:
        return False
    engine = await index.engine(workspace)
    async with engine.connect() as conn:
        content = (
            await conn.execute(
                text(
                    "SELECT content FROM search_doc WHERE ledger_id = :id "
                    "AND state NOT IN ('removed', 'deleting', 'deleted')"
                ),
                {"id": ledger_id},
            )
        ).scalar()
        if content is None:
            return False  # not indexed yet (the resync writes it), or marked terminal
        fields = await index_db.read_fields(conn, ledger_id)
        corrections = await index_db.read_corrections(conn, ledger_id)
    values: dict[str, str | None] = {}
    if fields is not None:
        values = {k: v for k, (v, _) in index_db.effective_fields(fields, corrections).items()}
    texts = chunk_texts(values, content)
    vectors = await ollama.embed(settings.embedding_model, texts)
    if any(len(vector) != EMBEDDING_DIM for vector in vectors):
        raise OllamaBadOutput("ai.bad_output: embedding dimensions")
    async with sessions() as session:
        state = await session.scalar(
            select(DocumentLedger.state)
            .where(DocumentLedger.id == ledger_id)
            .with_for_update(read=True)
        )
        if state is None or state in TERMINAL_STATES:
            return False
        async with engine.begin() as conn:
            written = await index_db.replace_chunks(
                conn, ledger_id, settings.embedding_model, list(zip(texts, vectors, strict=True))
            )
        await session.rollback()  # nothing written here: it only releases the lock
    return written


async def record_embedding_failed(
    ledger_id: UUID,
    code: str,
    *,
    sessions: async_sessionmaker[AsyncSession],
    settings: ExtractionSettings,
    now: datetime,
    workspace_id: UUID,
) -> None:
    """Retries ran out (or a final error): audit `document.embedding_failed` `{code}`. The
    document keeps its earlier chunks, if any. Another region's or workspace's is left alone."""
    async with sessions() as session:
        region = await session.scalar(
            select(Organisation.region)
            .join(Workspace, Workspace.org_id == Organisation.id)
            .join(DocumentLedger, DocumentLedger.workspace_id == Workspace.id)
            .where(DocumentLedger.id == ledger_id, Workspace.id == workspace_id)
        )
        if region is None:
            return
        try:
            assert_region(region, settings)
        except RegionMismatch:
            return
        log.warning("embedding of document %s failed: %s", ledger_id, code)
        await audit.append(
            session,
            workspace_id=workspace_id,
            action=EMBEDDING_FAILED,
            actor_type=ActorType.system,
            actor_id=None,
            target_type="document",
            target_id=str(ledger_id),
            data={"code": code},
            now=now,
        )
        await session.commit()
