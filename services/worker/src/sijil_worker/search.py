"""The workers' side of the search index (`sijil_core.search_index`).

Every write here takes the document's ledger row `FOR SHARE` in the control-plane database,
reads its state under that lock and keeps the lock until the index write is committed (the
ledger first, then the index, as the seal does). A seal, a removal or a purge then waits for
the write or happens before it, so a row never gets a state the ledger has moved on from, and a
purged document never gets a row again. Under the lock (`_write`):
- a live document's row is upserted with the state the ledger holds;
- a terminal one (`removed`, `deleting`, `deleted`) whose purge is done (`original_removed_at`
  set and `document.purged` audited) has its rows purged (`index.purge_document`): a row
  written after the purge would otherwise stay for good;
- any other terminal one has its row marked with that state until the purge deletes it.

- `index_document`, right after a commit that changed a document's ledger state in the
  control-plane database alone (ingest at `consumed`, routing after extraction), with the
  content when the caller read it. Best effort: the change is committed already, so a failure
  is logged by its class only and left to the resync.
- `resync_search` (`sijil.search_resync`, daily), for each active workspace of this region:
  first the rows of terminal documents (marked or purged, before any Paperless call), then the
  live documents whose row differs from the ledger, is missing, or has no text while Paperless
  has consumed the document (the text is fetched from Paperless once, fields come from
  `document_fields`), or whose values have drifted from their sources
  (`search_index.search_drift`: the derived columns and tags against the ledger, the
  effective values and `document_tag`, in one snapshot). `search_doc` is a derived row the
  index role may UPDATE (only its DELETE and TRUNCATE are gated), so this is what heals a
  row changed by hand: rewritten from its sources (a live ledger row also revives a row
  marked terminal), and audited `search.repaired` `{columns}` (column names, never values)
  per document. A row whose state merely lags the ledger's is a missed best-effort write: a
  fix, not a repair. With `full=True` and one workspace (`sijil.search_backfill`), every
  live row is rebuilt, text included. A document that fails (Paperless, the index) is logged
  by its class, counted and skipped; the run goes on. A workspace with fixes or failures gets
  one `search.resynced` `{fixed, failed}` event. Last, with `enqueue_embed`, every live
  document (`received`, `needs_review`, `filed`, `sealed`) whose chunks are missing or were
  made from another chunk input (`doc_chunk.source_sha256`, sijil_worker.embed) gets
  `sijil.embed_document` again: a lost message, or a review edit. Not `search_doc.updated_at`,
  which a tag write moves.

Only consumed (or unsupported) documents are indexed: one still being ingested, or whose
ingest failed, has no text and nothing to find. Content goes only to the workspace's own index
database, from its own Paperless; nothing leaves the region.
"""

import logging
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Final
from uuid import UUID

from sqlalchemy import Row, exists, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core import index as index_db
from sijil_core.crypto import Envelope, KeyService, open_envelope
from sijil_core.db.models import (
    TERMINAL_STATES,
    ActorType,
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    DocumentState,
    IngestState,
    Organisation,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.index import IndexEngines
from sijil_core.ingest import Enqueue
from sijil_core.paperless import PaperlessClient
from sijil_core.region import assert_region
from sijil_core.search_index import (
    IndexedState,
    SearchLedgerFacts,
    mark_search_state,
    refresh_search_doc,
    search_drift,
    search_states,
)
from sijil_core.settings import CoreSettings
from sijil_core.text import normalise
from sijil_worker.embed import needs_embedding

log = logging.getLogger(__name__)

RESYNCED: Final = "search.resynced"
REPAIRED: Final = "search.repaired"
PURGED: Final = "document.purged"  # sijil_worker.purge
_INDEXED: Final = (IngestState.consumed, IngestState.unsupported)
_DEAD: Final = frozenset(str(s) for s in TERMINAL_STATES)


async def _purged(session: AsyncSession, ledger: DocumentLedger) -> bool:
    """Whether the document's purge is done: the original removed and `document.purged`."""
    if ledger.original_removed_at is None:
        return False
    found = await session.scalar(
        select(
            exists().where(
                AuditEvent.workspace_id == ledger.workspace_id,
                AuditEvent.action == PURGED,
                AuditEvent.target_type == "document",
                AuditEvent.target_id == str(ledger.id),
            )
        )
    )
    return bool(found)


async def _write(
    sessions: async_sessionmaker[AsyncSession],
    engine: AsyncEngine,
    ledger_id: UUID,
    region: str,
    now: datetime,
    content: str | None,
    *,
    revive: bool = False,
) -> str | None:
    """See the module docstring: the ledger row locked FOR SHARE across the index write.
    Returns the state the row has now, None when it was purged (or the document is unknown).
    `revive` lets a live ledger row rewrite a terminal search row (`upsert_search_doc`)."""
    async with sessions() as session:
        ledger = await session.scalar(
            select(DocumentLedger).where(DocumentLedger.id == ledger_id).with_for_update(read=True)
        )
        if ledger is None:
            return None
        state: str | None = ledger.state
        async with engine.begin() as conn:
            if ledger.state in TERMINAL_STATES:
                if await _purged(session, ledger):
                    await index_db.purge_document(
                        conn, ledger_id, sealed=ledger.sealed_at is not None
                    )
                    state = None
                else:
                    await mark_search_state(conn, ledger_id, ledger.state)
            else:
                facts = SearchLedgerFacts(
                    ledger_id=ledger_id,
                    state=ledger.state,
                    ingested_at=ledger.ingested_at,
                    region=region,
                    now=now,
                )
                await refresh_search_doc(conn, facts, content, revive=revive)
        await session.rollback()  # nothing written here: it only releases the lock
    return state


async def index_document(
    sessions: async_sessionmaker[AsyncSession],
    index: IndexEngines,
    ledger_id: UUID,
    *,
    settings: CoreSettings,
    now: datetime,
    content: str | None = None,
) -> None:
    """See the module docstring. Never raises."""
    try:
        async with sessions() as session:
            found = (
                await session.execute(
                    select(Workspace, Organisation.region)
                    .join(Organisation, Organisation.id == Workspace.org_id)
                    .join(DocumentLedger, DocumentLedger.workspace_id == Workspace.id)
                    .where(DocumentLedger.id == ledger_id)
                )
            ).first()
        if found is None:
            return
        workspace, region = found
        assert_region(region, settings)
        await _write(sessions, await index.engine(workspace), ledger_id, region, now, content)
    except Exception as exc:
        log.warning("search index of document %s not updated: %s", ledger_id, type(exc).__name__)


async def resync_search(
    *,
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    index: IndexEngines,
    paperless: Callable[..., PaperlessClient],
    settings: CoreSettings,
    now: datetime,
    workspace_id: UUID | None = None,
    full: bool = False,
    enqueue_embed: Enqueue | None = None,
) -> int:
    """See the module docstring. Returns how many rows were fixed in all. A workspace that
    fails as a whole (its index or Paperless unreachable) is logged and skipped (the next run
    retries it), unless it is the one asked for."""
    query = (
        select(Workspace, Organisation.region)
        .join(Organisation, Organisation.id == Workspace.org_id)
        .where(Organisation.region == settings.region, Workspace.state == WorkspaceState.active)
        .order_by(Workspace.id)
    )
    if workspace_id is not None:
        query = query.where(Workspace.id == workspace_id)
    async with sessions() as session:
        workspaces = (await session.execute(query)).all()
    total = 0
    for workspace, region in workspaces:
        assert_region(region, settings)
        try:
            fixed, failed, repaired = await _resync_workspace(
                sessions, keys, index, paperless, workspace, region, now, full=full,
                enqueue_embed=enqueue_embed,
            )  # fmt: skip
        except Exception as exc:
            if workspace_id is not None:
                raise
            log.warning(
                "search resync of workspace %s failed: %s", workspace.id, type(exc).__name__
            )
            continue
        if fixed or failed:
            async with sessions() as session:
                for ledger_id, columns in repaired:
                    await audit.append(
                        session,
                        workspace_id=workspace.id,
                        action=REPAIRED,
                        actor_type=ActorType.system,
                        actor_id=None,
                        target_type="document",
                        target_id=str(ledger_id),
                        data={"columns": ",".join(columns)},
                        now=now,
                    )
                await audit.append(
                    session,
                    workspace_id=workspace.id,
                    action=RESYNCED,
                    actor_type=ActorType.system,
                    actor_id=None,
                    data={"fixed": fixed, "failed": failed},
                    now=now,
                )
                await session.commit()
        total += fixed
    return total


def _needs_text(have: IndexedState | None, doc_id: int | None, *, full: bool) -> bool:
    return doc_id is not None and (full or have is None or have.empty)


async def _resync_workspace(
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    index: IndexEngines,
    paperless: Callable[..., PaperlessClient],
    workspace: Workspace,
    region: str,
    now: datetime,
    *,
    full: bool,
    enqueue_embed: Enqueue | None = None,
) -> tuple[int, int, list[tuple[UUID, list[str]]]]:
    """(fixed, failed, repaired: each rewritten row whose values had drifted, and the columns).
    The snapshot below only chooses what to look at: each write reads the ledger again under
    its lock (`_write`)."""
    async with sessions() as session:
        rows = (
            await session.execute(
                select(
                    DocumentLedger.id,
                    DocumentLedger.state,
                    DocumentLedger.ingest_state,
                    DocumentLedger.paperless_doc_id,
                    DocumentLedger.ingested_at,
                ).where(DocumentLedger.workspace_id == workspace.id)
            )
        ).all()
        credential = await session.scalar(
            select(TenantCredential).where(
                TenantCredential.workspace_id == workspace.id,
                TenantCredential.kind == CredentialKind.service_token,
            )
        )
    if not workspace.paperless_url or credential is None:
        raise LookupError("workspace has no Paperless")
    token = open_envelope(
        Envelope.from_bytes(credential.envelope),
        keys,
        f"{workspace.id}:{CredentialKind.service_token}",
    ).decode()
    engine = await index.engine(workspace)
    async with engine.connect() as conn:
        indexed = await search_states(conn)
    drift = await _drift(engine, rows, indexed, region, now)
    fixed = failed = 0
    repaired: list[tuple[UUID, list[str]]] = []
    # Terminal documents first, before any Paperless call can fail or take long.
    for ledger_id, state, _, _, _ in rows:
        have = indexed.get(ledger_id)
        if state not in TERMINAL_STATES or have is None:
            continue
        try:
            after = await _write(sessions, engine, ledger_id, region, now, None)
        except Exception as exc:
            failed += 1
            log.warning("search resync of document %s failed: %s", ledger_id, type(exc).__name__)
            continue
        if after != have.state:
            fixed += 1
    async with paperless(workspace.paperless_url, token=token) as client:
        for ledger_id, state, ingest_state, doc_id, _ in rows:
            if state in TERMINAL_STATES or ingest_state not in _INDEXED:
                continue
            have = indexed.get(ledger_id)
            text_needed = _needs_text(have, doc_id, full=full)
            drifted = drift.get(ledger_id)
            if not (full or text_needed or have is None or have.state != state or drifted):
                continue
            try:
                content = None
                if text_needed and doc_id is not None:
                    content = normalise(await client.document_content(doc_id))
                await _write(sessions, engine, ledger_id, region, now, content, revive=True)
            except Exception as exc:
                failed += 1
                log.warning(
                    "search resync of document %s failed: %s", ledger_id, type(exc).__name__
                )
                continue
            fixed += 1
            # A row whose state lags the ledger's is a missed best-effort write, not a repair.
            if drifted and have is not None and (have.state == state or have.state in _DEAD):
                repaired.append((ledger_id, drifted))
    if enqueue_embed is not None:
        live = [lid for lid, state, _, _, _ in rows if state not in TERMINAL_STATES]
        await _resend_embeddings(engine, workspace.id, live, enqueue_embed)
    return fixed, failed, repaired


async def _drift(
    engine: AsyncEngine,
    rows: Sequence[Row[UUID, DocumentState, IngestState, int | None, datetime]],
    indexed: Mapping[UUID, IndexedState],
    region: str,
    now: datetime,
) -> dict[UUID, list[str]]:
    """The drifted columns of each live document's row (`search_index.search_drift`), read in
    one REPEATABLE READ snapshot of the index."""
    facts = [
        SearchLedgerFacts(
            ledger_id=ledger_id, state=state, ingested_at=ingested_at, region=region, now=now
        )
        for ledger_id, state, ingest_state, _, ingested_at in rows
        if state not in TERMINAL_STATES and ingest_state in _INDEXED and ledger_id in indexed
    ]
    if not facts:
        return {}
    async with engine.connect() as conn:
        await conn.execution_options(isolation_level="REPEATABLE READ")
        async with conn.begin():
            return await search_drift(conn, facts)


async def _resend_embeddings(
    engine: AsyncEngine, workspace_id: UUID, live: list[UUID], enqueue_embed: Enqueue
) -> int:
    """Sends `sijil.embed_document` (deduplicated by the sender) for each live document whose
    chunks are missing or stale (`sijil_worker.embed.needs_embedding`). A document that fails
    is logged by its class and skipped. Returns how many were sent."""
    sent = 0
    for ledger_id in live:
        try:
            async with engine.connect() as conn:
                stale = await needs_embedding(conn, ledger_id)
            if stale:
                await enqueue_embed(ledger_id, workspace_id)
                sent += 1
        except Exception as exc:
            log.warning("embedding check of document %s failed: %s", ledger_id, type(exc).__name__)
    if sent:
        log.info("embeddings re-sent for %d documents of workspace %s", sent, workspace_id)
    return sent
