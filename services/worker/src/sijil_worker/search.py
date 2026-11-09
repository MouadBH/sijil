"""The workers' side of the search index (`sijil_core.search_index`).

- `index_document`, right after a commit that changed a document's ledger state in the
  control-plane database alone (ingest at `consumed`, routing after extraction): the row
  takes the state the ledger now holds, its fields and, when given, the content. Best effort:
  the change is committed already, so a failure is logged by its class only and left to the
  resync.
- `resync_search` (`sijil.search_resync`, daily): for each active workspace of this region,
  compares every `search_doc` row's state with the ledger. A live document whose row differs
  is upserted again (its stored content kept), one that has none gets one (content fetched
  from Paperless once, fields from `document_fields`), and a row left live for a document now
  `removed`, `deleting` or `deleted` is marked so until the purge deletes it. With
  `full=True` and one workspace (`sijil.search_backfill`), every live row is rebuilt, content
  included. A workspace with fixes gets one `search.resynced` `{fixed}` event.

Only consumed (or unsupported) documents are indexed: one still being ingested, or whose
ingest failed, has no text and nothing to find. Content goes only to the workspace's own index
database, from its own Paperless; nothing leaves the region.
"""

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Final
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core.crypto import Envelope, KeyService, open_envelope
from sijil_core.db.models import (
    TERMINAL_STATES,
    ActorType,
    CredentialKind,
    DocumentLedger,
    IngestState,
    Organisation,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.index import IndexEngines
from sijil_core.paperless import PaperlessClient
from sijil_core.region import assert_region
from sijil_core.search_index import (
    SearchLedgerFacts,
    mark_search_state,
    refresh_search_doc,
    search_states,
)
from sijil_core.settings import CoreSettings
from sijil_core.text import normalise

log = logging.getLogger(__name__)

RESYNCED: Final = "search.resynced"
_INDEXED: Final = (IngestState.consumed, IngestState.unsupported)


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
                    select(DocumentLedger, Workspace, Organisation.region)
                    .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                    .join(Organisation, Organisation.id == Workspace.org_id)
                    .where(DocumentLedger.id == ledger_id)
                )
            ).first()
        if found is None:
            return
        ledger, workspace, region = found
        assert_region(region, settings)
        if ledger.state in TERMINAL_STATES:
            return  # removed meanwhile: its purge deletes whatever row there is
        facts = SearchLedgerFacts(
            ledger_id=ledger.id,
            state=ledger.state,
            ingested_at=ledger.ingested_at,
            region=region,
            now=now,
        )
        async with (await index.engine(workspace)).begin() as conn:
            await refresh_search_doc(conn, facts, content)
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
) -> int:
    """See the module docstring. Returns how many rows were fixed in all. A workspace that
    fails is logged and skipped (the next run retries it), unless it is the one asked for."""
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
            fixed = await _resync_workspace(
                sessions, keys, index, paperless, workspace, region, now, full=full
            )
        except Exception as exc:
            if workspace_id is not None:
                raise
            log.warning(
                "search resync of workspace %s failed: %s", workspace.id, type(exc).__name__
            )
            continue
        if fixed:
            async with sessions() as session:
                await audit.append(
                    session,
                    workspace_id=workspace.id,
                    action=RESYNCED,
                    actor_type=ActorType.system,
                    actor_id=None,
                    data={"fixed": fixed},
                    now=now,
                )
                await session.commit()
        total += fixed
    return total


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
) -> int:
    async with sessions() as session:
        rows = (
            await session.execute(
                select(
                    DocumentLedger.id,
                    DocumentLedger.state,
                    DocumentLedger.ingest_state,
                    DocumentLedger.ingested_at,
                    DocumentLedger.paperless_doc_id,
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
    fixed = 0
    async with paperless(workspace.paperless_url, token=token) as client:
        for ledger_id, state, ingest_state, ingested_at, doc_id in rows:
            have = indexed.get(ledger_id)
            if state in TERMINAL_STATES:
                if have is not None and have != state:
                    async with engine.begin() as conn:
                        await mark_search_state(conn, ledger_id, state)
                    fixed += 1
                continue
            if ingest_state not in _INDEXED or (have == state and not full):
                continue
            content = None
            if (have is None or full) and doc_id is not None:
                content = normalise(await client.document_content(doc_id))
            facts = SearchLedgerFacts(
                ledger_id=ledger_id, state=state, ingested_at=ingested_at, region=region, now=now
            )
            async with engine.begin() as conn:
                await refresh_search_doc(conn, facts, content)
            fixed += 1
    return fixed
