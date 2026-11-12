"""Learning from corrections (REC-5): the supplier a reviewer confirmed or corrected becomes the
workspace's supplier record, then a correspondent in its own Paperless.

`learn_from_document` runs after the control plane commits a field edit of the supplier's name
or ICE, a seal (the confirmed values) or a correction note on either (`sijil.learn_from_document`,
queue `default`, the main worker: it holds the index settings and the tenants' service tokens).
Only in this worker's region, and only for a document that a reviewer has seen (`filed`,
`needs_review` or `sealed`): a removed or deleted document never teaches anything. It:
1. holds the ledger row FOR SHARE (a removal waits) while it reads the document's effective
   values (`index.effective_fields`: a correction note wins) and learns them
   (`suppliers.learn`: nothing without an ICE), in one index transaction; the source is
   `correction` when a note corrects the supplier, else `seal` once sealed, else `review`.
   Never the workspace's own ICE (`suppliers.own_ices`, final review I2): it is no supplier,
   so it gets no record and no correspondent.
   Once a reviewer confirmed the document (sealed it, or PATCHed its supplier), the names the
   model read on it with the same ICE (`extraction_run`) join the record's aliases, sanitised
   as hints are: so a supplier printed in another spelling (Arabic, abbreviated) is recognised
   on its next document;
2. audits `supplier.learned` `{source, new}` when the record is new or renamed (never a name or
   an ICE, ruling P5), so a retry audits nothing twice;
3. then, best effort, in the workspace's Paperless with its service token: the correspondent of
   the record's name, created with a literal match on the ICE (`taxonomy.MATCH_LITERAL`) if
   missing, stored on the record, and set on the document.

Paperless unreachable (or the workspace paused) raises for the task to retry by the M3
transient rules; any other Paperless failure is logged by its class only. Neither undoes the
record. `index.purge_document` deletes a record with the last live document carrying its ICE.
"""

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Final
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from sijil_core import audit, suppliers
from sijil_core import index as index_db
from sijil_core.crypto import KeyService
from sijil_core.db.models import (
    TERMINAL_STATES,
    ActorType,
    CredentialKind,
    DocumentLedger,
    DocumentState,
    Organisation,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.fields import parse_field
from sijil_core.index import IndexEngines
from sijil_core.paperless import NAME_MAX, PaperlessClient, PaperlessError, PaperlessUnavailable
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.suppliers import Supplier, SupplierSource
from sijil_worker.extract import _hint_name, _service_token
from sijil_worker.ingest import WorkspaceNotActive
from sijil_worker.settings import ExtractionSettings
from sijil_worker.taxonomy import MATCH_LITERAL

log = logging.getLogger(__name__)

SUPPLIER_FIELDS: Final = ("supplier_name", "supplier_ice")


async def learn_from_document(
    ledger_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    index: IndexEngines,
    paperless: Callable[..., PaperlessClient],
    settings: ExtractionSettings,
    now: datetime,
    workspace_id: UUID | None = None,
) -> bool:
    """See the module docstring. True when a supplier was learnt (or confirmed); False when
    there was nothing to learn here. Raises PaperlessUnavailable / WorkspaceNotActive
    (transient) for the caller to retry."""
    async with sessions() as session:
        found = (
            await session.execute(
                select(DocumentLedger, Workspace, Organisation.region)
                .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(DocumentLedger.id == ledger_id)
                .with_for_update(read=True, of=DocumentLedger)
            )
        ).first()
        if found is None:
            raise LookupError("document not found")
        ledger, workspace, region = found
        if workspace_id is not None and workspace.id != workspace_id:
            raise LookupError("document not found")
        # Before anything leaves the databases: this worker only serves its own region.
        try:
            assert_region(region, settings)
        except RegionMismatch:
            log.warning("learning of document %s skipped: region %s not served", ledger_id, region)
            return False
        if ledger.state in TERMINAL_STATES or ledger.state is DocumentState.received:
            return False
        async with (await index.engine(workspace)).begin() as conn:
            row = await index_db.read_fields(conn, ledger_id)
            if row is None:
                return False
            notes = await index_db.read_corrections(conn, ledger_id)
            effective = index_db.effective_fields(row, notes)
            corrections = [n for n in notes if n.field in SUPPLIER_FIELDS]
            source: SupplierSource
            actor: UUID | None
            if corrections:
                source, actor = "correction", corrections[0].created_by
            elif ledger.sealed_at is not None:
                source, actor = "seal", ledger.sealed_by
            else:
                source, actor = "review", row.updated_by
            ice = effective.get("supplier_ice", (None, False))[0]
            if ice is not None and ice in await suppliers.own_ices(conn, [ice]):
                log.info("learning of document %s skipped: the workspace's own ICE", ledger_id)
                return False
            confirmed = ledger.sealed_at is not None or any(
                row.source.get(f) == "user" for f in SUPPLIER_FIELDS
            )
            spellings = await _spellings(conn, ledger_id, ice) if confirmed else []
            change = await suppliers.learn(
                conn,
                ice=ice,
                name=effective.get("supplier_name", (None, False))[0],
                source=source,
                actor=actor,
                now=now,
                aliases=spellings,
            )
        if change.supplier is None:
            return False
        if change.changed:
            await audit.append(
                session,
                workspace_id=workspace.id,
                action="supplier.learned",
                actor_type=ActorType.system,
                actor_id=None,
                target_type="document",
                target_id=str(ledger_id),
                data={"source": source, "new": change.new},
                now=now,
            )
        await session.commit()
        if workspace.state is not WorkspaceState.active or not workspace.paperless_url:
            raise WorkspaceNotActive  # transient: hibernated or provisioning, retried later
        credential = await session.scalar(
            select(TenantCredential).where(
                TenantCredential.workspace_id == workspace.id,
                TenantCredential.kind == CredentialKind.service_token,
            )
        )
        token = _service_token(credential, keys, workspace)
    await _correspond(
        index, workspace, paperless, token, change.supplier, ledger.paperless_doc_id, ledger_id
    )
    return True


async def _spellings(conn: AsyncConnection, ledger_id: UUID, ice: str | None) -> list[str]:
    """The supplier names the model runs read on the document next to its confirmed ICE,
    sanitised as hints are (`extract._hint_name`)."""
    found: list[str] = []
    for raw in await index_db.read_runs(conn, ledger_id):
        name, run_ice = raw.get("supplier_name"), raw.get("supplier_ice")
        if not isinstance(name, str) or not isinstance(run_ice, str):
            continue
        if ice is None or parse_field("supplier_ice", run_ice) != ice:
            continue
        spelling = _hint_name(parse_field("supplier_name", name) or "")
        if spelling:
            found.append(spelling)
    return found


async def _correspond(
    index: IndexEngines,
    workspace: Workspace,
    paperless: Callable[..., PaperlessClient],
    token: str,
    supplier: Supplier,
    doc_id: int | None,
    ledger_id: UUID,
) -> None:
    """The record's correspondent in the workspace's Paperless, and the document's. Transient
    failures raise; any other is logged and dropped (the record stands)."""
    try:
        async with paperless(workspace.paperless_url, token=token) as client:
            correspondent = await _ours(client, supplier)
            if correspondent is None:
                correspondent = await client.ensure_correspondent(
                    supplier.name, supplier.ice, MATCH_LITERAL
                )
            if correspondent != supplier.paperless_correspondent_id:
                async with (await index.engine(workspace)).begin() as conn:
                    await suppliers.set_correspondent_id(conn, supplier.ice, correspondent)
            if doc_id is not None:
                await client.set_correspondent(doc_id, correspondent)
    except PaperlessUnavailable:
        raise
    except PaperlessError as exc:
        log.warning(
            "paperless correspondent of document %s failed: %s", ledger_id, type(exc).__name__
        )


async def _ours(client: PaperlessClient, supplier: Supplier) -> int | None:
    """The correspondent we created for this record (its stored id), renamed to the record's
    name if needed, while its rule is still ours (literal, on the record's ICE). None when
    there is none, or a tenant user changed its rule: a correspondent we did not make, or no
    longer ours, is never touched."""
    stored = supplier.paperless_correspondent_id
    if stored is None:
        return None
    found = await client.correspondent(stored)
    if (
        found is None
        or found.get("matching_algorithm") != MATCH_LITERAL
        or found.get("match") != supplier.ice
    ):
        return None
    if found.get("name") != supplier.name[:NAME_MAX]:
        await client.rename_correspondent(stored, supplier.name)
    return stored
