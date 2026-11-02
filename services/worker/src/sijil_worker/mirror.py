"""A sealed document's fields copied into its own Paperless as custom fields (design §5.4).

`mirror_fields` runs after a seal commits (the control plane sends `sijil.mirror_fields`). It
reads the sealed index row and sets the `sijil_*` custom fields and the document type on the
Paperless document, so people working in Paperless see what was sealed. It only reads the
ledger and the index row: the sealed record stays ours, Paperless holds a copy.

The values go only to the workspace's own Paperless (its service token, on its tenant network),
never anywhere else, and the audit event names no value (ruling P5). A document that is not
sealed, or of another region, is left alone. Paperless being down raises for the task to retry;
every step is idempotent, so a retry or a second run sets the same values again. A workspace
that is not active (hibernated, provisioning) raises WorkspaceNotActive, retried later.

Index values that do not hash to the ledger's `fields_sha256` are never copied: the mirror
logs `mirror.hash_mismatch` and raises a `fields_hash_mismatch` integrity alert.
"""

import logging
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Final
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core import index as index_db
from sijil_core.crypto import KeyService
from sijil_core.db.models import (
    ActorType,
    CredentialKind,
    DocumentLedger,
    DocumentState,
    Organisation,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.fields import fields_digest
from sijil_core.index import IndexEngines
from sijil_core.paperless import PaperlessClient
from sijil_core.region import RegionMismatch, assert_region
from sijil_worker.drift import FIELDS_HASH_MISMATCH, record_finding
from sijil_worker.extract import _service_token
from sijil_worker.ingest import WorkspaceNotActive
from sijil_worker.settings import ExtractionSettings
from sijil_worker.taxonomy import ensure_taxonomy

log = logging.getLogger(__name__)

SEALED_FIELD: Final = "sijil_sealed"
# (Paperless custom field, its data type, our field name; None for the seal flag itself).
CUSTOM_FIELDS: Final[tuple[tuple[str, str, str | None], ...]] = (
    ("sijil_supplier", "string", "supplier_name"),
    ("sijil_ice", "string", "supplier_ice"),
    ("sijil_number", "string", "number"),
    ("sijil_date", "date", "date"),
    ("sijil_total_ht", "monetary", "total_ht"),
    ("sijil_vat", "monetary", "vat"),
    ("sijil_total_ttc", "monetary", "total_ttc"),
    (SEALED_FIELD, "boolean", None),
)


def _value(data_type: str, ours: str | None, values: Mapping[str, str | None]) -> str | bool | None:
    """Paperless's form of one value: monetary as currency code + amount (`MAD1500.00`), a date
    as ISO, the seal flag as true."""
    if ours is None:
        return True
    value = values.get(ours)
    if value is None:
        return None
    if data_type == "monetary":
        return f"{values.get('currency') or ''}{value}"
    return value


async def mirror_fields(
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
    """See the module docstring. True when the values were set; False when there was nothing
    to do here. Raises PaperlessUnavailable (transient) for the caller to retry."""
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
            raise LookupError("document not found")
        ledger, workspace, region = found
        if workspace_id is not None and workspace.id != workspace_id:
            raise LookupError("document not found")
        # Before anything leaves the databases: this worker only serves its own region.
        try:
            assert_region(region, settings)
        except RegionMismatch:
            log.warning("mirror of document %s skipped: region %s not served", ledger_id, region)
            return False
        if ledger.state is not DocumentState.sealed or ledger.paperless_doc_id is None:
            return False
        if workspace.state is not WorkspaceState.active or not workspace.paperless_url:
            raise WorkspaceNotActive  # transient: hibernated or provisioning, retried later
        credential = await session.scalar(
            select(TenantCredential).where(
                TenantCredential.workspace_id == workspace.id,
                TenantCredential.kind == CredentialKind.service_token,
            )
        )
        token = _service_token(credential, keys, workspace)
    async with (await index.engine(workspace)).connect() as conn:
        row = await index_db.read_fields(conn, ledger_id)
    if row is None or row.sealed_at is None:
        log.warning("mirror of document %s skipped: its index row is not sealed", ledger_id)
        return False
    # The values are recomputed into the digest: only what the ledger sealed is ever copied.
    if fields_digest(row.values) != ledger.fields_sha256:
        log.error("mirror.hash_mismatch: document %s", ledger_id)
        await record_finding(
            sessions, workspace.id, ledger_id, FIELDS_HASH_MISMATCH, {"source": "mirror"}, now=now
        )
        return False
    async with paperless(workspace.paperless_url, token=token) as client:
        ids = {
            name: await client.ensure_custom_field(name, kind) for name, kind, _ in CUSTOM_FIELDS
        }
        type_id = (await ensure_taxonomy(client))[row.document_type] if row.document_type else None
        values = {
            ids[name]: value
            for name, kind, ours in CUSTOM_FIELDS
            if (value := _value(kind, ours, row.values)) is not None
        }
        await client.set_custom_fields(ledger.paperless_doc_id, values, document_type=type_id)
    async with sessions() as session:
        await audit.append(
            session,
            workspace_id=workspace.id,
            action="document.fields_mirrored",
            actor_type=ActorType.system,
            actor_id=None,
            target_type="document",
            target_id=str(ledger_id),
            data={"count": len(values)},
            now=now,
        )
        await session.commit()
    return True
