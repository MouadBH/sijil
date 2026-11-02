"""Seeding for the review and seal tests: a ledger row, its original in the in-memory store and
its fields row in a throw-away index database. SAMPLE values only (fictitious company and ICE).
"""

import hashlib
import uuid
from collections.abc import Mapping
from datetime import UTC, date, datetime

from fastapi import FastAPI
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, create_async_engine

from sijil_core import index
from sijil_core.crypto import seal
from sijil_core.db.models import (
    CredentialKind,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    TenantCredential,
    Workspace,
)
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow, IndexEngines
from sijil_core.storage import ObjectRef
from tests.support.memory_store import MemoryObjectStore, StoredObject

ARRIVAL = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
PROVISIONAL = date(2036, 12, 31)  # arrival in 2026, MA
# SAMPLE: a complete invoice of a fictitious supplier, as canonical strings.
INVOICE: dict[str, str | None] = {
    "document_type": "invoice",
    "supplier_name": "Atlas Télécom SAMPLE SARL",
    "supplier_ice": "001905837000041",
    "date": "2026-09-30",
    "number": "F-2026-01001",
    "total_ht": "1250.00",
    "vat": "250.00",
    "vat_rate": "20",
    "total_ttc": "1500.00",
    "currency": "MAD",
}


async def add_index_credential(db: AsyncSession, app: FastAPI, workspace: Workspace) -> None:
    """The index database password the control plane's `IndexEngines` reads."""
    kind = CredentialKind.index_db_password
    envelope = seal(b"pw", app.state.keys, workspace.kms_key_id, f"{workspace.id}:{kind}")
    db.add(TenantCredential(workspace_id=workspace.id, kind=kind, envelope=envelope.to_bytes()))
    await db.commit()


def engines_on(app: FastAPI, index_db_url: str) -> IndexEngines:
    """The real engine cache, every workspace's database being the throw-away one."""

    def factory(_url: URL) -> AsyncEngine:
        return create_async_engine(index_db_url)

    return IndexEngines(
        app.state.settings, app.state.keys, app.state.sessionmaker, engine_factory=factory
    )


async def add_document(
    db: AsyncSession,
    store: MemoryObjectStore,
    conn: AsyncConnection,
    workspace: Workspace,
    *,
    state: DocumentState = DocumentState.filed,
    values: Mapping[str, str | None] | None = INVOICE,
    document_type: str | None = "invoice",
    ingested_at: datetime = ARRIVAL,
    retention_until: date = PROVISIONAL,
    flags: tuple[str, ...] = (),
) -> DocumentLedger:
    """A ledger row, its GOVERNANCE-locked original and (unless `values` is None) its fields."""
    sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    row = DocumentLedger(
        workspace_id=workspace.id,
        sha256=sha,
        md5="0" * 32,
        size_bytes=10,
        content_kind="pdf",
        source=DocumentSource.upload,
        object_key=f"t/{sha}",
        object_version_id="v1",
        state=state,
        ingest_state=IngestState.consumed,
        paperless_doc_id=7,
        retention_until=retention_until,
        ingested_at=ingested_at,
    )
    db.add(row)
    await db.commit()
    store.objects[ObjectRef(row.object_key, row.object_version_id)] = StoredObject(
        data=b"SAMPLE", mode="GOVERNANCE", retain_until=retention_until
    )
    if values is not None:
        full = {name: values.get(name) for name in FIELD_NAMES}
        await index.write_extraction(
            conn,
            FieldsRow(
                ledger_id=row.id,
                document_type=document_type,
                values=full,
                confidence=dict.fromkeys(FIELD_NAMES, 0.95),
                signals={name: {"runs_agree": True} for name in FIELD_NAMES},
                flags=flags,
                source=dict.fromkeys(FIELD_NAMES, "extraction"),
                model="qwen3:4b",
                extracted_at=ingested_at,
                updated_at=ingested_at,
            ),
        )
    return row


def stored(store: MemoryObjectStore, row: DocumentLedger) -> StoredObject:
    return store.objects[ObjectRef(row.object_key, row.object_version_id)]
