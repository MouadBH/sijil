"""Builds a period export in-process, as `worker-export` would, for `db`-marked tests: the
originals from the in-memory store, the fields from one throw-away index database, signed with
the test seed (the one `tests/support/worker_tasks.py` puts in SIJIL_EXPORT_SIGNING_KEY)."""

import base64
import hashlib
import uuid
from datetime import date, datetime, timedelta
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sijil_core import index
from sijil_core.db.models import (
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    User,
    Workspace,
)
from sijil_core.export_report import public_key
from sijil_core.fields import FIELD_NAMES, fields_digest
from sijil_core.index import FieldsRow
from sijil_core.settings import CoreSettings
from sijil_core.storage import ObjectRef
from sijil_worker.export import build_export
from tests.support.memory_store import MemoryExportStore, MemoryObjectStore, StoredObject

# Deterministic test-only export signing seed (32 bytes), and its public key.
TEST_SEED = bytes(range(32, 64))
TEST_PUBLIC_KEY_B64 = base64.b64encode(public_key(TEST_SEED)).decode()
_SETTINGS = CoreSettings.model_construct(region="ma-cas", environment="test")
FAR = date(2036, 12, 31)
REASON = "Avoir du fournisseur reçu après le scellement (SAMPLE)."


class OneIndex:
    """Every workspace's index database is the one at `url`."""

    def __init__(self, url: str) -> None:
        self._engine = create_async_engine(url)

    async def engine(self, _workspace: Workspace) -> AsyncEngine:
        return self._engine

    async def aclose(self) -> None:
        await self._engine.dispose()


def invoice_values(**over: str | None) -> dict[str, str | None]:
    values: dict[str, str | None] = dict.fromkeys(FIELD_NAMES)
    values.update(
        document_type="invoice",
        supplier_name="Atlas Télécom SAMPLE SARL",
        supplier_ice="001905837000041",
        number="F-2026-01001",
        total_ttc="1500.00",
        currency="MAD",
    )
    values.update(over)
    return values


async def add_exportable(
    db: AsyncSession,
    store: MemoryObjectStore,
    conn: AsyncConnection,
    ws: Workspace,
    user: User,
    *,
    state: DocumentState = DocumentState.filed,
    arrived: datetime,
    values: dict[str, str | None] | None = None,
    content_kind: str = "pdf",
) -> DocumentLedger:
    """A ledger row in `state`, its original in the store and (with `values`) its fields,
    sealed in the index too when the row is sealed."""
    data = b"%PDF-1.7 SAMPLE " + uuid.uuid4().bytes
    sha = hashlib.sha256(data).hexdigest()
    sealed = state is DocumentState.sealed
    digest = fields_digest(values) if sealed and values is not None else None
    row = DocumentLedger(
        workspace_id=ws.id,
        sha256=sha,
        md5="0" * 32,
        size_bytes=len(data),
        content_kind=content_kind,
        source=DocumentSource.upload,
        object_key=f"t/{sha}",
        object_version_id="v1",
        state=DocumentState.filed if state is DocumentState.removed else state,
        ingest_state=IngestState.consumed,
        retention_until=FAR,
        ingested_at=arrived,
        fields_sha256=digest,
        sealed_at=arrived + timedelta(hours=1) if sealed else None,
        sealed_by=user.id if sealed else None,
    )
    db.add(row)
    await db.commit()
    if state is DocumentState.removed:
        await db.execute(
            text(
                "UPDATE document_ledger SET state = 'removed', removed_at = now(),"
                " removed_by = :u, removal_reason = :r WHERE id = :id"
            ),
            {"id": row.id, "u": user.id, "r": REASON},
        )
        await db.commit()
    store.objects[ObjectRef(row.object_key, "v1")] = StoredObject(
        data=data, mode="GOVERNANCE", retain_until=FAR
    )
    if values is not None:
        await index.write_extraction(
            conn,
            FieldsRow(
                ledger_id=row.id,
                document_type=values.get("document_type"),
                values=values,
                updated_at=arrived,
            ),
        )
        if digest is not None:
            await index.seal_fields(
                conn, row.id, fields_sha256=digest, sealed_at=arrived + timedelta(hours=1)
            )
    return row


async def run_export(
    job_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    index_db_url: str,
    store: MemoryObjectStore,
    exports: MemoryExportStore,
    now: datetime,
    seed: bytes = TEST_SEED,
) -> bool:
    index = OneIndex(index_db_url)
    try:
        return await build_export(
            job_id,
            sessions=sessions,
            index=index,
            originals=store,
            exports=exports,
            settings=_SETTINGS,
            seed=seed,
            now=now,
        )
    finally:
        await index.aclose()
