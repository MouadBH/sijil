# ruff: noqa: F811
"""The provisional retention at arrival is the lowest policy across the workspace's types (final
review I2): routing and sealing then raise it to the type's own policy, so a type kept for less
than `other` really is, and raising `other` never raises another type.

Through `sijil_core.ingest.accept_file` (arrival) and `sijil_worker.extract._route` (routing).
"""

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from sijil_core import index, ingest
from sijil_core.db.models import (
    ActorType,
    DocumentLedger,
    DocumentSource,
    Organisation,
    RetentionPolicy,
    Workspace,
)
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow
from sijil_worker.extract import _route
from tests.retention.test_seal_lock import (  # noqa: F401 - fixtures of the lock suite
    NOW,
    index_conn,
    index_db_url,
    routing,
)
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import INVOICE, stored

pytestmark = pytest.mark.db

ARRIVED = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
PDF = (Path(__file__).parents[2] / "fixtures" / "formats" / "sample.pdf").read_bytes()


async def _chunks() -> AsyncIterator[bytes]:
    yield PDF + f"\n%SAMPLE {uuid.uuid4()}\n".encode()  # distinct bytes per arrival


async def _arrive(db: AsyncSession, store: MemoryObjectStore, ws: Workspace) -> DocumentLedger:
    async def enqueue(_ledger: uuid.UUID, _workspace: uuid.UUID) -> None:
        return None

    result = await ingest.accept_file(
        sessions=async_sessionmaker(db.bind, expire_on_commit=False), store=store,
        enqueue=enqueue, workspace=ws, region="ma-cas", chunks=_chunks(),
        source=DocumentSource.upload, audit_action="document.uploaded",
        actor_type=ActorType.system, actor_id=None, ip=None, now=ARRIVED,
    )  # fmt: skip
    assert isinstance(result, ingest.Accepted)
    return await _fresh(db, result.document_id)


async def _fresh(db: AsyncSession, row_id: uuid.UUID) -> DocumentLedger:
    found = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == row_id)
        .execution_options(populate_existing=True)
    )
    assert found is not None
    return found


async def _routed(
    routing: Any, db: AsyncSession, store: MemoryObjectStore, conn: AsyncConnection,
    ws: Workspace, row: DocumentLedger, document_type: str,
) -> DocumentLedger:  # fmt: skip
    """The document extracted as `document_type`, dated in its year of arrival, and routed."""
    values = {name: INVOICE.get(name) for name in FIELD_NAMES} | {"document_type": document_type}
    await index.write_extraction(
        conn,
        FieldsRow(
            ledger_id=row.id, document_type=document_type, values=values,
            confidence=dict.fromkeys(FIELD_NAMES, 0.95),
            signals={name: {"runs_agree": True} for name in FIELD_NAMES}, flags=(),
            source=dict.fromkeys(FIELD_NAMES, "extraction"), model="qwen3:4b",
            extracted_at=ARRIVED, updated_at=ARRIVED,
        ),
    )  # fmt: skip
    fields = await index.read_fields(conn, row.id)
    assert fields is not None
    sessions, engines = routing
    assert await _route(sessions, engines, store, ws, fields, NOW) is not None
    return await _fresh(db, row.id)


@pytest.fixture
async def de_workspace(db: AsyncSession, org: Organisation, workspace: Workspace) -> Workspace:
    """A German workspace on its country's preset: invoice 8 years, contract and other 10."""
    org.country = "DE"
    await db.commit()
    return workspace


@pytest.mark.parametrize(
    ("document_type", "final"),
    [("invoice", date(2034, 12, 31)), ("contract", date(2036, 12, 31))],
)
async def test_each_type_ends_at_its_own_policy(
    routing: Any, db: AsyncSession, de_workspace: Workspace, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection, document_type: str, final: date,
) -> None:  # fmt: skip
    row = await _arrive(db, memory_store, de_workspace)
    # Arrival: the lowest policy of the workspace (8 years), in the ledger and on the lock.
    assert row.retention_until == date(2034, 12, 31)
    assert stored(memory_store, row).retain_until == date(2034, 12, 31)
    routed = await _routed(routing, db, memory_store, index_conn, de_workspace, row, document_type)
    assert routed.retention_until == final
    assert stored(memory_store, routed).retain_until == final


async def test_raising_other_never_raises_an_invoice(
    routing: Any, db: AsyncSession, de_workspace: Workspace, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    db.add(
        RetentionPolicy(
            workspace_id=de_workspace.id, document_type="other", years=15, preset_years=10,
            locked=True,
        )
    )  # fmt: skip
    await db.commit()
    row = await _arrive(db, memory_store, de_workspace)
    assert row.retention_until == date(2034, 12, 31)
    routed = await _routed(routing, db, memory_store, index_conn, de_workspace, row, "invoice")
    assert routed.retention_until == date(2034, 12, 31)
    assert stored(memory_store, routed).retain_until == date(2034, 12, 31)
