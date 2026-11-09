# ruff: noqa: F811
"""A retention policy applies to what is computed after it changes: lowering one (back to its
locked preset) never lowers a retention already set, in the ledger or on the object lock."""

from datetime import date
from typing import Any

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from sijil_core.db.models import DocumentLedger, DocumentState, RetentionPolicy, Workspace
from sijil_worker.extract import _route
from tests.retention.test_seal_lock import (  # noqa: F401 - fixtures of the lock suite
    NOW,
    _received,
    index_conn,
    index_db_url,
    routing,
)
from tests.support.memory_store import MemoryObjectStore
from tests.support.sealing import stored

pytestmark = pytest.mark.db


async def _set_invoice_years(db: AsyncSession, workspace: Workspace, years: int) -> None:
    if (
        await db.execute(
            update(RetentionPolicy)
            .where(
                RetentionPolicy.workspace_id == workspace.id,
                RetentionPolicy.document_type == "invoice",
            )
            .values(years=years)
        )
    ).rowcount == 0:
        db.add(
            RetentionPolicy(
                workspace_id=workspace.id,
                document_type="invoice",
                years=years,
                preset_years=10,
                locked=True,
            )
        )
    await db.commit()


async def _ledger(db: AsyncSession, row: DocumentLedger) -> DocumentLedger:
    found = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == row.id)
        .execution_options(populate_existing=True)
    )
    assert found is not None
    return found


async def test_lowered_policy_never_lowers_existing_retention(
    routing: Any, db: AsyncSession, workspace: Workspace, memory_store: MemoryObjectStore,
    index_conn: AsyncConnection,
) -> None:  # fmt: skip
    sessions, engines = routing
    row, fields = await _received(
        db, memory_store, index_conn, workspace, date(2035, 12, 31), "2026-09-30"
    )
    await _set_invoice_years(db, workspace, 12)
    assert await _route(sessions, engines, memory_store, workspace, fields, NOW) is not None
    assert (await _ledger(db, row)).retention_until == date(2038, 12, 31)
    assert stored(memory_store, row).retain_until == date(2038, 12, 31)

    # Back to the preset (allowed: >= preset_years); the same document routed once more.
    await _set_invoice_years(db, workspace, 10)
    await db.execute(
        update(DocumentLedger)
        .where(DocumentLedger.id == row.id)
        .values(state=DocumentState.received)
    )
    await db.commit()
    assert await _route(sessions, engines, memory_store, workspace, fields, NOW) is not None
    assert (await _ledger(db, row)).retention_until == date(2038, 12, 31)
    assert stored(memory_store, row).retain_until == date(2038, 12, 31)
