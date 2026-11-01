"""Drift check against the real compose stack (Review Focus 5).

Upload -> consumed in the t1 Paperless -> the document is deleted in Paperless with the ADMIN
credential (bypassing our API and the service user's missing delete right) -> the drift check
raises `paperless_missing` and writes an `integrity.drift` audit event.

Observed with Paperless 3.4.1 (recorded in the M2 report): `/api/documents/{id}/metadata/`
carries `original_checksum`, the SHA-256 of the original, equal to the ledger's `sha256`.
"""

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select

from sijil_core.db.base import utcnow
from sijil_core.db.models import AuditEvent, DocumentLedger, IngestState, IntegrityAlert
from sijil_core.filetypes import FileKind
from sijil_core.paperless import PaperlessClient
from sijil_worker.drift import DriftReport, drift_check
from sijil_worker.runtime.base import ADMIN_USERNAME
from tests.integration.test_ingest_formats import FORMATS, T1_URL, Harness, harness  # noqa: F401

pytestmark = pytest.mark.integration

PDF = (Path(FORMATS) / "sample.pdf").read_bytes()


async def _drift(harness: Harness, workspace_id: uuid.UUID) -> DriftReport:  # noqa: F811
    return await drift_check(
        workspace_id,
        sessions=harness.sessions,
        keys=harness.keys,
        store=harness.store,
        paperless=PaperlessClient,
        settings=harness.settings,
        now=utcnow(),
    )


async def _consumed(harness: Harness) -> tuple[DocumentLedger, uuid.UUID]:  # noqa: F811
    data = PDF + b"\n%" + uuid.uuid4().hex.encode() + b"\n"  # unique bytes: a fresh document
    workspace = await harness.workspace()
    row = await harness.ingest(await harness.ledger(workspace, data, FileKind.pdf))
    assert row.ingest_state is IngestState.consumed
    assert row.paperless_doc_id is not None
    return row, workspace.id


async def test_clean_document_is_ok_and_checksum_field_is_the_sha256(
    harness: Harness,  # noqa: F811
) -> None:
    row, workspace_id = await _consumed(harness)
    async with PaperlessClient(T1_URL, token=harness.token) as client:
        assert row.paperless_doc_id is not None
        meta = await client.document_metadata(row.paperless_doc_id)
    assert meta is not None
    print("PAPERLESS-METADATA-KEYS", sorted(meta))
    assert str(meta["original_checksum"]).lower() == row.sha256
    assert await _drift(harness, workspace_id) == DriftReport(checked=1, problems=0)
    async with harness.sessions() as db:
        actions = list(
            await db.scalars(
                select(AuditEvent.action)
                .where(AuditEvent.workspace_id == workspace_id)
                .order_by(AuditEvent.seq)
            )
        )
    assert actions[-1] == "integrity.check_ok"


async def test_drift_check_flags_missing_paperless_document(
    harness: Harness,  # noqa: F811
) -> None:
    row, workspace_id = await _consumed(harness)
    admin = (ADMIN_USERNAME, os.environ["SIJIL_IT_T1_ADMIN_PASSWORD"])
    async with PaperlessClient(T1_URL, basic=admin) as client:
        assert row.paperless_doc_id is not None
        await client.delete_document(row.paperless_doc_id)

    assert await _drift(harness, workspace_id) == DriftReport(checked=1, problems=1)

    async with harness.sessions() as db:
        (alert,) = list(
            await db.scalars(
                select(IntegrityAlert).where(IntegrityAlert.workspace_id == workspace_id)
            )
        )
        events = list(
            await db.scalars(
                select(AuditEvent)
                .where(
                    AuditEvent.workspace_id == workspace_id, AuditEvent.action.like("integrity.%")
                )
                .order_by(AuditEvent.seq)
            )
        )
    assert (alert.ledger_id, alert.kind, alert.resolved_at) == (row.id, "paperless_missing", None)
    assert [e.action for e in events] == ["integrity.drift", "integrity.drift_summary"]
    assert events[0].target_id == str(row.id)
    assert events[0].data == {"kind": "paperless_missing"}
