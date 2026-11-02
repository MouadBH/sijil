"""Inbound email end to end: smtplib -> the `smtp` service -> ledger -> the ingest worker.

Runs in the compose `tests` runner (`make itest`) against the running stack: the `smtp` service
archives the mail, the `worker` consumes the queued rows into the t1 Paperless.
"""

import asyncio
import os
import smtplib
import time
import uuid
from email.message import EmailMessage
from pathlib import Path

import pytest
from sqlalchemy import select

from sijil_core.db.models import DocumentLedger, DocumentSource, IngestState
from sijil_core.storage import ObjectRef
from tests.integration.test_ingest_formats import Harness, harness  # noqa: F401

pytestmark = pytest.mark.integration

SMTP_HOST = os.environ.get("SIJIL_IT_SMTP_HOST", "smtp")
SMTP_PORT = 2525
DOMAIN = os.environ.get("SIJIL_INBOUND_DOMAIN", "in.sijil.local")
PDF = (Path(__file__).parents[2] / "fixtures" / "formats" / "sample.pdf").read_bytes()
CONSUME_TIMEOUT_S = 240


def _send(recipient: str, message: EmailMessage) -> None:
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as client:
        client.send_message(message, from_addr="sender@example.org", to_addrs=[recipient])


async def test_send_to_workspace_address_end_to_end(harness: Harness) -> None:  # noqa: F811
    workspace = await harness.workspace()
    address = f"{workspace.slug}.{workspace.inbound_token}@{DOMAIN}"

    message = EmailMessage()
    message["From"] = "sender@example.org"
    message["To"] = address
    message["Subject"] = f"SAMPLE facture {uuid.uuid4()}"
    message.set_content("SAMPLE: fictitious invoice attached.")
    attachment = PDF + b"\n%" + uuid.uuid4().hex.encode() + b"\n"
    message.add_attachment(
        attachment, maintype="application", subtype="pdf", filename="facture.pdf"
    )

    # An unknown address on the same domain is refused at RCPT.
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as client:
        client.ehlo()
        client.mail("sender@example.org")
        code, _ = client.rcpt(f"{workspace.slug}.aaaaaaaaaa@{DOMAIN}")
        assert code == 550

    await asyncio.to_thread(_send, address, message)

    deadline = time.monotonic() + CONSUME_TIMEOUT_S
    rows: list[DocumentLedger] = []
    while time.monotonic() < deadline:
        async with harness.sessions() as db:
            found = await db.scalars(
                select(DocumentLedger).where(DocumentLedger.workspace_id == workspace.id)
            )
            rows = list(found)
        if len(rows) == 2 and all(r.ingest_state is IngestState.consumed for r in rows):
            break
        await asyncio.sleep(3)
    for row in rows:
        harness.refs.append(ObjectRef(key=row.object_key, version_id=row.object_version_id))

    assert len(rows) == 2
    [eml] = [r for r in rows if r.content_kind == "eml"]
    [pdf] = [r for r in rows if r.content_kind == "pdf"]
    assert pdf.source_email_id == eml.id
    assert {r.source for r in rows} == {DocumentSource.email}
    assert all(r.ingest_state is IngestState.consumed for r in rows)
