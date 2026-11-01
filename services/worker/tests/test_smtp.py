"""Inbound email: the SMTP handler driven stage by stage (real Postgres, in-memory store)."""

import hashlib
from email.message import EmailMessage
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core.db.models import (
    AuditEvent,
    DocumentLedger,
    DocumentSource,
    IngestState,
    Organisation,
    Workspace,
    WorkspaceState,
)
from sijil_core.filetypes import MAX_UPLOAD_BYTES
from sijil_core.settings import CoreSettings
from sijil_worker.smtp import SijilSmtpHandler
from tests.support.memory_store import MemoryObjectStore

pytestmark = pytest.mark.db

DOMAIN = "in.sijil.local"
SETTINGS = CoreSettings.model_construct(region="ma-cas", environment="test")
FORMATS = Path(__file__).parents[3] / "fixtures" / "formats"
PDF = (FORMATS / "sample.pdf").read_bytes()
SUBJECT = "Facture-Secrete-Atlas"
SENDER = "alice.confidentielle@example.org"
FILENAME = "contrat-confidentiel.pdf"
OK = "250 OK"


class Enqueued:
    def __init__(self) -> None:
        self.calls: list[tuple[UUID, UUID]] = []

    async def __call__(self, ledger_id: UUID, workspace_id: UUID) -> None:
        self.calls.append((ledger_id, workspace_id))


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


@pytest.fixture
def enqueued() -> Enqueued:
    return Enqueued()


@pytest.fixture
def handler(
    sessions: async_sessionmaker[AsyncSession],
    memory_store: MemoryObjectStore,
    enqueued: Enqueued,
) -> SijilSmtpHandler:
    return SijilSmtpHandler(
        sessions=sessions, store=memory_store, enqueue=enqueued, settings=SETTINGS, domain=DOMAIN
    )


def address(ws: Workspace) -> str:
    return f"{ws.slug}.{ws.inbound_token}@{DOMAIN}"


def mail(*attachments: tuple[bytes, str, str, str], ident: str = "1") -> bytes:
    msg = EmailMessage()
    msg["From"] = SENDER
    msg["To"] = "dest@example.org"
    msg["Subject"] = SUBJECT
    msg["Message-ID"] = f"<unique-{ident}@example.org>"
    msg.set_content("Bonjour, voici les pieces.")
    for data, maintype, subtype, name in attachments:
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=name)
    return msg.as_bytes()


def envelope(content: bytes, *rcpts: str) -> SimpleNamespace:
    return SimpleNamespace(mail_from=SENDER, rcpt_tos=list(rcpts), content=content)


SESSION = SimpleNamespace(peer=("203.0.113.9", 4242))


async def rows(db: AsyncSession) -> list[DocumentLedger]:
    query = select(DocumentLedger).order_by(DocumentLedger.ingested_at)
    found = await db.scalars(query.execution_options(populate_existing=True))
    return list(found)


async def test_email_body_archived_as_eml(
    handler: SijilSmtpHandler, db: AsyncSession, workspace: Workspace, enqueued: Enqueued
) -> None:
    reply = await handler.handle_DATA(None, SESSION, envelope(mail(), address(workspace)))
    assert reply == OK
    [row] = await rows(db)
    assert row.content_kind == "eml"
    assert row.source is DocumentSource.email
    assert row.source_email_id is None
    assert row.ingest_state is IngestState.queued
    assert enqueued.calls == [(row.id, workspace.id)]


async def test_email_attachments_ingested(
    handler: SijilSmtpHandler, db: AsyncSession, workspace: Workspace, enqueued: Enqueued
) -> None:
    pdf2 = PDF + b"\n% second\n"
    content = mail((PDF, "application", "pdf", FILENAME), (pdf2, "application", "pdf", "autre.pdf"))
    assert await handler.handle_DATA(None, SESSION, envelope(content, address(workspace))) == OK
    found = await rows(db)
    assert len(found) == 3
    [eml] = [r for r in found if r.content_kind == "eml"]
    attachments = [r for r in found if r.content_kind == "pdf"]
    assert len(attachments) == 2
    assert {r.source_email_id for r in attachments} == {eml.id}
    assert {r.sha256 for r in attachments} == {
        hashlib.sha256(PDF).hexdigest(),
        hashlib.sha256(pdf2).hexdigest(),
    }
    assert all(r.source is DocumentSource.email for r in found)
    assert len(enqueued.calls) == 3


async def test_unknown_recipient_rejected(handler: SijilSmtpHandler, workspace: Workspace) -> None:
    env = envelope(b"")
    for bad in (
        f"{workspace.slug}.aaaaaaaaaa@{DOMAIN}",
        f"nobody@{DOMAIN}",
        address(workspace).replace(DOMAIN, "other.example"),
    ):
        reply = await handler.handle_RCPT(None, SESSION, env, bad, [])
        assert reply == "550 5.1.1 recipient rejected"
    assert env.rcpt_tos == []
    assert await handler.handle_RCPT(None, SESSION, env, address(workspace), []) == OK
    assert env.rcpt_tos == [address(workspace)]


async def test_inactive_or_foreign_region_workspace_rejected(
    handler: SijilSmtpHandler, db: AsyncSession, workspace: Workspace
) -> None:
    env = envelope(b"")
    addr = address(workspace)
    workspace.state = WorkspaceState.provisioning
    await db.commit()
    reply = await handler.handle_RCPT(None, SESSION, env, addr, [])
    assert reply.startswith("550 5.1.1")
    workspace.state = WorkspaceState.active
    await db.commit()
    assert await handler.handle_RCPT(None, SESSION, env, addr, []) == OK
    elsewhere = SijilSmtpHandler(
        sessions=handler._sessions,
        store=handler._store,
        enqueue=handler._enqueue,
        settings=CoreSettings.model_construct(region="fr-par", environment="test"),
        domain=DOMAIN,
    )
    reply = await elsewhere.handle_RCPT(None, SESSION, envelope(b""), addr, [])
    assert reply.startswith("550 5.1.1")


async def test_oversize_message_rejected(
    handler: SijilSmtpHandler, db: AsyncSession, workspace: Workspace
) -> None:
    big = envelope(b"x" * (MAX_UPLOAD_BYTES + 1), address(workspace))
    assert (await handler.handle_DATA(None, SESSION, big)).startswith("552 5.3.4")
    assert await rows(db) == []


async def test_attachment_already_uploaded_is_duplicate_but_email_accepted(
    handler: SijilSmtpHandler, db: AsyncSession, workspace: Workspace, enqueued: Enqueued
) -> None:
    pdf = (PDF, "application", "pdf", FILENAME)
    first = envelope(mail(pdf), address(workspace))
    assert await handler.handle_DATA(None, SESSION, first) == OK
    # Same attachment in a different email: the .eml is new, the PDF is a duplicate.
    second = envelope(mail(pdf, ident="2"), address(workspace))
    assert await handler.handle_DATA(None, SESSION, second) == OK
    assert len(await rows(db)) == 3  # eml, pdf, eml2
    assert len(enqueued.calls) == 3
    actions = list(await db.scalars(select(AuditEvent.action)))
    assert actions.count("document.duplicate_rejected") == 1


async def test_unsupported_attachment_skipped_and_audited(
    handler: SijilSmtpHandler, db: AsyncSession, workspace: Workspace
) -> None:
    content = mail((b"MZ\x90\x00 not allowed", "application", "octet-stream", "setup.exe"))
    assert await handler.handle_DATA(None, SESSION, envelope(content, address(workspace))) == OK
    [row] = await rows(db)
    assert row.content_kind == "eml"
    events = await db.scalars(select(AuditEvent).where(AuditEvent.action == "email.rejected"))
    [event] = list(events)
    assert event.data == {"reason": "unsupported_type", "kind": "unknown"}
    assert event.target_id == str(row.id)


async def test_multiple_recipients_each_get_a_copy(
    handler: SijilSmtpHandler,
    db: AsyncSession,
    workspace: Workspace,
    org: Organisation,
    enqueued: Enqueued,
) -> None:
    first_addr, first_id = address(workspace), workspace.id
    other = Workspace(
        org_id=org.id,
        name="Autre",
        slug="autre-ws",
        state=WorkspaceState.active,
        db_name="paperless_autre",
        index_db_name="index_autre",
        kms_key_id="kek-autre",
    )
    db.add(other)
    await db.commit()
    other_addr, other_id = address(other), other.id
    content = mail((PDF, "application", "pdf", FILENAME))
    env = envelope(content, first_addr, other_addr, first_addr.upper())
    assert await handler.handle_DATA(None, SESSION, env) == OK
    found = await rows(db)
    assert len(found) == 4  # eml + pdf in each of two workspaces; the repeated address is one
    assert {r.workspace_id for r in found} == {first_id, other_id}
    assert len(enqueued.calls) == 4


async def test_malformed_mime_is_archived_without_crashing(
    handler: SijilSmtpHandler, workspace: Workspace
) -> None:
    broken = (
        b"From: a@example.org\r\nTo: b@example.org\r\nContent-Type: multipart/mixed; "
        b'boundary="zz"\r\n\r\n--zz\r\nContent-Type: application/pdf\r\n'
        b"Content-Disposition: attachment; filename=\r\nContent-Transfer-Encoding: base64\r\n\r\n"
        b"!!!not base64!!!\r\n--zz"
    )
    reply = await handler.handle_DATA(None, SESSION, envelope(broken, address(workspace)))
    assert reply[:1] in "25"
    garbage = envelope(b"\x00\x01 no headers at all", address(workspace))
    assert (await handler.handle_DATA(None, SESSION, garbage)).startswith("554 5.6.0")


async def test_store_failure_is_a_temporary_error(
    handler: SijilSmtpHandler,
    workspace: Workspace,
    memory_store: MemoryObjectStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def boom(*_: object, **__: object) -> None:
        raise OSError("store down")

    monkeypatch.setattr(memory_store, "put_original", boom)
    reply = await handler.handle_DATA(None, SESSION, envelope(mail(), address(workspace)))
    assert reply.startswith("451 4.3.0")


async def test_no_subject_or_sender_in_shared_db(
    handler: SijilSmtpHandler, db: AsyncSession, workspace: Workspace
) -> None:
    content = mail(
        (PDF, "application", "pdf", FILENAME), (b"MZ junk", "application", "x-x", "setup.exe")
    )
    await handler.handle_DATA(None, SESSION, envelope(content, address(workspace)))
    dump: list[str] = []
    for table in ("document_ledger", "audit_event"):
        result = await db.execute(text(f"SELECT t::text FROM {table} t"))  # noqa: S608
        dump += [r[0] for r in result]
    blob = "\n".join(dump).lower()
    assert dump
    for secret in (SUBJECT, SENDER, "example.org", FILENAME, "setup", "dest@"):
        assert secret.lower() not in blob
