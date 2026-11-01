"""Inbound email capture: an in-region SMTP receiver that archives mail per workspace.

Each workspace has the address `{slug}.{inbound_token}@{SIJIL_INBOUND_DOMAIN}`. At RCPT the token
is looked up (the workspace must be active and in this region); at DATA the raw message is
archived as an `.eml` document and every attachment as its own document, all through the shared
`accept_file` path, once per recipient workspace.

Nothing about the message but its hashes, size and kind reaches the shared database: no subject,
sender, recipient or filename is stored, audited or logged. Replies:

- `550 5.1.1` unknown, inactive or out-of-region recipient (RCPT);
- `552 5.3.4` message over the upload limit;
- `554 5.6.0` the message is not a usable email (permanent);
- `451 4.3.0` anything transient (database, object store): the sender retries;
- `250 OK` otherwise. A duplicate or an unsupported attachment never fails the email.
"""

import asyncio
import email.policy
import logging
import signal
import threading
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from email.message import Message
from email.parser import BytesParser
from typing import Any
from uuid import UUID

from aiosmtpd.controller import Controller
from celery import Celery
from pydantic import RedisDsn
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit, ingest
from sijil_core.db.base import utcnow
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    ActorType,
    DocumentSource,
    Organisation,
    Workspace,
    WorkspaceState,
)
from sijil_core.filetypes import MAX_UPLOAD_BYTES
from sijil_core.inbound import parse
from sijil_core.region import RegionMismatch, assert_region
from sijil_core.settings import CoreSettings
from sijil_core.storage import ObjectStore, S3ObjectStore, StorageSettings

log = logging.getLogger(__name__)

SMTP_PORT = 2525
CHUNK_BYTES = 1024 * 1024
INGEST_TASK = "sijil.ingest_document"
INGEST_QUEUE = "ingest"

REJECT_RECIPIENT = "550 5.1.1 recipient rejected"
TOO_LARGE = "552 5.3.4 message too large"
BAD_MESSAGE = "554 5.6.0 message rejected"
TEMPORARY = "451 4.3.0 temporary failure, try again later"
OK = "250 OK"


class SmtpSettings(StorageSettings):
    """What the receiver needs and nothing more: no master key, no Docker, no tenant settings."""

    redis_url: RedisDsn
    database_url: str
    inbound_domain: str


@dataclass(frozen=True)
class _Target:
    workspace: Workspace
    country: str
    region: str


async def _chunks(data: bytes) -> AsyncIterator[bytes]:
    for start in range(0, len(data), CHUNK_BYTES):
        yield data[start : start + CHUNK_BYTES]


def _payload(part: Message) -> bytes | None:
    if part.get_content_type() == "message/rfc822":
        inner = part.get_payload()
        if isinstance(inner, list) and inner and isinstance(inner[0], Message):
            return inner[0].as_bytes()
        return None
    payload = part.get_payload(decode=True)
    return payload if isinstance(payload, bytes) else None


def attachments_of(content: bytes) -> list[bytes]:
    """The bytes of every attachment part. A part that cannot be read is skipped, never fatal."""
    try:
        message = BytesParser(policy=email.policy.compat32).parsebytes(content)
        parts = list(message.walk())
    except Exception:
        return []
    found: list[bytes] = []
    for part in parts:
        data: bytes | None = None
        try:
            if part.is_multipart() and part.get_content_type() != "message/rfc822":
                continue
            disposition = part.get_content_disposition()
            wanted = disposition == "attachment" or (
                disposition == "inline"
                and part.get_content_maintype() != "text"
                and part.get_filename() is not None
            )
            if not wanted:
                continue
            data = _payload(part)
        except Exception:
            log.debug("unreadable mime part skipped")
        if data is not None:
            found.append(data)
    return found


class SijilSmtpHandler:
    def __init__(
        self,
        *,
        sessions: async_sessionmaker[AsyncSession],
        store: ObjectStore,
        enqueue: ingest.Enqueue,
        settings: CoreSettings,
        domain: str,
    ) -> None:
        self._sessions = sessions
        self._store = store
        self._enqueue = enqueue
        self._settings = settings
        self._domain = domain

    async def _resolve(self, address: str) -> _Target | None:
        token = parse(address, self._domain)
        if token is None:
            return None
        async with self._sessions() as db:
            row = (
                await db.execute(
                    select(Workspace, Organisation.country, Organisation.region)
                    .join(Organisation, Organisation.id == Workspace.org_id)
                    .where(
                        Workspace.inbound_token == token,
                        Workspace.state == WorkspaceState.active,
                    )
                )
            ).first()
        if row is None:
            return None
        workspace, country, region = row
        try:
            assert_region(region, self._settings)
        except RegionMismatch:
            return None
        return _Target(workspace, country, region)

    async def handle_RCPT(
        self, server: Any, session: Any, envelope: Any, address: str, rcpt_options: Any
    ) -> str:
        try:
            target = await self._resolve(address)
        except Exception as exc:
            log.error("recipient lookup failed: %s", type(exc).__name__)
            return TEMPORARY
        if target is None:
            return REJECT_RECIPIENT
        envelope.rcpt_tos.append(address)
        return OK

    async def handle_DATA(self, server: Any, session: Any, envelope: Any) -> str:
        content: bytes = envelope.content
        if len(content) > MAX_UPLOAD_BYTES:
            return TOO_LARGE
        ip = _peer_ip(session)
        try:
            targets: dict[UUID, _Target] = {}
            for address in envelope.rcpt_tos:
                target = await self._resolve(address)
                if target is not None:
                    targets.setdefault(target.workspace.id, target)
            if not targets:
                return REJECT_RECIPIENT
            attachments = await asyncio.to_thread(attachments_of, content)
            for target in targets.values():
                refusal = await self._archive(target, content, attachments, ip)
                if refusal is not None:
                    return refusal
        except Exception as exc:
            # Type name only: messages and tracebacks can carry bound SQL parameters (the
            # recipient token), addresses or paths.
            log.error("inbound email not archived: %s", type(exc).__name__)
            return TEMPORARY
        log.info("inbound email archived for %d workspace(s)", len(targets))
        return OK

    async def _accept(
        self,
        target: _Target,
        data: bytes,
        ip: str | None,
        now: datetime,
        *,
        eml_id: UUID | None,
    ) -> ingest.Accepted | ingest.Duplicate:
        return await ingest.accept_file(
            sessions=self._sessions,
            store=self._store,
            enqueue=self._enqueue,
            workspace=target.workspace,
            country=target.country,
            region=target.region,
            chunks=_chunks(data),
            source=DocumentSource.email,
            # The message itself is the received email; each attachment (a forwarded `.eml`
            # too) is a document it brought.
            audit_action="email.received" if eml_id is None else "document.uploaded",
            actor_type=ActorType.email,
            actor_id=None,
            ip=ip,
            source_email_id=eml_id,
            now=now,
            allow_eml=True,  # forwarded message/rfc822 attachments too
        )

    async def _archive(
        self, target: _Target, content: bytes, attachments: list[bytes], ip: str | None
    ) -> str | None:
        """Archive one copy for one workspace. A reply string means the email is refused."""
        now = utcnow()
        try:
            eml = await self._accept(target, content, ip, now, eml_id=None)
        except ingest.TooLarge:
            return TOO_LARGE
        except (ingest.Unsupported, ingest.Empty):
            return BAD_MESSAGE
        for data in attachments:
            try:
                await self._accept(target, data, ip, now, eml_id=eml.document_id)
            except ingest.IngestError as exc:
                await self._audit_rejected(target, eml.document_id, exc, ip, now)
        return None

    async def _audit_rejected(
        self, target: _Target, eml_id: UUID, exc: ingest.IngestError, ip: str | None, now: datetime
    ) -> None:
        reason = {
            ingest.Unsupported: "unsupported_type",
            ingest.Empty: "empty",
            ingest.TooLarge: "too_large",
        }.get(type(exc), "invalid")
        async with self._sessions() as db:
            await audit.append(
                db,
                workspace_id=target.workspace.id,
                action="email.rejected",
                actor_type=ActorType.email,
                actor_id=None,
                target_type="document",
                target_id=str(eml_id),
                ip=ip,
                data={"reason": reason, "kind": "unknown"},
                now=now,
            )
            await db.commit()


def _peer_ip(session: Any) -> str | None:
    peer = getattr(session, "peer", None)
    if isinstance(peer, tuple) and peer and isinstance(peer[0], str):
        return peer[0]
    return None


def make_enqueue(app: Celery) -> ingest.Enqueue:
    """Send `sijil.ingest_document` through the broker; no task code is imported here."""

    async def enqueue(ledger_id: UUID, workspace_id: UUID) -> None:
        await asyncio.to_thread(
            app.send_task,
            INGEST_TASK,
            kwargs={"ledger_id": str(ledger_id), "workspace_id": str(workspace_id)},
            queue=INGEST_QUEUE,
        )

    return enqueue


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO)
    # aiosmtpd logs `sender:` / `recip:` at INFO; the recipient holds the inbound token.
    logging.getLogger("mail.log").setLevel(logging.WARNING)


def main() -> None:
    configure_logging()
    settings = SmtpSettings()
    engine = make_engine(settings.database_url)
    handler = SijilSmtpHandler(
        sessions=make_sessionmaker(engine),
        store=S3ObjectStore(settings),
        enqueue=make_enqueue(Celery("sijil-smtp", broker=str(settings.redis_url))),
        settings=settings,
        domain=settings.inbound_domain,
    )
    controller = Controller(
        handler,
        hostname="0.0.0.0",  # noqa: S104 - the receiver is the public edge of the region
        port=SMTP_PORT,
        data_size_limit=MAX_UPLOAD_BYTES,
        ident="Sijil",
    )
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    controller.start()
    log.info("smtp receiver listening on :%d", SMTP_PORT)
    try:
        stop.wait()
    finally:
        controller.stop()


if __name__ == "__main__":
    main()
