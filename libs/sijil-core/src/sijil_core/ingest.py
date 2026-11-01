"""The one way a file enters a workspace: web upload (control plane) and inbound email (worker).

`accept_file` streams the bytes to a spool file while hashing and counting them, sniffs the
type, puts the original into the object store (GOVERNANCE lock), records it in the ledger
(unique per workspace + SHA-256), audits it in the same transaction, and enqueues ingest only
after the commit. Paperless is not on this path: a down tenant never blocks acceptance.

Nothing about the file but its hashes, size and kind reaches the shared database: no filename,
subject or sender.
"""

import asyncio
import contextlib
import hashlib
import logging
import os
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO, Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core.db.models import (
    ActorType,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    Workspace,
)
from sijil_core.filetypes import MAX_UPLOAD_BYTES, FileKind, sniff
from sijil_core.retention import provisional_retain_until
from sijil_core.storage import ObjectStore
from sijil_core.tenancy import tenant_ref

logger = logging.getLogger(__name__)

# Incoming chunks are batched to this size before one threaded write + hash.
_WRITE_BYTES = 1024 * 1024
_UNIQUE = "uq_document_ledger_workspace_sha256"

Enqueue = Callable[[UUID, UUID], Awaitable[None]]


@dataclass(frozen=True)
class Accepted:
    document_id: UUID


@dataclass(frozen=True)
class Duplicate:
    """The workspace already holds these exact bytes, as `document_id`."""

    document_id: UUID


class IngestError(Exception):
    code = "upload.invalid"


class TooLarge(IngestError):
    code = "upload.too_large"


class Unsupported(IngestError):
    code = "upload.unsupported_type"


class Empty(IngestError):
    code = "upload.empty"


@dataclass(frozen=True)
class _Spooled:
    path: Path
    sha256: str
    md5: str
    size: int


def _write(out: IO[bytes], digests: tuple[Any, Any], blocks: list[bytes]) -> None:
    for block in blocks:
        out.write(block)
        for digest in digests:
            digest.update(block)


async def _close(chunks: AsyncIterator[bytes]) -> None:
    aclose = getattr(chunks, "aclose", None)
    if aclose is not None:
        await aclose()


async def _spool(chunks: AsyncIterator[bytes]) -> _Spooled:
    """Write the stream to a private temp file, refusing it once it passes the size limit.

    Memory holds at most one batch of chunks; the file is removed on any failure.
    """
    fd, name = tempfile.mkstemp(prefix="sijil-spool-")
    path = Path(name)
    sha256 = hashlib.sha256()
    md5 = hashlib.md5(usedforsecurity=False)  # only to compare with Paperless's checksum
    size = 0
    try:
        with os.fdopen(fd, "wb") as out:
            pending: list[bytes] = []
            pending_bytes = 0
            try:
                async for chunk in chunks:
                    size += len(chunk)
                    if size > MAX_UPLOAD_BYTES:
                        raise TooLarge
                    pending.append(chunk)
                    pending_bytes += len(chunk)
                    if pending_bytes >= _WRITE_BYTES:
                        await asyncio.to_thread(_write, out, (sha256, md5), pending)
                        pending, pending_bytes = [], 0
            finally:
                await _close(chunks)
            if pending:
                await asyncio.to_thread(_write, out, (sha256, md5), pending)
    except BaseException:
        # A single unlink syscall on the error path; not worth a thread hop under cancellation.
        with contextlib.suppress(FileNotFoundError):
            os.unlink(name)
        raise
    return _Spooled(path=path, sha256=sha256.hexdigest(), md5=md5.hexdigest(), size=size)


async def _existing_id(db: AsyncSession, workspace_id: UUID, sha256: str) -> UUID | None:
    found: UUID | None = await db.scalar(
        select(DocumentLedger.id).where(
            DocumentLedger.workspace_id == workspace_id, DocumentLedger.sha256 == sha256
        )
    )
    return found


@dataclass(frozen=True)
class _Actor:
    actor_type: ActorType
    actor_id: str | None
    ip: str | None
    source: DocumentSource
    now: datetime


async def _audit_duplicate(
    db: AsyncSession, workspace_id: UUID, existing: UUID, sha256: str, who: _Actor
) -> None:
    await audit.append(
        db,
        workspace_id=workspace_id,
        action="document.duplicate_rejected",
        actor_type=who.actor_type,
        actor_id=who.actor_id,
        target_type="document",
        target_id=str(existing),
        ip=who.ip,
        data={"sha256": sha256, "source": who.source.value},
        now=who.now,
    )


async def accept_file(
    *,
    sessions: async_sessionmaker[AsyncSession],
    store: ObjectStore,
    enqueue: Enqueue,
    workspace: Workspace,
    country: str,
    chunks: AsyncIterator[bytes],
    source: DocumentSource,
    actor_type: ActorType,
    actor_id: str | None,
    ip: str | None,
    source_email_id: UUID | None = None,
    now: datetime,
    allow_eml: bool = False,
) -> Accepted | Duplicate:
    """Archive one file. Raises TooLarge, Unsupported or Empty; nothing is stored then."""
    workspace_id = workspace.id
    who = _Actor(actor_type, actor_id, ip, source, now)
    spooled = await _spool(chunks)
    try:
        if spooled.size == 0:
            raise Empty
        kind = await asyncio.to_thread(sniff, spooled.path, allow_eml=allow_eml)
        if kind is None:
            raise Unsupported
        # Cheap pre-check: a plain re-upload never locks another version in the store.
        async with sessions() as db:
            existing = await _existing_id(db, workspace_id, spooled.sha256)
            if existing is not None:
                await _audit_duplicate(db, workspace_id, existing, spooled.sha256, who)
                await db.commit()
                return Duplicate(existing)
        retain_until = provisional_retain_until(now.date(), country)
        # Before the ledger row: a row must never point at an object that is not there. The key
        # is content-addressed, so a racer that loses below only added an identical version.
        ref = await store.put_original(
            f"{tenant_ref(workspace_id)}/{spooled.sha256}", spooled.path, retain_until
        )
    finally:
        await asyncio.to_thread(spooled.path.unlink, missing_ok=True)

    async with sessions() as db:
        document_id: UUID | None = await db.scalar(
            insert(DocumentLedger)
            .values(
                id=uuid4(),
                workspace_id=workspace_id,
                sha256=spooled.sha256,
                md5=spooled.md5,
                size_bytes=spooled.size,
                content_kind=kind.value,
                source=source,
                source_email_id=source_email_id,
                uploaded_by=UUID(actor_id) if actor_type is ActorType.user and actor_id else None,
                object_key=ref.key,
                object_version_id=ref.version_id,
                state=DocumentState.received,
                ingest_state=IngestState.queued,
                retention_until=retain_until,
                ingested_at=now,
            )
            .on_conflict_do_nothing(constraint=_UNIQUE)
            .returning(DocumentLedger.id)
        )
        if document_id is None:
            # A concurrent accept of the same bytes committed first (the insert waited for it),
            # so a fresh read in this READ COMMITTED transaction sees its row.
            existing = await _existing_id(db, workspace_id, spooled.sha256)
            if existing is None:  # ledger rows are never deleted (trigger); defensive only
                raise RuntimeError("ledger conflict without a conflicting row")
            await _audit_duplicate(db, workspace_id, existing, spooled.sha256, who)
            await db.commit()
            return Duplicate(existing)
        await audit.append(
            db,
            workspace_id=workspace_id,
            action="email.received" if kind is FileKind.eml else "document.uploaded",
            actor_type=actor_type,
            actor_id=actor_id,
            target_type="document",
            target_id=str(document_id),
            ip=ip,
            data={
                "content_kind": kind.value,
                "sha256": spooled.sha256,
                "size_bytes": spooled.size,
                "source": source.value,
            },
            now=now,
        )
        await db.commit()

    try:
        await enqueue(document_id, workspace_id)
    except Exception:
        # The original and its row are committed: the document is accepted. It stays
        # `ingest_state = queued` for a re-enqueue sweep rather than failing the caller.
        logger.exception("enqueue of ingest failed for document %s", document_id)
    return Accepted(document_id)


__all__ = [
    "MAX_UPLOAD_BYTES",
    "Accepted",
    "Duplicate",
    "Empty",
    "IngestError",
    "TooLarge",
    "Unsupported",
    "accept_file",
]
