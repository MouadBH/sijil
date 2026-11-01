"""Documents (CAP-2 web upload). The body is streamed straight into the shared ingest path.

The multipart body is parsed incrementally from `request.stream()`: FastAPI's `UploadFile`
would spool the whole part before the handler runs, so an oversized upload could only be
refused after it had been fully received. The client's filename and Content-Type are read past
and never kept: the type comes from sniffing, and no filename reaches the shared database.
"""

import asyncio
import base64
import binascii
import logging
from collections.abc import AsyncIterator
from datetime import date, datetime
from typing import TYPE_CHECKING, Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from python_multipart.exceptions import MultipartParseError
from python_multipart.multipart import MultipartParser, parse_options_header
from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit, ingest
from sijil_core.crypto import Envelope, open_envelope
from sijil_core.db.models import (
    ActorType,
    CredentialKind,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    Organisation,
    TenantCredential,
    User,
    Workspace,
)
from sijil_core.paperless import PaperlessClient, PaperlessDoc, PaperlessError
from sijil_cp.deps import get_db, get_now, require_totp_compliance
from sijil_cp.enqueue import get_enqueuer
from sijil_cp.errors import ApiError
from sijil_cp.permissions import Action, WorkspaceContext, require_workspace

if TYPE_CHECKING:
    from python_multipart.multipart import MultipartCallbacks

router = APIRouter(tags=["documents"], dependencies=[Depends(require_totp_compliance)])

Db = Annotated[AsyncSession, Depends(get_db)]
CurrentUser = Annotated[User, Depends(require_totp_compliance)]
Now = Annotated[datetime, Depends(get_now)]
Reading = Annotated[WorkspaceContext, Depends(require_workspace(Action.documents_read))]
Uploading = Annotated[WorkspaceContext, Depends(require_workspace(Action.documents_upload))]

FILE_FIELD = b"file"
# The whole body, not only the file part, is bounded: other parts and multipart framing get
# 1 MiB of slack on top of the file limit.
MAX_BODY_BYTES = ingest.MAX_UPLOAD_BYTES + 1024 * 1024


class UploadOut(BaseModel):
    document_id: UUID
    state: DocumentState
    ingest_state: IngestState


class _MissingFile(Exception):
    pass


def _invalid() -> RequestValidationError:
    return RequestValidationError(
        [{"loc": ("body", "file"), "msg": "a multipart `file` part is required", "type": "missing"}]
    )


def _boundary(request: Request) -> bytes:
    content_type, params = parse_options_header(request.headers.get("content-type"))
    boundary = params.get(b"boundary")
    if content_type != b"multipart/form-data" or not boundary:
        raise _invalid()
    return boundary


async def _file_part(request: Request, boundary: bytes) -> AsyncIterator[bytes]:
    """Yield the bytes of the first `file` part as they arrive; other parts are skipped."""
    seen = False
    in_file = False
    field = bytearray()
    value = bytearray()
    name: bytes | None = None
    data: list[bytes] = []

    def on_part_begin() -> None:
        nonlocal name
        name = None

    def on_header_field(chunk: bytes, start: int, end: int) -> None:
        field.extend(chunk[start:end])

    def on_header_value(chunk: bytes, start: int, end: int) -> None:
        value.extend(chunk[start:end])

    def on_header_end() -> None:
        nonlocal name
        if bytes(field).lower() == b"content-disposition":
            # Only the field name is read; the filename parameter is ignored.
            name = parse_options_header(bytes(value))[1].get(b"name")
        field.clear()
        value.clear()

    def on_headers_finished() -> None:
        nonlocal seen, in_file
        in_file = name == FILE_FIELD and not seen
        seen = seen or in_file

    def on_part_data(chunk: bytes, start: int, end: int) -> None:
        if in_file:
            data.append(chunk[start:end])

    def on_part_end() -> None:
        nonlocal in_file
        in_file = False

    callbacks: MultipartCallbacks = {
        "on_part_begin": on_part_begin,
        "on_header_field": on_header_field,
        "on_header_value": on_header_value,
        "on_header_end": on_header_end,
        "on_headers_finished": on_headers_finished,
        "on_part_data": on_part_data,
        "on_part_end": on_part_end,
    }
    parser = MultipartParser(boundary, callbacks)
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > MAX_BODY_BYTES:
            raise ingest.TooLarge
        parser.write(chunk)
        if data:
            out = b"".join(data)
            data.clear()
            yield out
    parser.finalize()
    if not seen:
        raise _MissingFile
    if data:
        yield b"".join(data)


@router.post("/workspaces/{workspace_id}/documents", status_code=202, response_model=UploadOut)
async def upload_document(
    request: Request, ctx: Uploading, user: CurrentUser, db: Db, now: Now
) -> UploadOut | JSONResponse:
    workspace = ctx.workspace
    org = await db.get(Organisation, workspace.org_id)
    if org is None:  # authorize already checked; keeps the type narrow
        raise ApiError("not_found", 404)
    country, region = org.country, org.region
    user_id = str(user.id)
    # Hand the connection back before the (possibly long) body arrives; accept_file opens its
    # own short transactions. Loaded attributes stay readable on the detached objects.
    await db.close()

    enqueuer = get_enqueuer(request)

    async def enqueue(ledger_id: UUID, workspace_id: UUID) -> None:
        await asyncio.to_thread(enqueuer.send_ingest, ledger_id, workspace_id)

    try:
        result = await ingest.accept_file(
            sessions=request.app.state.sessionmaker,
            store=request.app.state.store,
            enqueue=enqueue,
            workspace=workspace,
            country=country,
            region=region,
            chunks=_file_part(request, _boundary(request)),
            source=DocumentSource.upload,
            actor_type=ActorType.user,
            actor_id=user_id,
            ip=request.client.host if request.client else None,
            now=now,
        )
    except ingest.TooLarge:
        raise ApiError(ingest.TooLarge.code, 413) from None
    except ingest.Unsupported:
        raise ApiError(ingest.Unsupported.code, 415) from None
    except ingest.Empty:
        raise ApiError(ingest.Empty.code, 422) from None
    except (_MissingFile, MultipartParseError):
        raise _invalid() from None
    if isinstance(result, ingest.Duplicate):
        return JSONResponse(
            {"error": {"code": "document.duplicate"}, "document_id": str(result.document_id)},
            status_code=409,
        )
    return UploadOut(
        document_id=result.document_id,
        state=DocumentState.received,
        ingest_state=IngestState.queued,
    )


# --- list and detail -------------------------------------------------------------------------

log = logging.getLogger(__name__)

PAPERLESS_TIMEOUT_S = 3.0
# Paperless retries 502/503/504 itself with back-off; this bounds the whole lookup.
PAPERLESS_DEADLINE_S = 6.0
DEFAULT_LIMIT = 50
MAX_LIMIT = 100


class DocumentOut(BaseModel):
    id: UUID
    state: DocumentState
    ingest_state: IngestState
    source: DocumentSource
    content_kind: str
    size_bytes: int
    sha256: str
    ingested_at: datetime
    retention_until: date
    title: str | None
    original_file_name: str | None


class DocumentPage(BaseModel):
    items: list[DocumentOut]
    next_cursor: str | None
    engine: Literal["ok", "unavailable"]


def _encode_cursor(row: DocumentLedger) -> str:
    raw = f"{row.ingested_at.isoformat()}|{row.id}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        at_text, id_text = raw.split("|")
        at = datetime.fromisoformat(at_text)
        if at.tzinfo is None:
            raise ValueError("naive")
        return at, UUID(id_text)
    except (binascii.Error, ValueError, UnicodeDecodeError):
        raise RequestValidationError(
            [{"loc": ("query", "cursor"), "msg": "invalid cursor", "type": "value_error"}]
        ) from None


async def _titles(
    request: Request, db: AsyncSession, workspace: Workspace, ids: list[int]
) -> dict[int, PaperlessDoc] | None:
    """Titles from the workspace's own Paperless, or None when it cannot answer.

    Any failure (no URL or credential, undecryptable token, Paperless down, slow or odd) just
    means the engine flag goes to `unavailable`; the ledger fields are still served. Nothing
    from the exception is logged: it could carry the token.
    """
    if not ids:
        return {}
    try:
        if not workspace.paperless_url:
            return None
        credential = await db.scalar(
            select(TenantCredential).where(
                TenantCredential.workspace_id == workspace.id,
                TenantCredential.kind == CredentialKind.service_token,
            )
        )
        if credential is None:
            return None
        token = open_envelope(
            Envelope.from_bytes(credential.envelope),
            request.app.state.keys,
            f"{workspace.id}:{CredentialKind.service_token}",
        ).decode()
        async with PaperlessClient(
            workspace.paperless_url, token=token, timeout=PAPERLESS_TIMEOUT_S
        ) as client:
            async with asyncio.timeout(PAPERLESS_DEADLINE_S):
                return await client.documents_by_ids(ids)
    except (PaperlessError, TimeoutError, ValueError, KeyError, UnicodeDecodeError, OSError):
        return None
    except Exception as exc:  # noqa: BLE001 - e.g. a DecryptionError; never a 5xx
        log.warning("paperless lookup failed: %s", type(exc).__name__)
        return None


def _out(row: DocumentLedger, docs: dict[int, PaperlessDoc] | None) -> DocumentOut:
    doc = docs.get(row.paperless_doc_id) if docs and row.paperless_doc_id is not None else None
    return DocumentOut(
        id=row.id,
        state=row.state,
        ingest_state=row.ingest_state,
        source=row.source,
        content_kind=row.content_kind,
        size_bytes=row.size_bytes,
        sha256=row.sha256,
        ingested_at=row.ingested_at,
        retention_until=row.retention_until,
        title=doc.title if doc else None,
        original_file_name=doc.original_file_name if doc else None,
    )


def _ip(request: Request) -> str | None:
    return request.client.host if request.client else None


@router.get("/workspaces/{workspace_id}/documents", response_model=DocumentPage)
async def list_documents(
    request: Request,
    ctx: Reading,
    user: CurrentUser,
    db: Db,
    now: Now,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
) -> DocumentPage:
    workspace = ctx.workspace
    query = select(DocumentLedger).where(DocumentLedger.workspace_id == workspace.id)
    if cursor is not None:
        at, last_id = _decode_cursor(cursor)
        query = query.where(tuple_(DocumentLedger.ingested_at, DocumentLedger.id) < (at, last_id))
    query = query.order_by(DocumentLedger.ingested_at.desc(), DocumentLedger.id.desc()).limit(
        limit + 1
    )
    rows = list((await db.scalars(query)).all())
    more = len(rows) > limit
    rows = rows[:limit]
    ids = [r.paperless_doc_id for r in rows if r.paperless_doc_id is not None]
    docs = await _titles(request, db, workspace, ids)
    await audit.append(
        db,
        workspace_id=workspace.id,
        action="documents.listed",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        ip=_ip(request),
        data={"count": len(rows)},
        now=now,
    )
    await db.commit()
    return DocumentPage(
        items=[_out(r, docs) for r in rows],
        next_cursor=_encode_cursor(rows[-1]) if more else None,
        engine="ok" if docs is not None else "unavailable",
    )


@router.get("/workspaces/{workspace_id}/documents/{doc_id}", response_model=DocumentOut)
async def get_document(
    doc_id: UUID, request: Request, ctx: Reading, user: CurrentUser, db: Db, now: Now
) -> DocumentOut:
    workspace = ctx.workspace
    row = await db.scalar(
        select(DocumentLedger).where(
            DocumentLedger.id == doc_id, DocumentLedger.workspace_id == workspace.id
        )
    )
    if row is None:
        raise ApiError("document.not_found", 404)
    ids = [row.paperless_doc_id] if row.paperless_doc_id is not None else []
    docs = await _titles(request, db, workspace, ids)
    await audit.append(
        db,
        workspace_id=workspace.id,
        action="document.viewed",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        target_type="document",
        target_id=str(row.id),
        ip=_ip(request),
        now=now,
    )
    await db.commit()
    return _out(row, docs)
