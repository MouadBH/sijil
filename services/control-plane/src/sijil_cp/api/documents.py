"""Documents (CAP-2 web upload). The body is streamed straight into the shared ingest path.

The multipart body is parsed incrementally from `request.stream()`: FastAPI's `UploadFile`
would spool the whole part before the handler runs, so an oversized upload could only be
refused after it had been fully received. The client's filename and Content-Type are read past
and never kept: the type comes from sniffing, and no filename reaches the shared database.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime
from typing import TYPE_CHECKING, Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from python_multipart.exceptions import MultipartParseError
from python_multipart.multipart import MultipartParser, parse_options_header
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import ingest
from sijil_core.db.models import (
    ActorType,
    DocumentSource,
    DocumentState,
    IngestState,
    Organisation,
    User,
)
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
