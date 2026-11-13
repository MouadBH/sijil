"""Search, saved views and tags (plan 5.1).

Search reads the workspace's index database only (`sijil_core.search_query`), never Paperless,
and never returns a document in `removed`, `deleting` or `deleted`. Each search is audited
`search.performed` `{result_count, filters}`: the names of the filters used, never their values
or the query text (ruling P5). A body that does not parse, an unknown filter, a terminal status
or a cursor this API did not make answers 422 `search.query_invalid` with the fields at fault.

Saved views live in the index database too (`saved_view`: their filters are document content).
Every member who reads documents lists them and saves their own; changing or deleting one
takes its creator, an Owner or an Admin. Each change is audited `search.view_changed` `{op}`,
committed before the index write, so a change made is always audited.

Tags are annotations: allowed on a sealed document, they never change a field value,
`fields_sha256` or the original. `PUT …/tags` replaces the set (`search_index.clean_tags`; 422
`tag.invalid`), audited `document.tagged` `{count}` (never the names) first, then written to
`document_tag` and `search_doc.tags` in one index transaction while the ledger row is held FOR
SHARE, as every search write does (sijil_worker.search): a removal or a purge waits for it.
"""

import logging
from datetime import date, datetime
from typing import Annotated, Final
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit
from sijil_core.db.models import TERMINAL_STATES, ActorType, DocumentLedger, Role, Workspace
from sijil_core.search_index import TAG_INVALID, clean_tags, set_tags
from sijil_core.search_query import (
    MAX_CURSOR,
    MAX_LIMIT,
    QUERY_INVALID,
    QueryInvalid,
    SavedView,
    SearchFilters,
    Sort,
    delete_view,
    filter_names,
    get_view,
    list_views,
    run_search,
    save_view,
)
from sijil_cp.api.review import (
    NOT_REVIEWABLE,
    UPDATE_FAILED,
    CurrentUser,
    Db,
    Now,
    Reading,
    Reviewing,
    _engines,
    _ip,
)
from sijil_cp.deps import require_totp_compliance
from sijil_cp.errors import ApiError

log = logging.getLogger(__name__)

router = APIRouter(tags=["search"], dependencies=[Depends(require_totp_compliance)])

VIEW_NOT_FOUND: Final = "search.view_not_found"
DEFAULT_LIMIT: Final = 50
# Besides a view's creator, these may change or delete it.
VIEW_EDITORS: Final = frozenset({Role.owner, Role.admin})


class SearchIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filters: SearchFilters = Field(default_factory=SearchFilters)
    sort: Sort = "relevance"
    cursor: Annotated[str, Field(max_length=MAX_CURSOR)] | None = None
    limit: int = Field(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT)


class SearchItem(BaseModel):
    id: UUID
    state: str
    document_type: str | None
    basis_date: date
    supplier_name: str | None
    number: str | None
    total_ttc: str | None  # money is a string
    currency: str | None
    tags: list[str]
    corrected: list[str]
    snippet: str | None


class SearchOut(BaseModel):
    items: list[SearchItem]
    next_cursor: str | None


class ViewIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=80)]
    filters: SearchFilters = Field(default_factory=SearchFilters)
    sort: Sort = "relevance"


class ViewOut(BaseModel):
    id: UUID
    name: str
    filters: dict[str, object]
    sort: str
    created_by: UUID
    created_at: datetime
    updated_at: datetime


class ViewsOut(BaseModel):
    items: list[ViewOut]


class TagsIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tags: list[str]


class TagsOut(BaseModel):
    tags: list[str]


def _parse[M: BaseModel](model: type[M], raw: bytes, code: str) -> M:
    """The JSON body, or 422 `code` naming the fields at fault."""
    try:
        return model.model_validate_json(raw or b"{}")
    except ValidationError as exc:
        fields = [".".join(str(p) for p in ("body", *e["loc"])) for e in exc.errors()]
        raise ApiError(code, 422, {"fields": fields}) from None


# Search ---------------------------------------------------------------------------------------


@router.post("/workspaces/{workspace_id}/search", response_model=SearchOut)
async def search(request: Request, ctx: Reading, user: CurrentUser, db: Db, now: Now) -> SearchOut:
    workspace = ctx.workspace
    body = _parse(SearchIn, await request.body(), QUERY_INVALID)
    try:
        async with (await _engines(request).engine(workspace)).connect() as conn:
            page = await run_search(conn, body.filters, body.sort, body.cursor, body.limit)
    except QueryInvalid:
        raise ApiError(QUERY_INVALID, 422, {"fields": ["body.cursor"]}) from None
    await audit.append(
        db,
        workspace_id=workspace.id,
        action="search.performed",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        ip=_ip(request),
        data={"result_count": len(page.items), "filters": filter_names(body.filters)},
        now=now,
    )
    await db.commit()
    return SearchOut(
        items=[
            SearchItem(
                id=hit.id,
                state=hit.state,
                document_type=hit.document_type,
                basis_date=hit.basis_date,
                supplier_name=hit.supplier_name,
                number=hit.number,
                total_ttc=None if hit.total_ttc is None else str(hit.total_ttc),
                currency=hit.currency,
                tags=hit.tags,
                corrected=hit.corrected,
                snippet=hit.snippet,
            )
            for hit in page.items
        ],
        next_cursor=page.next_cursor,
    )


# Saved views ----------------------------------------------------------------------------------


def _view_out(view: SavedView) -> ViewOut:
    return ViewOut(
        id=view.id,
        name=view.name,
        filters=view.filters.model_dump(mode="json"),
        sort=view.sort,
        created_by=view.created_by,
        created_at=view.created_at,
        updated_at=view.updated_at,
    )


async def _audit_view(
    db: AsyncSession, request: Request, workspace: Workspace, user_id: UUID, view_id: UUID,
    op: str, now: datetime,
) -> None:  # fmt: skip
    await audit.append(
        db,
        workspace_id=workspace.id,
        action="search.view_changed",
        actor_type=ActorType.user,
        actor_id=str(user_id),
        target_type="saved_view",
        target_id=str(view_id),
        ip=_ip(request),
        data={"op": op},
        now=now,
    )
    await db.commit()


@router.get("/workspaces/{workspace_id}/search/views", response_model=ViewsOut)
async def views(request: Request, ctx: Reading) -> ViewsOut:
    async with (await _engines(request).engine(ctx.workspace)).connect() as conn:
        found = await list_views(conn)
    return ViewsOut(items=[_view_out(v) for v in found])


@router.post("/workspaces/{workspace_id}/search/views", response_model=ViewOut, status_code=201)
async def create_view(
    request: Request, ctx: Reading, user: CurrentUser, db: Db, now: Now
) -> ViewOut:
    body = _parse(ViewIn, await request.body(), QUERY_INVALID)
    view = SavedView(
        id=uuid4(), name=body.name, filters=body.filters, sort=body.sort, created_by=user.id,
        created_at=now, updated_at=now,
    )  # fmt: skip
    await _audit_view(db, request, ctx.workspace, user.id, view.id, "create", now)
    async with (await _engines(request).engine(ctx.workspace)).begin() as conn:
        await save_view(conn, view)
    return _view_out(view)


async def _editable(request: Request, ctx: Reading, user_id: UUID, view_id: UUID) -> SavedView:
    async with (await _engines(request).engine(ctx.workspace)).connect() as conn:
        view = await get_view(conn, view_id)
    if view is None:
        raise ApiError(VIEW_NOT_FOUND, 404)
    if view.created_by != user_id and ctx.role not in VIEW_EDITORS:
        raise ApiError("forbidden", 403)
    return view


@router.put("/workspaces/{workspace_id}/search/views/{view_id}", response_model=ViewOut)
async def update_view(
    view_id: UUID, request: Request, ctx: Reading, user: CurrentUser, db: Db, now: Now
) -> ViewOut:
    view = await _editable(request, ctx, user.id, view_id)
    body = _parse(ViewIn, await request.body(), QUERY_INVALID)
    changed = SavedView(
        id=view.id, name=body.name, filters=body.filters, sort=body.sort,
        created_by=view.created_by, created_at=view.created_at, updated_at=now,
    )  # fmt: skip
    await _audit_view(db, request, ctx.workspace, user.id, view.id, "update", now)
    async with (await _engines(request).engine(ctx.workspace)).begin() as conn:
        await save_view(conn, changed)
    return _view_out(changed)


@router.delete("/workspaces/{workspace_id}/search/views/{view_id}", status_code=204)
async def remove_view(
    view_id: UUID, request: Request, ctx: Reading, user: CurrentUser, db: Db, now: Now
) -> Response:
    view = await _editable(request, ctx, user.id, view_id)
    await _audit_view(db, request, ctx.workspace, user.id, view.id, "delete", now)
    async with (await _engines(request).engine(ctx.workspace)).begin() as conn:
        await delete_view(conn, view.id)
    return Response(status_code=204)


# Tags -----------------------------------------------------------------------------------------


async def _shared_ledger(db: AsyncSession, workspace: Workspace, doc_id: UUID) -> DocumentLedger:
    """The ledger row, locked FOR SHARE until the caller's transaction ends, and refused 409
    `document.not_reviewable` when terminal."""
    ledger = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == doc_id, DocumentLedger.workspace_id == workspace.id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if ledger is None:
        raise ApiError("document.not_found", 404)
    if ledger.state in TERMINAL_STATES:
        raise ApiError(NOT_REVIEWABLE, 409)
    return ledger


@router.put("/workspaces/{workspace_id}/documents/{doc_id}/tags", response_model=TagsOut)
async def put_tags(
    doc_id: UUID, request: Request, ctx: Reviewing, user: CurrentUser, db: Db, now: Now
) -> TagsOut:
    workspace = ctx.workspace
    body = _parse(TagsIn, await request.body(), TAG_INVALID)
    try:
        tags = clean_tags(body.tags)
    except ValueError:
        raise ApiError(TAG_INVALID, 422) from None
    ledger = await _shared_ledger(db, workspace, doc_id)
    await audit.append(
        db,
        workspace_id=workspace.id,
        action="document.tagged",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        target_type="document",
        target_id=str(ledger.id),
        ip=_ip(request),
        data={"count": len(tags)},
        now=now,
    )
    await db.commit()
    # Locked again for the index write: a removal committed meanwhile is seen here.
    ledger = await _shared_ledger(db, workspace, doc_id)
    try:
        async with (await _engines(request).engine(workspace)).begin() as conn:
            await set_tags(conn, ledger.id, tags, by=user.id, now=now)
    except Exception as exc:
        # An audited change that did not apply: acceptable, and said so.
        log.warning("tags of document %s not applied: %s", doc_id, type(exc).__name__)
        raise ApiError(UPDATE_FAILED, 503) from None
    finally:
        await db.rollback()  # nothing written here: it only releases the lock
    return TagsOut(tags=tags)
