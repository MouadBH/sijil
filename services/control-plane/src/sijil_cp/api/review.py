"""Review queue, field edits and sealing (REC-4, design §5.4).

Field values live only in the workspace's index database (`sijil_core.index`); the ledger
gets `fields_sha256` at seal, and the audit log names fields, never values (ruling P5).

Sealing (ruling P7), in this order, each step idempotent so a seal that failed half-way
completes exactly once when it is called again:
1. `fields_sha256` of the index row's values (`sijil_core.fields.fields_digest`);
2. the original's object lock switched to COMPLIANCE until `final`, the later of the ledger's
   retention and the fiscal year of the document's date (ruling P1; never lowered);
3. the index row frozen with that hash (same hash again: its stored `sealed_at` is reused);
4. the ledger row sealed and `document.sealed` audited, in one transaction;
5. after the commit, `sijil.mirror_fields` sent to the worker.

The whole seal runs under the ledger row's lock (`SELECT ... FOR UPDATE`, taken before the
audit chain's lock, as everywhere else), and so does a field edit: an edit and a seal of the
same document never interleave, the hash is always of the values that get frozen, and the
object store's read-then-write in `seal` is serialised per object version as it requires.
"""

import asyncio
import base64
import binascii
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Annotated, Final
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit
from sijil_core import index as index_db
from sijil_core.db.models import (
    ActorType,
    DocumentLedger,
    DocumentState,
    Organisation,
    User,
    Workspace,
)
from sijil_core.fields import FIELD_NAMES, REQUIRED_BY_TYPE, fields_digest, parse_field
from sijil_core.index import FieldsRow, IndexEngines, RetentionLocked
from sijil_core.retention import REGION_TZ, fiscal_retain_until, region_local_date
from sijil_core.storage import ObjectRef, ObjectStore
from sijil_cp.deps import get_db, get_now, require_totp_compliance
from sijil_cp.enqueue import get_enqueuer
from sijil_cp.errors import ApiError
from sijil_cp.permissions import Action, WorkspaceContext, require_workspace

log = logging.getLogger(__name__)

router = APIRouter(tags=["review"], dependencies=[Depends(require_totp_compliance)])

Db = Annotated[AsyncSession, Depends(get_db)]
CurrentUser = Annotated[User, Depends(require_totp_compliance)]
Now = Annotated[datetime, Depends(get_now)]
Reading = Annotated[WorkspaceContext, Depends(require_workspace(Action.documents_read))]
Reviewing = Annotated[WorkspaceContext, Depends(require_workspace(Action.documents_review))]
Sealing = Annotated[WorkspaceContext, Depends(require_workspace(Action.documents_seal))]

LOCKED: Final = "retention.locked"
NOT_REVIEWABLE: Final = "document.not_reviewable"
INCOMPLETE: Final = "seal.fields_incomplete"
REVIEWABLE: Final = frozenset({DocumentState.filed, DocumentState.needs_review})
DEFAULT_LIMIT: Final = 50
MAX_LIMIT: Final = 100
SEAL_BATCH_LIMIT = 500


class FieldOut(BaseModel):
    value: str | None
    confidence: float | None
    source: str | None


class ReviewItem(BaseModel):
    id: UUID
    state: DocumentState
    document_type: str | None
    fields: dict[str, FieldOut]
    flags: list[str]
    required: list[str]


class FieldsOut(ReviewItem):
    sealed_at: datetime | None
    fields_sha256: str | None


class ReviewPage(BaseModel):
    items: list[ReviewItem]
    next_cursor: str | None


class FieldsPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fields: dict[str, str | None] = Field(min_length=1)


class SealBatchIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    month: str = Field(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")


class Skipped(BaseModel):
    id: UUID
    code: str


class SealBatchOut(BaseModel):
    sealed: int
    skipped: list[Skipped]
    remaining: int


# Shapes --------------------------------------------------------------------------------------


def _required(document_type: str | None) -> frozenset[str]:
    return REQUIRED_BY_TYPE.get(document_type or "invoice", REQUIRED_BY_TYPE["invoice"])


def _item(ledger: DocumentLedger, row: FieldsRow | None) -> ReviewItem:
    values = row.values if row else {}
    confidence = row.confidence if row else {}
    source = row.source if row else {}
    document_type = row.document_type if row else None
    return ReviewItem(
        id=ledger.id,
        state=ledger.state,
        document_type=document_type,
        fields={
            name: FieldOut(
                value=values.get(name), confidence=confidence.get(name), source=source.get(name)
            )
            for name in FIELD_NAMES
        },
        flags=list(row.flags) if row else [],
        required=sorted(_required(document_type)),
    )


def _full(ledger: DocumentLedger, row: FieldsRow | None) -> FieldsOut:
    return FieldsOut(
        **_item(ledger, row).model_dump(),
        sealed_at=ledger.sealed_at,
        fields_sha256=ledger.fields_sha256,
    )


def _ip(request: Request) -> str | None:
    return request.client.host if request.client else None


def _engines(request: Request) -> IndexEngines:
    engines: IndexEngines = request.app.state.index
    return engines


def _cursor(row: DocumentLedger) -> str:
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


async def _read(engines: IndexEngines, workspace: Workspace, ledger_id: UUID) -> FieldsRow | None:
    # The region was asserted by `require_workspace` before anything of the workspace is read.
    async with (await engines.engine(workspace)).connect() as conn:
        return await index_db.read_fields(conn, ledger_id)


async def _locked_ledger(db: AsyncSession, workspace: Workspace, doc_id: UUID) -> DocumentLedger:
    """The ledger row, locked FOR UPDATE until the caller's transaction ends."""
    ledger = await db.scalar(
        select(DocumentLedger)
        .where(DocumentLedger.id == doc_id, DocumentLedger.workspace_id == workspace.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if ledger is None:
        raise ApiError("document.not_found", 404)
    return ledger


def _refuse_unless_reviewable(ledger: DocumentLedger) -> None:
    if ledger.sealed_at is not None or ledger.state is DocumentState.sealed:
        raise ApiError(LOCKED, 409)
    if ledger.state not in REVIEWABLE:
        raise ApiError(NOT_REVIEWABLE, 409)


# Review queue and fields ---------------------------------------------------------------------


@router.get("/workspaces/{workspace_id}/review", response_model=ReviewPage)
async def review_queue(
    request: Request,
    ctx: Reviewing,
    user: CurrentUser,
    db: Db,
    now: Now,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
) -> ReviewPage:
    workspace = ctx.workspace
    query = select(DocumentLedger).where(
        DocumentLedger.workspace_id == workspace.id,
        DocumentLedger.state == DocumentState.needs_review,
    )
    if cursor is not None:
        at, last_id = _decode_cursor(cursor)
        query = query.where(tuple_(DocumentLedger.ingested_at, DocumentLedger.id) > (at, last_id))
    query = query.order_by(DocumentLedger.ingested_at, DocumentLedger.id).limit(limit + 1)
    rows = list((await db.scalars(query)).all())
    more = len(rows) > limit
    rows = rows[:limit]
    async with (await _engines(request).engine(workspace)).connect() as conn:
        fields = [await index_db.read_fields(conn, r.id) for r in rows]
    await audit.append(
        db,
        workspace_id=workspace.id,
        action="review.listed",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        ip=_ip(request),
        data={"count": len(rows)},
        now=now,
    )
    await db.commit()
    return ReviewPage(
        items=[_item(r, f) for r, f in zip(rows, fields, strict=True)],
        next_cursor=_cursor(rows[-1]) if more else None,
    )


@router.get("/workspaces/{workspace_id}/documents/{doc_id}/fields", response_model=FieldsOut)
async def get_fields(
    doc_id: UUID, request: Request, ctx: Reading, user: CurrentUser, db: Db, now: Now
) -> FieldsOut:
    workspace = ctx.workspace
    ledger = await db.scalar(
        select(DocumentLedger).where(
            DocumentLedger.id == doc_id, DocumentLedger.workspace_id == workspace.id
        )
    )
    if ledger is None:
        raise ApiError("document.not_found", 404)
    row = await _read(_engines(request), workspace, ledger.id)
    await audit.append(
        db,
        workspace_id=workspace.id,
        action="document.viewed",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        target_type="document",
        target_id=str(ledger.id),
        ip=_ip(request),
        now=now,
    )
    await db.commit()
    return _full(ledger, row)


def _parsed(raw: Mapping[str, str | None]) -> dict[str, str | None]:
    """Canonical values, or 422 `field.unknown` / `field.invalid` naming the first bad one."""
    values: dict[str, str | None] = {}
    for name, value in raw.items():
        try:
            parsed = parse_field(name, value)
        except KeyError:
            raise ApiError("field.unknown", 422, {"field": name}) from None
        if value is not None and parsed is None:
            raise ApiError("field.invalid", 422, {"field": name})
        values[name] = parsed
    return values


@router.patch("/workspaces/{workspace_id}/documents/{doc_id}/fields", response_model=FieldsOut)
async def edit_fields(
    doc_id: UUID,
    body: FieldsPatch,
    request: Request,
    ctx: Reviewing,
    user: CurrentUser,
    db: Db,
    now: Now,
) -> FieldsOut:
    workspace = ctx.workspace
    ledger = await _locked_ledger(db, workspace, doc_id)
    _refuse_unless_reviewable(ledger)
    values = _parsed(body.fields)
    names = sorted(values)
    # The index transaction commits only after the audit: an edit is never left unaudited.
    async with (await _engines(request).engine(workspace)).begin() as conn:
        try:
            row = await index_db.update_fields(
                conn, ledger.id, values, source="user", updated_by=user.id, now=now
            )
        except RetentionLocked:
            # The index row is frozen although the ledger is not yet: a seal half-way done.
            raise ApiError(LOCKED, 409) from None
        except LookupError:
            raise ApiError(NOT_REVIEWABLE, 409) from None
        await audit.append(
            db,
            workspace_id=workspace.id,
            action="document.fields_edited",
            actor_type=ActorType.user,
            actor_id=str(user.id),
            target_type="document",
            target_id=str(ledger.id),
            ip=_ip(request),
            data={"fields": ",".join(names)},
            now=now,
        )
        await db.commit()
    return _full(ledger, row)


# Sealing -------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Sealer:
    """What one seal needs, so the single seal and the batch run the same code."""

    engines: IndexEngines
    store: ObjectStore
    workspace: Workspace
    country: str
    region: str
    user_id: UUID
    ip: str | None
    now: datetime


def _missing(row: FieldsRow) -> list[str]:
    return sorted(n for n in _required(row.document_type) if row.values.get(n) is None)


def _final_retention(ledger: DocumentLedger, row: FieldsRow, sealer: _Sealer) -> date:
    """The later of the ledger's retention and the fiscal year of the document's date (the
    arrival date without one). A COMPLIANCE lock can never be shortened, so a date after
    today (a mistyped year) counts as today."""
    today = region_local_date(sealer.now, sealer.region)
    value = row.values.get("date")
    dated = date.fromisoformat(value) if value else None
    basis = min(dated, today) if dated else region_local_date(ledger.ingested_at, sealer.region)
    return max(ledger.retention_until, fiscal_retain_until(basis, sealer.country))


async def _seal_locked(db: AsyncSession, sealer: _Sealer, ledger: DocumentLedger) -> FieldsOut:
    """Seals the locked ledger row (see the module docstring); the caller commits nothing:
    this commits. Raises ApiError for a document that cannot be sealed."""
    _refuse_unless_reviewable(ledger)
    engine = await sealer.engines.engine(sealer.workspace)
    async with engine.connect() as conn:
        row = await index_db.read_fields(conn, ledger.id)
    if row is None:
        raise ApiError(NOT_REVIEWABLE, 409)
    missing = _missing(row)
    if missing:
        raise ApiError(INCOMPLETE, 422, {"fields": missing})
    # 1. The hash of exactly the values that get frozen: every writer of an unsealed row (an
    #    edit, routing) holds this ledger row's lock, which we hold until the end.
    digest = fields_digest(row.values, document_type=row.document_type)
    if row.sealed_at is not None and row.fields_sha256 != digest:
        raise ApiError(LOCKED, 409)
    final = _final_retention(ledger, row, sealer)
    # 2. The object lock: COMPLIANCE, only ever later.
    await sealer.store.seal(ObjectRef(ledger.object_key, ledger.object_version_id), final)
    # 3. The index row frozen, in its own committed transaction. On a retry it is already
    #    sealed with this hash: its stored `sealed_at` is the one the ledger gets.
    try:
        async with engine.begin() as conn:
            frozen = await index_db.seal_fields(
                conn, ledger.id, fields_sha256=digest, sealed_at=sealer.now
            )
    except RetentionLocked:
        raise ApiError(LOCKED, 409) from None
    sealed_at = frozen.sealed_at or sealer.now
    # 4. The ledger, under the lock taken by the caller, and the audit, in one transaction.
    ledger.state = DocumentState.sealed
    ledger.fields_sha256 = digest
    ledger.sealed_at = sealed_at
    ledger.sealed_by = sealer.user_id
    ledger.retention_until = final
    await audit.append(
        db,
        workspace_id=sealer.workspace.id,
        action="document.sealed",
        actor_type=ActorType.user,
        actor_id=str(sealer.user_id),
        target_type="document",
        target_id=str(ledger.id),
        ip=sealer.ip,
        data={"fields_sha256": digest, "retention_until": final.isoformat()},
        now=sealer.now,
    )
    await db.commit()
    return _full(ledger, frozen)


async def _mirror(request: Request, ledger_id: UUID, workspace_id: UUID) -> None:
    """5. After the commit: Paperless's copy of the fields. The seal stands if this fails."""
    try:
        await asyncio.to_thread(get_enqueuer(request).send_mirror, ledger_id, workspace_id)
    except Exception as exc:  # noqa: BLE001 - the broker being down must not undo a seal
        log.warning("mirror enqueue of document %s failed: %s", ledger_id, type(exc).__name__)


async def _sealer(
    request: Request, db: AsyncSession, workspace: Workspace, user: User, now: datetime
) -> _Sealer:
    org = await db.get(Organisation, workspace.org_id)
    if org is None:  # authorize already checked; keeps the type narrow
        raise ApiError("not_found", 404)
    return _Sealer(
        engines=_engines(request),
        store=request.app.state.seal_store,
        workspace=workspace,
        country=org.country,
        region=org.region,
        user_id=user.id,
        ip=_ip(request),
        now=now,
    )


@router.post("/workspaces/{workspace_id}/documents/{doc_id}/seal", response_model=FieldsOut)
async def seal_document(
    doc_id: UUID, request: Request, ctx: Sealing, user: CurrentUser, db: Db, now: Now
) -> FieldsOut:
    sealer = await _sealer(request, db, ctx.workspace, user, now)
    ledger = await _locked_ledger(db, ctx.workspace, doc_id)
    out = await _seal_locked(db, sealer, ledger)
    await _mirror(request, ledger.id, ctx.workspace.id)
    return out


def _month_bounds(month: str, region: str) -> tuple[datetime, datetime]:
    """The month's first instant and the next month's, in the region's civil time."""
    year, number = (int(part) for part in month.split("-"))
    zone = ZoneInfo(REGION_TZ[region])
    start = datetime(year, number, 1, tzinfo=zone)
    end = datetime(year + number // 12, number % 12 + 1, 1, tzinfo=zone)
    return start, end


@router.post("/workspaces/{workspace_id}/seal-batch", response_model=SealBatchOut)
async def seal_batch(
    body: SealBatchIn, request: Request, ctx: Sealing, user: CurrentUser, db: Db, now: Now
) -> SealBatchOut:
    """Seals the month's `filed` documents (region-local arrival), oldest first, at most
    SEAL_BATCH_LIMIT per call, each in its own transaction through the single seal's code.
    `remaining` counts the month's filed documents this call did not reach."""
    workspace = ctx.workspace
    sealer = await _sealer(request, db, workspace, user, now)
    # Detached, so the rollback after a skipped document does not expire what it holds.
    db.expunge(workspace)
    start, end = _month_bounds(body.month, sealer.region)
    in_month = (
        DocumentLedger.workspace_id == workspace.id,
        DocumentLedger.state == DocumentState.filed,
        DocumentLedger.ingested_at >= start,
        DocumentLedger.ingested_at < end,
    )
    total = int(await db.scalar(select(func.count()).where(*in_month)) or 0)
    ids = list(
        await db.scalars(
            select(DocumentLedger.id)
            .where(*in_month)
            .order_by(DocumentLedger.ingested_at, DocumentLedger.id)
            .limit(SEAL_BATCH_LIMIT)
        )
    )
    await db.commit()
    sealed = 0
    skipped: list[Skipped] = []
    for doc_id in ids:
        try:
            ledger = await _locked_ledger(db, workspace, doc_id)
            await _seal_locked(db, sealer, ledger)
        except ApiError as exc:
            await db.rollback()
            skipped.append(Skipped(id=doc_id, code=exc.code))
            continue
        sealed += 1
        await _mirror(request, doc_id, workspace.id)
    return SealBatchOut(sealed=sealed, skipped=skipped, remaining=max(0, total - len(ids)))
