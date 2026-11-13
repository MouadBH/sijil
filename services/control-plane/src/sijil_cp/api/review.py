"""Review queue, field edits and sealing (REC-4, design §5.4).

Field values live only in the workspace's index database (`sijil_core.index`); the ledger
gets `fields_sha256` at seal, and the audit log names fields, never values (ruling P5).

Sealing (ruling P7, as amended by the product owner: the control plane holds no key that can
switch an object lock), in this order, each step idempotent so a seal that failed half-way
completes exactly once when it is called again:
1. the ledger row locked FOR UPDATE (before the audit chain's lock, as everywhere) and refused
   unless `filed` or `needs_review`;
2. the index row frozen by `index.seal_fields`, which recomputes `fields_sha256` from the row
   it locks and refuses (`seal.fields_changed`) a digest of values that changed since they
   were read; the same hash again returns the stored row and its `sealed_at`;
3. the ledger sealed (`state`, `fields_sha256`, `sealed_at`, `sealed_by`, `retention_until =
   final`, the later of its retention and the fiscal year of `retention_basis`, never lowered:
   ruling P1) and `document.sealed` audited, in one transaction;
4. after the commit, `sijil.lock_sealed` to the seal worker (queue `seal`), which switches the
   original to COMPLIANCE at the committed `retention_until`, `sijil.mirror_fields`,
   `sijil.embed_document` (the extraction worker, queue `embedding`) and
   `sijil.learn_from_document` (the supplier the seal confirms, REC-5).
The search row (`sijil_core.search_index`) becomes `sealed` in step 2's transaction.

Until the seal worker has locked it, the original stays under its GOVERNANCE lock at the
provisional date: the app key this service holds can neither delete nor shorten it (only the
seal worker's key could shorten it, and only the removal worker's could delete it, for a row in
`removed`, which a sealed row never is), and the `requeue_unlocked` sweep and the drift check
bound that window.

A field edit holds the same ledger lock; it commits its `document.fields_edited` event first and
its index write after (an applied edit is always audited; an index failure then answers 503
`field.update_failed`). The index write includes the search row, so search shows the edit as
soon as the request answers. A seal racing it either seals the values it locked, with their digest,
or gets `seal.fields_changed`. An applied edit of the supplier's name or ICE then sends
`sijil.learn_from_document` (`send_learning`).
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
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, AsyncTransaction

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
from sijil_core.index import FieldsChanged, FieldsRow, IndexEngines, RetentionLocked
from sijil_core.pending import EMBED_PREFIX, LOCK_PREFIX, PENDING_TTL_S, pending_key
from sijil_core.retention import REGION_TZ, fiscal_retain_until, policy_years, retention_basis
from sijil_core.search_index import SearchLedgerFacts, refresh_search_doc
from sijil_cp.api.documents import TOMBSTONES, TombstoneOut, tombstone
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
SEAL_UNFINISHED: Final = "seal.incomplete"
NOT_REVIEWABLE: Final = "document.not_reviewable"
INCOMPLETE: Final = "seal.fields_incomplete"
CHANGED: Final = "seal.fields_changed"
INDEX_UNAVAILABLE: Final = "seal.index_unavailable"
DB_UNAVAILABLE: Final = "seal.db_unavailable"
SEAL_FAILED: Final = "seal.failed"
UPDATE_FAILED: Final = "field.update_failed"
REVIEWABLE: Final = frozenset({DocumentState.filed, DocumentState.needs_review})
# An edit or a correction of one of these teaches the workspace its supplier (REC-5).
SUPPLIER_FIELDS: Final = frozenset({"supplier_name", "supplier_ice"})
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
    # The index row is frozen but the ledger is not: a seal that failed half-way, which
    # sealing again completes. Edits answer `seal.incomplete` meanwhile.
    seal_incomplete: bool


class FieldsOut(ReviewItem):
    sealed_at: datetime | None
    fields_sha256: str | None


class CorrectionOut(BaseModel):
    field: str
    value: str | None
    reason: str
    created_by: UUID
    created_at: datetime


class FieldsDetail(FieldsOut):
    """`GET …/fields`: the sealed values as they were, and the correction notes beside them
    (newest first)."""

    corrections: list[CorrectionOut]


class ReviewPage(BaseModel):
    items: list[ReviewItem]
    next_cursor: str | None


class FieldsPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fields: dict[str, str | None] = Field(min_length=1)


class SealBatchIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    month: str = Field(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
    # `next_cursor` of the previous call: continue after the last document it attempted.
    cursor: str | None = None


class Skipped(BaseModel):
    id: UUID
    code: str


class SealBatchOut(BaseModel):
    sealed: int
    skipped: list[Skipped]
    remaining: int
    next_cursor: str | None


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
        seal_incomplete=row is not None and row.sealed_at is not None and ledger.sealed_at is None,
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


def _decode_cursor(
    cursor: str, loc: tuple[str, str] = ("query", "cursor")
) -> tuple[datetime, UUID]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        at_text, id_text = raw.split("|")
        at = datetime.fromisoformat(at_text)
        if at.tzinfo is None:
            raise ValueError("naive")
        return at, UUID(id_text)
    except (binascii.Error, ValueError, UnicodeDecodeError):
        raise RequestValidationError(
            [{"loc": loc, "msg": "invalid cursor", "type": "value_error"}]
        ) from None


def _decode_batch_cursor(cursor: str) -> tuple[datetime, UUID]:
    return _decode_cursor(cursor, ("body", "cursor"))


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


@router.get(
    "/workspaces/{workspace_id}/documents/{doc_id}/fields",
    response_model=FieldsDetail | TombstoneOut,
    response_model_exclude_unset=True,
)
async def get_fields(
    doc_id: UUID, request: Request, ctx: Reading, user: CurrentUser, db: Db, now: Now
) -> FieldsDetail | TombstoneOut:
    """The fields, or for a removed or deleted document its tombstone, as GET …/documents/{id}
    answers: never the values, which stay in the index only until the purge."""
    workspace = ctx.workspace
    ledger = await db.scalar(
        select(DocumentLedger).where(
            DocumentLedger.id == doc_id, DocumentLedger.workspace_id == workspace.id
        )
    )
    if ledger is None:
        raise ApiError("document.not_found", 404)
    dead = ledger.state in TOMBSTONES
    row: FieldsRow | None = None
    notes: list[index_db.Correction] = []
    if not dead:
        async with (await _engines(request).engine(workspace)).connect() as conn:
            row = await index_db.read_fields(conn, ledger.id)
            notes = await index_db.read_corrections(conn, ledger.id)
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
    if dead:
        return tombstone(ledger, ctx.role)
    return FieldsDetail(
        **_full(ledger, row).model_dump(),
        corrections=[CorrectionOut.model_validate(n, from_attributes=True) for n in notes],
    )


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
    async with (await _engines(request).engine(workspace)).connect() as conn:
        trans = await conn.begin()
        try:
            row = await index_db.update_fields(
                conn, ledger.id, values, source="user", updated_by=user.id, now=now
            )
        except RetentionLocked:
            # The index row is frozen although the ledger is not yet: a seal half-way done,
            # which sealing again completes.
            raise ApiError(SEAL_UNFINISHED, 409) from None
        except LookupError:
            raise ApiError(NOT_REVIEWABLE, 409) from None
        try:
            await refresh_search_doc(conn, _search_facts(request, ledger, ledger.state, now))
        except Exception as exc:
            # Nothing applied: the index transaction rolls back with the edit.
            log.warning("field edit of document %s not applied: %s", doc_id, type(exc).__name__)
            raise ApiError(UPDATE_FAILED, 503) from None
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
        # The audit first (controller ruling): an applied edit always has its event. This
        # commit releases the ledger lock; a seal that starts now cannot hash the values being
        # replaced, because `seal_fields` recomputes the digest under the row lock this index
        # transaction still holds (`seal.fields_changed` if they moved).
        await db.commit()
        try:
            await _commit_index(trans)
        except Exception as exc:
            # A logged edit attempt that did not apply: acceptable, and said so.
            log.warning("field edit of document %s not applied: %s", doc_id, type(exc).__name__)
            raise ApiError(UPDATE_FAILED, 503) from None
    if SUPPLIER_FIELDS & set(names):
        await send_learning(request, ledger.id, workspace.id)
    return _full(ledger, row)


def _search_facts(
    request: Request, ledger: DocumentLedger, state: DocumentState, now: datetime
) -> SearchLedgerFacts:
    """The ledger facts of the document's search row (the region is this service's, which
    `require_workspace` checked against the workspace's)."""
    return SearchLedgerFacts(
        ledger_id=ledger.id,
        state=state,
        ingested_at=ledger.ingested_at,
        region=request.app.state.settings.region,
        now=now,
    )


async def _commit_index(trans: AsyncTransaction) -> None:
    """Commits the index edit (a seam: tests make it fail or pause)."""
    await trans.commit()


# Sealing -------------------------------------------------------------------------------------


class _IndexUnavailable(ApiError):
    """The workspace's index database failed during one seal."""

    def __init__(self) -> None:
        super().__init__(INDEX_UNAVAILABLE, 503)


@dataclass(frozen=True)
class _Sealer:
    """What one seal needs, so the single seal and the batch run the same code."""

    engine: AsyncEngine
    workspace_id: UUID
    region: str
    user_id: UUID
    ip: str | None
    now: datetime


def _missing(row: FieldsRow) -> list[str]:
    return sorted(n for n in _required(row.document_type) if row.values.get(n) is None)


async def _final_retention(
    db: AsyncSession, ledger: DocumentLedger, row: FieldsRow, sealer: _Sealer
) -> date:
    """The later of the ledger's retention and the fiscal year of the shared basis (a plausible
    document date, else the arrival date, never after today): a COMPLIANCE lock can never be
    shortened, so an implausible year never locks an original for longer."""
    basis = retention_basis(row.values.get("date"), ledger.ingested_at, sealer.region, sealer.now)
    years = await policy_years(db, sealer.workspace_id, row.document_type)
    return max(ledger.retention_until, fiscal_retain_until(basis, years))


async def _seal_locked(db: AsyncSession, sealer: _Sealer, ledger: DocumentLedger) -> FieldsOut:
    """Seals the ledger row the caller locked FOR UPDATE (see the module docstring) and
    commits. Raises ApiError for a document that cannot be sealed; nothing is committed then."""
    _refuse_unless_reviewable(ledger)
    try:
        async with sealer.engine.connect() as conn:
            row = await index_db.read_fields(conn, ledger.id)
    except (SQLAlchemyError, OSError) as exc:
        raise _IndexUnavailable from exc
    if row is None:
        raise ApiError(NOT_REVIEWABLE, 409)
    missing = _missing(row)
    if missing:
        raise ApiError(INCOMPLETE, 422, {"fields": missing})
    digest = fields_digest(row.values)
    if row.sealed_at is not None and row.fields_sha256 != digest:
        raise ApiError(LOCKED, 409)
    final = await _final_retention(db, ledger, row, sealer)
    # The index row frozen and the search row `sealed`, in their own committed transaction. The
    # digest is recomputed from the row it locks, so values that changed since the read above
    # are refused, never sealed under a stale hash. On a retry it is already sealed with this
    # hash: its stored `sealed_at` is the one the ledger gets.
    try:
        async with sealer.engine.begin() as conn:
            frozen = await index_db.seal_fields(
                conn, ledger.id, fields_sha256=digest, sealed_at=sealer.now
            )
            facts = SearchLedgerFacts(
                ledger_id=ledger.id,
                state=DocumentState.sealed,
                ingested_at=ledger.ingested_at,
                region=sealer.region,
                now=sealer.now,
            )
            await refresh_search_doc(conn, facts)
    except FieldsChanged:
        raise ApiError(CHANGED, 409) from None
    except RetentionLocked:
        raise ApiError(LOCKED, 409) from None
    except (SQLAlchemyError, OSError) as exc:
        raise _IndexUnavailable from exc
    # The ledger, under the lock taken by the caller, and the audit, in one transaction.
    ledger.state = DocumentState.sealed
    ledger.fields_sha256 = digest
    ledger.sealed_at = frozen.sealed_at or sealer.now
    ledger.sealed_by = sealer.user_id
    ledger.retention_until = final
    await audit.append(
        db,
        workspace_id=sealer.workspace_id,
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


async def _after_commit(request: Request, ledger_id: UUID, workspace_id: UUID) -> None:
    """The seal stands whatever happens here. A lost lock message is re-sent by the
    `requeue_unlocked` sweep (and flagged by the drift check after an hour); a lost mirror
    only delays Paperless's copy."""
    enqueuer = get_enqueuer(request)
    if await _claim(request, ledger_id, LOCK_PREFIX):
        try:
            await asyncio.to_thread(enqueuer.send_lock, ledger_id, workspace_id)
        except Exception as exc:  # noqa: BLE001 - the broker being down must not undo a seal
            log.warning("lock enqueue of document %s failed: %s", ledger_id, type(exc).__name__)
            await _release(request, ledger_id, LOCK_PREFIX)
    try:
        await asyncio.to_thread(enqueuer.send_mirror, ledger_id, workspace_id)
    except Exception as exc:  # noqa: BLE001
        log.warning("mirror enqueue of document %s failed: %s", ledger_id, type(exc).__name__)
    await send_embedding(request, ledger_id, workspace_id)
    await send_learning(request, ledger_id, workspace_id)


async def send_learning(request: Request, ledger_id: UUID, workspace_id: UUID) -> None:
    """After a commit that confirmed or corrected the document's supplier (an edit, a seal, a
    correction note): `sijil.learn_from_document`, which reads the effective values. Best
    effort: a lost message leaves the supplier to be learnt at the document's next edit."""
    try:
        await asyncio.to_thread(get_enqueuer(request).send_learn, ledger_id, workspace_id)
    except Exception as exc:  # noqa: BLE001 - the broker being down must not fail the request
        log.warning("learning enqueue of document %s failed: %s", ledger_id, type(exc).__name__)


async def send_embedding(request: Request, ledger_id: UUID, workspace_id: UUID) -> None:
    """After a commit that changed what the document's search row holds (seal, correction):
    `sijil.embed_document`, unless one is still queued (`embed:pending:<id>`), which will read
    the new row when it runs. Best effort: a lost message leaves the older chunks in place."""
    if not await _claim(request, ledger_id, EMBED_PREFIX):
        return
    try:
        await asyncio.to_thread(get_enqueuer(request).send_embed, ledger_id, workspace_id)
    except Exception as exc:  # noqa: BLE001 - the broker being down must not fail the request
        log.warning("embedding enqueue of document %s failed: %s", ledger_id, type(exc).__name__)
        await _release(request, ledger_id, EMBED_PREFIX)


async def _claim(request: Request, ledger_id: UUID, prefix: str) -> bool:
    """`<prefix>:pending:<id>`, the key the worker clears and the sweeps honour, so this
    message and another send never run together (`lock`: the seal worker; `embed`: the
    extraction worker). False when one is already in flight. A broker Redis that cannot answer
    does not stop the send (the workers are idempotent)."""
    try:
        claimed = await request.app.state.redis.set(
            pending_key(ledger_id, prefix), "1", nx=True, ex=PENDING_TTL_S
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("%s claim of document %s failed: %s", prefix, ledger_id, type(exc).__name__)
        return True
    return bool(claimed)


async def _release(request: Request, ledger_id: UUID, prefix: str) -> None:
    try:
        await request.app.state.redis.delete(pending_key(ledger_id, prefix))
    except Exception as exc:  # noqa: BLE001 - it expires anyway
        log.warning("%s release of document %s failed: %s", prefix, ledger_id, type(exc).__name__)


async def _sealer(
    request: Request, db: AsyncSession, workspace: Workspace, user: User, now: datetime
) -> _Sealer:
    """Workspace-level: an index database that cannot be reached fails the whole request."""
    org = await db.get(Organisation, workspace.org_id)
    if org is None:  # authorize already checked; keeps the type narrow
        raise ApiError("not_found", 404)
    try:
        engine = await _engines(request).engine(workspace)
    except (LookupError, SQLAlchemyError, OSError) as exc:
        log.warning("index of workspace %s unavailable: %s", workspace.id, type(exc).__name__)
        raise ApiError(INDEX_UNAVAILABLE, 503) from None
    return _Sealer(
        engine=engine,
        workspace_id=workspace.id,
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
    await _after_commit(request, ledger.id, sealer.workspace_id)
    return out


def _month_bounds(month: str, region: str) -> tuple[datetime, datetime]:
    """The month's first instant and the next month's, in the region's civil time."""
    year, number = (int(part) for part in month.split("-"))
    zone = ZoneInfo(REGION_TZ[region])
    start = datetime(year, number, 1, tzinfo=zone)
    end = datetime(year + number // 12, number % 12 + 1, 1, tzinfo=zone)
    return start, end


def _batch_cursor(at: datetime, ledger_id: UUID) -> str:
    raw = f"{at.isoformat()}|{ledger_id}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _failure_code(exc: Exception) -> str:
    """Index failures are `_IndexUnavailable` (an ApiError) by then; any other database or
    connection error is the control plane's own."""
    if isinstance(exc, ApiError):
        return exc.code
    if isinstance(exc, SQLAlchemyError | OSError):
        return DB_UNAVAILABLE
    return SEAL_FAILED


@router.post("/workspaces/{workspace_id}/seal-batch", response_model=SealBatchOut)
async def seal_batch(
    body: SealBatchIn, request: Request, ctx: Sealing, user: CurrentUser, db: Db, now: Now
) -> SealBatchOut:
    """Seals the month's `filed` documents (region-local arrival), oldest first, at most
    SEAL_BATCH_LIMIT per call, each in its own transaction through the single seal's code.

    A document that fails is rolled back and reported in `skipped` with its machine code; the
    others go on. `next_cursor` (the last attempted document) lets the client continue past
    documents that keep failing, and `remaining` counts the month's filed documents after it.
    """
    workspace = ctx.workspace
    sealer = await _sealer(request, db, workspace, user, now)
    # Detached, so the rollback after a skipped document does not expire what it holds.
    db.expunge(workspace)
    start, end = _month_bounds(body.month, sealer.region)
    in_month = [
        DocumentLedger.workspace_id == workspace.id,
        DocumentLedger.state == DocumentState.filed,
        DocumentLedger.ingested_at >= start,
        DocumentLedger.ingested_at < end,
    ]
    if body.cursor is not None:
        at, last_id = _decode_batch_cursor(body.cursor)
        in_month.append(tuple_(DocumentLedger.ingested_at, DocumentLedger.id) > (at, last_id))
    rows = list(
        (
            await db.execute(
                select(DocumentLedger.id, DocumentLedger.ingested_at)
                .where(*in_month)
                .order_by(DocumentLedger.ingested_at, DocumentLedger.id)
                .limit(SEAL_BATCH_LIMIT)
            )
        ).all()
    )
    await db.commit()
    sealed = 0
    skipped: list[Skipped] = []
    for doc_id, _ in rows:
        try:
            ledger = await _locked_ledger(db, workspace, doc_id)
            await _seal_locked(db, sealer, ledger)
        except Exception as exc:  # noqa: BLE001 - one document never aborts the batch
            await db.rollback()
            code = _failure_code(exc)
            if not isinstance(exc, ApiError):
                log.warning("seal of document %s failed: %s", doc_id, type(exc).__name__)
            skipped.append(Skipped(id=doc_id, code=code))
            continue
        sealed += 1
        await _after_commit(request, doc_id, workspace.id)
    remaining = 0
    next_cursor: str | None = None
    if rows:
        last_id, last_at = rows[-1]
        after = tuple_(DocumentLedger.ingested_at, DocumentLedger.id) > (last_at, last_id)
        remaining = int(await db.scalar(select(func.count()).where(*in_month, after)) or 0)
        if remaining:
            next_cursor = _batch_cursor(last_at, last_id)
    return SealBatchOut(
        sealed=sealed, skipped=skipped, remaining=remaining, next_cursor=next_cursor
    )
