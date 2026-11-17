"""Account closure (plan 6.4, ADM-5, design §7 item 11). Every route is the Owner's
(`Action.org_close`; an Admin or a Member gets 403, a non-member 404) and none is behind the
billing gate (ruling R4): an account can always be closed.

- `POST /orgs/{id}/closure` (step-up): opens a closure in `grace` until `requested_at + 30
  days`, sets the org `closing`, queues a full export (period `all`) of each of its workspaces
  (`export.requested` on each, then sent to the export worker after the commit) and audits
  `closure.requested` `{closure_id, grace_ends_at}`. 201. A closure already open is 409
  `closure.already_requested`. During the grace period everything keeps working under the
  billing gates as they stand; no invoice is issued (`sijil_core.billing.advance`).
- `DELETE /orgs/{id}/closure` (step-up): cancels it during the grace period only (to the end
  of its last region-local day, `sijil_core.closure.grace_over`): the org is `active` again,
  audited `closure.cancelled`. None open is 404 `closure.not_requested`; after the grace period
  (or once hibernated) 409 `closure.grace_over`. A later request starts a new grace period.
- `GET /orgs/{id}/closure`: the open closure, with its take-over statement if signed; none is
  404 `closure.not_requested`.
- `POST /orgs/{id}/closure/takeover` (step-up; multipart: `file`, the signed statement PDF,
  `signer_name`, `signer_capacity`): the statement that the org takes custody of its originals
  and has downloaded its full export. Every workspace still hosted needs a full export
  requested since the closure and `ready` (downloadable); a missing or expired one, a file
  that is not a PDF, an empty or over-long name or capacity, or a second statement is 422
  `takeover.invalid`. The PDF is put write-once in the originals bucket under its own
  COMPLIANCE lock (`takeover/<org_id>/<id>.pdf`, kept TAKEOVER_RETENTION_YEARS or until the
  org's latest retention, whichever is later), its SHA-256 in the write-once
  `takeover_statement` row, audited `closure.takeover_signed` `{statement_id, sha256}`. After
  the grace period the closure job then destroys every tenant even with documents under
  retention, whose originals stay locked until their retention passes (sijil_worker.closure).
"""

import hashlib
import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Annotated, Final
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit
from sijil_core import closure as closing
from sijil_core.db.models import (
    TERMINAL_STATES,
    ActorType,
    Closure,
    ClosureState,
    DocumentLedger,
    ExportJob,
    ExportState,
    Organisation,
    OrgStatus,
    TakeoverStatement,
    User,
    UserSession,
    Workspace,
    WorkspaceState,
)
from sijil_core.export_report import FULL_PERIOD
from sijil_core.filetypes import FileKind, sniff
from sijil_core.retention import region_local_date
from sijil_core.storage import ObjectStore
from sijil_cp.deps import (
    current_session,
    ensure_step_up,
    get_db,
    get_now,
    require_totp_compliance,
)
from sijil_cp.enqueue import Enqueuer, get_enqueuer
from sijil_cp.errors import ApiError
from sijil_cp.org_audit import audit_org_event
from sijil_cp.permissions import Action, authorize_org

log = logging.getLogger(__name__)

router = APIRouter(tags=["closure"], dependencies=[Depends(require_totp_compliance)])

Db = Annotated[AsyncSession, Depends(get_db)]
CurrentUser = Annotated[User, Depends(require_totp_compliance)]
Session = Annotated[UserSession, Depends(current_session())]
Now = Annotated[datetime, Depends(get_now)]
Queue = Annotated[Enqueuer, Depends(get_enqueuer)]

MAX_STATEMENT_BYTES: Final = 10 * 1024 * 1024
MAX_SIGNER_CHARS: Final = 200
_CHUNK: Final = 1024 * 1024


class TakeoverOut(BaseModel):
    id: UUID
    sha256: str
    size_bytes: int
    signer_name: str
    signer_capacity: str
    signed_at: datetime


class ClosureOut(BaseModel):
    id: UUID
    state: ClosureState
    requested_at: datetime
    grace_ends_at: datetime
    export_job_id: UUID | None
    cancelled_at: datetime | None
    hibernated_at: datetime | None
    destroyed_at: datetime | None
    takeover: TakeoverOut | None


async def _out(db: AsyncSession, row: Closure) -> ClosureOut:
    statement = await db.scalar(
        select(TakeoverStatement).where(TakeoverStatement.closure_id == row.id)
    )
    takeover = (
        None
        if statement is None
        else TakeoverOut(
            id=statement.id,
            sha256=statement.sha256,
            size_bytes=statement.size_bytes,
            signer_name=statement.signer_name,
            signer_capacity=statement.signer_capacity,
            signed_at=statement.created_at,
        )
    )
    return ClosureOut(
        id=row.id,
        state=row.state,
        requested_at=row.requested_at,
        grace_ends_at=row.grace_ends_at,
        export_job_id=row.export_job_id,
        cancelled_at=row.cancelled_at,
        hibernated_at=row.hibernated_at,
        destroyed_at=row.destroyed_at,
        takeover=takeover,
    )


async def _owner(db: AsyncSession, request: Request, user: User, org_id: UUID) -> Organisation:
    org, _role = await authorize_org(
        db, user.id, org_id, Action.org_close, request.app.state.settings
    )
    return org


async def _locked_org(db: AsyncSession, org_id: UUID) -> Organisation:
    org = await db.scalar(
        select(Organisation)
        .where(Organisation.id == org_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if org is None:  # authorize_org found it; keeps the type narrow
        raise ApiError("not_found", 404)
    return org


async def _hosted(db: AsyncSession, org_id: UUID) -> list[Workspace]:
    """The org's workspaces not yet destroyed, oldest first."""
    rows = await db.scalars(
        select(Workspace)
        .where(Workspace.org_id == org_id, Workspace.state != WorkspaceState.closed)
        .order_by(Workspace.created_at, Workspace.id)
    )
    return list(rows)


def _ip(request: Request) -> str | None:
    return request.client.host if request.client else None


@router.post("/orgs/{org_id}/closure", status_code=201, response_model=ClosureOut)
async def request_closure(
    org_id: UUID,
    request: Request,
    user: CurrentUser,
    session: Session,
    db: Db,
    now: Now,
    enqueuer: Queue,
) -> ClosureOut:
    await _owner(db, request, user, org_id)
    ensure_step_up(session, user, now)
    org = await _locked_org(db, org_id)
    if await closing.open_closure(db, org_id) is not None:
        raise ApiError(closing.ALREADY_REQUESTED, 409)
    row = Closure(
        org_id=org_id,
        requested_at=now,
        requested_by=user.id,
        grace_ends_at=closing.grace_ends(now),
        state=ClosureState.grace,
    )
    db.add(row)
    jobs: list[ExportJob] = []
    for workspace in await _hosted(db, org_id):
        job = ExportJob(
            workspace_id=workspace.id, period=FULL_PERIOD, requested_by=user.id, created_at=now
        )
        db.add(job)
        await db.flush()
        await audit.append(
            db,
            workspace_id=workspace.id,
            action="export.requested",
            actor_type=ActorType.user,
            actor_id=str(user.id),
            target_type="export",
            target_id=str(job.id),
            ip=_ip(request),
            data={"period": FULL_PERIOD},
            now=now,
        )
        jobs.append(job)
    row.export_job_id = jobs[0].id if jobs else None
    org.status = OrgStatus.closing
    await db.flush()
    await audit_org_event(
        db,
        request,
        org_id=org_id,
        actor=user,
        action="closure.requested",
        target_type="closure",
        target_id=str(row.id),
        data={"closure_id": str(row.id), "grace_ends_at": row.grace_ends_at.isoformat()},
        now=now,
    )
    try:
        await db.commit()
    except IntegrityError:
        # A concurrent request opened one first (`uq_closure_org_open`).
        await db.rollback()
        raise ApiError(closing.ALREADY_REQUESTED, 409) from None
    for job in jobs:
        try:
            enqueuer.send_export(job.id)
        except Exception as exc:
            # The job is committed and stays `queued`: the export sweep sends it again.
            log.error("export %s not sent: %s", job.id, type(exc).__name__)
    return await _out(db, row)


@router.delete("/orgs/{org_id}/closure", response_model=ClosureOut)
async def cancel_closure(
    org_id: UUID, request: Request, user: CurrentUser, session: Session, db: Db, now: Now
) -> ClosureOut:
    await _owner(db, request, user, org_id)
    ensure_step_up(session, user, now)
    org = await _locked_org(db, org_id)
    row = await closing.open_closure(db, org_id, lock=True)
    if row is None:
        raise ApiError(closing.NOT_REQUESTED, 404)
    if row.state is not ClosureState.grace or closing.grace_over(row, now, org.region):
        raise ApiError(closing.GRACE_OVER, 409)
    row.state = ClosureState.cancelled
    row.cancelled_at = now
    row.cancelled_by = user.id
    org.status = OrgStatus.active
    await db.flush()
    await audit_org_event(
        db,
        request,
        org_id=org_id,
        actor=user,
        action="closure.cancelled",
        target_type="closure",
        target_id=str(row.id),
        data={"closure_id": str(row.id)},
        now=now,
    )
    await db.commit()
    return await _out(db, row)


@router.get("/orgs/{org_id}/closure", response_model=ClosureOut)
async def get_closure(org_id: UUID, request: Request, user: CurrentUser, db: Db) -> ClosureOut:
    await _owner(db, request, user, org_id)
    row = await closing.open_closure(db, org_id)
    if row is None:
        raise ApiError(closing.NOT_REQUESTED, 404)
    return await _out(db, row)


def _signer(value: str) -> str:
    cleaned = value.strip()
    if not cleaned or len(cleaned) > MAX_SIGNER_CHARS:
        raise ApiError(closing.TAKEOVER_INVALID, 422)
    return cleaned


async def _exports_ready(db: AsyncSession, row: Closure) -> bool:
    """Every workspace still hosted has a full export requested since the closure and ready
    to download."""
    for workspace in await _hosted(db, row.org_id):
        ready = await db.scalar(
            select(ExportJob.id)
            .where(
                ExportJob.workspace_id == workspace.id,
                ExportJob.period == FULL_PERIOD,
                ExportJob.state == ExportState.ready,
                ExportJob.created_at >= row.requested_at,
            )
            .limit(1)
        )
        if ready is None:
            return False
    return True


async def _spool(upload: UploadFile, path: Path) -> tuple[str, int]:
    """Copies the upload to `path`, at most MAX_STATEMENT_BYTES; its SHA-256 and size."""
    digest, size = hashlib.sha256(), 0
    with path.open("wb") as out:
        while chunk := await upload.read(_CHUNK):
            size += len(chunk)
            if size > MAX_STATEMENT_BYTES:
                raise ApiError(closing.TAKEOVER_INVALID, 422)
            digest.update(chunk)
            out.write(chunk)
    if size == 0:
        raise ApiError(closing.TAKEOVER_INVALID, 422)
    return digest.hexdigest(), size


@router.post("/orgs/{org_id}/closure/takeover", status_code=201, response_model=TakeoverOut)
async def sign_takeover(
    org_id: UUID,
    request: Request,
    user: CurrentUser,
    session: Session,
    db: Db,
    now: Now,
    file: Annotated[UploadFile, File()],
    signer_name: Annotated[str, Form()],
    signer_capacity: Annotated[str, Form()],
) -> TakeoverOut:
    org = await _owner(db, request, user, org_id)
    ensure_step_up(session, user, now)
    row = await closing.open_closure(db, org_id, lock=True)
    if row is None:
        raise ApiError(closing.NOT_REQUESTED, 404)
    signed = await db.scalar(
        select(TakeoverStatement.id).where(TakeoverStatement.closure_id == row.id)
    )
    if row.state is ClosureState.destroyed or signed is not None:
        raise ApiError(closing.TAKEOVER_INVALID, 422)
    name, capacity = _signer(signer_name), _signer(signer_capacity)
    if not await _exports_ready(db, row):
        raise ApiError(closing.TAKEOVER_INVALID, 422)
    latest = await db.scalar(
        select(func.max(DocumentLedger.retention_until))
        .join(Workspace, Workspace.id == DocumentLedger.workspace_id)
        .where(Workspace.org_id == org_id, DocumentLedger.state.not_in(TERMINAL_STATES))
    )
    retain_until = closing.takeover_retain_until(region_local_date(now, org.region), latest)
    statement_id = uuid4()
    store: ObjectStore = request.app.state.store
    with tempfile.TemporaryDirectory(prefix="sijil-takeover-") as tmp:
        path = Path(tmp) / "statement.pdf"
        sha256, size = await _spool(file, path)
        if sniff(path) is not FileKind.pdf:
            raise ApiError(closing.TAKEOVER_INVALID, 422)
        # COMPLIANCE: legal evidence, which no key may delete or shorten before its date.
        ref = await store.put_original(
            f"takeover/{org_id}/{statement_id}.pdf", path, retain_until, compliance=True
        )
    statement = TakeoverStatement(
        id=statement_id,
        closure_id=row.id,
        org_id=org_id,
        signed_by=user.id,
        signer_name=name,
        signer_capacity=capacity,
        object_key=ref.key,
        object_version_id=ref.version_id,
        sha256=sha256,
        size_bytes=size,
        created_at=now,
    )
    db.add(statement)
    await db.flush()
    await audit_org_event(
        db,
        request,
        org_id=org_id,
        actor=user,
        action="closure.takeover_signed",
        target_type="closure",
        target_id=str(row.id),
        data={"statement_id": str(statement_id), "sha256": sha256},
        now=now,
    )
    await db.commit()
    return TakeoverOut(
        id=statement_id,
        sha256=sha256,
        size_bytes=size,
        signer_name=name,
        signer_capacity=capacity,
        signed_at=now,
    )
