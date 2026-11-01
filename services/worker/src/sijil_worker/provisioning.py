"""Turns a `provisioning` workspace into an `active` one. Safe to re-run at any point.

Each step uses its own short database session, so no transaction is held open while waiting
on Docker or Paperless. Secrets are created once, sealed, and reused on every retry. Errors
record only the step name: never a secret, never a response body.
"""

import logging
import secrets
from collections.abc import Callable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core.crypto import Envelope, KeyService, open_envelope, seal
from sijil_core.db.models import (
    CredentialKind,
    Job,
    JobStatus,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.paperless import SERVICE_PERMISSIONS, SUPPORT_PERMISSIONS, PaperlessClient
from sijil_worker.runtime.base import ADMIN_USERNAME, TenantRuntime, TenantSpec

log = logging.getLogger(__name__)

SERVICE_USERNAME = "sijil-service"
SUPPORT_USERNAME = "sijil-support"
READY_TIMEOUT_S = 300
SECRET_KEY_LENGTH = 50
_GENERATED = tuple(k for k in CredentialKind if k is not CredentialKind.service_token)


def _context(workspace_id: UUID, kind: CredentialKind) -> str:
    return f"{workspace_id}:{kind}"


def _new_secret(kind: CredentialKind) -> str:
    value = secrets.token_urlsafe(48 if kind is CredentialKind.secret_key else 32)
    return value[:SECRET_KEY_LENGTH] if kind is CredentialKind.secret_key else value


async def _load(
    session: AsyncSession, workspace_id: UUID
) -> dict[CredentialKind, TenantCredential]:
    rows = await session.scalars(
        select(TenantCredential).where(TenantCredential.workspace_id == workspace_id)
    )
    return {r.kind: r for r in rows}


async def _load_or_create_secrets(
    sessions: async_sessionmaker[AsyncSession], keys: KeyService, workspace: Workspace
) -> dict[CredentialKind, str]:
    for _attempt in range(2):
        async with sessions() as session:
            existing = await _load(session, workspace.id)
            for kind in _GENERATED:
                if kind not in existing:
                    envelope = seal(
                        _new_secret(kind).encode(),
                        keys,
                        workspace.kms_key_id,
                        _context(workspace.id, kind),
                    )
                    session.add(
                        TenantCredential(
                            workspace_id=workspace.id, kind=kind, envelope=envelope.to_bytes()
                        )
                    )
            try:
                await session.commit()
            except IntegrityError:
                # A concurrent run created some of them first: reload and reuse theirs.
                await session.rollback()
                continue
            existing = await _load(session, workspace.id)
            return {
                kind: open_envelope(
                    Envelope.from_bytes(existing[kind].envelope),
                    keys,
                    _context(workspace.id, kind),
                ).decode()
                for kind in _GENERATED
            }
    raise RuntimeError("could not settle tenant credentials")


async def _store_token(
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    workspace: Workspace,
    token: str,
) -> None:
    kind = CredentialKind.service_token
    blob = seal(token.encode(), keys, workspace.kms_key_id, _context(workspace.id, kind)).to_bytes()
    async with sessions() as session:
        existing = (await _load(session, workspace.id)).get(kind)
        if existing is None:
            session.add(TenantCredential(workspace_id=workspace.id, kind=kind, envelope=blob))
        else:
            existing.envelope = blob
        await session.commit()


async def _fail(
    sessions: async_sessionmaker[AsyncSession], workspace_id: UUID, job_id: UUID, step: str
) -> None:
    async with sessions() as session:
        job = await session.get(Job, job_id)
        workspace = await session.get(Workspace, workspace_id)
        if job is not None:
            job.status = JobStatus.failed
            job.error_code = f"provision.{step}"
        if workspace is not None:
            workspace.state = WorkspaceState.error
        await session.commit()


async def provision_workspace(
    workspace_id: UUID,
    job_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    runtime: TenantRuntime,
    paperless: Callable[..., PaperlessClient],
) -> None:
    step = "start"
    try:
        step = "start_job"
        async with sessions() as session:
            job = await session.get(Job, job_id)
            workspace = await session.get(Workspace, workspace_id)
            if job is None or workspace is None:
                raise LookupError("job or workspace not found")
            job.status = JobStatus.running
            job.error_code = None
            if workspace.state is WorkspaceState.error:
                workspace.state = WorkspaceState.provisioning
            await session.commit()

        step = "load_secrets"
        values = await _load_or_create_secrets(sessions, keys, workspace)
        spec = TenantSpec(
            workspace_id=workspace.id,
            slug=workspace.slug,
            db_name=workspace.db_name,
            index_db_name=workspace.index_db_name,
            db_password=values[CredentialKind.paperless_db_password],
            index_db_password=values[CredentialKind.index_db_password],
            secret_key=values[CredentialKind.secret_key],
            admin_password=values[CredentialKind.admin_password],
        )

        step = "ensure_databases"
        await runtime.ensure_databases(spec)

        step = "ensure_running"
        handle = await runtime.ensure_running(spec)

        async with paperless(handle.url, basic=(ADMIN_USERNAME, spec.admin_password)) as client:
            step = "wait_ready"
            await client.wait_ready(READY_TIMEOUT_S)

            step = "ensure_users"
            service_password = secrets.token_urlsafe(32)
            await client.ensure_user(SERVICE_USERNAME, service_password, SERVICE_PERMISSIONS)
            await client.ensure_user(
                SUPPORT_USERNAME, values[CredentialKind.support_password], SUPPORT_PERMISSIONS
            )

            step = "obtain_token"
            token = await client.obtain_token(SERVICE_USERNAME, service_password)

        step = "store_token"
        await _store_token(sessions, keys, workspace, token)

        step = "activate"
        async with sessions() as session:
            job = await session.get(Job, job_id)
            row = await session.get(Workspace, workspace_id)
            if job is None or row is None:
                raise LookupError("job or workspace not found")
            row.paperless_url = handle.url
            row.state = WorkspaceState.active
            job.status = JobStatus.succeeded
            await session.commit()
    except Exception:
        # Only the step name is recorded; the exception itself carries no secrets by contract.
        log.error("provisioning failed at step %s for workspace %s", step, workspace_id)
        await _fail(sessions, workspace_id, job_id, step)
        raise
