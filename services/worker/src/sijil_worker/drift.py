"""Nightly integrity drift check.

For every ledger row of a workspace it confirms that our object-locked copy still exists with
the size the ledger recorded, and for every `consumed` one also that the original is still in the
tenant's Paperless-ngx and unchanged (Paperless 3.4.1 reports the SHA-256 of the original as
`original_checksum`; it must equal the ledger's `sha256`).

Every problem becomes an `IntegrityAlert` (never a second open one for the same document and
kind) plus an `integrity.drift` audit event; the run ends with `integrity.check_ok` or
`integrity.drift_summary`. Paperless being unreachable is transient: the error propagates so the
task is retried later, and nothing is flagged on its account until the retries run out.

A check that cannot run is itself a finding, because the attacker the check defends against (one
who deleted a document with the admin password) can also revoke the service token or its view
permission. A refused token (401/403), a missing or undecryptable credential, an unexpected
error response, and exhausted retries each open a workspace-level alert (`ledger_id` NULL, kind
`engine_*`, deduplicated while open) and write `integrity.check_failed` on every run. Metadata
of an unexpected shape for one document is a per-document `paperless_bad_metadata` finding, and
the run goes on with the rest.

The ledger is read page by page (keyset on id) in short sessions and the network checks run
between them, so no transaction is ever held open across a network wait. Alerts and audit events
of a page are written together in one short transaction. Alert detail carries ids, sizes and
hashes only, never a filename or content.
"""

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core import audit
from sijil_core.crypto import DecryptionError, Envelope, KeyService, open_envelope
from sijil_core.db.models import (
    ActorType,
    CredentialKind,
    DocumentLedger,
    IngestState,
    IntegrityAlert,
    Organisation,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.paperless import (
    PaperlessBadResponse,
    PaperlessClient,
    PaperlessError,
    PaperlessUnavailable,
)
from sijil_core.region import assert_region
from sijil_core.settings import CoreSettings
from sijil_core.storage import ObjectRef, ObjectStore
from sijil_worker.ingest import MissingCredential

log = logging.getLogger(__name__)

BATCH_SIZE = 200

PAPERLESS_MISSING = "paperless_missing"
PAPERLESS_MODIFIED = "paperless_modified"
OBJECT_MISSING = "object_missing"
OBJECT_SIZE_MISMATCH = "object_size_mismatch"
PAPERLESS_BAD_METADATA = "paperless_bad_metadata"
# Workspace-level: the check could not run.
ENGINE_FORBIDDEN = "engine_forbidden"
ENGINE_CREDENTIAL_MISSING = "engine_credential_missing"
ENGINE_BAD_RESPONSE = "engine_bad_response"
ENGINE_UNREACHABLE = "engine_unreachable"


@dataclass(frozen=True)
class DriftReport:
    checked: int
    problems: int
    # The engine_* code when the check could not finish (recorded as a finding).
    failed: str | None = None


class _CheckFailed(Exception):  # noqa: N818
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class _Doc:
    id: UUID
    sha256: str
    size_bytes: int
    ref: ObjectRef
    paperless_doc_id: int | None


@dataclass(frozen=True)
class _Finding:
    ledger_id: UUID | None
    kind: str
    detail: dict[str, int | str]


@dataclass(frozen=True)
class _Target:
    paperless_url: str
    token: str


async def _target(
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    settings: CoreSettings,
    workspace_id: UUID,
) -> _Target | None:
    """The tenant's URL and service token, or None when the workspace is not active."""
    async with sessions() as session:
        workspace = await session.get(Workspace, workspace_id)
        org = None if workspace is None else await session.get(Organisation, workspace.org_id)
        if workspace is None or org is None:
            raise LookupError("workspace not found")
        # Before anything leaves the database: this worker only serves its own region.
        assert_region(org.region, settings)
        if workspace.state is not WorkspaceState.active or not workspace.paperless_url:
            return None
        credential = await session.scalar(
            select(TenantCredential).where(
                TenantCredential.workspace_id == workspace_id,
                TenantCredential.kind == CredentialKind.service_token,
            )
        )
        if credential is None:
            raise MissingCredential("no service token")
        token = open_envelope(
            Envelope.from_bytes(credential.envelope),
            keys,
            f"{workspace_id}:{CredentialKind.service_token}",
        ).decode()
        return _Target(workspace.paperless_url, token)


async def _page(
    sessions: async_sessionmaker[AsyncSession], workspace_id: UUID, after: UUID | None
) -> list[_Doc]:
    query = (
        select(
            DocumentLedger.id,
            DocumentLedger.sha256,
            DocumentLedger.size_bytes,
            DocumentLedger.object_key,
            DocumentLedger.object_version_id,
            DocumentLedger.paperless_doc_id,
            DocumentLedger.ingest_state,
        )
        .where(DocumentLedger.workspace_id == workspace_id)
        .order_by(DocumentLedger.id)
        .limit(BATCH_SIZE)
    )
    if after is not None:
        query = query.where(DocumentLedger.id > after)
    async with sessions() as session:
        rows = (await session.execute(query)).all()
    return [
        _Doc(
            r.id,
            r.sha256,
            r.size_bytes,
            ObjectRef(r.object_key, r.object_version_id),
            # Paperless is asked only about what it consumed; the object, about every row.
            r.paperless_doc_id if r.ingest_state == IngestState.consumed else None,
        )
        for r in rows
    ]


async def _metadata(client: PaperlessClient, doc_id: int) -> dict[str, object] | None:
    """Paperless's metadata for one document; a failure that is not about it ends the run."""
    try:
        return await client.document_metadata(doc_id)
    except (PaperlessBadResponse, PaperlessUnavailable):
        raise
    except PaperlessError as exc:
        if exc.status in {401, 403}:
            raise _CheckFailed(ENGINE_FORBIDDEN) from None
        raise _CheckFailed(ENGINE_BAD_RESPONSE) from None


async def _check(doc: _Doc, client: PaperlessClient, store: ObjectStore) -> list[_Finding]:
    found: list[_Finding] = []
    if doc.paperless_doc_id is not None:
        detail: dict[str, int | str] = {"paperless_doc_id": doc.paperless_doc_id}
        try:
            meta = await _metadata(client, doc.paperless_doc_id)
        except PaperlessBadResponse:
            meta, checksum = {}, None
        else:
            checksum = None if meta is None else meta.get("original_checksum")
        if meta is None:
            found.append(_Finding(doc.id, PAPERLESS_MISSING, detail))
        elif not isinstance(checksum, str) or not checksum:
            # Unknown shape: never guess a verdict on the content, but say so.
            found.append(_Finding(doc.id, PAPERLESS_BAD_METADATA, detail))
        elif checksum.lower() != doc.sha256.lower():
            found.append(_Finding(doc.id, PAPERLESS_MODIFIED, detail))
    size = await store.head(doc.ref)
    if size is None:
        found.append(_Finding(doc.id, OBJECT_MISSING, {"expected_size": doc.size_bytes}))
    elif size != doc.size_bytes:
        found.append(
            _Finding(
                doc.id,
                OBJECT_SIZE_MISMATCH,
                {"expected_size": doc.size_bytes, "observed_size": size},
            )
        )
    return found


async def _lock(session: AsyncSession, workspace_id: UUID) -> None:
    """Serialises concurrent runs for the workspace, so the open-alert check cannot race."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended('drift:' || CAST(:ws AS text), 0))"),
        {"ws": str(workspace_id)},
    )


async def _open_alert(
    session: AsyncSession, workspace_id: UUID, finding: _Finding, now: datetime
) -> bool:
    """Add an alert unless one is open for the same document (or workspace) and kind."""
    same_target = (
        IntegrityAlert.ledger_id.is_(None)
        if finding.ledger_id is None
        else IntegrityAlert.ledger_id == finding.ledger_id
    )
    open_alert = await session.scalar(
        select(IntegrityAlert.id).where(
            IntegrityAlert.workspace_id == workspace_id,
            same_target,
            IntegrityAlert.kind == finding.kind,
            IntegrityAlert.resolved_at.is_(None),
        )
    )
    if open_alert is not None:
        return False
    session.add(
        IntegrityAlert(
            workspace_id=workspace_id,
            ledger_id=finding.ledger_id,
            kind=finding.kind,
            detail=finding.detail,
            detected_at=now,
        )
    )
    return True


async def record_check_failed(
    sessions: async_sessionmaker[AsyncSession],
    workspace_id: UUID,
    code: str,
    *,
    now: datetime,
) -> None:
    """The check could not run: a workspace-level alert (once while open) and an audit event
    (every time), in one transaction."""
    async with sessions() as session:
        await _lock(session, workspace_id)
        await _open_alert(session, workspace_id, _Finding(None, code, {"code": code}), now)
        await audit.append(
            session,
            workspace_id=workspace_id,
            action="integrity.check_failed",
            actor_type=ActorType.system,
            actor_id=None,
            data={"code": code},
            now=now,
        )
        await session.commit()
    log.warning("drift check %s could not run: %s", workspace_id, code)


async def _record(
    sessions: async_sessionmaker[AsyncSession],
    workspace_id: UUID,
    findings: list[_Finding],
    now: datetime,
) -> int:
    """Open alerts for findings that have none yet, with audit. Returns how many are new."""
    if not findings:
        return 0
    new = 0
    async with sessions() as session:
        await _lock(session, workspace_id)
        for finding in findings:
            if not await _open_alert(session, workspace_id, finding, now):
                continue
            await audit.append(
                session,
                workspace_id=workspace_id,
                action="integrity.drift",
                actor_type=ActorType.system,
                actor_id=None,
                target_type="document",
                target_id=str(finding.ledger_id),
                data={"kind": finding.kind},
                now=now,
            )
            new += 1
        await session.commit()
    return new


async def _summarise(
    sessions: async_sessionmaker[AsyncSession],
    workspace_id: UUID,
    report: DriftReport,
    new: int,
    now: datetime,
) -> None:
    ok = report.problems == 0
    async with sessions() as session:
        await audit.append(
            session,
            workspace_id=workspace_id,
            action="integrity.check_ok" if ok else "integrity.drift_summary",
            actor_type=ActorType.system,
            actor_id=None,
            data=(
                {"checked": report.checked}
                if ok
                else {"checked": report.checked, "problems": report.problems, "new": new}
            ),
            now=now,
        )
        await session.commit()


async def drift_check(
    workspace_id: UUID,
    *,
    sessions: async_sessionmaker[AsyncSession],
    keys: KeyService,
    store: ObjectStore,
    paperless: Callable[..., PaperlessClient],
    settings: CoreSettings,
    now: datetime,
) -> DriftReport:
    try:
        target = await _target(sessions, keys, settings, workspace_id)
    except (MissingCredential, DecryptionError, ValueError):
        # ValueError: an envelope that does not even parse.
        await record_check_failed(sessions, workspace_id, ENGINE_CREDENTIAL_MISSING, now=now)
        return DriftReport(0, 0, failed=ENGINE_CREDENTIAL_MISSING)
    if target is None:
        return DriftReport(0, 0)
    checked = problems = new = 0
    after: UUID | None = None
    async with paperless(target.paperless_url, token=target.token) as client:
        while page := await _page(sessions, workspace_id, after):
            findings: list[_Finding] = []
            failed: str | None = None
            for doc in page:
                try:
                    findings += await _check(doc, client, store)
                except _CheckFailed as exc:
                    failed = exc.code
                    break
                checked += 1
            new += await _record(sessions, workspace_id, findings, now)
            problems += len(findings)
            if failed is not None:
                await record_check_failed(sessions, workspace_id, failed, now=now)
                return DriftReport(checked, problems, failed=failed)
            after = page[-1].id
    report = DriftReport(checked=checked, problems=problems)
    await _summarise(sessions, workspace_id, report, new, now)
    log.info("drift check %s: checked=%d problems=%d", workspace_id, checked, problems)
    return report


async def enqueue_drift_checks(
    sessions: async_sessionmaker[AsyncSession],
    settings: CoreSettings,
    enqueue: Callable[[UUID], Awaitable[None]],
) -> int:
    """Fan out one drift check per active workspace of this worker's region."""
    async with sessions() as session:
        ids = list(
            await session.scalars(
                select(Workspace.id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(
                    Workspace.state == WorkspaceState.active,
                    Organisation.region == settings.region,
                )
                .order_by(Workspace.id)
            )
        )
    for workspace_id in ids:
        await enqueue(workspace_id)
    return len(ids)
