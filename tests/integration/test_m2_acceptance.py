"""M2 acceptance: twelve SAMPLE invoices come in through the API (6) and by email (6) and are
archived, hashed, consumed by Paperless, audited and drift-checked. HTTP and SMTP only, like a
customer; the database is read to compare ledger hashes and to prove the app role cannot
rewrite them. Uses its own throw-away user and organisation (never the `make seed` ones).
"""

import asyncio
import hashlib
import os
import sys
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager

import pytest
from sqlalchemy import select, update
from sqlalchemy.exc import DBAPIError

from sijil_core.crypto import Envelope, LocalKeyService, open_envelope
from sijil_core.db.base import utcnow
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    ActorType,
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    IngestState,
    IntegrityAlert,
    TenantCredential,
    Workspace,
)
from sijil_core.paperless import PaperlessClient
from sijil_core.storage import S3ObjectStore, StorageSettings
from sijil_core.tenancy import tenant_ref
from sijil_worker.drift import DriftReport, drift_check
from sijil_worker.runtime.base import ADMIN_USERNAME
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import WorkerSettings
from tests.support import seed

AttachRunner = Callable[[str], AbstractContextManager[None]]

pytestmark = pytest.mark.integration

CONSUME_BUDGET_S = 120


async def test_m2_acceptance(
    docker_runtime: DockerRuntime, worker_settings: WorkerSettings, attach_runner: AttachRunner
) -> None:
    run = uuid.uuid4().hex[:8]
    created: list[uuid.UUID] = []  # filled the moment the workspace exists, before seeding can fail
    engine = make_engine(worker_settings.database_url)
    sessions = make_sessionmaker(engine)
    keys = LocalKeyService(worker_settings.master_key_bytes())
    try:
        seeded = await seed.seed(
            email=f"m2-{run}@sijil.local",
            org_name=f"SAMPLE M2 Acceptance {run}",
            consume_budget_s=CONSUME_BUDGET_S,
            on_workspace=created.append,
        )
        workspace_id = seeded.workspace_id
        ref = tenant_ref(workspace_id)
        # 1-3. Twelve invoices, six of them by email (+ their six .eml); sha256 equals the file's
        # own; everything consumed (seed waits up to CONSUME_BUDGET_S).
        assert len(seeded.fixtures) == 12
        async with sessions() as db:
            rows = list(
                await db.scalars(
                    select(DocumentLedger).where(DocumentLedger.workspace_id == workspace_id)
                )
            )
        by_sha = {r.sha256: r for r in rows}
        for path in seeded.fixtures:
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            assert sha in by_sha, path.name
            assert by_sha[sha].ingest_state is IngestState.consumed, path.name
        assert len(rows) == 12 + 6  # + the six raw .eml of the emailed invoices
        assert all(r.ingest_state is IngestState.consumed for r in rows)

        # 4. Uploading one again: 409 with the same document id.
        async with seed.signed_in(seeded.email) as http:
            first = seeded.fixtures[0]
            r = await seed.upload(http, workspace_id, first)
            assert r.status_code == 409, r.text
            body = r.json()
            assert body["error"]["code"] == "document.duplicate"
            assert body["document_id"] == str(
                by_sha[hashlib.sha256(first.read_bytes()).hexdigest()].id
            )

        # 5. The audit chain verifies.
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "sijil_cp.cli",
            "audit",
            "verify",
            "--workspace",
            str(workspace_id),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            # The runner carries the worker's environment; the CLI also wants the API's one flag.
            env=os.environ | {"SIJIL_COOKIE_SECURE": "false"},
        )
        out, _ = await proc.communicate()
        assert proc.returncode == 0, out.decode()
        assert out.decode().startswith("ok ")

        # 6. The app role cannot rewrite a ledger hash.
        async with sessions() as db:
            with pytest.raises(DBAPIError, match="ledger.immutable"):
                await db.execute(
                    update(DocumentLedger)
                    .where(DocumentLedger.id == rows[0].id)
                    .values(sha256="f" * 64)
                )
                await db.commit()

        # 7. Delete one document in Paperless with the admin credential; drift is flagged.
        victim = next(r for r in rows if r.content_kind == "pdf" and r.paperless_doc_id)
        async with sessions() as db:
            workspace = await db.get(Workspace, workspace_id)
            assert workspace is not None and workspace.paperless_url is not None
            cred = await db.scalar(
                select(TenantCredential).where(
                    TenantCredential.workspace_id == workspace_id,
                    TenantCredential.kind == CredentialKind.admin_password,
                )
            )
            assert cred is not None
            admin_password = open_envelope(
                Envelope.from_bytes(cred.envelope),
                keys,
                f"{workspace_id}:{CredentialKind.admin_password}",
            ).decode()
            url = workspace.paperless_url
        with attach_runner(docker_runtime.network_name(ref)):
            async with PaperlessClient(url, basic=(ADMIN_USERNAME, admin_password)) as admin:
                assert victim.paperless_doc_id is not None
                await admin.delete_document(victim.paperless_doc_id)
            report = await drift_check(
                workspace_id,
                sessions=sessions,
                keys=keys,
                store=S3ObjectStore(StorageSettings()),  # type: ignore[call-arg]
                paperless=PaperlessClient,
                settings=worker_settings,
                now=utcnow(),
            )
        assert report == DriftReport(checked=18, problems=1)
        async with sessions() as db:
            alerts = list(
                await db.scalars(
                    select(IntegrityAlert).where(IntegrityAlert.workspace_id == workspace_id)
                )
            )
        assert [(a.ledger_id, a.kind) for a in alerts] == [(victim.id, "paperless_missing")]
        async with sessions() as db:
            [event] = list(
                await db.scalars(
                    select(AuditEvent).where(
                        AuditEvent.workspace_id == workspace_id,
                        AuditEvent.action == "integrity.drift",
                    )
                )
            )
        assert event.actor_type is ActorType.system
        assert event.target_id == str(victim.id)
        assert event.data == {"kind": "paperless_missing"}
    finally:
        # Each step on its own, so one failing never skips the rest; the tenant always goes.
        try:
            await engine.dispose()
        finally:
            for created_id in created:
                await docker_runtime.destroy(tenant_ref(created_id))
