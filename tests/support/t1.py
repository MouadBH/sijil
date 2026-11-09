"""A throw-away workspace pointing at the compose sample tenant `paperless-t1`.

For integration tests in the compose `tests` runner: RustFS for the originals, the control-plane
database for the ledger, and t1 over its tenant network. The workspace row carries a sealed
token of a dedicated service user, so worker code runs exactly as in production.
"""

import hashlib
import os
import secrets
import uuid
from datetime import timedelta
from pathlib import Path

from sijil_core.crypto import LocalKeyService, seal
from sijil_core.db.base import utcnow
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    CredentialKind,
    DocumentLedger,
    DocumentSource,
    Organisation,
    OrgType,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.filetypes import FileKind
from sijil_core.paperless import SERVICE_PERMISSIONS, PaperlessClient
from sijil_core.storage import ObjectRef, S3ObjectStore, StorageSettings
from sijil_core.tenancy import tenant_db_names, tenant_ref
from sijil_worker.ingest import ingest_document
from sijil_worker.runtime.base import ADMIN_USERNAME
from sijil_worker.settings import WorkerSettings
from tests.support.workspaces import mark_closing

T1_URL = os.environ.get("SIJIL_IT_T1_URL", "http://paperless-t1:8000")
SERVICE_USER = "sijil-it-ingest"
CONSUME_TIMEOUT_S = 180


_TOKEN: list[str] = []


class Harness:
    def __init__(self) -> None:
        self.settings = WorkerSettings()
        self.keys = LocalKeyService(self.settings.master_key_bytes())
        self.engine = make_engine(self.settings.database_url)
        self.sessions = make_sessionmaker(self.engine)
        self.store = S3ObjectStore(StorageSettings())  # type: ignore[call-arg]
        self.refs: list[ObjectRef] = []
        self.workspace_ids: list[uuid.UUID] = []
        self.token = ""

    async def prepare_paperless(self) -> None:
        if _TOKEN:  # Paperless throttles /api/token/: ask once per run
            self.token = _TOKEN[0]
            return
        admin = (ADMIN_USERNAME, os.environ["SIJIL_IT_T1_ADMIN_PASSWORD"])
        async with PaperlessClient(T1_URL, basic=admin) as client:
            password = secrets.token_urlsafe(24)
            await client.ensure_user(SERVICE_USER, password, SERVICE_PERMISSIONS)
            self.token = await client.obtain_token(SERVICE_USER, password)
        _TOKEN.append(self.token)

    async def forget_in_paperless(self, data: bytes) -> None:
        """Trash earlier copies (admin may delete; the service user may not)."""
        sha = hashlib.sha256(data).hexdigest()
        admin = (ADMIN_USERNAME, os.environ["SIJIL_IT_T1_ADMIN_PASSWORD"])
        async with PaperlessClient(T1_URL, basic=admin) as client:
            while (doc_id := await client.find_document_by_checksum(sha)) is not None:
                await client.delete_document(doc_id)

    async def workspace(self) -> Workspace:
        async with self.sessions() as db:
            org = Organisation(
                name="IT Ingest", type=OrgType.business, country="MA", region="ma-cas"
            )
            db.add(org)
            await db.flush()
            workspace_id = uuid.uuid4()
            self.workspace_ids.append(workspace_id)  # marked closing by close()
            db_name, index_db_name = tenant_db_names(tenant_ref(workspace_id))
            workspace = Workspace(
                id=workspace_id,
                org_id=org.id,
                name="IT Ingest",
                slug=f"it-ingest-{uuid.uuid4().hex[:8]}",
                state=WorkspaceState.active,
                db_name=db_name,
                index_db_name=index_db_name,
                kms_key_id=f"ws-{workspace_id}",
                paperless_url=T1_URL,
            )
            db.add(workspace)
            await db.flush()
            envelope = seal(
                self.token.encode(),
                self.keys,
                workspace.kms_key_id,
                f"{workspace_id}:{CredentialKind.service_token}",
            )
            db.add(
                TenantCredential(
                    workspace_id=workspace_id,
                    kind=CredentialKind.service_token,
                    envelope=envelope.to_bytes(),
                )
            )
            await db.commit()
            return workspace

    async def ledger(self, workspace: Workspace, data: bytes, kind: FileKind) -> DocumentLedger:
        sha = hashlib.sha256(data).hexdigest()
        path = Path(os.environ.get("TMPDIR", "/tmp")) / f"it-ingest-{sha}"  # noqa: S108
        path.write_bytes(data)
        try:
            retain = utcnow().date() + timedelta(days=3650)
            ref = await self.store.put_original(f"{tenant_ref(workspace.id)}/{sha}", path, retain)
        finally:
            path.unlink(missing_ok=True)
        self.refs.append(ref)
        async with self.sessions() as db:
            row = DocumentLedger(
                workspace_id=workspace.id,
                sha256=sha,
                md5=hashlib.md5(data, usedforsecurity=False).hexdigest(),
                size_bytes=len(data),
                content_kind=kind.value,
                source=DocumentSource.upload,
                object_key=ref.key,
                object_version_id=ref.version_id,
                retention_until=retain,
            )
            db.add(row)
            await db.commit()
            return row

    async def ingest(self, row: DocumentLedger) -> DocumentLedger:
        await ingest_document(
            row.id,
            sessions=self.sessions,
            keys=self.keys,
            store=self.store,
            paperless=PaperlessClient,
            poll_timeout_s=CONSUME_TIMEOUT_S,
            settings=self.settings,
        )
        return await self.reload(row)

    async def reload(self, row: DocumentLedger) -> DocumentLedger:
        async with self.sessions() as db:
            fresh = await db.get(DocumentLedger, row.id)
            assert fresh is not None
            return fresh

    async def close(self) -> None:
        try:
            for ref in self.refs:
                await self.store.remove_original(ref, governance=True)
        finally:
            try:
                await mark_closing(self.workspace_ids, database_url=self.settings.database_url)
            finally:
                await self.engine.dispose()
