"""Ingest of every allow-listed format into the real compose Paperless (`paperless-t1`).

Runs in the compose `tests` runner (`make itest`): RustFS for the originals, the control-plane
database for the ledger, and the t1 Paperless over its tenant network. A throw-away workspace
row points at t1 and carries a sealed token of a dedicated service user, so the worker code runs
exactly as in production. Documents that earlier runs left in t1 are trashed first, so every run
goes through Paperless's real consume path.

Observed with Paperless 3.4.1 (recorded in the M2 report):
- PDF, JPEG, PNG, TIFF, DOCX, XLSX and EML are consumed;
- XML is refused at upload (HTTP 400 `File type text/xml not supported`) -> unsupported;
- the sample HEIC is a bare ftyp box, not a decodable image: the consume task fails with
  `cannot identify image file` -> unsupported;
- Paperless does not refuse duplicates (it flags them), so ingest looks the SHA-256 up first.
"""

import hashlib
import os
import secrets
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest

from sijil_core.crypto import LocalKeyService, seal
from sijil_core.db.base import utcnow
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    CredentialKind,
    DocumentLedger,
    DocumentSource,
    IngestState,
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

pytestmark = pytest.mark.integration

T1_URL = os.environ.get("SIJIL_IT_T1_URL", "http://paperless-t1:8000")
FORMATS = Path(__file__).parents[2] / "fixtures" / "formats"
SERVICE_USER = "sijil-it-ingest"
CONSUME_TIMEOUT_S = 180

SAMPLES: list[tuple[str, FileKind]] = [
    ("sample.pdf", FileKind.pdf),
    ("sample.jpg", FileKind.jpeg),
    ("sample.png", FileKind.png),
    ("sample.tiff", FileKind.tiff),
    ("sample.heic", FileKind.heic),
    ("sample.docx", FileKind.docx),
    ("sample.xlsx", FileKind.xlsx),
    ("sample.xml", FileKind.xml),
    ("sample.eml", FileKind.eml),
]
MUST_CONSUME = {FileKind.pdf, FileKind.jpeg, FileKind.png, FileKind.tiff}


_TOKEN: list[str] = []


class Harness:
    def __init__(self) -> None:
        self.settings = WorkerSettings()
        self.keys = LocalKeyService(self.settings.master_key_bytes())
        self.engine = make_engine(self.settings.database_url)
        self.sessions = make_sessionmaker(self.engine)
        self.store = S3ObjectStore(StorageSettings())  # type: ignore[call-arg]
        self.refs: list[ObjectRef] = []
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
        for ref in self.refs:
            await self.store.governance_remove(ref)
        await self.engine.dispose()


@pytest.fixture
async def harness() -> AsyncIterator[Harness]:
    h = Harness()
    await h.prepare_paperless()
    try:
        yield h
    finally:
        await h.close()


@pytest.mark.parametrize(("name", "kind"), SAMPLES, ids=[name for name, _ in SAMPLES])
async def test_each_cap2_format_ingests(harness: Harness, name: str, kind: FileKind) -> None:
    data = (FORMATS / name).read_bytes()
    await harness.forget_in_paperless(data)
    row = await harness.ingest(await harness.ledger(await harness.workspace(), data, kind))
    outcome = row.ingest_state
    print(f"INGEST-OUTCOME {name} {outcome.value} {row.ingest_error_code}")
    assert outcome in {IngestState.consumed, IngestState.unsupported}
    if kind in MUST_CONSUME:
        assert outcome is IngestState.consumed
    if outcome is IngestState.consumed:
        assert row.paperless_doc_id is not None
        assert row.ingest_error_code is None
    else:
        assert row.paperless_doc_id is None
        assert row.ingest_error_code == "ingest.unsupported_format"


async def test_eml_ingests(harness: Harness) -> None:
    data = (FORMATS / "sample.eml").read_bytes()
    await harness.forget_in_paperless(data)
    row = await harness.ingest(await harness.ledger(await harness.workspace(), data, FileKind.eml))
    assert row.ingest_state is IngestState.consumed
    assert row.paperless_doc_id is not None


async def test_same_bytes_in_a_second_workspace_adopt_the_existing_document(
    harness: Harness,
) -> None:
    """Paperless 3.4.1 accepts duplicates instead of refusing them: ingest must not add one."""
    data = (FORMATS / "sample.pdf").read_bytes()
    await harness.forget_in_paperless(data)
    first = await harness.ingest(
        await harness.ledger(await harness.workspace(), data, FileKind.pdf)
    )
    second = await harness.ingest(
        await harness.ledger(await harness.workspace(), data, FileKind.pdf)
    )
    assert first.ingest_state is second.ingest_state is IngestState.consumed
    assert first.paperless_doc_id == second.paperless_doc_id
    assert second.paperless_task_id is None  # resolved by checksum, nothing was uploaded
