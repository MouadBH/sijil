"""Performance budget: a 10 MB upload is accepted (202) in under 2 s at p95 on the dev stack.

Runs in the compose `tests` runner (`make itest`) against the real control plane and RustFS.
The workspace is written straight into the database so no tenant gets provisioned: the upload
path never touches Paperless. The locked test originals are removed afterwards (they are
unsealed, so the governance bypass is allowed); ledger rows cannot be deleted and stay behind
in the throw-away workspace.
"""

import os
import time
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
from sqlalchemy import select

from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    DocumentLedger,
    Membership,
    Organisation,
    OrgType,
    Role,
    Workspace,
    WorkspaceState,
)
from sijil_core.storage import ObjectRef, S3ObjectStore, StorageSettings
from sijil_core.tenancy import tenant_db_names, tenant_ref
from sijil_worker.settings import WorkerSettings

pytestmark = pytest.mark.integration

CP_URL = os.environ.get("SIJIL_IT_CP_URL", "http://control-plane:8000")
PASSWORD = "it-upload-Passw0rd"
UPLOADS = 20
SIZE = 10 * 1000 * 1000
BUDGET_S = 2.0


@pytest.fixture
async def http() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=CP_URL, timeout=30) as client:
        yield client


async def _member_workspace(http: httpx.AsyncClient) -> uuid.UUID:
    """Sign up over HTTP, then attach the user to a fresh workspace written in the DB."""
    (await http.get("/v1/auth/csrf")).raise_for_status()
    email = f"it-upload-{uuid.uuid4().hex[:10]}@example.com"
    r = await http.post(
        "/v1/auth/signup",
        json={"email": email, "password": PASSWORD, "name": "IT"},
        headers={"X-CSRF-Token": http.cookies["sijil_csrf"]},
    )
    assert r.status_code == 201, r.text
    user_id = uuid.UUID(r.json()["user"]["id"])

    settings = WorkerSettings()
    engine = make_engine(settings.database_url)
    try:
        async with make_sessionmaker(engine)() as db:
            org = Organisation(
                name="IT Upload", type=OrgType.business, country="MA", region="ma-cas"
            )
            db.add(org)
            await db.flush()
            workspace_id = uuid.uuid4()
            db_name, index_db_name = tenant_db_names(tenant_ref(workspace_id))
            db.add(
                Workspace(
                    id=workspace_id,
                    org_id=org.id,
                    name="IT Upload",
                    slug=f"it-upload-{uuid.uuid4().hex[:8]}",
                    state=WorkspaceState.active,
                    db_name=db_name,
                    index_db_name=index_db_name,
                    kms_key_id=f"ws-{workspace_id}",
                )
            )
            await db.flush()
            db.add(Membership(user_id=user_id, workspace_id=workspace_id, role=Role.member))
            await db.commit()
    finally:
        await engine.dispose()
    return workspace_id


async def _remove_originals(workspace_id: uuid.UUID) -> None:
    settings = WorkerSettings()
    engine = make_engine(settings.database_url)
    store = S3ObjectStore(StorageSettings())  # type: ignore[call-arg]
    try:
        async with make_sessionmaker(engine)() as db:
            rows = await db.execute(
                select(DocumentLedger.object_key, DocumentLedger.object_version_id).where(
                    DocumentLedger.workspace_id == workspace_id
                )
            )
            refs = [ObjectRef(key, version) for key, version in rows]
    finally:
        await engine.dispose()
    for ref in refs:
        await store.governance_remove(ref)


async def test_upload_p95_under_2s(http: httpx.AsyncClient) -> None:
    workspace_id = await _member_workspace(http)
    durations: list[float] = []
    try:
        for _ in range(UPLOADS):
            # Distinct bytes each time (else 409), sniffed as a PDF by the signature.
            data = b"%PDF-1.7\n" + os.urandom(SIZE - 9)
            started = time.monotonic()
            r = await http.post(
                f"/v1/workspaces/{workspace_id}/documents",
                files={"file": ("perf.pdf", data, "application/pdf")},
                headers={"X-CSRF-Token": http.cookies["sijil_csrf"]},
            )
            durations.append(time.monotonic() - started)
            assert r.status_code == 202, r.text
    finally:
        await _remove_originals(workspace_id)
    ordered = sorted(durations)
    p95 = ordered[int(UPLOADS * 0.95) - 1]  # the 19th fastest of 20
    print(
        f"upload 10 MB: p50 {ordered[UPLOADS // 2 - 1]:.3f}s p95 {p95:.3f}s max {ordered[-1]:.3f}s"
    )  # noqa: T201
    assert p95 < BUDGET_S
