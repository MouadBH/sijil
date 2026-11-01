import json
import uuid
from typing import Any, Literal

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sijil_core.crypto import DecryptionError, Envelope, LocalKeyService, open_envelope
from sijil_core.db.models import (
    CredentialKind,
    Job,
    JobKind,
    JobStatus,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.paperless import PaperlessClient, PaperlessUnavailable
from sijil_worker.provisioning import provision_workspace
from sijil_worker.runtime.base import ADMIN_USERNAME, TenantHandle, TenantSpec

pytestmark = pytest.mark.db

URL = "http://paperless-ws:8000"
TOKEN = "tok-" + "a" * 36


class FakeRuntime:
    def __init__(self, fail_running_once: bool = False) -> None:
        self.calls: list[str] = []
        self.specs: list[TenantSpec] = []
        self.fail_running_once = fail_running_once

    async def ensure_databases(self, spec: TenantSpec) -> None:
        self.calls.append("ensure_databases")
        self.specs.append(spec)

    async def ensure_running(self, spec: TenantSpec) -> TenantHandle:
        self.calls.append("ensure_running")
        if self.fail_running_once:
            self.fail_running_once = False
            raise RuntimeError("boom")
        return TenantHandle(url=URL)

    async def status(self, slug: str) -> Literal["absent", "starting", "running", "stopped"]:
        return "running"

    async def destroy(self, slug: str) -> None:
        self.calls.append("destroy")


@pytest.fixture
def keys() -> LocalKeyService:
    return LocalKeyService(b"k" * 32)


@pytest.fixture
def sessions(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db.bind, expire_on_commit=False)


@pytest.fixture
async def job(db: AsyncSession, workspace: Workspace) -> Job:
    workspace.state = WorkspaceState.provisioning
    row = Job(kind=JobKind.provision_workspace, workspace_id=workspace.id)
    db.add(row)
    await db.commit()
    return row


def paperless(base_url: str, **kw: Any) -> PaperlessClient:
    return PaperlessClient(base_url, **kw)


def mock_paperless(router: respx.MockRouter) -> None:
    router.get(f"{URL}/api/").respond(200, headers={"X-Api-Version": "9"})
    router.get(f"{URL}/api/users/").respond(200, json={"results": []})
    router.post(f"{URL}/api/users/").respond(201, json={"id": 7})
    router.post(f"{URL}/api/token/").respond(200, json={"token": TOKEN})


async def run(
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    runtime: FakeRuntime,
    workspace: Workspace,
    job: Job,
) -> None:
    await provision_workspace(
        workspace.id, job.id, sessions=sessions, keys=keys, runtime=runtime, paperless=paperless
    )


async def reload(db: AsyncSession, workspace: Workspace, job: Job) -> tuple[Workspace, Job]:
    ws = await db.get(Workspace, workspace.id, populate_existing=True)
    jb = await db.get(Job, job.id, populate_existing=True)
    assert ws is not None
    assert jb is not None
    return ws, jb


async def creds(db: AsyncSession, workspace_id: uuid.UUID) -> dict[CredentialKind, bytes]:
    rows = await db.scalars(
        select(TenantCredential)
        .where(TenantCredential.workspace_id == workspace_id)
        .execution_options(populate_existing=True)
    )
    return {r.kind: r.envelope for r in rows}


async def test_provision_happy_path_activates_workspace(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    runtime = FakeRuntime()
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(sessions, keys, runtime, workspace, job)
    ws, jb = await reload(db, workspace, job)
    assert ws.state == WorkspaceState.active
    assert ws.paperless_url == URL
    assert jb.status == JobStatus.succeeded
    assert runtime.calls == ["ensure_databases", "ensure_running"]
    assert set(await creds(db, workspace.id)) == set(CredentialKind)
    spec = runtime.specs[0]
    assert len(spec.secret_key) == 50
    assert spec.db_name == workspace.db_name


async def test_token_stored_encrypted(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(sessions, keys, FakeRuntime(), workspace, job)
    blob = (await creds(db, workspace.id))[CredentialKind.service_token]
    assert TOKEN.encode() not in blob
    env = Envelope.from_bytes(blob)
    ctx = f"{workspace.id}:{CredentialKind.service_token}"
    assert open_envelope(env, keys, ctx) == TOKEN.encode()
    with pytest.raises(DecryptionError):
        open_envelope(env, keys, f"{workspace.id}:{CredentialKind.admin_password}")


async def test_service_user_created_without_delete_permissions(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(sessions, keys, FakeRuntime(), workspace, job)
        posts = [
            c.request
            for c in router.calls
            if c.request.method == "POST" and c.request.url.path == "/api/users/"
        ]
    bodies = {json.loads(r.content)["username"]: json.loads(r.content) for r in posts}
    assert set(bodies) == {"sijil-service", "sijil-support"}
    for body in bodies.values():
        assert not [p for p in body["user_permissions"] if p.startswith("delete_")]
        assert body["is_superuser"] is False
    assert "add_document" in bodies["sijil-service"]["user_permissions"]
    assert "add_document" not in bodies["sijil-support"]["user_permissions"]
    assert all(r.headers["authorization"].startswith("Basic ") for r in posts)
    assert ADMIN_USERNAME == "sijil-admin"


async def test_provision_is_idempotent_on_retry(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    runtime = FakeRuntime(fail_running_once=True)
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        with pytest.raises(RuntimeError):
            await run(sessions, keys, runtime, workspace, job)
        ws, jb = await reload(db, workspace, job)
        assert jb.status == JobStatus.failed
        assert jb.error_code == "provision.ensure_running"
        assert ws.state == WorkspaceState.error
        first = await creds(db, workspace.id)
        assert CredentialKind.service_token not in first

        await run(sessions, keys, runtime, workspace, job)
    ws, jb = await reload(db, workspace, job)
    assert jb.status == JobStatus.succeeded
    assert jb.error_code is None
    assert ws.state == WorkspaceState.active
    second = await creds(db, workspace.id)
    assert {k: v for k, v in second.items() if k in first} == first
    assert len(second) == len(CredentialKind)
    assert runtime.calls.count("ensure_databases") == 2
    assert runtime.specs[0].db_password == runtime.specs[1].db_password
    assert runtime.specs[0].secret_key == runtime.specs[1].secret_key


async def test_service_token_replaced_not_duplicated(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    with respx.mock(assert_all_called=False) as router:
        mock_paperless(router)
        await run(sessions, keys, FakeRuntime(), workspace, job)
        before = (await creds(db, workspace.id))[CredentialKind.service_token]
        await run(sessions, keys, FakeRuntime(), workspace, job)
    rows = await creds(db, workspace.id)
    assert len(rows) == len(CredentialKind)
    assert rows[CredentialKind.service_token] != before


async def test_failure_marks_workspace_error(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{URL}/api/").respond(200)
        router.get(f"{URL}/api/users/").respond(200, json={"results": []})
        router.post(f"{URL}/api/users/").mock(return_value=httpx.Response(500))
        with pytest.raises(Exception, match="users"):
            await run(sessions, keys, FakeRuntime(), workspace, job)
    ws, jb = await reload(db, workspace, job)
    assert ws.state == WorkspaceState.error
    assert jb.status == JobStatus.failed
    assert jb.error_code == "provision.ensure_users"


async def test_unavailable_is_reraised_for_celery_retry(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    class Down(FakeRuntime):
        async def ensure_running(self, spec: TenantSpec) -> TenantHandle:
            raise PaperlessUnavailable("down")

    with pytest.raises(PaperlessUnavailable):
        await run(sessions, keys, Down(), workspace, job)
    _, jb = await reload(db, workspace, job)
    assert jb.error_code == "provision.ensure_running"
