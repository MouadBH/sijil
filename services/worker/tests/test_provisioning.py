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
    Organisation,
    OrgType,
    TenantCredential,
    Workspace,
    WorkspaceState,
)
from sijil_core.paperless import PaperlessClient, PaperlessUnavailable
from sijil_core.region import RegionMismatch
from sijil_core.settings import CoreSettings
from sijil_worker.provisioning import provision_workspace
from sijil_worker.runtime.base import ADMIN_USERNAME, TenantHandle, TenantSpec

pytestmark = pytest.mark.db

URL = "http://paperless-ws:8000"
TOKEN = "tok-" + "a" * 36
SETTINGS = CoreSettings.model_construct(region="ma-cas", environment="test")


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
    *,
    will_retry: bool = False,
) -> None:
    await provision_workspace(
        workspace.id,
        job.id,
        sessions=sessions,
        keys=keys,
        runtime=runtime,
        paperless=paperless,
        settings=SETTINGS,
        will_retry=lambda _exc: will_retry,
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
        # A fresh provisioning job for the same workspace (a finished job is never re-run).
        again = Job(kind=JobKind.provision_workspace, workspace_id=workspace.id)
        db.add(again)
        await db.commit()
        await run(sessions, keys, FakeRuntime(), workspace, again)
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


async def test_failure_with_a_retry_pending_is_not_final(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    """While Celery will retry, clients must not see a final-looking `failed`."""

    class Down(FakeRuntime):
        async def ensure_running(self, spec: TenantSpec) -> TenantHandle:
            raise PaperlessUnavailable("down")

    with pytest.raises(PaperlessUnavailable):
        await run(sessions, keys, Down(), workspace, job, will_retry=True)
    ws, jb = await reload(db, workspace, job)
    assert jb.status == JobStatus.running
    assert jb.error_code == "provision.ensure_running"
    assert ws.state == WorkspaceState.provisioning

    # Retries exhausted: now it is final.
    with pytest.raises(PaperlessUnavailable):
        await run(sessions, keys, Down(), workspace, job, will_retry=False)
    ws, jb = await reload(db, workspace, job)
    assert jb.status == JobStatus.failed
    assert jb.error_code == "provision.ensure_running"
    assert ws.state == WorkspaceState.error


async def test_region_mismatch_fails_at_once_and_touches_nothing(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
) -> None:
    org = Organisation(name="Paris", type=OrgType.business, country="FR", region="eu-fra")
    db.add(org)
    await db.flush()
    workspace = Workspace(
        org_id=org.id,
        name="Paris",
        slug="paris-abc123",
        state=WorkspaceState.provisioning,
        db_name="paperless_paris",
        index_db_name="index_paris",
        kms_key_id="ws-paris",
    )
    db.add(workspace)
    await db.flush()
    job = Job(kind=JobKind.provision_workspace, workspace_id=workspace.id)
    db.add(job)
    await db.commit()

    runtime = FakeRuntime()
    with pytest.raises(RegionMismatch):
        # Even if the task would retry anything, a region mismatch is never retried.
        await run(sessions, keys, runtime, workspace, job, will_retry=True)
    ws, jb = await reload(db, workspace, job)
    assert jb.status == JobStatus.failed
    assert jb.error_code == "provision.region_mismatch"
    assert ws.state == WorkspaceState.error
    assert runtime.calls == []
    assert await creds(db, workspace.id) == {}


async def test_retry_reuses_existing_paperless_users(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    """A retry after the users were created PATCHes them: Paperless never gets duplicates."""
    users: dict[str, int] = {}

    def list_users(request: httpx.Request) -> httpx.Response:
        name = request.url.params.get("username__iexact", "")
        rows = [{"id": i, "username": u} for u, i in users.items() if u == name]
        return httpx.Response(200, json={"results": rows})

    def create_user(request: httpx.Request) -> httpx.Response:
        name = json.loads(request.content)["username"]
        assert name not in users, f"duplicate Paperless user {name}"
        users[name] = len(users) + 1
        return httpx.Response(201, json={"id": users[name]})

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{URL}/api/").respond(200, headers={"X-Api-Version": "9"})
        router.get(f"{URL}/api/users/").mock(side_effect=list_users)
        router.post(f"{URL}/api/users/").mock(side_effect=create_user)
        patch = router.patch(url__regex=rf"{URL}/api/users/\d+/").respond(200, json={})
        token = router.post(f"{URL}/api/token/")
        token.side_effect = [httpx.ReadTimeout("slow"), httpx.Response(200, json={"token": TOKEN})]

        with pytest.raises(PaperlessUnavailable):
            await run(sessions, keys, FakeRuntime(), workspace, job, will_retry=True)
        _, jb = await reload(db, workspace, job)
        assert jb.status == JobStatus.running
        assert jb.error_code == "provision.obtain_token"
        assert patch.call_count == 0

        await run(sessions, keys, FakeRuntime(), workspace, job, will_retry=True)
        posts = [
            c
            for c in router.calls
            if c.request.method == "POST" and c.request.url.path == "/api/users/"
        ]
    ws, jb = await reload(db, workspace, job)
    assert jb.status == JobStatus.succeeded
    assert jb.error_code is None
    assert ws.state == WorkspaceState.active
    assert set(users) == {"sijil-service", "sijil-support"}
    assert len(posts) == 2
    assert patch.call_count == 2


async def test_failure_never_touches_a_job_that_succeeded_meanwhile(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    """A late failure from a duplicate run must not flip a finished job or active tenant."""

    class RacedRuntime(FakeRuntime):
        async def ensure_running(self, spec: TenantSpec) -> TenantHandle:
            async with sessions() as session:
                row = await session.get(Job, job.id)
                ws_row = await session.get(Workspace, workspace.id)
                assert row is not None
                assert ws_row is not None
                row.status = JobStatus.succeeded
                ws_row.state = WorkspaceState.active
                await session.commit()
            raise RuntimeError("late failure")

    with pytest.raises(RuntimeError):
        await run(sessions, keys, RacedRuntime(), workspace, job)
    ws, jb = await reload(db, workspace, job)
    assert jb.status == JobStatus.succeeded
    assert jb.error_code is None
    assert ws.state == WorkspaceState.active


async def test_already_succeeded_job_is_a_no_op(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    """A redelivered task (acks_late) must not re-run provisioning of a finished job."""
    job.status = JobStatus.succeeded
    workspace.state = WorkspaceState.active
    await db.commit()

    class Untouchable(FakeRuntime):
        async def ensure_databases(self, spec: TenantSpec) -> None:
            raise AssertionError("runtime must not be called for a finished job")

    runtime = Untouchable()
    await run(sessions, keys, runtime, workspace, job)
    ws, jb = await reload(db, workspace, job)
    assert runtime.calls == []
    assert jb.status == JobStatus.succeeded
    assert ws.state == WorkspaceState.active
    assert await creds(db, workspace.id) == {}


async def test_failure_never_downgrades_an_active_workspace(
    db: AsyncSession,
    sessions: async_sessionmaker[AsyncSession],
    keys: LocalKeyService,
    workspace: Workspace,
    job: Job,
) -> None:
    workspace.state = WorkspaceState.active
    await db.commit()
    with pytest.raises(RuntimeError):
        await run(sessions, keys, FakeRuntime(fail_running_once=True), workspace, job)
    ws, jb = await reload(db, workspace, job)
    assert jb.status == JobStatus.failed
    assert jb.error_code == "provision.ensure_running"
    assert ws.state == WorkspaceState.active


def test_provision_task_routes_to_provisioning_queue() -> None:
    from sijil_worker.tasks import provision_workspace_task

    # celery-types does not declare the option attributes set by the decorator.
    assert provision_workspace_task.queue == "provisioning"  # type: ignore[attr-defined]
