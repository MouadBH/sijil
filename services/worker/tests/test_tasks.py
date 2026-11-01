"""The provisioning task: which failures Celery retries, and how many times."""

from collections.abc import Callable, Iterator

import asyncpg
import docker
import docker.errors
import pytest
import requests
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import Job, JobKind, JobStatus, Workspace, WorkspaceState
from sijil_core.paperless import PaperlessError, PaperlessUnavailable
from sijil_core.region import RegionMismatch
from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.settings import WorkerSettings

ARGS = ("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002")


def _settings(database_url: str = "postgresql+asyncpg://x:y@127.0.0.1:1/z") -> WorkerSettings:
    return WorkerSettings(
        region="ma-cas",
        environment="test",
        redis_url="redis://127.0.0.1:1/0",
        master_key="a" * 43 + "=",
        database_url=database_url,
        provisioner_database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
        tenant_image="img:1",
        compose_project="sijil",
        tessdata_volume="sijil_tessdata",
    )


@pytest.fixture
def eager_app() -> Iterator[None]:
    app = make_celery(_settings())
    app.conf.task_always_eager = True
    app.set_current()
    yield


def _api_error(status: int) -> docker.errors.APIError:
    response = requests.Response()
    response.status_code = status
    return docker.errors.APIError("docker", response=response)


@pytest.mark.parametrize(
    ("exc", "transient"),
    [
        (PaperlessUnavailable("down"), True),
        (ConnectionRefusedError(), True),
        (TimeoutError(), True),
        (asyncpg.ConnectionFailureError("lost"), True),
        (asyncpg.CannotConnectNowError("starting"), True),
        (OperationalError("SELECT 1", {}, Exception("gone")), True),
        (_api_error(500), True),
        (_api_error(503), True),
        (_api_error(404), False),
        (_api_error(409), False),
        (PaperlessError("HTTP 400", 400), False),
        (RegionMismatch("eu-fra", "ma-cas"), False),
        (IntegrityError("INSERT", {}, Exception("dup")), False),
        (docker.errors.DockerException("no socket"), False),
        (RuntimeError("tessdata volume missing"), False),
    ],
)
def test_transient_classification(exc: Exception, transient: bool) -> None:
    assert tasks.is_transient(exc) is transient


def _recording(
    error: Exception, decisions: list[bool]
) -> Callable[[str, str, Callable[[Exception], bool]], object]:
    async def fake(job_id: str, workspace_id: str, will_retry: Callable[[Exception], bool]) -> None:
        decisions.append(will_retry(error))
        raise error

    return fake


def test_transient_error_is_retried_until_exhausted(
    eager_app: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    decisions: list[bool] = []
    monkeypatch.setattr(tasks, "_provision", _recording(PaperlessUnavailable("down"), decisions))
    result = tasks.provision_workspace_task.apply(args=ARGS)
    assert isinstance(result.result, PaperlessUnavailable)
    # The first run and three retries; only the last one is told it is final.
    assert decisions == [True, True, True, False]


def test_non_retryable_error_fails_at_once(
    eager_app: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    decisions: list[bool] = []
    monkeypatch.setattr(tasks, "_provision", _recording(RuntimeError("bad"), decisions))
    result = tasks.provision_workspace_task.apply(args=ARGS)
    assert isinstance(result.result, RuntimeError)
    assert decisions == [False]


@pytest.mark.db
async def test_docker_client_failure_fails_the_job(
    db: AsyncSession, workspace: Workspace, monkeypatch: pytest.MonkeyPatch, migrated_db: object
) -> None:
    workspace.state = WorkspaceState.provisioning
    job = Job(kind=JobKind.provision_workspace, workspace_id=workspace.id)
    db.add(job)
    await db.commit()

    def no_socket() -> docker.DockerClient:
        raise docker.errors.DockerException("Error while fetching server API version")

    app_url: str = migrated_db.app_url  # type: ignore[attr-defined]
    monkeypatch.setattr(tasks, "WorkerSettings", lambda: _settings(app_url))
    monkeypatch.setattr(docker, "from_env", no_socket)
    with pytest.raises(docker.errors.DockerException):
        await tasks._provision(str(job.id), str(workspace.id), lambda _exc: False)

    jb = await db.get(Job, job.id, populate_existing=True)
    ws = await db.get(Workspace, workspace.id, populate_existing=True)
    assert jb is not None
    assert ws is not None
    assert jb.status == JobStatus.failed
    assert jb.error_code == "provision.docker_client"
    assert ws.state == WorkspaceState.error
