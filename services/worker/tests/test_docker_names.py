"""Every Docker resource name for a tenant fits DNS and Docker limits, whatever the org name."""

import re
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from sijil_core.tenancy import tenant_db_names, tenant_ref
from sijil_worker.runtime.base import TenantSpec
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import WorkerSettings

ENV_EXAMPLE = Path(__file__).parents[3] / "infra" / "compose" / ".env.example"
_HOSTNAME = re.compile(r"^(?=.{1,63}$)[a-z0-9]([a-z0-9-]*[a-z0-9])?$")
_DOCKER_NAME = re.compile(r"^(?=.{1,63}$)[a-z0-9][a-z0-9_.-]*$")
_IDENTIFIER = re.compile(r"^[a-z0-9_]{1,63}$")


def _compose_project() -> str:
    for line in ENV_EXAMPLE.read_text().splitlines():
        if line.startswith("COMPOSE_PROJECT_NAME="):
            return line.partition("=")[2].strip()
    raise AssertionError("COMPOSE_PROJECT_NAME missing from .env.example")


def _runtime(project: str) -> tuple[DockerRuntime, MagicMock]:
    client = MagicMock()
    settings = WorkerSettings.model_construct(compose_project=project, tessdata_volume="tess")
    return DockerRuntime(client, settings), client


def _spec(ref: str) -> TenantSpec:
    db_name, index_db_name = tenant_db_names(ref)
    return TenantSpec(
        workspace_id=uuid.uuid4(),
        ref=ref,
        db_name=db_name,
        index_db_name=index_db_name,
        db_password="p",
        index_db_password="p",
        secret_key="s",
        admin_password="a",
    )


def test_every_derived_name_fits() -> None:
    runtime, _ = _runtime(_compose_project())
    ref = tenant_ref(uuid.uuid4())
    for hostname in (runtime.paperless_name(ref), runtime.redis_name(ref)):
        assert _HOSTNAME.fullmatch(hostname), hostname
    for name in (
        runtime.network_name(ref),
        runtime.volume_name(ref, "data"),
        runtime.volume_name(ref, "media"),
    ):
        assert _DOCKER_NAME.fullmatch(name), name
    for identifier in tenant_db_names(ref):
        assert _IDENTIFIER.fullmatch(identifier), identifier


def test_names_carry_no_org_name() -> None:
    runtime, _ = _runtime("sijil")
    with pytest.raises(ValueError, match="invalid tenant ref"):
        runtime.paperless_name("societe-marocaine-de-distribution-et-de-commerce-sarl-ab12cd")


def _container(role: str, running: bool) -> MagicMock:
    container = MagicMock()
    container.labels = {"sijil.role": role}
    container.status = "running" if running else "exited"
    return container


async def test_stop_stops_only_the_tenant_containers_and_keeps_everything_else() -> None:
    """Hibernation (ADM-5): the tenant's containers stop; nothing is removed, so its volumes,
    network and databases stay for `start`."""
    runtime, client = _runtime("sijil")
    ref = tenant_ref(uuid.uuid4())
    redis, paperless = _container("redis", True), _container("paperless", True)
    client.containers.list.return_value = [paperless, redis]
    await runtime.stop(ref)
    client.containers.list.assert_called_once_with(
        all=True, filters={"label": f"sijil.tenant={ref}"}
    )
    paperless.stop.assert_called_once()
    redis.stop.assert_called_once()
    for container in (redis, paperless):
        container.remove.assert_not_called()
    client.networks.assert_not_called()
    client.volumes.assert_not_called()


async def test_start_starts_redis_before_paperless_and_skips_running_ones() -> None:
    runtime, client = _runtime("sijil")
    ref = tenant_ref(uuid.uuid4())
    order: list[str] = []
    redis, paperless = _container("redis", False), _container("paperless", False)
    redis.start.side_effect = lambda: order.append("redis")
    paperless.start.side_effect = lambda: order.append("paperless")
    client.containers.list.return_value = [paperless, redis]
    await runtime.start(ref)
    assert order == ["redis", "paperless"]
    running = _container("paperless", True)
    client.containers.list.return_value = [running]
    await runtime.start(ref)
    running.start.assert_not_called()


async def test_start_of_an_absent_tenant_is_refused() -> None:
    runtime, client = _runtime("sijil")
    client.containers.list.return_value = []
    with pytest.raises(LookupError):
        await runtime.start(tenant_ref(uuid.uuid4()))


async def test_stop_and_start_refuse_a_bad_ref_before_any_call() -> None:
    runtime, client = _runtime("sijil")
    for call in (runtime.stop, runtime.start):
        with pytest.raises(ValueError, match="invalid tenant ref"):
            await call("not-a-ref")
    assert client.mock_calls == []


async def test_overlong_hostname_refused_before_creating_anything() -> None:
    runtime, client = _runtime("p" * 40)
    with pytest.raises(ValueError, match="hostname"):
        await runtime.ensure_running(_spec(tenant_ref(uuid.uuid4())))
    assert client.mock_calls == []
