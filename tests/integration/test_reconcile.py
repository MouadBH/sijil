"""Re-attachment of compose services to tenant networks. Run with `make itest`."""

import asyncio
import secrets
import uuid
from collections.abc import Iterator
from ipaddress import IPv4Network

import docker
import pytest
from docker.models.containers import Container

from sijil_core.tenancy import tenant_db_names, tenant_ref
from sijil_worker.reconcile import reconcile_tenants
from sijil_worker.runtime.base import TenantSpec
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import WorkerSettings

pytestmark = pytest.mark.integration


def make_spec() -> TenantSpec:
    workspace_id = uuid.uuid4()
    ref = tenant_ref(workspace_id)
    db_name, index_db_name = tenant_db_names(ref)
    return TenantSpec(
        workspace_id=workspace_id,
        ref=ref,
        db_name=db_name,
        index_db_name=index_db_name,
        db_password=secrets.token_urlsafe(24),
        index_db_password=secrets.token_urlsafe(24),
        secret_key=secrets.token_urlsafe(48)[:50],
        admin_password=secrets.token_urlsafe(24),
    )


@pytest.fixture(scope="module")
def tenant_network(docker_runtime: DockerRuntime) -> Iterator[str]:
    spec = make_spec()
    try:
        asyncio.run(docker_runtime.ensure_databases(spec))
        asyncio.run(docker_runtime.ensure_running(spec))
        yield docker_runtime.network_name(spec.ref)
    finally:
        asyncio.run(docker_runtime.destroy(spec.ref))


def _control_plane(client: docker.DockerClient, settings: WorkerSettings) -> Container:
    return client.containers.list(
        filters={
            "label": [
                f"com.docker.compose.project={settings.compose_project}",
                "com.docker.compose.service=control-plane",
            ]
        }
    )[0]


def _members(client: docker.DockerClient, name: str) -> set[str]:
    network = client.networks.get(name)
    network.reload()
    return {c.id for c in network.containers}


def test_reconcile_reattaches_disconnected_control_plane(
    docker_client: docker.DockerClient, worker_settings: WorkerSettings, tenant_network: str
) -> None:
    cp = _control_plane(docker_client, worker_settings)
    docker_client.networks.get(tenant_network).disconnect(cp, force=True)
    assert cp.id not in _members(docker_client, tenant_network)
    fixed = reconcile_tenants(docker_client, worker_settings)
    assert f"control-plane@{tenant_network}" in fixed
    assert cp.id in _members(docker_client, tenant_network)
    cp.reload()
    aliases = cp.attrs["NetworkSettings"]["Networks"][tenant_network]["Aliases"] or []
    assert "control-plane" in aliases


def test_reconcile_is_idempotent(
    docker_client: docker.DockerClient, worker_settings: WorkerSettings, tenant_network: str
) -> None:
    reconcile_tenants(docker_client, worker_settings)
    assert reconcile_tenants(docker_client, worker_settings) == []


def test_provisioned_network_uses_pool_subnet(
    docker_client: docker.DockerClient, worker_settings: WorkerSettings, tenant_network: str
) -> None:
    network = docker_client.networks.get(tenant_network)
    configs = network.attrs["IPAM"]["Config"]
    assert len(configs) == 1
    subnet = IPv4Network(configs[0]["Subnet"])
    assert subnet.prefixlen == 28
    assert subnet.subnet_of(worker_settings.tenant_subnet_pool)
    assert str(worker_settings.tenant_subnet_pool) == "10.240.0.0/16"
    assert network.attrs["Labels"]["sijil.project"] == worker_settings.compose_project
