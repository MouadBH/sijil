"""DockerRuntime against the real Docker Engine and Postgres. Run with `make itest`.

One tenant is provisioned for the module (a Paperless boot takes about a minute) and destroyed
in `finally`, whatever the tests do.
"""

import asyncio
import secrets
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass

import asyncpg
import docker
import pytest
from sqlalchemy.engine import make_url

from sijil_core.paperless import PaperlessClient
from sijil_core.tenancy import tenant_db_names, tenant_ref
from sijil_worker.runtime.base import ADMIN_USERNAME, TenantHandle, TenantSpec
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.runtime.postgres import drop_tenant_databases
from sijil_worker.settings import WorkerSettings

AttachRunner = Callable[[str], AbstractContextManager[None]]

pytestmark = pytest.mark.integration

READY_TIMEOUT_S = 240


@dataclass(frozen=True)
class Provisioned:
    spec: TenantSpec
    handle: TenantHandle


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
def tenant(docker_runtime: DockerRuntime, attach_runner: AttachRunner) -> Iterator[Provisioned]:
    spec = make_spec()
    try:
        asyncio.run(docker_runtime.ensure_databases(spec))
        handle = asyncio.run(docker_runtime.ensure_running(spec))
        with attach_runner(docker_runtime.network_name(spec.ref)):
            yield Provisioned(spec, handle)
    finally:
        asyncio.run(docker_runtime.destroy(spec.ref))


def tenant_resources(client: docker.DockerClient, ref: str) -> dict[str, set[str]]:
    flt = {"label": f"sijil.tenant={ref}"}
    return {
        "containers": {c.id for c in client.containers.list(all=True, filters=flt)},
        "networks": {n.id for n in client.networks.list(filters=flt)},
        "volumes": {v.id for v in client.volumes.list(filters=flt)},
    }


async def test_provision_creates_running_tenant(
    tenant: Provisioned, docker_runtime: DockerRuntime
) -> None:
    name = docker_runtime.paperless_name(tenant.spec.ref)
    assert tenant.handle.url == f"http://{name}:8000"
    async with PaperlessClient(
        tenant.handle.url, basic=(ADMIN_USERNAME, tenant.spec.admin_password)
    ) as admin:
        await admin.wait_ready(READY_TIMEOUT_S)
        assert await admin.api_version() >= 1
    deadline = time.monotonic() + READY_TIMEOUT_S
    status = await docker_runtime.status(tenant.spec.ref)
    while status != "running" and time.monotonic() < deadline:
        await asyncio.sleep(5)
        status = await docker_runtime.status(tenant.spec.ref)
    assert status == "running"


async def test_provisioned_tenant_is_isolated(
    tenant: Provisioned, docker_runtime: DockerRuntime, docker_client: docker.DockerClient
) -> None:
    container = docker_client.containers.get(docker_runtime.paperless_name(tenant.spec.ref))
    network = docker_client.networks.get(docker_runtime.network_name(tenant.spec.ref))
    assert network.attrs["Internal"] is True
    assert set(container.attrs["NetworkSettings"]["Networks"]) == {network.name}
    assert not container.attrs["HostConfig"]["PortBindings"]
    labels = container.attrs["Config"]["Labels"]
    assert labels["sijil.tenant"] == tenant.spec.ref
    for secret in (tenant.spec.db_password, tenant.spec.secret_key, tenant.spec.admin_password):
        assert all(secret not in value for value in labels.values())

    def curl(url: str) -> int:
        result = container.exec_run(["curl", "-s", "--max-time", "5", "-o", "/dev/null", url])
        return int(result.exit_code)

    assert curl("http://paperless-t1:8000/") != 0, "provisioned tenant reaches paperless-t1"
    assert curl("https://1.1.1.1") != 0, "provisioned tenant has a route out"
    assert curl("https://example.com") != 0, "provisioned tenant resolves the internet"


async def test_provision_is_idempotent_against_docker(
    tenant: Provisioned, docker_runtime: DockerRuntime, docker_client: docker.DockerClient
) -> None:
    before = tenant_resources(docker_client, tenant.spec.ref)
    assert len(before["containers"]) == 2
    assert len(before["networks"]) == 1
    assert len(before["volumes"]) == 2
    await docker_runtime.ensure_databases(tenant.spec)
    handle = await docker_runtime.ensure_running(tenant.spec)
    assert handle == tenant.handle
    assert tenant_resources(docker_client, tenant.spec.ref) == before


async def _connect_as(settings: WorkerSettings, user: str, password: str, database: str) -> None:
    url = make_url(settings.provisioner_database_url)
    conn = await asyncpg.connect(
        host=url.host, port=url.port, user=user, password=password, database=database
    )
    await conn.close()


async def test_provisioned_databases_are_closed_to_other_tenants(
    docker_runtime: DockerRuntime, worker_settings: WorkerSettings
) -> None:
    """PUBLIC holds nothing on a provisioned database, so no other role can even connect."""
    a = make_spec()
    b = make_spec()
    try:
        await docker_runtime.ensure_databases(a)
        await docker_runtime.ensure_databases(b)
        url = make_url(worker_settings.provisioner_database_url)
        conn = await asyncpg.connect(
            host=url.host,
            port=url.port,
            user=url.username,
            password=url.password,
            database=url.database,
        )
        try:
            for spec in (a, b):
                for db in (spec.db_name, spec.index_db_name):
                    for privilege in ("CONNECT", "TEMPORARY"):
                        granted = await conn.fetchval(
                            "SELECT has_database_privilege('public', $1, $2)", db, privilege
                        )
                        assert granted is False, f"PUBLIC has {privilege} on {db}"
        finally:
            await conn.close()

        # Each role reaches its own database...
        await _connect_as(worker_settings, a.db_name, a.db_password, a.db_name)
        await _connect_as(worker_settings, a.index_db_name, a.index_db_password, a.index_db_name)
        # ...and nothing else: not the other tenant's, not its own index, not a sample tenant's.
        for database in (b.db_name, b.index_db_name, a.index_db_name, "paperless_t1"):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await _connect_as(worker_settings, a.db_name, a.db_password, database)
    finally:
        for spec in (a, b):
            await drop_tenant_databases(
                worker_settings.provisioner_database_url, spec.db_name, spec.index_db_name
            )
