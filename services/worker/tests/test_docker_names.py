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


async def test_overlong_hostname_refused_before_creating_anything() -> None:
    runtime, client = _runtime("p" * 40)
    with pytest.raises(ValueError, match="hostname"):
        await runtime.ensure_running(_spec(tenant_ref(uuid.uuid4())))
    assert client.mock_calls == []
