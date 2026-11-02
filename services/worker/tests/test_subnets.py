"""Tenant subnet allocation from the dedicated pool."""

import uuid
from ipaddress import IPv4Network
from typing import Any

import pytest
from docker.errors import APIError

from sijil_core.tenancy import tenant_db_names, tenant_ref
from sijil_worker.runtime.base import TenantSpec
from sijil_worker.runtime.docker import (
    DockerRuntime,
    SubnetPoolExhausted,
    _existing_subnets,
    allocate_subnet,
)
from sijil_worker.settings import WorkerSettings

POOL = IPv4Network("10.240.0.0/16")


def test_allocate_first_free_28() -> None:
    assert allocate_subnet([], POOL) == IPv4Network("10.240.0.0/28")
    taken = [IPv4Network("10.240.0.0/28"), IPv4Network("10.240.0.16/28")]
    assert allocate_subnet(taken, POOL) == IPv4Network("10.240.0.32/28")


def test_allocate_skips_overlapping_networks() -> None:
    existing = [IPv4Network("10.240.0.0/24"), IPv4Network("172.18.0.0/16")]
    assert allocate_subnet(existing, POOL) == IPv4Network("10.240.1.0/28")


def test_pool_exhausted_raises() -> None:
    pool = IPv4Network("10.240.0.0/27")
    taken = [IPv4Network("10.240.0.0/28"), IPv4Network("10.240.0.16/28")]
    with pytest.raises(SubnetPoolExhausted):
        allocate_subnet(taken, pool)
    assert issubclass(SubnetPoolExhausted, RuntimeError)


# DockerRuntime network creation, against a fake Docker client ---------------------------------


class _FakeNet:
    def __init__(self, name: str, configs: list[dict[str, str]] | None) -> None:
        self.name = name
        self.attrs: dict[str, Any] = {"IPAM": {"Config": configs} if configs is not None else {}}


class _FakeNetworks:
    def __init__(self, existing: list[_FakeNet], create_errors: list[APIError]) -> None:
        self.existing = existing
        self.errors = list(create_errors)
        self.created: list[dict[str, Any]] = []
        self.got: list[str] = []

    def list(self, **_: object) -> list[_FakeNet]:
        return self.existing

    def create(self, name: str, **kwargs: Any) -> str:
        self.created.append({"name": name, **kwargs})
        if self.errors:
            raise self.errors.pop(0)
        return "created"

    def get(self, name: str) -> str:
        self.got.append(name)
        return "existing"


class _FakeClient:
    def __init__(self, networks: _FakeNetworks) -> None:
        self.networks = networks


def _settings() -> WorkerSettings:
    return WorkerSettings(
        region="ma-cas",
        environment="test",
        redis_url="redis://127.0.0.1:1/0",
        master_key="a" * 43 + "=",
        database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
        provisioner_database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
        tenant_image="img",
        compose_project="sijil",
        tessdata_volume="sijil_tessdata",
    )


def _runtime(networks: _FakeNetworks) -> DockerRuntime:
    return DockerRuntime(_FakeClient(networks), _settings())  # type: ignore[arg-type]


def _spec() -> TenantSpec:
    wid = uuid.uuid4()
    ref = tenant_ref(wid)
    db, idx = tenant_db_names(ref)
    return TenantSpec(
        workspace_id=wid,
        ref=ref,
        db_name=db,
        index_db_name=idx,
        db_password="p",
        index_db_password="p",
        secret_key="k",
        admin_password="a",
    )


def _overlap() -> APIError:
    return APIError("x", explanation="Pool overlaps with other one on this address space")


def test_existing_subnets_skips_ipv6_and_missing() -> None:
    nets = _FakeNetworks(
        [
            _FakeNet("a", [{"Subnet": "fd00::/64"}, {"Gateway": "1.1.1.1"}]),
            _FakeNet("b", None),
            _FakeNet("c", [{"Subnet": "172.18.0.0/16"}]),
        ],
        [],
    )
    assert _existing_subnets(_FakeClient(nets)) == [IPv4Network("172.18.0.0/16")]  # type: ignore[arg-type]


def test_non_pool_network_does_not_block_allocation() -> None:
    nets = _FakeNetworks([_FakeNet("c", [{"Subnet": "172.18.0.0/16"}])], [])
    _runtime(nets)._create_network(_spec(), "sijil_tenant_x")
    assert nets.created[0]["ipam"]["Config"][0]["Subnet"] == "10.240.0.0/28"
    assert nets.created[0]["labels"]["sijil.project"] == "sijil"


def test_create_network_retries_after_overlap() -> None:
    nets = _FakeNetworks([], [_overlap()])
    assert _runtime(nets)._create_network(_spec(), "n") == "created"
    assert len(nets.created) == 2


def test_create_network_same_name_conflict_is_existing() -> None:
    nets = _FakeNetworks([], [APIError("x", explanation="network with name n already exists")])
    assert _runtime(nets)._create_network(_spec(), "n") == "existing"
    assert nets.got == ["n"]


def test_create_network_third_overlap_raises() -> None:
    nets = _FakeNetworks([], [_overlap(), _overlap(), _overlap()])
    with pytest.raises(APIError):
        _runtime(nets)._create_network(_spec(), "n")
    assert len(nets.created) == 3


def test_allocation_lock_is_process_wide() -> None:
    first, second = _runtime(_FakeNetworks([], [])), _runtime(_FakeNetworks([], []))
    assert first._network_lock is second._network_lock
