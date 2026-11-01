"""Tenant subnet allocation from the dedicated pool."""

from ipaddress import IPv4Network

import pytest

from sijil_worker.runtime.docker import SubnetPoolExhausted, allocate_subnet

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
