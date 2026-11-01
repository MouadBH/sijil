"""Fixtures for tests that drive the real Docker Engine from the compose `tests` runner.

Provisioned tenants sit on their own internal network, which the runner is not on. Tests attach
the runner container itself to that network (and detach it afterwards) so they can reach the
tenant like the control plane does. DockerRuntime's own attachment list stays production-shaped.
"""

import contextlib
import socket
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager

import docker
import pytest
from docker.errors import APIError, NotFound

from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import WorkerSettings

AttachRunner = Callable[[str], AbstractContextManager[None]]


@pytest.fixture(scope="session")
def docker_client() -> Iterator[docker.DockerClient]:
    client = docker.from_env()
    yield client
    client.close()


@pytest.fixture(scope="session")
def worker_settings() -> WorkerSettings:
    # The `tests` service carries the worker's environment (see docker-compose.yml).
    return WorkerSettings()


@pytest.fixture(scope="session")
def docker_runtime(
    docker_client: docker.DockerClient, worker_settings: WorkerSettings
) -> DockerRuntime:
    return DockerRuntime(docker_client, worker_settings)


@pytest.fixture(scope="session")
def attach_runner(docker_client: docker.DockerClient) -> AttachRunner:
    @contextlib.contextmanager
    def attach(network_name: str) -> Iterator[None]:
        # In a container, the hostname is the container id prefix.
        me = docker_client.containers.get(socket.gethostname())
        network = docker_client.networks.get(network_name)
        network.connect(me)
        try:
            yield
        finally:
            with contextlib.suppress(NotFound, APIError):
                network.disconnect(me, force=True)

    return attach
