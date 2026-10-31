from collections.abc import Iterator

import pytest

from sijil_worker.app import QUEUES, make_celery
from sijil_worker.settings import WorkerSettings


@pytest.fixture
def eager_app() -> Iterator[None]:
    settings = WorkerSettings(
        region="ma-cas", environment="test", redis_url="redis://127.0.0.1:1/0"
    )
    app = make_celery(settings)
    app.conf.task_always_eager = True
    app.set_current()
    yield


def test_ping_returns_region(eager_app: None) -> None:
    from sijil_worker.tasks import ping

    assert ping.delay().get(timeout=5) == {"pong": True, "region": "ma-cas"}


def test_queues_declared() -> None:
    assert QUEUES == ("default", "provisioning", "ingest", "extraction")
