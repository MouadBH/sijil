"""The worker's Celery tasks run in-process for `db`-marked tests, as the worker would run them.

`WorkerTasks` points the services' settings (SIJIL_* env) at the test database and Redis,
swaps the object store for the in-memory one and the Docker client for an empty one, and
records every task a task sends instead of handing it to a broker. Tasks call `asyncio.run`, so
each runs in a thread of its own (its own event loop, its own database engine).
"""

import asyncio
import base64
from dataclasses import dataclass, field
from typing import Any

import docker
import pytest
from celery import Celery
from celery.result import EagerResult

from sijil_worker import tasks
from sijil_worker.app import make_celery
from sijil_worker.settings import BrokerSettings
from tests.support.memory_store import MemoryExportStore, MemoryObjectStore

# Deterministic test-only master key (32 bytes); never contacted endpoints.
_ENV = {
    "SIJIL_REGION": "ma-cas",
    "SIJIL_ENVIRONMENT": "test",
    "SIJIL_MASTER_KEY": base64.b64encode(bytes(range(32))).decode(),
    "SIJIL_PROVISIONER_DATABASE_URL": "postgresql+asyncpg://x:y@127.0.0.1:1/z",
    "SIJIL_TENANT_IMAGE": "img:1",
    "SIJIL_COMPOSE_PROJECT": "sijil-test",
    "SIJIL_TESSDATA_VOLUME": "sijil-test_tessdata",
    "SIJIL_OBJECTSTORE_ENDPOINT": "http://127.0.0.1:1",
    "SIJIL_OBJECTSTORE_ACCESS_KEY": "test-access",
    "SIJIL_OBJECTSTORE_SECRET_KEY": "test-secret",
    "SIJIL_OBJECTSTORE_BUCKET": "sijil-originals-test",
    # Deterministic test-only export signing seed (32 bytes).
    "SIJIL_EXPORT_SIGNING_KEY": base64.b64encode(bytes(range(32, 64))).decode(),
}


@dataclass(frozen=True)
class Sent:
    name: str
    kwargs: dict[str, Any]
    queue: str | None


class _NoDocker:
    """A Docker daemon with no networks and no containers."""

    class _Listing:
        def list(self, **_: Any) -> list[Any]:
            return []

    networks = _Listing()
    containers = _Listing()

    def close(self) -> None:
        pass


@dataclass
class WorkerTasks:
    app: Celery
    sent: list[Sent] = field(default_factory=list)

    def _record(self, name: str, *_: Any, **options: Any) -> None:
        self.sent.append(Sent(name, dict(options.get("kwargs") or {}), options.get("queue")))

    async def run(self, name: str, **kwargs: Any) -> "EagerResult[Any]":
        """Runs the task `name` (eagerly, exceptions kept in the result) in a thread."""

        def apply() -> "EagerResult[Any]":
            self.app.set_current()  # Celery's current app is per thread
            return self.app.tasks[name].apply(kwargs=kwargs)

        return await asyncio.to_thread(apply)


def worker_tasks(
    monkeypatch: pytest.MonkeyPatch,
    *,
    database_url: str,
    redis_url: str,
    store: MemoryObjectStore,
    exports: MemoryExportStore | None = None,
) -> WorkerTasks:
    for name, value in (_ENV | {"SIJIL_DATABASE_URL": database_url, "SIJIL_REDIS_URL": redis_url}
                        ).items():  # fmt: skip
        monkeypatch.setenv(name, value)
    app = make_celery(BrokerSettings())
    app.conf.task_always_eager = True
    runner = WorkerTasks(app)
    monkeypatch.setattr(app, "send_task", runner._record)
    monkeypatch.setattr(tasks, "S3ObjectStore", lambda _settings: store)
    exports = exports or MemoryExportStore()
    monkeypatch.setattr(tasks, "S3ExportStore", lambda _settings: exports)
    monkeypatch.setattr(docker, "from_env", lambda **_: _NoDocker())
    return runner
