"""Hands jobs to the worker. The control plane only ever *sends* tasks, by name."""

from typing import Protocol
from uuid import UUID

from celery import Celery
from fastapi import Request

from sijil_core.db.models import JobKind

_TASKS = {JobKind.provision_workspace: ("sijil.provision_workspace", "provisioning")}
INGEST_TASK = ("sijil.ingest_document", "ingest")


class Enqueuer(Protocol):
    def send(self, kind: JobKind, job_id: UUID, workspace_id: UUID) -> None: ...

    def send_ingest(self, ledger_id: UUID, workspace_id: UUID) -> None:
        """Push an accepted original into the workspace's Paperless (worker, queue `ingest`)."""
        ...


class CeleryEnqueuer:
    """A Celery client: no task code is imported, so no worker is needed to build it."""

    def __init__(self, redis_url: str) -> None:
        self._app = Celery("sijil-cp", broker=redis_url)

    def send(self, kind: JobKind, job_id: UUID, workspace_id: UUID) -> None:
        name, queue = _TASKS[kind]
        self._app.send_task(
            name,
            kwargs={"job_id": str(job_id), "workspace_id": str(workspace_id)},
            queue=queue,
        )

    def send_ingest(self, ledger_id: UUID, workspace_id: UUID) -> None:
        name, queue = INGEST_TASK
        self._app.send_task(
            name,
            kwargs={"ledger_id": str(ledger_id), "workspace_id": str(workspace_id)},
            queue=queue,
        )


def get_enqueuer(request: Request) -> Enqueuer:
    enqueuer: Enqueuer = request.app.state.enqueuer
    return enqueuer
