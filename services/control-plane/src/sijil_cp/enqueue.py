"""Hands jobs to the worker. The control plane only ever *sends* tasks, by name."""

from typing import Protocol
from uuid import UUID

from celery import Celery
from fastapi import Request

from sijil_core.db.models import JobKind

_TASKS = {JobKind.provision_workspace: ("sijil.provision_workspace", "provisioning")}
INGEST_TASK = ("sijil.ingest_document", "ingest")
# The main worker: it holds the index database settings and the tenants' service tokens.
MIRROR_TASK = ("sijil.mirror_fields", "default")
# The seal worker: the only holder of the key that switches an original to COMPLIANCE.
LOCK_TASK = ("sijil.lock_sealed", "seal")
# The removal worker: the only holder of the key that can delete an original.
REMOVE_TASK = ("sijil.remove_original", "removal")


class Enqueuer(Protocol):
    def send(self, kind: JobKind, job_id: UUID, workspace_id: UUID) -> None: ...

    def send_ingest(self, ledger_id: UUID, workspace_id: UUID) -> None:
        """Push an accepted original into the workspace's Paperless (worker, queue `ingest`)."""
        ...

    def send_mirror(self, ledger_id: UUID, workspace_id: UUID) -> None:
        """Copy a sealed document's fields into its Paperless (worker, queue `default`)."""
        ...

    def send_lock(self, ledger_id: UUID, workspace_id: UUID) -> None:
        """Switch a sealed original to COMPLIANCE (seal worker, queue `seal`)."""
        ...

    def send_remove(self, ledger_id: UUID, workspace_id: UUID) -> None:
        """Delete a removed original from the store (removal worker, queue `removal`)."""
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
        self._send_document(INGEST_TASK, ledger_id, workspace_id)

    def send_mirror(self, ledger_id: UUID, workspace_id: UUID) -> None:
        self._send_document(MIRROR_TASK, ledger_id, workspace_id)

    def send_lock(self, ledger_id: UUID, workspace_id: UUID) -> None:
        self._send_document(LOCK_TASK, ledger_id, workspace_id)

    def send_remove(self, ledger_id: UUID, workspace_id: UUID) -> None:
        self._send_document(REMOVE_TASK, ledger_id, workspace_id)

    def _send_document(self, task: tuple[str, str], ledger_id: UUID, workspace_id: UUID) -> None:
        name, queue = task
        self._app.send_task(
            name,
            kwargs={"ledger_id": str(ledger_id), "workspace_id": str(workspace_id)},
            queue=queue,
        )


def get_enqueuer(request: Request) -> Enqueuer:
    enqueuer: Enqueuer = request.app.state.enqueuer
    return enqueuer
