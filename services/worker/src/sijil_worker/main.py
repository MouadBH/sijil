"""Celery entrypoint, see the worker Dockerfile CMD for the queue list."""

from sijil_worker.app import make_celery
from sijil_worker.settings import WorkerSettings

celery_app = make_celery(WorkerSettings())
