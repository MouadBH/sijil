"""Celery entrypoint: `celery -A sijil_worker.main worker -Q default,provisioning,ingest,extraction`."""

from sijil_worker.app import make_celery
from sijil_worker.settings import WorkerSettings

celery_app = make_celery(WorkerSettings())
