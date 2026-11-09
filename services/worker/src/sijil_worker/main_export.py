"""Celery entrypoint of the export worker (`-Q export`): export settings only.

The only runtime service holding the region's signing seed and the export key: no provisioner
role, no app, seal or removal key, no tenant or AI network, no Docker socket (see
sijil_worker.export).
"""

from sijil_worker.app import make_celery
from sijil_worker.settings import ExportSettings

celery_app = make_celery(ExportSettings())
