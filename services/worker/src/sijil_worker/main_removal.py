"""Celery entrypoint of the removal worker (`-Q removal`): removal settings only.

The only runtime service holding the removal key: no master key, no provisioner role, no app or
seal key, no tenant or AI network (see sijil_worker.removal).
"""

from sijil_worker.app import make_celery
from sijil_worker.settings import RemovalSettings

celery_app = make_celery(RemovalSettings())
