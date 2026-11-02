"""Celery entrypoint of the seal worker (`-Q seal`): seal settings only.

The only runtime service holding the sealing key: no master key, no provisioner role, no
tenant or AI network (see sijil_worker.lock).
"""

from sijil_worker.app import make_celery
from sijil_worker.settings import SealSettings

celery_app = make_celery(SealSettings())
