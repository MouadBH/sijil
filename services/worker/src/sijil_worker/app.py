from typing import Any

from celery import Celery
from celery.signals import worker_ready
from kombu import Queue

from sijil_worker.settings import WorkerSettings

RECONCILE_INTERVAL_S = 300
REQUEUE_INGEST_INTERVAL_S = 300
QUEUES = ("default", "provisioning", "ingest", "extraction")


def make_celery(settings: WorkerSettings) -> Celery:
    app = Celery("sijil", include=["sijil_worker.tasks"])
    app.conf.update(
        broker_url=str(settings.redis_url),
        result_backend=str(settings.redis_url),
        task_queues=[Queue(name) for name in QUEUES],
        task_default_queue="default",
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        worker_prefetch_multiplier=1,
        sijil_region=settings.region,
        beat_schedule={
            "reconcile-tenants": {
                "task": "sijil.reconcile_tenants",
                "schedule": RECONCILE_INTERVAL_S,
            },
            "requeue-stuck-ingest": {
                "task": "sijil.requeue_stuck_ingest",
                "schedule": REQUEUE_INGEST_INTERVAL_S,
            },
        },
    )

    @worker_ready.connect(weak=False)
    def _reconcile_on_start(**_: Any) -> None:
        app.send_task("sijil.reconcile_tenants")

    return app
