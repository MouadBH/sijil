from celery import Celery
from kombu import Queue

from sijil_worker.settings import WorkerSettings

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
    )
    return app
