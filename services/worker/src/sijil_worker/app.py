from typing import Any

from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_ready
from kombu import Queue

from sijil_worker.settings import BrokerSettings

RECONCILE_INTERVAL_S = 300
REQUEUE_INGEST_INTERVAL_S = 300
REQUEUE_EXTRACT_INTERVAL_S = 600
REQUEUE_UNLOCKED_INTERVAL_S = 300
REQUEUE_UNREMOVED_INTERVAL_S = 300
REQUEUE_UNPURGED_INTERVAL_S = 300
SWEEP_EXPORTS_INTERVAL_S = 300
SWEEP_ASKS_INTERVAL_S = 300
QUEUES = (
    "default",
    "provisioning",
    "ingest",
    "extraction",
    "embedding",
    "seal",
    "removal",
    "export",
    "ask",
)


def make_celery(settings: BrokerSettings, *, reconcile_on_start: bool = False) -> Celery:
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
        # Only crontab entries follow it; the interval entries are plain seconds.
        timezone="Africa/Casablanca",
        beat_schedule={
            "reconcile-tenants": {
                "task": "sijil.reconcile_tenants",
                "schedule": RECONCILE_INTERVAL_S,
            },
            "requeue-stuck-ingest": {
                "task": "sijil.requeue_stuck_ingest",
                "schedule": REQUEUE_INGEST_INTERVAL_S,
            },
            "requeue-unextracted": {
                "task": "sijil.requeue_unextracted",
                "schedule": REQUEUE_EXTRACT_INTERVAL_S,
            },
            "requeue-unlocked": {
                "task": "sijil.requeue_unlocked",
                "schedule": REQUEUE_UNLOCKED_INTERVAL_S,
            },
            "requeue-unremoved": {
                "task": "sijil.requeue_unremoved",
                "schedule": REQUEUE_UNREMOVED_INTERVAL_S,
            },
            "requeue-unpurged": {
                "task": "sijil.requeue_unpurged",
                "schedule": REQUEUE_UNPURGED_INTERVAL_S,
            },
            "sweep-exports": {
                "task": "sijil.sweep_exports",
                "schedule": SWEEP_EXPORTS_INTERVAL_S,
            },
            # Marks only: nothing is deleted without an Owner's approval (sijil_worker.eligibility).
            "mark-eligible-nightly": {
                "task": "sijil.mark_eligible",
                "schedule": crontab(hour=2, minute=0),
            },
            "drift-check-nightly": {
                "task": "sijil.drift_check_all",
                "schedule": crontab(hour=2, minute=30),
            },
            # Mends search rows a best-effort step missed (sijil_worker.search).
            "search-resync-daily": {
                "task": "sijil.search_resync",
                "schedule": crontab(hour=3, minute=30),
            },
            # Exports older than 7 days go; only the export worker can delete one.
            "expire-exports-daily": {
                "task": "sijil.expire_exports",
                "schedule": crontab(hour=3, minute=0),
                "options": {"queue": "export"},
            },
            # Lost questions sent again, abandoned ones failed (sijil_worker.ask), on the Ask
            # worker: it holds the master key that opens the index databases.
            "sweep-asks": {
                "task": "sijil.sweep_asks",
                "schedule": SWEEP_ASKS_INTERVAL_S,
                "options": {"queue": "ask"},
            },
            # Questions asked more than 90 days ago go (sijil_worker.ask), on the Ask worker.
            "expire-asks-daily": {
                "task": "sijil.expire_asks",
                "schedule": crontab(hour=4, minute=0),
                "options": {"queue": "ask"},
            },
        },
    )

    if reconcile_on_start:
        # The main worker only (it has the Docker socket): the extraction, seal, removal, export
        # and Ask workers never ask for a reconcile.
        @worker_ready.connect(weak=False)
        def _reconcile_on_start(**_: Any) -> None:
            app.send_task("sijil.reconcile_tenants")

    return app
