"""Celery entrypoint, see the worker Dockerfile CMD for the queue list."""

from sijil_worker.app import make_celery
from sijil_worker.settings import BillingSettings, WorkerSettings

# The main worker runs `sijil.billing_daily`: without the seller's settings (SIJIL_SELLER_*,
# ruling R2) it fails here, at start, not at the first invoice.
BillingSettings()
celery_app = make_celery(WorkerSettings(), reconcile_on_start=True)
