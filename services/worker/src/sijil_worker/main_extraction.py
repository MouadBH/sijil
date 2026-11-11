"""Celery entrypoint of the extraction worker (`-Q extraction,embedding`): extraction settings
only."""

from sijil_worker.app import make_celery
from sijil_worker.settings import ExtractionSettings

celery_app = make_celery(ExtractionSettings())
