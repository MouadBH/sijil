"""Celery entrypoint of the Ask worker (`-Q ask`): Ask settings only.

It reads and writes the workspaces' index databases (through the master key) and calls the
in-region Ollama: on the `core` and `ai` networks only, with no tenant network, no Docker
socket, no object-store key and no provisioner role (see sijil_worker.ask).
"""

from sijil_worker.app import make_celery
from sijil_worker.settings import AskSettings

celery_app = make_celery(AskSettings())
