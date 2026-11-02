"""The model never shares a worker pool with ingest: extraction has its own worker.

Reads the compose file as text (no YAML dependency): each service is a block indented by two
spaces under `services:`.
"""

import json
import re
from pathlib import Path

ROOT = Path(__file__).parents[3]
COMPOSE = (ROOT / "infra" / "compose" / "docker-compose.yml").read_text()
DOCKERFILE = (ROOT / "services" / "worker" / "Dockerfile").read_text()


def _block(service: str) -> str:
    match = re.search(rf"^  {re.escape(service)}:\n((?:    .*\n|\s*\n)+)", COMPOSE, re.MULTILINE)
    assert match is not None, service
    return match[1]


def _key(block: str, key: str) -> str | None:
    match = re.search(rf"^    {key}: (.*)$", block, re.MULTILINE)
    return None if match is None else match[1]


def _queues(command: list[str]) -> set[str]:
    return set(command[command.index("-Q") + 1].split(","))


def test_main_worker_does_not_consume_extraction() -> None:
    worker = _block("worker")
    assert _key(worker, "command") is None  # the Dockerfile CMD applies
    cmd = re.search(r"^CMD (\[.*\])$", DOCKERFILE, re.MULTILINE)
    assert cmd is not None
    assert _queues(json.loads(cmd[1])) == {"default", "provisioning", "ingest"}


def test_extraction_worker_consumes_only_extraction_one_at_a_time() -> None:
    block = _block("worker-extraction")
    raw = _key(block, "command")
    assert raw is not None
    command = json.loads(raw)
    assert _queues(command) == {"extraction"}
    assert command[command.index("--concurrency") + 1] == "1"
    assert _key(block, "environment") == _key(_block("worker"), "environment") == "*worker-env"
    # The database, store and broker; Ollama; and the tenants' Paperless content.
    assert _key(block, "networks") == "[core, ai, tenant_t1, tenant_t2]"
    # It provisions nothing: no Docker socket.
    assert "docker.sock" not in block
    assert "group_add" not in block
