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


def test_ollama_is_capped_and_fails_fast_when_busy() -> None:
    """Six CPUs at most (of the dev host's eight: tenants' OCR keeps headroom), one generation
    at a time, and a short queue: excess requests get 503 (transient) instead of piling up."""
    block = _block("ollama")
    assert _key(block, "cpus") == '"6"'
    # A quarter of the default CPU weight: under contention the tenants' OCR comes first.
    assert _key(block, "cpu_shares") == "256"
    # The model sizes its thread pool from the host's CPUs (eight), not the quota: eight threads
    # under six CPUs were throttled to 0.4 tokens/s (measured), so every client asks for six.
    text = COMPOSE
    for anchor in ("x-worker-env", "x-extraction-env"):
        env = text.split(f"{anchor}: &", 1)[1].split("\n\n", 1)[0]
        assert re.search(r'^  SIJIL_OLLAMA_NUM_THREAD: "6"$', env, re.MULTILINE), anchor
    assert re.search(r'^      OLLAMA_NUM_PARALLEL: "1"$', block, re.MULTILINE)
    assert re.search(r'^      OLLAMA_MAX_QUEUE: "8"$', block, re.MULTILINE)


def _anchor(name: str) -> str:
    match = re.search(rf"^x-{name}: &{name}\n((?:  .*\n)+)", COMPOSE, re.MULTILINE)
    assert match is not None, name
    return match[1]


def test_extraction_worker_env_is_least_privilege() -> None:
    """No provisioner role, no Docker or tenant-provisioning settings; only what extraction and
    routing use."""
    env = _anchor("extraction-env")
    for absent in (
        "SIJIL_PROVISIONER_DATABASE_URL",
        "SIJIL_TENANT_IMAGE",
        "SIJIL_TESSDATA_VOLUME",
        "SIJIL_TENANT_SUBNET_POOL",
        "SIJIL_COMPOSE_PROJECT",
        "OBJECTSTORE_ACCESS_KEY:",  # the root key
    ):
        assert absent not in env, absent
    assert "<<: [*sijil-env, *objectstore-env]" in env  # region, environment, broker, app key
    for present in (
        "SIJIL_MASTER_KEY",
        "SIJIL_DATABASE_URL: postgresql+asyncpg://sijil_cp_app:",
        "SIJIL_OLLAMA_URL",
        "SIJIL_AI_ALLOWED_HOSTS",
        "SIJIL_EXTRACTION_MODEL",
    ):
        assert present in env, present
    assert "SIJIL_PROVISIONER_DATABASE_URL" in _anchor("worker-env")


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
    assert _key(block, "environment") == "*extraction-env"
    assert "sijil_worker.main_extraction" in command
    # The database, store and broker; Ollama; and the tenants' Paperless content.
    assert _key(block, "networks") == "[core, ai, tenant_t1, tenant_t2]"
    # It provisions nothing: no Docker socket.
    assert "docker.sock" not in block
    assert "group_add" not in block


def test_seal_worker_consumes_only_seal_off_the_internet_with_no_socket() -> None:
    block = _block("worker-seal")
    raw = _key(block, "command")
    assert raw is not None
    command = json.loads(raw)
    assert _queues(command) == {"seal"}
    assert command[command.index("--concurrency") + 1] == "2"
    assert "sijil_worker.main_seal" in command
    assert _key(block, "environment") == "*seal-env"
    assert _key(block, "networks") == "[core]"  # no tenant, AI or published port
    assert "ports" not in block
    assert "docker.sock" not in block
    assert "group_add" not in block


def test_only_the_seal_worker_holds_the_seal_key() -> None:
    """The product owner's ruling: the internet-facing control plane never holds the key that
    can switch a lock; only `worker-seal` (and `migrate` and the test runner, through the
    root-key anchor) do."""
    env = _anchor("seal-env")
    assert "SIJIL_OBJECTSTORE_ACCESS_KEY: ${SIJIL_OBJECTSTORE_SEAL_ACCESS_KEY:?}" in env
    assert "SIJIL_OBJECTSTORE_SECRET_KEY: ${SIJIL_OBJECTSTORE_SEAL_SECRET_KEY:?}" in env
    for absent in ("SIJIL_PROVISIONER_DATABASE_URL", "SIJIL_MASTER_KEY", "*objectstore-env"):
        assert absent not in env, absent
    assert "SIJIL_DATABASE_URL: postgresql+asyncpg://sijil_cp_app:" in env
    assert "SIJIL_OBJECTSTORE_SEAL" not in _block("control-plane")
    for anchor in ("worker-env", "extraction-env", "sijil-env", "objectstore-env"):
        assert "SIJIL_OBJECTSTORE_SEAL" not in _anchor(anchor), anchor
    for service in ("worker", "worker-extraction", "smtp", "beat"):
        assert "SIJIL_OBJECTSTORE_SEAL" not in _block(service), service


def test_only_the_main_worker_reconciles_on_start() -> None:
    """`sijil.reconcile_tenants` needs the Docker socket: only the main worker sends it when it
    starts, never the extraction or the seal worker."""
    from celery.signals import worker_ready
    from pydantic import RedisDsn

    from sijil_worker.app import make_celery
    from sijil_worker.settings import BrokerSettings

    settings = BrokerSettings.model_construct(
        region="ma-cas", environment="test", redis_url=RedisDsn("redis://127.0.0.1:1/0")
    )
    before = len(worker_ready.receivers)
    make_celery(settings)
    assert len(worker_ready.receivers) == before
    make_celery(settings, reconcile_on_start=True)
    assert len(worker_ready.receivers) == before + 1
    src = ROOT / "services" / "worker" / "src" / "sijil_worker"
    assert "reconcile_on_start=True" in (src / "main.py").read_text()
    for entry in ("main_extraction.py", "main_seal.py", "main_removal.py"):
        assert "reconcile_on_start" not in (src / entry).read_text(), entry


def test_removal_worker_consumes_only_removal_off_the_internet_with_no_socket() -> None:
    block = _block("worker-removal")
    raw = _key(block, "command")
    assert raw is not None
    command = json.loads(raw)
    assert command[:5] == ["celery", "-A", "sijil_worker.main_removal", "worker", "-Q"]
    assert _queues(command) == {"removal"}
    assert command[command.index("--concurrency") + 1] == "1"
    assert command[command.index("-n") + 1] == "removal@%h"
    assert _key(block, "environment") == "*removal-env"
    assert _key(block, "networks") == "[core]"  # no tenant, AI or published port
    assert "ports" not in block
    assert "docker.sock" not in block
    assert "group_add" not in block


def test_only_the_removal_worker_holds_the_removal_key() -> None:
    """The product owner's ruling (2026-11-03): one single-purpose worker holds the key that
    can delete an original; `migrate` and the test runner see it through the root-key anchor."""
    env = _anchor("removal-env")
    assert "SIJIL_OBJECTSTORE_ACCESS_KEY: ${SIJIL_OBJECTSTORE_REMOVAL_ACCESS_KEY:?}" in env
    assert "SIJIL_OBJECTSTORE_SECRET_KEY: ${SIJIL_OBJECTSTORE_REMOVAL_SECRET_KEY:?}" in env
    assert "SIJIL_DATABASE_URL: postgresql+asyncpg://sijil_cp_app:" in env
    for absent in (
        "SIJIL_PROVISIONER_DATABASE_URL",
        "SIJIL_MASTER_KEY",
        "*objectstore-env",  # the app key
        "SIJIL_OBJECTSTORE_APP_",
        "SEAL_",
    ):
        assert absent not in env, absent
    assert "SIJIL_OBJECTSTORE_REMOVAL_ACCESS_KEY" in _anchor("objectstore-root-env")
    for anchor in ("worker-env", "extraction-env", "seal-env", "sijil-env", "objectstore-env"):
        assert "SIJIL_OBJECTSTORE_REMOVAL" not in _anchor(anchor), anchor
    for service in ("control-plane", "worker", "worker-extraction", "worker-seal", "smtp", "beat"):
        assert "SIJIL_OBJECTSTORE_REMOVAL" not in _block(service), service


def test_export_worker_consumes_only_export_off_the_internet_with_no_socket() -> None:
    """The product owner's ruling (2026-11-03): an export-only worker, on `core` only, with no
    Docker socket and no tenant network."""
    block = _block("worker-export")
    raw = _key(block, "command")
    assert raw is not None
    command = json.loads(raw)
    assert command[:5] == ["celery", "-A", "sijil_worker.main_export", "worker", "-Q"]
    assert _queues(command) == {"export"}
    assert command[command.index("--concurrency") + 1] == "1"
    assert command[command.index("-n") + 1] == "export@%h"
    assert _key(block, "environment") == "*export-env"
    assert _key(block, "networks") == "[core]"
    assert _key(block, "ports") is None  # "exports" holds the substring
    assert "docker.sock" not in block
    assert "group_add" not in block
    src = ROOT / "services" / "worker" / "src" / "sijil_worker"
    assert "reconcile_on_start" not in (src / "main_export.py").read_text()


def test_only_the_export_worker_holds_the_signing_seed_and_export_key() -> None:
    env = _anchor("export-env")
    assert "SIJIL_OBJECTSTORE_ACCESS_KEY: ${SIJIL_OBJECTSTORE_EXPORT_ACCESS_KEY:?}" in env
    assert "SIJIL_OBJECTSTORE_SECRET_KEY: ${SIJIL_OBJECTSTORE_EXPORT_SECRET_KEY:?}" in env
    assert "SIJIL_EXPORT_SIGNING_KEY: ${SIJIL_EXPORT_SIGNING_KEY:?}" in env
    # The index databases are opened like the control plane does: through the master key.
    assert "SIJIL_MASTER_KEY: ${SIJIL_MASTER_KEY:?}" in env
    assert "SIJIL_DATABASE_URL: postgresql+asyncpg://sijil_cp_app:" in env
    for absent in (
        "SIJIL_PROVISIONER_DATABASE_URL",
        "*objectstore-env",
        "SIJIL_OBJECTSTORE_APP_",
        "SEAL_",
        "REMOVAL_",
    ):
        assert absent not in env, absent
    assert "SIJIL_OBJECTSTORE_EXPORT_ACCESS_KEY" in _anchor("objectstore-root-env")
    anchors = ("worker-env", "extraction-env", "seal-env", "removal-env", "sijil-env")
    for anchor in (*anchors, "objectstore-env", "objectstore-root-env"):
        assert "SIJIL_EXPORT_SIGNING_KEY" not in _anchor(anchor), anchor
    for anchor in anchors:
        assert "SIJIL_OBJECTSTORE_EXPORT" not in _anchor(anchor), anchor
    services = ("control-plane", "worker", "worker-extraction", "worker-seal", "worker-removal")
    for service in (*services, "smtp", "beat", "tests", "migrate"):
        assert "SIJIL_EXPORT_SIGNING_KEY" not in _block(service), service
        assert "SIJIL_OBJECTSTORE_EXPORT" not in _block(service), service
    # The control plane publishes the public key and verifies with it.
    assert "SIJIL_EXPORT_PUBLIC_KEY: ${SIJIL_EXPORT_PUBLIC_KEY:?}" in _block("control-plane")


def test_the_daily_expiry_runs_on_the_export_worker() -> None:
    from pydantic import RedisDsn

    from sijil_worker.app import QUEUES, make_celery
    from sijil_worker.settings import BrokerSettings

    settings = BrokerSettings.model_construct(
        region="ma-cas", environment="test", redis_url=RedisDsn("redis://127.0.0.1:1/0")
    )
    assert "export" in QUEUES
    entry = make_celery(settings).conf.beat_schedule["expire-exports-daily"]
    assert entry["task"] == "sijil.expire_exports"
    assert entry["options"] == {"queue": "export"}
