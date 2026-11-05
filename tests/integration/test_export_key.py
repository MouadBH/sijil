"""The export key and the exports bucket against RustFS 1.0.4 (plan 4.6, ARC-5).

`migrate` provisions the `sijil-export` user (policy `sijil-export`) and the plain exports
bucket `<originals>-exports` with the root key; only `worker-export` holds the export key and
the region's signing seed. The key reads originals and owns the exports bucket, nothing else;
the app key (the control plane's download) only reads an export. Run with `make itest`.
"""

import os
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import docker
import pytest
from botocore.exceptions import ClientError
from pydantic import SecretStr

from sijil_core.storage import ObjectRef, S3ExportStore, S3ObjectStore, StorageSettings
from sijil_worker.settings import WorkerSettings

pytestmark = pytest.mark.integration


def _settings(prefix: str | None) -> StorageSettings:
    settings = StorageSettings()  # type: ignore[call-arg]
    if prefix is not None:
        settings.objectstore_access_key = SecretStr(os.environ[f"{prefix}_ACCESS_KEY"])
        settings.objectstore_secret_key = SecretStr(os.environ[f"{prefix}_SECRET_KEY"])
    return settings


@pytest.fixture
def root() -> S3ObjectStore:
    return S3ObjectStore(_settings(None))


@pytest.fixture
def app() -> S3ObjectStore:
    return S3ObjectStore(_settings("SIJIL_OBJECTSTORE_APP"))


@pytest.fixture
def exporter() -> S3ObjectStore:
    return S3ObjectStore(_settings("SIJIL_OBJECTSTORE_EXPORT"))


@pytest.fixture
def exports() -> S3ExportStore:
    return S3ExportStore(_settings("SIJIL_OBJECTSTORE_EXPORT"))


@pytest.fixture
def app_exports() -> S3ExportStore:
    return S3ExportStore(_settings("SIJIL_OBJECTSTORE_APP"))


def _denied(call: Any) -> None:
    with pytest.raises(ClientError) as raised:
        call()
    assert raised.value.response["Error"]["Code"] == "AccessDenied"


async def _put(app: S3ObjectStore, tmp_path: Path) -> tuple[ObjectRef, bytes]:
    data = b"SAMPLE export " + uuid.uuid4().bytes
    path = tmp_path / f"original-{uuid.uuid4().hex}"
    path.write_bytes(data)
    key = f"it-{uuid.uuid4().hex}/{uuid.uuid4().hex}"
    return await app.put_original(key, path, date.today() + timedelta(days=2)), data


async def _read(chunks: Any) -> bytes:
    return b"".join([chunk async for chunk in chunks])


async def test_export_key_reads_originals_and_changes_none(
    app: S3ObjectStore, exporter: S3ObjectStore, root: S3ObjectStore, tmp_path: Path
) -> None:
    ref, data = await _put(app, tmp_path)
    assert await _read(exporter.open(ref)) == data
    target = {"Bucket": exporter.bucket, "Key": ref.key, "VersionId": ref.version_id}
    _denied(lambda: exporter.client.put_object(Bucket=exporter.bucket, Key=ref.key, Body=b"x"))
    _denied(lambda: exporter.client.delete_object(Bucket=exporter.bucket, Key=ref.key))
    _denied(lambda: exporter.client.delete_object(**target, BypassGovernanceRetention=True))
    until = datetime.now(UTC) + timedelta(days=30)
    _denied(
        lambda: exporter.client.put_object_retention(
            **target, Retention={"Mode": "COMPLIANCE", "RetainUntilDate": until}
        )
    )
    _denied(lambda: exporter.client.put_object_legal_hold(**target, LegalHold={"Status": "ON"}))
    assert await root.head(ref) is not None
    await root.remove_original(ref, governance=True)


async def test_export_key_owns_the_exports_bucket_and_the_app_key_only_reads_it(
    exports: S3ExportStore, app_exports: S3ExportStore, tmp_path: Path
) -> None:
    key = f"it-{uuid.uuid4().hex}/{uuid.uuid4().hex}.zip"
    path = tmp_path / "export.zip"
    path.write_bytes(b"PK SAMPLE " + uuid.uuid4().bytes)
    await exports.put_export(key, path)
    assert await _read(exports.open_export(key)) == path.read_bytes()
    assert await _read(app_exports.open_export(key)) == path.read_bytes()
    _denied(lambda: app_exports.client.put_object(Bucket=app_exports.bucket, Key=key, Body=b"x"))
    _denied(lambda: app_exports.client.delete_object(Bucket=app_exports.bucket, Key=key))
    status = exports.client.get_bucket_versioning(Bucket=exports.bucket).get("Status")
    assert status not in ("Enabled", "Suspended")  # an expired export really goes
    await exports.delete_export(key)
    with pytest.raises(ClientError):
        await _read(exports.open_export(key))
    await exports.delete_export(key)  # an absent export counts as deleted


def _env(docker_client: docker.DockerClient, project: str, service: str) -> dict[str, Any]:
    (container,) = docker_client.containers.list(
        filters={
            "label": [
                f"com.docker.compose.project={project}",
                f"com.docker.compose.service={service}",
            ]
        }
    )
    attrs: dict[str, Any] = container.attrs
    return attrs


def test_export_worker_holds_the_seed_and_the_export_key_only(
    docker_client: docker.DockerClient, worker_settings: WorkerSettings
) -> None:
    attrs = _env(docker_client, worker_settings.compose_project, "worker-export")
    env = dict(e.split("=", 1) for e in attrs["Config"]["Env"])
    assert env["SIJIL_OBJECTSTORE_ACCESS_KEY"] == os.environ["SIJIL_OBJECTSTORE_EXPORT_ACCESS_KEY"]
    assert env["SIJIL_OBJECTSTORE_SECRET_KEY"] == os.environ["SIJIL_OBJECTSTORE_EXPORT_SECRET_KEY"]
    assert env["SIJIL_EXPORT_SIGNING_KEY"]
    for other in ("APP", "SEAL", "REMOVAL"):
        assert os.environ[f"SIJIL_OBJECTSTORE_{other}_SECRET_KEY"] not in env.values(), other
    assert "SIJIL_PROVISIONER_DATABASE_URL" not in env
    assert set(attrs["NetworkSettings"]["Networks"]) == {f"{worker_settings.compose_project}_core"}
    assert not [m for m in attrs["Mounts"] if "docker.sock" in m.get("Source", "")]


@pytest.mark.parametrize(
    "service",
    ["control-plane", "worker", "worker-extraction", "worker-seal", "worker-removal", "smtp"],
)
def test_no_other_service_holds_the_signing_seed(
    docker_client: docker.DockerClient, worker_settings: WorkerSettings, service: str
) -> None:
    project = worker_settings.compose_project
    seed = dict(
        e.split("=", 1) for e in _env(docker_client, project, "worker-export")["Config"]["Env"]
    )["SIJIL_EXPORT_SIGNING_KEY"]
    env = dict(e.split("=", 1) for e in _env(docker_client, project, service)["Config"]["Env"])
    assert seed not in env.values()
    assert "SIJIL_EXPORT_SIGNING_KEY" not in env
    assert os.environ["SIJIL_OBJECTSTORE_EXPORT_SECRET_KEY"] not in env.values()
