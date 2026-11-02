"""The services' object-store credential cannot destroy an original (retention-lock layer 4).

`migrate` provisions it with the root key (`sijil-cp bootstrap-storage`); every running service
uses it. This runner holds both: the root key for the store tests that need the bypass, and the
app key to prove what the services can and cannot do. Run with `make itest`.
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

from sijil_core.storage import ObjectRef, S3ObjectStore, StorageSettings
from sijil_worker.settings import WorkerSettings

pytestmark = pytest.mark.integration


@pytest.fixture
def root() -> S3ObjectStore:
    return S3ObjectStore(StorageSettings())  # type: ignore[call-arg]


@pytest.fixture
def app() -> S3ObjectStore:
    settings = StorageSettings()  # type: ignore[call-arg]
    settings.objectstore_access_key = SecretStr(os.environ["SIJIL_OBJECTSTORE_APP_ACCESS_KEY"])
    settings.objectstore_secret_key = SecretStr(os.environ["SIJIL_OBJECTSTORE_APP_SECRET_KEY"])
    return S3ObjectStore(settings)


def _denied(call: Any) -> None:
    with pytest.raises(ClientError) as raised:
        call()
    assert raised.value.response["Error"]["Code"] == "AccessDenied"


async def test_app_credential_puts_and_reads_but_never_destroys(
    app: S3ObjectStore, root: S3ObjectStore, tmp_path: Path
) -> None:
    data = b"least privilege " + uuid.uuid4().bytes
    path = tmp_path / "original"
    path.write_bytes(data)
    key = f"it-{uuid.uuid4().hex}/{uuid.uuid4().hex}"
    retain = date.today() + timedelta(days=2)

    ref = await app.put_original(key, path, retain)  # with its GOVERNANCE lock
    assert await app.head(ref) == len(data)
    assert b"".join([chunk async for chunk in app.open(ref)]) == data
    assert await app.head(ObjectRef(key + "-absent", ref.version_id)) is None  # 404, not 403

    target = {"Bucket": app.bucket, "Key": ref.key, "VersionId": ref.version_id}
    _denied(lambda: app.client.delete_object(**target, BypassGovernanceRetention=True))
    _denied(lambda: app.client.delete_object(**target))
    _denied(lambda: app.client.delete_object(Bucket=app.bucket, Key=ref.key))  # delete marker
    sooner = {"Mode": "GOVERNANCE", "RetainUntilDate": datetime.now(UTC) + timedelta(hours=1)}
    for bypass in (False, True):  # the store refuses to shorten, with or without the flag
        _denied(
            lambda bypass=bypass: app.client.put_object_retention(
                **target, Retention=sooner, BypassGovernanceRetention=bypass
            )
        )
    _denied(lambda: app.client.put_object_legal_hold(**target, LegalHold={"Status": "ON"}))
    with pytest.raises(ClientError):
        await app.governance_remove(ref)
    with pytest.raises(ClientError):
        await app.seal(ref, retain)  # the sealing path gets its own credential (M3/M4)

    # Still there, unchanged, with its lock: then the root key cleans up the test object.
    assert await root.head(ref) == len(data)
    await root.governance_remove(ref)


@pytest.mark.parametrize("service", ["control-plane", "worker", "smtp", "beat"])
def test_running_services_hold_the_app_key_not_the_root_key(
    docker_client: docker.DockerClient, worker_settings: WorkerSettings, service: str
) -> None:
    (container,) = docker_client.containers.list(
        filters={
            "label": [
                f"com.docker.compose.project={worker_settings.compose_project}",
                f"com.docker.compose.service={service}",
            ]
        }
    )
    env = dict(e.split("=", 1) for e in container.attrs["Config"]["Env"])
    root_secret = os.environ["SIJIL_OBJECTSTORE_SECRET_KEY"]  # this runner holds the root key
    assert env["SIJIL_OBJECTSTORE_ACCESS_KEY"] == os.environ["SIJIL_OBJECTSTORE_APP_ACCESS_KEY"]
    assert root_secret not in env.values()
