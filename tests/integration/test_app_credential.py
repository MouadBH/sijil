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
    for governance in (True, False):  # the removal path gets its own credential (M4)
        with pytest.raises(ClientError) as raised:
            await app.remove_original(ref, governance=governance)
        assert raised.value.response["Error"]["Code"] == "AccessDenied"
    with pytest.raises(ClientError):
        await app.seal(ref, retain)  # the sealing path gets its own credential (M3)

    # Still there, unchanged, with its lock: then the root key cleans up the test object.
    assert await root.head(ref) == len(data)
    await root.remove_original(ref, governance=True)


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
    # The sealing key is the seal worker's alone (product-owner ruling): never the API's.
    assert os.environ["SIJIL_OBJECTSTORE_SEAL_SECRET_KEY"] not in env.values()
    assert not [name for name in env if name.startswith("SIJIL_OBJECTSTORE_SEAL_")]
    # Nor the removal key, which is `worker-removal`'s alone (tests/integration/test_removal_key).
    assert os.environ["SIJIL_OBJECTSTORE_REMOVAL_SECRET_KEY"] not in env.values()


def test_seal_worker_holds_the_seal_key_and_nothing_more(
    docker_client: docker.DockerClient, worker_settings: WorkerSettings
) -> None:
    (container,) = docker_client.containers.list(
        filters={
            "label": [
                f"com.docker.compose.project={worker_settings.compose_project}",
                "com.docker.compose.service=worker-seal",
            ]
        }
    )
    env = dict(e.split("=", 1) for e in container.attrs["Config"]["Env"])
    assert env["SIJIL_OBJECTSTORE_ACCESS_KEY"] == os.environ["SIJIL_OBJECTSTORE_SEAL_ACCESS_KEY"]
    for secret in (
        os.environ["SIJIL_OBJECTSTORE_SECRET_KEY"],  # root
        os.environ["SIJIL_OBJECTSTORE_APP_SECRET_KEY"],
    ):
        assert secret not in env.values()
    for absent in ("SIJIL_PROVISIONER_DATABASE_URL", "SIJIL_MASTER_KEY", "SIJIL_TENANT_IMAGE"):
        assert absent not in env, absent
    networks = set(container.attrs["NetworkSettings"]["Networks"])
    assert networks == {f"{worker_settings.compose_project}_core"}


@pytest.fixture
def sealer() -> S3ObjectStore:
    settings = StorageSettings()  # type: ignore[call-arg]
    settings.objectstore_access_key = SecretStr(os.environ["SIJIL_OBJECTSTORE_SEAL_ACCESS_KEY"])
    settings.objectstore_secret_key = SecretStr(os.environ["SIJIL_OBJECTSTORE_SEAL_SECRET_KEY"])
    return S3ObjectStore(settings)


async def test_seal_credential_seals_but_never_reads_writes_or_destroys(
    app: S3ObjectStore, sealer: S3ObjectStore, root: S3ObjectStore, tmp_path: Path
) -> None:
    """The seal worker's key switches an original to COMPLIANCE (the app key cannot,
    see above), and can do nothing else to it: no bytes in or out, no delete even with the
    bypass flag, no legal hold."""
    path = tmp_path / "original"
    path.write_bytes(b"sealing " + uuid.uuid4().bytes)
    ref = await app.put_original(
        f"it-{uuid.uuid4().hex}/{uuid.uuid4().hex}", path, date.today() + timedelta(days=1)
    )
    target = {"Bucket": sealer.bucket, "Key": ref.key, "VersionId": ref.version_id}
    _denied(lambda: sealer.client.get_object(**target))
    _denied(lambda: sealer.client.put_object(Bucket=sealer.bucket, Key=ref.key, Body=b"x"))
    _denied(lambda: sealer.client.delete_object(**target, BypassGovernanceRetention=True))
    _denied(lambda: sealer.client.put_object_legal_hold(**target, LegalHold={"Status": "ON"}))

    final = date.today() + timedelta(days=2)
    await sealer.seal(ref, final)
    retention = root.client.get_object_retention(**target)["Retention"]
    assert retention["Mode"] == "COMPLIANCE"
    assert retention["RetainUntilDate"].date() == final
    await sealer.seal(ref, final)  # idempotent: a retried seal changes nothing
    _denied(lambda: sealer.client.delete_object(**target, BypassGovernanceRetention=True))
    _denied(lambda: root.client.delete_object(**target, BypassGovernanceRetention=True))
    assert await root.head(ref) == path.stat().st_size
