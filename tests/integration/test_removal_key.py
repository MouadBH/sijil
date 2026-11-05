"""The removal key against RustFS 1.0.4 (retention-lock layer 4): it deletes an unsealed
(GOVERNANCE) original or one whose lock has expired, and can do nothing else.

`migrate` provisions it with the root key (`sijil-cp bootstrap-storage`, policy
`sijil-removal`); only `worker-removal` holds it. This runner holds every key, to put with the
app key, seal with the seal key and prove what the removal key can and cannot do. Run with
`make itest`.
"""

import asyncio
import os
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import docker
import pytest
from botocore.exceptions import ClientError
from pydantic import SecretStr

from sijil_core.storage import ObjectRef, RetentionActive, S3ObjectStore, StorageSettings
from sijil_worker.settings import WorkerSettings

pytestmark = pytest.mark.integration


def _store(prefix: str | None) -> S3ObjectStore:
    settings = StorageSettings()  # type: ignore[call-arg]
    if prefix is not None:
        settings.objectstore_access_key = SecretStr(os.environ[f"{prefix}_ACCESS_KEY"])
        settings.objectstore_secret_key = SecretStr(os.environ[f"{prefix}_SECRET_KEY"])
    return S3ObjectStore(settings)


@pytest.fixture
def root() -> S3ObjectStore:
    return _store(None)


@pytest.fixture
def app() -> S3ObjectStore:
    return _store("SIJIL_OBJECTSTORE_APP")


@pytest.fixture
def sealer() -> S3ObjectStore:
    return _store("SIJIL_OBJECTSTORE_SEAL")


@pytest.fixture
def removal() -> S3ObjectStore:
    return _store("SIJIL_OBJECTSTORE_REMOVAL")


def _denied(call: Any) -> None:
    with pytest.raises(ClientError) as raised:
        call()
    assert raised.value.response["Error"]["Code"] == "AccessDenied"


def _key() -> str:
    return f"it-{uuid.uuid4().hex}/{uuid.uuid4().hex}"


async def _put(app: S3ObjectStore, tmp_path: Path, days: int = 2) -> ObjectRef:
    path = tmp_path / f"original-{uuid.uuid4().hex}"
    path.write_bytes(b"SAMPLE removal " + uuid.uuid4().bytes)
    return await app.put_original(_key(), path, date.today() + timedelta(days=days))


def _target(store: S3ObjectStore, ref: ObjectRef) -> dict[str, str]:
    return {"Bucket": store.bucket, "Key": ref.key, "VersionId": ref.version_id}


async def test_removal_key_deletes_governance_version(
    app: S3ObjectStore, removal: S3ObjectStore, root: S3ObjectStore, tmp_path: Path
) -> None:
    """The Owner's pre-seal removal: the version goes, with the GOVERNANCE bypass."""
    ref = await _put(app, tmp_path)
    await removal.remove_original(ref, governance=True)
    assert await root.head(ref) is None
    # A redelivery finds no version and returns normally. RustFS 1.0.4 answers the retention
    # read of an absent version with 500 "Version not found" (sijil_core.storage).
    assert await removal.retention(ref) is None
    await removal.remove_original(ref, governance=True)
    await removal.remove_original(ObjectRef(_key(), ref.version_id), governance=True)
    await removal.remove_original(ObjectRef(ref.key, str(uuid.uuid4())), governance=False)
    assert await root.head(ref) is None


async def test_an_already_deleted_version_still_ends_removed(
    app: S3ObjectStore, removal: S3ObjectStore, root: S3ObjectStore, tmp_path: Path
) -> None:
    """Deleted behind the worker's back (or by a run that crashed before its commit): the
    absent read still sends the versioned delete, which RustFS answers 204, with and without
    the bypass."""
    for governance in (True, False):
        ref = await _put(app, tmp_path)
        await root.remove_original(ref, governance=True)
        assert await root.head(ref) is None
        await removal.remove_original(ref, governance=governance)
        assert await root.head(ref) is None


async def test_removal_key_deletes_an_expired_lock_without_the_bypass(
    removal: S3ObjectStore, root: S3ObjectStore
) -> None:
    """After retention the delete is sent without the bypass, and the store takes it."""
    key = _key()
    out = root.client.put_object(
        Bucket=root.bucket,
        Key=key,
        Body=b"SAMPLE expired",
        ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=datetime.now(UTC) + timedelta(seconds=2),
    )
    ref = ObjectRef(key, out["VersionId"])
    # Unexpired, the store refuses the delete without the bypass.
    _denied(lambda: removal.client.delete_object(**_target(removal, ref)))
    await asyncio.sleep(3)
    await removal.remove_original(ref, governance=False)
    assert await root.head(ref) is None


async def test_removal_key_cannot_delete_compliance_version(
    app: S3ObjectStore, sealer: S3ObjectStore, removal: S3ObjectStore, root: S3ObjectStore,
    tmp_path: Path,
) -> None:  # fmt: skip
    """A sealed original: `remove_original` refuses before any call, and the store refuses
    the delete itself, with or without the bypass."""
    ref = await _put(app, tmp_path, days=1)
    await sealer.seal(ref, date.today() + timedelta(days=1))
    for governance in (True, False):
        with pytest.raises(RetentionActive):
            await removal.remove_original(ref, governance=governance)
    target = _target(removal, ref)
    _denied(lambda: removal.client.delete_object(**target, BypassGovernanceRetention=True))
    _denied(lambda: removal.client.delete_object(**target))
    assert await root.head(ref) is not None


async def test_removal_key_cannot_read_or_shorten(
    app: S3ObjectStore, removal: S3ObjectStore, root: S3ObjectStore, tmp_path: Path
) -> None:
    """No bytes in or out, no retention change of any kind, no legal hold, no delete marker."""
    ref = await _put(app, tmp_path)
    target = _target(removal, ref)
    _denied(lambda: removal.client.get_object(**target))
    with pytest.raises(ClientError) as raised:  # a HEAD answer has no body: code is the status
        removal.client.head_object(**target)
    assert raised.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    _denied(lambda: removal.client.put_object(Bucket=removal.bucket, Key=ref.key, Body=b"x"))
    for days, bypass in ((-1, False), (-1, True), (30, False)):  # shorten, or even extend
        until = datetime.now(UTC) + timedelta(days=2 + days)
        _denied(
            lambda until=until, bypass=bypass: removal.client.put_object_retention(
                **target,
                Retention={"Mode": "GOVERNANCE", "RetainUntilDate": until},
                BypassGovernanceRetention=bypass,
            )
        )
    _denied(lambda: removal.client.put_object_legal_hold(**target, LegalHold={"Status": "ON"}))
    _denied(lambda: removal.client.delete_object(Bucket=removal.bucket, Key=ref.key))
    retention = await removal.retention(ref)  # it reads the lock, which it needs to decide
    assert retention is not None
    assert retention[0] == "GOVERNANCE"
    assert await root.head(ref) is not None
    await root.remove_original(ref, governance=True)


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


def test_removal_worker_holds_the_removal_key_and_nothing_more(
    docker_client: docker.DockerClient, worker_settings: WorkerSettings
) -> None:
    attrs = _env(docker_client, worker_settings.compose_project, "worker-removal")
    env = dict(e.split("=", 1) for e in attrs["Config"]["Env"])
    assert env["SIJIL_OBJECTSTORE_ACCESS_KEY"] == os.environ["SIJIL_OBJECTSTORE_REMOVAL_ACCESS_KEY"]
    assert env["SIJIL_OBJECTSTORE_SECRET_KEY"] == os.environ["SIJIL_OBJECTSTORE_REMOVAL_SECRET_KEY"]
    for other in ("APP", "SEAL"):
        assert os.environ[f"SIJIL_OBJECTSTORE_{other}_SECRET_KEY"] not in env.values(), other
    assert os.environ["SIJIL_OBJECTSTORE_SECRET_KEY"] not in env.values()  # this runner: root
    for absent in ("SIJIL_PROVISIONER_DATABASE_URL", "SIJIL_MASTER_KEY", "SIJIL_TENANT_IMAGE"):
        assert absent not in env, absent
    assert not [n for n in env if n.startswith("SIJIL_OBJECTSTORE_APP_") or "SEAL_" in n]
    networks = set(attrs["NetworkSettings"]["Networks"])
    assert networks == {f"{worker_settings.compose_project}_core"}


@pytest.mark.parametrize(
    "service", ["control-plane", "worker", "worker-extraction", "worker-seal", "smtp", "beat"]
)
def test_no_other_service_holds_the_removal_key(
    docker_client: docker.DockerClient, worker_settings: WorkerSettings, service: str
) -> None:
    attrs = _env(docker_client, worker_settings.compose_project, service)
    env = dict(e.split("=", 1) for e in attrs["Config"]["Env"])
    assert os.environ["SIJIL_OBJECTSTORE_REMOVAL_SECRET_KEY"] not in env.values()
    assert not [name for name in env if name.startswith("SIJIL_OBJECTSTORE_REMOVAL_")]
