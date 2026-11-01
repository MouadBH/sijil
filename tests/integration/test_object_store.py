"""The object store honours governance and compliance lock. Run with `make itest`."""

import hashlib
import subprocess
import sys
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from sijil_core.storage import ObjectRef, S3ObjectStore, StorageSettings

pytestmark = pytest.mark.integration


@pytest.fixture
async def store() -> S3ObjectStore:
    s = S3ObjectStore(StorageSettings())  # type: ignore[call-arg]
    await s.ensure_bucket()
    return s


def _key() -> str:
    return f"it-{uuid.uuid4().hex}/{uuid.uuid4().hex}"


def _file(tmp_path: Path, data: bytes) -> Path:
    path = tmp_path / uuid.uuid4().hex
    path.write_bytes(data)
    return path


def _code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _delete(store: S3ObjectStore, ref: ObjectRef, *, bypass: bool) -> Any:
    kwargs: dict[str, Any] = {"Bucket": store.bucket, "Key": ref.key, "VersionId": ref.version_id}
    if bypass:
        kwargs["BypassGovernanceRetention"] = True
    return store.client.delete_object(**kwargs)


def _retention(store: S3ObjectStore, ref: ObjectRef) -> dict[str, Any]:
    out = store.client.get_object_retention(
        Bucket=store.bucket, Key=ref.key, VersionId=ref.version_id
    )
    retention: dict[str, Any] = out["Retention"]
    return retention


async def test_store_honours_governance_and_compliance(
    store: S3ObjectStore, tmp_path: Path
) -> None:
    tomorrow = date.today() + timedelta(days=1)
    gov = await store.put_original(_key(), _file(tmp_path, b"governance"), tomorrow)
    assert (await store.head(gov)) == len(b"governance")
    assert _retention(store, gov)["Mode"] == "GOVERNANCE"

    # 2. delete by version without bypass is denied
    with pytest.raises(ClientError) as denied:
        _delete(store, gov, bypass=False)
    assert _code(denied.value) == "AccessDenied"
    assert (await store.head(gov)) is not None
    # 3. with bypass it is deleted
    _delete(store, gov, bypass=True)
    assert (await store.head(gov)) is None

    # 4. a sealed object is COMPLIANCE
    sealed = await store.put_original(_key(), _file(tmp_path, b"compliance"), tomorrow)
    await store.seal(sealed, tomorrow)
    assert _retention(store, sealed)["Mode"] == "COMPLIANCE"

    # 5. bypass does not help
    with pytest.raises(ClientError) as denied:
        _delete(store, sealed, bypass=True)
    assert _code(denied.value) == "AccessDenied"

    # 6. retention cannot be shortened, even with bypass
    earlier = datetime.combine(date.today(), datetime.min.time(), tzinfo=UTC)
    with pytest.raises(ClientError) as denied:
        store.client.put_object_retention(
            Bucket=store.bucket,
            Key=sealed.key,
            VersionId=sealed.version_id,
            BypassGovernanceRetention=True,
            Retention={"Mode": "COMPLIANCE", "RetainUntilDate": earlier},
        )
    assert _code(denied.value) == "AccessDenied"
    assert (await store.head(sealed)) == len(b"compliance")


async def test_put_and_read_back_roundtrip(store: S3ObjectStore, tmp_path: Path) -> None:
    data = b"sijil" * 500_000
    ref = await store.put_original(_key(), _file(tmp_path, data), date.today() + timedelta(days=1))
    digest = hashlib.sha256()
    chunks = 0
    async for chunk in store.open(ref):
        digest.update(chunk)
        chunks += 1
    assert digest.hexdigest() == hashlib.sha256(data).hexdigest()
    assert chunks > 1


async def test_extend_retention_rejects_shortening(store: S3ObjectStore, tmp_path: Path) -> None:
    far = date.today() + timedelta(days=30)
    ref = await store.put_original(_key(), _file(tmp_path, b"x"), far)
    with pytest.raises(ValueError):
        await store.extend_retention(ref, far - timedelta(days=1))
    assert _retention(store, ref)["RetainUntilDate"].date() == far
    await store.extend_retention(ref, far + timedelta(days=1))
    assert _retention(store, ref)["RetainUntilDate"].date() == far + timedelta(days=1)


async def test_governance_remove_deletes_unsealed(store: S3ObjectStore, tmp_path: Path) -> None:
    ref = await store.put_original(_key(), _file(tmp_path, b"x"), date.today() + timedelta(days=1))
    await store.governance_remove(ref)
    assert (await store.head(ref)) is None


async def test_head_absent_returns_none(store: S3ObjectStore) -> None:
    assert await store.head(ObjectRef(key="nope/none", version_id=str(uuid.uuid4()))) is None


def test_bootstrap_storage_idempotent() -> None:
    for _ in range(2):
        done = subprocess.run(  # noqa: S603
            [sys.executable, "-m", "sijil_cp.cli", "bootstrap-storage"],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert done.returncode == 0, done.stderr
