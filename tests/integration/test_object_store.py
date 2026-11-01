"""The object store honours governance and compliance lock. Run with `make itest`."""

import base64
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


async def test_put_with_wrong_checksum_is_rejected(store: S3ObjectStore) -> None:
    wrong = base64.b64encode(hashlib.sha256(b"other bytes").digest()).decode()
    with pytest.raises(ClientError) as rejected:
        store.client.put_object(
            Bucket=store.bucket,
            Key=_key(),
            Body=b"actual bytes",
            ChecksumAlgorithm="SHA256",
            ChecksumSHA256=wrong,
        )
    print("wrong-checksum response:", rejected.value.response)


async def test_put_original_sends_sha256_checksum(
    store: S3ObjectStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: dict[str, Any] = {}
    real = store.client.put_object

    def spy(**kwargs: Any) -> Any:
        sent.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(store.client, "put_object", spy)
    data = b"checksummed"
    await store.put_original(_key(), _file(tmp_path, data), date.today() + timedelta(days=1))
    assert sent["ChecksumAlgorithm"] == "SHA256"
    assert sent["ChecksumSHA256"] == base64.b64encode(hashlib.sha256(data).digest()).decode()


async def test_governance_shortening_without_bypass_is_refused(
    store: S3ObjectStore, tmp_path: Path
) -> None:
    far = date.today() + timedelta(days=30)
    ref = await store.put_original(_key(), _file(tmp_path, b"x"), far)
    earlier = datetime.combine(date.today() + timedelta(days=2), datetime.min.time(), tzinfo=UTC)
    with pytest.raises(ClientError) as refused:
        store.client.put_object_retention(
            Bucket=store.bucket,
            Key=ref.key,
            VersionId=ref.version_id,
            Retention={"Mode": "GOVERNANCE", "RetainUntilDate": earlier},
        )
    assert _code(refused.value) == "AccessDenied"
    assert _retention(store, ref)["RetainUntilDate"].date() == far


async def test_seal_keeps_later_existing_date(store: S3ObjectStore, tmp_path: Path) -> None:
    far = date.today() + timedelta(days=30)
    ref = await store.put_original(_key(), _file(tmp_path, b"x"), far)
    await store.seal(ref, date.today() + timedelta(days=2))
    got = _retention(store, ref)
    assert got["Mode"] == "COMPLIANCE"
    assert got["RetainUntilDate"].date() == far


async def test_seal_sets_requested_end_of_day(store: S3ObjectStore, tmp_path: Path) -> None:
    ref = await store.put_original(_key(), _file(tmp_path, b"x"), date.today() + timedelta(days=1))
    target = date.today() + timedelta(days=40)
    await store.seal(ref, target)
    got = _retention(store, ref)
    assert got["Mode"] == "COMPLIANCE"
    assert got["RetainUntilDate"].replace(microsecond=0) == datetime(
        target.year, target.month, target.day, 23, 59, 59, tzinfo=UTC
    )


async def test_ensure_bucket_requires_versioning(
    store: S3ObjectStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(store.client, "get_bucket_versioning", lambda **_: {})
    with pytest.raises(RuntimeError):
        await store.ensure_bucket()
