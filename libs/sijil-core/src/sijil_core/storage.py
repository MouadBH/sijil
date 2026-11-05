"""Write-once original store with object lock (retention-lock layer 4, design §5.2a).

Originals are put in GOVERNANCE mode while unsealed: only the Owner's pre-seal removal
(`remove_original(..., governance=True)`, the removal worker's key) may bypass it. `seal`
switches the object to COMPLIANCE, which nobody can remove or shorten until the retain-until
date; after it, `remove_original(..., governance=False)` deletes the version without a bypass.

boto3 is synchronous, so every S3 call runs in `asyncio.to_thread`.
"""

import asyncio
import base64
import hashlib
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pydantic import AnyHttpUrl, SecretStr
from pydantic_settings import SettingsConfigDict

from sijil_core.settings import CoreSettings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

CHUNK_BYTES = 1024 * 1024
# S3 signs requests for an AWS region name; this is not the Sijil region.
_SIGNING_REGION = "us-east-1"
_ABSENT_CODES = {"404", "NoSuchKey", "NoSuchVersion", "NotFound"}


class StorageSettings(CoreSettings):
    """Object-store connection. Credentials are secrets and never appear in errors."""

    model_config = SettingsConfigDict(
        env_prefix="SIJIL_", extra="ignore", hide_input_in_errors=True
    )

    objectstore_endpoint: AnyHttpUrl
    objectstore_access_key: SecretStr
    objectstore_secret_key: SecretStr
    objectstore_bucket: str


class RetentionActive(RuntimeError):
    """`remove_original` refused before calling the store: the version is COMPLIANCE-locked
    until today or later. Carries the object reference only."""


@dataclass(frozen=True)
class ObjectRef:
    key: str
    version_id: str


class ObjectStore(Protocol):
    async def ensure_bucket(self) -> None: ...

    async def put_original(self, key: str, path: Path, retain_until: date) -> ObjectRef:
        """Store the file in GOVERNANCE mode until `retain_until`."""
        ...

    def open(self, ref: ObjectRef) -> AsyncIterator[bytes]: ...

    async def head(self, ref: ObjectRef) -> int | None:
        """Size in bytes, or None if the object version is absent."""
        ...

    async def extend_retention(self, ref: ObjectRef, retain_until: date) -> None:
        """Move the retain-until date later. Shortening raises ValueError."""
        ...

    async def seal(self, ref: ObjectRef, retain_until: date) -> None:
        """Switch the object to COMPLIANCE mode, keeping any later existing date.

        Idempotent: on a COMPLIANCE object it is a no-op at the same or an earlier date and only
        extends to a later one. Callers need no row lock: the seal worker passes the ledger's
        `retention_until`, which the ledger trigger freezes once sealed, so concurrent runs
        (a redelivery, a sweep re-send) all ask for the same date. (`extend_retention` still
        runs under the ledger row lock at routing.)
        """
        ...

    async def remove_original(self, ref: ObjectRef, *, governance: bool) -> None:
        """Delete the version: with `governance`, bypassing its GOVERNANCE lock (the pre-seal
        removal); without, only once its lock has expired (after retention).

        Reads the lock first and raises `RetentionActive`, sending no delete, when the version
        is COMPLIANCE until today or later (the store refuses that delete too). A read that
        finds no version still sends the versioned delete: the store's success, or its
        NoSuchVersion/NoSuchKey, counts as removed; any refusal propagates.
        """
        ...

    async def retention(self, ref: ObjectRef) -> tuple[str, date] | None:
        """The version's lock mode and retain-until date (UTC calendar date), or None when the
        version is absent. Read-only (`s3:GetObjectRetention`): the drift check uses it."""
        ...


def _end_of_day(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=UTC)


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _retention_absent(exc: ClientError) -> bool:
    """GetObjectRetention found no such version. RustFS 1.0.4 answers every absent version
    (deleted, unknown version, unknown key) with 500 InternalError "Version not found: ...",
    never a 404 (probed; pinned by tests/integration/test_removal_key.py)."""
    if _error_code(exc) in _ABSENT_CODES:
        return True
    message = str(exc.response.get("Error", {}).get("Message", ""))
    return _error_code(exc) == "InternalError" and message.startswith("Version not found")


class S3ObjectStore(ObjectStore):
    def __init__(self, settings: StorageSettings) -> None:
        self.bucket = settings.objectstore_bucket
        self.client: S3Client = boto3.client(
            "s3",
            endpoint_url=str(settings.objectstore_endpoint),
            aws_access_key_id=settings.objectstore_access_key.get_secret_value(),
            aws_secret_access_key=settings.objectstore_secret_key.get_secret_value(),
            region_name=_SIGNING_REGION,
            config=Config(
                s3={"addressing_style": "path"},
                retries={"max_attempts": 3, "mode": "standard"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )

    async def ensure_bucket(self) -> None:
        await asyncio.to_thread(self._ensure_bucket)

    def _ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            if _error_code(exc) not in _ABSENT_CODES:
                raise
            try:
                self.client.create_bucket(Bucket=self.bucket, ObjectLockEnabledForBucket=True)
            except ClientError as created:
                if _error_code(created) not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                    raise
        config = self.client.get_object_lock_configuration(Bucket=self.bucket)
        enabled = config.get("ObjectLockConfiguration", {}).get("ObjectLockEnabled")
        if enabled != "Enabled":
            raise RuntimeError(f"bucket {self.bucket} does not have object lock enabled")
        versioning = self.client.get_bucket_versioning(Bucket=self.bucket).get("Status")
        if versioning != "Enabled":
            raise RuntimeError(f"bucket {self.bucket} does not have versioning enabled")

    async def put_original(self, key: str, path: Path, retain_until: date) -> ObjectRef:
        return await asyncio.to_thread(self._put, key, path, retain_until)

    def _put(self, key: str, path: Path, retain_until: date) -> ObjectRef:
        # The store verifies the bytes it received against this digest, so corruption in
        # transit is rejected instead of being locked for the retention period.
        digest = hashlib.sha256()
        with path.open("rb") as source:
            while block := source.read(CHUNK_BYTES):
                digest.update(block)
        checksum = base64.b64encode(digest.digest()).decode()
        with path.open("rb") as body:
            out = self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=checksum,
                ObjectLockMode="GOVERNANCE",
                ObjectLockRetainUntilDate=_end_of_day(retain_until),
            )
        return ObjectRef(key=key, version_id=out["VersionId"])

    async def open(self, ref: ObjectRef) -> AsyncIterator[bytes]:
        body = await asyncio.to_thread(
            lambda: self.client.get_object(
                Bucket=self.bucket, Key=ref.key, VersionId=ref.version_id
            )["Body"]
        )
        chunks: Iterator[bytes] = body.iter_chunks(CHUNK_BYTES)
        try:
            while True:
                chunk = await asyncio.to_thread(next, chunks, None)
                if chunk is None:
                    return
                yield chunk
        finally:
            await asyncio.to_thread(body.close)

    async def head(self, ref: ObjectRef) -> int | None:
        return await asyncio.to_thread(self._head, ref)

    def _head(self, ref: ObjectRef) -> int | None:
        try:
            out = self.client.head_object(Bucket=self.bucket, Key=ref.key, VersionId=ref.version_id)
        except ClientError as exc:
            if _error_code(exc) in _ABSENT_CODES:
                return None
            raise
        return int(out["ContentLength"])

    def _current(self, ref: ObjectRef) -> tuple[str, datetime]:
        out = self.client.get_object_retention(
            Bucket=self.bucket, Key=ref.key, VersionId=ref.version_id
        )["Retention"]
        return str(out["Mode"]), out["RetainUntilDate"]

    def _set(self, ref: ObjectRef, mode: Any, until: datetime, *, bypass: bool = False) -> None:
        self.client.put_object_retention(
            Bucket=self.bucket,
            Key=ref.key,
            VersionId=ref.version_id,
            BypassGovernanceRetention=bypass,
            Retention={"Mode": mode, "RetainUntilDate": until},
        )

    async def extend_retention(self, ref: ObjectRef, retain_until: date) -> None:
        await asyncio.to_thread(self._extend, ref, retain_until)

    def _extend(self, ref: ObjectRef, retain_until: date) -> None:
        mode, current = self._current(ref)
        target = _end_of_day(retain_until)
        if target < current:
            raise ValueError("retention can only be extended, never shortened")
        try:
            self._set(ref, mode, target)
        except ClientError as exc:
            # The store itself refuses shortening (e.g. a concurrent extend landed first).
            if _error_code(exc) == "AccessDenied":
                raise ValueError("retention can only be extended, never shortened") from exc
            raise

    async def seal(self, ref: ObjectRef, retain_until: date) -> None:
        await asyncio.to_thread(self._seal, ref, retain_until)

    def _seal(self, ref: ObjectRef, retain_until: date) -> None:
        mode, current = self._current(ref)
        target = max(current, _end_of_day(retain_until))
        if mode == "COMPLIANCE":
            # Already sealed (a retried lock task): idempotent. A later date only extends,
            # which COMPLIANCE allows without any bypass; never shortened.
            if target > current:
                self._set(ref, "COMPLIANCE", target)
            return
        # (a) Extend in GOVERNANCE without bypass: the store refuses shortening here, so a
        # concurrent extend cannot be overwritten by a stale date.
        if mode == "GOVERNANCE" and target > current:
            self._set(ref, "GOVERNANCE", target)
        # (b) Flip the mode. Replacing a GOVERNANCE retention needs the bypass flag.
        self._set(ref, "COMPLIANCE", target, bypass=True)
        # (c) Verify, never assume.
        final_mode, final_until = self._current(ref)
        if final_mode != "COMPLIANCE" or final_until < target:
            raise RuntimeError("seal did not result in COMPLIANCE at the requested date")

    async def remove_original(self, ref: ObjectRef, *, governance: bool) -> None:
        await asyncio.to_thread(self._remove, ref, governance)

    async def retention(self, ref: ObjectRef) -> tuple[str, date] | None:
        return await asyncio.to_thread(self._retention, ref)

    def _retention(self, ref: ObjectRef) -> tuple[str, date] | None:
        try:
            mode, until = self._current(ref)
        except ClientError as exc:
            if _retention_absent(exc):
                return None
            raise
        return mode, until.astimezone(UTC).date()

    def _remove(self, ref: ObjectRef, governance: bool) -> None:
        try:
            mode, until = self._current(ref)
        except ClientError as exc:
            if not _retention_absent(exc):
                raise
            # Absent by the read alone is never trusted (a misread 500, a changed message, a
            # wrong version id): the versioned delete below is sent anyway, and the store's
            # answer decides. A COMPLIANCE version the read missed is still refused by the store.
        else:
            if mode == "COMPLIANCE" and until.astimezone(UTC).date() >= datetime.now(UTC).date():
                raise RetentionActive(f"{ref.key}@{ref.version_id}")
        try:
            if governance:
                self.client.delete_object(
                    Bucket=self.bucket,
                    Key=ref.key,
                    VersionId=ref.version_id,
                    BypassGovernanceRetention=True,
                )
            else:  # after retention: no bypass flag at all
                self.client.delete_object(Bucket=self.bucket, Key=ref.key, VersionId=ref.version_id)
        except ClientError as exc:
            if _error_code(exc) not in _ABSENT_CODES:
                raise


__all__ = ["ObjectRef", "ObjectStore", "RetentionActive", "S3ObjectStore", "StorageSettings"]
