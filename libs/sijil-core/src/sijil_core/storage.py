"""Write-once original store with object lock (retention-lock layer 4, design §5.2a).

Originals are put in GOVERNANCE mode while unsealed: only the Owner's pre-seal removal flow
(`governance_remove`) may bypass it. `seal` switches the object to COMPLIANCE, which nobody can
remove or shorten until the retain-until date.

boto3 is synchronous, so every S3 call runs in `asyncio.to_thread`.
"""

import asyncio
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
        """Switch the object to COMPLIANCE mode."""
        ...

    async def governance_remove(self, ref: ObjectRef) -> None:
        """Delete an unsealed object, bypassing GOVERNANCE. Fails on a sealed one."""
        ...


def _end_of_day(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=UTC)


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


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

    async def put_original(self, key: str, path: Path, retain_until: date) -> ObjectRef:
        return await asyncio.to_thread(self._put, key, path, retain_until)

    def _put(self, key: str, path: Path, retain_until: date) -> ObjectRef:
        with path.open("rb") as body:
            out = self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
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
        self._set(ref, mode, target)

    async def seal(self, ref: ObjectRef, retain_until: date) -> None:
        await asyncio.to_thread(self._seal, ref, retain_until)

    def _seal(self, ref: ObjectRef, retain_until: date) -> None:
        _, current = self._current(ref)
        # Replacing a GOVERNANCE retention needs the bypass flag, even to make it stricter.
        self._set(ref, "COMPLIANCE", max(current, _end_of_day(retain_until)), bypass=True)

    async def governance_remove(self, ref: ObjectRef) -> None:
        await asyncio.to_thread(self._remove, ref)

    def _remove(self, ref: ObjectRef) -> None:
        self.client.delete_object(
            Bucket=self.bucket,
            Key=ref.key,
            VersionId=ref.version_id,
            BypassGovernanceRetention=True,
        )


__all__ = ["ObjectRef", "ObjectStore", "S3ObjectStore", "StorageSettings"]
