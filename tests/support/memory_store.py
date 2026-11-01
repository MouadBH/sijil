"""An in-memory `ObjectStore` with the semantics of the real object-locked bucket.

Every put creates a new version (the bucket is versioned), retention can only move later,
`seal` switches to COMPLIANCE and `governance_remove` refuses sealed versions. Tests read
`puts` and `objects` to assert what reached the store.
"""

import itertools
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from sijil_core.storage import ObjectRef, ObjectStore

CHUNK_BYTES = 1024 * 1024


@dataclass
class StoredObject:
    data: bytes
    mode: str
    retain_until: date


@dataclass(frozen=True)
class Put:
    key: str
    version_id: str
    retain_until: date
    size: int


class SealedObjectError(PermissionError):
    """The store refuses to remove a COMPLIANCE-locked version."""


class MemoryObjectStore(ObjectStore):
    def __init__(self) -> None:
        self.objects: dict[ObjectRef, StoredObject] = {}
        self.puts: list[Put] = []
        self.removed: list[ObjectRef] = []
        self._versions = itertools.count(1)

    async def ensure_bucket(self) -> None:
        return None

    async def put_original(self, key: str, path: Path, retain_until: date) -> ObjectRef:
        data = path.read_bytes()  # noqa: ASYNC240 - test double, small files
        ref = ObjectRef(key=key, version_id=f"v{next(self._versions)}")
        self.objects[ref] = StoredObject(data=data, mode="GOVERNANCE", retain_until=retain_until)
        self.puts.append(Put(key, ref.version_id, retain_until, len(data)))
        return ref

    def _get(self, ref: ObjectRef) -> StoredObject:
        try:
            return self.objects[ref]
        except KeyError:
            raise FileNotFoundError(f"{ref.key}@{ref.version_id}") from None

    async def open(self, ref: ObjectRef) -> AsyncIterator[bytes]:
        data = self._get(ref).data
        for start in range(0, len(data), CHUNK_BYTES):
            yield data[start : start + CHUNK_BYTES]

    async def head(self, ref: ObjectRef) -> int | None:
        stored = self.objects.get(ref)
        return None if stored is None else len(stored.data)

    async def extend_retention(self, ref: ObjectRef, retain_until: date) -> None:
        stored = self._get(ref)
        if retain_until < stored.retain_until:
            raise ValueError("retention can only be extended, never shortened")
        stored.retain_until = retain_until

    async def seal(self, ref: ObjectRef, retain_until: date) -> None:
        stored = self._get(ref)
        stored.mode = "COMPLIANCE"
        stored.retain_until = max(stored.retain_until, retain_until)

    async def governance_remove(self, ref: ObjectRef) -> None:
        stored = self._get(ref)
        if stored.mode == "COMPLIANCE":
            raise SealedObjectError(f"{ref.key}@{ref.version_id} is sealed")
        del self.objects[ref]
        self.removed.append(ref)

    def versions(self, key: str) -> list[ObjectRef]:
        return [ref for ref in self.objects if ref.key == key]
