"""Per-workspace hash-chained, append-only audit log.

`hash = sha256(bytes.fromhex(prev_hash) || canonical_json(event))`; the first event's `prev_hash`
is 64 zeros. Appends are serialised per workspace by a transaction-scoped advisory lock and run
inside the caller's transaction (the caller commits).
"""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import ActorType, AuditEvent

GENESIS_HASH = "0" * 64


@dataclass(frozen=True)
class VerifyResult:
    ok: bool
    count: int
    first_bad_seq: int | None


def _default(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("naive datetime in audit event")
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"not canonical-JSON serialisable: {type(value).__name__}")


def canonical_json(obj: Mapping[str, Any]) -> bytes:
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=_default,
    ).encode("utf-8")


def _check_data(value: Any) -> None:
    """Audit data holds only str, int, bool, None and lists/dicts (str keys) of those.

    Floats are refused: JSONB stores numbers as numeric and prints them its own way, so a float
    would hash differently after the round trip and verify would flag an untouched row.
    """
    if value is None or isinstance(value, str | bool | int):
        return
    if isinstance(value, list | tuple):
        for item in value:
            _check_data(item)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("audit data keys must be str")
            _check_data(item)
        return
    raise TypeError(f"audit data does not accept {type(value).__name__}")


def event_hash(prev_hash: str, event: Mapping[str, Any]) -> str:
    return hashlib.sha256(bytes.fromhex(prev_hash) + canonical_json(event)).hexdigest()


def _hashed_fields(
    *,
    workspace_id: UUID,
    seq: int,
    at: datetime,
    actor_type: ActorType,
    actor_id: str | None,
    action: str,
    target_type: str | None,
    target_id: str | None,
    ip: str | None,
    data: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "workspace_id": workspace_id,
        "seq": seq,
        "at": at,
        "actor_type": ActorType(actor_type).value,
        "actor_id": actor_id,
        "action": action,
        "target_type": target_type,
        "target_id": target_id,
        "ip": ip,
        "data": data,
    }


def _row_event(row: AuditEvent) -> dict[str, Any]:
    return _hashed_fields(
        workspace_id=row.workspace_id,
        seq=row.seq,
        at=row.at,
        actor_type=row.actor_type,
        actor_id=row.actor_id,
        action=row.action,
        target_type=row.target_type,
        target_id=row.target_id,
        ip=row.ip,
        data=row.data,
    )


async def append(
    db: AsyncSession,
    *,
    workspace_id: UUID,
    action: str,
    actor_type: ActorType,
    actor_id: str | None,
    target_type: str | None = None,
    target_id: str | None = None,
    ip: str | None = None,
    data: Mapping[str, Any] | None = None,
    now: datetime,
) -> AuditEvent:
    """Add the next event of the workspace's chain, in the caller's transaction."""
    if now.tzinfo is None:
        raise ValueError("audit timestamp must be timezone-aware")
    _check_data(data or {})
    # The lock comes first: the last seq must be read only once no other append can interleave.
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended('audit:' || CAST(:ws AS text), 0))"),
        {"ws": str(workspace_id)},
    )
    last = (
        await db.execute(
            select(AuditEvent.seq, AuditEvent.hash)
            .where(AuditEvent.workspace_id == workspace_id)
            .order_by(AuditEvent.seq.desc())
            .limit(1)
        )
    ).first()
    seq, prev_hash = (last[0] + 1, last[1]) if last else (1, GENESIS_HASH)
    # Postgres keeps microseconds, so the stored value hashes identically when read back.
    at = now.astimezone(UTC)
    payload: dict[str, Any] = json.loads(canonical_json(dict(data or {})))
    fields = _hashed_fields(
        workspace_id=workspace_id,
        seq=seq,
        at=at,
        actor_type=actor_type,
        actor_id=actor_id,
        action=action,
        target_type=target_type,
        target_id=target_id,
        ip=ip,
        data=payload,
    )
    row = AuditEvent(
        workspace_id=workspace_id,
        seq=seq,
        at=at,
        actor_type=ActorType(actor_type),
        actor_id=actor_id,
        action=action,
        target_type=target_type,
        target_id=target_id,
        ip=ip,
        data=payload,
        prev_hash=prev_hash,
        hash=event_hash(prev_hash, fields),
    )
    db.add(row)
    await db.flush()
    return row


async def verify(db: AsyncSession, workspace_id: UUID) -> VerifyResult:
    """Recompute the whole chain: links, hashes and a gap-free seq."""
    rows = await db.stream_scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == workspace_id)
        .order_by(AuditEvent.seq)
        .execution_options(yield_per=500, populate_existing=True)
    )
    expected_seq = 1
    prev_hash = GENESIS_HASH
    async for row in rows:
        if (
            row.seq != expected_seq
            or row.prev_hash != prev_hash
            or row.hash != event_hash(prev_hash, _row_event(row))
        ):
            return VerifyResult(ok=False, count=expected_seq - 1, first_bad_seq=row.seq)
        prev_hash = row.hash
        expected_seq += 1
    return VerifyResult(ok=True, count=expected_seq - 1, first_bad_seq=None)
