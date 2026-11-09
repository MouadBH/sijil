"""The terminal ledger states (retention lock, layer 3): the Owner's pre-seal removal and the
Owner-approved deletion after retention, enforced by the `document_ledger_immutable` trigger
and the `ck_document_ledger_terminal_consistent` CHECK (0011), through the app role the API
uses and, where it matters, the table owner.

    received | needs_review | filed --(removed_at, removed_by, reason)--> removed (frozen)
    any but removed/deleting/deleted --(approval, retention over)--> deleting
    deleting --(original_removed_at)--> deleted (frozen)
"""

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core.db.models import (
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    User,
    Workspace,
)
from sijil_core.retention import region_local_date

pytestmark = pytest.mark.db

FAR = date(2036, 12, 31)
REASON = "Doublon déposé par erreur (SAMPLE)"
UNSEALED = (DocumentState.received, DocumentState.needs_review, DocumentState.filed)

SEAL = "state = 'sealed', fields_sha256 = repeat('f', 64), sealed_at = now(), sealed_by = :u"
REMOVE = "state = 'removed', removed_at = now(), removed_by = :u, removal_reason = :reason"
APPROVE = "state = 'deleting', deletion_approved_at = now(), deletion_approved_by = :u"
ORIGINAL_GONE = "original_removed_at = now()"
DELETE = "state = 'deleted', deleted_at = now()"


@pytest.fixture
def ws(workspace: Workspace) -> uuid.UUID:
    """Plain ids: the ORM objects expire with each refused statement's rollback."""
    return workspace.id


@pytest.fixture
def uid(user: User) -> uuid.UUID:
    return user.id


def _today() -> date:
    return region_local_date(datetime.now(UTC), "ma-cas")


async def _add(
    db: AsyncSession,
    ws: uuid.UUID,
    *,
    state: DocumentState = DocumentState.filed,
    retention_until: date = FAR,
) -> uuid.UUID:
    sha = uuid.uuid4().hex * 2
    row_id = uuid.uuid4()
    db.add(
        DocumentLedger(
            id=row_id,
            workspace_id=ws,
            sha256=sha,
            md5="d" * 32,
            size_bytes=10,
            content_kind="pdf",
            source=DocumentSource.upload,
            object_key=f"t/{sha}",
            object_version_id="v1",
            state=state,
            ingest_state=IngestState.consumed,
            retention_until=retention_until,
        )
    )
    await db.commit()
    return row_id


async def _set(db: AsyncSession, row_id: uuid.UUID, assignment: str, **params: Any) -> None:
    await db.execute(
        text(f"UPDATE document_ledger SET {assignment} WHERE id = :id"),  # noqa: S608
        {"id": row_id, **params},
    )
    await db.commit()


async def _refused(
    db: AsyncSession, row_id: uuid.UUID, assignment: str, match: str = "ledger.immutable",
    **params: Any,
) -> None:  # fmt: skip
    with pytest.raises(DBAPIError, match=match):
        await _set(db, row_id, assignment, **params)
    await db.rollback()


async def _state(db: AsyncSession, row_id: uuid.UUID) -> str:
    found = await db.scalar(
        text("SELECT state FROM document_ledger WHERE id = :id"), {"id": row_id}
    )
    await db.commit()
    assert isinstance(found, str)
    return found


async def _in_state(
    db: AsyncSession, ws: uuid.UUID, uid: uuid.UUID, state: DocumentState
) -> uuid.UUID:
    """A row brought to `state` the way the services will: the deletion path on a sealed
    row whose retention ended yesterday."""
    yesterday = _today() - timedelta(days=1)
    if state in UNSEALED:
        return await _add(db, ws, state=state)
    if state == DocumentState.removed:
        row_id = await _add(db, ws)
        await _set(db, row_id, REMOVE, u=uid, reason=REASON)
        return row_id
    row_id = await _add(db, ws, retention_until=yesterday)
    await _set(db, row_id, SEAL, u=uid)
    if state == DocumentState.sealed:
        return row_id
    await _set(db, row_id, APPROVE, u=uid)
    if state == DocumentState.deleting:
        return row_id
    await _set(db, row_id, ORIGINAL_GONE)
    await _set(db, row_id, DELETE)
    return row_id


async def test_sealed_row_cannot_become_removed(
    db: AsyncSession, ws: uuid.UUID, uid: uuid.UUID
) -> None:
    """Removal is pre-seal only: a sealed row refuses it even with every removal column."""
    row_id = await _in_state(db, ws, uid, DocumentState.sealed)
    await _refused(db, row_id, REMOVE, u=uid, reason=REASON)
    await _refused(db, row_id, "state = 'removed'")
    assert await _state(db, row_id) == "sealed"


async def test_removal_needs_reason_actor_time(
    db: AsyncSession, ws: uuid.UUID, uid: uuid.UUID
) -> None:
    for state in UNSEALED:
        row_id = await _add(db, ws, state=state)
        params = {"u": uid, "reason": REASON}
        for assignment in (
            "state = 'removed'",
            "state = 'removed', removed_by = :u, removal_reason = :reason",  # no time
            "state = 'removed', removed_at = now(), removal_reason = :reason",  # no actor
            "state = 'removed', removed_at = now(), removed_by = :u",  # no reason
            # The removal columns alone, without the state, are refused too.
            "removed_at = now(), removed_by = :u, removal_reason = :reason",
            "removal_reason = :reason",
        ):
            await _refused(db, row_id, assignment, **params)
        # The reason is 10 to 1000 characters.
        for reason in ("x" * 9, "x" * 1001):
            await _refused(
                db, row_id, REMOVE, match="ck_document_ledger_removal_reason_length",
                u=uid, reason=reason,
            )  # fmt: skip
        await _set(db, row_id, REMOVE, u=uid, reason="x" * 10)
        assert await _state(db, row_id) == "removed", state


async def test_removed_row_is_frozen_except_original_removed_at(
    db: AsyncSession, owner_conn: AsyncConnection, ws: uuid.UUID, uid: uuid.UUID
) -> None:
    row_id = await _in_state(db, ws, uid, DocumentState.removed)
    past = await _add(db, ws, retention_until=_today() - timedelta(days=1))
    await _set(db, past, REMOVE, u=uid, reason=REASON)
    for target in (row_id, past):
        for assignment in (
            "state = 'filed'",
            "state = 'received'",
            "state = 'removed', removed_at = NULL",
            SEAL,
            APPROVE,  # a removed document is never approved for deletion, even after retention
            DELETE,
            "removed_at = removed_at + interval '1 second'",
            "removed_by = NULL",
            "removal_reason = 'Une autre raison SAMPLE'",
            "retention_until = retention_until + 1",
            "ingest_state = 'failed'",
            "paperless_doc_id = 9",
            "deletion_eligible_at = now()",
        ):
            await _refused(db, target, assignment, u=uid)
    # The object-store copy goes, once.
    await _set(db, row_id, ORIGINAL_GONE)
    for assignment in (
        "original_removed_at = NULL",
        "original_removed_at = now() + interval '1 day'",
    ):
        await _refused(db, row_id, assignment)
        with pytest.raises(DBAPIError, match="ledger.immutable"):
            await owner_conn.execute(
                text(f"UPDATE document_ledger SET {assignment} WHERE id = :id"),  # noqa: S608
                {"id": row_id},
            )
    # Re-writing the same values (an ORM flush of an unchanged row) is not a change.
    await _set(db, row_id, "original_removed_at = original_removed_at, state = state")
    assert await _state(db, row_id) == "removed"


async def test_deleting_refused_before_retention_ends(
    db: AsyncSession, ws: uuid.UUID, uid: uuid.UUID
) -> None:
    """Approval needs the retention over in Casablanca: `retention_until` before today. A
    lowered retention is refused anyway, so the past rows are inserted fresh."""
    today = _today()
    for state in (*UNSEALED, DocumentState.sealed):
        current = await _add(db, ws, retention_until=today)
        if state == DocumentState.sealed:
            await _set(db, current, SEAL, u=uid)
        else:
            await _set(db, current, f"state = '{state}'")
        await _refused(db, current, APPROVE, u=uid)
        assert await _state(db, current) == state

        past = await _add(db, ws, retention_until=today - timedelta(days=1))
        if state == DocumentState.sealed:
            await _set(db, past, SEAL, u=uid)
        else:
            await _set(db, past, f"state = '{state}'")
        # The approval is the Owner and the time, together, with the state.
        for assignment in (
            "state = 'deleting'",
            "state = 'deleting', deletion_approved_at = now()",
            "state = 'deleting', deletion_approved_by = :u",
            "deletion_approved_at = now(), deletion_approved_by = :u",
        ):
            await _refused(db, past, assignment, u=uid)
        await _set(db, past, APPROVE, u=uid)
        assert await _state(db, past) == "deleting", state


async def test_deleted_needs_original_removed_at(
    db: AsyncSession, ws: uuid.UUID, uid: uuid.UUID
) -> None:
    row_id = await _in_state(db, ws, uid, DocumentState.deleting)
    await _refused(db, row_id, DELETE)
    await _refused(db, row_id, "state = 'deleted'")
    # From deleting the only way is forward.
    for assignment in ("state = 'sealed'", "state = 'filed'", REMOVE):
        await _refused(db, row_id, assignment, u=uid, reason=REASON)
    await _set(db, row_id, ORIGINAL_GONE)
    await _refused(db, row_id, "state = 'deleted'", match="ck_document_ledger_terminal_consistent")
    await _set(db, row_id, DELETE)
    assert await _state(db, row_id) == "deleted"
    # `deleted` is frozen.
    for assignment in (
        "state = 'deleting'",
        "state = 'sealed'",
        "deleted_at = deleted_at + interval '1 second'",
        "original_removed_at = NULL",
        "deletion_approved_by = NULL",
        "ingest_state = 'failed'",
        "paperless_doc_id = 9",
        "retention_until = retention_until + 1",
    ):
        await _refused(db, row_id, assignment)


async def test_deleted_in_one_update_with_original_removed_at(
    db: AsyncSession, ws: uuid.UUID, uid: uuid.UUID
) -> None:
    row_id = await _in_state(db, ws, uid, DocumentState.deleting)
    await _set(db, row_id, f"{DELETE}, {ORIGINAL_GONE}")
    assert await _state(db, row_id) == "deleted"


async def test_original_removed_at_only_in_a_terminal_state(
    db: AsyncSession, ws: uuid.UUID, uid: uuid.UUID
) -> None:
    for state in (*UNSEALED, DocumentState.sealed):
        row_id = await _in_state(db, ws, uid, state)
        await _refused(db, row_id, ORIGINAL_GONE)


async def test_seal_never_lands_on_a_deleting_row(
    db: AsyncSession, ws: uuid.UUID, uid: uuid.UUID
) -> None:
    """An unsealed row approved for deletion can no longer be sealed: the relaxed seal CHECK
    accepts `sealed_at` on a deleting row, the trigger only lets it be written with `sealed`."""
    row_id = await _add(db, ws, retention_until=_today() - timedelta(days=1))
    await _set(db, row_id, APPROVE, u=uid)
    await _refused(
        db, row_id, "fields_sha256 = repeat('f', 64), sealed_at = now(), sealed_by = :u", u=uid
    )
    await _refused(db, row_id, SEAL, u=uid)


TOMBSTONE = {
    "sha256 = repeat('b', 64)": "another hash",
    "object_key = 'other'": "another key",
    "object_version_id = 'v2'": "another version",
    "fields_sha256 = repeat('e', 64)": "another fields hash",
    "sealed_at = NULL": "the seal cleared",
}


@pytest.mark.parametrize(
    "state", [DocumentState.removed, DocumentState.deleting, DocumentState.deleted]
)
async def test_tombstone_hash_never_changes(
    db: AsyncSession, owner_conn: AsyncConnection, ws: uuid.UUID, uid: uuid.UUID,
    state: DocumentState,
) -> None:  # fmt: skip
    row_id = await _in_state(db, ws, uid, state)
    before = (
        await owner_conn.execute(
            text(
                "SELECT sha256, object_key, object_version_id FROM document_ledger WHERE id = :id"
            ),
            {"id": row_id},
        )
    ).one()
    for assignment in sorted(TOMBSTONE):
        if state == DocumentState.removed and assignment == "sealed_at = NULL":
            continue  # never sealed: a no-op
        await _refused(db, row_id, assignment)
        with pytest.raises(DBAPIError, match="ledger.immutable"):
            await owner_conn.execute(
                text(f"UPDATE document_ledger SET {assignment} WHERE id = :id"),  # noqa: S608
                {"id": row_id},
            )
    after = (
        await owner_conn.execute(
            text(
                "SELECT sha256, object_key, object_version_id FROM document_ledger WHERE id = :id"
            ),
            {"id": row_id},
        )
    ).one()
    assert tuple(after) == tuple(before)


@pytest.mark.parametrize("state", list(DocumentState))
async def test_ledger_row_never_deleted(
    db: AsyncSession, owner_conn: AsyncConnection, ws: uuid.UUID, uid: uuid.UUID,
    state: DocumentState,
) -> None:  # fmt: skip
    row_id = await _in_state(db, ws, uid, state)
    assert await _state(db, row_id) == state
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await db.execute(text("DELETE FROM document_ledger WHERE id = :id"), {"id": row_id})
    await db.rollback()
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await owner_conn.execute(text("DELETE FROM document_ledger WHERE id = :id"), {"id": row_id})
    assert await _state(db, row_id) == state


# Fix round 1: the region date ignores the session; no INSERT lands in a terminal state. ------


@pytest.mark.parametrize(
    "setting",
    [
        "SET sijil.region_tz = 'UTC-167'",
        "SET TIME ZONE 'UTC-167'",
        "SET sijil.region_tz = 'Pacific/Kiritimati'",
    ],
)
async def test_session_cannot_move_the_region_date(
    db: AsyncSession, ws: uuid.UUID, uid: uuid.UUID, setting: str
) -> None:
    """A session that pretends to be a week ahead still cannot approve a deletion whose
    retention ends today in Casablanca: the zone is compiled into `sijil_region_today()`."""
    row_id = await _add(db, ws, retention_until=_today())
    await db.execute(text(setting))
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await db.execute(
            text(f"UPDATE document_ledger SET {APPROVE} WHERE id = :id"),  # noqa: S608
            {"id": row_id, "u": uid},
        )
    await db.rollback()
    await db.execute(text(setting))
    assert await db.scalar(text("SELECT sijil_region_today()")) == _today()
    await db.rollback()
    assert await _state(db, row_id) == "filed"


INSERT = (
    "INSERT INTO document_ledger (id, workspace_id, sha256, md5, size_bytes, content_kind, "
    "source, object_key, object_version_id, state, ingest_state, retention_until, ingested_at, "
    "removed_at, removed_by, removal_reason, deletion_approved_at, deletion_approved_by, "
    "deleted_at, original_removed_at) VALUES (:id, :ws, :sha, repeat('d', 32), 10, 'pdf', "
    "'upload', :key, 'v1', :state, 'consumed', '2020-12-31', now(), "
    ":removed_at, :removed_by, :reason, :approved_at, :approved_by, :deleted_at, :gone_at)"
)


def _insert_params(ws: uuid.UUID, state: str, **over: Any) -> dict[str, Any]:
    sha = uuid.uuid4().hex * 2
    params: dict[str, Any] = {
        "id": uuid.uuid4(),
        "ws": ws,
        "sha": sha,
        "key": f"t/{sha}",
        "state": state,
        "removed_at": None,
        "removed_by": None,
        "reason": None,
        "approved_at": None,
        "approved_by": None,
        "deleted_at": None,
        "gone_at": None,
    }
    return params | over


def _terminal_insert(ws: uuid.UUID, uid: uuid.UUID, state: DocumentState) -> dict[str, Any]:
    """A row in `state` with every column that state carries: only the INSERT guard refuses."""
    now = datetime.now(UTC)
    if state == DocumentState.removed:
        return _insert_params(ws, state, removed_at=now, removed_by=uid, reason=REASON)
    over: dict[str, Any] = {"approved_at": now, "approved_by": uid}
    if state == DocumentState.deleted:
        over |= {"deleted_at": now, "gone_at": now}
    return _insert_params(ws, state, **over)


@pytest.mark.parametrize(
    "state", [DocumentState.removed, DocumentState.deleting, DocumentState.deleted]
)
async def test_insert_never_lands_in_a_terminal_state(
    db: AsyncSession, owner_conn: AsyncConnection, ws: uuid.UUID, uid: uuid.UUID,
    state: DocumentState,
) -> None:  # fmt: skip
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await db.execute(text(INSERT), _terminal_insert(ws, uid, state))
    await db.rollback()
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await owner_conn.execute(text(INSERT), _terminal_insert(ws, uid, state))
    # Control: the same INSERT in a live state is accepted.
    await db.execute(text(INSERT), _insert_params(ws, "filed"))
    await db.commit()


@pytest.mark.parametrize("column", ["removed_by", "reason", "approved_by", "approved_at"])
async def test_terminal_columns_never_stray_onto_a_live_row(
    db: AsyncSession, ws: uuid.UUID, uid: uuid.UUID, column: str
) -> None:
    """CHECK: the removal trio and the approval pair are all set or all NULL, so none of them
    sits alone on a live row (an INSERT, which the UPDATE trigger never sees)."""
    value = {"removed_by": uid, "reason": REASON, "approved_by": uid}.get(column, datetime.now(UTC))
    with pytest.raises(DBAPIError, match="ck_document_ledger_terminal_consistent"):
        await db.execute(text(INSERT), _insert_params(ws, "filed", **{column: value}))
    await db.rollback()


_NOW = datetime(2026, 11, 5, 10, 0, tzinfo=UTC)
CHECK_ONLY = {
    "deleted without original_removed_at": {
        "state": "deleted", "approved_at": _NOW, "deleted_at": _NOW,
    },
    "removed without removed_by": {"state": "removed", "removed_at": _NOW, "reason": REASON},
    "removed without removal_reason": {"state": "removed", "removed_at": _NOW},
    "deleting without deletion_approved_by": {"state": "deleting", "approved_at": _NOW},
}  # fmt: skip


@pytest.mark.parametrize("case", sorted(CHECK_ONLY))
async def test_check_alone_requires_the_terminal_columns(
    migrated_db: Any, ws: uuid.UUID, uid: uuid.UUID, case: str
) -> None:
    """With the INSERT guard disabled (the DDL is rolled back), the CHECK by itself still
    refuses a terminal row missing one of its columns."""
    params = dict(CHECK_ONLY[case])
    if case.startswith("deleted"):
        params["approved_by"] = uid
    if case == "removed without removal_reason":
        params["removed_by"] = uid
    state = params.pop("state")
    engine = create_async_engine(migrated_db.owner_url)
    try:
        async with engine.connect() as conn:
            trans = await conn.begin()
            await conn.execute(
                text("ALTER TABLE document_ledger DISABLE TRIGGER document_ledger_insert_guard")
            )
            with pytest.raises(DBAPIError, match="ck_document_ledger_terminal_consistent"):
                await conn.execute(text(INSERT), _insert_params(ws, state, **params))
            await trans.rollback()
    finally:
        await engine.dispose()
