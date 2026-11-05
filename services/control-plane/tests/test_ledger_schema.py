import os
import re
import subprocess
import sys
import uuid
from collections.abc import Awaitable, Callable
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from sijil_core.db.base import utcnow
from sijil_core.db.models import (
    ActorType,
    AuditEvent,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    Organisation,
    OrgType,
    User,
    Workspace,
    WorkspaceState,
)

pytestmark = pytest.mark.db

MakeActor = Callable[[str], Awaitable[Any]]
CP_DIR = Path(__file__).parents[1]
SHA = "a" * 64
IMMUTABLE_UPDATES = {
    "id": "gen_random_uuid()",
    "workspace_id": "gen_random_uuid()",
    "sha256": f"'{'b' * 64}'",
    "md5": f"'{'c' * 32}'",
    "size_bytes": "99",
    "content_kind": "'png'",
    "source": "'email'",
    "source_email_id": "gen_random_uuid()",
    "object_key": "'other'",
    "object_version_id": "'v2'",
    "ingested_at": "now() + interval '1 day'",
    "uploaded_by": "gen_random_uuid()",
}


def ledger(workspace_id: uuid.UUID, sha: str = SHA, **over: Any) -> DocumentLedger:
    values: dict[str, Any] = {
        "workspace_id": workspace_id,
        "sha256": sha,
        "md5": "d" * 32,
        "size_bytes": 10,
        "content_kind": "pdf",
        "source": DocumentSource.upload,
        "object_key": f"t/{sha}",
        "object_version_id": "v1",
        "retention_until": date(2036, 12, 31),
    }
    return DocumentLedger(**(values | over))


async def test_ledger_sha_immutable(db: AsyncSession, workspace: Workspace) -> None:
    row = ledger(workspace.id)
    db.add(row)
    await db.commit()
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await db.execute(
            update(DocumentLedger).where(DocumentLedger.id == row.id).values(sha256="f" * 64)
        )
        await db.commit()


@pytest.mark.parametrize("column", sorted(IMMUTABLE_UPDATES))
async def test_every_immutable_column_is_guarded(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace, column: str
) -> None:
    db.add(ledger(workspace.id))
    await db.commit()
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await owner_conn.execute(
            text(f"UPDATE document_ledger SET {column} = {IMMUTABLE_UPDATES[column]}")  # noqa: S608
        )


async def test_mutable_columns_can_change(db: AsyncSession, workspace: Workspace) -> None:
    row = ledger(workspace.id)
    db.add(row)
    await db.commit()
    row.state = DocumentState.filed
    row.paperless_doc_id = 7
    row.retention_until = date(2040, 12, 31)
    await db.commit()


async def test_ledger_row_delete_rejected_by_db(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace
) -> None:
    db.add(ledger(workspace.id))
    await db.commit()
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await db.execute(text("DELETE FROM document_ledger"))
    await db.rollback()
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await owner_conn.execute(text("DELETE FROM document_ledger"))


async def test_retention_can_only_increase_in_db(db: AsyncSession, workspace: Workspace) -> None:
    row = ledger(workspace.id)
    db.add(row)
    await db.commit()
    row.retention_until = date(2037, 12, 31)
    await db.commit()
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await db.execute(
            update(DocumentLedger)
            .where(DocumentLedger.id == row.id)
            .values(retention_until=date(2030, 12, 31))
        )
        await db.commit()
    await db.rollback()


async def test_seal_fields_set_once_then_frozen(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace, user: User
) -> None:
    row = ledger(workspace.id)
    db.add(row)
    await db.commit()
    # NULL -> value: sealing is allowed, once (0008: with the state and the hash, together).
    await owner_conn.execute(text(SEAL_SQL), {"u": user.id})
    other = User(email=f"{uuid.uuid4().hex[:8]}@example.com", name="Other", password_hash="x")
    db.add(other)
    await db.commit()
    for assignment, params in (
        ("sealed_at = sealed_at + interval '1 second'", {}),
        ("sealed_at = NULL", {}),
        ("sealed_by = :u", {"u": other.id}),
        ("sealed_by = NULL", {}),
    ):
        with pytest.raises(DBAPIError, match="ledger.immutable"):
            await owner_conn.execute(
                text(f"UPDATE document_ledger SET {assignment}"),  # noqa: S608
                params,
            )
    # Re-writing the same values (an ORM flush of an unchanged row) is not a change.
    await owner_conn.execute(text("UPDATE document_ledger SET sealed_at = sealed_at"))


SEAL_SQL = (
    "UPDATE document_ledger SET state = 'sealed', fields_sha256 = repeat('f', 64), "
    "sealed_at = now(), sealed_by = :u, retention_until = '2037-12-31'"
)
SEALED_FROZEN = {
    "fields_sha256 = repeat('e', 64)": "another hash",
    "fields_sha256 = NULL": "the hash cleared",
    "state = 'filed'": "back to filed",
    "state = 'removed'": "removed after seal",
    "retention_until = '2040-12-31'": "retention raised after seal",
    "retention_until = '2030-12-31'": "retention lowered after seal",
}


@pytest.mark.parametrize("assignment", sorted(SEALED_FROZEN))
async def test_sealed_row_hash_state_and_retention_frozen(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace, user: User, assignment: str
) -> None:
    """0008: once sealed, `fields_sha256`, `state` and `retention_until` never change, even for
    the table owner."""
    db.add(ledger(workspace.id))
    await db.commit()
    await owner_conn.execute(text(SEAL_SQL), {"u": user.id})
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await owner_conn.execute(text(f"UPDATE document_ledger SET {assignment}"))  # noqa: S608
    # The app role is refused the same way.
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await db.execute(text(f"UPDATE document_ledger SET {assignment}"))  # noqa: S608
    await db.rollback()


async def test_unsealed_row_hash_is_set_once(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace, user: User
) -> None:
    """`fields_sha256` goes NULL -> value once, at seal; an unsealed row keeps its mutable
    columns (state, retention upwards)."""
    db.add(ledger(workspace.id))
    await db.commit()
    await owner_conn.execute(
        text("UPDATE document_ledger SET state = 'needs_review', retention_until = '2037-12-31'")
    )
    await owner_conn.execute(text(SEAL_SQL), {"u": user.id})
    sealed = (
        await owner_conn.execute(
            text("SELECT state, fields_sha256, sealed_by, retention_until FROM document_ledger")
        )
    ).one()
    assert tuple(sealed) == ("sealed", "f" * 64, user.id, date(2037, 12, 31))


SEAL_INCONSISTENT = {
    "state = 'sealed'": "sealed state without sealed_at",
    "sealed_at = now(), state = 'sealed'": "sealed without a hash",
    "fields_sha256 = repeat('f', 64)": "a hash on an unsealed row",
    "sealed_by = :u": "sealed_by without sealed_at",
    "state = 'sealed', fields_sha256 = repeat('f', 64)": "sealed state and hash, no sealed_at",
}


@pytest.mark.parametrize("assignment", sorted(SEAL_INCONSISTENT))
async def test_seal_columns_consistent(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace, user: User, assignment: str
) -> None:
    """CHECK ck_document_ledger_seal_consistent: state sealed iff sealed_at iff fields_sha256,
    and sealed_by only on a sealed row."""
    db.add(ledger(workspace.id))
    await db.commit()
    with pytest.raises(IntegrityError, match="ck_document_ledger_seal_consistent"):
        await owner_conn.execute(
            text(f"UPDATE document_ledger SET {assignment}"),  # noqa: S608
            {"u": user.id} if ":u" in assignment else {},
        )


async def test_sealed_at_without_the_sealed_state_refused_by_trigger(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace
) -> None:
    """0011: `sealed_at` is written only together with the state `sealed` (the trigger refuses
    it before the CHECK, which now accepts a seal on a deleting row)."""
    db.add(ledger(workspace.id))
    await db.commit()
    with pytest.raises(DBAPIError, match="ledger.immutable"):
        await owner_conn.execute(text("UPDATE document_ledger SET sealed_at = now()"))


async def test_seal_consistency_check_on_insert(db: AsyncSession, workspace: Workspace) -> None:
    db.add(ledger(workspace.id, state=DocumentState.sealed))
    with pytest.raises(IntegrityError, match="ck_document_ledger_seal_consistent"):
        await db.commit()
    await db.rollback()


async def test_0008_upgrade_keeps_existing_unsealed_rows(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace, migrated_db: Any
) -> None:
    """Rows written before 0008 (unsealed, no hash) pass its CHECK: downgrade to 0007 with rows
    of every unsealed state present, then upgrade again."""
    for n, state in enumerate(
        (DocumentState.received, DocumentState.needs_review, DocumentState.filed), start=1
    ):
        db.add(ledger(workspace.id, sha=f"{n:064x}", state=state))
    await db.commit()
    await db.close()
    _alembic(migrated_db.owner_url, "downgrade", "0007")
    try:
        names = await _constraint_names(owner_conn)
        assert "ck_document_ledger_seal_consistent" not in names
        assert "ck_document_ledger_object_locked_after_seal" not in names
        assert "object_locked_at" not in await _columns(owner_conn)
        # 0006's function is back: a sealed row's hash may change again (0008 forbade it),
        # while sealed_at stays frozen.
        sealed = f"{1:064x}"
        await owner_conn.execute(
            text(
                "UPDATE document_ledger SET state = 'sealed', fields_sha256 = repeat('f', 64), "
                "sealed_at = now() WHERE sha256 = :sha"
            ),
            {"sha": sealed},
        )
        await owner_conn.execute(
            text("UPDATE document_ledger SET fields_sha256 = repeat('e', 64) WHERE sha256 = :sha"),
            {"sha": sealed},
        )
        with pytest.raises(DBAPIError, match="ledger.immutable"):
            await owner_conn.execute(
                text("UPDATE document_ledger SET sealed_at = NULL WHERE sha256 = :sha"),
                {"sha": sealed},
            )
    finally:
        _alembic(migrated_db.owner_url, "upgrade", "head")
    names = await _constraint_names(owner_conn)
    assert {
        "ck_document_ledger_seal_consistent",
        "ck_document_ledger_object_locked_after_seal",
    } <= names
    assert "object_locked_at" in await _columns(owner_conn)


async def _columns(conn: AsyncConnection) -> set[str]:
    rows = await conn.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'document_ledger'"
        )
    )
    return {r[0] for r in rows}


async def test_object_locked_at_set_once_then_frozen(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace, user: User
) -> None:
    """0009: the worker records the COMPLIANCE lock once; nobody moves or clears it."""
    db.add(ledger(workspace.id))
    await db.commit()
    await owner_conn.execute(text(SEAL_SQL), {"u": user.id})
    await owner_conn.execute(text("UPDATE document_ledger SET object_locked_at = now()"))
    for assignment in (
        "object_locked_at = object_locked_at + interval '1 second'",
        "object_locked_at = NULL",
    ):
        with pytest.raises(DBAPIError, match="ledger.immutable"):
            await owner_conn.execute(text(f"UPDATE document_ledger SET {assignment}"))  # noqa: S608
    await owner_conn.execute(text("UPDATE document_ledger SET object_locked_at = object_locked_at"))


async def test_object_locked_at_needs_a_sealed_row(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace
) -> None:
    db.add(ledger(workspace.id))
    await db.commit()
    with pytest.raises(IntegrityError, match="ck_document_ledger_object_locked_after_seal"):
        await owner_conn.execute(text("UPDATE document_ledger SET object_locked_at = now()"))


async def _constraint_names(conn: AsyncConnection) -> set[str]:
    rows = await conn.execute(
        text("SELECT conname FROM pg_constraint WHERE conrelid = 'document_ledger'::regclass")
    )
    return {r[0] for r in rows}


def _alembic(owner_url: str, *args: str) -> None:
    subprocess.run(  # noqa: S603
        [sys.executable, "-m", "alembic", *args],
        cwd=CP_DIR,
        env={
            **os.environ,
            "SIJIL_MIGRATION_DATABASE_URL": owner_url,
            "SIJIL_REGION": "ma-cas",
            "SIJIL_ENVIRONMENT": "test",
            "SIJIL_APP_DB_ROLE": "sijil_test_app",
        },
        check=True,
    )


async def test_ledger_unique_per_workspace_sha(db: AsyncSession, workspace: Workspace) -> None:
    other = Organisation(name="Other", type=OrgType.business, country="MA", region="ma-cas")
    db.add(other)
    await db.flush()
    slug = f"ws-{uuid.uuid4().hex[:8]}"
    second = Workspace(
        org_id=other.id,
        name="Other",
        slug=slug,
        state=WorkspaceState.active,
        db_name=f"paperless_{slug}",
        index_db_name=f"index_{slug}",
        kms_key_id=f"kek-{slug}",
    )
    db.add(second)
    await db.flush()
    db.add_all([ledger(workspace.id), ledger(second.id)])
    await db.commit()
    db.add(ledger(workspace.id))
    with pytest.raises(IntegrityError):
        await db.commit()


def audit(workspace_id: uuid.UUID) -> AuditEvent:
    return AuditEvent(
        workspace_id=workspace_id,
        seq=1,
        at=utcnow(),
        actor_type=ActorType.system,
        action="document.uploaded",
        data={},
        prev_hash="0" * 64,
        hash="e" * 64,
    )


async def test_audit_update_and_delete_rejected(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace
) -> None:
    db.add(audit(workspace.id))
    await db.commit()
    # The app role has no grant at all.
    for sql in ("UPDATE audit_event SET action = 'x'", "DELETE FROM audit_event"):
        with pytest.raises(DBAPIError, match="permission denied"):
            await db.execute(text(sql))
        await db.rollback()
    # The trigger binds the owner too.
    for sql in ("UPDATE audit_event SET action = 'x'", "DELETE FROM audit_event"):
        with pytest.raises(DBAPIError, match="audit.append_only"):
            await owner_conn.execute(text(sql))


async def test_workspace_gets_inbound_token_and_email(make_actor: MakeActor) -> None:
    alice = await make_actor("alice@example.com")
    r = await alice.client.post(
        "/v1/orgs", json={"name": "Atlas SARL", "type": "business", "country": "MA"}
    )
    assert r.status_code == 202, r.text
    ws = r.json()["workspace"]
    assert re.fullmatch(r"[a-z0-9-]+\.[a-z2-7]{10}@in\.sijil\.local", ws["inbound_email"])
    got = await alice.client.get(f"/v1/workspaces/{ws['id']}")
    assert got.json()["inbound_email"] == ws["inbound_email"]


async def test_0009_downgrade_to_0008_keeps_the_0008_guards(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace, user: User,
    migrated_db: Any,
) -> None:  # fmt: skip
    """Down to 0008 exactly: `object_locked_at` and its CHECK are gone, the sealed row's hash,
    state and retention are still frozen and the seal CHECK still holds."""
    db.add(ledger(workspace.id))
    db.add(ledger(workspace.id, sha=f"{9:064x}"))
    await db.commit()
    await db.close()
    await owner_conn.execute(text(SEAL_SQL + " WHERE sha256 = :sha"), {"u": user.id, "sha": SHA})
    _alembic(migrated_db.owner_url, "downgrade", "0008")
    try:
        assert "object_locked_at" not in await _columns(owner_conn)
        names = await _constraint_names(owner_conn)
        assert "ck_document_ledger_object_locked_after_seal" not in names
        assert "ck_document_ledger_seal_consistent" in names
        for assignment in SEALED_FROZEN:
            with pytest.raises(DBAPIError, match="ledger.immutable"):
                await owner_conn.execute(
                    text(f"UPDATE document_ledger SET {assignment} WHERE sha256 = :sha"),  # noqa: S608
                    {"sha": SHA},
                )
        with pytest.raises(IntegrityError, match="ck_document_ledger_seal_consistent"):
            await owner_conn.execute(
                text("UPDATE document_ledger SET state = 'sealed' WHERE sha256 = :sha"),
                {"sha": f"{9:064x}"},
            )
    finally:
        _alembic(migrated_db.owner_url, "upgrade", "head")
    assert "object_locked_at" in await _columns(owner_conn)


TERMINAL_COLUMNS = {
    "removed_at",
    "removed_by",
    "removal_reason",
    "deletion_eligible_at",
    "deletion_approved_at",
    "deletion_approved_by",
    "deleted_at",
    "original_removed_at",
}


async def _has_region_today(conn: AsyncConnection) -> bool:
    found = await conn.scalar(
        text("SELECT count(*) FROM pg_proc WHERE proname = 'sijil_region_today'")
    )
    return bool(found)


async def _has_insert_guard(conn: AsyncConnection) -> bool:
    trigger = await conn.scalar(
        text("SELECT count(*) FROM pg_trigger WHERE tgname = 'document_ledger_insert_guard'")
    )
    function = await conn.scalar(
        text("SELECT count(*) FROM pg_proc WHERE proname = 'document_ledger_insert_guard'")
    )
    return bool(trigger) and bool(function)


async def test_0011_downgrade_restores_0009_guard(
    db: AsyncSession, owner_conn: AsyncConnection, workspace: Workspace, user: User,
    migrated_db: Any,
) -> None:  # fmt: skip
    """Down to 0010 (whose function is 0009's): the terminal columns, CHECKs, the INSERT guard
    and `sijil_region_today()` are gone, the sealed row is frozen again in every state (no way to
    `deleting`), and the seal CHECK is 0008's (a seal only on a sealed row)."""
    db.add(ledger(workspace.id))
    db.add(ledger(workspace.id, sha=f"{9:064x}"))
    await db.commit()
    await db.close()
    await owner_conn.execute(text(SEAL_SQL + " WHERE sha256 = :sha"), {"u": user.id, "sha": SHA})
    _alembic(migrated_db.owner_url, "downgrade", "0010")
    try:
        assert not TERMINAL_COLUMNS & await _columns(owner_conn)
        names = await _constraint_names(owner_conn)
        assert "ck_document_ledger_terminal_consistent" not in names
        assert "ck_document_ledger_removal_reason_length" not in names
        assert {
            "ck_document_ledger_seal_consistent",
            "ck_document_ledger_object_locked_after_seal",
        } <= names
        assert not await _has_region_today(owner_conn)
        assert not await _has_insert_guard(owner_conn)
        for assignment in (*SEALED_FROZEN, "state = 'deleting'", "state = 'deleted'"):
            with pytest.raises(DBAPIError, match="ledger.immutable"):
                await owner_conn.execute(
                    text(f"UPDATE document_ledger SET {assignment} WHERE sha256 = :sha"),  # noqa: S608
                    {"sha": SHA},
                )
        # 0009's trigger lets an unsealed row change state; 0008's CHECK refuses the seal on it.
        with pytest.raises(IntegrityError, match="ck_document_ledger_seal_consistent"):
            await owner_conn.execute(
                text(
                    "UPDATE document_ledger SET state = 'deleting', sealed_at = now(), "
                    "fields_sha256 = repeat('f', 64) WHERE sha256 = :sha"
                ),
                {"sha": f"{9:064x}"},
            )
    finally:
        _alembic(migrated_db.owner_url, "upgrade", "head")
    assert TERMINAL_COLUMNS <= await _columns(owner_conn)
    assert await _has_region_today(owner_conn)
    assert await _has_insert_guard(owner_conn)
