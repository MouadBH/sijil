import re
import uuid
from collections.abc import Awaitable, Callable
from datetime import date
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
    # NULL -> value: sealing is allowed, once.
    await owner_conn.execute(
        text("UPDATE document_ledger SET sealed_at = now(), sealed_by = :u"), {"u": user.id}
    )
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
