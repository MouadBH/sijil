"""document ledger, audit log and integrity alerts, with database-level immutability

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-01
"""

import os
import re
import secrets
from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ROLE_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_TOKEN_ALPHABET = "abcdefghijklmnopqrstuvwxyz234567"  # noqa: S105  (base32, not a secret)


def _app_role() -> str:
    role = os.environ.get("SIJIL_APP_DB_ROLE", "sijil_cp_app")
    if not _ROLE_RE.fullmatch(role):
        raise RuntimeError("SIJIL_APP_DB_ROLE is not a valid role name")
    return role


def _tstz(name: str, nullable: bool = True) -> sa.Column[datetime]:
    return sa.Column(name, sa.DateTime(timezone=True), nullable=nullable)


def upgrade() -> None:
    # Inbound email token: backfill existing workspaces, then make it required and unique.
    op.add_column("workspace", sa.Column("inbound_token", sa.String(10), nullable=True))
    conn = op.get_bind()
    for (workspace_id,) in conn.execute(sa.text("SELECT id FROM workspace")).all():
        token = "".join(secrets.choice(_TOKEN_ALPHABET) for _ in range(10))
        conn.execute(
            sa.text("UPDATE workspace SET inbound_token = :t WHERE id = :i"),
            {"t": token, "i": workspace_id},
        )
    op.alter_column("workspace", "inbound_token", nullable=False)
    op.create_unique_constraint("uq_workspace_inbound_token", "workspace", ["inbound_token"])

    op.create_table(
        "document_ledger",
        sa.Column("id", sa.Uuid, nullable=False),
        sa.Column("workspace_id", sa.Uuid, nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("md5", sa.String(32), nullable=False),
        sa.Column("size_bytes", sa.BigInteger, nullable=False),
        sa.Column("content_kind", sa.String(32), nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("source_email_id", sa.Uuid, nullable=True),
        sa.Column("uploaded_by", sa.Uuid, nullable=True),
        sa.Column("object_key", sa.String, nullable=False),
        sa.Column("object_version_id", sa.String, nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("ingest_state", sa.String(32), nullable=False),
        sa.Column("ingest_error_code", sa.String, nullable=True),
        sa.Column("paperless_task_id", sa.String, nullable=True),
        sa.Column("paperless_doc_id", sa.Integer, nullable=True),
        sa.Column("retention_until", sa.Date, nullable=False),
        _tstz("ingested_at", nullable=False),
        sa.Column("fields_sha256", sa.String(64), nullable=True),
        _tstz("sealed_at"),
        sa.Column("sealed_by", sa.Uuid, nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_document_ledger"),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspace.id"], name="fk_document_ledger_workspace_id_workspace"
        ),
        sa.ForeignKeyConstraint(
            ["source_email_id"],
            ["document_ledger.id"],
            name="fk_document_ledger_source_email_id_document_ledger",
        ),
        sa.ForeignKeyConstraint(
            ["uploaded_by"], ["user_account.id"], name="fk_document_ledger_uploaded_by_user_account"
        ),
        sa.ForeignKeyConstraint(
            ["sealed_by"], ["user_account.id"], name="fk_document_ledger_sealed_by_user_account"
        ),
        sa.UniqueConstraint("workspace_id", "sha256", name="uq_document_ledger_workspace_sha256"),
    )
    op.create_index(
        "ix_document_ledger_workspace_ingested",
        "document_ledger",
        ["workspace_id", sa.text("ingested_at DESC")],
    )

    op.create_table(
        "audit_event",
        sa.Column("id", sa.Uuid, nullable=False),
        sa.Column("workspace_id", sa.Uuid, nullable=False),
        sa.Column("seq", sa.Integer, nullable=False),
        _tstz("at", nullable=False),
        sa.Column("actor_type", sa.String(32), nullable=False),
        sa.Column("actor_id", sa.String, nullable=True),
        sa.Column("action", sa.String, nullable=False),
        sa.Column("target_type", sa.String, nullable=True),
        sa.Column("target_id", sa.String, nullable=True),
        sa.Column("ip", sa.String, nullable=True),
        sa.Column("data", JSONB, nullable=False),
        sa.Column("prev_hash", sa.String(64), nullable=False),
        sa.Column("hash", sa.String(64), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_audit_event"),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspace.id"], name="fk_audit_event_workspace_id_workspace"
        ),
        sa.UniqueConstraint("workspace_id", "seq", name="uq_audit_event_workspace_seq"),
        sa.UniqueConstraint("hash", name="uq_audit_event_hash"),
    )

    op.create_table(
        "integrity_alert",
        sa.Column("id", sa.Uuid, nullable=False),
        sa.Column("workspace_id", sa.Uuid, nullable=False),
        sa.Column("ledger_id", sa.Uuid, nullable=False),
        sa.Column("kind", sa.String(64), nullable=False),
        sa.Column("detail", JSONB, nullable=False),
        _tstz("detected_at", nullable=False),
        _tstz("resolved_at"),
        sa.PrimaryKeyConstraint("id", name="pk_integrity_alert"),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspace.id"], name="fk_integrity_alert_workspace_id_workspace"
        ),
        sa.ForeignKeyConstraint(
            ["ledger_id"],
            ["document_ledger.id"],
            name="fk_integrity_alert_ledger_id_document_ledger",
        ),
    )
    op.create_index("ix_integrity_alert_workspace_id", "integrity_alert", ["workspace_id"])
    op.create_index("ix_integrity_alert_ledger_id", "integrity_alert", ["ledger_id"])

    # Retention layer 3: the ledger's identity columns never change, retention never shrinks,
    # and no row is ever deleted. The triggers bind the table owner as well.
    op.execute("""
        CREATE FUNCTION document_ledger_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'ledger.immutable';
          END IF;
          IF NEW.workspace_id IS DISTINCT FROM OLD.workspace_id
             OR NEW.sha256 IS DISTINCT FROM OLD.sha256
             OR NEW.md5 IS DISTINCT FROM OLD.md5
             OR NEW.size_bytes IS DISTINCT FROM OLD.size_bytes
             OR NEW.content_kind IS DISTINCT FROM OLD.content_kind
             OR NEW.source IS DISTINCT FROM OLD.source
             OR NEW.source_email_id IS DISTINCT FROM OLD.source_email_id
             OR NEW.object_key IS DISTINCT FROM OLD.object_key
             OR NEW.object_version_id IS DISTINCT FROM OLD.object_version_id
             OR NEW.ingested_at IS DISTINCT FROM OLD.ingested_at
             OR NEW.uploaded_by IS DISTINCT FROM OLD.uploaded_by
             OR NEW.retention_until IS NULL
             OR NEW.retention_until < OLD.retention_until THEN
            RAISE EXCEPTION 'ledger.immutable';
          END IF;
          RETURN NEW;
        END $$
    """)
    op.execute("""
        CREATE TRIGGER document_ledger_immutable
        BEFORE UPDATE OR DELETE ON document_ledger
        FOR EACH ROW EXECUTE FUNCTION document_ledger_immutable()
    """)

    # Append-only audit log: no grant for the app role, and a trigger for everyone else.
    op.execute(f'REVOKE UPDATE, DELETE ON audit_event FROM "{_app_role()}"')
    op.execute("""
        CREATE FUNCTION audit_event_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'audit.append_only';
        END $$
    """)
    op.execute("""
        CREATE TRIGGER audit_event_append_only
        BEFORE UPDATE OR DELETE ON audit_event
        FOR EACH STATEMENT EXECUTE FUNCTION audit_event_append_only()
    """)


def downgrade() -> None:
    op.drop_table("integrity_alert")
    op.drop_table("audit_event")
    op.execute("DROP FUNCTION audit_event_append_only()")
    op.drop_table("document_ledger")
    op.execute("DROP FUNCTION document_ledger_immutable()")
    op.drop_constraint("uq_workspace_inbound_token", "workspace", type_="unique")
    op.drop_column("workspace", "inbound_token")
