"""object_locked_at: when the seal worker switched a sealed original to COMPLIANCE

The control plane commits the seal; the seal worker (the only holder of the sealing key) then
locks the original and records `object_locked_at`. It goes NULL -> value once and never changes
after, and only a sealed row can carry it. Everything 0008 guarded stays guarded.

Revision ID: 0009
Revises: 0008
Create Date: 2026-11-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_GUARDED_0004 = """
             NEW.workspace_id IS DISTINCT FROM OLD.workspace_id
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
             OR NEW.retention_until < OLD.retention_until"""

_GUARDED_0006 = """
             OR NEW.id IS DISTINCT FROM OLD.id
             OR (OLD.sealed_at IS NOT NULL AND NEW.sealed_at IS DISTINCT FROM OLD.sealed_at)
             OR (OLD.sealed_by IS NOT NULL AND NEW.sealed_by IS DISTINCT FROM OLD.sealed_by)"""

_GUARDED_0008 = """
             OR (OLD.fields_sha256 IS NOT NULL
                 AND NEW.fields_sha256 IS DISTINCT FROM OLD.fields_sha256)
             OR (OLD.sealed_at IS NOT NULL AND (
                    NEW.fields_sha256 IS DISTINCT FROM OLD.fields_sha256
                    OR NEW.state IS DISTINCT FROM OLD.state
                    OR NEW.retention_until IS DISTINCT FROM OLD.retention_until))"""

_GUARDED_0009 = """
             OR (OLD.object_locked_at IS NOT NULL
                 AND NEW.object_locked_at IS DISTINCT FROM OLD.object_locked_at)"""

_CHECK = "ck_document_ledger_object_locked_after_seal"


def _function(extra: str) -> str:
    return f"""
        CREATE OR REPLACE FUNCTION document_ledger_immutable() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'ledger.immutable';
          END IF;
          IF {_GUARDED_0004}{extra} THEN
            RAISE EXCEPTION 'ledger.immutable';
          END IF;
          RETURN NEW;
        END $$
    """


def upgrade() -> None:
    op.add_column(
        "document_ledger", sa.Column("object_locked_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.execute(_function(_GUARDED_0006 + _GUARDED_0008 + _GUARDED_0009))
    op.create_check_constraint(
        op.f(_CHECK), "document_ledger", "object_locked_at IS NULL OR sealed_at IS NOT NULL"
    )


def downgrade() -> None:
    op.drop_constraint(op.f(_CHECK), "document_ledger", type_="check")
    # The function first: 0008's never mentions the column, which then goes.
    op.execute(_function(_GUARDED_0006 + _GUARDED_0008))
    op.drop_column("document_ledger", "object_locked_at")
