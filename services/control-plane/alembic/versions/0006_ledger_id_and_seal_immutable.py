"""ledger immutability: the row id never changes, and the seal fields are set at most once

`sealed_at` and `sealed_by` may go NULL -> value once; never value -> other value or back to
NULL. Everything 0004 guarded stays guarded.

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-01
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
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
    op.execute(
        _function("""
             OR NEW.id IS DISTINCT FROM OLD.id
             OR (OLD.sealed_at IS NOT NULL AND NEW.sealed_at IS DISTINCT FROM OLD.sealed_at)
             OR (OLD.sealed_by IS NOT NULL AND NEW.sealed_by IS DISTINCT FROM OLD.sealed_by)""")
    )


def downgrade() -> None:
    op.execute(_function(""))
