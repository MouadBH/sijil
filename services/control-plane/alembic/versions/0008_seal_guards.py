"""seal guards: a sealed ledger row's hash, state and retention are frozen

Retention-lock layer 3 (design §5.4, ruling P8). Once `sealed_at` is set, `fields_sha256`,
`state` and `retention_until` never change, and `fields_sha256` goes NULL -> value once. The
CHECK keeps the seal columns consistent: the state is `sealed` exactly when `sealed_at` is set,
exactly when `fields_sha256` is, and `sealed_by` only on a sealed row. Everything 0006 guarded
stays guarded.

Rows written before this migration are unsealed with no hash (sealing arrives with it), so they
pass the CHECK; it is validated against them as it is added.

Revision ID: 0008
Revises: 0007
Create Date: 2026-11-02
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
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

_CHECK = "ck_document_ledger_seal_consistent"
_CONDITION = (
    "(state = 'sealed') = (sealed_at IS NOT NULL) "
    "AND (sealed_at IS NULL) = (fields_sha256 IS NULL) "
    "AND (sealed_by IS NULL OR sealed_at IS NOT NULL)"
)


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
    op.execute(_function(_GUARDED_0006 + _GUARDED_0008))
    op.create_check_constraint(op.f(_CHECK), "document_ledger", _CONDITION)


def downgrade() -> None:
    op.drop_constraint(op.f(_CHECK), "document_ledger", type_="check")
    op.execute(_function(_GUARDED_0006))
