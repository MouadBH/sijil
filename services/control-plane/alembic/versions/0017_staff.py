"""Platform staff and the platform's own audit chain (plan 6.3, ADM-4)

- `user_account.is_staff`: platform staff, who reach the `/v1/staff` API. Set only by
  `sijil-cp staff grant|revoke`.
- `platform_audit_event`: the hash chain of events that belong to no organisation (a staff
  grant), append-only like `audit_event`: no UPDATE or DELETE for the app role, and a trigger
  refusing both to everyone else.

Revision ID: 0017
Revises: 0016
Create Date: 2026-11-16
"""

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0017"
down_revision: str | None = "0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ROLE_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def _app_role() -> str:
    role = os.environ.get("SIJIL_APP_DB_ROLE", "sijil_cp_app")
    if not _ROLE_RE.fullmatch(role):
        raise RuntimeError("SIJIL_APP_DB_ROLE is not a valid role name")
    return role


def upgrade() -> None:
    op.add_column(
        "user_account",
        sa.Column("is_staff", sa.Boolean(), server_default=sa.false(), nullable=False),
    )

    op.create_table(
        "platform_audit_event",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actor_type", sa.String(32), nullable=False),
        sa.Column("actor_id", sa.String(), nullable=True),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("target_type", sa.String(), nullable=True),
        sa.Column("target_id", sa.String(), nullable=True),
        sa.Column("ip", sa.String(), nullable=True),
        sa.Column("data", JSONB(), nullable=False),
        sa.Column("prev_hash", sa.String(64), nullable=False),
        sa.Column("hash", sa.String(64), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_platform_audit_event"),
        sa.UniqueConstraint("seq", name="uq_platform_audit_event_seq"),
        sa.UniqueConstraint("hash", name="uq_platform_audit_event_hash"),
    )
    op.execute(f'REVOKE UPDATE, DELETE ON platform_audit_event FROM "{_app_role()}"')
    op.execute("""
        CREATE FUNCTION platform_audit_event_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'audit.append_only';
        END $$
    """)
    op.execute("""
        CREATE TRIGGER platform_audit_event_append_only
        BEFORE UPDATE OR DELETE ON platform_audit_event
        FOR EACH STATEMENT EXECUTE FUNCTION platform_audit_event_append_only()
    """)


def downgrade() -> None:
    op.drop_table("platform_audit_event")
    op.execute("DROP FUNCTION platform_audit_event_append_only()")
    op.drop_column("user_account", "is_staff")
