"""export_job: signed period exports (plan 4.6, ARC-5)

One row per requested export: `queued` -> `building` -> `ready` (or `failed`), and `expired`
once the daily expiry has deleted a ready export's ZIP. The app role gets SELECT, INSERT and
UPDATE (default privileges would also give it DELETE): a job stays as the record of an export.

Revision ID: 0012
Revises: 0011
Create Date: 2026-11-05
"""

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ROLE_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def _app_role() -> str:
    role = os.environ.get("SIJIL_APP_DB_ROLE", "sijil_cp_app")
    if not _ROLE_RE.fullmatch(role):
        raise RuntimeError("SIJIL_APP_DB_ROLE is not a valid role name")
    return role


def upgrade() -> None:
    op.create_table(
        "export_job",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("workspace_id", sa.Uuid(), sa.ForeignKey("workspace.id"), nullable=False),
        sa.Column("period", sa.String(7), nullable=False),
        sa.Column("requested_by", sa.Uuid(), sa.ForeignKey("user_account.id"), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("object_key", sa.String(), nullable=True),
        sa.Column("sha256", sa.String(64), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("document_count", sa.Integer(), nullable=True),
        sa.Column("error_code", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ready_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            r"period ~ '^[0-9]{4}(-(0[1-9]|1[0-2]))?$'", name=op.f("ck_export_job_period_format")
        ),
        sa.CheckConstraint(
            "state IN ('queued', 'building', 'ready', 'failed', 'expired')",
            name=op.f("ck_export_job_state_known"),
        ),
        sa.CheckConstraint(
            "state NOT IN ('ready', 'expired') OR (object_key IS NOT NULL AND sha256 IS NOT NULL"
            " AND size_bytes IS NOT NULL AND document_count IS NOT NULL AND ready_at IS NOT NULL)",
            name=op.f("ck_export_job_ready_complete"),
        ),
    )
    op.create_index(
        "ix_export_job_workspace_created",
        "export_job",
        ["workspace_id", sa.text("created_at DESC")],
    )
    op.execute(f'REVOKE DELETE ON export_job FROM "{_app_role()}"')


def downgrade() -> None:
    op.drop_index("ix_export_job_workspace_created", table_name="export_job")
    op.drop_table("export_job")
