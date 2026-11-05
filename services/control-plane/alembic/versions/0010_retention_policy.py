"""retention_policy: years of retention per workspace and document type

Every row starts locked at its country's preset (`years = preset_years`); a workspace may raise
`years`, never go below `preset_years` while `locked` (checked by the API). Existing workspaces
are backfilled from their organisation's country. The app role cannot DELETE a policy.

The preset values below are a frozen copy of `sijil_core.retention.PRESETS` at this revision:
a migration must not change meaning when the code's presets do.

Revision ID: 0010
Revises: 0009
Create Date: 2026-11-05
"""

import os
import re
import uuid
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ROLE_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_TYPES = ("invoice", "receipt", "contract", "bank_statement", "payslip", "tax_notice", "other")
_DE_SHORT = ("invoice", "receipt", "bank_statement")
_PRESETS: dict[str, dict[str, int]] = {
    "MA": dict.fromkeys(_TYPES, 10),
    "FR": dict.fromkeys(_TYPES, 10),
    "DE": {t: 8 if t in _DE_SHORT else 10 for t in _TYPES},
    "AE": dict.fromkeys(_TYPES, 5),
    "SA": dict.fromkeys(_TYPES, 6),
}


def _app_role() -> str:
    role = os.environ.get("SIJIL_APP_DB_ROLE", "sijil_cp_app")
    if not _ROLE_RE.fullmatch(role):
        raise RuntimeError("SIJIL_APP_DB_ROLE is not a valid role name")
    return role


def upgrade() -> None:
    table = op.create_table(
        "retention_policy",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("workspace_id", sa.Uuid(), sa.ForeignKey("workspace.id"), nullable=False),
        sa.Column("document_type", sa.String(), nullable=False),
        sa.Column("years", sa.Integer(), nullable=False),
        sa.Column("preset_years", sa.Integer(), nullable=False),
        sa.Column("locked", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.UniqueConstraint(
            "workspace_id", "document_type", name=op.f("uq_retention_policy_workspace_id")
        ),
        sa.CheckConstraint("years BETWEEN 1 AND 50", name=op.f("ck_retention_policy_years_range")),
    )
    conn = op.get_bind()
    rows = conn.execute(
        sa.text("SELECT w.id, o.country FROM workspace w JOIN organisation o ON o.id = w.org_id")
    ).all()
    op.bulk_insert(
        table,
        [
            {
                "id": uuid.uuid4(),
                "workspace_id": workspace_id,
                "document_type": doc_type,
                "years": years,
                "preset_years": years,
                "locked": True,
            }
            for workspace_id, country in rows
            for doc_type, years in _PRESETS[country].items()
        ],
    )
    # Default privileges give the app role DELETE on new tables: a policy is raised, never removed.
    op.execute(f'REVOKE DELETE ON retention_policy FROM "{_app_role()}"')


def downgrade() -> None:
    op.drop_table("retention_policy")
