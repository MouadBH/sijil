"""export_document: the documents an export holds (final review I1, ADM-5)

One row per document the export worker put in a ready export, written in the transaction that
makes the job `ready`. A take-over may end the hosting of a workspace only when its newest
built full export holds every live document (`sijil_core.closure.uncovered`): the recorded set,
never a guess from timestamps. Write-once: the app role has SELECT and INSERT only.

Revision ID: 0019
Revises: 0018
Create Date: 2026-11-17
"""

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0019"
down_revision: str | None = "0018"
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
        "export_document",
        sa.Column("export_job_id", sa.Uuid(), sa.ForeignKey("export_job.id"), nullable=False),
        sa.Column("ledger_id", sa.Uuid(), sa.ForeignKey("document_ledger.id"), nullable=False),
        sa.PrimaryKeyConstraint("export_job_id", "ledger_id", name=op.f("pk_export_document")),
    )
    op.execute(f'REVOKE UPDATE, DELETE ON export_document FROM "{_app_role()}"')


def downgrade() -> None:
    op.drop_table("export_document")
