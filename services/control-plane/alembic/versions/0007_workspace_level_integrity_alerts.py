"""integrity alerts about a whole workspace: `ledger_id` becomes nullable

A drift check that cannot run (token refused, credential missing, engine unreachable) is a
finding about the workspace, not about one document.

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column("integrity_alert", "ledger_id", existing_type=sa.Uuid, nullable=True)


def downgrade() -> None:
    # Workspace-level alerts have no document to point at: they cannot survive the downgrade.
    op.execute("DELETE FROM integrity_alert WHERE ledger_id IS NULL")
    op.alter_column("integrity_alert", "ledger_id", existing_type=sa.Uuid, nullable=False)
