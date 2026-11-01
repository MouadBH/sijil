"""unique tenant database names: two workspaces never share databases or roles

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-01
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_unique_constraint("uq_workspace_db_name", "workspace", ["db_name"])
    op.create_unique_constraint("uq_workspace_index_db_name", "workspace", ["index_db_name"])


def downgrade() -> None:
    op.drop_constraint("uq_workspace_index_db_name", "workspace", type_="unique")
    op.drop_constraint("uq_workspace_db_name", "workspace", type_="unique")
