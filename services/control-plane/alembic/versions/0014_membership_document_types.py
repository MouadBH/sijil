"""membership.document_types: Members and Read-only users limited to document types (ADM-1)

NULL means every type. A grant restricts only the `member` and `readonly` roles and names at
least one type; the API checks the values against the taxonomy (`sijil_core.fields`).

Revision ID: 0014
Revises: 0013
Create Date: 2026-11-16
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CHECK = "ck_membership_document_types_restricted"


def upgrade() -> None:
    op.add_column(
        "membership", sa.Column("document_types", postgresql.ARRAY(sa.Text), nullable=True)
    )
    op.create_check_constraint(
        op.f(_CHECK),
        "membership",
        "document_types IS NULL OR (role IN ('member', 'readonly')"
        " AND cardinality(document_types) >= 1)",
    )


def downgrade() -> None:
    op.drop_constraint(op.f(_CHECK), "membership", type_="check")
    op.drop_column("membership", "document_types")
