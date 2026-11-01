"""ingest claim lease, attempt counter and next-attempt time on the document ledger

The immutability trigger guards only identity columns, so these stay writable.

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "document_ledger",
        sa.Column("ingest_claimed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "document_ledger",
        sa.Column("ingest_attempts", sa.Integer, nullable=False, server_default="0"),
    )
    op.add_column(
        "document_ledger",
        sa.Column("ingest_next_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_document_ledger_ingest_pending",
        "document_ledger",
        ["ingest_next_attempt_at"],
        postgresql_where=sa.text("ingest_state IN ('queued','sending')"),
    )


def downgrade() -> None:
    op.drop_index("ix_document_ledger_ingest_pending", table_name="document_ledger")
    op.drop_column("document_ledger", "ingest_next_attempt_at")
    op.drop_column("document_ledger", "ingest_attempts")
    op.drop_column("document_ledger", "ingest_claimed_at")
