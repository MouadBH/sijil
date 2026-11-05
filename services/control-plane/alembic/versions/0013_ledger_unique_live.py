"""document_ledger: (workspace_id, sha256) unique among live rows only (final review I1)

A file whose ledger row is `removed`, `deleting` or `deleted` may be archived again: a mistaken
removal, or a deletion at the end of retention, must not bar the real document from the archive.
The new upload is a new ledger row and a new object version under the same content key; the
tombstone keeps its own row and version id. Two live rows still never share a hash.

0004's unique constraint becomes a partial unique index over the non-terminal rows.

Downgrade restores the constraint over every row. It refuses (raises, so its transaction rolls
back and nothing changes) while any workspace holds a hash on more than one row: the constraint
cannot hold then, and deleting or altering a ledger row to make it hold is never an option.

Revision ID: 0013
Revises: 0012
Create Date: 2026-11-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CONSTRAINT = "uq_document_ledger_workspace_sha256"
_INDEX = "uq_document_ledger_workspace_sha256_live"
LIVE = "state NOT IN ('removed', 'deleting', 'deleted')"


def upgrade() -> None:
    # The index first: no instant without uniqueness among the live rows.
    op.create_index(
        _INDEX,
        "document_ledger",
        ["workspace_id", "sha256"],
        unique=True,
        postgresql_where=sa.text(LIVE),
    )
    op.drop_constraint(_CONSTRAINT, "document_ledger", type_="unique")


def downgrade() -> None:
    shared = op.get_bind().scalar(
        sa.text(
            "SELECT count(*) FROM (SELECT 1 FROM document_ledger"
            " GROUP BY workspace_id, sha256 HAVING count(*) > 1) AS s"
        )
    )
    if shared:
        raise RuntimeError(
            f"0013 downgrade refused: {shared} hash(es) archived again after a removal or"
            " deletion; 0012's unique constraint cannot hold, and no ledger row is ever deleted"
        )
    op.create_unique_constraint(_CONSTRAINT, "document_ledger", ["workspace_id", "sha256"])
    op.drop_index(_INDEX, table_name="document_ledger")
