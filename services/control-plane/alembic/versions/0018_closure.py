"""Account closure and the full export (plan 6.4, ADM-5, design §7 item 11)

- `export_job.period` also takes `all`: a full export of every non-terminal document.
- `closure`: one row per closure request of an organisation, `grace` -> `hibernated` ->
  `destroyed`, or `grace` -> `cancelled`. At most one closure of an organisation is open (any
  state but `cancelled`): a second request after a cancel starts a new row. The app role may
  not DELETE it: a closure stays as the record of the request.
- `takeover_statement`: the Owner's signed statement that the organisation takes custody of its
  originals, at most one per closure. The PDF itself is in the object store (write-once, its
  own object lock); the row holds its key, version and SHA-256 and is write-once: the app role
  has SELECT and INSERT only.

`workspace.state` gains `closed` (a destroyed workspace): the column is plain text validated in
Python, so no DDL is needed for it.

Revision ID: 0018
Revises: 0017
Create Date: 2026-11-17
"""

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018"
down_revision: str | None = "0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ROLE_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_PERIOD_CHECK = "ck_export_job_period_format"
_PERIOD_0012 = r"period ~ '^[0-9]{4}(-(0[1-9]|1[0-2]))?$'"
_PERIOD_0018 = r"period = 'all' OR period ~ '^[0-9]{4}(-(0[1-9]|1[0-2]))?$'"


def _app_role() -> str:
    role = os.environ.get("SIJIL_APP_DB_ROLE", "sijil_cp_app")
    if not _ROLE_RE.fullmatch(role):
        raise RuntimeError("SIJIL_APP_DB_ROLE is not a valid role name")
    return role


def upgrade() -> None:
    op.drop_constraint(op.f(_PERIOD_CHECK), "export_job", type_="check")
    op.create_check_constraint(op.f(_PERIOD_CHECK), "export_job", _PERIOD_0018)

    op.create_table(
        "closure",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), sa.ForeignKey("organisation.id"), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("requested_by", sa.Uuid(), sa.ForeignKey("user_account.id"), nullable=False),
        sa.Column("grace_ends_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("export_job_id", sa.Uuid(), sa.ForeignKey("export_job.id"), nullable=True),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancelled_by", sa.Uuid(), sa.ForeignKey("user_account.id"), nullable=True),
        sa.Column("hibernated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("destroyed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_closure")),
        sa.CheckConstraint(
            "state IN ('grace', 'hibernated', 'destroyed', 'cancelled')",
            name=op.f("ck_closure_state_known"),
        ),
        sa.CheckConstraint("grace_ends_at > requested_at", name=op.f("ck_closure_grace_ordered")),
        sa.CheckConstraint(
            "(state = 'cancelled') = (cancelled_at IS NOT NULL)"
            " AND (cancelled_at IS NULL) = (cancelled_by IS NULL)",
            name=op.f("ck_closure_cancel_consistent"),
        ),
    )
    op.create_index(
        "uq_closure_org_open",
        "closure",
        ["org_id"],
        unique=True,
        postgresql_where=sa.text("state <> 'cancelled'"),
    )
    op.execute(f'REVOKE DELETE ON closure FROM "{_app_role()}"')

    op.create_table(
        "takeover_statement",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("closure_id", sa.Uuid(), sa.ForeignKey("closure.id"), nullable=False),
        sa.Column("org_id", sa.Uuid(), sa.ForeignKey("organisation.id"), nullable=False),
        sa.Column("signed_by", sa.Uuid(), sa.ForeignKey("user_account.id"), nullable=False),
        sa.Column("signer_name", sa.String(200), nullable=False),
        sa.Column("signer_capacity", sa.String(200), nullable=False),
        sa.Column("object_key", sa.String(), nullable=False),
        sa.Column("object_version_id", sa.String(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_takeover_statement")),
        sa.UniqueConstraint("closure_id", name=op.f("uq_takeover_statement_closure_id")),
        sa.CheckConstraint(
            "sha256 ~ '^[0-9a-f]{64}$'", name=op.f("ck_takeover_statement_sha256_hex")
        ),
        sa.CheckConstraint("size_bytes > 0", name=op.f("ck_takeover_statement_size_positive")),
    )
    op.execute(f'REVOKE UPDATE, DELETE ON takeover_statement FROM "{_app_role()}"')


def downgrade() -> None:
    op.drop_table("takeover_statement")
    op.drop_index("uq_closure_org_open", table_name="closure")
    op.drop_table("closure")
    op.drop_constraint(op.f(_PERIOD_CHECK), "export_job", type_="check")
    op.create_check_constraint(op.f(_PERIOD_CHECK), "export_job", _PERIOD_0012)
