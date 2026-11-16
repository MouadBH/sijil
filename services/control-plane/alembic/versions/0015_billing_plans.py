"""plans, subscriptions, the usage meter and the organisation's ICE (plan 6.2, ADM-3)

- `plan`: the catalogue, seeded below (MAD excluding VAT; edited only by a later migration).
- `subscription`: one per organisation. Every status (`trial`, `active`, `overdue`,
  `read_only`, `closed`) exists from here, so the column migrates once (ruling R1). Each
  existing organisation gets a 30-day trial on `essentiel` from its creation.
- `usage_record`: the AI pages of a document, once per (ledger_id, kind). The app role gets
  SELECT and INSERT only: a usage record is never changed or deleted.
- `quota_warning`: a warning given once per (org, kind, threshold, billing year). SELECT and
  INSERT only, too.
- `organisation.ice`: 15 digits, or NULL.

Revision ID: 0015
Revises: 0014
Create Date: 2026-11-16
"""

import os
import re
from collections.abc import Sequence
from decimal import Decimal

import sqlalchemy as sa
from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ROLE_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_GB = 1024**3

# A frozen copy of the product owner's catalogue (2026-11-13) at this revision.
_PLANS = [
    {
        "code": "essentiel",
        "price_ht": Decimal("2400.00"),
        "max_users": 3,
        "max_documents_year": 2_000,
        "max_storage_bytes": 5 * _GB,
        "max_ai_pages_year": 2_000,
        "active": True,
    },
    {
        "code": "pro",
        "price_ht": Decimal("6000.00"),
        "max_users": 10,
        "max_documents_year": 10_000,
        "max_storage_bytes": 25 * _GB,
        "max_ai_pages_year": 10_000,
        "active": True,
    },
    {
        "code": "entreprise",
        "price_ht": Decimal("14400.00"),
        "max_users": 30,
        "max_documents_year": 40_000,
        "max_storage_bytes": 100 * _GB,
        "max_ai_pages_year": 40_000,
        "active": True,
    },
]


def _app_role() -> str:
    role = os.environ.get("SIJIL_APP_DB_ROLE", "sijil_cp_app")
    if not _ROLE_RE.fullmatch(role):
        raise RuntimeError("SIJIL_APP_DB_ROLE is not a valid role name")
    return role


def upgrade() -> None:
    op.add_column("organisation", sa.Column("ice", sa.String(15), nullable=True))
    op.create_check_constraint(
        op.f("ck_organisation_ice_format"), "organisation", "ice ~ '^[0-9]{15}$'"
    )

    plan = op.create_table(
        "plan",
        sa.Column("code", sa.String(32), primary_key=True),
        sa.Column("price_ht", sa.Numeric(14, 2), nullable=False),
        sa.Column("max_users", sa.Integer(), nullable=False),
        sa.Column("max_documents_year", sa.Integer(), nullable=False),
        sa.Column("max_storage_bytes", sa.BigInteger(), nullable=False),
        sa.Column("max_ai_pages_year", sa.Integer(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.CheckConstraint("price_ht >= 0", name=op.f("ck_plan_price_non_negative")),
        sa.CheckConstraint(
            "max_users > 0 AND max_documents_year > 0 AND max_storage_bytes > 0"
            " AND max_ai_pages_year > 0",
            name=op.f("ck_plan_quotas_positive"),
        ),
    )
    op.bulk_insert(plan, _PLANS)

    op.create_table(
        "subscription",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "org_id", sa.Uuid(), sa.ForeignKey("organisation.id"), nullable=False, unique=True
        ),
        sa.Column("plan_code", sa.String(32), sa.ForeignKey("plan.code"), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("trial_ends_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pending_plan_code", sa.String(32), sa.ForeignKey("plan.code"), nullable=True),
        sa.CheckConstraint(
            "status IN ('trial', 'active', 'overdue', 'read_only', 'closed')",
            name=op.f("ck_subscription_status_known"),
        ),
        sa.CheckConstraint(
            "period_end > period_start", name=op.f("ck_subscription_period_ordered")
        ),
    )
    # Existing organisations: a trial on the trial plan from their creation.
    op.execute(
        """
        INSERT INTO subscription (id, created_at, org_id, plan_code, status, trial_ends_at,
                                  period_start, period_end)
        SELECT gen_random_uuid(), now(), id, 'essentiel', 'trial',
               created_at + interval '30 days', created_at, created_at + interval '30 days'
        FROM organisation
        """
    )

    op.create_table(
        "usage_record",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("org_id", sa.Uuid(), sa.ForeignKey("organisation.id"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), sa.ForeignKey("workspace.id"), nullable=False),
        sa.Column("ledger_id", sa.Uuid(), sa.ForeignKey("document_ledger.id"), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("ledger_id", "kind", name="uq_usage_record_ledger_kind"),
        sa.CheckConstraint("kind IN ('ai_pages')", name=op.f("ck_usage_record_kind_known")),
        sa.CheckConstraint("quantity > 0", name=op.f("ck_usage_record_quantity_positive")),
    )
    op.create_index("ix_usage_record_org_at", "usage_record", ["org_id", "at"])

    op.create_table(
        "quota_warning",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("org_id", sa.Uuid(), sa.ForeignKey("organisation.id"), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("threshold", sa.Integer(), nullable=False),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "org_id", "kind", "threshold", "period_start", name="uq_quota_warning_once"
        ),
        sa.CheckConstraint("threshold IN (80, 100)", name=op.f("ck_quota_warning_threshold_known")),
    )

    role = _app_role()
    op.execute(f'REVOKE UPDATE, DELETE ON usage_record FROM "{role}"')
    op.execute(f'REVOKE UPDATE, DELETE ON quota_warning FROM "{role}"')
    op.execute(f'REVOKE INSERT, UPDATE, DELETE ON plan FROM "{role}"')


def downgrade() -> None:
    op.drop_table("quota_warning")
    op.drop_index("ix_usage_record_org_at", table_name="usage_record")
    op.drop_table("usage_record")
    op.drop_table("subscription")
    op.drop_table("plan")
    op.drop_constraint(op.f("ck_organisation_ice_format"), "organisation", type_="check")
    op.drop_column("organisation", "ice")
