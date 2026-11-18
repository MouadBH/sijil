"""MAD invoices with gapless numbers, and why a subscription is overdue (plan 6.3, ADM-4)

- `invoice_counter`: the last number given per calendar year of issue, locked FOR UPDATE by
  the issuing transaction (`sijil_core.invoicing.next_number`). The app role may not DELETE a
  counter.
- `invoice`: one row per invoice, frozen at issue. A trigger refuses any change but the
  payment's (`state`, `paid_at`, `payment_reference`) and any DELETE, the owner included; the
  app role has no DELETE either.
- `subscription.status_reason`: `org.ice_required` while a trial that ended without an ICE
  keeps the subscription overdue.

Revision ID: 0016
Revises: 0015
Create Date: 2026-11-16
"""

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0016"
down_revision: str | None = "0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ROLE_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

# Everything an issued invoice carries but its payment.
_FROZEN = (
    "id",
    "org_id",
    "number",
    "year",
    "kind",
    "plan_code",
    "period_start",
    "period_end",
    "amount_ht",
    "vat_rate",
    "amount_vat",
    "amount_ttc",
    "currency",
    "issued_at",
    "due_at",
    "buyer",
    "seller",
    "locale",
    "pdf",
    "pdf_sha256",
)


def _app_role() -> str:
    role = os.environ.get("SIJIL_APP_DB_ROLE", "sijil_cp_app")
    if not _ROLE_RE.fullmatch(role):
        raise RuntimeError("SIJIL_APP_DB_ROLE is not a valid role name")
    return role


def upgrade() -> None:
    op.add_column("subscription", sa.Column("status_reason", sa.String(64), nullable=True))

    op.create_table(
        "invoice_counter",
        sa.Column("year", sa.Integer(), primary_key=True),
        sa.Column("last", sa.Integer(), nullable=False),
        sa.CheckConstraint("last >= 0", name=op.f("ck_invoice_counter_last_non_negative")),
    )

    op.create_table(
        "invoice",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("org_id", sa.Uuid(), sa.ForeignKey("organisation.id"), nullable=False),
        sa.Column("number", sa.String(16), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("plan_code", sa.String(32), sa.ForeignKey("plan.code"), nullable=False),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("amount_ht", sa.Numeric(14, 2), nullable=False),
        sa.Column("vat_rate", sa.Numeric(5, 2), nullable=False),
        sa.Column("amount_vat", sa.Numeric(14, 2), nullable=False),
        sa.Column("amount_ttc", sa.Numeric(14, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("paid_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("payment_reference", sa.String(128), nullable=True),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("buyer", JSONB(), nullable=False),
        sa.Column("seller", JSONB(), nullable=False),
        sa.Column("locale", sa.String(32), nullable=False),
        sa.Column("pdf", sa.LargeBinary(), nullable=False),
        sa.Column("pdf_sha256", sa.String(64), nullable=False),
        sa.CheckConstraint("kind IN ('annual', 'upgrade')", name=op.f("ck_invoice_kind_known")),
        sa.CheckConstraint(
            "state IN ('issued', 'overdue', 'paid')", name=op.f("ck_invoice_state_known")
        ),
        sa.CheckConstraint("currency = 'MAD'", name=op.f("ck_invoice_currency_mad")),
        sa.CheckConstraint(
            "amount_ttc = amount_ht + amount_vat", name=op.f("ck_invoice_ttc_is_ht_plus_vat")
        ),
        sa.CheckConstraint(
            "amount_ht >= 0 AND amount_vat >= 0", name=op.f("ck_invoice_amounts_non_negative")
        ),
        sa.CheckConstraint(
            "number ~ '^SIJ-[0-9]{4}-[0-9]{6}$'", name=op.f("ck_invoice_number_format")
        ),
        sa.CheckConstraint("period_end > period_start", name=op.f("ck_invoice_period_ordered")),
        sa.CheckConstraint(
            "(state = 'paid') = (paid_at IS NOT NULL)", name=op.f("ck_invoice_paid_at_iff_paid")
        ),
        sa.UniqueConstraint("number", name="uq_invoice_number"),
    )
    op.create_index("ix_invoice_org_issued", "invoice", ["org_id", "issued_at"])

    changed = "\n             OR ".join(f"NEW.{c} IS DISTINCT FROM OLD.{c}" for c in _FROZEN)
    op.execute(
        f"""
        CREATE FUNCTION invoice_immutable() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'invoice.immutable';
          END IF;
          IF {changed} THEN
            RAISE EXCEPTION 'invoice.immutable';
          END IF;
          RETURN NEW;
        END $$
        """
    )
    op.execute(
        "CREATE TRIGGER invoice_immutable BEFORE UPDATE OR DELETE ON invoice"
        " FOR EACH ROW EXECUTE FUNCTION invoice_immutable()"
    )

    role = _app_role()
    op.execute(f'REVOKE DELETE ON invoice FROM "{role}"')
    op.execute(f'REVOKE DELETE ON invoice_counter FROM "{role}"')


def downgrade() -> None:
    op.execute("DROP TRIGGER invoice_immutable ON invoice")
    op.execute("DROP FUNCTION invoice_immutable()")
    op.drop_index("ix_invoice_org_issued", table_name="invoice")
    op.drop_table("invoice")
    op.drop_table("invoice_counter")
    op.drop_column("subscription", "status_reason")
