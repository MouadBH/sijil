"""tenancy: organisations, workspaces, users, memberships, sessions, credentials, jobs

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-31
"""

import uuid
from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import CITEXT

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLES = (
    "job",
    "tenant_credential",
    "recovery_code",
    "user_session",
    "membership",
    "user_account",
    "workspace",
    "organisation",
)


def _id() -> sa.Column[uuid.UUID]:
    return sa.Column("id", sa.Uuid, nullable=False)


def _created_at() -> sa.Column[datetime]:
    return sa.Column("created_at", sa.DateTime(timezone=True), nullable=False)


def _ts(name: str, nullable: bool = True) -> sa.Column[datetime]:
    return sa.Column(name, sa.DateTime(timezone=True), nullable=nullable)


def _bool(name: str) -> sa.Column[bool]:
    return sa.Column(name, sa.Boolean, nullable=False, server_default=sa.false())


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS citext")

    op.create_table(
        "organisation",
        _id(),
        _created_at(),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("type", sa.String(32), nullable=False),
        sa.Column("country", sa.String(2), nullable=False),
        sa.Column("region", sa.String, nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        _bool("require_totp"),
        sa.PrimaryKeyConstraint("id", name="pk_organisation"),
    )
    op.execute("""
        CREATE FUNCTION organisation_region_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'org.region_immutable';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER organisation_region_immutable
        BEFORE UPDATE OF region ON organisation
        FOR EACH ROW WHEN (OLD.region IS DISTINCT FROM NEW.region)
        EXECUTE FUNCTION organisation_region_immutable()
    """)

    op.create_table(
        "user_account",
        _id(),
        _created_at(),
        sa.Column("email", CITEXT, nullable=False),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("password_hash", sa.String, nullable=False),
        sa.Column("locale", sa.String(32), nullable=False),
        sa.Column("digits", sa.String(32), nullable=False),
        _bool("totp_enabled"),
        sa.Column("totp_secret", sa.LargeBinary),
        sa.Column("totp_pending", sa.LargeBinary),
        sa.Column("totp_last_step", sa.Integer),
        sa.PrimaryKeyConstraint("id", name="pk_user_account"),
        sa.UniqueConstraint("email", name="uq_user_account_email"),
    )

    op.create_table(
        "workspace",
        _id(),
        _created_at(),
        sa.Column("org_id", sa.Uuid, nullable=False),
        sa.Column("client_of_firm_id", sa.Uuid),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("slug", sa.String, nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("paperless_url", sa.String),
        sa.Column("db_name", sa.String, nullable=False),
        sa.Column("index_db_name", sa.String, nullable=False),
        sa.Column("kms_key_id", sa.String, nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_workspace"),
        sa.ForeignKeyConstraint(
            ["org_id"], ["organisation.id"], name="fk_workspace_org_id_organisation"
        ),
        sa.ForeignKeyConstraint(
            ["client_of_firm_id"],
            ["organisation.id"],
            name="fk_workspace_client_of_firm_id_organisation",
        ),
        sa.UniqueConstraint("slug", name="uq_workspace_slug"),
    )
    op.create_index("ix_workspace_org_id", "workspace", ["org_id"])
    op.create_index("ix_workspace_client_of_firm_id", "workspace", ["client_of_firm_id"])

    op.create_table(
        "membership",
        _id(),
        _created_at(),
        sa.Column("user_id", sa.Uuid, nullable=False),
        sa.Column("org_id", sa.Uuid),
        sa.Column("workspace_id", sa.Uuid),
        sa.Column("role", sa.String(32), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_membership"),
        sa.ForeignKeyConstraint(
            ["user_id"], ["user_account.id"], name="fk_membership_user_id_user_account"
        ),
        sa.ForeignKeyConstraint(
            ["org_id"], ["organisation.id"], name="fk_membership_org_id_organisation"
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspace.id"], name="fk_membership_workspace_id_workspace"
        ),
        sa.CheckConstraint(
            "(org_id IS NOT NULL) <> (workspace_id IS NOT NULL)",
            name="ck_membership_exactly_one_scope",
        ),
        sa.UniqueConstraint("user_id", "org_id", name="uq_membership_user_org"),
        sa.UniqueConstraint("user_id", "workspace_id", name="uq_membership_user_workspace"),
    )
    op.create_index("ix_membership_user_id", "membership", ["user_id"])
    op.create_index("ix_membership_org_id", "membership", ["org_id"])
    op.create_index("ix_membership_workspace_id", "membership", ["workspace_id"])

    op.create_table(
        "user_session",
        _id(),
        _created_at(),
        sa.Column("user_id", sa.Uuid, nullable=False),
        sa.Column("token_hash", sa.LargeBinary, nullable=False),
        _ts("expires_at", nullable=False),
        _ts("last_seen_at", nullable=False),
        _ts("totp_verified_at"),
        _bool("pending_totp"),
        _ts("revoked_at"),
        sa.Column("ip", sa.String),
        sa.Column("user_agent", sa.String),
        sa.PrimaryKeyConstraint("id", name="pk_user_session"),
        sa.ForeignKeyConstraint(
            ["user_id"], ["user_account.id"], name="fk_user_session_user_id_user_account"
        ),
        sa.UniqueConstraint("token_hash", name="uq_user_session_token_hash"),
    )
    op.create_index("ix_user_session_user_id", "user_session", ["user_id"])

    op.create_table(
        "recovery_code",
        _id(),
        _created_at(),
        sa.Column("user_id", sa.Uuid, nullable=False),
        sa.Column("code_hmac", sa.LargeBinary, nullable=False),
        _ts("used_at"),
        sa.PrimaryKeyConstraint("id", name="pk_recovery_code"),
        sa.ForeignKeyConstraint(
            ["user_id"], ["user_account.id"], name="fk_recovery_code_user_id_user_account"
        ),
    )
    op.create_index("ix_recovery_code_user_id", "recovery_code", ["user_id"])

    op.create_table(
        "tenant_credential",
        _id(),
        _created_at(),
        sa.Column("workspace_id", sa.Uuid, nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("envelope", sa.LargeBinary, nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_tenant_credential"),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspace.id"], name="fk_tenant_credential_workspace_id_workspace"
        ),
        sa.UniqueConstraint("workspace_id", "kind", name="uq_tenant_credential_workspace_kind"),
    )

    op.create_table(
        "job",
        _id(),
        _created_at(),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("workspace_id", sa.Uuid),
        sa.Column("error_code", sa.String),
        _ts("updated_at", nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_job"),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspace.id"], name="fk_job_workspace_id_workspace"
        ),
    )
    op.create_index("ix_job_workspace_id", "job", ["workspace_id"])


def downgrade() -> None:
    for table in TABLES:
        op.drop_table(table)
    op.execute("DROP FUNCTION organisation_region_immutable()")
    # citext stays: another migration or role may rely on it.
