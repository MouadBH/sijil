import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
    Uuid,
    false,
)
from sqlalchemy.dialects.postgresql import CITEXT
from sqlalchemy.orm import Mapped, mapped_column, validates

from sijil_core.db.base import Base, utcnow


class OrgType(StrEnum):
    business = "business"
    firm = "firm"


class OrgStatus(StrEnum):
    active = "active"
    closing = "closing"
    closed = "closed"


class WorkspaceState(StrEnum):
    provisioning = "provisioning"
    active = "active"
    hibernated = "hibernated"
    closing = "closing"
    error = "error"


class Role(StrEnum):
    owner = "owner"
    admin = "admin"
    member = "member"
    readonly = "readonly"

    @property
    def rank(self) -> int:
        return {"owner": 4, "admin": 3, "member": 2, "readonly": 1}[self.value]


class Locale(StrEnum):
    fr = "fr"
    ar = "ar"
    en = "en"


class Digits(StrEnum):
    latn = "latn"
    arab = "arab"


class CredentialKind(StrEnum):
    paperless_db_password = "paperless_db_password"
    index_db_password = "index_db_password"
    secret_key = "secret_key"
    admin_password = "admin_password"
    service_token = "service_token"
    support_password = "support_password"


class JobKind(StrEnum):
    provision_workspace = "provision_workspace"


class JobStatus(StrEnum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"


def _enum(cls: type[StrEnum]) -> Enum:
    # Stored as text holding the member value, validated in Python.
    return Enum(
        cls,
        native_enum=False,
        length=32,
        create_constraint=False,
        values_callable=lambda e: [m.value for m in e],
    )


def _uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(Uuid, primary_key=True, default=uuid.uuid4)


def _created_at() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), default=utcnow)


def _tstz() -> Mapped[datetime | None]:
    return mapped_column(DateTime(timezone=True))


class Organisation(Base):
    __tablename__ = "organisation"

    id: Mapped[uuid.UUID] = _uuid_pk()
    created_at: Mapped[datetime] = _created_at()
    name: Mapped[str] = mapped_column(String)
    type: Mapped[OrgType] = mapped_column(_enum(OrgType))
    country: Mapped[str] = mapped_column(String(2))
    # Immutable after insert: enforced by the organisation_region_immutable trigger.
    region: Mapped[str] = mapped_column(String)
    status: Mapped[OrgStatus] = mapped_column(_enum(OrgStatus), default=OrgStatus.active)
    require_totp: Mapped[bool] = mapped_column(default=False, server_default=false())


class Workspace(Base):
    __tablename__ = "workspace"

    id: Mapped[uuid.UUID] = _uuid_pk()
    created_at: Mapped[datetime] = _created_at()
    org_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("organisation.id"), index=True)
    client_of_firm_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("organisation.id"), index=True
    )
    name: Mapped[str] = mapped_column(String)
    slug: Mapped[str] = mapped_column(String, unique=True)
    state: Mapped[WorkspaceState] = mapped_column(
        _enum(WorkspaceState), default=WorkspaceState.provisioning
    )
    paperless_url: Mapped[str | None] = mapped_column(String)
    db_name: Mapped[str] = mapped_column(String)
    index_db_name: Mapped[str] = mapped_column(String)
    kms_key_id: Mapped[str] = mapped_column(String)


class User(Base):
    # Not "user": that is a reserved word in PostgreSQL.
    __tablename__ = "user_account"

    id: Mapped[uuid.UUID] = _uuid_pk()
    created_at: Mapped[datetime] = _created_at()
    email: Mapped[str] = mapped_column(CITEXT, unique=True)
    name: Mapped[str] = mapped_column(String)
    password_hash: Mapped[str] = mapped_column(String)
    locale: Mapped[Locale] = mapped_column(_enum(Locale), default=Locale.fr)
    digits: Mapped[Digits] = mapped_column(_enum(Digits), default=Digits.latn)
    totp_enabled: Mapped[bool] = mapped_column(default=False, server_default=false())
    totp_secret: Mapped[bytes | None] = mapped_column(LargeBinary)
    totp_pending: Mapped[bytes | None] = mapped_column(LargeBinary)
    totp_last_step: Mapped[int | None] = mapped_column(Integer)

    @validates("email")
    def _normalise_email(self, _key: str, value: str) -> str:
        return value.strip().lower()


class Membership(Base):
    __tablename__ = "membership"
    __table_args__ = (
        CheckConstraint(
            "(org_id IS NOT NULL) <> (workspace_id IS NOT NULL)", name="exactly_one_scope"
        ),
        UniqueConstraint("user_id", "org_id", name="uq_membership_user_org"),
        UniqueConstraint("user_id", "workspace_id", name="uq_membership_user_workspace"),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    created_at: Mapped[datetime] = _created_at()
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("user_account.id"), index=True)
    org_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("organisation.id"), index=True)
    workspace_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("workspace.id"), index=True)
    role: Mapped[Role] = mapped_column(_enum(Role))


class UserSession(Base):
    __tablename__ = "user_session"

    id: Mapped[uuid.UUID] = _uuid_pk()
    created_at: Mapped[datetime] = _created_at()
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("user_account.id"), index=True)
    token_hash: Mapped[bytes] = mapped_column(LargeBinary, unique=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    totp_verified_at: Mapped[datetime | None] = _tstz()
    pending_totp: Mapped[bool] = mapped_column(default=False, server_default=false())
    revoked_at: Mapped[datetime | None] = _tstz()
    ip: Mapped[str | None] = mapped_column(String)
    user_agent: Mapped[str | None] = mapped_column(String)


class RecoveryCode(Base):
    __tablename__ = "recovery_code"

    id: Mapped[uuid.UUID] = _uuid_pk()
    created_at: Mapped[datetime] = _created_at()
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("user_account.id"), index=True)
    code_hmac: Mapped[bytes] = mapped_column(LargeBinary)
    used_at: Mapped[datetime | None] = _tstz()


class TenantCredential(Base):
    __tablename__ = "tenant_credential"
    __table_args__ = (
        UniqueConstraint("workspace_id", "kind", name="uq_tenant_credential_workspace_kind"),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    created_at: Mapped[datetime] = _created_at()
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspace.id"))
    kind: Mapped[CredentialKind] = mapped_column(_enum(CredentialKind))
    envelope: Mapped[bytes] = mapped_column(LargeBinary)


class Job(Base):
    __tablename__ = "job"

    id: Mapped[uuid.UUID] = _uuid_pk()
    created_at: Mapped[datetime] = _created_at()
    kind: Mapped[JobKind] = mapped_column(_enum(JobKind))
    status: Mapped[JobStatus] = mapped_column(_enum(JobStatus), default=JobStatus.queued)
    workspace_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("workspace.id"), index=True)
    error_code: Mapped[str | None] = mapped_column(String)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
