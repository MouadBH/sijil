import uuid
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Final

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    false,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, CITEXT, JSONB
from sqlalchemy.orm import Mapped, mapped_column, validates

from sijil_core.db.base import Base, utcnow
from sijil_core.inbound import new_token


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


class DocumentState(StrEnum):
    received = "received"
    needs_review = "needs_review"
    filed = "filed"
    sealed = "sealed"
    # Terminal (0011): removed by the Owner before seal; approved for deletion after
    # retention, then deleted. The row stays as a tombstone in all three.
    removed = "removed"
    deleting = "deleting"
    deleted = "deleted"


# Rows on their way out or gone: nothing ingests, extracts, seals or removes them again.
TERMINAL_STATES: Final = frozenset(
    {DocumentState.removed, DocumentState.deleting, DocumentState.deleted}
)


# The rows among which (workspace_id, sha256) is unique (0013): every non-terminal one.
LIVE_LEDGER_ROW: Final = "state NOT IN ('removed', 'deleting', 'deleted')"


class ExportState(StrEnum):
    queued = "queued"
    building = "building"
    ready = "ready"
    failed = "failed"
    # A ready export whose ZIP the daily expiry deleted (7 days after `ready_at`).
    expired = "expired"


class IngestState(StrEnum):
    queued = "queued"
    sending = "sending"
    consumed = "consumed"
    failed = "failed"
    unsupported = "unsupported"


class DocumentSource(StrEnum):
    upload = "upload"
    email = "email"


class SubscriptionStatus(StrEnum):
    # 0015 (ADM-3, ADM-4): every status is defined now so the column migrates once; Task 3
    # gives `overdue` and `read_only` their behaviour (ruling R1).
    trial = "trial"
    active = "active"
    overdue = "overdue"
    read_only = "read_only"
    closed = "closed"


class InvoiceKind(StrEnum):
    annual = "annual"
    upgrade = "upgrade"


class InvoiceState(StrEnum):
    issued = "issued"
    overdue = "overdue"
    paid = "paid"


class ActorType(StrEnum):
    user = "user"
    system = "system"
    email = "email"


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
    __table_args__ = (CheckConstraint("ice ~ '^[0-9]{15}$'", name="ice_format"),)

    id: Mapped[uuid.UUID] = _uuid_pk()
    created_at: Mapped[datetime] = _created_at()
    name: Mapped[str] = mapped_column(String)
    type: Mapped[OrgType] = mapped_column(_enum(OrgType))
    country: Mapped[str] = mapped_column(String(2))
    # Immutable after insert: enforced by the organisation_region_immutable trigger.
    region: Mapped[str] = mapped_column(String)
    status: Mapped[OrgStatus] = mapped_column(_enum(OrgStatus), default=OrgStatus.active)
    require_totp: Mapped[bool] = mapped_column(default=False, server_default=false())
    # 0015 (ADM-3): the organisation's ICE, 15 digits; required to choose a plan, and the first
    # own-ICE guard of extraction and learning (`sijil_core.suppliers.own_ices`).
    ice: Mapped[str | None] = mapped_column(String(15))


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
    db_name: Mapped[str] = mapped_column(String, unique=True)
    index_db_name: Mapped[str] = mapped_column(String, unique=True)
    kms_key_id: Mapped[str] = mapped_column(String)
    # The secret part of the workspace's inbound email address (see sijil_core.inbound).
    inbound_token: Mapped[str] = mapped_column(String(10), unique=True, default=new_token)


class RetentionPolicy(Base):
    """A workspace's retention in years for one document type. `locked` rows (all the seeded
    ones) can be raised but never go below `preset_years`."""

    __tablename__ = "retention_policy"
    __table_args__ = (
        UniqueConstraint("workspace_id", "document_type"),
        CheckConstraint("years BETWEEN 1 AND 50", name="years_range"),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspace.id"))
    document_type: Mapped[str] = mapped_column(String)
    years: Mapped[int] = mapped_column(Integer)
    preset_years: Mapped[int] = mapped_column(Integer)
    locked: Mapped[bool] = mapped_column(default=True, server_default=false())


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
        # A grant restricts a Member or a Read-only user only, and names at least one type.
        CheckConstraint(
            "document_types IS NULL OR (role IN ('member', 'readonly')"
            " AND cardinality(document_types) >= 1)",
            name="document_types_restricted",
        ),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    created_at: Mapped[datetime] = _created_at()
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("user_account.id"), index=True)
    org_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("organisation.id"), index=True)
    workspace_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("workspace.id"), index=True)
    role: Mapped[Role] = mapped_column(_enum(Role))
    # 0014 (ADM-1): the document types a Member or Read-only user may see; NULL means all.
    document_types: Mapped[list[str] | None] = mapped_column(ARRAY(Text))


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


class DocumentLedger(Base):
    """One row per archived original. Holds no content: no filename, subject or sender.

    Immutable columns, retention monotonicity and the ban on DELETE are enforced by the
    document_ledger_immutable trigger (retention layer 3); once sealed, `fields_sha256`, `state`
    and `retention_until` are frozen too (0008), and `object_locked_at` is set once (0009).
    The terminal states (0011) keep the row as a tombstone: `removed` (before seal only) and
    `deleted` are frozen; a sealed row moves only to `deleting`, once its retention is over;
    no row is inserted in a terminal state (document_ledger_insert_guard).
    """

    __tablename__ = "document_ledger"
    __table_args__ = (
        # 0013: one live row per hash; a tombstone's bytes may be archived again beside it.
        Index(
            "uq_document_ledger_workspace_sha256_live",
            "workspace_id",
            "sha256",
            unique=True,
            postgresql_where=text(LIVE_LEDGER_ROW),
        ),
        # 0008, relaxed by 0011: a sealed row has sealed_at, which only a sealed, deleting or
        # deleted row carries; sealed_at iff fields_sha256; sealed_by only once sealed.
        CheckConstraint(
            "(state <> 'sealed' OR sealed_at IS NOT NULL) "
            "AND (sealed_at IS NULL OR state IN ('sealed', 'deleting', 'deleted')) "
            "AND (sealed_at IS NULL) = (fields_sha256 IS NULL) "
            "AND (sealed_by IS NULL OR sealed_at IS NOT NULL)",
            name="seal_consistent",
        ),
        # 0011: each terminal state carries its own columns, and only it.
        CheckConstraint(
            "(state = 'removed') = (removed_at IS NOT NULL) "
            "AND (state IN ('deleting', 'deleted')) = (deletion_approved_at IS NOT NULL) "
            "AND (state = 'deleted') = (deleted_at IS NOT NULL) "
            "AND (removed_at IS NULL) = (removed_by IS NULL) "
            "AND (removed_at IS NULL) = (removal_reason IS NULL) "
            "AND (deletion_approved_at IS NULL) = (deletion_approved_by IS NULL) "
            "AND (state <> 'deleted' OR original_removed_at IS NOT NULL)",
            name="terminal_consistent",
        ),
        CheckConstraint(
            "char_length(removal_reason) BETWEEN 10 AND 1000", name="removal_reason_length"
        ),
        # 0009: the COMPLIANCE lock is recorded only on a sealed row.
        CheckConstraint(
            "object_locked_at IS NULL OR sealed_at IS NOT NULL", name="object_locked_after_seal"
        ),
        Index("ix_document_ledger_workspace_ingested", "workspace_id", text("ingested_at DESC")),
        Index(
            "ix_document_ledger_ingest_pending",
            "ingest_next_attempt_at",
            postgresql_where=text("ingest_state IN ('queued','sending')"),
        ),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspace.id"))
    sha256: Mapped[str] = mapped_column(String(64))
    md5: Mapped[str] = mapped_column(String(32))
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    content_kind: Mapped[str] = mapped_column(String(32))
    source: Mapped[DocumentSource] = mapped_column(_enum(DocumentSource))
    source_email_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("document_ledger.id"))
    uploaded_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user_account.id"))
    object_key: Mapped[str] = mapped_column(String)
    object_version_id: Mapped[str] = mapped_column(String)
    state: Mapped[DocumentState] = mapped_column(
        _enum(DocumentState), default=DocumentState.received
    )
    ingest_state: Mapped[IngestState] = mapped_column(
        _enum(IngestState), default=IngestState.queued
    )
    ingest_error_code: Mapped[str | None] = mapped_column(String)
    # Claim lease, attempt counter and the earliest retry time of the ingest worker.
    ingest_claimed_at: Mapped[datetime | None] = _tstz()
    ingest_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    ingest_next_attempt_at: Mapped[datetime | None] = _tstz()
    paperless_task_id: Mapped[str | None] = mapped_column(String)
    paperless_doc_id: Mapped[int | None] = mapped_column(Integer)
    retention_until: Mapped[date] = mapped_column(Date)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    fields_sha256: Mapped[str | None] = mapped_column(String(64))
    sealed_at: Mapped[datetime | None] = _tstz()
    sealed_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user_account.id"))
    # When the seal worker switched the original to COMPLIANCE (set once, 0009).
    object_locked_at: Mapped[datetime | None] = _tstz()
    # 0011: the Owner's removal before seal (all three written together, then frozen).
    removed_at: Mapped[datetime | None] = _tstz()
    removed_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user_account.id"))
    removal_reason: Mapped[str | None] = mapped_column(Text)
    # 0011: the deletion after retention: eligible (set once), approved by the Owner, deleted.
    deletion_eligible_at: Mapped[datetime | None] = _tstz()
    deletion_approved_at: Mapped[datetime | None] = _tstz()
    deletion_approved_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user_account.id"))
    deleted_at: Mapped[datetime | None] = _tstz()
    # 0011: when the object-store copy went, on either path (set once).
    original_removed_at: Mapped[datetime | None] = _tstz()


class AuditEvent(Base):
    """Append-only, hash-chained per workspace. The app role has no UPDATE/DELETE grant."""

    __tablename__ = "audit_event"
    __table_args__ = (UniqueConstraint("workspace_id", "seq", name="uq_audit_event_workspace_seq"),)

    id: Mapped[uuid.UUID] = _uuid_pk()
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspace.id"))
    seq: Mapped[int] = mapped_column(Integer)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    actor_type: Mapped[ActorType] = mapped_column(_enum(ActorType))
    actor_id: Mapped[str | None] = mapped_column(String)
    action: Mapped[str] = mapped_column(String)
    target_type: Mapped[str | None] = mapped_column(String)
    target_id: Mapped[str | None] = mapped_column(String)
    ip: Mapped[str | None] = mapped_column(String)
    data: Mapped[dict[str, Any]] = mapped_column(JSONB)
    prev_hash: Mapped[str] = mapped_column(String(64))
    hash: Mapped[str] = mapped_column(String(64), unique=True)


class IntegrityAlert(Base):
    __tablename__ = "integrity_alert"

    id: Mapped[uuid.UUID] = _uuid_pk()
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspace.id"), index=True)
    # None for a workspace-level finding (the check itself could not run: engine_*).
    ledger_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("document_ledger.id"), index=True
    )
    kind: Mapped[str] = mapped_column(String(64))
    detail: Mapped[dict[str, Any]] = mapped_column(JSONB)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    resolved_at: Mapped[datetime | None] = _tstz()


class ExportJob(Base):
    """A period export (ARC-5): built by the export worker into the exports bucket at
    `object_key` (`<workspace_id>/<job_id>.zip`). The app role has SELECT, INSERT and UPDATE
    only (0012). Holds no content: the period, the ZIP's hash and size, a count."""

    __tablename__ = "export_job"
    __table_args__ = (
        CheckConstraint(r"period ~ '^[0-9]{4}(-(0[1-9]|1[0-2]))?$'", name="period_format"),
        CheckConstraint(
            "state IN ('queued', 'building', 'ready', 'failed', 'expired')", name="state_known"
        ),
        CheckConstraint(
            "state NOT IN ('ready', 'expired') OR (object_key IS NOT NULL AND sha256 IS NOT NULL"
            " AND size_bytes IS NOT NULL AND document_count IS NOT NULL AND ready_at IS NOT NULL)",
            name="ready_complete",
        ),
        Index("ix_export_job_workspace_created", "workspace_id", text("created_at DESC")),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspace.id"))
    period: Mapped[str] = mapped_column(String(7))
    requested_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("user_account.id"))
    state: Mapped[ExportState] = mapped_column(_enum(ExportState), default=ExportState.queued)
    object_key: Mapped[str | None] = mapped_column(String)
    sha256: Mapped[str | None] = mapped_column(String(64))
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    document_count: Mapped[int | None] = mapped_column(Integer)
    error_code: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = _created_at()
    ready_at: Mapped[datetime | None] = _tstz()


class Plan(Base):
    """A plan of the catalogue (0015, seeded; edited only by migration). Users and storage are
    current totals; documents and AI pages are counted per billing year."""

    __tablename__ = "plan"
    __table_args__ = (
        CheckConstraint("price_ht >= 0", name="price_non_negative"),
        CheckConstraint(
            "max_users > 0 AND max_documents_year > 0 AND max_storage_bytes > 0"
            " AND max_ai_pages_year > 0",
            name="quotas_positive",
        ),
    )

    code: Mapped[str] = mapped_column(String(32), primary_key=True)
    # MAD, excluding VAT.
    price_ht: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    max_users: Mapped[int] = mapped_column(Integer)
    max_documents_year: Mapped[int] = mapped_column(Integer)
    max_storage_bytes: Mapped[int] = mapped_column(BigInteger)
    max_ai_pages_year: Mapped[int] = mapped_column(Integer)
    active: Mapped[bool] = mapped_column(default=True)


class Subscription(Base):
    """One per organisation (0015). The billing year is `[period_start, period_end)`; during
    the trial it is the trial itself, and the first year starts at `trial_ends_at`."""

    __tablename__ = "subscription"
    __table_args__ = (
        CheckConstraint(
            "status IN ('trial', 'active', 'overdue', 'read_only', 'closed')", name="status_known"
        ),
        CheckConstraint("period_end > period_start", name="period_ordered"),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    created_at: Mapped[datetime] = _created_at()
    org_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("organisation.id"), unique=True)
    plan_code: Mapped[str] = mapped_column(ForeignKey("plan.code"))
    status: Mapped[SubscriptionStatus] = mapped_column(
        _enum(SubscriptionStatus), default=SubscriptionStatus.trial
    )
    trial_ends_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # A downgrade waits for the renewal, which applies it (`sijil_core.billing.advance`).
    pending_plan_code: Mapped[str | None] = mapped_column(ForeignKey("plan.code"))
    # 0016: why the status is what it is, when the status alone does not say
    # (`org.ice_required`: the trial ended with no ICE to invoice).
    status_reason: Mapped[str | None] = mapped_column(String(64))


class UsageRecord(Base):
    """Metered usage (0015): the AI pages of one document, written once by the extraction
    worker when it calls the model. The app role may not UPDATE or DELETE it."""

    __tablename__ = "usage_record"
    __table_args__ = (
        UniqueConstraint("ledger_id", "kind", name="uq_usage_record_ledger_kind"),
        CheckConstraint("kind IN ('ai_pages')", name="kind_known"),
        CheckConstraint("quantity > 0", name="quantity_positive"),
        Index("ix_usage_record_org_at", "org_id", "at"),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    org_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("organisation.id"))
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspace.id"))
    ledger_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("document_ledger.id"))
    kind: Mapped[str] = mapped_column(String(32))
    quantity: Mapped[int] = mapped_column(Integer)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class QuotaWarning(Base):
    """A quota warning already given (0015): once per org, quota, threshold and billing year
    (`period_start`), whoever notices it first; the unique key makes concurrent ones one."""

    __tablename__ = "quota_warning"
    __table_args__ = (
        UniqueConstraint(
            "org_id", "kind", "threshold", "period_start", name="uq_quota_warning_once"
        ),
        CheckConstraint("threshold IN (80, 100)", name="threshold_known"),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    org_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("organisation.id"))
    kind: Mapped[str] = mapped_column(String(32))
    threshold: Mapped[int] = mapped_column(Integer)
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class InvoiceCounter(Base):
    """The last invoice number given in a calendar year of issue (0016): locked FOR UPDATE in
    the issuing transaction, so numbers are gapless and a rollback gives its number back."""

    __tablename__ = "invoice_counter"
    __table_args__ = (CheckConstraint("last >= 0", name="last_non_negative"),)

    year: Mapped[int] = mapped_column(Integer, primary_key=True)
    last: Mapped[int] = mapped_column(Integer)


class Invoice(Base):
    """An invoice in MAD (0016, ADM-4): `SIJ-{YYYY}-{NNNNNN}`. Everything but its payment
    (`state`, `paid_at`, `payment_reference`) is frozen at issue, the buyer and seller as
    snapshots and the PDF with its SHA-256; a trigger refuses any other change, and the app role
    may not DELETE it."""

    __tablename__ = "invoice"
    __table_args__ = (
        CheckConstraint("kind IN ('annual', 'upgrade')", name="kind_known"),
        CheckConstraint("state IN ('issued', 'overdue', 'paid')", name="state_known"),
        CheckConstraint("currency = 'MAD'", name="currency_mad"),
        CheckConstraint("amount_ttc = amount_ht + amount_vat", name="ttc_is_ht_plus_vat"),
        CheckConstraint("amount_ht >= 0 AND amount_vat >= 0", name="amounts_non_negative"),
        CheckConstraint("number ~ '^SIJ-[0-9]{4}-[0-9]{6}$'", name="number_format"),
        CheckConstraint("period_end > period_start", name="period_ordered"),
        CheckConstraint("(state = 'paid') = (paid_at IS NOT NULL)", name="paid_at_iff_paid"),
        UniqueConstraint("number", name="uq_invoice_number"),
        Index("ix_invoice_org_issued", "org_id", "issued_at"),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    org_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("organisation.id"))
    number: Mapped[str] = mapped_column(String(16))
    year: Mapped[int] = mapped_column(Integer)
    kind: Mapped[InvoiceKind] = mapped_column(_enum(InvoiceKind))
    plan_code: Mapped[str] = mapped_column(ForeignKey("plan.code"))
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    amount_ht: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    vat_rate: Mapped[Decimal] = mapped_column(Numeric(5, 2))
    amount_vat: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    amount_ttc: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    currency: Mapped[str] = mapped_column(String(3))
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    due_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    paid_at: Mapped[datetime | None] = _tstz()
    payment_reference: Mapped[str | None] = mapped_column(String(128))
    state: Mapped[InvoiceState] = mapped_column(_enum(InvoiceState), default=InvoiceState.issued)
    buyer: Mapped[dict[str, Any]] = mapped_column(JSONB)
    seller: Mapped[dict[str, Any]] = mapped_column(JSONB)
    locale: Mapped[Locale] = mapped_column(_enum(Locale))
    pdf: Mapped[bytes] = mapped_column(LargeBinary)
    pdf_sha256: Mapped[str] = mapped_column(String(64))
