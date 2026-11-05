"""terminal ledger states: the Owner's pre-seal removal and the approved deletion after retention

Retention-lock layer 3 (design §5.4) for the three terminal states. The ledger row is never
deleted: it stays as a tombstone (`sha256`, `object_key`, `object_version_id` never change).

- `received`, `needs_review` or `filed` -> `removed` with `removed_at`, `removed_by` and
  `removal_reason` written in the same UPDATE; `sealed` -> `removed` is refused. From `removed`
  the row is frozen but for `original_removed_at` (NULL -> value, once).
- Any state but `removed`, `deleting` or `deleted` -> `deleting` with `deletion_approved_at`
  and `deletion_approved_by`, only once `retention_until` is before today in the region
  (`sijil_region_today()`). This is the one relaxation of the sealed-state freeze (0008).
- `deleting` -> `deleted` once `original_removed_at` is set; `deleted` is frozen.
- `sealed_at` is written only with the state `sealed`, so a deleting row is never sealed.
- `deletion_eligible_at` goes NULL -> value once; `original_removed_at` likewise, and only in a
  terminal state. Everything 0004, 0006, 0008 and 0009 guarded stays guarded in every state.

`ck_document_ledger_seal_consistent` is redefined: a seal (`sealed_at`) may sit on a `sealed`,
`deleting` or `deleted` row, and a `sealed` row still needs one.

`sijil_region_today()` is today in the zone of SIJIL_REGION ('Africa/Casablanca' for ma-cas),
compiled into the function: no session setting moves it (a session-settable zone would let
any role approve a deletion up to a week early).

A BEFORE INSERT guard refuses a row born `removed`, `deleting` or `deleted`, and the terminal
CHECK keeps each state's columns together: the removal trio and the approval pair are all set
or all NULL, and `deleted` carries `original_removed_at`.

Revision ID: 0011
Revises: 0010
Create Date: 2026-11-05
"""

import os
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# A frozen copy of `sijil_core.retention.REGION_TZ` at this revision.
_REGION_TZ = {"ma-cas": "Africa/Casablanca"}

_GUARDED_0004 = """
             NEW.workspace_id IS DISTINCT FROM OLD.workspace_id
             OR NEW.sha256 IS DISTINCT FROM OLD.sha256
             OR NEW.md5 IS DISTINCT FROM OLD.md5
             OR NEW.size_bytes IS DISTINCT FROM OLD.size_bytes
             OR NEW.content_kind IS DISTINCT FROM OLD.content_kind
             OR NEW.source IS DISTINCT FROM OLD.source
             OR NEW.source_email_id IS DISTINCT FROM OLD.source_email_id
             OR NEW.object_key IS DISTINCT FROM OLD.object_key
             OR NEW.object_version_id IS DISTINCT FROM OLD.object_version_id
             OR NEW.ingested_at IS DISTINCT FROM OLD.ingested_at
             OR NEW.uploaded_by IS DISTINCT FROM OLD.uploaded_by
             OR NEW.retention_until IS NULL
             OR NEW.retention_until < OLD.retention_until"""

_GUARDED_0006 = """
             OR NEW.id IS DISTINCT FROM OLD.id
             OR (OLD.sealed_at IS NOT NULL AND NEW.sealed_at IS DISTINCT FROM OLD.sealed_at)
             OR (OLD.sealed_by IS NOT NULL AND NEW.sealed_by IS DISTINCT FROM OLD.sealed_by)"""

_GUARDED_0008 = """
             OR (OLD.fields_sha256 IS NOT NULL
                 AND NEW.fields_sha256 IS DISTINCT FROM OLD.fields_sha256)
             OR (OLD.sealed_at IS NOT NULL AND (
                    NEW.fields_sha256 IS DISTINCT FROM OLD.fields_sha256
                    OR NEW.state IS DISTINCT FROM OLD.state
                    OR NEW.retention_until IS DISTINCT FROM OLD.retention_until))"""

# 0008 without its state clause: from 0011 a sealed row's state moves only by `_TRANSITIONS`.
_GUARDED_0008_HASH_RETENTION = """
             OR (OLD.fields_sha256 IS NOT NULL
                 AND NEW.fields_sha256 IS DISTINCT FROM OLD.fields_sha256)
             OR (OLD.sealed_at IS NOT NULL AND (
                    NEW.fields_sha256 IS DISTINCT FROM OLD.fields_sha256
                    OR NEW.retention_until IS DISTINCT FROM OLD.retention_until))"""

_GUARDED_0009 = """
             OR (OLD.object_locked_at IS NOT NULL
                 AND NEW.object_locked_at IS DISTINCT FROM OLD.object_locked_at)"""

_GUARDED_0011 = """
             OR (NEW.sealed_at IS DISTINCT FROM OLD.sealed_at AND NEW.state <> 'sealed')
             OR ((NEW.removed_at IS DISTINCT FROM OLD.removed_at
                  OR NEW.removed_by IS DISTINCT FROM OLD.removed_by
                  OR NEW.removal_reason IS DISTINCT FROM OLD.removal_reason)
                 AND NOT (NEW.state = 'removed' AND OLD.state <> 'removed'))
             OR ((NEW.deletion_approved_at IS DISTINCT FROM OLD.deletion_approved_at
                  OR NEW.deletion_approved_by IS DISTINCT FROM OLD.deletion_approved_by)
                 AND NOT (NEW.state = 'deleting' AND OLD.state <> 'deleting'))
             OR (NEW.deleted_at IS DISTINCT FROM OLD.deleted_at
                 AND NOT (NEW.state = 'deleted' AND OLD.state = 'deleting'))
             OR (OLD.deletion_eligible_at IS NOT NULL
                 AND NEW.deletion_eligible_at IS DISTINCT FROM OLD.deletion_eligible_at)
             OR (NEW.original_removed_at IS DISTINCT FROM OLD.original_removed_at
                 AND (OLD.original_removed_at IS NOT NULL
                      OR NEW.state NOT IN ('removed', 'deleting', 'deleted')))
             OR (OLD.state = 'removed'
                 AND to_jsonb(NEW) - 'original_removed_at'
                     IS DISTINCT FROM to_jsonb(OLD) - 'original_removed_at')
             OR (OLD.state = 'deleted' AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD))"""

# The state changes 0011 allows; any other change of state is refused.
_TRANSITIONS = """
             (OLD.sealed_at IS NULL
                 AND OLD.state IN ('received', 'needs_review', 'filed')
                 AND NEW.state IN ('received', 'needs_review', 'filed', 'sealed'))
             OR (NEW.state = 'removed'
                 AND OLD.state IN ('received', 'needs_review', 'filed')
                 AND NEW.removed_at IS NOT NULL
                 AND NEW.removed_by IS NOT NULL
                 AND NEW.removal_reason IS NOT NULL)
             OR (NEW.state = 'deleting'
                 AND OLD.state NOT IN ('removed', 'deleting', 'deleted')
                 AND NEW.deletion_approved_at IS NOT NULL
                 AND NEW.deletion_approved_by IS NOT NULL
                 AND OLD.retention_until < sijil_region_today())
             OR (NEW.state = 'deleted' AND OLD.state = 'deleting'
                 AND NEW.original_removed_at IS NOT NULL)"""

_SEAL_CHECK = "ck_document_ledger_seal_consistent"
_SEAL_0008 = (
    "(state = 'sealed') = (sealed_at IS NOT NULL) "
    "AND (sealed_at IS NULL) = (fields_sha256 IS NULL) "
    "AND (sealed_by IS NULL OR sealed_at IS NOT NULL)"
)
_SEAL_0011 = (
    "(state <> 'sealed' OR sealed_at IS NOT NULL) "
    "AND (sealed_at IS NULL OR state IN ('sealed', 'deleting', 'deleted')) "
    "AND (sealed_at IS NULL) = (fields_sha256 IS NULL) "
    "AND (sealed_by IS NULL OR sealed_at IS NOT NULL)"
)
_TERMINAL_CHECK = "ck_document_ledger_terminal_consistent"
_TERMINAL = (
    "(state = 'removed') = (removed_at IS NOT NULL) "
    "AND (state IN ('deleting', 'deleted')) = (deletion_approved_at IS NOT NULL) "
    "AND (state = 'deleted') = (deleted_at IS NOT NULL) "
    "AND (removed_at IS NULL) = (removed_by IS NULL) "
    "AND (removed_at IS NULL) = (removal_reason IS NULL) "
    "AND (deletion_approved_at IS NULL) = (deletion_approved_by IS NULL) "
    "AND (state <> 'deleted' OR original_removed_at IS NOT NULL)"
)
_REASON_CHECK = "ck_document_ledger_removal_reason_length"

_COLUMNS = (
    ("removed_at", sa.DateTime(timezone=True)),
    ("removal_reason", sa.Text()),
    ("deletion_eligible_at", sa.DateTime(timezone=True)),
    ("deletion_approved_at", sa.DateTime(timezone=True)),
    ("deleted_at", sa.DateTime(timezone=True)),
    ("original_removed_at", sa.DateTime(timezone=True)),
)
_ACTORS = ("removed_by", "deletion_approved_by")


def _function_0009() -> str:
    return f"""
        CREATE OR REPLACE FUNCTION document_ledger_immutable() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'ledger.immutable';
          END IF;
          IF {_GUARDED_0004}{_GUARDED_0006}{_GUARDED_0008}{_GUARDED_0009} THEN
            RAISE EXCEPTION 'ledger.immutable';
          END IF;
          RETURN NEW;
        END $$
    """


def _function_0011() -> str:
    guarded = _GUARDED_0006 + _GUARDED_0008_HASH_RETENTION + _GUARDED_0009 + _GUARDED_0011
    return f"""
        CREATE OR REPLACE FUNCTION document_ledger_immutable() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'ledger.immutable';
          END IF;
          IF coalesce({_GUARDED_0004}{guarded}, true) THEN
            RAISE EXCEPTION 'ledger.immutable';
          END IF;
          IF NEW.state IS DISTINCT FROM OLD.state AND NOT coalesce({_TRANSITIONS}, false) THEN
            RAISE EXCEPTION 'ledger.immutable';
          END IF;
          RETURN NEW;
        END $$
    """


def _region_today(tz: str) -> str:
    # The zone is a literal: nothing a session sets (a GUC, its TimeZone) moves the date.
    return f"""
        CREATE FUNCTION sijil_region_today() RETURNS date LANGUAGE sql STABLE AS $$
          SELECT (now() AT TIME ZONE '{tz}')::date
        $$
    """


# The UPDATE trigger never sees an INSERT: a row is born live, never in a terminal state.
_INSERT_GUARD = """
    CREATE FUNCTION document_ledger_insert_guard() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.state IN ('removed', 'deleting', 'deleted') THEN
        RAISE EXCEPTION 'ledger.immutable';
      END IF;
      RETURN NEW;
    END $$
"""
_INSERT_TRIGGER = """
    CREATE TRIGGER document_ledger_insert_guard BEFORE INSERT ON document_ledger
    FOR EACH ROW EXECUTE FUNCTION document_ledger_insert_guard()
"""


def upgrade() -> None:
    for name, type_ in _COLUMNS:
        op.add_column("document_ledger", sa.Column(name, type_, nullable=True))
    for name in _ACTORS:
        op.add_column("document_ledger", sa.Column(name, sa.Uuid(), nullable=True))
        op.create_foreign_key(
            f"fk_document_ledger_{name}_user_account",
            "document_ledger",
            "user_account",
            [name],
            ["id"],
        )
    # An unknown region fails the migration rather than guess a calendar.
    op.execute(_region_today(_REGION_TZ[os.environ.get("SIJIL_REGION", "ma-cas")]))
    op.execute(_function_0011())
    op.execute(_INSERT_GUARD)
    op.execute(_INSERT_TRIGGER)
    op.drop_constraint(op.f(_SEAL_CHECK), "document_ledger", type_="check")
    op.create_check_constraint(op.f(_SEAL_CHECK), "document_ledger", _SEAL_0011)
    op.create_check_constraint(op.f(_TERMINAL_CHECK), "document_ledger", _TERMINAL)
    op.create_check_constraint(
        op.f(_REASON_CHECK), "document_ledger", "char_length(removal_reason) BETWEEN 10 AND 1000"
    )


def downgrade() -> None:
    op.drop_constraint(op.f(_REASON_CHECK), "document_ledger", type_="check")
    op.drop_constraint(op.f(_TERMINAL_CHECK), "document_ledger", type_="check")
    op.drop_constraint(op.f(_SEAL_CHECK), "document_ledger", type_="check")
    op.create_check_constraint(op.f(_SEAL_CHECK), "document_ledger", _SEAL_0008)
    # The function first: 0009's never mentions the columns, which then go.
    op.execute(_function_0009())
    op.execute("DROP TRIGGER document_ledger_insert_guard ON document_ledger")
    op.execute("DROP FUNCTION document_ledger_insert_guard()")
    op.execute("DROP FUNCTION sijil_region_today()")
    for name in _ACTORS:
        op.drop_column("document_ledger", name)
    for name, _type in reversed(_COLUMNS):
        op.drop_column("document_ledger", name)
