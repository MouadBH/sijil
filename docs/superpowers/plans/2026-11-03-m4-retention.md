# M4 Retention and Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retention presets per document type, the Owner's pre-seal removal, Owner-approved deletion after retention, correction notes on sealed documents, and a signed period export that verifies, with a lock proof suite showing that no role can delete or alter a document under retention through any path.

**Architecture:**
- Two new single-purpose Celery workers, both on the `core` network only, with no Docker socket and no tenant or AI network:
  - `worker-removal` (queue `removal`) is the only holder of the object-store key that can delete an original.
  - `worker-export` (queue `export`) is the only holder of the region's Ed25519 signing key.
- The Paperless delete after a removal runs in the main `worker`, which already holds the tenant admin credential.
- The ledger keeps a tombstone row forever.
- Correction notes live in the per-workspace index database.
- Exports are built from the object store, never from Paperless.

**Tech Stack:** FastAPI, SQLAlchemy 2 async, Alembic, Celery, boto3 against RustFS 1.0.4, `cryptography` (Ed25519), Python `zipfile` and `csv`.

**Spec:**
- `SPEC.md`: ARC-3 and ARC-5.
- Amended by `docs/superpowers/specs/2026-10-31-sijil-mvp-design.md`: §5.2a, §5.4, §5.5 and §5.6. The design wins where they differ.
- `docs/plan.md`: Milestone 4, tasks 4.1–4.6.

**Product-owner decisions for M4 (2026-11-03):**
- The delete key is held by a removal-only worker. The Paperless delete runs afterwards in the main worker.
- The export signing key is held by an export-only worker.

## Global Constraints

- Paperless-ngx is unmodified: the image `ghcr.io/paperless-ngx/paperless-ngx:3.4.1`, configured only by environment variables, reached over REST, only from `services/` and only through `sijil_core.paperless`.
- No JS package talks to Paperless.
- Every service requires `SIJIL_REGION`. Every workspace-scoped request and job calls `sijil_core.region.assert_region` before it reads or writes anything of the workspace.
- No document content leaves the region. Exports are built in-region and downloaded only through the control plane.
- Retention lock is absolute, in four layers: API, Paperless service-user permissions, DB constraint, object lock. Every new path that can change or delete a document gets a test in `tests/retention/`.
- **A sealed document can never be removed.** A document is deleted only when an Owner approves it after `retention_until`. Nothing is purged automatically.
- SHA-256 for every file. The audit log is append-only and hash-chained. Never write UPDATE or DELETE against `audit_event`.
- Audit `data` carries str, int, bool or None only, and never field values or correction values (ruling P5). A removal reason is user text about the document, not its content: it is stored on the ledger tombstone, and the audit event carries only its length.
- The API returns machine codes, never prose. New codes:
  - `document.sealed_cannot_be_removed` 409;
  - `document.not_removable` 409;
  - `removal.reason_required` 422;
  - `retention.not_eligible` 409 (carries `"ids"`);
  - `retention.policy_below_preset` 422 (carries `"document_type"`, `"minimum"`);
  - `correction.not_sealed` 409;
  - `export.period_invalid` 422;
  - `export.not_ready` 409;
  - `auth.step_up_required` 403 (exists).
- Money is `Decimal` (Python) or a string (TS). CSV amounts are the canonical strings from `sijil_core.fields`.
- SAMPLE fixtures only.
- Python 3.13, ruff (line length 100), `mypy --strict`, pytest `asyncio_mode=auto`.
- TDD: each test is seen failing before the code exists.
- Conventional Commits, small. Never import `sijil_cp` from the worker or the reverse; shared code goes in `sijil_core`.
- Lock order everywhere: the ledger row `FOR UPDATE` first, then the audit chain's advisory lock (as in `review.py`).
- Migrations continue from `0009`. The index schema goes from `SCHEMA_VERSION = 1` to `2` via the existing `ensure_schema` path.

## Review Focus

1. **A removal races a seal.** Two requests on one unsealed document, one sealing and one removing. Exactly one wins under the ledger lock. Neither a sealed tombstone nor a removed-then-sealed row can exist. The removal worker re-reads the ledger and refuses to delete the original of a row that is not `removed`.
2. **The removal key against a sealed or COMPLIANCE original.** If a `removal` message names a sealed document (forged, or a bug), the worker refuses before calling the store. The store also refuses: COMPLIANCE can't be bypassed. Both are tested, the second against RustFS.
3. **Deletion right at the boundary.** A document with `retention_until = 2036-12-31` is not eligible on 2036-12-31 in Casablanca and is eligible on 2037-01-01. The DB guard uses the same rule. A COMPLIANCE object whose lock has not expired refuses the delete, and the worker then leaves the ledger in `deleting`, not `deleted`.
4. **An export of a period while documents change.** A document sealed, corrected or removed while the ZIP is being built never appears in the report with a hash or value it doesn't have in the ZIP. The report is built from one ledger snapshot (a single read transaction), and each original is checked against its ledger `sha256` as it is streamed in.
5. **Arabic and Excel in `index.csv`.** Arabic supplier names and Arabic-Indic-free canonical amounts open correctly in Excel (UTF-8 BOM, CRLF, RFC 4180 quoting). A value with a comma, quote or newline round-trips.

---

### Task 1: Retention presets (plan 4.1, ARC-3)

**Files:**
- Create: `services/control-plane/alembic/versions/0010_retention_policy.py`, `services/control-plane/src/sijil_cp/api/retention.py`
- Modify:
  - `libs/sijil-core/src/sijil_core/retention.py`: presets, and lookups by workspace and type;
  - `libs/sijil-core/src/sijil_core/db/models.py`: `RetentionPolicy`;
  - `services/worker/src/sijil_worker/extract.py`: routing uses the policy;
  - `services/control-plane/src/sijil_cp/api/review.py`: seal uses the policy;
  - workspace creation (wherever `Workspace` rows are created) seeds policies.
- Test: `libs/sijil-core/tests/test_retention.py`, `services/control-plane/tests/test_retention_api.py`, `tests/retention/test_policy_never_lowers.py`

**Interfaces:**
- Produces:
  - `sijil_core.retention.PRESETS: Mapping[str, Mapping[str, int]]`: country → document type → years. Every type in `sijil_core.fields.DOCUMENT_TYPES` has a value. The values are working values, marked as such in a comment, and none is confirmed by an accountant.
    - MA: 10 for every type.
    - FR: 10 for every type.
    - DE: `invoice` 8, `receipt` 8, `bank_statement` 8, every other type 10.
    - AE: 5 for every type.
    - SA: 6 for every type.
  - `RetentionPolicy` columns:
    - `id` uuid pk;
    - `workspace_id` fk;
    - `document_type` text;
    - `years` int CHECK `years BETWEEN 1 AND 50`;
    - `preset_years` int;
    - `locked` bool.

    `UNIQUE (workspace_id, document_type)`. `locked = true` means `years` may never go below `preset_years`. Every seeded row is locked.
  - `async def policy_years(db: AsyncSession, workspace_id: UUID, document_type: str | None) -> int`. `None` and unknown types read `other`. A workspace with no row falls back to `PRESETS[country]["other"]`, looked up through the workspace's org country.
  - `def fiscal_retain_until(doc_date: date, years: int) -> date`. The signature changes from taking `country` to taking `years`; update every caller. The fiscal year is the calendar year (design §5.4 "calendar fiscal year by default"). A non-calendar fiscal year is out of scope; leave a `# M7+` note only if a caller needs one.
  - `retention_years(country)` is removed. `provisional_retain_until(arrival, years)` likewise takes `years`.
  - API:
    - `GET /v1/workspaces/{id}/retention/policies` (`retention_manage`) returns `[{document_type, years, preset_years, locked}]`.
    - `PUT /v1/workspaces/{id}/retention/policies/{document_type}` (`retention_manage` + `require_recent_totp(10)`) takes `{years}`. Below `preset_years` on a locked row gives 422 `retention.policy_below_preset`. It audits `retention.policy_changed` with `{document_type, years, previous}`.
  - A changed policy applies only to retention computed from then on. Nothing already set is ever lowered: the 0004 trigger already refuses a lower `retention_until`.

- [ ] **Step 1: Write the failing tests.**
  - `test_ma_preset_10_years`: `PRESETS["MA"]` is 10 for every document type.
  - `test_retention_until_end_of_fiscal_year_plus_years`:
    - `fiscal_retain_until(date(2026, 3, 15), 10) == date(2036, 12, 31)`;
    - `fiscal_retain_until(date(2026, 12, 31), 8) == date(2034, 12, 31)`;
    - `fiscal_retain_until(date(2026, 1, 1), 5) == date(2031, 12, 31)`.
  - `test_every_preset_covers_every_type`.
  - `test_policy_years_falls_back_to_other_and_country`.
  - `test_new_workspace_gets_locked_presets`: creating a workspace in an MA org creates one locked row per type, each with `years = preset_years = 10`.
  - API tests:
    - `test_put_policy_raises_years_and_audits`;
    - `test_put_policy_below_preset_is_422`;
    - `test_put_policy_requires_step_up`;
    - `test_member_cannot_change_policy` (403).
  - `tests/retention/test_policy_never_lowers.py::test_lowered_policy_never_lowers_existing_retention`:
    1. Raise `invoice` to 12.
    2. Route a document; `retention_until` reflects 12.
    3. Set it back to 10 (allowed: `>= preset_years`).
    4. Re-route the same document; `retention_until` is unchanged.
- [ ] **Step 2: Run them and watch them fail** (`uv run pytest libs/sijil-core/tests/test_retention.py services/control-plane/tests/test_retention_api.py tests/retention/test_policy_never_lowers.py -v`). Expected: import or attribute errors.
- [ ] **Step 3: Implement.**
  - Migration 0010 creates `retention_policy`. It backfills a locked preset row per (existing workspace × type) from the workspace's org country, and grants the app role SELECT, INSERT and UPDATE only (no DELETE).
  - Seed policies in the same transaction that creates a workspace.
  - Routing (`extract.py`) and seal (`review.py`) compute the fiscal date from `policy_years(db, workspace_id, document_type)`. The type is the one being routed or sealed; the retention basis is unchanged (`retention_basis`).
  - Ingest's provisional retention uses `policy_years(…, None)` (that is, `other`).
- [ ] **Step 4: Run the tests, then `make check`.** Expected: PASS.
- [ ] **Step 5: Commit**: `feat(cp): retention policies per document type with locked country presets`.

### Task 2: Terminal ledger states and the index purge (DB layer for 4.3 and 4.4)

**Files:**
- Create: `services/control-plane/alembic/versions/0011_removal_and_deletion.py`
- Modify:
  - `libs/sijil-core/src/sijil_core/db/models.py`;
  - `libs/sijil-core/src/sijil_core/index.py` (`SCHEMA_VERSION = 2`).
- Test: `tests/retention/test_terminal_states.py`, `libs/sijil-core/tests/test_index_purge.py`

**Interfaces:**
- Produces:
  - `DocumentState` gains `deleting` and `deleted`. The full set is `received`, `needs_review`, `filed`, `sealed`, `removed`, `deleting`, `deleted`.
  - Ledger columns, all nullable:
    - `removed_at` timestamptz, `removed_by` uuid, `removal_reason` text (CHECK `char_length(removal_reason) BETWEEN 10 AND 1000` when set);
    - `deletion_eligible_at` timestamptz;
    - `deletion_approved_at` timestamptz, `deletion_approved_by` uuid;
    - `deleted_at` timestamptz;
    - `original_removed_at` timestamptz: set when the object-store copy is gone, for both paths.
  - Trigger rules, added to `document_ledger_immutable()` in the 0008 style (keep every earlier guard):
    - `received`, `needs_review` or `filed` → `removed` requires `removed_at`, `removed_by` and `removal_reason` to be set in the same UPDATE. `sealed` → `removed` is refused. Once set, all three are frozen.
    - From `removed`, the state is frozen. Only `original_removed_at` may go NULL → value, once.
    - Any state except `removed`, `deleting` or `deleted` → `deleting` requires `deletion_approved_at` and `deletion_approved_by`, plus `OLD.retention_until < (now() AT TIME ZONE 'Africa/Casablanca')::date`. Use a SQL function `sijil_region_today()` returning that date. It reads the zone from the GUC `sijil.region_tz` and defaults to `'Africa/Casablanca'`; the migration sets the default for the database. This is the one DB-side relaxation of the sealed-state freeze, and only after retention.
    - `deleting` → `deleted` requires `original_removed_at` to be set. `deleted` is frozen.
    - Every column guarded by 0004, 0006 and 0008 stays guarded in every state. `sha256`, `object_key` and `object_version_id` are the tombstone and never change.
  - CHECK `ck_document_ledger_terminal_consistent`:
    - `(state = 'removed') = (removed_at IS NOT NULL)`;
    - `(state IN ('deleting','deleted')) = (deletion_approved_at IS NOT NULL)`;
    - `(state = 'deleted') = (deleted_at IS NOT NULL)`.

    It replaces nothing: `ck_document_ledger_seal_consistent` is redefined to accept `sealed_at IS NOT NULL` with `state IN ('sealed','deleting','deleted')`.
  - Index schema v2:
    - `correction_note` table (for Task 6), append-only (SJ002 on UPDATE, DELETE and TRUNCATE): `id` uuid pk, `ledger_id` uuid, `field` text, `value` text NULL, `reason` text, `created_by` uuid, `created_at` timestamptz.
    - `async def purge_document(conn, ledger_id: UUID) -> None`: deletes the document's `document_fields`, `extraction_run` and `correction_note` rows.
      - The triggers allow a DELETE only when `current_setting('sijil.purge', true) = OLD.ledger_id::text`, which `purge_document` sets with `SET LOCAL`.
      - The trigger still raises `retention.locked` (SJ001) for a `document_fields` row with `sealed_at IS NOT NULL`, unless `current_setting('sijil.purge_sealed', true) = OLD.ledger_id::text`, which only `purge_document(…, sealed=True)` sets.
      - The caller is responsible for the ledger state: removal purges an unsealed document; approved deletion purges a sealed one.
      - The services connect as the index owner (Task 3 ruling, backlog item). This GUC gate is no weaker than today's trigger. Say so in the docstring.
- [ ] **Step 1: Write the failing tests** in `tests/retention/test_terminal_states.py`, against the real Postgres through the app role, like the existing lock tests:
  - `test_sealed_row_cannot_become_removed`;
  - `test_removal_needs_reason_actor_time`;
  - `test_removed_row_is_frozen_except_original_removed_at`;
  - `test_deleting_refused_before_retention_ends`. With `retention_until` = today in Casablanca it is refused; set `retention_until` to yesterday by inserting a fresh row, since lowering is refused anyway, and it is allowed.
  - `test_deleted_needs_original_removed_at`;
  - `test_tombstone_hash_never_changes`, in every terminal state;
  - `test_ledger_row_never_deleted`, in every state.

  In `test_index_purge.py`:
  - `test_purge_removes_unsealed_rows`;
  - `test_plain_delete_still_refused`;
  - `test_purge_of_sealed_needs_sealed_flag`;
  - `test_correction_note_append_only`.
- [ ] **Step 2: Watch them fail.** Expected: unknown enum value, missing columns, or the trigger allowing what it should refuse.
- [ ] **Step 3: Implement** migration 0011, with a downgrade to the 0009 function, the models and index schema v2. Write the downgrade test (`test_0011_downgrade_restores_0009_guard`), as M3 did for 0009.
- [ ] **Step 4: Run the tests, then `make check`.** Expected: PASS.
- [ ] **Step 5: Commit** (two commits): `feat(cp): terminal ledger states removed, deleting, deleted with DB guards`, then `feat(core): index schema v2 with correction notes and a gated purge`.

### Task 3: The removal worker and its key

**Files:**
- Create:
  - `services/worker/src/sijil_worker/main_removal.py`, `services/worker/src/sijil_worker/removal.py`;
  - `libs/sijil-core/src/sijil_core/removal_settings.py` (or a `RemovalSettings` class next to `SealSettings`, matching where `SealSettings` lives).
- Modify:
  - `services/control-plane/src/sijil_cp/storage_admin.py` (policy `sijil-removal`; also fix the stale "only the control plane holds" docstrings, an M3 deferred minor);
  - `libs/sijil-core/src/sijil_core/storage.py` (`remove_original`);
  - `services/worker/src/sijil_worker/tasks.py`;
  - `infra/compose/docker-compose.yml` (`worker-removal`), `infra/compose/.env.example`, `infra/scripts/smoke.sh`;
  - `docs/architecture.md`.
- Test:
  - `services/control-plane/tests/test_storage_admin.py`;
  - `services/worker/tests/test_removal.py`;
  - `tests/integration/test_removal_key.py` (RustFS).

**Interfaces:**
- Produces:
  - Object-store user `SIJIL_OBJECTSTORE_REMOVAL_ACCESS_KEY` / `_SECRET_KEY`, with policy `sijil-removal`:
    - allow `s3:GetObjectRetention`, `s3:DeleteObjectVersion` and `s3:BypassGovernanceRetention` on `bucket/*`;
    - deny `s3:PutObject`, `s3:GetObject`, `s3:GetObjectVersion`, `s3:PutObjectRetention`, `s3:PutObjectLegalHold`, `s3:DeleteObject` (unversioned) and every bucket-configuration action, as in `SEAL_DENIED_ACTIONS`.

    It can delete a version under GOVERNANCE, or one whose lock has expired. It can never read bytes, write, or change a retention.
  - `ObjectStore.remove_original(ref: ObjectRef, *, governance: bool) -> None`.
    - With `governance=True` it deletes with `BypassGovernanceRetention`: the pre-seal removal.
    - With `governance=False` it deletes without the bypass: after retention.
    - Before deleting, it reads the current mode and date and refuses (`RuntimeError`, no call) if the mode is COMPLIANCE with a date at or after today. The store refuses too.
    - An absent version (`NoSuchVersion` / `NoSuchKey`) counts as already removed and returns normally.
    - It replaces `governance_remove`; delete that method and its callers.
  - Task `sijil.remove_original(ledger_id: str, workspace_id: str)` on queue `removal`, in `worker-removal`:
    1. `assert_region`.
    2. Lock the ledger row `FOR UPDATE`.
    3. Only state `removed` (`governance=True`) or `deleting` (`governance=False`) proceeds. Any other state logs, audits nothing and returns: a forged or stale message never deletes.
    4. Call `store.remove_original`.
    5. Set `original_removed_at`; for `deleting`, also `state = deleted` and `deleted_at`.
    6. Audit `document.original_removed` with `{mode: "governance" | "expired"}`.
    7. Commit.
    8. Enqueue `sijil.purge_document` (Task 4) on `default`.

    Retries: transient store errors get 8 retries with backoff (the M3 classification in `storage.py`). A final failure audits `document.removal_failed` with `{code}` and leaves the state as is.
  - The `requeue_unremoved` beat sweep, every 300 s, re-sends `remove_original` for rows in `removed` or `deleting` with `original_removed_at IS NULL`, active workspaces or not. It dedupes with `sijil_core.pending` key prefix `remove:pending:` (TTL 3600), keeping the key on an unrecorded failure as M3 does, and is capped at 50.
  - Compose `worker-removal`:
    - command `celery -A sijil_worker.main_removal worker -Q removal --concurrency 1 -n removal@%h`;
    - networks `[core]`;
    - environment: region, DB URL (app role) and the removal key only; no master key, no app object-store key, no seal key;
    - `smoke.sh` checks it is healthy, has no tenant network and holds no `SIJIL_OBJECTSTORE_APP_` or `SEAL_` variable.
  - The main `worker` and `worker-seal` env must NOT contain `SIJIL_OBJECTSTORE_REMOVAL_*`. A smoke check enforces it.
- [ ] **Step 1: Write the failing tests.**
  - `test_removal_policy_allows_only_delete_version_and_bypass`;
  - `test_remove_original_refuses_unexpired_compliance`;
  - `test_remove_task_ignores_rows_not_removed_or_deleting`, for each other state including `sealed`;
  - `test_remove_task_sets_original_removed_and_audits`;
  - `test_deleting_becomes_deleted_after_store_delete`;
  - `test_absent_version_counts_as_removed`;
  - `test_sweep_resends_unremoved_and_dedupes`.
  - Integration against RustFS: `test_removal_key_deletes_governance_version`, `test_removal_key_cannot_delete_compliance_version`, `test_removal_key_cannot_read_or_shorten`, `test_app_key_cannot_delete` (exists in M2: extend it to the removal path).
- [ ] **Step 2: Watch them fail.**
- [ ] **Step 3: Implement.** Make `bootstrap_storage` idempotent for the new user, like the seal user.
- [ ] **Step 4: Run the unit tests, then `make check`, `make up`, `make smoke` and the new integration file** (`docker compose … --profile test run --rm tests uv run --frozen pytest tests/integration/test_removal_key.py -v`). Expected: PASS.
- [ ] **Step 5: Commit** (small commits): `feat(infra): removal-only object-store user and worker-removal`, `feat(worker): remove_original task and requeue sweep`.

### Task 4: Pre-seal removal (plan 4.3)

**Files:**
- Create: `services/control-plane/src/sijil_cp/api/removal.py`
- Modify:
  - `services/control-plane/src/sijil_cp/permissions.py` (`Action.documents_remove`, owner only);
  - `services/worker/src/sijil_worker/tasks.py` (`sijil.purge_document`).
- Test: `services/control-plane/tests/test_removal_api.py`, `tests/retention/test_removal_paths.py`, `services/worker/tests/test_purge.py`

**Interfaces:**
- Consumes: Task 2's states, columns and `index.purge_document`; Task 3's `sijil.remove_original`.
- Produces:
  - `POST /v1/workspaces/{id}/documents/{doc_id}/remove`, body `{reason: str}` (10–1000 characters after trimming, else 422 `removal.reason_required`).
    - Needs `Action.documents_remove` (Owner only; Admin, Member and ReadOnly get 403 `permission.denied`) and `require_recent_totp(10)` (403 `auth.step_up_required`).
    - The handler:
      1. Takes the ledger row `FOR UPDATE`. `sealed` gives 409 `document.sealed_cannot_be_removed`. Already `removed`, `deleting` or `deleted` gives 409 `document.not_removable`.
      2. Also refuses with `document.sealed_cannot_be_removed` when the index row is frozen (a half-done seal, `seal_incomplete`): a document a person confirmed is never removed.
      3. Sets `state = removed`, `removed_at`, `removed_by` and `removal_reason`.
      4. Audits `document.removed` with `{reason_length: int}` and the target.
      5. Commits.
      6. After the commit, enqueues `sijil.remove_original`.
    - It returns 202 `{id, state: "removed"}`.
    - A `received` document still being ingested can be removed; the ingest pipeline must then skip it. `ingest.py` checks the state before each step that writes to Paperless, and a removed row stops there.
  - Task `sijil.purge_document(ledger_id, workspace_id)` on `default`, in the main worker:
    1. `assert_region`.
    2. Proceeds only if the ledger state is `removed` or `deleted` and `original_removed_at` is set.
    3. Paperless delete through the tenant admin credential (`PaperlessClient` built with the admin user's token). The service token has no delete right and must stay so.
    4. Then `POST /api/trash/` with `{"documents": [id], "action": "empty"}`, so Paperless keeps no copy; add `empty_trash` to `PaperlessClient`.
    5. `index.purge_document` (with `sealed=True` only for `deleted`).
    6. Audits `document.purged` with `{paperless: bool}`.

    An already-absent Paperless document (404) counts as deleted. A paused, closing or hibernated tenant retries later. Nothing here touches the object store.
  - Removed and deleted rows are excluded from the document list, the review queue, search sources and exports. `GET …/documents/{id}` still answers with a tombstone body: `id`, `state`, `sha256`, `removed_at` / `deleted_at`, plus `removal_reason` for Owners only.
- [ ] **Step 1: Write the failing tests.**
  - `test_owner_can_remove_unsealed_with_reason`;
  - `test_admin_cannot_remove` (and member, readonly);
  - `test_sealed_cannot_be_removed`;
  - `test_half_sealed_cannot_be_removed`;
  - `test_remove_requires_step_up`;
  - `test_remove_requires_reason`;
  - `test_tombstone_keeps_hash`: `sha256`, `object_key` and `object_version_id` are unchanged and visible on GET;
  - `test_remove_enqueues_after_commit`;
  - `test_removed_is_hidden_from_lists_and_review`;
  - `test_remove_races_seal_one_wins`: two concurrent requests; assert exactly one of {200 seal, 202 remove} and a consistent ledger;
  - `test_purge_deletes_in_paperless_with_admin_and_empties_trash`, with a fake Paperless recording calls;
  - `test_purge_refuses_unless_original_removed`;
  - `test_ingest_stops_on_removed`.
- [ ] **Step 2: Watch them fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests and `make check`.**
- [ ] **Step 5: Commit**: `feat(cp): owner pre-seal removal with step-up and tombstone`, `feat(worker): purge a removed document from Paperless and the index`.

### Task 5: End of retention (plan 4.4)

**Files:**
- Modify:
  - `services/control-plane/src/sijil_cp/api/retention.py`;
  - `services/control-plane/src/sijil_cp/permissions.py` (`Action.retention_approve_deletion`, owner only);
  - `services/worker/src/sijil_worker/tasks.py` (`sijil.mark_eligible`, daily in beat).
- Test: `services/worker/tests/test_eligibility.py`, `services/control-plane/tests/test_deletion_api.py`, `tests/retention/test_deletion_paths.py`

**Interfaces:**
- Produces:
  - Task `sijil.mark_eligible()`, daily at 02:00 in the region's time zone, all workspaces in this region:
    - sets `deletion_eligible_at = now` on rows whose `retention_until < region_local_date(now)`, whose state is not `removed`, `deleting` or `deleted`, and where `deletion_eligible_at IS NULL`;
    - audits one `retention.eligible` event per workspace with `{count}` when the count is above 0;
    - changes nothing else.

    `deletion_eligible_at` is a marker, not a guarded column; the DB guard on `deleting` checks `retention_until` itself.
  - `GET /v1/workspaces/{id}/retention/eligible?cursor&limit` (`retention_manage`) lists eligible documents (id, `retention_until`, state, document type), oldest `retention_until` first.
  - `POST /v1/workspaces/{id}/retention/approve-deletion`, body `{ids: [uuid, …]}` with 1 to 500 ids.
    - Needs `Action.retention_approve_deletion` (Owner) and `require_recent_totp(10)`.
    - All or nothing: lock every row `FOR UPDATE` in id order. If any is not eligible (retention not over by the region-local date, already terminal, or in another workspace), answer 409 `retention.not_eligible` with the offending `ids`, and change nothing.
    - Otherwise:
      1. Set `state = deleting`, `deletion_approved_at` and `deletion_approved_by` on each row.
      2. Audit one `document.deletion_approved` per document, plus a `retention.deletion_batch` event with `{count}`, all in one transaction.
      3. After the commit, enqueue `sijil.remove_original` for each.
    - Returns 202 `{approved: n}`.
  - Nothing deletes without that call: `test_nothing_purged_automatically` runs `mark_eligible` and the beat schedule's every task against an expired document and asserts its original, Paperless document and index row are all still present.
- [ ] **Step 1: Write the failing tests.**
  - `test_eligible_only_after_retention_until`, at the Casablanca date boundary with a frozen clock: 2036-12-31 23:30 local is not eligible; 2037-01-01 00:10 local is.
  - `test_nothing_purged_automatically`;
  - `test_mark_eligible_audits_count_once`;
  - `test_approve_deletion_owner_only_with_step_up`;
  - `test_approve_deletion_all_or_nothing`;
  - `test_approve_deletion_of_sealed_after_retention`: allowed; the original is COMPLIANCE-expired; the worker deletes; the state ends `deleted`.
  - `test_unexpired_compliance_stays_deleting`: forge the row's date past while the store's lock is still active. The worker refuses, audits `document.removal_failed`, and the state stays `deleting`.
- [ ] **Step 2: Watch them fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests and `make check`.**
- [ ] **Step 5: Commit**: `feat(worker): daily eligibility marking after retention`, `feat(cp): owner-approved deletion in batches`.

### Task 6: Correction notes (plan 4.5)

**Files:**
- Modify:
  - `services/control-plane/src/sijil_cp/api/review.py` (or a new `corrections.py` router, matching the file's size);
  - `libs/sijil-core/src/sijil_core/index.py` (`add_correction`, `read_corrections`).
- Test: `services/control-plane/tests/test_corrections_api.py`, `tests/retention/test_corrections_leave_seal.py`

**Interfaces:**
- Consumes: Task 2's `correction_note` table.
- Produces:
  - `POST /v1/workspaces/{id}/documents/{doc_id}/corrections`, body `{field, value: str | null, reason: str}`.
    - Permission `documents_review`.
    - `field` must be in `FIELD_NAMES` (422 `field.unknown`). `value` is parsed with `parse_field` (422 `field.invalid`). `reason` is 10–1000 characters (422 `correction.reason_required`).
    - An unsealed document gives 409 `correction.not_sealed` (edit the fields instead). A terminal state gives 409 `document.not_reviewable`.
    - It appends a `correction_note` and audits `document.corrected` with `{field}` only. Order: the CP audit commit first, then the index write; an index failure gives 503 `field.update_failed` (same as M3's PATCH).
  - `async def effective_fields(row: FieldsRow, corrections: list[Correction]) -> dict[str, tuple[str | None, bool]]`: for each field, its value and whether it is corrected. The latest note per field wins.
  - `GET …/documents/{id}/fields` gains `corrections: [{field, value, reason, created_by, created_at}]`, newest first. The sealed `fields` values are unchanged, and `fields_sha256` still matches them.
- [ ] **Step 1: Write the failing tests.**
  - `test_correction_keeps_original_value`: the sealed value and `fields_sha256` are unchanged, and the ledger row is untouched.
  - `test_correction_audited`: names the field, never the value.
  - `test_correction_on_unsealed_is_409`;
  - `test_correction_value_validated`;
  - `test_latest_correction_wins`;
  - `tests/retention/test_corrections_leave_seal.py::test_correction_never_changes_sealed_index_row`.
- [ ] **Step 2: Watch them fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests and `make check`.**
- [ ] **Step 5: Commit**: `feat(cp): correction notes on sealed documents`.

### Task 7: Signed period export (plan 4.6, ARC-5)

**Files:**
- Create:
  - `services/control-plane/alembic/versions/0012_export_job.py`, `services/control-plane/src/sijil_cp/api/exports.py`;
  - `libs/sijil-core/src/sijil_core/export_report.py` (report schema, canonical bytes, sign and verify: shared by the worker and the CLI);
  - `services/worker/src/sijil_worker/export.py`, `services/worker/src/sijil_worker/main_export.py`;
  - `infra/scripts/ensure-signing-key.sh`.
- Modify:
  - `services/control-plane/src/sijil_cp/cli.py` (`sijil-cp export verify`);
  - `services/control-plane/src/sijil_cp/storage_admin.py` (exports bucket and the `sijil-export` user);
  - `infra/compose/docker-compose.yml` (`worker-export`), `Makefile` (`up` runs `ensure-signing-key.sh`), `infra/scripts/smoke.sh`, `infra/compose/.env.example`.
- Test:
  - `libs/sijil-core/tests/test_export_report.py`;
  - `services/worker/tests/test_export.py`;
  - `services/control-plane/tests/test_exports_api.py`, `services/control-plane/tests/test_export_verify_cli.py`.

**Interfaces:**
- Produces:
  - `ExportJob` table:
    - `id` uuid;
    - `workspace_id`;
    - `period` text: `YYYY` or `YYYY-MM`;
    - `requested_by`;
    - `state` (`queued`, `building`, `ready`, `failed`);
    - `object_key`, `sha256`, `size_bytes`;
    - `document_count`;
    - `error_code`;
    - `created_at`, `ready_at`.

    The app role gets SELECT, INSERT and UPDATE.
  - A document is in the period when its `retention_basis(date field, ingested_at, region, now)` falls in it: the plausible document date, else the region-local arrival date. That is the same basis retention uses.
    - Included states: `received`, `needs_review`, `filed` and `sealed`. Removed, deleting and deleted documents are left out.
    - The `state` column in `index.csv` says which documents are sealed.
  - `POST /v1/workspaces/{id}/exports {period}`.
    - Needs `documents_export` plus `require_recent_totp(10)`.
    - A bad period gives 422 `export.period_invalid`. So does a future period, or one before the workspace was created.
    - It creates the job, audits `export.requested` with `{period}`, enqueues `sijil.build_export` on `export` after the commit, and returns 202 `{id, state}`.
  - `GET …/exports/{id}` returns the state.
  - `GET …/exports/{id}/download`: `ready` only (else 409 `export.not_ready`). It streams the ZIP through the control plane with the app key (read-only on the exports bucket) and audits `export.downloaded`. Same permission and step-up.
  - Task `sijil.build_export(job_id)` in `worker-export`:
    1. `assert_region`.
    2. Read one snapshot: a single REPEATABLE READ transaction for the ledger rows and the audit chain head (the workspace's last `audit_event` `id` and `hash`). The index rows come from one index-DB transaction opened right after; the index has no seal-time races left, because sealed rows are frozen.
    3. Stream each original from the object store into the ZIP at `originals/<ledger_id>.<ext>`, where `<ext>` comes from `content_kind`. Hash while streaming. A mismatch with the ledger `sha256` fails the job with `error_code = "export.hash_mismatch"` and audits `integrity.export_mismatch`.
    4. Write `index.csv`: UTF-8 with BOM, CRLF, RFC 4180 (`csv` module, `QUOTE_MINIMAL`). Columns, in this order:
       - `document_id`, `state`, `sha256`, `size_bytes`, `content_kind`, `source`, `ingested_at`, `sealed_at`, `fields_sha256`, `retention_until`;
       - then every name in `FIELD_NAMES`, holding the effective value (Task 6);
       - then `corrected_fields`: the corrected field names joined with `;`, empty if none.
    5. Write `integrity-report.json` (canonical JSON, `sijil_core.audit.canonical_json`):
       ```
       {"v": 1, "workspace_id", "region", "period", "generated_at",
        "documents": [{"id", "path", "sha256", "size_bytes", "fields_sha256"}],
        "index_csv_sha256", "audit_head": {"id", "hash"},
        "key_id"}
       ```
       `key_id` is the first 16 hex characters of the SHA-256 of the raw public key.
    6. Write `integrity-report.json.sig`: the raw 64-byte Ed25519 signature over the report file's exact bytes, base64.
    7. Put the ZIP in the exports bucket at `<workspace_id>/<job_id>.zip`.
    8. Set the job `ready`, and audit `export.ready` with `{period, document_count}`.
  - Exports bucket `<SIJIL_OBJECTSTORE_BUCKET>-exports`: versioning off, no object lock, created by `bootstrap_storage`.
    - The `sijil-export` user: `GetObject` on the originals bucket (read only) and `PutObject` / `GetObject` / `DeleteObject` on the exports bucket.
    - The app user gains `GetObject` on the exports bucket only.
    - A daily task in `worker-export` deletes exports older than 7 days and audits `export.expired` per job.
  - Signing key:
    - `SIJIL_EXPORT_SIGNING_KEY`: base64 of the raw 32-byte Ed25519 seed. Only `worker-export` has it.
    - `SIJIL_EXPORT_PUBLIC_KEY`: base64 of the raw 32 bytes. The control plane has it, and so does the CLI.
    - `ensure-signing-key.sh` generates the pair into `infra/compose/.env` on first `make up`, if absent, and never overwrites an existing pair. The keys in `.env.example` are empty placeholders.
    - `GET /v1/regions/{region}/export-key` (unauthenticated) returns `{region, key_id, public_key}`: the published key.
  - `sijil-export-report` functions in `sijil_core.export_report`:
    - `build_report(…) -> bytes`;
    - `sign(report: bytes, seed: bytes) -> bytes`;
    - `verify_zip(path: Path, public_key: bytes) -> VerifyOutcome(ok: bool, problems: list[str])`. Problem codes:
      - `signature.invalid`;
      - `file.missing:<path>`, `file.extra:<path>`, `file.hash:<path>`;
      - `index_csv.hash`;
      - `key_id.mismatch`.
  - CLI `sijil-cp export verify <zip> [--public-key B64] [--against-ledger]`:
    - with no `--public-key`, it uses `SIJIL_EXPORT_PUBLIC_KEY`;
    - `--against-ledger` also compares each document's `sha256` and `fields_sha256` with the ledger (DB access), adding `ledger.mismatch:<id>` problems;
    - exit 0 when ok, 1 otherwise, printing one problem code per line.
  - Compose `worker-export`:
    - `-Q export --concurrency 1`;
    - `[core]` network only;
    - env: region, DB URL (app role), the export object-store key, the master key (to open the index DBs, the same way the control plane does) and the signing seed;
    - no Docker socket.

    The main worker, CP and `worker-seal` must not hold the seed (smoke check).
- [ ] **Step 1: Write the failing tests.**
  - Report (`libs/sijil-core/tests/test_export_report.py`):
    - `test_report_is_canonical_and_signed`;
    - `test_verify_detects_each_problem`: one case per problem code.
  - Export worker (`services/worker/tests/test_export.py`, with the memory store and a test key):
    - `test_export_contains_all_period_docs`, including a document dated in the period that arrived after it, and excluding a removed one;
    - `test_index_csv_bom_crlf_arabic_and_quotes`;
    - `test_corrected_value_flagged`;
    - `test_hash_mismatch_fails_job`.
  - Control plane:
    - `test_integrity_report_verifies` (end to end through `verify_zip` and the CLI);
    - `test_tampered_zip_fails_verify`: change one byte of an original; separately, edit one CSV cell; separately, swap the signature;
    - `test_export_requires_step_up_and_is_audited`;
    - `test_download_not_ready_is_409`;
    - `test_export_key_endpoint_matches_signing_key`.
- [ ] **Step 2: Watch them fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests, then `make check`, `make up` and `make smoke`. Then, against the stack:**
  1. `make seed`.
  2. `POST` an export for the current month.
  3. Download it.
  4. `uv run sijil-cp export verify <zip> --against-ledger` exits 0.
- [ ] **Step 5: Commit** (small commits): `feat(core): signed integrity report and zip verification`, `feat(infra): export-only worker, exports bucket and region signing key`, `feat(worker): build period exports from the object store`, `feat(cp): export API, published key and export verify CLI`.

### Task 8: Lock proof suite and M4 acceptance (plan 4.2)

**Files:**
- Create: `tests/retention/test_lock_all_paths.py`, `tests/integration/test_m4_acceptance.py`
- Modify:
  - `docs/architecture.md`: the M4 section, the key holders, the removal and deletion flows, the export format;
  - `CLAUDE.md`: worker list and commands;
  - `docs/plan.md`: M4 marked done, with a pointer to this plan.
- Test: those files.

**Interfaces:**
- Consumes: every earlier task.
- Produces:
  - `test_lock_all_paths.py`, parametrised over roles {Owner, Admin, Member, ReadOnly} × paths × {sealed, unsealed}. The paths:
    1. **API delete:** `POST …/remove`, plus `DELETE …/documents/{id}`. That route doesn't exist, so it must be 404 or 405, never 2xx.
    2. **API field edit:** `PATCH …/fields`.
    3. **API approve deletion** before retention ends.
    4. **Paperless service-token delete:** `DELETE /api/documents/{id}/` with the tenant service token. It must be refused (403). This is integration-marked, against a live tenant.
    5. **Direct SQL on the ledger** through the app role: DELETE, an UPDATE of `sha256`, an UPDATE of `state` to `removed` without the tombstone columns, and an UPDATE of a sealed row's `fields_sha256`.
    6. **Object-store delete** with the app key, and with the seal key. With the removal key: the original of a sealed (COMPLIANCE) document. Integration-marked, against RustFS.
    7. **Index direct SQL:** UPDATE or DELETE of a sealed `document_fields` row without the purge GUC.

    Expected: every combination is refused for sealed documents. For unsealed ones, only {Owner, `POST …/remove`} succeeds. Every other path is refused, including the Owner's direct SQL and object-store paths, which have no role.
  - Each refused API path writes nothing to the ledger, the index or the store. A refused removal or deletion attempt through the API is audited as `document.removal_refused` with `{code}` (design §5.5: "every refused deletion is logged").
    - Wire this in Task 4's and Task 5's handlers if they aren't already; this task tests it.
  - The `test_m4_acceptance.py` path:
    1. A fresh workspace.
    2. Seed 4 SAMPLE invoices through the API.
    3. Wait until they are routed (no model needed: use `filed`/`needs_review` as M3 left them; mark it `slow` only if it needs the model). Prefer seeding fields directly through the index, as `tests/support/sealing.py` does, so the test stays in `make itest`.
    4. Seal two documents.
    5. The Owner removes one unsealed document with step-up. Within 120 s, its object-store version is gone, its Paperless document is gone, and its index rows are gone. The tombstone keeps `sha256`.
    6. Every role fails to remove a sealed one.
    7. Add a correction note to a sealed one.
    8. Export the month. Download it. `verify_zip` is ok, and `--against-ledger` is ok.
    9. The CSV flags the corrected field, and the removed document is absent from it.
    10. Tamper one byte, and verification fails.
    11. The audit chain verifies (`sijil-cp audit verify`).
- [ ] **Step 1: Write the suite and the acceptance test.**
- [ ] **Step 2: Run them.** Expected: any combination that passes when it should be refused is a defect in an earlier task. Fix it there, with its own failing test first.
- [ ] **Step 3: Run `make check`, `make up`, `make smoke` and `make itest`.** Expected: all green, M1–M3 acceptance included.
- [ ] **Step 4: Update the docs.**
- [ ] **Step 5: Commit**: `test: retention lock proof across roles and paths`, `test: M4 acceptance`, `docs: M4 retention, removal, deletion and export`.
