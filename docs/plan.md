# Sijil MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Sijil Morocco MVP (without the accountant portal): a compliance archive in which a document is captured, hashed, recognised, reviewed, sealed, locked for its retention period, and exported with a signed integrity report. It runs on an unmodified Paperless-ngx 3.4.1 per workspace.

**Architecture:**
- A Next.js web app talks only to a FastAPI control plane (`/v1`). Every request goes through permission, audit and region checks there.
- A Celery worker provisions one Paperless-ngx container per workspace through a `TenantRuntime` interface, polls its tasks, keeps a write-once copy of each original in object storage, and runs our own extraction on in-region Ollama.
- The control plane owns the ledger, the hash-chained audit log, retention and export. Document content stays in per-workspace stores.

**Tech Stack:**
- Python 3.13 (uv workspace): FastAPI, SQLAlchemy 2, Alembic, Pydantic v2, Celery, Redis, PostgreSQL 17 + pgvector, RustFS (S3 with object lock), Ollama.
- TypeScript (pnpm workspace): Next.js 15 App Router, next-intl.
- Tests and checks: pytest, Vitest, Playwright, ruff, mypy, eslint.
- CI and infra: GitHub Actions, docker-compose.

**Spec:** `SPEC.md`, as amended by `docs/superpowers/specs/2026-10-31-sijil-mvp-design.md` (the design wins where they differ). Executors read both.

## How this plan is organised

- **Milestone 0 is planned to the step**, below.
- **Milestones 1–7 are planned to the task:** each task lists its files, interfaces, acceptance tests and requirement IDs.
- At the start of each later milestone, its tasks are expanded to the step in `docs/superpowers/plans/<date>-m<N>-<name>.md`, using writing-plans against the code that exists by then. Each milestone runs on its own branch (`m<N>-<name>`) in a git worktree.
- Stop points: **after M0** (show the plan and skeleton), and **after M2** (review).

## Global Constraints

- Paperless-ngx: image `ghcr.io/paperless-ngx/paperless-ngx:3.4.1`.
  - Configured only by environment variables.
  - Never patched, forked or copied into this repo.
  - Reached only by the control plane and the worker, over REST, on the private `sijil-tenants` network with no published ports.
- The web app and site call only the control-plane `/v1` API. There is no Paperless URL in any JS package (enforced by a lint test in M0).
- Region:
  - `SIJIL_REGION` is a required setting for every service. Dev region: `ma-cas`.
  - `Organisation.region` never changes.
  - Every request and job asserts that the workspace's region equals `SIJIL_REGION`.
  - There is no outbound call carrying document content to a host outside the region. In dev, the only AI or OCR endpoints are the compose `ollama` service and Paperless's own Tesseract.
- Retention lock (four layers): API refusal; Paperless service user without delete right; database constraint on sealed ledger rows; object lock on the original. Every role (Owner, Admin, Member, ReadOnly) is tested against every layer.
- Integrity:
  - Every ingested file gets a SHA-256 hash.
  - `AuditEvent.hash = sha256(prev_hash ‖ canonical_json(event))`, chained per workspace. `prev_hash` for the first event is 64 × `"0"`.
  - Canonical JSON: sorted keys, no whitespace, UTF-8, ISO-8601 UTC timestamps with a `Z`.
- Confidence threshold `0.90`. Moroccan VAT rates `{20, 14, 10, 7}`. ICE = exactly 15 digits. Morocco retention preset = 10 years after the end of the fiscal year (calendar year by default).
- Locales: `fr` (default), `ar` (RTL), `en`.
  - No literal user-facing strings in JSX (eslint `react/jsx-no-literals`) or in API responses. The API returns machine error codes such as `retention.locked`, and the web app translates them.
  - Digits: the user preference is `latn` or `arab`.
- Uploads: up to 100 MB per file. Accepted types: PDF, JPG, PNG, TIFF, HEIC, DOCX, XLSX, XML.
- Sample data lives only in `fixtures/`, every file carries the `SAMPLE` mark, and ICE numbers are fictitious.
- Python 3.13, Node 24 LTS in CI (newer locally is fine), pnpm 12, uv 0.10+, TypeScript 5.9, ESLint 9.
- Commits are small and in the Conventional Commits style, on the milestone branch. Never push to `main` without asking.

## Review Focus

Failure modes the design implies but no happy-path test exercises. Each line has a test in the task named.

1. **The same file uploaded twice at the same moment** (a double click, or email and web together) must create exactly one ledger row. → M2 T2.3 `test_concurrent_identical_uploads_create_one_row` (unique `(workspace_id, sha256)` + `ON CONFLICT`).
2. **Two audit events written at once in one workspace** must not fork the chain. → M2 T2.5 `test_parallel_appends_keep_single_chain` (per-workspace advisory lock).
3. **A Paperless-ngx tenant that is down or slow** when a user uploads: the upload is accepted (< 2 s), hashed and stored in the object store, then queued and retried. It is never lost. → M2 T2.3 `test_upload_accepted_when_tenant_unreachable`.
4. **Arabic-Indic digits, French decimal commas and spaces in amounts** (`١٢٠٫٠٠`, `1 200,00`) in extracted text must parse to the same `Decimal`, and the HT+VAT=TTC check must use `Decimal`, never float. → M3 T3.2 `test_parse_amount_variants`.
5. **A retained document deleted through a path we didn't build**, for example a direct Paperless call with the admin password, must be detected by the drift check and raise an alert. → M2 T2.6 `test_drift_check_flags_missing_paperless_document`.

---

## Repository structure (fixed in M0)

```
sijil/
├── apps/
│   ├── web/                    # Next.js 15 customer app (next-intl, fr/ar/en, RTL)
│   └── site/                   # Next.js 15 marketing site (static export)
├── packages/
│   ├── i18n/                   # shared message catalogues messages/{fr,ar,en}.json + key-parity test
│   └── api-client/             # TS types generated from control-plane OpenAPI (openapi-typescript)
├── services/
│   ├── control-plane/          # uv member: sijil_cp (FastAPI, SQLAlchemy, Alembic)
│   └── worker/                 # uv member: sijil_worker (Celery)
├── libs/
│   └── sijil-core/             # uv member: sijil_core — settings, region guard, crypto, Paperless client (shared by cp + worker)
├── infra/
│   ├── compose/                # docker-compose.yml, env files, postgres init, paperless tenant env
│   └── scripts/                # smoke.sh, seed.sh
├── fixtures/                   # SAMPLE data only (README states it)
├── docs/                       # plan.md, architecture.md, superpowers/{specs,plans}
├── .github/workflows/ci.yml
├── CLAUDE.md
├── pyproject.toml              # uv workspace root + ruff/mypy config
├── pnpm-workspace.yaml
└── package.json
```

---

## Milestone 0: Plan and skeleton

Branch `m0-skeleton`. Exit: `docker compose up` boots every service healthy, `make check` passes locally, CI is green, and the docs are written.

### Task 0.1: Workspace roots, conventions, CLAUDE.md

**Files:**
- Create: `.gitignore`, `.editorconfig`, `pyproject.toml` (uv workspace `members = ["libs/*", "services/*"]`, ruff with `line-length = 100` and rules `E,F,I,B,UP,S,ASYNC`, mypy `strict = true`), `.python-version` (`3.13`), `package.json` (scripts `lint`, `typecheck`, `test`), `pnpm-workspace.yaml` (`apps/*`, `packages/*`), `Makefile` (targets `install`, `lint`, `typecheck`, `test`, `check`, `up`, `down`, `smoke`), `CLAUDE.md`

**Interfaces:**
- Produces: `make check` = lint + typecheck + test for both languages. Every later task adds to it without renaming targets.

- [ ] **Step 1:** Write the root files. `CLAUDE.md` records:
  - the layout above and the Global Constraints;
  - test commands, TDD and branch-per-milestone;
  - machine error codes in the API and translated strings only in `packages/i18n`;
  - `Decimal` for money;
  - `sijil_core` for anything both Python services use;
  - "never call Paperless from JS";
  - the fixtures rule.
- [ ] **Step 2:** Run `uv sync && pnpm install`. Expected: both succeed with empty members.
- [ ] **Step 3:** Commit `chore: add workspace roots and conventions`.

### Task 0.2: sijil-core settings and region guard

**Files:**
- Create: `libs/sijil-core/pyproject.toml`, `libs/sijil-core/src/sijil_core/{__init__,settings,region}.py`
- Test: `libs/sijil-core/tests/test_region.py`, `libs/sijil-core/tests/test_settings.py`

**Interfaces:**
- Produces:
  - `class CoreSettings(BaseSettings)` with `region: str` (env `SIJIL_REGION`, required) and `environment: Literal["dev","test","prod"]`.
  - `class RegionMismatch(Exception)` with attribute `code = "region.mismatch"`.
  - `def assert_region(resource_region: str, settings: CoreSettings) -> None`.

- [ ] **Step 1: Write failing tests**

```python
def test_settings_require_region(monkeypatch):
    monkeypatch.delenv("SIJIL_REGION", raising=False)
    with pytest.raises(ValidationError):
        CoreSettings()


def test_assert_region_passes_same_region():
    assert_region("ma-cas", CoreSettings(region="ma-cas", environment="test"))


def test_assert_region_refuses_other_region():
    with pytest.raises(RegionMismatch) as e:
        assert_region("eu-fra", CoreSettings(region="ma-cas", environment="test"))
    assert e.value.code == "region.mismatch"
```

- [ ] **Step 2:** Run `uv run pytest libs/sijil-core -v`. Expected: FAIL (import error).
- [ ] **Step 3:** Implement with `pydantic-settings`.
- [ ] **Step 4:** Run `uv run pytest libs/sijil-core -v && uv run mypy libs/sijil-core`. Expected: PASS, no mypy errors.
- [ ] **Step 5:** Commit `feat(core): add settings and region guard`.

### Task 0.3: Control-plane skeleton

**Files:**
- Create: `services/control-plane/pyproject.toml` (depends on `sijil-core`), `src/sijil_cp/{__init__,main,db}.py`, `src/sijil_cp/api/health.py`, `alembic.ini`, `alembic/env.py`, `alembic/versions/0001_baseline.py` (empty), `Dockerfile` (python:3.13-slim, uv, non-root user `sijil`)
- Test: `services/control-plane/tests/{conftest,test_health}.py`

**Interfaces:**
- Consumes: `CoreSettings`.
- Produces:
  - `create_app(settings: CpSettings) -> FastAPI`, where `CpSettings(CoreSettings)` adds `database_url: PostgresDsn` and `redis_url`.
  - `GET /v1/health` → `200 {"status": "ok", "region": "<SIJIL_REGION>", "version": "<pkg version>"}`.
  - `GET /v1/health/ready` → `200` when the database and Redis respond, otherwise `503 {"status": "unavailable", "checks": {...}}`.
  - `sijil_cp.db.session()` (async SQLAlchemy 2 session factory).
  - The OpenAPI schema at `/v1/openapi.json`.

- [ ] **Step 1: Write failing tests**
  - `test_health_reports_region`: GET `/v1/health` returns 200 and `region == "ma-cas"`.
  - `test_ready_returns_503_when_db_down`: with `database_url` pointing at a closed port, the response is 503 and `checks.database == "down"`.
  - `test_openapi_served_under_v1`: `/v1/openapi.json` is 200.
- [ ] **Step 2:** Run `uv run pytest services/control-plane -v`. Expected: FAIL.
- [ ] **Step 3:** Implement `create_app`, the health router, async engine and Alembic env (the URL comes from `CpSettings`).
- [ ] **Step 4:** Run the tests, then `uv run alembic -c services/control-plane/alembic.ini upgrade head` against the compose Postgres. Expected: PASS, and the migration applies.
- [ ] **Step 5:** Commit `feat(cp): add FastAPI skeleton with health and alembic baseline`.

### Task 0.4: Worker skeleton

**Files:**
- Create: `services/worker/pyproject.toml`, `src/sijil_worker/{__init__,app,tasks}.py`, `Dockerfile`
- Test: `services/worker/tests/test_ping.py`

**Interfaces:**
- Produces:
  - `celery_app` (broker and backend = `redis_url`), with queues `default`, `provisioning`, `ingest` and `extraction`, declared now and used from M1.
  - Task `sijil.ping() -> {"pong": True, "region": str}`.

- [ ] **Step 1:** Write the failing test `test_ping_returns_region`: with `task_always_eager=True`, `ping.delay().get()` returns `{"pong": True, "region": "ma-cas"}`.
- [ ] **Step 2:** Run `uv run pytest services/worker -v`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the test. Expected: PASS.
- [ ] **Step 5:** Commit `feat(worker): add celery skeleton with ping task`.

### Task 0.5: i18n package and web app shell

**Files:**
- Create:
  - `packages/i18n/{package.json,index.ts,messages/fr.json,messages/ar.json,messages/en.json}`
  - `packages/i18n/tests/parity.test.ts`
  - `apps/web/` (Next.js 15 App Router, TypeScript strict): `app/[locale]/layout.tsx`, `app/[locale]/page.tsx`, `i18n/routing.ts` (next-intl, locales `["fr","ar","en"]`, default `fr`), `middleware.ts`, `eslint.config.mjs`, `vitest.config.ts`, `playwright.config.ts`
- Test: `apps/web/tests/e2e/shell.spec.ts`, `apps/web/tests/no-paperless.test.ts`

**Interfaces:**
- Produces:
  - `locales`, `defaultLocale`, `dirFor(locale): "rtl" | "ltr"` from `@sijil/i18n`.
  - The layout sets `<html lang dir>`.
  - The ESLint config enforces `react/jsx-no-literals` (error) for every later screen.

- [ ] **Step 1: Write failing tests**
  - `parity.test.ts`: the key sets of `fr`, `ar` and `en` are identical, and no value is an empty string.
  - `no-paperless.test.ts`: no file under `apps/` or `packages/` matches `/paperless/i` in code (catalogues and comments excluded).
  - `shell.spec.ts` (Playwright):
    - `/ar` has `html[dir="rtl"][lang="ar"]`;
    - `/fr` has `dir="ltr"`;
    - `/` redirects to `/fr`;
    - the visible heading text equals `messages.<locale>.shell.title`.
- [ ] **Step 2:** Run `pnpm -r test && pnpm --filter web exec playwright test`. Expected: FAIL.
- [ ] **Step 3:** Implement. The page shows only `shell.title` and `shell.subtitle`, with no styling beyond resets. The visual identity is M7's job.
- [ ] **Step 4:** Run the tests again, then `pnpm -r lint` and `pnpm -r typecheck`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(web): add next-intl shell with fr/ar/en and rtl`.

### Task 0.6: Marketing site shell

**Files:**
- Create: `apps/site/` (Next.js 15, `output: "export"`, same next-intl setup and ESLint config, using `@sijil/i18n` under the `site.*` namespace)
- Test: `apps/site/tests/e2e/shell.spec.ts`

- [ ] **Step 1:** Write a failing test: `/ar/` has `dir="rtl"`, and `next build` produces `out/fr/index.html`, `out/ar/index.html` and `out/en/index.html`.
- [ ] **Step 2:** Run it. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5:** Commit `feat(site): add static marketing shell`.

### Task 0.7: docker-compose that boots everything

**Files:**
- Create:
  - `infra/compose/docker-compose.yml`
  - `infra/compose/.env.example`
  - `infra/compose/postgres/init.sql` (databases `sijil_cp`, `paperless_t1`, `paperless_t2`, `index_t1`, `index_t2`; `CREATE EXTENSION vector` in the `index_*` databases; roles per database)
  - `infra/compose/paperless/tenant.env` (shared tenant config)
  - `infra/scripts/smoke.sh`

**Services:**

| Service | Image | Networks | Notes |
| --- | --- | --- | --- |
| postgres | `pgvector/pgvector:pg17` | core, tenants | healthcheck `pg_isready` |
| redis | `redis:7.4-alpine` | core | |
| paperless-redis | `redis:7.4-alpine` | tenants | Paperless broker, separate from ours |
| objectstore | `rustfs/rustfs:1.0.4` | core | S3 API with object lock; the locked bucket is created by `ObjectStore` in M2 |
| ollama | `ollama/ollama` (pinned) | core, tenants | volume `ollama`, no model pull at boot (`make models` pulls) |
| tika | `apache/tika` (pinned) | tenants | for DOCX/XLSX |
| gotenberg | `gotenberg/gotenberg:8` | tenants | for DOCX/XLSX |
| paperless-t1, paperless-t2 | `ghcr.io/paperless-ngx/paperless-ngx:3.4.1` | tenants only | `env_file: paperless/tenant.env` + per-tenant `PAPERLESS_DBNAME`, `PAPERLESS_SECRET_KEY`, `PAPERLESS_ADMIN_USER/PASSWORD`; `PAPERLESS_OCR_LANGUAGES=ara fra`, `PAPERLESS_OCR_LANGUAGE=ara+fra+eng`, `PAPERLESS_TIKA_ENABLED=1`; volumes `t1_data`, `t1_media`; **no `ports:`** |
| control-plane | build | core, tenants | port `8000` |
| worker | build | core, tenants | the Docker socket mount for `DockerRuntime` arrives with M1 |

- Networks: `tenants` is `internal: true`, so tenants have no route out and no published ports. `core` is a normal bridge network, because the control-plane port is published on it.
- Arabic OCR: `tessdata/ara.traineddata` (tessdata_fast 4.1.0, checksummed) is mounted read-only into each tenant.

- [ ] **Step 1:** Write `smoke.sh`. It waits up to 300 s, then asserts:
  - `curl localhost:8000/v1/health/ready` → 200;
  - from inside the control-plane container, `GET http://paperless-t1:8000/api/` and the same for `t2` → 200 or 401 (alive);
  - `curl localhost:8000` cannot reach Paperless from the host: `docker compose port paperless-t1 8000` prints nothing;
  - `ollama list` succeeds.

  Exit non-zero on any failure.
- [ ] **Step 2:** Run `make up && make smoke` before the compose file is complete. Expected: FAIL.
- [ ] **Step 3:** Complete the compose file. Pin every image to an exact tag.
- [ ] **Step 4:** Run `make down -v && make up && make smoke`. Expected: `smoke: all checks passed`.
- [ ] **Step 5:** Commit `feat(infra): compose stack with two sample paperless tenants`.

### Task 0.8: Sample fixtures scaffold

**Files:**
- Create: `fixtures/README.md` (states: **SAMPLE DATA — fictitious companies, fictitious ICE numbers, not real documents**), `fixtures/suppliers.json`, `fixtures/invoices/manifest.json`, `fixtures/generate.py`
- Test: `fixtures/tests/test_fixtures.py`

**Interfaces:**
- Produces:
  - `suppliers.json`: `[{name, name_ar, ice, city, vat_rate}]`, 8 fictitious suppliers, half with Arabic names.
  - `manifest.json`: `[{file, lang, supplier_ice, number, date, ht, tva, ttc, currency:"MAD"}]`, 12 invoices (6 fr, 6 ar).
  - `generate.py`: renders each manifest entry to `invoices/<file>.pdf` with a visible "SAMPLE / ÉCHANTILLON / عينة" watermark.

- [ ] **Step 1:** Write failing tests:
  - every ICE has 15 digits;
  - for every invoice, `ht + tva == ttc` and `tva == round(ht * rate/100, 2)` using `Decimal`;
  - each rate is in `{20,14,10,7}`;
  - every PDF exists and its text contains `SAMPLE`.
- [ ] **Step 2:** Run `uv run pytest fixtures -v`. Expected: FAIL.
- [ ] **Step 3:** Write the data and generator (reportlab, with an Arabic font such as Noto Naskh Arabic, OFL, vendored under `fixtures/fonts/`, shaped with `arabic-reshaper` + `python-bidi`).
- [ ] **Step 4:** Run `uv run python fixtures/generate.py && uv run pytest fixtures -v`. Expected: PASS.
- [ ] **Step 5:** Commit `chore(fixtures): add sample Moroccan suppliers and invoices`.

### Task 0.9: CI

**Files:**
- Create: `.github/workflows/ci.yml`

**Jobs:**
- `python`: uv sync, `ruff check`, `ruff format --check`, `mypy`, `pytest`, with a `pgvector/pgvector:pg17` service and Redis.
- `js`: pnpm install, `lint`, `typecheck`, `vitest`, Playwright on `web` and `site` (chromium).
- `compose`: `docker compose -f infra/compose/docker-compose.yml config -q`.
- `security`: `pip-audit`, `pnpm audit --prod --audit-level high`, and the Trivy fs scan plus scans of the cp and worker images (fail on CRITICAL).

- [ ] **Step 1:** Write the workflow.
- [ ] **Step 2:** Verify locally with `make check`. Expected: PASS. CI itself runs once a GitHub remote exists. That needs the product owner, and nothing is pushed without asking.
- [ ] **Step 3:** Commit `ci: lint, typecheck, test, compose validation and scans`.

### Task 0.10: Architecture document

**Files:**
- Create: `docs/architecture.md`

- [ ] **Step 1:** Write it:
  - a component diagram (Mermaid);
  - trust boundaries (browser → cp; cp/worker → tenants network; nothing else reaches Paperless);
  - the data placement table (shared control-plane DB / per-workspace index DB / Paperless DB / object store, and which holds document content);
  - the four retention-lock layers;
  - the audit-chain formula;
  - a sequence for upload → ledger → Paperless → extraction → review → seal;
  - region enforcement;
  - the `TenantRuntime` lifecycle.

  Link the design doc instead of repeating it.
- [ ] **Step 2:** Commit `docs: add architecture`.

**M0 acceptance:**
- `make check` passes.
- `make up && make smoke` passes on a clean machine.
- `docs/plan.md`, `docs/architecture.md` and `CLAUDE.md` exist.
- Stop and show the product owner.

---

## Milestone 1: Tenancy

Branch `m1-tenancy`. Covers **ADM-1 (roles), ADM-2**, NFR tenant isolation, residency and encryption (local key service).

| Task | Deliverable | Acceptance tests |
| --- | --- | --- |
| 1.1 Models + migration | `Organisation(type ∈ {business, firm}, country, region immutable)`, `Workspace(state ∈ provisioning/active/hibernated/closing, paperless_url, db_name, index_db_name, kms_key_id)`, `User(email, name, locale, digits, totp_enabled)`, `Membership(role ∈ Owner/Admin/Member/ReadOnly)`, `Session`, `TenantCredential(ciphertext, nonce, wrapped_key)` | `test_org_region_cannot_change` (DB trigger + API 409 `org.region_immutable`); `test_membership_role_enum` |
| 1.2 Crypto + local key service | `sijil_core.crypto`: `KeyService` protocol (`generate_data_key(key_id) -> (plaintext, wrapped)`, `unwrap(key_id, wrapped)`), `LocalKeyService` (master key from file per region), `seal(plaintext, KeyService, key_id) -> Envelope`, `open_(Envelope, ...)`, AES-256-GCM | `test_roundtrip`; `test_tampered_ciphertext_fails`; `test_other_tenant_key_cannot_open` |
| 1.3 Auth | Signup, login, logout; argon2id; server-side sessions (hashed token, `HttpOnly; Secure; SameSite=Lax`); CSRF double-submit; rate limit on login | `test_password_never_stored_plain`; `test_session_cookie_flags`; `test_revoked_session_rejected`; `test_csrf_required_on_post` |
| 1.4 TOTP | Enrol (otpauth URI), verify, 10 one-time recovery codes; owner-enforced 2FA per org; step-up decorator `require_recent_totp(minutes=10)` | `test_login_requires_totp_when_enabled`; `test_owner_enforcement_blocks_users_without_totp`; `test_recovery_code_single_use`; `test_step_up_required_for_role_change` |
| 1.5 Permissions | `authorize(user, workspace, action)` with a role × action matrix as data; FastAPI dependency; region guard on every workspace route | `test_role_matrix` (parametrised over all roles × actions); `test_cross_workspace_access_404`; `test_cross_region_workspace_refused` (fake `eu-fra` workspace) |
| 1.6 Paperless client | `sijil_core.paperless.PaperlessClient(base_url, token)`: `ping`, `create_user`, `create_token`, `upload`, `task_status`, `get_document`, `list_documents`, `set_custom_fields`; httpx with timeouts and retries; contract-tested against the real 3.4.1 container | `test_client_against_live_tenant` (marked `integration`, runs in CI with a compose tenant) |
| 1.7 TenantRuntime + provisioning | `TenantRuntime` protocol; `DockerRuntime` (creates DBs, volumes and a container on `sijil-tenants`, with no ports); Celery `provision_workspace(workspace_id)`: create the tenant → wait healthy → create the service user (view/add/change, **no delete**) and the read-only support user → take the token → store it encrypted → state `active`; `POST /v1/orgs`, `POST /v1/workspaces` return a job id; `GET /v1/jobs/{id}` | `test_provision_creates_running_tenant` (integration); `test_service_token_cannot_delete` (DELETE via token → 403 from Paperless); `test_support_user_read_only`; `test_token_stored_encrypted`; `test_provision_is_idempotent_on_retry` |

**M1 acceptance:** a user signs up, enrols TOTP, creates a business organisation and workspace, and within 3 minutes the workspace is `active` with its own Paperless container, database and encrypted token. All of the above tests pass.

## Milestone 2: Capture and ledger (stop for review)

Branch `m2-capture`. Covers **CAP-1, CAP-2, CAP-4, ARC-4**.

| Task | Deliverable | Acceptance tests |
| --- | --- | --- |
| 2.1 Object store | `sijil_core.storage.ObjectStore` protocol + `S3ObjectStore` (boto3; RustFS locally); `put_original(sha256, bytes, retain_until)` in COMPLIANCE mode; `extend_retention`; `governance_remove` (used only by the pre-seal removal flow, M4) | `test_locked_object_cannot_be_deleted`; `test_retention_can_only_extend` |
| 2.2 Ledger model | `DocumentLedger` (design §5.5): states, `sha256`, `object_key`, `fields_sha256`, `retention_until` (provisional), unique `(workspace_id, sha256)`; DB trigger rejecting UPDATE of `sha256`/`object_key` and DELETE of any row whose state ≠ `removed` tombstone flow | `test_ledger_sha_immutable`; `test_ledger_row_delete_rejected_by_db` |
| 2.3 Upload API | `POST /v1/workspaces/{id}/uploads` (multipart, ≤ 100 MB, MIME sniffing on the allow-list) → hash while streaming → object store → ledger `received` → enqueue `ingest` → `202 {task_id}` in < 2 s; worker pushes the file to Paperless and polls the task; `paperless_doc_id` stored | `test_upload_rejects_101mb`; `test_upload_rejects_exe`; `test_each_cap2_format_ingests` (integration, per format; failures recorded and handled per design §5.7); `test_concurrent_identical_uploads_create_one_row`; `test_upload_accepted_when_tenant_unreachable`; `test_upload_p95_under_2s` |
| 2.4 Duplicates | Content-hash duplicate → `409 document.duplicate` with the existing id; invoice-number + supplier + amount duplicate → flagged `possible_duplicate` (needs M3 fields; the hook and test are added in 2.4 and activated in 3.4) | `test_same_hash_rejected`; `test_duplicate_logged_in_audit` |
| 2.5 Audit log | `AuditEvent(id, workspace_id, actor, action, target, ip, at, prev_hash, hash)`; `append_event()` under a per-workspace advisory lock; DB role without UPDATE/DELETE + trigger; `sijil-cp audit verify --workspace` CLI; middleware logs every view, edit and export | `test_chain_links`; `test_tamper_detected_by_verify`; `test_update_and_delete_rejected`; `test_parallel_appends_keep_single_chain`; `test_document_view_is_audited` |
| 2.6 Drift check | Nightly Celery beat `drift_check(workspace_id)`: for every ledger row, the Paperless original exists and its hash matches, and the object-store copy matches; mismatches → `AuditEvent integrity.drift` + alert row | `test_drift_check_flags_missing_paperless_document`; `test_drift_check_flags_modified_file`; `test_clean_workspace_reports_ok` |
| 2.7 Inbound email | In-region SMTP receiver service (`services/worker` entrypoint `sijil-smtp`, aiosmtpd) on compose port 2525; address `<workspace-slug>.<token>@in.sijil.local` stored on the workspace; each attachment and the body (rendered as a PDF) → the same ingest path; unknown address → rejected at RCPT | `test_email_attachments_ingested`; `test_email_body_ingested_as_document`; `test_unknown_recipient_rejected`; `test_oversize_attachment_rejected` |
| 2.8 Documents list | `GET /v1/workspaces/{id}/documents`: Paperless list merged with ledger fields, permission-checked, audited | `test_list_merges_ledger_state`; `test_readonly_can_list_member_of_other_ws_cannot` |

**M2 acceptance:**
- `make seed` uploads the 12 sample invoices through the API and the email path.
- Each one has a ledger row with a SHA-256 hash matching `sha256sum` of the fixture.
- Uploading one again returns 409.
- `audit verify` reports an intact chain.
- Editing a row in psql is refused.
- **Stop for review.**

## Milestone 3: Recognition and review

Branch `m3-recognition`. Covers **REC-1, REC-2, REC-3, REC-4**.

| Task | Deliverable | Acceptance tests |
| --- | --- | --- |
| 3.1 OCR config | Tenant env: `ara+fra+eng`, PDF/A output; content read back via the client | `test_arabic_fixture_text_extracted` (integration: Arabic supplier name appears in the content) |
| 3.2 Field parsers | `sijil_core.fields`: `parse_amount(str) -> Decimal` (Arabic-Indic digits, `٫`, decimal comma, thin and normal spaces), `parse_date`, `normalise_ice` | `test_parse_amount_variants` (`"١٢٠٫٠٠"`, `"1 200,00"`, `"1.200,00"`, `"1200.00"` → `Decimal("1200.00")` or `Decimal("120.00")`); `test_ice_must_be_15_digits` |
| 3.3 Extraction | Celery `extract(ledger_id)`: content → Ollama structured JSON (model per design §8) run twice → `Extraction(fields, per_field_signals)`; stored in the per-workspace index DB | `test_extraction_schema` (with a stubbed Ollama); `test_no_external_host_called` (httpx transport asserts only `ollama` hosts) |
| 3.4 Confidence | `score(field, signals) -> float`, with weights as data; signals per design §5.3; type signal from Paperless's suggestion | `test_totals_mismatch_lowers_confidence_below_090`; `test_all_checks_pass_scores_at_least_090`; `test_invalid_vat_rate_flags_vat`; `test_possible_duplicate_flag` (activates 2.4) |
| 3.5 Routing | All fields ≥ 0.90 → `filed`, else `needs_review`; provisional `retention_until`; webhook-style internal events `document.filed`, `document.needs_review` | `test_routing_threshold_boundary` (0.8999 → review, 0.90 → filed) |
| 3.6 Review + seal API | `GET /v1/workspaces/{id}/review` (fields with confidences); `PATCH` fields before seal; `POST .../documents/{id}/seal` → fields hashed, `retention_until` final, object lock extended, Paperless custom fields mirrored; `POST .../seal-batch` for "confirm all filed this month" | `test_seal_freezes_fields`; `test_patch_after_seal_409_retention_locked`; `test_batch_seal_one_audit_event_per_document`; `test_readonly_cannot_seal` |
| 3.7 Accuracy harness | `sijil-eval extraction --set fixtures/invoices/manifest.json` → per-field, per-language accuracy report (reported, not gated) | `test_eval_report_format` |

**M3 acceptance:** the 12 sample invoices are processed; those with deliberately broken totals land in Review; sealing one makes any field edit return `409 retention.locked`; the eval report prints accuracy per language.

## Milestone 4: Retention and export

Branch `m4-retention`. Covers **ARC-3, ARC-5**.

| Task | Deliverable | Acceptance tests |
| --- | --- | --- |
| 4.1 Presets | `RetentionPolicy(workspace_id, document_type, years, locked)`, seeded per country (MA 10, FR 10, DE 8/10, AE 5, SA 6, marked as working values) | `test_ma_preset_10_years`; `test_retention_until_end_of_fiscal_year_plus_years` |
| 4.2 Lock proof suite | `tests/retention/test_lock_all_paths.py`, parametrised over roles {Owner, Admin, Member, ReadOnly} × paths {API delete, API field edit, Paperless service token delete, direct SQL on ledger, object-store delete} for sealed and unsealed documents | Every combination refused for sealed documents; for unsealed ones only the Owner removal flow (4.3) succeeds |
| 4.3 Pre-seal removal | `POST .../documents/{id}/remove` (Owner + TOTP step-up + reason) → tombstone + governance remove + Paperless delete through the admin credential | `test_owner_can_remove_unsealed_with_reason`; `test_admin_cannot_remove`; `test_sealed_cannot_be_removed`; `test_tombstone_keeps_hash` |
| 4.4 End of retention | Daily job marks `eligible_for_deletion`; `POST .../retention/approve-deletion` (Owner + step-up), audited | `test_nothing_purged_automatically`; `test_eligible_only_after_retention_until` |
| 4.5 Correction notes | `POST .../documents/{id}/corrections` on sealed documents; originals untouched | `test_correction_keeps_original_value`; `test_correction_audited` |
| 4.6 Export | `POST /v1/workspaces/{id}/exports {period}` → Celery builds the ZIP from the object store: originals, `index.csv` (UTF-8 BOM for Excel, corrected values flagged), `integrity-report.json` + `.sig` (Ed25519 region key); `export.ready`; `sijil-cp export verify <zip>` | `test_export_contains_all_period_docs`; `test_integrity_report_verifies`; `test_tampered_zip_fails_verify`; `test_export_requires_step_up_and_is_audited` |

**M4 acceptance:** design acceptance "a document under retention cannot be deleted by any role via UI or API" is proven by 4.2, and "an exported period's integrity report verifies against the stored hashes" is proven by 4.6.

## Milestone 5: Search and Ask

Branch `m5-search`. Covers **ARC-1, ARC-2, REC-5**.

| Task | Deliverable | Acceptance tests |
| --- | --- | --- |
| 5.1 Search | `POST /v1/workspaces/{id}/search`: full text through Paperless plus field filters (type, supplier, period, amount range, tag, status) from the index DB; saved views CRUD | `test_filter_amount_range`; `test_saved_view_roundtrip`; `test_search_p95_under_500ms_10k_docs` (perf, nightly) |
| 5.2 Embeddings | Index-DB pgvector table; embed on seal or file using the Ollama embedding model | `test_embeddings_stay_in_workspace_index_db` |
| 5.3 Ask | `POST .../ask {question, locale}` → retrieve → in-region model answers in the asked language with `citations[]`; sums computed from field values in code | `test_answer_cites_documents`; `test_sum_computed_not_generated` ("VAT paid to <sample supplier> in 2027" equals the fixture sum); `test_answer_in_arabic_when_asked_in_arabic` |
| 5.4 Learning | Corrections → workspace `Supplier` records and Paperless matching rules; hints fed to extraction | `test_corrected_supplier_recognised_next_time` |

## Milestone 6: Admin and billing

Branch `m6-admin`. Covers **ADM-1 (document-type permissions), ADM-3, ADM-4, ADM-5**.

| Task | Deliverable | Acceptance tests |
| --- | --- | --- |
| 6.1 Document-type permissions | Membership grants restricted to document types; enforced in list, search, ask and export | `test_member_limited_to_invoices_cannot_see_payslips` (including via Ask and export) |
| 6.2 Plans, quotas, meter | `Plan`, `Subscription`, `UsageRecord`; meters for users, documents, storage and AI pages; behaviour per design §5.7a | `test_capture_never_blocked_over_quota`; `test_ai_stops_over_quota_documents_go_to_review`; `test_warning_at_80_and_100` |
| 6.3 MAD invoicing | Annual `Invoice` in MAD with a bank-transfer reference, a PDF in fr/ar, manual "mark paid", overdue states | `test_invoice_totals_mad_with_vat_20`; `test_overdue_restricts_exports_not_capture` |
| 6.4 Workspace export and closure | Full export (reuses 4.6 for all periods); closure: export → 30-day grace → archive tenant hibernated, read-only Owner access, until retention ends or a signed take-over statement | `test_closure_keeps_retained_documents`; `test_takeover_statement_allows_destruction_after_grace`; `test_grace_period_cancel` |

## Milestone 7: Screens and site

Branch `m7-screens`. Covers **CAP-3**, UX, NFR languages and accessibility.

- 7.1 **Identity:** run design-taste-frontend on the app (`--app --kind finance`, no `--keep`) and the site (page run), producing a new identity with a typeface pairing that has a real Arabic companion. Tokens go in `packages/ui` (added in this milestone).
- Screens, each built test-first against the API with Playwright and Vitest:
  - 7.2 Inbox: opens with a one-sentence finding and has "confirm all filed this month" one click away.
  - 7.3 Document review: low-confidence fields beside the scan; seal.
  - 7.4 Ask the archive: cited answers.
  - 7.5 Mobile capture PWA: camera, edge detection, multi-page, offline queue (CAP-3).
  - 7.6 Settings: members and roles, 2FA, retention, plan and usage, export and closure.
  - 7.7 Marketing site.
- **Per screen:**
  - `lint.mjs` and `inspect.mjs` until no FAIL remains, with their screenshots reviewed;
  - impeccable critique, audit and polish;
  - Playwright `rtl.spec.ts` (ar layout: no horizontal overflow, logical properties only, directional icons flipped and non-directional icons not mirrored);
  - an axe-core WCAG 2.2 AA check;
  - digit preference applied.

**MVP acceptance (amended):**
- A new business signs up and receives documents by email in under 15 minutes.
- No role can delete a retained document.
- The integrity report verifies.
- The Arabic UI passes RTL review on the five screens.
- Extraction accuracy is reported against the sample set, and against real invoices once supplied.

## Requirement coverage

| ID | Milestone | | ID | Milestone |
| --- | --- | --- | --- | --- |
| CAP-1 | M2 (2.7) | | ARC-1 | M5 (5.1) |
| CAP-2 | M2 (2.3) | | ARC-2 | M5 (5.2–5.3) |
| CAP-3 | M7 (7.5) | | ARC-3 | M4 (4.1–4.4) |
| CAP-4 | M2 (2.4), M3 (3.4) | | ARC-4 | M2 (2.2, 2.5) |
| REC-1 | M3 (3.1) | | ARC-5 | M4 (4.6) |
| REC-2 | M3 (3.3–3.4) | | ADM-1 | M1 (1.5), M6 (6.1) |
| REC-3 | M3 (3.2–3.4) | | ADM-2 | M1 (1.4) |
| REC-4 | M3 (3.5–3.6) | | ADM-3 | M6 (6.2) |
| REC-5 | M5 (5.4) | | ADM-4 | M6 (6.3) |
| | | | ADM-5 | M6 (6.4) |

Deferred: ACC-1–4 (the accountant portal), ARC-6 and the Auditor guest (R2), and production hosting, backups and the pen test (a later production milestone).
