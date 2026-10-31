# Sijil MVP (Morocco) — Design

Date: 2026-10-31 · Status: draft for approval · Source of truth: `SPEC.md`, amended by the decisions below.

## 1. Intent

Sijil is a hosted compliance archive for Moroccan SMEs (accounting firms later). A document arrives, is captured, recognised and filed, checked by a human, sealed, locked for its retention period, and can be found or exported on demand. It runs on an **unmodified Paperless-ngx 3.x instance per workspace**. Everything we build sits beside it and talks to it over its REST API.

The product promise comes down to three things that must hold under test:

1. A document under retention cannot be deleted or altered by anyone.
2. Every file and every action can be shown to be untampered: the SHA-256 ledger and the hash-chained audit log.
3. Document content never leaves the workspace's region.

## 2. Goals, non-goals, anti-goals

**Goals (this build):** the Morocco MVP from `SPEC.md`, **minus the accountant portal**: tenancy and auth, capture, ledger, recognition and review, sealing, retention, export, search and Ask, admin and billing, and the web app and marketing site in fr/ar/en with full RTL.

**Non-goals (deferred by the product owner):**

- Accountant portal: ACC-1, ACC-2, ACC-3, ACC-4, and the Month-close screen.
- Auditor guest role and guest links: ADM-1's Auditor guest and ARC-6 move to R2.
- Everything the spec already puts in R2 or R3: e-invoice XML (REC-6), card or SEPA billing, SSO, WhatsApp, Kubernetes.

**Anti-goals (a failure even if it "works"):**

- Any patch, fork or copied source of Paperless-ngx.
- The web app calling Paperless-ngx directly.
- Document content, OCR text or prompts sent to an API outside the region, including in development.
- A deletion or field edit on a retained document succeeding through any path: UI, API, the support fallback, or a direct Paperless token.
- Hard-coded user-facing strings, or an Arabic layout that is merely mirrored text instead of real RTL.
- A generic, template-looking UI.

## 3. Constraints (non-negotiable)

1. Official image `ghcr.io/paperless-ngx/paperless-ngx`, pinned to an exact 3.x tag (currently `3.4.1`). One instance per workspace, configured only by environment variables. Upgrades are tracked within 30 days.
2. The web app talks only to the control-plane API (`/v1`). That API runs the permission check, the audit write and the region check for every request.
3. Data residency: the region is set when the organisation is created and never changes. Documents, backups, OCR and AI inference stay in it. Development uses local Ollama, and Tesseract inside Paperless-ngx.
4. The retention lock is absolute (see §5.4), with tests proving it for every role and every path.
5. A SHA-256 hash is recorded for every ingested file. The audit log is append-only and hash-chained.
6. fr/ar/en from day one, all strings in message catalogues, full RTL, locale-aware dates, numbers and MAD, with Arabic-Indic or Western digits per user.
7. GPL boundary: REST only, separate processes, no Paperless-ngx source in the repo.

## 4. Stack and repo (as given by the product owner)

- pnpm workspaces for JS, and uv for Python.
- `apps/web`: Next.js 15 App Router + TypeScript.
- `apps/site`: the marketing landing page.
- `services/control-plane`: FastAPI, SQLAlchemy 2, Alembic, PostgreSQL, Pydantic v2.
- `services/worker`: Celery + Redis.
- `infra/`: docker-compose with control-plane, worker, Postgres, Redis, Ollama and two sample Paperless-ngx tenants, plus the Paperless dependencies they need (Redis broker, Tika and Gotenberg for Office files).
- Tests and lint: Playwright, pytest, Vitest, ruff, mypy, eslint.
- CI: GitHub Actions.

## 5. Decisions

### 5.1 Authentication and sessions (decided)

- Built in the control plane:
  - Passwords: argon2id.
  - TOTP: `pyotp`, with one-time recovery codes. The owner can enforce 2FA for the workspace (ADM-2).
- Sessions are stored on the server in Postgres:
  - The browser holds a random opaque token in an `HttpOnly; Secure; SameSite=Lax` cookie. The database stores only its hash.
  - Sessions can be revoked one by one.
  - CSRF protection applies to every state-changing request.
- Step-up re-authentication (TOTP again) is required for export, role changes, disabling 2FA and account closure.
- SSO (R3) is added later with `authlib` without changing this design.

### 5.2 Tenant runtime and Paperless-ngx access (decided)

- **`TenantRuntime` interface** in the worker: `provision`, `start`, `stop`/hibernate, `destroy` and `status`.
  - The local implementation drives the Docker Engine API. Each workspace gets one Paperless-ngx container on a private `sijil-tenants` network with no published ports, its own Postgres database and its own volume.
  - Kubernetes is a later implementation of the same interface.
- The two tenant containers in compose are **sample tenants** seeded from `fixtures/`, so the stack can be demoed as soon as it boots. Real signups go through `TenantRuntime`.
- **Credentials:**
  - At creation, a random Paperless admin password is set by environment variable.
  - The worker uses it to create a **service user with view, add and change rights but no delete right**, and takes an API token for that user.
  - The token and the admin password are stored encrypted with AES-256-GCM, using a per-tenant data key wrapped by a regional master key. A local stand-in for the key service fills the spec's `kms_key_id`.
  - The admin credential is used only for provisioning, token rotation and the support path.
- **Client:** a typed `PaperlessClient` (httpx) per tenant, used only by the control plane and the worker.
- **Status updates:** completion is tracked by **polling** Paperless-ngx's tasks endpoint for the MVP. Webhooks would need Paperless workflows created through its API, and that is deferred.
- **Support access** to the stock Paperless-ngx UI goes through a separate Paperless user that can only read. It is reachable only through the control plane's admin role, and every use is audited.

### 5.2a Original files and official field values (decided)

- **Paperless-ngx is a working copy, not the record.** Every original file is also written to **our own write-once, in-region object storage with object lock**. While unsealed it is locked in governance mode, so that only the Owner's pre-seal removal flow (§5.4) can bypass it. At sealing it switches to compliance mode until `retention_until`, after which nobody can remove it. Locally this is RustFS (S3 API with object lock; MinIO stopped publishing images in 2025), behind an `ObjectStore` interface.
  - The object key is the SHA-256 hash.
  - The lock is extended when sealing sets the final retention date.
- **Official field values** are the ledger's sealed fields (see §5.5 for where they are stored). Paperless-ngx custom fields are only a mirror for search.
- **Drift check:** a nightly worker job compares every Paperless-ngx original against the ledger hash and the object-store copy, and raises an alert on any difference. Paperless permissions can't stop the page edits that "change" rights allow, so this check is how we catch them. It is audited.
- **Exports and restores** read originals from the object store, never from Paperless-ngx.

### 5.3 Recognition and confidence (decided)

- **OCR:** Paperless-ngx's own OCR, configured with `PAPERLESS_OCR_LANGUAGES` = Arabic + French + English and PDF/A output (REC-1). The open question in the spec about benchmarking an Arabic OCR engine stays open. The extraction step must not care which OCR engine produced the text.
- **Extraction** is a worker step of our own:
  - It reads the OCR text from Paperless-ngx and asks a local Ollama model for structured JSON: document type, supplier, ICE, date, number, HT, VAT, TTC and currency.
  - It writes confirmed values back to Paperless-ngx custom fields.
- **Confidence per field** is computed from objective signals, never from the model rating itself:
  - The ICE has 15 digits.
  - HT + VAT = TTC, within rounding.
  - The VAT rate is one of the Moroccan rates (20, 14, 10, 7%).
  - The date parses and is plausible.
  - The supplier matches a known supplier in the workspace.
  - Two extraction runs agree.
  - For the document type, the Paperless-ngx matching or AI suggestion agrees.
- **Thresholds (REC-4):** a document with any field under 0.90 goes to Review. Otherwise it files itself.
- **Learning (REC-5):** corrections become workspace supplier records and Paperless-ngx matching rules, and are fed back into later extractions as hints.
- **Ask (ARC-2, builder's call):** our own retrieval pipeline. Embeddings go in the per-workspace index database, and answers are generated on the in-region Ollama and cite document IDs. Sums are computed from ledger and index values, not by the model. Paperless-ngx's built-in AI is pointed at the same in-region Ollama through environment variables and used only as one signal for document type.
- **Accuracy (decided):** an evaluation tool scores extraction against a labelled set and reports per-field accuracy for French and Arabic. The spec targets (≥95% fr, ≥90% ar on supplier, date, total) are **reported, not a gate**, until the product owner supplies the 50 real Moroccan invoices. Until then the tool runs on the sample fixtures only.

### 5.4 Filing, sealing and retention (decided)

- **File lock from arrival.** From the moment a file is ingested and hashed, its ledger row is under retention. No role can delete it through any path. The original file can never be altered.
- **Filed, not sealed.** A document whose fields all score 0.90 or above is *filed*: it appears in the archive with its fields, but it is not sealed.
- **Only a person confirming a document in the review queue seals it.** The inbox offers a one-click "confirm all filed documents this month" that goes through the same confirmation path, one audit event per document.
- **Sealing:**
  - The confirmed field values, with their hash, are stored on the ledger row.
  - `retention_until` is set to the end of the fiscal year of the document date plus the country preset (Morocco 10 years; calendar fiscal year by default).
  - From then on, fields cannot be edited, only annotated.
- **Correcting a sealed document:** a *correction note*, which is audited, shown next to the original value and leaves the original untouched. Exports use the corrected value and flag it as corrected.
- **Provisional retention (builder's call):** at filing, `retention_until` gets a provisional value: the end of the fiscal year of the document date (or, if no date was read, of the arrival date) plus the preset. Sealing makes it final.
- **Uploaded by mistake (decided):** before sealing, an **Owner** can remove a document, with a written reason and a TOTP step-up. The removal is audited, and only a tombstone ledger row is kept (hash, reason, actor, time). The object-store copy is removed through a governance-mode bypass that only the removal flow holds. Once a document is sealed, it can never be removed.
- **End of retention (builder's call):** nothing is purged automatically. Documents past `retention_until` become *eligible for deletion*. An Owner approves deletion in batches, and every approval is audited.
- **Account closure (decided, needs a lawyer's confirmation):** ADM-5 first gives the customer a full export. Documents still under retention are kept in a **read-only archive** until retention ends, unless the Owner signs a statement that they are taking over the legal duty to keep them. Only after that statement are retained documents destroyed. The archive tenant is hibernated and reachable only by the Owner, read-only.
- **Personal-data erasure requests under Law 09-08 (builder's call, needs a lawyer):** the legal duty to keep documents overrides erasure while retention runs. The refusal and its reason are logged and sent to the person who asked.
- **Enforcement in four layers, each tested on its own:**
  1. The control-plane API refuses the request.
  2. The service-user token in Paperless-ngx has no delete right.
  3. A database constraint on the ledger blocks changes to sealed rows.
  4. Object lock on the original in our object store (§5.2a), with the nightly drift check.
- **Retention presets (ARC-3):** per document type, seeded per country.

### 5.5 Ledger and audit log (decided)

- **`DocumentLedger`** (shared control-plane database) holds the spec fields plus `state` (received, needs_review, filed, sealed, removed), `object_key`, `fields_sha256`, `sealed_at` and `sealed_by`. It holds **no document content**.
- **Field values, correction notes, OCR-derived text and Ask embeddings** live in a **per-workspace index database**, separate from both Paperless-ngx's own database and the shared control-plane database (builder's call; pgvector). This satisfies "no shared tables holding documents".
- **Duplicates (CAP-4):**
  - A file whose content hash already exists in the workspace is refused at ingestion.
  - A matching invoice number + supplier + amount flags the document as a possible duplicate in Review.
- **`AuditEvent`** gains a `hash` column: `hash = sha256(prev_hash ‖ canonical_json(event))`, chained per workspace.
  - Append-only is enforced in the database: the app role has no UPDATE or DELETE grant, and a trigger rejects them.
  - A verification command walks the chain.
  - Every view, edit, export, seal, correction and refused deletion is logged.

### 5.6 Export (ARC-5)

- A period export is a ZIP containing the original files, `index.csv` (one row per document with ledger fields and hashes) and `integrity-report`:
  - The report holds a hash list plus the audit-chain head.
  - It is signed with the region's Ed25519 platform key, whose public key is published.
- A verify command checks an exported ZIP against the stored hashes.

### 5.7 Capture (CAP-1/2/3)

- **CAP-1:** each workspace gets a unique inbound address, received by **our own in-region SMTP receiver**. We do not use Paperless-ngx's IMAP fetching. Attachments and the email body (as a document) are pushed into Paperless-ngx through its API.
- **CAP-2:** web upload of up to 100 MB per file. If a listed format turns out not to be ingestible by Paperless-ngx 3.4, the worker converts it or stores the original next to a rendition. That must be verified per format.
- **CAP-3:** a mobile capture PWA with camera, edge detection, multi-page, and an offline queue.

### 5.7a Quotas and non-payment (builder's call)

- Capture and read access are **never blocked**: legally required documents must always be able to arrive and be read.
- When a workspace goes over quota, it gets warnings at 80% and 100%. Beyond that, AI extraction stops, so new documents go to Review without suggestions, and new users can't be added.
- When an invoice is overdue, the same restrictions apply, plus exports. After the grace period the workspace becomes read-only plus capture.

### 5.7b Definition of done and regions (decided)

- This build is done when the **compose stack and CI** are done.
  - The key service, object storage, backups and TLS each sit behind an interface with a local implementation.
  - Real in-region hosting, backup and restore within the RTO, the pen test and real key-service integration form a **later production milestone** and are not part of this build's acceptance.
- **Region check:** the region is a config value on every service and a fixed column on the organisation. Every request and job checks that the workspace's region matches the service's own region. Tests run a second, fake region to prove that cross-region access is refused.

### 5.8 Visual identity and screens

- **Identity:** a **new visual identity created from scratch**. The spec's identity is dropped (Jost, teal #06525C on #F7F9F8, the folder-tab motif), along with the linked landing preview and redesign canvas.
  - The app is dealt with design-taste-frontend's app-screens run (`--app --kind finance`, without `--keep`). The site follows its page run.
  - `lint.mjs` and `inspect.mjs` run until no FAIL remains, then impeccable critique, audit and polish.
  - The identity must include a typeface pairing with a real Arabic companion face.
  - The product name stays **Sijil**, pending the trademark check in the spec.
- **Design rules kept from the spec:**
  - Each screen opens with one sentence stating its finding.
  - The most frequent job is one click from the first screen.
  - Money is right-aligned in tabular figures.
  - Colour marks data and "needs a human", not chrome.
  - WCAG 2.2 AA.
- **Screens:** Inbox, Document review, Ask the archive, Mobile capture (phone) and Settings, plus the marketing site. All in fr/ar/en with RTL.

## 6. Build order

Each milestone is on its own branch or worktree, uses TDD, ends with verification and a code review, and closes with a report.

| # | Milestone | Requirement IDs |
| --- | --- | --- |
| 0 | Plan and skeleton: `docs/plan.md`, `docs/architecture.md`, repo structure, compose that boots everything, CI (lint + tests), `CLAUDE.md`. **Stop for review.** | — |
| 1 | Tenancy: signup (business organisations; the `firm` type stays in the model, with no firm UI), workspaces, roles Owner/Admin/Member/ReadOnly, TOTP, `TenantRuntime` provisioning, encrypted per-tenant token, region check | ADM-1 (roles), ADM-2, NFR tenancy, residency, encryption |
| 2 | Capture and ledger: upload, inbound email, ingestion into Paperless-ngx plus a write-once object-store copy, SHA-256 ledger, duplicate detection, hash-chained audit log, nightly drift check. **Stop for review.** | CAP-1, CAP-2, CAP-4, ARC-4 |
| 3 | Recognition and review: OCR configuration, classification, extraction with confidence, review queue, sealing | REC-1, REC-2, REC-3, REC-4 |
| 4 | Retention and export: presets, the three-layer lock, period ZIP with index and signed integrity report | ARC-3, ARC-5 |
| 5 | Search and Ask: field and full-text search with saved views; natural-language Q&A with citations on in-region Ollama; learning from corrections | ARC-1, ARC-2, REC-5 |
| 6 | Admin and billing: permissions down to document type, plans and quotas with a usage meter, annual MAD invoices by bank transfer, full workspace export and account closure with a 30-day grace period | ADM-1 (doc-type), ADM-3, ADM-4, ADM-5 |
| 7 | Screens and site: new identity; Inbox, Document review, Ask the archive, Mobile capture (PWA), Settings; marketing site; fr/ar/en with RTL | CAP-3, UX, NFR languages, accessibility |
| later | Production (not this build): in-region hosting, real key service, TLS, backups and restore test within the RTO, pen test | NFR availability, encryption, security operations |
| later | Accountant portal, then R2 | ACC-1–4, ARC-6 |

Seeded sample data lives in `fixtures/`, clearly labelled as sample data: French and Arabic invoices, suppliers with valid-format ICE numbers, MAD totals that add up.

## 7. Deviations from `SPEC.md` (agreed)

1. The accountant portal (ACC-1 to ACC-4, Month-close screen) is out of this build. The MVP acceptance criterion "a new accountant can sign up, add 3 clients, and receive documents by email in under 15 minutes" becomes **"a new business can sign up and receive documents by email in under 15 minutes"** until the portal returns.
2. The Auditor guest role moves to R2 with ARC-6.
3. The spec's visual identity and linked design artifacts are replaced by a new identity.
4. The support fallback to the stock Paperless-ngx UI is read-only.
5. `AuditEvent` gains a `hash` column.
6. Sealing is defined (§5.4). The spec did not define it.
7. Webhooks from Paperless-ngx are replaced by polling for the MVP.
8. Originals are also kept in our own write-once object store. The spec has Paperless-ngx as the only store.
9. Field values, OCR text and embeddings live in a per-workspace index database, not in the shared ledger.
10. The acceptance criteria for restore within the RTO and for the pen test move to the later production milestone. The extraction-accuracy criterion is reported only, until real invoices are supplied.
11. Rules for removing a file uploaded by mistake (before sealing, Owner only) and for account closure with documents still under retention, both added. They need legal confirmation.

**Proposed during M0, approved by the product owner on 2026-10-31:**

12. Arabic OCR data (`ara.traineddata`, tessdata_fast 4.1.0, checksummed) is bind-mounted read-only into the official Paperless-ngx image. The image ships without Arabic, and tenants have no internet to install it. This is a file mount, not an image change.
13. RustFS replaces MinIO as the local object store, because MinIO stopped publishing images. Production uses the region's own object storage. Whether RustFS enforces object lock is proven in M2 before anything relies on it.
14. Object lock is governance mode while unsealed and compliance mode once sealed (§5.2a). This resolves a contradiction between §5.2a and §5.4.
15. One internal network and one Redis per tenant (tightening SPEC's "network policies deny cross-tenant traffic"), and separate database roles for migrations and for the app.

## 8. Left to the builder

- Choice of Ollama models (extraction, embeddings, Q&A) that run on a dev laptop and handle Arabic.
- The SMTP receiver implementation and the inbound address format.
- How the frontend handles i18n (library, message format), provided ICU plurals, RTL and digit preference work.
- Internal module layout, naming and test structure, recorded in `CLAUDE.md`.
- Hibernation of idle tenants (spec: after 30 minutes) is designed into `TenantRuntime` but can arrive after the MVP features.
