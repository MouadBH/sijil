# Product Specification — Compliance Archive on Paperless-ngx

Oct 31, 2026 · @Moad Boulahdoud

## Summary

**Sijil** is a hosted compliance archive for small businesses and their accounting firms: capture paper and e-invoices, keep them in a tamper-evident archive in the customer's own country, and hand an audit-ready file to the tax office on request. It launches in Morocco in 2027, then France and Belgium, then the UAE and Saudi Arabia.

- **Engine:** an unmodified Paperless-ngx 3.x instance per customer for OCR, storage, search, workflows and AI suggestions.
- **What we build:** a new web app (Next.js) on top of the Paperless-ngx REST API, a control plane for tenants and billing, an e-invoice service, a retention and audit service, and an Arabic/French OCR service.
- **Who buys:** accounting firms first (one firm brings 20 to 200 clients), SMEs second.
- **Why now:** Morocco's e-invoicing reform starts with large firms in 2026 and reaches SMEs in 2027–2028; France, Germany, the UAE and Saudi Arabia have mandates landing 2026–2028.
- **The one rule:** a customer's documents, backups and AI processing never leave the region they chose.

## Name and brand

Recommended name: **Sijil** (سجل), Arabic for "register" or "official record". Every Moroccan business already knows the word from its *sijil tijari* (commercial register), it is two syllables a French or German speaker can say on first reading, and it describes exactly what the product keeps.

| Candidate | Meaning | For | Against |
| --- | --- | --- | --- |
| **Sijil** (recommended) | Register, official record (Arabic) | Native to Morocco and the GCC, easy in Europe, says "record" | Generic word: may limit trademark strength; .com likely taken |
| Kaghet | Paper (Moroccan Darija) | Warm, local, memorable | Means little outside the Maghreb |
| Wathiq | Trusted, certain (Arabic, root of *wathiqa*, document) | Strong trust meaning | Hard to spell for Europeans |
| Archiva | Archive (Latin, Italian) | Instantly clear in Europe | Generic; many existing uses |

Before committing:

- [ ] Search OMPIC (Morocco), EUIPO and WIPO Global Brand Database in class 9 and 42
- [ ] Check domains: sijil.ma, getsijil.com, sijil.app, sijil.eu
- [ ] Check social handles and the Arabic spelling in app stores
- [ ] Ask 10 target users in Morocco and France to say and spell it after hearing it once

A web search found no document-software product using the name, but that is not a trademark search.

## Users and jobs to be done

Four roles share one product; the accountant is the buyer to win, the client's bookkeeper is the daily user.

| Persona | Context | Top jobs | What success looks like |
| --- | --- | --- | --- |
| Accountant (firm partner or staff) | 2–30 staff, 20–200 SME clients, Casablanca, Agadir, Lyon, Dubai | Collect clients' documents without chasing; close each month on time; answer a tax audit fast | Every client's month is complete by day 5; audit file exported in minutes |
| SME owner or bookkeeper | 2–50 staff, often one person does the admin | Get paper and PDF invoices off the desk; find a contract in seconds; stay compliant without learning the rules | Snap or forward a document and forget it; search finds it |
| Workspace admin | Owner or office manager | Invite users, set permissions, choose retention, pay the bill | Set up in an afternoon; no surprises on the invoice |
| Auditor (read-only guest) | Tax inspector, external auditor, buyer's due diligence | See only the period and documents in scope, with proof nothing was altered | Time-boxed link, integrity report, nothing else visible |

The core loop: **document arrives → captured → recognised and filed → checked by a human only when needed → locked for its retention period → found or exported on demand.**

## Scope by release

Each release is sellable on its own; anything not listed is out of scope for that release.

| Release | Target | In scope | Explicitly out |
| --- | --- | --- | --- |
| MVP | Morocco, Q2 2027 | Signup and workspace provisioning; new web app (inbox, documents, search, document view); email and mobile upload; French/Arabic/English UI with RTL; Arabic+French OCR; retention presets with lock; audit log; accountant portal v1 (client list, completeness per month, export ZIP/CSV); annual invoicing by bank transfer; hosting in Morocco (Oracle Casablanca) | Card subscriptions; e-invoice XML (Morocco format not final); SSO; mobile app; integrations |
| R2 | France and Belgium, Q4 2027 | EU region; Factur-X, UBL, XRechnung detection, validation, readable rendering, field extraction; card and SEPA subscriptions via an EU entity; exports to Pennylane, Sage, Xero; Moroccan DGI e-invoice format once published; auditor guest links with integrity report | GCC hosting; ZATCA/PINT AE; WhatsApp capture |
| R3 | UAE then Saudi Arabia, 2028 | UAE and KSA regions with in-region AI; PINT AE and ZATCA XML rendering; ASP partner integrations; WhatsApp document capture; SSO (Google, Microsoft); ISO 27001 | Native mobile apps (PWA only); on-premise edition |

## Functional requirements

IDs let you track each requirement into tickets and tests. **M** = MVP, **R2**/**R3** = later release.

**Capture**

| ID | Requirement | Release |
| --- | --- | --- |
| CAP-1 | Each workspace (and each accountant's client) gets a unique inbound email address; attachments and the email body are ingested | M |
| CAP-2 | Drag-and-drop and multi-file web upload up to 100 MB per file; PDF, JPG, PNG, TIFF, HEIC, DOCX, XLSX, XML | M |
| CAP-3 | Mobile web capture: camera, edge detection, multi-page, works offline and uploads when online (PWA) | M |
| CAP-4 | Duplicate detection by content hash and by invoice number + supplier + amount | M |
| CAP-5 | WhatsApp Business number per workspace for document capture | R3 |

**Recognition and filing**

| ID | Requirement | Release |
| --- | --- | --- |
| REC-1 | OCR in Arabic, French and English, including mixed-script pages; searchable PDF/A output | M |
| REC-2 | Classify document type (invoice, receipt, contract, bank statement, payslip, tax notice, other) | M |
| REC-3 | Extract supplier, document date, number, totals, VAT and currency into fields; show a confidence for each | M |
| REC-4 | Anything below the confidence threshold goes to a **Review** queue; everything else files itself | M |
| REC-5 | Learn from corrections per workspace (uses Paperless-ngx matching plus AI suggestions) | M |
| REC-6 | Detect structured e-invoices (Factur-X, UBL, XRechnung, later DGI, PINT AE, ZATCA); validate; render a readable view; fill fields from the XML, not OCR | R2/R3 |

**Archive, search and retention**

| ID | Requirement | Release |
| --- | --- | --- |
| ARC-1 | Full-text and field search with filters (type, supplier, period, amount range, tag, status) and saved views | M |
| ARC-2 | Natural-language search and Q&A over the archive ("VAT paid to Maroc Telecom in 2027"), answers cite documents | M |
| ARC-3 | Retention policies per document type with presets per country (Morocco 10 years, Germany 8/10, France 10, UAE 5, KSA 6); documents under retention cannot be deleted or edited, only annotated | M |
| ARC-4 | Each stored file gets a SHA-256 hash and a timestamp; an append-only audit log records every view, edit, export and deletion | M |
| ARC-5 | Export a period as ZIP with an index CSV and an integrity report (hash list signed by the platform) | M |
| ARC-6 | Auditor guest link: read-only, time-boxed, scoped to a period or a saved view, every access logged | R2 |

**Accountant portal**

| ID | Requirement | Release |
| --- | --- | --- |
| ACC-1 | One login across all client workspaces; client list with document counts and review backlog | M |
| ACC-2 | Month-close board: per client and month, expected documents vs received, missing items flagged | M |
| ACC-3 | Request missing documents from a client by email (and WhatsApp in R3) with a one-click upload link, no login needed | M |
| ACC-4 | Bulk export to accounting tools (CSV first; Sage, Pennylane, Xero in R2) | M/R2 |
| ACC-5 | Firm branding on client-facing pages and emails | R2 |

**Administration and billing**

| ID | Requirement | Release |
| --- | --- | --- |
| ADM-1 | Roles: Owner, Admin, Member, Read-only, Auditor guest; permissions down to document type | M |
| ADM-2 | 2FA (TOTP) for all, enforceable by the owner | M |
| ADM-3 | Plans with quotas (users, documents, storage, AI pages) and in-app usage meter | M |
| ADM-4 | Annual invoices in MAD by bank transfer (Morocco); card and SEPA subscriptions in EUR (R2); AED/SAR (R3) | M/R2/R3 |
| ADM-5 | Self-serve data export of the whole workspace and account closure with a 30-day grace period | M |

## Non-functional requirements

| Area | Requirement |
| --- | --- |
| Tenant isolation | One Paperless-ngx instance, database and storage volume per workspace; no shared tables holding documents; network policies deny cross-tenant traffic |
| Encryption | AES-256 volumes with per-tenant keys in a KMS; TLS 1.3 everywhere; backups encrypted with a separate key |
| Data residency | Region chosen at signup and immutable; documents, backups, logs with document content, OCR and AI inference stay in it. Morocco (Oracle Casablanca) at MVP, EU (Germany/France) in R2, UAE and KSA in R3. Moroccan transfers abroad follow Law 09-08 and CNDP rules |
| Availability | 99.5% monthly at MVP, 99.9% from R2; RPO 24 h, RTO 8 h; quarterly restore test per region |
| Performance | Upload accepted in < 2 s; OCR + classification of a 2-page invoice in < 60 s at p95; search results in < 500 ms at p95 for 100k documents |
| Scale | 1,000 workspaces per region on the MVP design; idle tenants hibernate after 30 min |
| Security operations | Annual external pen test; dependency and container scanning in CI; 24 h patch window for critical CVEs; security.txt and disclosure policy |
| Privacy | DPA and subprocessor list published; CNDP declaration (Morocco) before launch; GDPR in R2; PDPL and SDAIA registration before KSA |
| Accessibility | WCAG 2.2 AA for the web app and landing page |
| Languages | French, Arabic and English at MVP with full right-to-left layout for Arabic; German in R2; dates, numbers and currencies per locale; Arabic-Indic or Western digits per user preference |
| Upstream | Track Paperless-ngx stable releases within 30 days; no changes to its code beyond configuration; all custom logic in our services |

## Architecture

We replace the Paperless-ngx user interface, not its engine: a new Next.js web app talks to our control plane, which drives an unmodified Paperless-ngx per tenant through its REST API.

&#91;embedded content: system architecture · per region\]

The blue box is where nearly all our code lives; Paperless-ngx is a dependency we upgrade, never a fork we maintain.

- **Why a new frontend instead of re-theming the Angular UI:** re-theming a fork means rebasing on every upstream release; a separate app over the API gives full control of UX, RTL and branding, matches the team's Next.js and TypeScript skills, and keeps a clean GPL boundary (to be confirmed by a lawyer).
- **Stack:** Next.js 15 (App Router) + TypeScript for the web app; FastAPI + SQLAlchemy + PostgreSQL for the control plane; Celery/Redis for jobs; Kubernetes per region (k3s or managed); object storage with per-tenant keys; OpenTelemetry for tracing.
- **Fallback:** the stock Paperless-ngx UI stays reachable for support staff only, behind the control plane's admin role.

## Data model and APIs

The control plane owns accounts, tenancy, billing and compliance metadata; documents and their OCR text stay in each tenant's Paperless-ngx.

**Control-plane entities (PostgreSQL)**

| Entity | Key fields | Notes |
| --- | --- | --- |
| Organisation | id, name, type (firm, business), country, region, plan\_id, status | A firm or a business; region fixed at creation |
| Workspace | id, org\_id, client\_of\_firm\_id?, paperless\_url, db\_name, kms\_key\_id, state (provisioning, active, hibernated, closing) | One per business; firms get one per client |
| User | id, email, name, locale, digits, totp\_enabled | Global identity, SSO links in R3 |
| Membership | user\_id, workspace\_id or org\_id, role | Owner, Admin, Member, ReadOnly |
| GuestLink | id, workspace\_id, scope (period or saved view), expires\_at, created\_by | Auditor access |
| RetentionPolicy | workspace\_id, document\_type, years, locked | Presets per country |
| DocumentLedger | workspace\_id, paperless\_doc\_id, sha256, ingested\_at, retention\_until, einvoice\_format?, validation\_status | Integrity and retention source of truth |
| AuditEvent | id, workspace\_id, actor, action, target, ip, at, prev\_hash | Hash-chained, append-only |
| MonthClose | workspace\_id, period, expected, received, missing\[\], status | Accountant board |
| Plan, Subscription, Invoice, UsageRecord | standard billing fields | MAD, EUR, AED, SAR |

**API surface (REST, JSON, versioned under /v1)**

| Endpoint | Purpose |
| --- | --- |
| POST /v1/orgs, POST /v1/workspaces | Signup and provisioning (async; returns job id) |
| GET /v1/workspaces/{id}/documents | Proxied, permission-checked list from Paperless-ngx with ledger fields merged in |
| POST /v1/workspaces/{id}/uploads | Upload; returns task id; webhook on completion |
| GET /v1/workspaces/{id}/review | Review queue with field confidences |
| POST /v1/workspaces/{id}/search, /ask | Field search and natural-language Q&A with citations |
| POST /v1/workspaces/{id}/exports | Period export with integrity report |
| POST /v1/guest-links | Auditor links |
| GET /v1/firms/{id}/close-board?period= | Month-close board across clients |
| POST /v1/firms/{id}/requests | Request missing documents from a client |
| Webhooks | document.filed, document.needs\_review, export.ready, month.completed |

The web app talks only to this API, never to Paperless-ngx directly, so permissions, audit logging and residency checks happen in one place.

## UX and visual identity

The new interface is organised around the work (arrive, check, seal, close the month) instead of around the archive's data types. The [landing page preview](https://claude.ai/artifact/KonK2ufBN3f9PdcXG7j5U9) and the [app redesign canvas](https://claude.ai/artifact/3Ep5RqeraogqtL8E6BJqcM) show both in the same identity.

**Identity**

- **Motif:** the tab of an archive folder, the cardboard dossier every accountant keeps per client. It is the logo mark, the hero's folder stack, the top corners of buttons and the footer wordmark.
- **Colour:** one teal (#06525C) on a cool paper ground (#F7F9F8), no black. In the app, chrome stays neutral and teal marks only the main data (filed documents, completed clients); amber marks only what needs a human.
- **Type:** Jost, one family, from 13 px meta to 144 px display; tabular figures everywhere money appears; Noto Naskh Arabic for Arabic text.

**How it differs from stock Paperless-ngx**

| Stock Paperless-ngx | Sijil |
| --- | --- |
| Home is a dashboard of saved-view widgets | Home is the inbox, with one sentence on what needs a check and a completeness bar for the month |
| Vocabulary: correspondents, tags, document types, storage paths | Words of the trade: supplier, ICE, VAT, total TTC, month close, retention |
| Every document filed silently; the user audits later | Review queue: only fields under 90% confidence ask for a human, shown beside the scan |
| No visible retention or integrity state | Every document shows "kept until", where it is stored and its seal |
| One organisation per instance | Accountant scope: one login across clients, a month-close board, requests to clients |
| Left-to-right only in practice | Arabic, French and English, with full right-to-left layout |
| Search box with filters | Ask the archive in plain words; the answer cites each document and sums from them |

**Five core screens for the MVP:** Inbox, Document review, Month-close board, Ask the archive, Mobile capture (phone). Settings, exports and the audit log follow the same shell.

**Design rules for the team:** no colour on chrome; numbers right-aligned in tabular figures; zeros in grey; each screen opens with one sentence stating its finding; the most frequent job is one click from the first screen.

## Product improvements beyond the plan

The biggest wins turn the archive from a place documents go to die into something that saves the accountant hours every month. Ranked by impact for the effort.

| Idea | Why it matters | Effort | When |
| --- | --- | --- | --- |
| Month-close board with "missing document" chasing | The accountant's biggest time sink is chasing clients; this is the reason they will pay | Medium | MVP |
| Review queue with confidence scores instead of filing everything | Trust: people only check what the machine is unsure of | Low | MVP |
| Bank statement matching: import a CSV/statement, match each line to an invoice, list unmatched payments | Turns "is everything here?" into a number; strongest daily-use hook | Medium | R2 |
| Audit-ready pack with hash-chained integrity report | The compliance promise made tangible; a sales demo in itself | Low | MVP |
| Ask the archive in Arabic, French or English with cited answers | Native Paperless-ngx AI already exists; we add in-region models and citations | Low | MVP |
| Supplier directory built from documents (ICE/VAT number, IBAN, address) with change alerts | Catches invoice fraud (changed IBAN) and fills accounting exports | Medium | R2 |
| Deadline and expiry tracker: contracts, insurance, leases, licences; reminders before renewal | Value for the owner, not just the accountant | Low | R2 |
| Expense receipts for staff: photo, auto-fill, approve, export | Expands seats per workspace | Medium | R2 |
| Client upload link and WhatsApp capture without accounts | Removes the client's excuse; huge in Morocco and GCC | Low (link), Medium (WhatsApp) | MVP / R3 |
| Physical archive digitisation service with partner scanning bureaus | One-off revenue and a fast way to fill a new workspace | Low (partnership) | R2 |
| Public API and Zapier/Make connectors | Lets firms wire it into their own tools | Medium | R3 |
| Native mobile apps | Only if PWA capture proves insufficient | High | Later |

## Success metrics and acceptance criteria

| Metric | Target at 6 months after launch |
| --- | --- |
| Time from signup to first document filed | < 10 minutes median |
| Documents filed without human review | ≥ 80% |
| Field extraction accuracy (supplier, date, total) on reviewed sample | ≥ 95% French, ≥ 90% Arabic |
| Accountant clients with a complete month by day 5 | ≥ 60% |
| Weekly active workspaces / paying workspaces | ≥ 70% |
| Monthly logo churn | < 2% |

**MVP is accepted when:**

- [ ] A new accountant can sign up, add 3 clients, and receive documents by email in under 15 minutes
- [ ] 50 real Moroccan invoices (25 Arabic, 25 French) are filed with the accuracy targets above
- [ ] A document under retention cannot be deleted by any role, including Owner, via UI or API
- [ ] An exported period's integrity report verifies against the stored hashes
- [ ] Arabic UI passes an RTL review with no mirrored icons or broken layouts on the five core screens
- [ ] Pen test finds no high or critical issue open
- [ ] Restore of a tenant from backup completes within the RTO

## Open questions and decisions needed

- [ ] **Morocco e-invoice format:** the DGI has not published a technical standard; MVP ships without XML support and adds it when the spec is out. Confirm the timeline with the DGI or a certified provider.
- [ ] **Retention presets:** the years listed in ARC-3 are working values; confirm each with a local accountant before launch.
- [ ] **Payments in Morocco:** Stripe does not serve Moroccan companies; confirm whether CMI supports recurring card billing, or stay on annual bank transfer.
- [ ] **Legal entities:** Moroccan SARL at launch; when to add the EU entity for R2.
- [ ] **GPL boundary:** have a lawyer confirm that the new web app and services, talking to Paperless-ngx only over its API, are separate programs.
- [ ] **Hosting in Morocco:** Oracle Casablanca vs a local provider (N+ONE, others) on price and managed Kubernetes availability.
- [ ] **Arabic OCR engine:** benchmark Tesseract, an open vision model run in-region, and a commercial engine on 200 real Moroccan documents.
- [ ] **Name:** clear trademark and domains for Sijil or pick an alternative.
