# M3 Recognition and Review Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every consumed document is read (OCR text from its Paperless, REC-1), classified and extracted by the in-region Ollama (REC-2, REC-3), and scored per field from objective signals. It then either files itself or goes to Review (REC-4). A person confirming a document seals it: the fields are hashed onto the ledger, retention becomes final, and the original's object lock switches to COMPLIANCE.

**Architecture:**
- **Pipeline.** Ingest settles `consumed` → enqueue `sijil.extract_document` (queue `extraction`).
  1. The worker reads `content` from Paperless and normalises it (NFKC).
  2. It runs the extraction prompt twice against Ollama and parses the values with `sijil_core.fields`.
  3. It scores every field (`sijil_core.confidence`) and writes the result to the workspace's **index database**.
  4. It routes the ledger row to `filed` or `needs_review` and sets the provisional retention.
- **Values in the index DB.** Field values never enter the shared control-plane DB; the ledger only gets `fields_sha256` at seal. The audit log records field *names*, never values.
- **Seal.** The control plane does it, in this order:
  1. the object store's COMPLIANCE lock;
  2. the index row frozen;
  3. the ledger row sealed, plus audit;
  4. after commit, enqueue `sijil.mirror_fields`, which writes Paperless custom fields.

  Every step is idempotent, so a failed seal is retried by calling it again.

**Tech Stack:** httpx against Ollama 0.40.13 (`/api/chat`, `format` = JSON schema, `think: false`), Pydantic v2 for the extraction schema, asyncpg/SQLAlchemy per-workspace engines for the index DB, Celery, FastAPI.

**Spec:** `SPEC.md` (REC-1…REC-4, CAP-4), as amended by `docs/superpowers/specs/2026-10-31-sijil-mvp-design.md` §5.3 (recognition and confidence), §5.4 (filing, sealing and retention), §5.5 (ledger and index DB), and §8 (model choice is the builder's). Parent plan: `docs/plan.md` (M3 table). Backlog: `docs/backlog-m1.md` ("Paperless HTTP 500 is final … do this early in M3").

## Global Constraints

- Everything in `docs/plan.md` § Global Constraints and in the M1 and M2 plans' Global Constraints still applies: error envelope, region checks, audit canonical JSON, and audit `data` values limited to str/int/bool/None.
- **New error codes:**

| Code | Status | Meaning |
| --- | --- | --- |
| `retention.locked` | 409 | the document is sealed: no field edit, no re-seal |
| `field.invalid` | 422 | body also carries `"field": "<name>"`; the value doesn't parse for that field |
| `field.unknown` | 422 | body also carries `"field": "<name>"` |
| `seal.fields_incomplete` | 422 | body also carries `"fields": [<names>]` that are required for the type and empty |
| `document.not_reviewable` | 409 | state is not `filed` or `needs_review` (e.g. still `received`) |

- **Field set** (names are API and storage keys, exactly):
  - `document_type`: one of `invoice`, `receipt`, `contract`, `bank_statement`, `payslip`, `tax_notice`, `other` (REC-2).
  - Text fields: `supplier_name`, `supplier_ice` (15 ASCII digits), `number`.
  - `date` (ISO `YYYY-MM-DD`).
  - Amounts: `total_ht`, `vat`, `total_ttc`. They are decimal strings with exactly 2 places, like `"1200.00"`, stored and sent as strings and never as floats.
  - `vat_rate`: a decimal string percent, like `"20"`.
  - `currency`: one of `MAD`, `EUR`, `USD`.
- **Required fields by type:**
  - `invoice` and `receipt`: `document_type`, `supplier_name`, `date`, `number`, `total_ht`, `vat`, `total_ttc`, `currency`. `supplier_ice` is also required for `invoice`.
  - Every other type: `document_type` and `date`.
  - Routing and `seal.fields_incomplete` use only the required fields. Optional fields are scored, but they don't block.
- **Confidence:**
  - A score is a float in [0, 1], from signals only, never from the model's own rating.
  - **Threshold `0.90`:** a document is `filed` iff every required field is ≥ 0.90 and it has no blocking flag. Otherwise it is `needs_review`.
  - Blocking flags: `possible_duplicate`, `no_text`, `ingest.unsupported_format`, `extraction_failed`.
- **Moroccan VAT rates:** 20, 14, 10 and 7 percent (design §5.3). A rate is valid when `abs(vat / total_ht * 100 - rate) <= 0.1` for one of them. `vat == 0` with `total_ht > 0` is invalid in M3: exemptions come later.
- **Totals check:** `abs(total_ht + vat - total_ttc) <= Decimal("0.02")`.
- **Plausible date:** between 15 years before the region-local today and 1 day after it.
- **Retention:**
  - Fiscal year = the calendar year (design §5.4). `retain_until = date(doc_date.year + retention_years(country), 12, 31)`.
  - Without a document date, the arrival date is used.
  - The ledger trigger only lets retention go up. So at filing **and** at seal, `retention_until = max(current, computed)`, and the object store lock is extended to match (ruling P1).
- **Model:**
  - `SIJIL_EXTRACTION_MODEL`, default `qwen3:4b`. It must fit the dev laptop: Docker has 8 GB, shared with two Paperless tenants (ruling P3).
  - `SIJIL_OLLAMA_URL`, default `http://ollama:11434`.
  - `SIJIL_AI_ALLOWED_HOSTS`, default `ollama`. The Ollama client refuses at construction any base URL whose host is not on that list.
  - Requests use `temperature: 0`, with `seed: 1` for run A and `seed: 2` for run B. Run B also uses the alternate prompt.
  - `options.num_ctx: 8192`, `think: false`.
  - Content is truncated to the first 12,000 characters after normalisation.
- **Text normalisation:**
  1. `unicodedata.normalize("NFKC", s)`.
  2. Remove the bidi controls U+200E, U+200F, U+061C, U+202A–U+202E and U+2066–U+2069.
  3. Collapse runs of horizontal whitespace (space, tab, U+00A0, U+202F, U+2009) to one space, keeping newlines.

  Facts observed in M3 pre-flight against `sample-04-ar` in a seeded tenant:
  - Paperless 3.4.1's `content` for Arabic PDFs is in logical order, with the digits correct (`٦١٥٫٨٠`, `٢٠٢٦`). It wraps every run in U+202A/U+202B…U+202C, and labels and amounts can come out in a different order from the page.
  - The raw PDF text layer (pypdf) is in visual order with reversed digit runs. Never extract from it: always read Paperless's content.
  - Presentation forms (U+FB50–U+FDFF, U+FE70–U+FEFF) map to base letters under NFKC. Arabic-Indic digits are kept by NFKC and handled by the parsers.
- **Audit actions written in M3:**
  - `document.extracted`, with data `{model, runs_agree_count: int, min_confidence_pct: int}`;
  - `document.filed`, `document.needs_review`, with data `{min_confidence_pct: int, flags: "<comma-joined flag names>"}`;
  - `document.extraction_failed`, with data `{code}`;
  - `document.fields_edited`, with data `{fields: "<comma-joined names>"}` and **never values**;
  - `document.sealed`, with data `{fields_sha256, retention_until: "YYYY-MM-DD"}`;
  - `review.listed`;
  - `document.fields_mirrored`.
- **Fields hash:** `fields_sha256 = sha256(canonical_json({"fields": <required+optional values with nulls>, "document_type": …}))`, using `sijil_core.audit.canonical_json`. Values are strings or null.

## Review Focus

1. **An Arabic invoice with Arabic-Indic digits** (`sample-04-ar`: `٦١٥٫٨٠`, month name) gets its amounts and date parsed to the same Decimal and date as its Latin-digit twin. → Task 2 `test_parse_amount_variants`, `test_parse_date_arabic_month_and_digits`; Task 8 asserts sample-04's parsed `total_ttc`.
2. **Ollama down, slow or returning malformed JSON:** the document stays `received` and is retried. Once the attempts run out it goes to `needs_review` with the flag `extraction_failed`, never silently `filed`. → Task 4 `test_ollama_unavailable_is_retried`, `test_malformed_json_twice_routes_to_review`.
3. **A sealed document edited through every path:** the API PATCH, a direct UPDATE on the index row, a direct UPDATE of the ledger's `fields_sha256`, `state` or `retention_until`, and shortening the object lock are all refused. → Task 6 `tests/retention/test_seal_lock.py`.
4. **A seal that fails halfway** (the object store locked, then the ledger commit fails) can be retried. It ends sealed exactly once, with one `document.sealed` audit event. → Task 6 `test_seal_retry_after_partial_failure`.
5. **An invoice dated in an earlier year than its arrival** never lowers `retention_until`, because the ledger trigger refuses that. The provisional and final retention take the max. → Task 5 `test_filing_never_lowers_retention`, Task 6 `test_seal_retention_is_max_of_provisional_and_doc_date`.

---

## File structure

```
libs/sijil-core/src/sijil_core/
  paperless.py        # + document_content, document_suggestions, ensure_document_type,
                      #   ensure_custom_field, set_custom_fields, set_title_and_type; 500 → PaperlessUnavailable
  text.py             # normalise(s) (NFKC + whitespace)
  fields.py           # parse_amount, parse_date, normalise_ice, parse_currency, FIELD_NAMES, REQUIRED_BY_TYPE
  confidence.py       # Signals, WEIGHTS (data), score(field, signals), route(scores, flags, required)
  ollama.py           # OllamaClient (allowlisted host), chat_json(...)
  index.py            # index DB schema (versioned DDL), ensure_schema, IndexEngines (per-workspace engine cache),
                      #   FieldsRow read/write helpers
  retention.py        # + fiscal_retain_until(doc_date, country)
services/worker/src/sijil_worker/
  extract.py          # extract_document orchestration (content → 2 runs → parse → score → index → route)
  taxonomy.py         # ensure_taxonomy(client) → {type: paperless_id}, custom fields
  mirror.py           # mirror_fields(ledger_id) → Paperless custom fields + document type
  eval.py             # `sijil-eval extraction --set …` (console script)
  ingest.py           # _settle(consumed) → enqueue extract after commit
  tasks.py            # extract_document, mirror_fields, requeue_unextracted tasks
services/control-plane/
  alembic/versions/0008_seal_guards.py
  src/sijil_cp/api/review.py   # review queue, fields get/patch, seal, seal-batch
fixtures/invoices/scan-02-ar.png  # SAMPLE: page 1 of sample-02-ar rasterised at 200 dpi (OCR path)
tests/retention/test_seal_lock.py
tests/integration/test_ocr.py, test_extraction.py, test_m3_acceptance.py
```

---

### Task 1: Paperless 500 is transient (backlog, "early in M3")

**Files:**
- Modify: `libs/sijil-core/src/sijil_core/paperless.py`, `services/worker/src/sijil_worker/ingest.py` (if `error_code`/`is_transient_cause` need it), `docs/backlog-m1.md` (strike the item)
- Test: `libs/sijil-core/tests/test_paperless_client.py`, `services/worker/tests/test_ingest.py`

**Interfaces:**
- Produces: a Paperless response with status 500 raises `PaperlessUnavailable(status=500)`; `is_transient_cause` is therefore true for it. Paperless 500s are **not** retried inside `_request` (only 502–504 are), so the Celery backoff and 24 h delay handle them.

- [ ] **Step 1: Write failing tests:**
  - `test_500_raises_unavailable`: client, respx 500 on `/api/documents/` → `PaperlessUnavailable` with status 500.
  - `test_ingest_500_is_delayed_not_failed`: ingest, upload answers 500 on every attempt with `will_retry` false → `ingest_state == queued`, an `ingest_next_attempt_at` is set, `document.ingest_delayed` is audited, and the state is never `failed`.
- [ ] **Step 2:** `uv run pytest libs/sijil-core/tests/test_paperless_client.py services/worker/tests/test_ingest.py -v`. Expected: the two new tests FAIL.
- [ ] **Step 3:** Map 500 in `_checked` and `_request` to `PaperlessUnavailable`.
- [ ] **Step 4:** `make check`. Expected: PASS.
- [ ] **Step 5:** Commit `fix(core): treat Paperless 500 as transient so ingest is delayed, not failed`.

### Task 2: Text normalisation and field parsers (3.2)

**Files:**
- Create: `libs/sijil-core/src/sijil_core/{text,fields}.py`
- Test: `libs/sijil-core/tests/test_text.py`, `libs/sijil-core/tests/test_fields.py`

**Interfaces:**
- Produces:
  - `text.normalise(s: str) -> str`.
  - `fields.parse_amount(s: str | None) -> Decimal | None`. Returns the value quantised to `0.01`, or None when it doesn't parse.
  - `fields.parse_date(s: str | None) -> date | None`.
  - `fields.normalise_ice(s: str | None) -> str | None`. Returns 15 ASCII digits, or None.
  - `fields.parse_currency(s: str | None) -> str | None`.
  - `fields.parse_vat_rate(s: str | None) -> Decimal | None`.
  - Constants: `FIELD_NAMES: tuple[str, ...]`, `DOCUMENT_TYPES: tuple[str, ...]`, `REQUIRED_BY_TYPE: dict[str, frozenset[str]]`, all as in Global Constraints.
  - `fields.parse_field(name: str, raw: str | None) -> str | None`: the canonical string form for storage (`"1200.00"`, `"2026-09-30"`, `"MAD"`). It raises `KeyError` for an unknown name.
- **Amount rules:**
  1. Map Arabic-Indic (U+0660–0669) and Extended Arabic-Indic (U+06F0–06F9) digits to ASCII. Map `٫` to the decimal mark and `٬` to the grouping mark.
  2. Strip currency tokens, case-insensitively, before parsing: `MAD`, `DH`, `DHS`, `درهم`, `د.م.`, `€`, `EUR`, `$`, `USD`. Also strip all spaces, including U+00A0, U+202F and U+2009.
  3. If both `.` and `,` are present, the right-most one is the decimal mark and the other is grouping.
  4. If only one kind is present: it is the decimal mark when exactly 1 or 2 digits follow its last occurrence. Otherwise it is grouping.
  5. Reject anything left that is not `^\d+(\.\d{1,2})?$`, as well as negatives.
- **Date rules:**
  - Accepted forms: ISO `YYYY-MM-DD`; `DD/MM/YYYY`, `DD-MM-YYYY` and `DD.MM.YYYY` (day first, never US order); `D <month> YYYY`.
  - The month name can be French (with or without accents, e.g. `septembre`, `aout`/`août`). It can also be Moroccan Arabic (`يناير فبراير مارس أبريل ماي يونيو يوليوز غشت شتنبر أكتوبر نونبر دجنبر`) or Mashriq Arabic (`كانون الثاني … كانون الأول`, `أيلول`, etc.).
  - Arabic-Indic digits are accepted in every form. Invalid calendar dates return None.

- [ ] **Step 1: Write failing tests:**
  - `test_normalise_maps_presentation_forms` (`"ﻓﺎﺗﻮﺭﺓ"` → `"فاتورة"`)
  - `test_normalise_collapses_nbsp_and_thin_space` (newlines are kept)
  - `test_normalise_strips_bidi_controls` (`"‫‪ ٦١٥٫٨٠‬درهم‬"` → `" ٦١٥٫٨٠درهم"` after the controls are removed; no U+202A–U+202E remains)
  - `test_parse_amount_variants`:

    | Input | Result |
    | --- | --- |
    | `"١٢٠٫٠٠"` | `Decimal("120.00")` |
    | `"1 200,00"` | `Decimal("1200.00")` |
    | `"1.200,00"` | `Decimal("1200.00")` |
    | `"1200.00"` | `Decimal("1200.00")` |
    | `"1,200.00"` | `Decimal("1200.00")` |
    | `"1 200,00 MAD"` | `Decimal("1200.00")` |
    | `"٦١٥٫٨٠ درهم"` | `Decimal("615.80")` |
    | `"3.275,50"` | `Decimal("3275.50")` |
    | `"1.200"` | `Decimal("1200.00")` |
    | `"12,5"` | `Decimal("12.50")` |
    | `"abc"`, `""`, `None`, `"-5,00"` | `None` |

  - `test_parse_amount_never_float`: assert the result type is `Decimal`.
  - `test_parse_date_forms`: `"2026-09-30"`, `"30/09/2026"`, `"30 septembre 2026"` and `"30 aout 2026"` → the matching date; `"31/02/2026"` → None.
  - `test_parse_date_arabic_month_and_digits`: `"٥ أكتوبر ٢٠٢٦"` → `date(2026,10,5)`, and `"31 غشت 2026"` → `date(2026,8,31)`.
  - `test_ice_must_be_15_digits`: `"001905837000041"` and `"001 905 837 000 041"` → `"001905837000041"`; Arabic-Indic digits map to ASCII; 14 or 16 digits, or letters → None.
  - `test_parse_field_canonical_strings`.
- [ ] **Step 2:** `uv run pytest libs/sijil-core/tests/test_text.py libs/sijil-core/tests/test_fields.py -v`. Expected: FAIL (module not found).
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the same command, then `make check`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(core): field parsers for Moroccan amounts, dates and ICE`.

### Task 3: Index database schema and access

**Files:**
- Create: `libs/sijil-core/src/sijil_core/index.py`
- Modify:
  - `libs/sijil-core/src/sijil_core/settings.py` (or a new `IndexSettings`): `SIJIL_INDEX_DB_HOST`, default `postgres`, and `SIJIL_INDEX_DB_PORT`, default `5432`;
  - `services/worker/src/sijil_worker/provisioning.py`: new step `ensure_index_schema` after `ensure_databases`;
  - compose env for the control plane and the worker, if a default doesn't fit.
- Test: `libs/sijil-core/tests/test_index.py` (db marker, against the test Postgres), `services/worker/tests/test_provisioning.py`

**Interfaces:**
- Produces:
  - `index.SCHEMA_VERSION = 1`. The DDL is applied in one transaction and recorded in `index_schema(version int primary key)`. `async ensure_schema(conn: AsyncConnection) -> None` is idempotent.
  - Table `document_fields`:
    - `ledger_id uuid primary key`
    - `document_type text`
    - `values jsonb not null` (name → canonical string or null)
    - `confidence jsonb not null` (name → float)
    - `signals jsonb not null` (name → {signal: bool|null})
    - `flags text[] not null default '{}'`
    - `source jsonb not null` (name → `"extraction"` or `"user"`)
    - `model text`
    - `extracted_at timestamptz`
    - `updated_at timestamptz not null`
    - `updated_by uuid` (null for system)
    - `sealed_at timestamptz`
    - `fields_sha256 text`
  - Trigger `document_fields_frozen`:
    - `DELETE` → `RAISE 'retention.locked'`;
    - `UPDATE` when `OLD.sealed_at IS NOT NULL` → `RAISE 'retention.locked'`;
    - `sealed_at` and `fields_sha256` can only be set together.
  - Table `extraction_run`: `id bigserial`, `ledger_id uuid`, `run text` (`"a"` or `"b"`), `model text`, `raw jsonb`, `at timestamptz`. This is the evidence for "two runs agree". It is append-only by trigger.
  - `index.IndexEngines(settings, keys: KeyService, sessions_cp)`, with `async engine(workspace: Workspace) -> AsyncEngine`:
    - it decrypts the `index_db_password` credential, which is enveloped with the workspace context like the other credentials;
    - it builds `postgresql+asyncpg://{index_db_name}:{pw}@{host}:{port}/{index_db_name}`, with `hide_parameters=True` and `pool_size=2`;
    - it runs `ensure_schema` once per engine;
    - it is an LRU of 32 engines, and evicted engines are disposed;
    - `async aclose()` disposes all of them.
  - `index.read_fields(conn, ledger_id) -> FieldsRow | None`, `index.write_extraction(conn, row: FieldsRow) -> None` (upsert that refuses once sealed), `index.find_possible_duplicates(conn, ledger_id, number, supplier_ice, total_ttc) -> list[UUID]`, `index.known_supplier(conn, supplier_ice) -> str | None` (the name on the most recently sealed row with that ICE). `FieldsRow` is a frozen dataclass with exactly the table's columns.

- [ ] **Step 1: Write failing tests:**
  - `test_ensure_schema_idempotent`
  - `test_sealed_row_update_refused` (`retention.locked` in the error)
  - `test_fields_row_delete_refused`
  - `test_extraction_run_append_only`
  - `test_engines_lru_disposes_evicted` (fake factory)
  - `test_index_db_password_from_credential` (round trip through `LocalKeyService`)
  - `test_find_possible_duplicates_matches_number_ice_ttc` (excludes the row itself)
  - `test_provisioning_ensures_index_schema` (fake runtime records the call)
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `make check`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(core): per-workspace index database schema for document fields`.

### Task 4: Ollama client and extraction (3.3)

**Files:**
- Create: `libs/sijil-core/src/sijil_core/ollama.py`, `services/worker/src/sijil_worker/{extract,taxonomy}.py`
- Modify:
  - `libs/sijil-core/src/sijil_core/paperless.py`: `document_content(doc_id) -> str`, `document_suggestions(doc_id) -> dict[str, list[int]]`, `ensure_document_type(name, match, algorithm) -> int`, `set_title_and_type(doc_id, title, document_type_id)`;
  - `services/worker/src/sijil_worker/{ingest,tasks,app}.py`: enqueue after `consumed`, the task `sijil.extract_document` on queue `extraction`, and the requeue sweep `sijil.requeue_unextracted` every 600 s;
  - `services/worker/src/sijil_worker/settings.py`: `extraction_model`, `ollama_url`, `ai_allowed_hosts`;
  - `infra/compose/docker-compose.yml`: the worker consumes `extraction`; `OLLAMA_MODEL` defaults to `qwen3:4b`;
  - `Makefile` (`make models` pulls `SIJIL_EXTRACTION_MODEL`).
- Test: `libs/sijil-core/tests/test_ollama.py`, `services/worker/tests/test_extract.py`, `tests/integration/test_ocr.py`, `tests/integration/test_extraction.py`

**Interfaces:**
- Consumes: Task 2 parsers, Task 3 `IndexEngines` and `write_extraction`, Task 5 `confidence.score_document` (the order is fine: Task 4 stores signals, and Task 5 adds the scoring and routing call. Until Task 5 lands, `extract_document` stops after `write_extraction`).
- Produces:
  - `ollama.OllamaClient(base_url: str, allowed_hosts: Collection[str], *, timeout_s: float = 180, transport: httpx.AsyncBaseTransport | None = None)`. It raises `ValueError("ai.host_not_allowed")` when the host isn't allowed.
  - `async chat_json(model, system, user, schema: dict, seed: int) -> dict[str, Any]`. It raises `OllamaUnavailable` (transient: connect errors, timeouts, 5xx) or `OllamaBadOutput` (unparseable JSON or schema mismatch).
  - `extract.RawExtraction`, a Pydantic model: every field in `FIELD_NAMES` is `str | None`, and `document_type` is a `Literal` of `DOCUMENT_TYPES` or None.
  - `extract.PROMPT_A` and `extract.PROMPT_B`: two differently worded system prompts, in English, which instruct the model to copy values as printed and to answer null when a value is absent. Both must also say:
    - `supplier_name` and `supplier_ice` belong to the company that **issued** the document, never the client (`Client`, `الزبون`). The pre-flight spike with the small model picked the client otherwise.
    - The labels, French and Arabic: HT `Total HT` / `المجموع دون احتساب الرسوم`; VAT `TVA` / `الضريبة على القيمة المضافة`; TTC `Total TTC` / `المجموع مع احتساب الرسوم`; ICE / `التعريف الموحد للمقاولة`; and `DH` / `درهم` → `MAD`.
    - Labels and amounts may appear in a different order from the page.
  - `async extract_document(ledger_id: UUID, *, sessions, keys, index: IndexEngines, paperless, ollama: OllamaClient, settings, now) -> None`. It runs only when `ledger.state == received` and `ingest_state == consumed`; otherwise it is a no-op.
    1. Assert region.
    2. `content = normalise(await client.document_content(doc_id))[:12000]`. Empty → flag `no_text`.
    3. Run A and run B, each stored in `extraction_run`.
    4. Parse each with `parse_field`.
    5. The value kept is run A's, or run B's when run A's is None.
    6. Signals: `runs_agree` is true when both canonical values are equal and not None.
    7. `paperless_type`: the type name of the first id in `document_suggestions(doc_id)["document_types"]` mapped back through `ensure_taxonomy`, or None.
    8. Write `document_fields`.
    9. Audit `document.extracted`.
  - `ingest_state == unsupported` documents: no extraction; Task 5 routes them to review with flag `ingest.unsupported_format`.
  - `taxonomy.ensure_taxonomy(client) -> dict[str, int]` creates or finds the 7 document types, named exactly as in `DOCUMENT_TYPES`, each with `matching_algorithm = 4` (regex) and `is_insensitive = true`:

    | Type | Regex |
    | --- | --- |
    | `invoice` | `\b(facture|invoice)\b|فاتورة` |
    | `receipt` | `\b(re[çc]u|receipt|ticket de caisse)\b|وصل` |
    | `contract` | `\b(contrat|contract)\b|عقد` |
    | `bank_statement` | `relev[ée] (bancaire|de compte)|bank statement|كشف الحساب` |
    | `payslip` | `bulletin de (paie|salaire)|payslip|ورقة الأداء` |
    | `tax_notice` | `avis d'imposition|tax notice|إشعار ضريبي` |
    | `other` | no rule, `matching_algorithm = 0` |

    It is idempotent: an existing type with the same name is reused.
  - The Celery task retries `OllamaUnavailable` and `PaperlessUnavailable` with backoff: factor 30 s, max 900 s, 8 retries. After that, and on `OllamaBadOutput` from both runs, the row goes to `needs_review` with flag `extraction_failed` and audit `document.extraction_failed`.

- [ ] **Step 1: Write failing unit tests** (an httpx `MockTransport` for Ollama, respx for Paperless, `MemoryObjectStore`, and the test index DB):
  - `test_no_external_host_called`: a transport that records every request host, with a pipeline run → every host is `ollama` or the tenant's Paperless host. Also `OllamaClient("https://api.example.com", {"ollama"})` raises `ai.host_not_allowed`.
  - `test_extraction_schema`: stubbed Ollama returns fixture JSON for sample-01 → `document_fields.values` equal the canonical strings from the manifest.
  - `test_two_runs_disagree_sets_signal_false`
  - `test_ollama_unavailable_is_retried` (raises for Celery; the state stays `received`)
  - `test_malformed_json_twice_routes_to_review` (after Task 5 lands; in Task 4 assert the flag `extraction_failed` is stored)
  - `test_extract_noop_unless_received_and_consumed`
  - `test_audit_has_no_field_values`: the `document.extracted` data contains no supplier name, number or amount.
  - `test_ensure_taxonomy_idempotent`
  - `test_ingest_consumed_enqueues_extract_after_commit`
- [ ] **Step 2: Write failing integration tests:**
  - `tests/integration/test_ocr.py::test_arabic_fixture_text_extracted`: seed `sample-02-ar.pdf` into a provisioned workspace and wait for `consumed`. `normalise(document_content)` contains `normalise("أطلس لخدمات الاتصالات")` and `F-2026-01002`.
  - `test_scanned_arabic_ocr`: add `fixtures/invoices/scan-02-ar.png`, rendered by `fixtures/generate.py` with `pypdfium2` at 200 dpi and marked SAMPLE in the README. After consume, the content contains `001905837000041` and `F-2026-01002`, and `difflib.SequenceMatcher` on the supplier name gives ≥ 0.8.
  - `test_archive_is_pdfa`: download the archive version, whose XMP contains `pdfaid:part`.
  - `test_extraction.py::test_extracts_sample_01_with_real_ollama`: `total_ttc == "1500.00"` and `supplier_ice == "001905837000041"`. Skipped with a clear reason if the model isn't pulled.
- [ ] **Step 3:** Run the unit tests and `make itest`. Expected: FAIL.
- [ ] **Step 4:** Implement. Pull the model with `make models` once.
- [ ] **Step 5:** `make check && make itest`. Expected: PASS. Record the seconds per run on the dev laptop in the report.
- [ ] **Step 6:** Commit `feat(worker): extract document fields with the in-region model`, plus a separate `chore(fixtures): scanned Arabic sample for the OCR path`.

### Task 5: Confidence and routing (3.4, 3.5)

**Files:**
- Create: `libs/sijil-core/src/sijil_core/confidence.py`
- Modify: `libs/sijil-core/src/sijil_core/retention.py` (`fiscal_retain_until`), `services/worker/src/sijil_worker/extract.py` (score, then route)
- Test: `libs/sijil-core/tests/test_confidence.py`, `services/worker/tests/test_extract.py`

**Interfaces:**
- Produces:
  - `confidence.WEIGHTS: Mapping[str, Mapping[str, float]]` (data, per field → signal → weight) and `confidence.HARD: Mapping[str, frozenset[str]]`. A failed hard signal caps the field at `0.5`.

    | Field | Signals (weight) | Hard |
    | --- | --- | --- |
    | `document_type` | `runs_agree` 0.6, `paperless_agrees` 0.4 | — |
    | `supplier_name` | `runs_agree` 0.5, `in_text` 0.2, `supplier_known` 0.3 | `present` |
    | `supplier_ice` | `runs_agree` 0.4, `ice_15_digits` 0.3, `supplier_known` 0.3 | `present`, `ice_15_digits` |
    | `date` | `runs_agree` 0.6, `date_plausible` 0.4 | `present`, `date_plausible` |
    | `number` | `runs_agree` 0.6, `in_text` 0.4 | `present` |
    | `total_ht`, `vat`, `total_ttc` | `runs_agree` 0.5, `totals_consistent` 0.5 | `present`, `totals_consistent` |
    | `vat` (additionally) | `vat_rate_valid` 0.5 | `vat_rate_valid` |
    | `vat_rate` | `runs_agree` 0.5, `vat_rate_valid` 0.5 | — |
    | `currency` | `runs_agree` 0.6, `currency_valid` 0.4 | `present`, `currency_valid` |

    A signal whose value is None is not applicable: it is left out of both numerator and denominator. A field with no applicable signals scores 0.0.
  - `confidence.compute_signals(values_a, values_b, content, *, paperless_type: str | None, known_supplier: str | None, today: date) -> dict[str, dict[str, bool | None]]`:
    - `in_text`: the normalised, case-folded value is a substring of the normalised, case-folded content.
    - `supplier_known`: None when no known supplier exists for the ICE. Otherwise the supplier name equals the known one, compared case-folded after `normalise`.
    - `paperless_agrees`: None when `paperless_type` is None. Otherwise `paperless_type == value`.
  - `confidence.score(field: str, signals: Mapping[str, bool | None]) -> float`.
  - `confidence.route(scores: Mapping[str, float], flags: Collection[str], document_type: str | None) -> Literal["filed", "needs_review"]`, using `REQUIRED_BY_TYPE` (type None → the `invoice` set) and threshold `0.90` (`>=`).
  - `retention.fiscal_retain_until(doc_date: date, country: str) -> date`.
  - Extract, after scoring:
    1. Look up `find_possible_duplicates`; any hit adds flag `possible_duplicate` (CAP-4 part 2).
    2. Route.
    3. `retention_until = max(ledger.retention_until, fiscal_retain_until(date or region_local_date(ingested_at), country))`. If it increased, `store.extend_retention(ref, new)` runs first, then the ledger update.
    4. Set the ledger state.
    5. Audit `document.filed` or `document.needs_review`.
    6. Best effort `set_title_and_type(doc_id, f"{date} {supplier_name} {number}", type_id)`, with missing parts skipped. A Paperless failure here is logged and doesn't change routing.

- [ ] **Step 1: Write failing tests:**
  - `test_totals_mismatch_lowers_confidence_below_090`: sample-05 values with printed TTC `2808.50` → `total_ttc` < 0.90 and the route is `needs_review`.
  - `test_all_checks_pass_scores_at_least_090`
  - `test_invalid_vat_rate_flags_vat`: VAT 15% → `vat` < 0.90.
  - `test_possible_duplicate_flag`: a second document with the same number, ICE and TTC is routed to `needs_review` with the flag.
  - `test_routing_threshold_boundary`: 0.8999 → `needs_review`; 0.90 → `filed`.
  - `test_not_applicable_signal_ignored`
  - `test_optional_field_low_does_not_block`
  - `test_unsupported_ingest_routes_to_review`
  - `test_filing_never_lowers_retention`: arrival in 2026, document date in 2024 → unchanged 2036-12-31.
  - `test_filing_extends_retention_and_object_lock`: arrival 2026-01-05, document date 2027-01-02 → 2037-12-31, with the store extended before the ledger.
  - `test_fiscal_retain_until_ma`
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `make check`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(core): signal-based field confidence and filing/review routing`.

### Task 6: Seal guards, review and seal API (3.6)

**Files:**
- Create: `services/control-plane/alembic/versions/0008_seal_guards.py`, `services/control-plane/src/sijil_cp/api/review.py`, `tests/retention/__init__.py`, `tests/retention/test_seal_lock.py`
- Modify:
  - `services/control-plane/src/sijil_cp/main.py`: router, plus an `IndexEngines` on `app.state`, closed in the lifespan;
  - `services/control-plane/src/sijil_cp/enqueue.py`: `sijil.mirror_fields`;
  - `docs/architecture.md`: lock layers and the seal order;
  - the root `pyproject.toml`: if `tests/retention` isn't collected by `make check`, add it.
- Test: `services/control-plane/tests/test_review_api.py`, `services/control-plane/tests/test_ledger_schema.py`, `tests/retention/test_seal_lock.py`

**Interfaces:**
- **Migration 0008.** The ledger trigger also raises `ledger.immutable` when `OLD.sealed_at IS NOT NULL` and any of `fields_sha256`, `state` or `retention_until` differs. `fields_sha256` is NULL → value once. A CHECK `(state = 'sealed') = (sealed_at IS NOT NULL) AND (sealed_at IS NULL) = (fields_sha256 IS NULL) AND (sealed_by IS NULL OR sealed_at IS NOT NULL)` applies, named `ck_document_ledger_seal_consistent`.
- **Endpoints**, all under `/v1/workspaces/{workspace_id}`, with region and TOTP enforcement as in M2:
  - `GET /review?cursor=&limit=` (`documents_review`):
    - returns `needs_review` rows, oldest first, as `{items: [{id, state, document_type, fields: {name: {value, confidence, source}}, flags, required: [names]}], next_cursor}`;
    - field values come from the index DB;
    - audited as `review.listed`.
  - `GET /documents/{doc_id}/fields` (`documents_read`) returns the same item shape plus `sealed_at` and `fields_sha256`, and is audited as `document.viewed`.
  - `PATCH /documents/{doc_id}/fields` (`documents_review`), with body `{"fields": {name: string|null}}`:
    - unknown name → 422 `field.unknown`; value fails `parse_field` → 422 `field.invalid`;
    - a sealed ledger, or the index trigger raising → 409 `retention.locked`;
    - state `received` → 409 `document.not_reviewable`;
    - stores the canonical values, with `source[name] = "user"` and `confidence[name] = 1.0`;
    - audited as `document.fields_edited` with names only;
    - returns the item.
  - `POST /documents/{doc_id}/seal` (`documents_seal`):
    - state not in {`filed`, `needs_review`} → 409 (`retention.locked` if sealed, `document.not_reviewable` otherwise);
    - missing required fields → 422 `seal.fields_incomplete`;
    - then, in order:
      1. compute `fields_sha256`;
      2. `final = max(ledger.retention_until, fiscal_retain_until(date, country))`;
      3. `store.seal(ref, final)` (COMPLIANCE);
      4. index UPDATE setting `sealed_at` and `fields_sha256` (if already sealed with the same hash, continue; with a different hash, 409 `retention.locked`);
      5. ledger `state=sealed`, `fields_sha256`, `sealed_at`, `sealed_by`, `retention_until=final`, plus audit `document.sealed`, in one transaction;
      6. after commit, enqueue `sijil.mirror_fields`;
    - returns the item, with 200.
  - `POST /seal-batch` (`documents_seal`), with body `{"month": "YYYY-MM"}`:
    - seals every `filed` row of the workspace whose region-local `ingested_at` falls in that month, oldest first, at most 500 per call, through the same function as the single seal (one `document.sealed` per document);
    - returns `{sealed: int, skipped: [{id, code}], remaining: int}`.
- **Worker** `mirror.mirror_fields(ledger_id)`:
  - ensures the custom fields `sijil_supplier` (string), `sijil_ice` (string), `sijil_number` (string), `sijil_date` (date), `sijil_total_ht`, `sijil_vat` and `sijil_total_ttc` (monetary, e.g. `"MAD1500.00"`), and `sijil_sealed` (boolean);
  - sets them and the document type on the Paperless document;
  - audited as `document.fields_mirrored`; transient errors are retried.
- `tests/retention/test_seal_lock.py` (layer per test) holds:
  - `test_api_patch_after_seal_409`
  - `test_index_update_after_seal_refused`
  - `test_ledger_fields_sha256_state_retention_frozen_after_seal`
  - `test_object_lock_cannot_shorten_after_seal`, which uses `MemoryObjectStore` semantics and the RustFS integration variant if one exists.

- [ ] **Step 1: Write failing tests:**
  - `test_seal_freezes_fields`
  - `test_patch_after_seal_409_retention_locked`
  - `test_batch_seal_one_audit_event_per_document`
  - `test_readonly_cannot_seal` (403)
  - `test_member_can_seal`
  - `test_seal_requires_required_fields`
  - `test_patch_invalid_amount_422_field_invalid`
  - `test_patch_audit_names_only`
  - `test_seal_retention_is_max_of_provisional_and_doc_date`
  - `test_seal_retry_after_partial_failure` (Review Focus 4: the ledger commit fails once → the second call succeeds, with exactly one `document.sealed`)
  - `test_review_lists_needs_review_only`
  - `test_seal_enqueues_mirror_after_commit`
  - `test_mirror_sets_custom_fields` (worker, respx)
  - the lock suite above
  - the schema tests for 0008
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `make check`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(cp): review queue, field edits and sealing with compliance lock`.

### Task 7: Accuracy harness (3.7)

**Files:**
- Create: `services/worker/src/sijil_worker/eval.py` (console script `sijil-eval` in `services/worker/pyproject.toml`)
- Modify: `Makefile` (`make eval WS=<workspace id>`: runs `sijil-eval` in the worker container)
- Test: `services/worker/tests/test_eval.py`

**Interfaces:**
- Produces: `sijil-eval extraction --set fixtures/invoices/manifest.json --workspace <uuid> [--json]`.
  - For each manifest entry, it finds the document in the workspace's Paperless by the SHA-256 of the fixture file. It reads its `document_fields` from the index DB, or runs `extract_document`'s extraction step without routing when absent.
  - It compares `supplier_name` (against `fixtures/suppliers.json` by ICE, NFKC-normalised and case-folded), `supplier_ice`, `date`, `number`, `total_ht`, `vat` and `total_ttc`. The expected `total_ttc` is `printed_ttc` when present.
  - It prints a table with one row per field and columns `fr`, `ar` and `all`. Each cell is `correct/total (pct%)`, the percentage as an integer.
  - It prints the spec targets (fr ≥ 95%, ar ≥ 90% on supplier, date and total) as a reference line marked "reported, not gated" (design §5.3).
  - `--json` prints `{"fields": {field: {"fr": {"correct", "total"}, "ar": {...}}}, "model": ...}`.
  - It exits 0 whatever the accuracy, and 2 when the set or workspace is not found.
- `report(results: Sequence[EvalResult]) -> str` and `tally(...)` are pure functions.

- [ ] **Step 1: Write failing tests:**
  - `test_eval_report_format` (fixed results → exact table text)
  - `test_eval_uses_printed_ttc`
  - `test_eval_supplier_compared_normalised`
  - `test_eval_json_shape`
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `make check`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(worker): extraction accuracy report per field and language`.

### Task 8: Seed, docs and M3 acceptance

**Files:**
- Create: `tests/integration/test_m3_acceptance.py`
- Modify:
  - `infra/scripts/seed.sh` (wait until no seeded document is `received`, then print the review count);
  - `docs/architecture.md` (pipeline, index DB, seal order);
  - `CLAUDE.md` (commands: `make models`, `make eval`);
  - `docs/plan.md` (mark M3 done items).
- Test: `tests/integration/test_m3_acceptance.py`

- [ ] **Step 1: Write the acceptance test.** Seed the 12 invoices (reuse `tests/support/seed.py`), wait up to 45 min until every invoice leaves `received`, then assert:
  - `sample-05-fr` and `sample-08-ar` are `needs_review`, with `total_ttc` confidence < 0.90;
  - every document is either `filed` or `needs_review` (none `received` or stuck);
  - sample-04's `total_ttc` value is `"658.91"`, which proves the Arabic-Indic path, or the document is in review with that field below 0.90. The test records which;
  - after sealing one review document (PATCH `total_ttc` to the manifest value, then seal), a PATCH returns `409 retention.locked`;
  - the object's retention mode is `COMPLIANCE` with the final date;
  - Paperless shows `sijil_sealed = true` within 60 s;
  - `sijil-eval extraction --set fixtures/invoices/manifest.json --workspace <id>` exits 0, and its output has `fr` and `ar` columns. The output is attached to the test log.
- [ ] **Step 2:** `make itest`. Expected: FAIL before the docs and seed changes only if they're needed. Otherwise PASS; investigate any failure with superpowers:systematic-debugging.
- [ ] **Step 3:** Update the docs and seed.
- [ ] **Step 4:** `make check && make smoke && make itest`. Expected: PASS. Record the eval table and timings in the M3 report.
- [ ] **Step 5:** Commit `test: M3 acceptance` and `docs: M3 pipeline, index DB and seal order`.
