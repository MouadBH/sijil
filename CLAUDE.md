# Sijil — conventions

Sijil is a hosted compliance archive for Moroccan SMEs, built beside an **unmodified** Paperless-ngx per workspace.

- Product source of truth: `SPEC.md`, amended by `docs/superpowers/specs/2026-10-31-sijil-mvp-design.md` (the design wins where they differ).
- Plan: `docs/plan.md`. Architecture: `docs/architecture.md`.

## Hard rules

1. **Never modify, fork or copy Paperless-ngx.**
   - Use the image `ghcr.io/paperless-ngx/paperless-ngx:3.4.1`, configured only by environment variables.
   - Talk to it over REST, from `services/` only, through `sijil_core.paperless`.
2. **No JS package talks to Paperless.** `apps/*` and `packages/*` call only the control-plane `/v1` API. `apps/web/tests/no-paperless.test.ts` enforces this.
3. **Region.**
   - Every service requires `SIJIL_REGION`.
   - Every workspace-scoped request and job calls `sijil_core.region.assert_region`.
   - Never send document content to a host outside the region. AI goes to the compose `ollama` service only.
4. **Retention lock is absolute.** It is enforced in four layers: API, Paperless service-user permissions, DB constraint, object lock. Every new path that can change or delete a document needs a test in the lock suite (`tests/retention/`).
5. **Integrity.**
   - SHA-256 for every ingested file.
   - The audit log is append-only: `hash = sha256(prev_hash ‖ canonical_json(event))`.
   - Never write UPDATE or DELETE against `audit_event`.
6. **No user-facing strings outside `packages/i18n/messages/{fr,ar,en}.json`.**
   - The API returns machine codes (`retention.locked`, `region.mismatch`), never prose.
   - JSX literals are an ESLint error (`react/jsx-no-literals`).
   - Use logical CSS properties (`margin-inline-start`, not `margin-left`) so Arabic RTL works.
7. **Money is `Decimal`** (Python) or a string/minor units (TS). Never use float.

## Layout

| Path | What |
| --- | --- |
| `apps/web` | Next.js 15 customer app (next-intl, locales `fr` default, `ar` RTL, `en`) |
| `apps/site` | Next.js 15 marketing site, static export |
| `packages/i18n` | Message catalogues + `locales`, `dirFor()` |
| `services/control-plane` | `sijil_cp`: FastAPI, SQLAlchemy 2 (async), Alembic |
| `services/worker` | `sijil_worker`: Celery (queues `default`, `provisioning`, `ingest`, `extraction`, `seal`, `removal`, `export`). Compose runs it as `worker` (`default`, `provisioning`, `ingest`; Docker socket; purges removed documents in Paperless and the index), `worker-extraction` (`extraction`, concurrency 1, the model), `worker-seal` (`seal`, the only holder of the sealing key), `worker-removal` (`removal`, concurrency 1, the only holder of the removal key: the only service that deletes an original), `worker-export` (`export`, the only holder of the export key and the region's signing seed), `smtp` and `beat`. The seal, removal and export workers are on the `core` network only |
| `libs/sijil-core` | `sijil_core`: anything both Python services use (settings, region, crypto, Paperless client) |
| `infra/compose` | Local stack; tenants live on the internal `tenants` network with no published ports |
| `fixtures/` | **Sample data only**, fictitious companies and ICE numbers |

## Commands

```bash
make install    # uv sync --all-packages && pnpm install
make check      # lint + typecheck + unit tests, Python and JS
make e2e        # Playwright
make up         # boot the compose stack (copies .env.example on first run)
make smoke      # assert every service is healthy and tenants are not exposed
make models     # pull the extraction model (qwen3:4b) into the ollama volume, once
make seed       # 12 SAMPLE invoices (6 via the API, 6 by email) into the SAMPLE workspace; idempotent,
                # prints the workspace id, then waits until all are routed and prints filed/review counts
make eval WS=…  # per-field extraction accuracy (fr, ar, all) on the SAMPLE set in workspace WS; read-only
make itest      # integration tests in the compose `tests` runner, except `slow` (includes the M1/M2/M4 acceptance
                # and the lock suite's Paperless and object-store paths)
make acceptance # the `slow` integration tests: the M3 acceptance (18 documents through the model, up to 45 min)
make down ARGS=-v
```

- Single Python test: `uv run pytest path/to/test.py::test_name -v`.
- The retention-lock suite: `uv run pytest tests/retention -v` (needs `SIJIL_TEST_DATABASE_URL`); its `test_lock_all_paths.py` is the role × path matrix.
- An export ZIP: `sijil-cp export verify <zip> [--against-ledger]` prints `ok` or one problem code per line.
- Single JS test: `pnpm --filter web exec vitest run path`.
- Mark a model-bound integration test `@pytest.mark.slow` so `make itest` stays usable day to day.
- An integration test that creates a workspace appends its id to the `created_workspaces` fixture (or uses the t1 `harness`), which marks it `closing` at teardown: only `active` workspaces extract.

## Working

- TDD: write the failing test, watch it fail, then write the code.
- Each milestone gets a branch `m<N>-<name>` in a worktree under `.claude/worktrees/`. Never push to `main` without asking.
- Commits follow Conventional Commits (`feat(cp): …`, `fix(worker): …`, `chore(infra): …`) and stay small.
- Python 3.13: ruff (line length 100), `mypy --strict`, pytest with `asyncio_mode=auto`.
- TypeScript: strict, ESLint flat config, Vitest for units, Playwright for e2e.
- Shared Python code goes in `sijil_core`. Never import `sijil_cp` from the worker or the reverse.
- Sample data: put it in `fixtures/` and mark it `SAMPLE`. Never commit real customer documents.
