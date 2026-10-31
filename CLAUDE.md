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
| `services/worker` | `sijil_worker`: Celery (queues `default`, `provisioning`, `ingest`, `extraction`) |
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
make down ARGS=-v
```

- Single Python test: `uv run pytest path/to/test.py::test_name -v`.
- Single JS test: `pnpm --filter web exec vitest run path`.

## Working

- TDD: write the failing test, watch it fail, then write the code.
- Each milestone gets a branch `m<N>-<name>` in a worktree under `.claude/worktrees/`. Never push to `main` without asking.
- Commits follow Conventional Commits (`feat(cp): …`, `fix(worker): …`, `chore(infra): …`) and stay small.
- Python 3.13: ruff (line length 100), `mypy --strict`, pytest with `asyncio_mode=auto`.
- TypeScript: strict, ESLint flat config, Vitest for units, Playwright for e2e.
- Shared Python code goes in `sijil_core`. Never import `sijil_cp` from the worker or the reverse.
- Sample data: put it in `fixtures/` and mark it `SAMPLE`. Never commit real customer documents.
