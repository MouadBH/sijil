# The worker joins the Docker socket's group (GNU stat on Linux; Docker Desktop uses group 0).
export DOCKER_SOCKET_GID ?= $(shell stat -c %g /var/run/docker.sock 2>/dev/null || echo 0)
COMPOSE := docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env

.PHONY: install lint typecheck test check itest acceptance e2e up down smoke seed models eval

install:
	uv sync --all-packages
	pnpm install

lint:
	uv run ruff check .
	uv run ruff format --check .
	pnpm lint

typecheck:
	uv run mypy libs services fixtures
	pnpm typecheck

test: export SIJIL_TEST_DATABASE_URL ?= postgresql+asyncpg://postgres:dev-postgres-change-me@127.0.0.1:55432/postgres
test: export SIJIL_TEST_REDIS_URL ?= redis://127.0.0.1:56379/15
test:
	uv run pytest
	pnpm test

# Day to day: everything but the slow (model-bound) acceptance.
itest:
	$(COMPOSE) --profile test run --rm --build tests uv run --frozen pytest -m "integration and not slow"

# The slow acceptance (M3: 18 documents through the model, up to 45 min), its log printed (-s).
acceptance:
	$(COMPOSE) --profile test run --rm --build tests uv run --frozen pytest -m "integration and slow" -s -v

e2e:
	pnpm e2e

check: lint typecheck test

up:
	test -f infra/compose/.env || cp infra/compose/.env.example infra/compose/.env
	@# An .env from an earlier milestone gets the keys added since (e.g. the removal key, M4),
	@# after a newline if its last line has none (else the first key would join that line).
	@[ -z "$$(tail -c 1 infra/compose/.env)" ] || echo >> infra/compose/.env
	@awk -F= 'NR == FNR { have[$$1] = 1; next } /^[A-Z][A-Z0-9_]*=/ && !($$1 in have) { print; print "added " $$1 > "/dev/stderr" }' \
		infra/compose/.env infra/compose/.env.example >> infra/compose/.env
	@# The region's export signing pair: generated once, never overwritten.
	infra/scripts/ensure-signing-key.sh infra/compose/.env
	$(COMPOSE) up -d --build

# Plain `make down` only stops the compose services: provisioned tenants stay, because their
# rows stay in the database. `ARGS=-v` wipes the database too, so it removes the tenants first.
down:
	$(if $(findstring -v,$(ARGS)),infra/scripts/clean-tenants.sh --volumes)
	$(COMPOSE) down $(ARGS)

smoke:
	infra/scripts/smoke.sh

# Pulls SIJIL_EXTRACTION_MODEL and SIJIL_EMBEDDING_MODEL (from the environment or
# infra/compose/.env; qwen3:4b and bge-m3 by default).
models:
	$(COMPOSE) --profile models run --rm ollama-pull

seed:
	infra/scripts/seed.sh

# Extraction accuracy on the SAMPLE invoices ingested in workspace WS (reported, not gated;
# read-only: nothing is written to the archive). A one-off worker-extraction container (same
# networks and settings) with the fixtures mounted read-only; no permanent mount.
eval:
	$(if $(WS),,$(error usage: make eval WS=<workspace id>))
	$(COMPOSE) run --rm --no-deps -T -v "$(CURDIR)/fixtures:/src/fixtures:ro" --entrypoint sijil-eval worker-extraction extraction --set /src/fixtures/invoices/manifest.json --workspace $(WS) $(ARGS)
