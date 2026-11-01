# The worker joins the Docker socket's group (GNU stat on Linux; Docker Desktop uses group 0).
export DOCKER_SOCKET_GID ?= $(shell stat -c %g /var/run/docker.sock 2>/dev/null || echo 0)
COMPOSE := docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env

.PHONY: install lint typecheck test check itest e2e up down smoke seed models

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

itest:
	$(COMPOSE) --profile test run --rm --build tests uv run --frozen pytest -m integration

e2e:
	pnpm e2e

check: lint typecheck test

up:
	test -f infra/compose/.env || cp infra/compose/.env.example infra/compose/.env
	$(COMPOSE) up -d --build

down:
	infra/scripts/clean-tenants.sh $(if $(findstring -v,$(ARGS)),--volumes)
	$(COMPOSE) down $(ARGS)

smoke:
	infra/scripts/smoke.sh

models:
	$(COMPOSE) --profile models run --rm ollama-pull

seed:
	infra/scripts/seed.sh
