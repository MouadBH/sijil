COMPOSE := docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env

.PHONY: install lint typecheck test check e2e up down smoke models

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

test:
	uv run pytest
	pnpm test

e2e:
	pnpm e2e

check: lint typecheck test

up:
	test -f infra/compose/.env || cp infra/compose/.env.example infra/compose/.env
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down $(ARGS)

smoke:
	infra/scripts/smoke.sh

models:
	$(COMPOSE) exec ollama ollama pull $${OLLAMA_MODEL:-qwen3:8b}
