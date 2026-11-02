#!/usr/bin/env bash
# Seed 12 SAMPLE Moroccan invoices (6 via the API, 6 by email) into the SAMPLE workspace.
# Idempotent. Runs inside the compose `tests` runner so it can reach the control plane and smtp.
set -euo pipefail

cd "$(dirname "$0")/../.."
docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env \
  --profile test run --rm --build tests uv run --frozen python -m tests.support.seed
