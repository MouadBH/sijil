#!/usr/bin/env bash
# Seed 12 SAMPLE Moroccan invoices (6 via the API, 6 by email) into the SAMPLE workspace, then
# wait until none of its documents is still `received` (each extracted by the model and routed),
# printing progress, and print how many were filed and how many wait in review.
# Idempotent. Runs inside the compose `tests` runner so it can reach the control plane and smtp.
# SEED_ROUTED_TIMEOUT_S (default 2700 s: two model runs per document, one at a time).
set -euo pipefail

cd "$(dirname "$0")/../.."
docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env \
  --profile test run --rm --build tests uv run --frozen python -m tests.support.seed \
  --wait-routed "${SEED_ROUTED_TIMEOUT_S:-2700}"
