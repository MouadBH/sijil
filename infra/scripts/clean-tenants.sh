#!/usr/bin/env bash
# Remove the worker-provisioned tenants (Paperless + Redis containers, tenant networks) of THIS
# compose project; with --volumes also their data/media volumes. `docker compose down` knows
# nothing about them. Only resources labelled sijil.tenant on a network labelled
# sijil.project=<this project> are touched, so other projects on the same engine are safe.
# `make down ARGS=-v` runs it (the database goes too, so the tenants must); plain `make down`
# does not, since the tenants' rows survive it.
# Usage: clean-tenants.sh [--volumes]
set -euo pipefail

cd "$(dirname "$0")/../compose"
project="${COMPOSE_PROJECT_NAME:-}"
if [ -z "$project" ] && [ -f .env ]; then
  project="$(sed -n 's/^COMPOSE_PROJECT_NAME=//p' .env | tail -1)"
fi
project="${project:-sijil}"
volumes=0
[ "${1:-}" = "--volumes" ] && volumes=1

warn() { echo "clean-tenants: warning: $*" >&2; }

networks="$(docker network ls --filter "label=sijil.project=$project" --filter label=sijil.tenant --format '{{.Name}}')"
for net in $networks; do
  ref="$(docker network inspect "$net" --format '{{index .Labels "sijil.tenant"}}' || true)"
  [ -n "$ref" ] || continue
  echo "removing tenant $ref of project $project"
  containers="$(docker ps -a -q --filter "label=sijil.tenant=$ref")"
  if [ -n "$containers" ]; then
    # shellcheck disable=SC2086  # one id per word
    docker rm -f -v $containers >/dev/null || warn "could not remove the containers of $ref"
  fi
  # Compose services (control-plane, worker, postgres, ...) are attached to the tenant network.
  for endpoint in $(docker network inspect "$net" --format '{{range .Containers}}{{.Name}} {{end}}' || true); do
    docker network disconnect -f "$net" "$endpoint" >/dev/null \
      || warn "could not disconnect $endpoint from $net"
  done
  docker network rm "$net" >/dev/null || warn "could not remove network $net"
  if [ "$volumes" = 1 ]; then
    for kind in data media; do
      volume="${project}_${ref}_${kind}"
      if docker volume inspect "$volume" >/dev/null 2>&1; then
        docker volume rm "$volume" >/dev/null || warn "could not remove volume $volume"
      fi
    done
  fi
done
