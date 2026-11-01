#!/usr/bin/env bash
# Remove the worker-provisioned tenants (Paperless + Redis containers, tenant networks) of THIS
# compose project; with --volumes also their data/media volumes. `docker compose down` knows
# nothing about them. Only resources labelled sijil.tenant on a network labelled
# sijil.project=<this project> are touched, so other projects on the same engine are safe.
# Usage: clean-tenants.sh [--volumes]
set -uo pipefail

cd "$(dirname "$0")/../compose"
project="${COMPOSE_PROJECT_NAME:-}"
if [ -z "$project" ] && [ -f .env ]; then
  project="$(sed -n 's/^COMPOSE_PROJECT_NAME=//p' .env | tail -1)"
fi
project="${project:-sijil}"
volumes=0
[ "${1:-}" = "--volumes" ] && volumes=1

networks="$(docker network ls --filter "label=sijil.project=$project" --filter label=sijil.tenant --format '{{.Name}}')"
for net in $networks; do
  ref="$(docker network inspect "$net" --format '{{index .Labels "sijil.tenant"}}')"
  [ -n "$ref" ] || continue
  echo "removing tenant $ref of project $project"
  docker ps -a -q --filter "label=sijil.tenant=$ref" | xargs -r docker rm -f -v >/dev/null
  # Compose services (control-plane, worker, postgres, ...) are attached to the tenant network.
  for endpoint in $(docker network inspect "$net" --format '{{range .Containers}}{{.Name}} {{end}}'); do
    docker network disconnect -f "$net" "$endpoint" >/dev/null
  done
  docker network rm "$net" >/dev/null
  if [ "$volumes" = 1 ]; then
    for kind in data media; do
      docker volume rm "${project}_${ref}_${kind}" >/dev/null 2>&1 || true
    done
  fi
done
exit 0
