#!/usr/bin/env bash
# Boot check for the local stack: every service answers, tenants are private.
set -uo pipefail

cd "$(dirname "$0")/../compose"
compose() { docker compose --env-file .env "$@"; }
failures=0
pass() { printf '  ok    %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; failures=$((failures + 1)); }

echo "waiting for control plane readiness (up to 300 s)..."
for _ in $(seq 1 60); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' localhost:8000/v1/health/ready)" = "200" ] && break
  sleep 5
done

[ "$(curl -s -o /dev/null -w '%{http_code}' localhost:8000/v1/health/ready)" = "200" ] \
  && pass "control plane ready (db + redis up)" || fail "control plane /v1/health/ready != 200"

curl -s localhost:8000/v1/health | grep -q '"region":"ma-cas"' \
  && pass "control plane reports region ma-cas" || fail "control plane region"

probe() { # probe <url>: HTTP status seen from inside the control-plane container
  compose exec -T control-plane python - "$1" 2>/dev/null <<'PY'
import sys, urllib.error, urllib.request
try:
    print(urllib.request.urlopen(sys.argv[1], timeout=10).status)
except urllib.error.HTTPError as e:
    print(e.code)
except Exception:
    print("000")
PY
}

for t in t1 t2; do
  echo "waiting for paperless-$t (up to 300 s)..."
  for _ in $(seq 1 60); do
    case "$(probe "http://paperless-$t:8000/api/")" in 200|401|403) break ;; esac
    sleep 5
  done
  case "$(probe "http://paperless-$t:8000/api/")" in
    200|401|403) pass "paperless-$t answers on the tenants network" ;;
    *) fail "paperless-$t unreachable from control plane" ;;
  esac
  # `compose port` prints ":0" (or nothing) when the port is not published.
  case "$(compose port "paperless-$t" 8000 2>/dev/null)" in ''|':0') true ;; *) false ;; esac \
    && pass "paperless-$t has no published port" || fail "paperless-$t is published to the host"
  langs=$(compose exec -T "paperless-$t" tesseract --list-langs 2>/dev/null)
  grep -qx ara <<<"$langs" \
    && pass "paperless-$t has Arabic OCR" || fail "paperless-$t missing tesseract ara"
  compose exec -T "paperless-$t" curl -s --max-time 5 -o /dev/null https://example.com \
    && fail "paperless-$t can reach the internet" || pass "paperless-$t has no route out"
done

compose exec -T ollama ollama list >/dev/null 2>&1 \
  && pass "ollama answers" || fail "ollama"

case "$(probe "http://objectstore:9000/")" in
  ''|000) fail "object store unreachable" ;;
  *) pass "object store answers" ;;
esac

# Capture first: `grep -q` exiting early would SIGPIPE celery and trip pipefail.
ping_out=$(compose exec -T worker celery -A sijil_worker.main inspect ping --timeout 10 2>/dev/null)
grep -q pong <<<"$ping_out" \
  && pass "worker answers celery ping" || fail "worker ping"

if [ "$failures" -eq 0 ]; then echo "smoke: all checks passed"; else echo "smoke: $failures check(s) failed"; exit 1; fi
