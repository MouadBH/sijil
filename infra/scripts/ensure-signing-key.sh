#!/usr/bin/env bash
# The region's Ed25519 export key pair in infra/compose/.env (or the file given as $1):
# SIJIL_EXPORT_SIGNING_KEY (base64 of the raw 32-byte seed, only `worker-export` holds it) and
# SIJIL_EXPORT_PUBLIC_KEY (base64 of the raw 32-byte public key).
#
# Generated once, when both are absent or empty; an existing pair is never overwritten, and
# half a pair is refused (a new seed would orphan every export signed so far). Neither value is
# ever printed.
set -euo pipefail
umask 077

env_file=${1:-"$(dirname "$0")/../compose/.env"}
[ -f "$env_file" ] || { echo "ensure-signing-key: $env_file not found" >&2; exit 1; }

value() { sed -n "s/^$1=//p" "$env_file" | tail -n 1; }
seed=$(value SIJIL_EXPORT_SIGNING_KEY)
public=$(value SIJIL_EXPORT_PUBLIC_KEY)
if [ -n "$seed" ] && [ -n "$public" ]; then
  exit 0
fi
if [ -n "$seed" ] || [ -n "$public" ]; then
  echo "ensure-signing-key: only half of the export key pair is set in $env_file; refusing" >&2
  exit 1
fi

# PKCS#8 DER of an Ed25519 key is 48 bytes ending with the seed; its SPKI DER 44 bytes ending
# with the public key. The DER travels as base64 only, never as a file.
generate_openssl() {
  local der
  der=$(openssl genpkey -algorithm ed25519 -outform DER 2>/dev/null | openssl base64 -A) || return 1
  [ "$(printf '%s' "$der" | openssl base64 -d -A | wc -c | tr -d ' ')" = 48 ] || return 1
  seed=$(printf '%s' "$der" | openssl base64 -d -A | tail -c 32 | openssl base64 -A)
  public=$(printf '%s' "$der" | openssl base64 -d -A \
    | openssl pkey -inform DER -pubout -outform DER | tail -c 32 | openssl base64 -A)
}

# An openssl without Ed25519 (e.g. an old LibreSSL): the project's own cryptography package.
generate_python() {
  local pair
  pair=$(uv run --frozen --quiet python -c '
import base64
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
key = Ed25519PrivateKey.generate()
seed = key.private_bytes_raw()
print(base64.b64encode(seed).decode(), base64.b64encode(key.public_key().public_bytes_raw()).decode())
')
  seed=${pair% *}
  public=${pair#* }
}

generate_openssl || generate_python
[ "${#seed}" = 44 ] && [ "${#public}" = 44 ] \
  || { echo "ensure-signing-key: key generation failed" >&2; exit 1; }

# Replace the (empty) lines in place, or add them; the file keeps its permissions.
tmp=$(mktemp "${env_file}.XXXXXX")
trap 'rm -f "$tmp"' EXIT
SEED="$seed" PUBLIC="$public" awk '
  /^SIJIL_EXPORT_SIGNING_KEY=/ { print "SIJIL_EXPORT_SIGNING_KEY=" ENVIRON["SEED"]; s = 1; next }
  /^SIJIL_EXPORT_PUBLIC_KEY=/ { print "SIJIL_EXPORT_PUBLIC_KEY=" ENVIRON["PUBLIC"]; p = 1; next }
  { print }
  END {
    if (!s) print "SIJIL_EXPORT_SIGNING_KEY=" ENVIRON["SEED"]
    if (!p) print "SIJIL_EXPORT_PUBLIC_KEY=" ENVIRON["PUBLIC"]
  }
' "$env_file" > "$tmp"
cat "$tmp" > "$env_file"
echo "ensure-signing-key: generated the region's export key pair into $env_file" >&2
