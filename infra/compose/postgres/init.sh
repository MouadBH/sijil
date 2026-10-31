#!/usr/bin/env bash
# One role and one database per owner: the control plane, and each sample tenant's
# Paperless-ngx database plus its Sijil index database (pgvector).
set -euo pipefail

create() { # create <role> <password> <database> [extension]
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<SQL
CREATE ROLE $1 LOGIN PASSWORD '$2';
CREATE DATABASE $3 OWNER $1;
REVOKE ALL ON DATABASE $3 FROM PUBLIC;
SQL
  if [ -n "${4:-}" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$3" -c "CREATE EXTENSION $4;"
  fi
}

create sijil_cp "$SIJIL_CP_DB_PASSWORD" sijil_cp
create paperless_t1 "$T1_DB_PASSWORD" paperless_t1
create paperless_t2 "$T2_DB_PASSWORD" paperless_t2
create index_t1 "$T1_DB_PASSWORD" index_t1 vector
create index_t2 "$T2_DB_PASSWORD" index_t2 vector
