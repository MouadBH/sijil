#!/usr/bin/env bash
# Roles and databases for the local stack. Values are passed as psql variables
# (:"ident" / :'literal'), never spliced into SQL, so any character is safe.
#
#   sijil_cp_owner  owns sijil_cp; used only by migrations
#   sijil_cp_app    DML on sijil_cp; what the API and worker connect as
#   paperless_tN    owns paperless_tN; handed to that tenant's Paperless-ngx
#   index_tN        owns index_tN (pgvector); Sijil only, never handed to Paperless
set -euo pipefail

sql() { psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" "$@"; }

create_owned_db() { # create_owned_db <role> <password> <database> [extension]
  sql -v role="$1" -v pw="$2" -v db="$3" <<'SQL'
CREATE ROLE :"role" LOGIN PASSWORD :'pw';
CREATE DATABASE :"db" OWNER :"role";
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
SQL
  if [ -n "${4:-}" ]; then
    sql --dbname "$3" -v ext="$4" <<<'CREATE EXTENSION :"ext";'
  fi
}

# Nobody but the superuser connects to the maintenance database.
sql <<<'REVOKE CONNECT ON DATABASE postgres FROM PUBLIC;'

create_owned_db sijil_cp_owner "$SIJIL_CP_OWNER_DB_PASSWORD" sijil_cp
sql --dbname sijil_cp -v pw="$SIJIL_CP_APP_DB_PASSWORD" <<'SQL'
CREATE ROLE sijil_cp_app LOGIN PASSWORD :'pw';
GRANT CONNECT ON DATABASE sijil_cp TO sijil_cp_app;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO sijil_cp_app;
-- Tables the owner creates later are readable/writable by the app, nothing more.
-- Append-only tables (audit_event) revoke UPDATE/DELETE in their own migration.
ALTER DEFAULT PRIVILEGES FOR ROLE sijil_cp_owner IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO sijil_cp_app;
ALTER DEFAULT PRIVILEGES FOR ROLE sijil_cp_owner IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO sijil_cp_app;
SQL

create_owned_db paperless_t1 "$T1_DB_PASSWORD" paperless_t1
create_owned_db paperless_t2 "$T2_DB_PASSWORD" paperless_t2
create_owned_db index_t1 "$T1_INDEX_DB_PASSWORD" index_t1 vector
create_owned_db index_t2 "$T2_INDEX_DB_PASSWORD" index_t2 vector
