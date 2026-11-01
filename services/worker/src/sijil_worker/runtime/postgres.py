"""Tenant databases, created as the `sijil_provisioner` role (CREATEDB CREATEROLE, no superuser).

Each tenant gets two roles and two databases, each role owning its own database only:
`paperless_<ws>` (handed to that tenant's Paperless-ngx) and `index_<ws>` (pgvector, cloned from
`template_index`, used by Sijil only). Every call is idempotent and resets the passwords, so a
retry after a partial run converges.

DDL cannot take bind parameters. Identifiers are validated against a strict pattern first, and
the statements are then built server-side with `format('%I', '%L')`, so Postgres does the quoting.
"""

import re

import asyncpg

from sijil_worker.runtime.base import TenantSpec

# Lower-case, digits and underscore only, at most Postgres's 63-byte identifier limit.
_IDENTIFIER = re.compile(r"^[a-z0-9_]{1,63}$")
INDEX_TEMPLATE = "template_index"


def validate_identifier(name: str) -> str:
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError("invalid SQL identifier")
    return name


def _dsn(url: str) -> str:
    # Settings carry SQLAlchemy URLs (postgresql+asyncpg://); asyncpg wants plain postgresql://.
    return re.sub(r"^postgresql\+asyncpg://", "postgresql://", url)


async def _run(conn: asyncpg.Connection, template: str, *args: str) -> None:
    """Executes `template` (a constant) with `args` quoted by Postgres's own `format()`."""
    params = ", ".join(f"${i}::text" for i in range(2, len(args) + 2))
    statement = await conn.fetchval(f"SELECT format($1::text, {params})", template, *args)  # noqa: S608
    await conn.execute(statement)


async def _ensure_role(conn: asyncpg.Connection, role: str, password: str) -> None:
    if not await conn.fetchval("SELECT 1 FROM pg_roles WHERE rolname = $1", role):
        try:
            await _run(conn, "CREATE ROLE %I LOGIN", role)
        except asyncpg.DuplicateObjectError:
            pass  # a concurrent run created it first
    # NOSUPERUSER is the default and only a superuser may even state it.
    await _run(
        conn,
        "ALTER ROLE %I WITH LOGIN NOCREATEDB NOCREATEROLE PASSWORD %L",
        role,
        password,
    )


async def _ensure_database(
    conn: asyncpg.Connection, db: str, owner: str, template: str | None = None
) -> None:
    if not await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", db):
        try:
            if template is None:
                await _run(conn, "CREATE DATABASE %I OWNER %I", db, owner)
            else:
                await _run(conn, "CREATE DATABASE %I OWNER %I TEMPLATE %I", db, owner, template)
        except asyncpg.DuplicateDatabaseError:
            pass
    # Only the owner can revoke PUBLIC's default CONNECT/TEMP: as the provisioner (no grant
    # option) the REVOKE would be a silent no-op, leaving the database open to every tenant role.
    await _run(conn, "SET ROLE %I", owner)
    try:
        await _run(conn, "REVOKE ALL ON DATABASE %I FROM PUBLIC", db)
    finally:
        await conn.execute("RESET ROLE")
    still_open = await conn.fetchval(
        "SELECT has_database_privilege('public', $1, 'CONNECT')"
        " OR has_database_privilege('public', $1, 'TEMPORARY')",
        db,
    )
    if still_open:
        raise RuntimeError("PUBLIC still holds privileges on a tenant database")


async def ensure_tenant_databases(provisioner_url: str, spec: TenantSpec) -> None:
    db = validate_identifier(spec.db_name)
    index_db = validate_identifier(spec.index_db_name)
    conn = await asyncpg.connect(_dsn(provisioner_url))
    try:
        await _ensure_role(conn, db, spec.db_password)
        await _ensure_role(conn, index_db, spec.index_db_password)
        await _ensure_database(conn, db, db)
        await _ensure_database(conn, index_db, index_db, INDEX_TEMPLATE)
    finally:
        await conn.close()


async def drop_tenant_databases(provisioner_url: str, db_name: str, index_db_name: str) -> None:
    """Drops both databases and roles if present. Only `destroy` calls this."""
    names = [validate_identifier(db_name), validate_identifier(index_db_name)]
    conn = await asyncpg.connect(_dsn(provisioner_url))
    try:
        for name in names:
            if await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", name):
                # Dropping needs ownership: act as the owner (the provisioner may SET to roles it
                # created, without inheriting their rights).
                await _run(conn, "SET ROLE %I", name)
                try:
                    await _run(conn, "DROP DATABASE IF EXISTS %I WITH (FORCE)", name)
                finally:
                    await conn.execute("RESET ROLE")
            if await conn.fetchval("SELECT 1 FROM pg_roles WHERE rolname = $1", name):
                await _run(conn, "DROP ROLE IF EXISTS %I", name)
    finally:
        await conn.close()
