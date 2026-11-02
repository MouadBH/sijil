from pydantic import PostgresDsn, RedisDsn

from sijil_core.crypto import KeySettings
from sijil_core.settings import CoreSettings
from sijil_core.storage import StorageSettings


class CpSettings(KeySettings, StorageSettings):
    # Object-store fields come from StorageSettings (SIJIL_OBJECTSTORE_*).
    # The app role: DML only. It can't alter tables, triggers or grants (retention layer 3).
    database_url: PostgresDsn
    redis_url: RedisDsn
    # Required, no default: a forgotten flag must not silently ship non-Secure cookies.
    cookie_secure: bool
    # Domain of the workspaces' inbound email addresses (`{slug}.{token}@{domain}`).
    inbound_domain: str


class MigrationSettings(CoreSettings):
    # The schema owner, used only by Alembic in the one-shot `migrate` service.
    migration_database_url: PostgresDsn
