from pydantic import PostgresDsn, RedisDsn, SecretStr

from sijil_core.crypto import KeySettings
from sijil_core.settings import CoreSettings
from sijil_core.storage import StorageSettings


class CpSettings(KeySettings, StorageSettings):
    # Object-store fields come from StorageSettings (SIJIL_OBJECTSTORE_*): the services' app
    # key, which can put and extend a lock but never replace one.
    # The sealing key (SIJIL_OBJECTSTORE_SEAL_*): retention only (read it, set it, with the
    # GOVERNANCE bypass needed to switch to COMPLIANCE), never an object's bytes or a delete.
    # Only the control plane holds it, for the seal path.
    objectstore_seal_access_key: SecretStr
    objectstore_seal_secret_key: SecretStr
    # The app role: DML only. It can't alter tables, triggers or grants (retention layer 3).
    database_url: PostgresDsn
    redis_url: RedisDsn
    # Required, no default: a forgotten flag must not silently ship non-Secure cookies.
    cookie_secure: bool
    # Domain of the workspaces' inbound email addresses (`{slug}.{token}@{domain}`).
    inbound_domain: str

    def seal_storage(self) -> StorageSettings:
        """The object-store settings of the sealing key (same endpoint and bucket)."""
        return StorageSettings.model_construct(
            **{
                **{name: getattr(self, name) for name in StorageSettings.model_fields},
                "objectstore_access_key": self.objectstore_seal_access_key,
                "objectstore_secret_key": self.objectstore_seal_secret_key,
            }
        )


class MigrationSettings(CoreSettings):
    # The schema owner, used only by Alembic in the one-shot `migrate` service.
    migration_database_url: PostgresDsn
