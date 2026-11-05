from pydantic import PostgresDsn, RedisDsn, field_validator

from sijil_core.crypto import KeySettings
from sijil_core.export_report import decode_key
from sijil_core.settings import CoreSettings
from sijil_core.storage import StorageSettings


class CpSettings(KeySettings, StorageSettings):
    # Object-store fields come from StorageSettings (SIJIL_OBJECTSTORE_*): the services' app key,
    # which can put and extend a lock but never replace one. The control plane never holds the
    # sealing key: the seal worker (`sijil.lock_sealed`) switches originals to COMPLIANCE.
    # The app role: DML only. It can't alter tables, triggers or grants (retention layer 3).
    database_url: PostgresDsn
    redis_url: RedisDsn
    # Required, no default: a forgotten flag must not silently ship non-Secure cookies.
    cookie_secure: bool
    # Domain of the workspaces' inbound email addresses (`{slug}.{token}@{domain}`).
    inbound_domain: str
    # The region's Ed25519 export key, public half (base64 of the raw 32 bytes): published at
    # `GET /v1/regions/{region}/export-key`. The signing seed is the export worker's alone.
    export_public_key: str

    @field_validator("export_public_key")
    @classmethod
    def _check_public_key(cls, v: str) -> str:
        decode_key(v)
        return v

    def export_public_key_bytes(self) -> bytes:
        return decode_key(self.export_public_key)


class MigrationSettings(CoreSettings):
    # The schema owner, used only by Alembic in the one-shot `migrate` service.
    migration_database_url: PostgresDsn
