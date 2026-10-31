from pydantic import PostgresDsn, RedisDsn

from sijil_core.settings import CoreSettings


class CpSettings(CoreSettings):
    # The app role: DML only. It can't alter tables, triggers or grants (retention layer 3).
    database_url: PostgresDsn
    redis_url: RedisDsn


class MigrationSettings(CoreSettings):
    # The schema owner, used only by Alembic in the one-shot `migrate` service.
    migration_database_url: PostgresDsn
