from pydantic import PostgresDsn, RedisDsn

from sijil_core.settings import CoreSettings


class CpSettings(CoreSettings):
    database_url: PostgresDsn
    redis_url: RedisDsn
