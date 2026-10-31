from pydantic import RedisDsn

from sijil_core.settings import CoreSettings


class WorkerSettings(CoreSettings):
    redis_url: RedisDsn
