from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class CoreSettings(BaseSettings):
    """Settings every Sijil Python service shares. Read from SIJIL_* env vars."""

    model_config = SettingsConfigDict(env_prefix="SIJIL_", extra="ignore")

    region: str
    environment: Literal["dev", "test", "prod"] = "dev"
