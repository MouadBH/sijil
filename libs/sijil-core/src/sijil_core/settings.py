from typing import Annotated, Literal

from pydantic import StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

# Region ids are <country>-<site>, e.g. "ma-cas" (Morocco, Casablanca).
RegionId = Annotated[str, StringConstraints(pattern=r"^[a-z]{2}-[a-z]{3}$")]


class CoreSettings(BaseSettings):
    """Settings every Sijil Python service shares. Read from SIJIL_* env vars.

    Both are required with no default: a service that boots without knowing its
    region or environment must fail loudly, not run as an empty region or as dev.
    """

    model_config = SettingsConfigDict(env_prefix="SIJIL_", extra="ignore")

    region: RegionId
    environment: Literal["dev", "test", "prod"]
