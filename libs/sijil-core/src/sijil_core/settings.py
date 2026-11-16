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

    # Where the per-workspace index databases live (the shared Postgres on the core network).
    index_db_host: str = "postgres"
    index_db_port: int = 5432


# 15 digits, like an organisation's ICE (`organisation.ice`).
Ice = Annotated[str, StringConstraints(pattern=r"^[0-9]{15}$")]
NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class SellerSettings(CoreSettings):
    """Who issues the invoices (ADM-4): SIJIL_SELLER_*, every one required, with no default in
    code (ruling R2). The deployment's env carries the real values; `infra/compose/.env.example`
    carries SAMPLE ones. Copied into each invoice at issue (`seller()`), so a later change never
    alters an issued invoice."""

    seller_name: NonBlank
    seller_address: NonBlank
    seller_ice: Ice
    # Identifiant fiscal, registre du commerce, taxe professionnelle.
    seller_if: NonBlank
    seller_rc: NonBlank
    seller_tp: NonBlank
    seller_rib: NonBlank
    seller_bank: NonBlank

    def seller(self) -> dict[str, str]:
        """The snapshot stored in `invoice.seller`."""
        return {
            "name": self.seller_name,
            "address": self.seller_address,
            "ice": self.seller_ice,
            "if": self.seller_if,
            "rc": self.seller_rc,
            "tp": self.seller_tp,
            "rib": self.seller_rib,
            "bank": self.seller_bank,
        }
