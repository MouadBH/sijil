"""The SAMPLE seller every test suite issues invoices as (SIJIL_SELLER_*, ruling R2): a
fictitious company, ICE, bank and RIB, the same values as infra/compose/.env.example."""

from sijil_core.settings import SellerSettings

SAMPLE_SELLER = {
    "seller_name": "SAMPLE Sijil SARL",
    "seller_address": "SAMPLE 1 rue Exemple, Casablanca",
    "seller_ice": "009999999000099",
    "seller_if": "99999999",
    "seller_rc": "999999",
    "seller_tp": "99999999",
    "seller_rib": "999 780 0000000000000000 99",
    "seller_bank": "SAMPLE Banque",
}


def sample_seller(**overrides: str) -> SellerSettings:
    return SellerSettings.model_validate(
        {"region": "ma-cas", "environment": "test"} | SAMPLE_SELLER | overrides
    )
