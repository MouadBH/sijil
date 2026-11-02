"""The document types every workspace's Paperless carries, with our regex matching rules.

Paperless's own suggestion for a document (`/api/documents/{id}/suggestions/`) is matched
against these rules and used as one confidence signal for `document_type` (ruling P2: the
built-in Paperless AI is not enabled). Names are exactly `sijil_core.fields.DOCUMENT_TYPES`.
"""

from typing import Final

from sijil_core.fields import DOCUMENT_TYPES
from sijil_core.paperless import PaperlessClient

MATCH_NONE: Final = 0
MATCH_REGEX: Final = 4

RULES: Final[dict[str, str]] = {
    "invoice": r"\b(facture|invoice)\b|فاتورة",
    "receipt": r"\b(re[çc]u|receipt|ticket de caisse)\b|وصل",
    "contract": r"\b(contrat|contract)\b|عقد",
    "bank_statement": r"relev[ée] (bancaire|de compte)|bank statement|كشف الحساب",
    "payslip": r"bulletin de (paie|salaire)|payslip|ورقة الأداء",
    "tax_notice": r"avis d'imposition|tax notice|إشعار ضريبي",
}


async def ensure_taxonomy(client: PaperlessClient) -> dict[str, int]:
    """Creates or finds the document types: `{type name: Paperless id}`. Idempotent."""
    found: dict[str, int] = {}
    for name in DOCUMENT_TYPES:
        rule = RULES.get(name)
        if rule is None:  # `other`: never matched automatically
            found[name] = await client.ensure_document_type(name, "", MATCH_NONE)
        else:
            found[name] = await client.ensure_document_type(name, rule, MATCH_REGEX)
    return found
