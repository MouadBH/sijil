"""Ask with the real in-region models (slow: `make acceptance`), on the dev SAMPLE workspace.

`make seed` archives the 12 SAMPLE invoices (fixtures/invoices/manifest.json, fictitious
suppliers) in the SAMPLE workspace and prints its id. The test signs in as the SAMPLE Owner and
makes sure the two Atlas Télécom invoices of 2026 are reviewed and sealed with their manifest
values, as a reviewer would (a field edit, then the seal; nothing is done again once they are
sealed). Then it asks, through the API and the Ask worker (`qwen3:4b` reads and writes,
`bge-m3` embeds), for the VAT paid to Atlas Télécom in 2026: once in Arabic, once in French.

Each answer must carry the exact figure, computed independently here from the manifest with
Decimal, and at least one citation; its text is either the model's in the question's language,
or null with the templated `ask.answer.sum`. The answers, figures, citations and timings are
printed (run with -s).

SKIPPED, with the reason, when the SAMPLE workspace does not exist (`make seed`).
"""

import asyncio
import json
import os
import re
import time
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from sqlalchemy import select

from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import Organisation, Workspace
from tests.support.seed import SAMPLE_EMAIL, SAMPLE_ORG, signed_in

pytestmark = [pytest.mark.integration, pytest.mark.slow]

FIXTURES = Path(__file__).parents[2] / "fixtures"
MANIFEST: list[dict[str, Any]] = json.loads((FIXTURES / "invoices" / "manifest.json").read_text())
SUPPLIERS = {s["ice"]: s for s in json.loads((FIXTURES / "suppliers.json").read_text())}
ATLAS = "001905837000041"  # SAMPLE: Atlas Télécom Services SARL (fictitious)
YEAR = 2026
QUESTIONS = {
    "ar": "كم دفعنا من الضريبة على القيمة المضافة لشركة أطلس لخدمات الاتصالات سنة 2026؟",
    "fr": "Combien de TVA avons-nous payée à Atlas Télécom Services en 2026 ?",
}
ANSWER_BUDGET_S = 1800
POLL_S = 5
_ARABIC = re.compile(r"[ء-ي]")
_LATIN = re.compile(r"[A-Za-z]")


def _csrf(http: httpx.AsyncClient) -> dict[str, str]:
    return {"X-CSRF-Token": http.cookies["sijil_csrf"]}


def _items() -> list[dict[str, Any]]:
    return [i for i in MANIFEST if i["supplier_ice"] == ATLAS and i["date"].startswith(f"{YEAR}-")]


def _expected() -> Decimal:
    """The VAT paid to Atlas in 2026, from the manifest alone."""
    return sum((Decimal(i["tva"]) for i in _items()), Decimal("0"))


async def _workspace() -> UUID:
    engine = make_engine(os.environ["SIJIL_DATABASE_URL"])
    try:
        async with make_sessionmaker(engine)() as db:
            found = await db.scalar(
                select(Workspace.id)
                .join(Organisation, Organisation.id == Workspace.org_id)
                .where(Organisation.name == SAMPLE_ORG)
                .order_by(Workspace.created_at)
            )
    finally:
        await engine.dispose()
    if found is None:
        pytest.skip("no SAMPLE workspace: run `make seed`")
    return found


def _values(item: dict[str, Any]) -> dict[str, str]:
    """The invoice's fields as printed (the Arabic one names the supplier in Arabic)."""
    supplier = SUPPLIERS[item["supplier_ice"]]
    return {
        "document_type": "invoice",
        "supplier_name": supplier["name_ar"] if item["lang"] == "ar" else supplier["name"],
        "supplier_ice": item["supplier_ice"],
        "date": item["date"],
        "number": item["number"],
        "total_ht": item["ht"],
        "vat": item["tva"],
        "vat_rate": str(supplier["vat_rate"]),
        "total_ttc": item["ttc"],
        "currency": item["currency"],
    }


async def _reviewed(http: httpx.AsyncClient, base: str, item: dict[str, Any]) -> str:
    """The invoice's document, sealed with its manifest values (reviewed now if it waits)."""
    r = await http.post(
        f"{base}/search",
        json={"filters": {"q": item["number"]}, "sort": "date_desc"},
        headers=_csrf(http),
    )
    assert r.status_code == 200, r.text
    hits = [h for h in r.json()["items"] if h["state"] in ("needs_review", "filed", "sealed")]
    assert hits, f"{item['file']} is not in the SAMPLE workspace: run `make seed`"
    sealed = [h for h in hits if h["state"] == "sealed"]
    if sealed:
        return str(sealed[0]["id"])
    doc = hits[0]["id"]
    r = await http.patch(
        f"{base}/documents/{doc}/fields", json={"fields": _values(item)}, headers=_csrf(http)
    )
    assert r.status_code == 200, r.text
    r = await http.post(f"{base}/documents/{doc}/seal", headers=_csrf(http))
    assert r.status_code == 200, r.text
    return str(doc)


async def _answered(http: httpx.AsyncClient, base: str, ask_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + ANSWER_BUDGET_S
    while True:
        r = await http.get(f"{base}/ask/{ask_id}")
        assert r.status_code == 200, r.text
        body: dict[str, Any] = r.json()
        if body["state"] in ("answered", "failed"):
            return body
        assert time.monotonic() < deadline, f"no answer within {ANSWER_BUDGET_S} s: {body}"
        await asyncio.sleep(POLL_S)


async def test_vat_question_answered_with_the_computed_figure() -> None:
    expected = _expected()
    assert expected == Decimal("429.80")
    workspace_id = await _workspace()
    base = f"/v1/workspaces/{workspace_id}"
    async with signed_in(SAMPLE_EMAIL) as http:
        docs = {item["file"]: await _reviewed(http, base, item) for item in _items()}
        print(f"\nSAMPLE workspace {workspace_id}; Atlas 2026 documents: {docs}")
        print(f"expected VAT (manifest, Decimal): {expected}")
        for locale, question in QUESTIONS.items():
            started = time.monotonic()
            r = await http.post(
                f"{base}/ask", json={"question": question, "locale": locale}, headers=_csrf(http)
            )
            assert r.status_code == 202, r.text
            answer = await _answered(http, base, r.json()["id"])
            took = time.monotonic() - started
            print(f"\n[{locale}] {question}\n  took {took:.1f} s")
            print(f"  answer_text: {answer['answer_text']!r}")
            print(f"  answer_code: {answer['answer_code']!r}")
            print(f"  figures: {answer['figures']}")
            print(f"  excluded: {answer['excluded']}")
            print(f"  citations: {[(c['id'], c['supplier_name']) for c in answer['citations']]}")

            assert answer["state"] == "answered", answer
            vat = [f for f in answer["figures"] if f["measure"] == "vat"]
            assert vat == [
                {"measure": "vat", "value": str(expected), "currency": "MAD",
                 "document_count": len(docs)}
            ]  # fmt: skip
            assert answer["citations"], "no citation"
            assert {c["id"] for c in answer["citations"]} <= set(docs.values())
            text = answer["answer_text"]
            if text is None:
                assert answer["answer_code"] == "ask.answer.sum"
                continue
            assert answer["answer_code"] is None
            arabic, latin = len(_ARABIC.findall(text)), len(_LATIN.findall(text))
            assert arabic > latin if locale == "ar" else latin > arabic, text
