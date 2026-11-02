"""REC-1: the text extraction reads comes from the workspace's Paperless (`paperless-t1`).

Runs in the compose `tests` runner (`make itest`). No model is needed: these check the text
Paperless gives for an Arabic PDF with a text layer, for a raster scan of it (Tesseract
`ara+fra+eng`), and that the archived version is PDF/A.
"""

import difflib
import io
import json
from pathlib import Path

import pytest
from pypdf import PdfReader

from sijil_core.db.models import DocumentLedger, IngestState
from sijil_core.filetypes import FileKind
from sijil_core.paperless import PaperlessClient
from sijil_core.text import normalise
from tests.support.t1 import T1_URL, Harness

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).parents[2] / "fixtures"
SUPPLIER = next(
    s
    for s in json.loads((FIXTURES / "suppliers.json").read_text())
    if s["ice"] == "001905837000041"
)
NAME_AR = SUPPLIER["name_ar"]  # أطلس لخدمات الاتصالات
NUMBER = "F-2026-01002"
ICE = "001905837000041"
MIN_NAME_SIMILARITY = 0.8


async def consumed(harness: Harness, name: str, kind: FileKind) -> DocumentLedger:
    data = (FIXTURES / "invoices" / name).read_bytes()
    await harness.forget_in_paperless(data)
    row = await harness.ingest(await harness.ledger(await harness.workspace(), data, kind))
    assert row.ingest_state is IngestState.consumed, row.ingest_error_code
    assert row.paperless_doc_id is not None
    return row


async def content_of(harness: Harness, row: DocumentLedger) -> str:
    assert row.paperless_doc_id is not None
    async with PaperlessClient(T1_URL, token=harness.token) as client:
        return normalise(await client.document_content(row.paperless_doc_id))


def best_line_similarity(content: str, wanted: str) -> float:
    target = normalise(wanted)
    return max(
        (
            difflib.SequenceMatcher(None, target, line.strip()).ratio()
            for line in content.splitlines()
            if line.strip()
        ),
        default=0.0,
    )


async def test_arabic_fixture_text_extracted(harness: Harness) -> None:
    content = await content_of(harness, await consumed(harness, "sample-02-ar.pdf", FileKind.pdf))
    assert normalise(NAME_AR) in content
    assert NUMBER in content


@pytest.fixture
async def scan_content(harness: Harness) -> str:
    """The text Paperless's OCR made of scan-02-ar.png, once consumed."""
    return await content_of(harness, await consumed(harness, "scan-02-ar.png", FileKind.png))


async def test_scanned_arabic_ocr(scan_content: str) -> None:
    similarity = best_line_similarity(scan_content, NAME_AR)
    print(f"OCR-SUPPLIER-SIMILARITY {similarity:.3f}")
    assert ICE in scan_content
    # The digits of the number; the full number is the strict xfail below.
    assert NUMBER.removeprefix("F-") in scan_content
    assert similarity >= MIN_NAME_SIMILARITY


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "Observed with Tesseract ara+fra+eng in Paperless 3.4.1: the bold Latin number in the "
        "RTL line 'فاتورة رقم F-2026-01002' comes out as '2026-01002-]' (the 'F-' prefix "
        "misread and moved). Strict: this starts failing the day OCR gets it right."
    ),
)
async def test_scanned_arabic_ocr_reads_the_full_number(scan_content: str) -> None:
    assert NUMBER in scan_content


async def test_archive_is_pdfa(harness: Harness) -> None:
    row = await consumed(harness, "scan-02-ar.png", FileKind.png)
    assert row.paperless_doc_id is not None
    async with PaperlessClient(T1_URL, token=harness.token) as client:
        archive = await client.download(row.paperless_doc_id)
    assert archive.startswith(b"%PDF")
    metadata = PdfReader(io.BytesIO(archive)).trailer["/Root"]["/Metadata"].get_object()
    assert b"pdfaid:part" in metadata.get_data()
