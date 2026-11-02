"""Ingest of every allow-listed format into the real compose Paperless (`paperless-t1`).

Runs in the compose `tests` runner (`make itest`): RustFS for the originals, the control-plane
database for the ledger, and the t1 Paperless over its tenant network. A throw-away workspace
row points at t1 and carries a sealed token of a dedicated service user, so the worker code runs
exactly as in production. Documents that earlier runs left in t1 are trashed first, so every run
goes through Paperless's real consume path.

Observed with Paperless 3.4.1 (recorded in the M2 report):
- PDF, JPEG, PNG, TIFF, DOCX, XLSX and EML are consumed;
- XML is refused at upload (HTTP 400 `File type text/xml not supported`) -> unsupported;
- the sample HEIC is a bare ftyp box, not a decodable image: the consume task fails with
  `cannot identify image file` -> unsupported;
- Paperless does not refuse duplicates (it flags them), so ingest looks the SHA-256 up first.
"""

from pathlib import Path

import pytest

from sijil_core.db.models import IngestState
from sijil_core.filetypes import FileKind
from tests.support.t1 import Harness

pytestmark = pytest.mark.integration

FORMATS = Path(__file__).parents[2] / "fixtures" / "formats"

SAMPLES: list[tuple[str, FileKind]] = [
    ("sample.pdf", FileKind.pdf),
    ("sample.jpg", FileKind.jpeg),
    ("sample.png", FileKind.png),
    ("sample.tiff", FileKind.tiff),
    ("sample.heic", FileKind.heic),
    ("sample.docx", FileKind.docx),
    ("sample.xlsx", FileKind.xlsx),
    ("sample.xml", FileKind.xml),
    ("sample.eml", FileKind.eml),
]
MUST_CONSUME = {FileKind.pdf, FileKind.jpeg, FileKind.png, FileKind.tiff}


@pytest.mark.parametrize(("name", "kind"), SAMPLES, ids=[name for name, _ in SAMPLES])
async def test_each_cap2_format_ingests(harness: Harness, name: str, kind: FileKind) -> None:
    data = (FORMATS / name).read_bytes()
    await harness.forget_in_paperless(data)
    row = await harness.ingest(await harness.ledger(await harness.workspace(), data, kind))
    outcome = row.ingest_state
    print(f"INGEST-OUTCOME {name} {outcome.value} {row.ingest_error_code}")
    assert outcome in {IngestState.consumed, IngestState.unsupported}
    if kind in MUST_CONSUME:
        assert outcome is IngestState.consumed
    if outcome is IngestState.consumed:
        assert row.paperless_doc_id is not None
        assert row.ingest_error_code is None
    else:
        assert row.paperless_doc_id is None
        assert row.ingest_error_code == "ingest.unsupported_format"


async def test_eml_ingests(harness: Harness) -> None:
    data = (FORMATS / "sample.eml").read_bytes()
    await harness.forget_in_paperless(data)
    row = await harness.ingest(await harness.ledger(await harness.workspace(), data, FileKind.eml))
    assert row.ingest_state is IngestState.consumed
    assert row.paperless_doc_id is not None


async def test_same_bytes_in_a_second_workspace_adopt_the_existing_document(
    harness: Harness,
) -> None:
    """Paperless 3.4.1 accepts duplicates instead of refusing them: ingest must not add one."""
    data = (FORMATS / "sample.pdf").read_bytes()
    await harness.forget_in_paperless(data)
    first = await harness.ingest(
        await harness.ledger(await harness.workspace(), data, FileKind.pdf)
    )
    second = await harness.ingest(
        await harness.ledger(await harness.workspace(), data, FileKind.pdf)
    )
    assert first.ingest_state is second.ingest_state is IngestState.consumed
    assert first.paperless_doc_id == second.paperless_doc_id
    assert second.paperless_task_id is None  # resolved by checksum, nothing was uploaded
