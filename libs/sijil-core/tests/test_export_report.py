"""The signed integrity report of a period export and the ZIP verification (design §5.6)."""

import base64
import hashlib
import json
import struct
import zipfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from sijil_core.audit import canonical_json
from sijil_core.export_report import (
    INDEX_NAME,
    REPORT_NAME,
    SIGNATURE_NAME,
    AuditHead,
    ReportDocument,
    build_report,
    key_id,
    period_bounds,
    public_key,
    sign,
    verify_zip,
)

SEED = bytes(range(32))
OTHER_SEED = bytes(range(1, 33))
WS = UUID("11111111-1111-4111-8111-111111111111")
DOC = UUID("22222222-2222-4222-8222-222222222222")
EVENT = UUID("33333333-3333-4333-8333-333333333333")
ORIGINAL = b"%PDF-1.7 SAMPLE invoice"
CSV = "﻿document_id,state\r\n22222222-2222-4222-8222-222222222222,sealed\r\n".encode()
GENERATED = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)


def _report(csv: bytes = CSV, pub: bytes | None = None) -> bytes:
    return build_report(
        workspace_id=WS,
        region="ma-cas",
        period="2026-10",
        generated_at=GENERATED,
        documents=[
            ReportDocument(
                id=DOC,
                path=f"originals/{DOC}.pdf",
                sha256=hashlib.sha256(ORIGINAL).hexdigest(),
                size_bytes=len(ORIGINAL),
                fields_sha256="f" * 64,
            )
        ],
        index_csv_sha256=hashlib.sha256(csv).hexdigest(),
        audit_head=AuditHead(id=EVENT, hash="a" * 64),
        public_key=pub or public_key(SEED),
    )


def _zip(path: Path, entries: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return path


def _entries(report: bytes | None = None) -> dict[str, bytes]:
    report = report or _report()
    return {
        f"originals/{DOC}.pdf": ORIGINAL,
        INDEX_NAME: CSV,
        REPORT_NAME: report,
        SIGNATURE_NAME: base64.b64encode(sign(report, SEED)),
    }


def test_report_is_canonical_and_signed() -> None:
    report = _report()
    parsed = json.loads(report)
    assert canonical_json(parsed) == report
    assert parsed == {
        "v": 1,
        "workspace_id": str(WS),
        "region": "ma-cas",
        "period": "2026-10",
        "generated_at": "2026-11-05T12:00:00Z",
        "documents": [
            {
                "id": str(DOC),
                "path": f"originals/{DOC}.pdf",
                "sha256": hashlib.sha256(ORIGINAL).hexdigest(),
                "size_bytes": len(ORIGINAL),
                "fields_sha256": "f" * 64,
            }
        ],
        "index_csv_sha256": hashlib.sha256(CSV).hexdigest(),
        "audit_head": {"id": str(EVENT), "hash": "a" * 64},
        "key_id": hashlib.sha256(public_key(SEED)).hexdigest()[:16],
    }
    assert key_id(public_key(SEED)) == parsed["key_id"]
    signature = sign(report, SEED)
    assert len(signature) == 64
    Ed25519PublicKey.from_public_bytes(public_key(SEED)).verify(signature, report)


def test_an_untouched_zip_verifies(tmp_path: Path) -> None:
    outcome = verify_zip(_zip(tmp_path / "e.zip", _entries()), public_key(SEED))
    assert outcome.ok
    assert outcome.problems == []


def _swap_signature(e: dict[str, bytes]) -> None:
    e[SIGNATURE_NAME] = base64.b64encode(sign(b"another report", SEED))


def _drop_original(e: dict[str, bytes]) -> None:
    del e[f"originals/{DOC}.pdf"]


def _add_file(e: dict[str, bytes]) -> None:
    e["originals/extra.pdf"] = b"%PDF-1.7 smuggled"


def _change_original(e: dict[str, bytes]) -> None:
    e[f"originals/{DOC}.pdf"] = b"%PDF-1.7 SAMPLE invoicf"


def _change_csv(e: dict[str, bytes]) -> None:
    e[INDEX_NAME] = CSV.replace(b"sealed", b"filed")


def _other_key_id(e: dict[str, bytes]) -> None:
    # Signed with the region's key but naming another key: the report lies about its signer.
    report = _report(pub=public_key(OTHER_SEED))
    e[REPORT_NAME] = report
    e[SIGNATURE_NAME] = base64.b64encode(sign(report, SEED))


@pytest.mark.parametrize(
    ("tamper", "problem"),
    [
        (_swap_signature, "signature.invalid"),
        (_drop_original, f"file.missing:originals/{DOC}.pdf"),
        (_add_file, "file.extra:originals/extra.pdf"),
        (_change_original, f"file.hash:originals/{DOC}.pdf"),
        (_change_csv, "index_csv.hash"),
        (_other_key_id, "key_id.mismatch"),
    ],
)
def test_verify_detects_each_problem(
    tmp_path: Path, tamper: Callable[[dict[str, bytes]], None], problem: str
) -> None:
    entries = _entries()
    tamper(entries)
    outcome = verify_zip(_zip(tmp_path / "e.zip", entries), public_key(SEED))
    assert not outcome.ok
    assert outcome.problems == [problem]


def test_verify_with_another_public_key_fails(tmp_path: Path) -> None:
    outcome = verify_zip(_zip(tmp_path / "e.zip", _entries()), public_key(OTHER_SEED))
    assert outcome.problems == ["signature.invalid", "key_id.mismatch"]


def test_verify_names_a_missing_report_and_a_non_zip(tmp_path: Path) -> None:
    entries = _entries()
    del entries[REPORT_NAME]
    outcome = verify_zip(_zip(tmp_path / "e.zip", entries), public_key(SEED))
    assert outcome.problems == [f"file.missing:{REPORT_NAME}"]
    junk = tmp_path / "junk.zip"
    junk.write_bytes(b"not a zip")
    assert verify_zip(junk, public_key(SEED)).problems == ["zip.invalid"]


def test_verify_flags_a_duplicated_entry(tmp_path: Path) -> None:
    path = _zip(tmp_path / "e.zip", _entries())
    with zipfile.ZipFile(path, "a") as zf, pytest.warns(UserWarning, match="Duplicate name"):
        zf.writestr(f"originals/{DOC}.pdf", ORIGINAL)
    assert verify_zip(path, public_key(SEED)).problems == [f"file.extra:originals/{DOC}.pdf"]


@pytest.mark.parametrize(
    ("period", "bounds"),
    [
        ("2026", (date(2026, 1, 1), date(2026, 12, 31))),
        ("2026-02", (date(2026, 2, 1), date(2026, 2, 28))),
        ("2028-02", (date(2028, 2, 1), date(2028, 2, 29))),
        ("2026-12", (date(2026, 12, 1), date(2026, 12, 31))),
    ],
)
def test_period_bounds(period: str, bounds: tuple[date, date]) -> None:
    assert period_bounds(period) == bounds


@pytest.mark.parametrize("period", ["26", "2026-13", "2026-00", "2026-1", "2026/10", " 2026", ""])
def test_period_bounds_refuses_a_malformed_period(period: str) -> None:
    with pytest.raises(ValueError, match="period"):
        period_bounds(period)


# In-place byte flips: the ZIP's own bytes edited, as damage on disk or in transit would be,
# never rebuilt through `writestr` (which would compute a fresh, matching CRC).

BIG_ORIGINAL = b"%PDF-1.7 SAMPLE " + bytes(range(256)) * 8


def _flip_zip(path: Path, compression: int) -> Path:
    """An intact export whose original is BIG_ORIGINAL, stored or deflated."""
    report = build_report(
        workspace_id=WS, region="ma-cas", period="2026-10", generated_at=GENERATED,
        documents=[
            ReportDocument(
                id=DOC, path=f"originals/{DOC}.pdf",
                sha256=hashlib.sha256(BIG_ORIGINAL).hexdigest(), size_bytes=len(BIG_ORIGINAL),
                fields_sha256=None,
            )
        ],
        index_csv_sha256=hashlib.sha256(CSV).hexdigest(),
        audit_head=AuditHead(id=EVENT, hash="a" * 64), public_key=public_key(SEED),
    )  # fmt: skip
    with zipfile.ZipFile(path, "w", compression=compression) as zf:
        zf.writestr(f"originals/{DOC}.pdf", BIG_ORIGINAL)
        zf.writestr(INDEX_NAME, CSV)
        zf.writestr(REPORT_NAME, report)
        zf.writestr(SIGNATURE_NAME, base64.b64encode(sign(report, SEED)))
    return path


def _data_span(path: Path, name: str) -> range:
    """The byte offsets of `name`'s (compressed) data in the archive."""
    with zipfile.ZipFile(path) as zf:
        info = zf.getinfo(name)
    raw = path.read_bytes()
    name_len, extra_len = struct.unpack(
        "<HH", raw[info.header_offset + 26 : info.header_offset + 30]
    )
    start = info.header_offset + 30 + name_len + extra_len
    return range(start, start + info.compress_size)


def _flipped(src: Path, dst: Path, offset: int) -> Path:
    raw = bytearray(src.read_bytes())
    raw[offset] ^= 0xFF
    dst.write_bytes(bytes(raw))
    return dst


COMPRESSIONS = pytest.mark.parametrize(
    "compression", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED], ids=["stored", "deflated"]
)


@COMPRESSIONS
def test_a_byte_flipped_in_place_in_an_original_is_a_hash_problem(
    tmp_path: Path, compression: int
) -> None:
    intact = _flip_zip(tmp_path / "intact.zip", compression)
    span = _data_span(intact, f"originals/{DOC}.pdf")
    # Not the last byte: in a deflate stream it may hold only padding bits, which no reader sees.
    for offset in (span.start, span.start + len(span) // 3, span.start + len(span) // 2):
        outcome = verify_zip(_flipped(intact, tmp_path / "f.zip", offset), public_key(SEED))
        assert outcome.problems == [f"file.hash:originals/{DOC}.pdf"], offset


@COMPRESSIONS
def test_a_byte_flipped_in_place_in_the_index_is_an_index_hash_problem(
    tmp_path: Path, compression: int
) -> None:
    intact = _flip_zip(tmp_path / "intact.zip", compression)
    span = _data_span(intact, INDEX_NAME)
    for offset in (span.start, span.start + len(span) // 2):
        outcome = verify_zip(_flipped(intact, tmp_path / "f.zip", offset), public_key(SEED))
        assert outcome.problems == ["index_csv.hash"], offset


@COMPRESSIONS
def test_a_byte_flipped_in_the_central_directory_never_raises(
    tmp_path: Path, compression: int
) -> None:
    intact = _flip_zip(tmp_path / "intact.zip", compression)
    with zipfile.ZipFile(intact) as zf:
        start_dir = zf.start_dir
    raw = intact.read_bytes()
    # Its first record's signature: the archive itself is unreadable.
    assert verify_zip(
        _flipped(intact, tmp_path / "f.zip", start_dir), public_key(SEED)
    ).problems == ["zip.invalid"]
    # Any byte of the directory (and of the end record): a verdict, never an exception.
    for offset in range(start_dir, len(raw)):
        verify_zip(_flipped(intact, tmp_path / "f.zip", offset), public_key(SEED))


@COMPRESSIONS
def test_every_single_byte_flip_is_reported_never_raised(tmp_path: Path, compression: int) -> None:
    intact = _flip_zip(tmp_path / "intact.zip", compression)
    assert verify_zip(intact, public_key(SEED)).ok
    for offset in range(len(intact.read_bytes())):
        verify_zip(_flipped(intact, tmp_path / "f.zip", offset), public_key(SEED))
