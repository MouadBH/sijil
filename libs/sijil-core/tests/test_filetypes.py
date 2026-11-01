"""Upload types are decided by content alone, never by the name or a client Content-Type."""

import io
import shutil
import struct
import time
import tracemalloc
import zipfile
from collections.abc import Callable
from pathlib import Path

import pytest

from sijil_core.filetypes import FileKind, _zip_directory_bounded, sniff

FORMATS = Path(__file__).resolve().parents[3] / "fixtures" / "formats"
EXPECTED = {
    "sample.pdf": FileKind.pdf,
    "sample.jpg": FileKind.jpeg,
    "sample.png": FileKind.png,
    "sample.tiff": FileKind.tiff,
    "sample.heic": FileKind.heic,
    "sample.docx": FileKind.docx,
    "sample.xlsx": FileKind.xlsx,
    "sample.xml": FileKind.xml,
    "sample.eml": FileKind.eml,
}


def test_fixtures_cover_every_kind() -> None:
    assert set(EXPECTED.values()) == set(FileKind)
    assert {p.name for p in FORMATS.glob("sample.*")} == set(EXPECTED)


@pytest.mark.parametrize("path", sorted(FORMATS.glob("sample.*")), ids=lambda p: p.name)
def test_sniff_each_allowed_format(path: Path, tmp_path: Path) -> None:
    expected = EXPECTED[path.name]
    # The name carries no information: sniff a copy with a meaningless one.
    blind = tmp_path / "upload.bin"
    shutil.copyfile(path, blind)
    assert sniff(blind, allow_eml=True) == expected
    if expected is FileKind.eml:
        assert sniff(blind) is None  # the web upload path refuses raw email
    else:
        assert sniff(blind) == expected


def test_sniff_rejects_exe_and_renamed_exe_as_pdf(tmp_path: Path) -> None:
    exe = FORMATS / "notallowed.exe"
    assert sniff(exe) is None
    renamed = tmp_path / "invoice.pdf"
    shutil.copyfile(exe, renamed)
    assert sniff(renamed) is None
    assert sniff(renamed, allow_eml=True) is None


def _write(tmp_path: Path, data: bytes) -> Path:
    path = tmp_path / "f"
    path.write_bytes(data)
    return path


def _zip(names: list[str]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        for name in names:
            archive.writestr(name, "<x/>")
    return out.getvalue()


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (b"MM\x00\x2a" + b"\x00" * 8, FileKind.tiff),
        (b"\x00\x00\x00\x18ftypheix\x00\x00\x00\x00mif1heic", FileKind.heic),
        (b"\x00\x00\x00\x18ftypmif1\x00\x00\x00\x00mif1heic", FileKind.heic),
        (b"\x00\x00\x00\x18ftypmsf1\x00\x00\x00\x00msf1heic", FileKind.heic),
        (b"\x00\x00\x00\x18ftypavif\x00\x00\x00\x00mif1avif", None),
        (b"\x00\x00\x00\x18ftypisom\x00\x00\x00\x00isommp41", None),
        (b"\xef\xbb\xbf  \n<root><a/></root>", FileKind.xml),
        (b"<root/>", FileKind.xml),
        (b"<?xml version='1.0'?><root>", None),  # does not parse
        (b"<1/>", None),
        (b"hello <root/>", None),
        (b'<?xml version="1.0"?><!DOCTYPE r [<!ENTITY a "aaaa">]><r>&a;</r>', None),
        (b"%PDF", None),  # too short for the `%PDF-` signature
        (b"\x89PNG\r\n\x1a", None),
        (b"", None),
    ],
)
def test_sniff_edge_cases(tmp_path: Path, data: bytes, expected: FileKind | None) -> None:
    assert sniff(_write(tmp_path, data)) == expected


@pytest.mark.parametrize(
    ("names", "expected"),
    [
        (["[Content_Types].xml", "word/document.xml"], FileKind.docx),
        (["[Content_Types].xml", "xl/workbook.xml"], FileKind.xlsx),
        (["word/document.xml"], None),
        (["[Content_Types].xml", "ppt/presentation.xml"], None),
        (["readme.txt"], None),
    ],
)
def test_sniff_zip_containers(tmp_path: Path, names: list[str], expected: FileKind | None) -> None:
    assert sniff(_write(tmp_path, _zip(names))) == expected


def test_sniff_truncated_zip_is_refused(tmp_path: Path) -> None:
    data = _zip(["[Content_Types].xml", "word/document.xml"])
    assert sniff(_write(tmp_path, data[: len(data) // 2])) is None


def test_sniff_eml_needs_a_header_block(tmp_path: Path) -> None:
    not_mail = _write(tmp_path, b"Dear sir,\r\nplease find attached.\r\n")
    assert sniff(not_mail, allow_eml=True) is None


def _timed_peak(fn: Callable[[], object]) -> tuple[object, float, int]:
    tracemalloc.start()
    started = time.monotonic()
    try:
        result = fn()
        elapsed = time.monotonic() - started
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return result, elapsed, peak


def test_sniff_refuses_zip_with_too_many_entries_cheaply(tmp_path: Path) -> None:
    # A crafted central directory must not make the sniffer build one object per entry.
    path = tmp_path / "many.docx"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as archive:
        archive.writestr("[Content_Types].xml", "<x/>")
        archive.writestr("word/document.xml", "<x/>")
        for n in range(20_000):
            archive.writestr(f"e{n}", b"")
    result, elapsed, peak = _timed_peak(lambda: sniff(path))
    assert result is None
    assert elapsed < 0.5
    assert peak < 2 * 1024 * 1024


def _eocd(entries: int, cd_size: int, cd_offset: int, comment: bytes = b"") -> bytes:
    return (
        b"PK\x05\x06"
        + struct.pack("<HHHHIIH", 0, 0, entries, entries, cd_size, cd_offset, len(comment))
        + comment
    )


def test_sniff_refuses_oversized_central_directory(tmp_path: Path) -> None:
    data = b"PK\x03\x04" + b"\0" * 100 + _eocd(1, 17 * 1024 * 1024, 0)
    assert sniff(_write(tmp_path, data)) is None


def test_sniff_refuses_zip64_with_too_many_entries(tmp_path: Path) -> None:
    body = b"PK\x03\x04" + b"\0" * 100
    zip64_eocd_at = len(body)
    zip64_eocd = b"PK\x06\x06" + struct.pack(
        "<QHHIIQQQQ", 44, 45, 45, 0, 0, 1_000_000, 1_000_000, 1024, 0
    )
    locator = b"PK\x06\x07" + struct.pack("<IQI", 0, zip64_eocd_at, 1)
    data = body + zip64_eocd + locator + _eocd(0xFFFF, 0xFFFFFFFF, 0xFFFFFFFF)
    assert sniff(_write(tmp_path, data)) is None


def test_sniff_zip_without_eocd_is_refused(tmp_path: Path) -> None:
    assert sniff(_write(tmp_path, b"PK\x03\x04" + b"\0" * 200)) is None


def test_sniff_docx_with_archive_comment(tmp_path: Path) -> None:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        archive.writestr("[Content_Types].xml", "<x/>")
        archive.writestr("word/document.xml", "<x/>")
        archive.comment = b"SAMPLE " * 100
    assert sniff(_write(tmp_path, out.getvalue())) is FileKind.docx


def _zip64(entries: int) -> bytes:
    body = b"PK\x03\x04" + b"\0" * 100
    record = b"PK\x06\x06" + struct.pack("<QHHIIQQQQ", 44, 45, 45, 0, 0, entries, entries, 1024, 0)
    locator = b"PK\x06\x07" + struct.pack("<IQI", 0, len(body), 1)
    return body + record + locator + _eocd(0xFFFF, 0xFFFFFFFF, 0xFFFFFFFF)


@pytest.mark.parametrize(
    ("data", "bounded"),
    [
        (_zip64(5), True),
        (_zip64(1_000_000), False),
        (b"PK\x03\x04" + b"\0" * 100 + _eocd(1, 17 * 1024 * 1024, 0), False),
        (b"PK\x03\x04" + b"\0" * 100 + _eocd(10_001, 1024, 0), False),
        (b"PK\x03\x04" + b"\0" * 100 + _eocd(0xFFFF, 0xFFFFFFFF, 0xFFFFFFFF), False),
        (b"PK\x03\x04" + b"\0" * 100 + _eocd(10_000, 1024, 0), True),
    ],
)
def test_zip_directory_bound_reads_eocd_and_zip64(
    tmp_path: Path, data: bytes, bounded: bool
) -> None:
    assert _zip_directory_bounded(_write(tmp_path, data)) is bounded
