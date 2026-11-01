"""The upload allow-list, decided by content sniffing only (never the name or Content-Type).

Rules (M2 plan, Global Constraints): magic numbers for PDF/JPEG/PNG/TIFF, the ISO-BMFF major
brand for HEIC, the part names of the OOXML zip for DOCX/XLSX, and a real parse for XML. The
email path additionally accepts a raw RFC 5322 message.
"""

import re
import zipfile
from enum import StrEnum
from pathlib import Path
from xml.parsers import expat

MAX_UPLOAD_BYTES = 104_857_600  # 100 MiB

_HEAD_BYTES = 64 * 1024
_XML_CHUNK = 1024 * 1024
_HEIC_BRANDS = {b"heic", b"heix", b"mif1", b"msf1"}
_UTF8_BOM = b"\xef\xbb\xbf"
# RFC 5322 field name: printable ASCII except the colon.
_HEADER_FIELD = re.compile(rb"^[\x21-\x39\x3b-\x7e]+:")
_MAIL_FIELDS = {b"date", b"message-id", b"received", b"mime-version", b"to", b"subject"}


class FileKind(StrEnum):
    pdf = "pdf"
    jpeg = "jpeg"
    png = "png"
    tiff = "tiff"
    heic = "heic"
    docx = "docx"
    xlsx = "xlsx"
    xml = "xml"
    eml = "eml"


def _magic(head: bytes) -> FileKind | None:
    if head.startswith(b"%PDF-"):
        return FileKind.pdf
    if head.startswith(b"\xff\xd8\xff"):
        return FileKind.jpeg
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return FileKind.png
    if head.startswith((b"II\x2a\x00", b"MM\x00\x2a")):
        return FileKind.tiff
    if head[4:8] == b"ftyp" and head[8:12] in _HEIC_BRANDS:
        return FileKind.heic
    return None


def _ooxml(path: Path) -> FileKind | None:
    # Reads only the central directory: nothing is decompressed, so a zip bomb costs nothing.
    try:
        with zipfile.ZipFile(path) as archive:
            names = set(archive.namelist())
    except (zipfile.BadZipFile, OSError, ValueError, EOFError):
        return None
    if "[Content_Types].xml" not in names:
        return None
    if "word/document.xml" in names:
        return FileKind.docx
    if "xl/workbook.xml" in names:
        return FileKind.xlsx
    return None


class _EntityDeclared(Exception):
    pass


def _refuse_entity(*_: object) -> None:
    # Entity declarations are what billion-laughs and external-entity attacks need.
    raise _EntityDeclared


def _parses_as_xml(path: Path) -> bool:
    parser = expat.ParserCreate()
    parser.SetParamEntityParsing(expat.XML_PARAM_ENTITY_PARSING_NEVER)
    parser.EntityDeclHandler = _refuse_entity
    parser.UnparsedEntityDeclHandler = _refuse_entity
    try:
        with path.open("rb") as source:
            while block := source.read(_XML_CHUNK):
                parser.Parse(block, False)
            parser.Parse(b"", True)
    except (expat.ExpatError, _EntityDeclared, ValueError):
        return False
    return True


def _xml(path: Path, head: bytes) -> FileKind | None:
    text = head.removeprefix(_UTF8_BOM).lstrip(b" \t\r\n")
    if not (text.startswith(b"<?xml") or (text[:1] == b"<" and text[1:2].isalpha())):
        return None
    return FileKind.xml if _parses_as_xml(path) else None


def _eml(head: bytes) -> FileKind | None:
    """A header block of RFC 5322 fields ending in a blank line, with a From and a mail field."""
    block, blank, _ = head.replace(b"\r\n", b"\n").partition(b"\n\n")
    if not blank:
        return None
    names: set[bytes] = set()
    for line in block.split(b"\n"):
        if line[:1] in (b" ", b"\t") and names:
            continue  # folded continuation of the previous field
        if not _HEADER_FIELD.match(line):
            return None
        names.add(line.split(b":", 1)[0].lower())
    return FileKind.eml if b"from" in names and names & _MAIL_FIELDS else None


def sniff(path: Path, *, allow_eml: bool = False) -> FileKind | None:
    """The allow-listed kind of the file's content, or None if it is not allowed."""
    with path.open("rb") as source:
        head = source.read(_HEAD_BYTES)
    if not head:
        return None
    kind = _magic(head)
    if kind is not None:
        return kind
    if head.startswith(b"PK\x03\x04"):
        return _ooxml(path)
    kind = _xml(path, head)
    if kind is not None:
        return kind
    if allow_eml:
        return _eml(head)
    return None


__all__ = ["MAX_UPLOAD_BYTES", "FileKind", "sniff"]
