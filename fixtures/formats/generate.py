"""Write one tiny SAMPLE file per upload format, plus a fake executable, for sniffing tests.

Run: uv run python fixtures/formats/generate.py
The output is deterministic (fixed zip timestamps, no PDF ids) so regenerating is a no-op.
"""

import io
import zipfile
from pathlib import Path

from PIL import Image

OUT = Path(__file__).resolve().parent
# A fixed timestamp keeps the zip containers byte-identical across runs.
ZIP_TIME = (2026, 1, 1, 0, 0, 0)
SAMPLE = "SAMPLE / ÉCHANTILLON / عينة"


def _pdf() -> bytes:
    stream = b"BT /F1 24 Tf 72 720 Td (SAMPLE) Tj ET"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
        b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(stream), stream),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = io.BytesIO()
    out.write(b"%PDF-1.4\n% SAMPLE\n")
    offsets = []
    for number, body in enumerate(objects, start=1):
        offsets.append(out.tell())
        out.write(b"%d 0 obj\n%s\nendobj\n" % (number, body))
    xref = out.tell()
    out.write(b"xref\n0 %d\n0000000000 65535 f \n" % (len(objects) + 1))
    for offset in offsets:
        out.write(b"%010d 00000 n \n" % offset)
    out.write(
        b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objects) + 1, xref)
    )
    return out.getvalue()


def _image(fmt: str) -> bytes:
    out = io.BytesIO()
    Image.new("RGB", (16, 16), (200, 30, 30)).save(out, format=fmt)
    return out.getvalue()


def _heic() -> bytes:
    # Only the ISO-BMFF `ftyp` box matters to the sniffer; the rest is a `free` box marker.
    ftyp_body = b"heic" + b"\0\0\0\0" + b"mif1heic"
    ftyp = (8 + len(ftyp_body)).to_bytes(4, "big") + b"ftyp" + ftyp_body
    free_body = b"SAMPLE: not a decodable image"
    free = (8 + len(free_body)).to_bytes(4, "big") + b"free" + free_body
    return ftyp + free


def _zip(parts: dict[str, str]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in parts.items():
            info = zipfile.ZipInfo(name, date_time=ZIP_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, body)
    return out.getvalue()


_XML_DECL = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
_RELS_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_OFFICE_DOC = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"
_CT_NS = "http://schemas.openxmlformats.org/package/2006/content-types"
_RELS_CT = "application/vnd.openxmlformats-package.relationships+xml"


def _docx() -> bytes:
    main = "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"
    w = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    return _zip(
        {
            "[Content_Types].xml": f'{_XML_DECL}<Types xmlns="{_CT_NS}">'
            f'<Default Extension="rels" ContentType="{_RELS_CT}"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            f'<Override PartName="/word/document.xml" ContentType="{main}"/></Types>',
            "_rels/.rels": f'{_XML_DECL}<Relationships xmlns="{_RELS_NS}">'
            f'<Relationship Id="rId1" Type="{_OFFICE_DOC}" Target="word/document.xml"/>'
            "</Relationships>",
            "word/document.xml": f'{_XML_DECL}<w:document xmlns:w="{w}"><w:body>'
            f"<w:p><w:r><w:t>{SAMPLE}</w:t></w:r></w:p></w:body></w:document>",
        }
    )


def _xlsx() -> bytes:
    sheet_ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    r_ns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    workbook_ct = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"
    sheet_ct = "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"
    sheet_rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
    return _zip(
        {
            "[Content_Types].xml": f'{_XML_DECL}<Types xmlns="{_CT_NS}">'
            f'<Default Extension="rels" ContentType="{_RELS_CT}"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            f'<Override PartName="/xl/workbook.xml" ContentType="{workbook_ct}"/>'
            f'<Override PartName="/xl/worksheets/sheet1.xml" ContentType="{sheet_ct}"/>'
            "</Types>",
            "_rels/.rels": f'{_XML_DECL}<Relationships xmlns="{_RELS_NS}">'
            f'<Relationship Id="rId1" Type="{_OFFICE_DOC}" Target="xl/workbook.xml"/>'
            "</Relationships>",
            "xl/workbook.xml": f'{_XML_DECL}<workbook xmlns="{sheet_ns}" xmlns:r="{r_ns}">'
            '<sheets><sheet name="SAMPLE" sheetId="1" r:id="rId1"/></sheets></workbook>',
            "xl/_rels/workbook.xml.rels": f'{_XML_DECL}<Relationships xmlns="{_RELS_NS}">'
            f'<Relationship Id="rId1" Type="{sheet_rel}" Target="worksheets/sheet1.xml"/>'
            "</Relationships>",
            "xl/worksheets/sheet1.xml": f'{_XML_DECL}<worksheet xmlns="{sheet_ns}"><sheetData>'
            f'<row r="1"><c r="A1" t="inlineStr"><is><t>{SAMPLE}</t></is></c></row>'
            "</sheetData></worksheet>",
        }
    )


def _xml() -> bytes:
    return (
        f"{_XML_DECL}<!-- {SAMPLE}: fictitious e-invoice -->\n"
        '<Invoice xmlns="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2">\n'
        "  <ID>SAMPLE-0001</ID>\n"
        "  <IssueDate>2026-01-15</IssueDate>\n"
        "  <Note>SAMPLE</Note>\n"
        "</Invoice>\n"
    ).encode()


def _eml() -> bytes:
    return (
        b"From: Fournisseur Exemple <factures@example.com>\r\n"
        b"To: atlas@in.sijil.local\r\n"
        b"Subject: SAMPLE facture\r\n"
        b"Date: Thu, 15 Jan 2026 10:00:00 +0000\r\n"
        b"Message-ID: <sample-0001@example.com>\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"SAMPLE: fictitious message.\r\n"
    )


def _exe() -> bytes:
    # A DOS/PE stub header only: not runnable, just the magic a sniffer would see.
    return b"MZ" + b"\x90\x00" * 29 + b"\x80\x00\x00\x00" + b"SAMPLE: not a program\r\n"


FILES = {
    "sample.pdf": _pdf,
    "sample.jpg": lambda: _image("JPEG"),
    "sample.png": lambda: _image("PNG"),
    "sample.tiff": lambda: _image("TIFF"),
    "sample.heic": _heic,
    "sample.docx": _docx,
    "sample.xlsx": _xlsx,
    "sample.xml": _xml,
    "sample.eml": _eml,
    "notallowed.exe": _exe,
}


def main() -> None:
    for name, build in FILES.items():
        (OUT / name).write_bytes(build())


if __name__ == "__main__":
    main()
