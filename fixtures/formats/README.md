# SAMPLE DATA — upload format fixtures

Tiny **fictitious sample files**, one per format on the upload allow-list, for the content
sniffer (`sijil_core.filetypes`) and the upload tests. Each carries a `SAMPLE` marker. Never put
real documents here.

| File | Format |
| --- | --- |
| `sample.pdf` | PDF (one page, "SAMPLE") |
| `sample.jpg`, `sample.png`, `sample.tiff` | 16×16 images |
| `sample.heic` | an ISO-BMFF `ftyp` box with brand `heic` only: **not a decodable image** |
| `sample.docx`, `sample.xlsx` | minimal OOXML packages |
| `sample.xml` | a fictitious UBL-style e-invoice |
| `sample.eml` | a fictitious RFC 5322 message (accepted on the email path only) |
| `notallowed.exe` | a DOS stub header, not runnable: must be refused |

Regenerate (deterministic): `uv run python fixtures/formats/generate.py`.
