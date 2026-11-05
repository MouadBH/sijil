"""The signed integrity report of a period export, and the verification of an export ZIP
(design §5.6, ARC-5). Shared by the export worker, which signs, and `sijil-cp export verify`.

An export ZIP holds `originals/<ledger_id>.<ext>`, `index.csv`, `integrity-report.json` and
`integrity-report.json.sig`. The report is canonical JSON (`sijil_core.audit.canonical_json`):

    {"v": 1, "workspace_id", "region", "period", "generated_at",
     "documents": [{"id", "path", "sha256", "size_bytes", "fields_sha256"}],
     "index_csv_sha256", "audit_head": {"id", "hash"}, "key_id"}

`key_id` is the first 16 hex characters of the SHA-256 of the raw 32-byte Ed25519 public key.
The `.sig` file is the base64 of the raw 64-byte Ed25519 signature over the report file's exact
bytes, made with the region's platform key (only the export worker holds its seed).

`verify_zip` answers with problem codes, never prose: `signature.invalid`, `key_id.mismatch`,
`file.missing:<path>`, `file.extra:<path>`, `file.hash:<path>`, `index_csv.hash`, and
`zip.invalid` / `report.invalid` for a file that is not an export at all.
"""

import base64
import binascii
import calendar
import hashlib
import json
import re
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final
from uuid import UUID

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from sijil_core.audit import canonical_json

REPORT_VERSION: Final = 1
REPORT_NAME: Final = "integrity-report.json"
SIGNATURE_NAME: Final = "integrity-report.json.sig"
INDEX_NAME: Final = "index.csv"
KEY_BYTES: Final = 32
_CHUNK: Final = 1024 * 1024
# The same rule as the `export_job.period` CHECK (0012).
_PERIOD: Final = re.compile(r"(?P<year>[0-9]{4})(?:-(?P<month>0[1-9]|1[0-2]))?")


@dataclass(frozen=True)
class ReportDocument:
    id: UUID
    path: str
    sha256: str
    size_bytes: int
    fields_sha256: str | None


@dataclass(frozen=True)
class AuditHead:
    """The workspace's last audit event at the snapshot."""

    id: UUID | None
    hash: str | None


@dataclass(frozen=True)
class VerifyOutcome:
    ok: bool
    problems: list[str] = field(default_factory=list)


def period_bounds(period: str) -> tuple[date, date]:
    """The first and last day of a period, `YYYY` or `YYYY-MM`. ValueError otherwise."""
    match = _PERIOD.fullmatch(period)
    if match is None:
        raise ValueError("period must be YYYY or YYYY-MM")
    year = int(match["year"])
    if match["month"] is None:
        return date(year, 1, 1), date(year, 12, 31)
    month = int(match["month"])
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def decode_key(b64: str) -> bytes:
    """A raw 32-byte key (seed or public key) from its base64. ValueError otherwise."""
    try:
        raw = base64.b64decode(b64.strip(), validate=True)
    except binascii.Error as exc:
        raise ValueError("key is not base64") from exc
    if len(raw) != KEY_BYTES:
        raise ValueError("key must be 32 bytes")
    return raw


def public_key(seed: bytes) -> bytes:
    """The raw 32-byte Ed25519 public key of a raw 32-byte seed."""
    return Ed25519PrivateKey.from_private_bytes(seed).public_key().public_bytes_raw()


def key_id(public: bytes) -> str:
    return hashlib.sha256(public).hexdigest()[:16]


def build_report(
    *,
    workspace_id: UUID,
    region: str,
    period: str,
    generated_at: datetime,
    documents: Sequence[ReportDocument],
    index_csv_sha256: str,
    audit_head: AuditHead,
    public_key: bytes,
) -> bytes:
    """The report file's exact bytes (canonical JSON)."""
    return canonical_json(
        {
            "v": REPORT_VERSION,
            "workspace_id": workspace_id,
            "region": region,
            "period": period,
            "generated_at": generated_at,
            "documents": [
                {
                    "id": d.id,
                    "path": d.path,
                    "sha256": d.sha256,
                    "size_bytes": d.size_bytes,
                    "fields_sha256": d.fields_sha256,
                }
                for d in documents
            ],
            "index_csv_sha256": index_csv_sha256,
            "audit_head": {"id": audit_head.id, "hash": audit_head.hash},
            "key_id": key_id(public_key),
        }
    )


def sign(report: bytes, seed: bytes) -> bytes:
    """The raw 64-byte Ed25519 signature over `report`."""
    return Ed25519PrivateKey.from_private_bytes(seed).sign(report)


def _signature_ok(report: bytes, sig_file: bytes, public: bytes) -> bool:
    try:
        signature = base64.b64decode(sig_file.strip(), validate=True)
        Ed25519PublicKey.from_public_bytes(public).verify(signature, report)
    except (binascii.Error, ValueError, InvalidSignature):
        return False
    return True


def _entry_sha256(zf: zipfile.ZipFile, name: str) -> str:
    digest = hashlib.sha256()
    with zf.open(name) as entry:
        while block := entry.read(_CHUNK):
            digest.update(block)
    return digest.hexdigest()


def _parse(report: bytes) -> dict[str, Any] | None:
    try:
        parsed = json.loads(report)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict) or not isinstance(parsed.get("documents"), list):
        return None
    return parsed


def read_report(path: Path) -> dict[str, Any] | None:
    """The parsed report of an export ZIP, or None when it has none (or is not a ZIP). It is
    not verified: call `verify_zip` first."""
    try:
        with zipfile.ZipFile(path) as zf:
            return _parse(zf.read(REPORT_NAME))
    except (zipfile.BadZipFile, KeyError, OSError):
        return None


def verify_zip(path: Path, public_key: bytes) -> VerifyOutcome:
    """Check the signature, the signer's key id and every file's hash against the report."""
    try:
        with zipfile.ZipFile(path) as zf:
            problems = _verify(zf, public_key)
    except (zipfile.BadZipFile, OSError):
        problems = ["zip.invalid"]
    return VerifyOutcome(ok=not problems, problems=problems)


def _verify(zf: zipfile.ZipFile, public: bytes) -> list[str]:
    names = set(zf.namelist())
    missing = [f"file.missing:{n}" for n in (REPORT_NAME, SIGNATURE_NAME) if n not in names]
    if missing:
        return missing
    report = zf.read(REPORT_NAME)
    problems: list[str] = []
    if not _signature_ok(report, zf.read(SIGNATURE_NAME), public):
        problems.append("signature.invalid")
    parsed = _parse(report)
    if parsed is None:
        return [*problems, "report.invalid"]
    if parsed.get("key_id") != key_id(public):
        problems.append("key_id.mismatch")
    expected = {REPORT_NAME, SIGNATURE_NAME, INDEX_NAME}
    for doc in parsed["documents"]:
        doc_path = str(doc.get("path"))
        expected.add(doc_path)
        if doc_path not in names:
            problems.append(f"file.missing:{doc_path}")
        elif _entry_sha256(zf, doc_path) != doc.get("sha256"):
            problems.append(f"file.hash:{doc_path}")
    if INDEX_NAME not in names:
        problems.append(f"file.missing:{INDEX_NAME}")
    elif _entry_sha256(zf, INDEX_NAME) != parsed.get("index_csv_sha256"):
        problems.append("index_csv.hash")
    problems.extend(f"file.extra:{n}" for n in sorted(names - expected))
    # A second entry of the same name: readers disagree on which one they show.
    seen: set[str] = set()
    for name in zf.namelist():
        if name in seen:
            problems.append(f"file.extra:{name}")
        seen.add(name)
    return problems


__all__ = [
    "INDEX_NAME",
    "REPORT_NAME",
    "SIGNATURE_NAME",
    "AuditHead",
    "ReportDocument",
    "VerifyOutcome",
    "build_report",
    "decode_key",
    "key_id",
    "period_bounds",
    "public_key",
    "read_report",
    "sign",
    "verify_zip",
]
