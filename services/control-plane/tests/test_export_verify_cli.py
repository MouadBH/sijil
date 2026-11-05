"""`sijil-cp export verify <zip> [--public-key B64] [--against-ledger]`: exit 0 when the export
is intact, 1 with one problem code per line otherwise, 2 when it could not verify at all."""

import asyncio
import base64
import hashlib
import uuid
import zipfile
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.models import (
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    Workspace,
)
from sijil_core.export_report import (
    INDEX_NAME,
    REPORT_NAME,
    SIGNATURE_NAME,
    AuditHead,
    ReportDocument,
    build_report,
    public_key,
    sign,
)
from sijil_cp import cli
from sijil_cp.cli import export_verify
from tests.support.export_keys import TEST_PUBLIC_KEY, TEST_PUBLIC_KEY_B64, TEST_SEED

ORIGINAL = b"%PDF-1.7 SAMPLE invoice"
CSV = b"\xef\xbb\xbfdocument_id\r\n"
OTHER_PUBLIC_B64 = base64.b64encode(public_key(bytes(32))).decode()


def _export(
    path: Path, workspace_id: uuid.UUID, docs: Sequence[tuple[uuid.UUID, str, str | None]]
) -> Path:
    """A signed export whose report lists `docs` as (id, sha256, fields_sha256); every original
    holds ORIGINAL, so the report's hashes must say ORIGINAL's to pass the file checks."""
    report = build_report(
        workspace_id=workspace_id,
        region="ma-cas",
        period="2026-10",
        generated_at=datetime(2026, 11, 5, tzinfo=UTC),
        documents=[
            ReportDocument(
                id=d,
                path=f"originals/{d}.pdf",
                sha256=sha,
                size_bytes=len(ORIGINAL),
                fields_sha256=fields,
            )
            for d, sha, fields in docs
        ],  # fmt: skip
        index_csv_sha256=hashlib.sha256(CSV).hexdigest(),
        audit_head=AuditHead(id=None, hash=None),
        public_key=TEST_PUBLIC_KEY,
    )
    with zipfile.ZipFile(path, "w") as zf:
        for d, _sha, _fields in docs:
            zf.writestr(f"originals/{d}.pdf", ORIGINAL)
        zf.writestr(INDEX_NAME, CSV)
        zf.writestr(REPORT_NAME, report)
        zf.writestr(SIGNATURE_NAME, base64.b64encode(sign(report, TEST_SEED)))
    return path


SHA = hashlib.sha256(ORIGINAL).hexdigest()


def test_an_intact_export_exits_0(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = _export(tmp_path / "e.zip", uuid.uuid4(), [(uuid.uuid4(), SHA, None)])
    assert export_verify(path, public_key_b64=TEST_PUBLIC_KEY_B64) == 0
    assert capsys.readouterr().out == "ok\n"


def test_the_public_key_comes_from_the_environment_unless_given(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _export(tmp_path / "e.zip", uuid.uuid4(), [])
    monkeypatch.setenv("SIJIL_EXPORT_PUBLIC_KEY", TEST_PUBLIC_KEY_B64)
    with pytest.raises(SystemExit) as raised:
        cli.main(["export", "verify", str(path)])
    assert raised.value.code == 0
    with pytest.raises(SystemExit) as raised:
        cli.main(["export", "verify", str(path), "--public-key", OTHER_PUBLIC_B64])
    assert raised.value.code == 1
    assert capsys.readouterr().out.splitlines() == ["ok", "signature.invalid", "key_id.mismatch"]
    monkeypatch.delenv("SIJIL_EXPORT_PUBLIC_KEY")
    assert export_verify(path) == 2
    assert export_verify(path, public_key_b64="not a key") == 2
    assert "public key" in capsys.readouterr().err


@pytest.mark.db
async def test_against_ledger_flags_each_document_that_differs(
    db: AsyncSession, workspace: Workspace, migrated_db: Any, tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:  # fmt: skip
    def row(sha: str) -> DocumentLedger:
        return DocumentLedger(
            workspace_id=workspace.id, sha256=sha, md5="0" * 32, size_bytes=len(ORIGINAL),
            content_kind="pdf", source=DocumentSource.upload, object_key=f"t/{sha}",
            object_version_id="v1", state=DocumentState.filed,
            ingest_state=IngestState.consumed, retention_until=datetime(2036, 12, 31).date(),
            ingested_at=datetime(2026, 10, 3, tzinfo=UTC),
        )  # fmt: skip

    same = row(SHA)
    other_sha = row(hashlib.sha256(b"another original").hexdigest())
    db.add_all([same, other_sha])
    await db.commit()
    unknown = uuid.uuid4()
    docs = [
        (same.id, SHA, None),
        (other_sha.id, SHA, None),  # the ZIP matches its report, the ledger says otherwise
        (unknown, SHA, None),
    ]
    path = _export(tmp_path / "e.zip", workspace.id, docs)
    assert export_verify(path, public_key_b64=TEST_PUBLIC_KEY_B64) == 0
    capsys.readouterr()
    code = await asyncio.to_thread(  # the CLI runs its own event loop
        export_verify, path, public_key_b64=TEST_PUBLIC_KEY_B64, against_ledger=True,
        database_url=migrated_db.app_url,
    )  # fmt: skip
    assert code == 1
    assert capsys.readouterr().out.splitlines() == [
        f"ledger.mismatch:{other_sha.id}",
        f"ledger.mismatch:{unknown}",
    ]
    # Another workspace's export naming this workspace's rows: they are not its documents.
    path = _export(tmp_path / "f.zip", uuid.uuid4(), [(same.id, SHA, None)])
    assert await asyncio.to_thread(
        export_verify, path, public_key_b64=TEST_PUBLIC_KEY_B64, against_ledger=True,
        database_url=migrated_db.app_url,
    ) == 1  # fmt: skip
    assert capsys.readouterr().out.splitlines() == [f"ledger.mismatch:{same.id}"]


def test_against_ledger_without_a_database_exits_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _export(tmp_path / "e.zip", uuid.uuid4(), [(uuid.uuid4(), SHA, None)])
    code = export_verify(
        path, public_key_b64=TEST_PUBLIC_KEY_B64, against_ledger=True,
        database_url="postgresql+asyncpg://x:secret-pw@127.0.0.1:1/none",
    )  # fmt: skip
    assert code == 2
    err = capsys.readouterr().err
    assert "database error" in err
    assert "secret-pw" not in err
