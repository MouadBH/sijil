"""M3 acceptance: the twelve SAMPLE invoices (6 by the API, 6 by email) are extracted by the
in-region model and routed; the deliberately broken totals go to review; one review document is
corrected and sealed over the API, and the seal worker locks its original in COMPLIANCE at the
final date while Paperless shows it sealed; the accuracy report runs on the workspace.

Slow (two model runs per document, one at a time): `make acceptance`, not `make itest`. Its own
fresh workspace, and it waits only for its own documents, never for the global queue. Prints
the routing (state and confidences, never a field value the manifest does not already hold) and
the accuracy table, for the M3 report.
"""

import asyncio
import hashlib
import json
import os
import time
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

from sijil_core.crypto import Envelope, LocalKeyService, open_envelope
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    CredentialKind,
    DocumentLedger,
    DocumentState,
    TenantCredential,
    Workspace,
)
from sijil_core.paperless import PaperlessClient
from sijil_core.storage import ObjectRef, S3ObjectStore, StorageSettings
from sijil_core.tenancy import tenant_ref
from sijil_worker.mirror import SEALED_FIELD
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import WorkerSettings
from tests.support import seed

AttachRunner = Callable[[str], AbstractContextManager[None]]

pytestmark = [pytest.mark.integration, pytest.mark.slow]

CONSUME_BUDGET_S = 600  # OCR shares the host with the model
EXTRACT_BUDGET_S = 45 * 60  # 18 documents x 2 runs, one at a time (measured: ~15 min)
POLL_S = 15
LOCK_BUDGET_S = 120
MIRROR_BUDGET_S = 60  # the brief: within 60 s (measured: 4 s)
REVIEW_THRESHOLD = 0.90
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
MANIFEST_PATH = FIXTURES / "invoices" / "manifest.json"
MANIFEST: dict[str, dict[str, Any]] = {e["file"]: e for e in json.loads(MANIFEST_PATH.read_text())}
SUPPLIERS = {s["ice"]: s for s in json.loads((FIXTURES / "suppliers.json").read_text())}
SETTLED = {DocumentState.filed, DocumentState.needs_review}
CORRECTED = "sample-05-fr.pdf"


def _log(text: str) -> None:
    print(text, flush=True)  # noqa: T201 - the acceptance log (`make acceptance` runs with -s)


def _min_confidence(fields: dict[str, Any]) -> float:
    """The lowest confidence over the required fields (a missing one counts as 0)."""
    return min((fields["fields"][n]["confidence"] or 0.0) for n in fields["required"])


def _routing_table(rows: list[tuple[str, str, str, float, float | None]]) -> str:
    lines = [f"{'file':<20} {'lang':<4} {'state':<13} {'min_conf':>8} {'ttc_conf':>8}"]
    for name, lang, state, low, ttc in rows:
        ttc_cell = "-" if ttc is None else f"{ttc:.2f}"
        lines.append(f"{name:<20} {lang:<4} {state:<13} {low:>8.2f} {ttc_cell:>8}")
    return "\n".join(lines)


async def _service_token(sessions: Any, keys: LocalKeyService, workspace_id: uuid.UUID) -> str:
    async with sessions() as db:
        cred = await db.scalar(
            select(TenantCredential).where(
                TenantCredential.workspace_id == workspace_id,
                TenantCredential.kind == CredentialKind.service_token,
            )
        )
    assert cred is not None
    return open_envelope(
        Envelope.from_bytes(cred.envelope), keys, f"{workspace_id}:{CredentialKind.service_token}"
    ).decode()


async def _eval_output(workspace_id: uuid.UUID) -> tuple[int, str]:
    """`make eval`'s command (`sijil-eval extraction`), run in this runner: it carries the
    worker's environment and the fixtures (read-only here: eval writes nothing)."""
    proc = await asyncio.create_subprocess_exec(
        "sijil-eval",
        "extraction",
        "--set",
        str(MANIFEST_PATH),
        "--workspace",
        str(workspace_id),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=dict(os.environ),
    )
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode()


async def test_m3_acceptance(
    docker_runtime: DockerRuntime,
    worker_settings: WorkerSettings,
    attach_runner: AttachRunner,
    created_workspaces: list[uuid.UUID],
) -> None:
    run = uuid.uuid4().hex[:8]
    engine = make_engine(worker_settings.database_url)
    sessions = make_sessionmaker(engine)
    keys = LocalKeyService(worker_settings.master_key_bytes())
    try:
        started = time.monotonic()
        seeded = await seed.seed(
            email=f"m3-{run}@sijil.local",
            org_name=f"SAMPLE M3 Acceptance {run}",
            consume_budget_s=CONSUME_BUDGET_S,
            on_workspace=created_workspaces.append,
        )
        workspace_id = seeded.workspace_id
        ref = tenant_ref(workspace_id)
        _log(f"workspace {workspace_id}: seeded in {time.monotonic() - started:.0f} s")
        name_of = {hashlib.sha256(p.read_bytes()).hexdigest(): p.name for p in seeded.fixtures}

        # 1. Wait (only for this workspace's documents) until none is still `received`.
        async def ledger() -> list[DocumentLedger]:
            async with sessions() as db:
                return list(
                    await db.scalars(
                        select(DocumentLedger).where(DocumentLedger.workspace_id == workspace_id)
                    )
                )

        waited = time.monotonic()
        last = -1
        while True:
            rows = await ledger()
            pending = [r for r in rows if r.state is DocumentState.received]
            if len(pending) != last:
                _log(
                    f"extraction: {len(rows) - len(pending)}/{len(rows)} routed after "
                    f"{time.monotonic() - waited:.0f} s"
                )
                last = len(pending)
            if not pending:
                break
            if time.monotonic() - waited > EXTRACT_BUDGET_S:
                pytest.fail(f"{len(pending)} documents still received after {EXTRACT_BUDGET_S} s")
            await asyncio.sleep(POLL_S)
        extract_s = time.monotonic() - waited
        _log(f"all {len(rows)} documents routed in {extract_s:.0f} s")
        assert len(rows) == 12 + 6  # + the six raw .eml of the emailed invoices
        assert all(r.state in SETTLED for r in rows), sorted({r.state for r in rows})

        # 2. The routing per invoice, as the review API gives it.
        by_name = {name_of[r.sha256]: r for r in rows if r.sha256 in name_of}
        assert sorted(by_name) == sorted(MANIFEST)
        fields: dict[str, dict[str, Any]] = {}
        async with seed.signed_in(seeded.email) as http:
            base = f"/v1/workspaces/{workspace_id}"
            for name, row in sorted(by_name.items()):
                r = await http.get(f"{base}/documents/{row.id}/fields")
                assert r.status_code == 200, r.text
                fields[name] = r.json()
            table = _routing_table(
                [
                    (
                        name,
                        MANIFEST[name]["lang"],
                        f["state"],
                        _min_confidence(f),
                        f["fields"]["total_ttc"]["confidence"],
                    )
                    for name, f in sorted(fields.items())
                ]
            )
            emails = [r for r in rows if r.sha256 not in name_of]
            _log("routing:\n" + table)
            _log(
                "emails (.eml): "
                + ", ".join(sorted(f"{r.content_kind}:{r.state.value}" for r in emails))
            )

            # 3. The accuracy report on this workspace, before anything is corrected.
            code, report = await _eval_output(workspace_id)
            _log("sijil-eval extraction:\n" + report)
            assert code == 0, report
            header = next(line for line in report.splitlines() if line.startswith("field"))
            assert header.split()[:4] == ["field", "fr", "ar", "all"], header

            # 4. The broken totals are in review, with a low total_ttc confidence.
            for broken in ("sample-05-fr.pdf", "sample-08-ar.pdf"):
                f = fields[broken]
                ttc = f["fields"]["total_ttc"]["confidence"]
                assert f["state"] == DocumentState.needs_review, (broken, f["state"])
                assert ttc is None or ttc < REVIEW_THRESHOLD, (broken, ttc)

            # 5. sample-04 (Arabic-Indic digits): read right, or in review with a low TTC.
            s04 = fields["sample-04-ar.pdf"]
            s04_ttc = s04["fields"]["total_ttc"]
            if s04_ttc["value"] == MANIFEST["sample-04-ar.pdf"]["ttc"]:
                _log("sample-04: total_ttc read as 658.91 (Arabic-Indic digits)")
            else:
                _log(f"sample-04: total_ttc not read; {s04['state']}, conf {s04_ttc['confidence']}")
                assert s04["state"] == DocumentState.needs_review
                assert (s04_ttc["confidence"] or 0.0) < REVIEW_THRESHOLD

            # 6. Correct sample-05 to what is printed, complete it from the manifest, and seal.
            entry = MANIFEST[CORRECTED]
            target = by_name[CORRECTED]
            patch: dict[str, str | None] = {
                "total_ttc": entry["printed_ttc"],  # the value printed on the invoice
                "total_ht": entry["ht"],
                "vat": entry["tva"],
            }
            known = {
                "document_type": "invoice",
                "supplier_name": SUPPLIERS[entry["supplier_ice"]]["name"],
                "supplier_ice": entry["supplier_ice"],
                "date": entry["date"],
                "number": entry["number"],
                "currency": entry["currency"],
            }
            current = fields[CORRECTED]
            if current["document_type"] != "invoice":
                patch["document_type"] = "invoice"
            for name, value in known.items():
                if current["fields"][name]["value"] is None:
                    patch[name] = value
            _log(f"{CORRECTED}: PATCH {sorted(patch)}")
            r = await http.patch(
                f"{base}/documents/{target.id}/fields",
                json={"fields": patch},
                headers=seed._csrf(http),
            )
            assert r.status_code == 200, r.text
            assert r.json()["fields"]["total_ttc"]["value"] == entry["printed_ttc"]
            r = await http.post(f"{base}/documents/{target.id}/seal", headers=seed._csrf(http))
            assert r.status_code == 200, r.text
            assert r.json()["state"] == DocumentState.sealed
            sealed_at = time.monotonic()

            # 7. Sealed: no edit any more.
            r = await http.patch(
                f"{base}/documents/{target.id}/fields",
                json={"fields": {"total_ttc": entry["ttc"]}},
                headers=seed._csrf(http),
            )
            assert r.status_code == 409, r.text
            assert r.json()["error"]["code"] == "retention.locked"

        # 8. The seal worker locks the original in COMPLIANCE at the final date.
        while True:
            async with sessions() as db:
                locked = await db.get(DocumentLedger, target.id, populate_existing=True)
            assert locked is not None
            if locked.object_locked_at is not None:
                break
            if time.monotonic() - sealed_at > LOCK_BUDGET_S:
                pytest.fail(f"object_locked_at not set within {LOCK_BUDGET_S} s")
            await asyncio.sleep(2)
        _log(f"object locked {time.monotonic() - sealed_at:.0f} s after the seal")
        store = S3ObjectStore(StorageSettings())  # type: ignore[call-arg]
        retention = await store.retention(ObjectRef(locked.object_key, locked.object_version_id))
        assert retention == ("COMPLIANCE", locked.retention_until), retention

        # 9. Paperless shows it sealed.
        async with sessions() as db:
            workspace = await db.get(Workspace, workspace_id)
        assert workspace is not None and workspace.paperless_url is not None
        token = await _service_token(sessions, keys, workspace_id)
        assert locked.paperless_doc_id is not None
        with attach_runner(docker_runtime.network_name(ref)):
            async with PaperlessClient(workspace.paperless_url, token=token) as client:
                while True:
                    field_id = await client._custom_field_id(SEALED_FIELD)
                    doc = await client.get_document(locked.paperless_doc_id)
                    values = {c["field"]: c["value"] for c in doc.get("custom_fields", [])}
                    if field_id is not None and values.get(field_id) is True:
                        break
                    if time.monotonic() - sealed_at > MIRROR_BUDGET_S:
                        pytest.fail(f"{SEALED_FIELD} not true within {MIRROR_BUDGET_S} s")
                    await asyncio.sleep(2)
        _log(f"{SEALED_FIELD} mirrored {time.monotonic() - sealed_at:.0f} s after the seal")
    finally:
        # Each step on its own; the tenant always goes (the workspace is marked closing by the
        # `created_workspaces` fixture). Originals stay: they are under retention.
        try:
            await engine.dispose()
        finally:
            for created_id in created_workspaces:
                await docker_runtime.destroy(tenant_ref(created_id))
