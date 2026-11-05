"""M4 acceptance: retention, removal and export, end to end against the compose stack.

A fresh workspace gets 4 SAMPLE invoices through the API. Their fields are written straight
into the workspace's index, as `tests/support/sealing.py` does (so the model never runs and
the test stays in `make itest`), and routed by the worker's own routing. Two are sealed; the
Owner removes an unsealed one with a step-up, and within 120 s its original, its Paperless
copy and its index rows are gone while the tombstone keeps its hash; every role fails to
remove a sealed one; a correction note is added to a sealed one. The month is exported and
downloaded: the ZIP verifies (signature against the published key, every hash, and the ledger
with `sijil-cp export verify --against-ledger`), its CSV flags the corrected field and leaves
the removed document out, one byte tampered fails verification, and the audit chain verifies.

HTTP like a customer; the database and the index are read to check what the API cannot show.
The Owner signs up over the API; the Admin, Member and ReadOnly users are created in the
database (the signup rate limit is per IP and hourly) and are then added by the Owner over the
API and sign in like anyone.
"""

import asyncio
import csv
import hashlib
import io
import json
import os
import sys
import time
import uuid
import zipfile
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pyotp
import pytest
from sqlalchemy import select

from sijil_core.crypto import Envelope, LocalKeyService, open_envelope
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    DocumentState,
    IngestState,
    TenantCredential,
    User,
    Workspace,
    WorkspaceState,
)
from sijil_core.export_report import decode_key, read_report, verify_zip
from sijil_core.fields import FIELD_NAMES
from sijil_core.index import FieldsRow, IndexEngines, read_fields, write_extraction
from sijil_core.paperless import PaperlessClient, PaperlessError
from sijil_core.retention import region_local_date
from sijil_core.storage import ObjectRef, S3ObjectStore, StorageSettings
from sijil_core.tenancy import tenant_ref
from sijil_cp.auth.passwords import hash_password
from sijil_worker.extract import _route
from sijil_worker.runtime.base import ADMIN_USERNAME
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import WorkerSettings

AttachRunner = Callable[[str], AbstractContextManager[None]]

pytestmark = pytest.mark.integration

CP_URL = os.environ.get("SIJIL_IT_CP_URL", "http://control-plane:8000")
REGION = "ma-cas"
PASSWORD = "it-m4-acceptance-Passw0rd"
PROVISION_BUDGET_S = 240
CONSUME_BUDGET_S = 300
REMOVAL_BUDGET_S = 120  # the brief: original, Paperless copy and index rows gone within 120 s
EXPORT_BUDGET_S = 180
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
MANIFEST = {e["file"]: e for e in json.loads((FIXTURES / "invoices" / "manifest.json").read_text())}
SUPPLIERS = {s["ice"]: s for s in json.loads((FIXTURES / "suppliers.json").read_text())}
FILES = ["sample-01-fr.pdf", "sample-02-ar.pdf", "sample-03-fr.pdf", "sample-04-ar.pdf"]
REMOVAL_REASON = "Facture déposée dans le mauvais espace (SAMPLE)"
CORRECTION_REASON = "Avoir reçu du fournisseur après le scellement (SAMPLE)"


def _log(text: str) -> None:
    print(text, flush=True)  # noqa: T201 - the acceptance log (-s runs)


def _csrf(http: httpx.AsyncClient) -> dict[str, str]:
    return {"X-CSRF-Token": http.cookies["sijil_csrf"]}


async def _post(http: httpx.AsyncClient, path: str, body: Any) -> httpx.Response:
    return await http.post(path, json=body, headers=_csrf(http))


class Owner:
    """The signed-up Owner, with TOTP: each step-up spends a time step never used before."""

    def __init__(self, http: httpx.AsyncClient) -> None:
        self.http = http
        self.totp: pyotp.TOTP | None = None
        self.last_step = 0

    async def sign_up(self, email: str) -> None:
        (await self.http.get("/v1/auth/csrf")).raise_for_status()
        body = {"email": email, "password": PASSWORD, "name": "SAMPLE M4 Owner"}
        r = await _post(self.http, "/v1/auth/signup", body)
        assert r.status_code == 201, r.text
        r = await _post(self.http, "/v1/auth/totp/enrol", {"password": PASSWORD})
        assert r.status_code == 200, r.text
        self.totp = pyotp.TOTP(parse_qs(urlparse(r.json()["otpauth_uri"]).query)["secret"][0])
        # As in M1: well inside a step, confirm with the previous step and verify with this one.
        left = self.totp.interval - time.time() % self.totp.interval
        if left < 10:
            await asyncio.sleep(left + 0.5)
        now = time.time()
        r = await _post(self.http, "/v1/auth/totp/confirm", {"code": self.totp.at(now - 30)})
        assert r.status_code == 200, r.text
        r = await _post(self.http, "/v1/auth/totp/verify", {"code": self.totp.at(now)})
        assert r.status_code == 200, r.text
        self.last_step = int(now // 30)

    async def step_up(self) -> None:
        assert self.totp is not None
        # A code is accepted once per step: wait for the next step if this one is spent.
        await asyncio.sleep(max(0.0, (self.last_step + 1) * 30 - time.time() + 0.5))
        now = time.time()
        r = await _post(self.http, "/v1/auth/totp/verify", {"code": self.totp.at(now)})
        assert r.status_code == 200, r.text
        self.last_step = int(now // 30)


async def _signed_in(email: str) -> httpx.AsyncClient:
    http = httpx.AsyncClient(base_url=CP_URL, timeout=60)
    (await http.get("/v1/auth/csrf")).raise_for_status()
    r = await _post(http, "/v1/auth/login", {"email": email, "password": PASSWORD})
    assert r.status_code == 200, r.text
    return http


def _fields(name: str, ledger_id: uuid.UUID, today: str, now: datetime) -> FieldsRow:
    """The invoice's values from the SAMPLE manifest, dated today so the month export holds
    it; complete and confident, as an extraction that went well would leave it."""
    entry = MANIFEST[name]
    supplier = SUPPLIERS[entry["supplier_ice"]]
    values: dict[str, str | None] = {
        "document_type": "invoice",
        "supplier_name": supplier["name"],
        "supplier_ice": entry["supplier_ice"],
        "date": today,
        "number": entry["number"],
        "total_ht": entry["ht"],
        "vat": entry["tva"],
        "vat_rate": str(supplier["vat_rate"]),
        "total_ttc": entry["ttc"],
        "currency": entry["currency"],
    }
    return FieldsRow(
        ledger_id=ledger_id,
        document_type="invoice",
        values={n: values.get(n) for n in FIELD_NAMES},
        confidence=dict.fromkeys(FIELD_NAMES, 0.95),
        signals={n: {"runs_agree": True} for n in FIELD_NAMES},
        source=dict.fromkeys(FIELD_NAMES, "extraction"),
        extracted_at=now,
        updated_at=now,
    )


async def _cli(*args: str) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "sijil_cp.cli", *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        # The runner carries the worker's environment; the CLI also wants the API's one flag.
        env=os.environ | {"SIJIL_COOKIE_SECURE": "false"},
    )  # fmt: skip
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode()


def _tampered(source: Path, target: Path, entry: str) -> None:
    """A copy of the ZIP with one byte of `entry` flipped."""
    with zipfile.ZipFile(source) as zin, zipfile.ZipFile(target, "w") as zout:
        for info in zin.infolist():
            data = zin.read(info)
            if info.filename == entry:
                flipped = bytearray(data)
                flipped[len(flipped) // 2] ^= 0x01
                data = bytes(flipped)
            zout.writestr(info, data)


async def test_m4_acceptance(
    docker_runtime: DockerRuntime,
    worker_settings: WorkerSettings,
    attach_runner: AttachRunner,
    created_workspaces: list[uuid.UUID],
    tmp_path: Path,
) -> None:
    run = uuid.uuid4().hex[:8]
    engine = make_engine(worker_settings.database_url)
    sessions = make_sessionmaker(engine)
    keys = LocalKeyService(worker_settings.master_key_bytes())
    engines = IndexEngines(worker_settings, keys, sessions)
    root = S3ObjectStore(StorageSettings())  # type: ignore[call-arg]
    clients: list[httpx.AsyncClient] = []
    try:
        # 1. A fresh workspace, its Owner with TOTP.
        owner = Owner(httpx.AsyncClient(base_url=CP_URL, timeout=60))
        clients.append(owner.http)
        await owner.sign_up(f"m4-{run}@sijil.local")
        http = owner.http
        r = await _post(
            http, "/v1/orgs", {"name": f"SAMPLE M4 Acceptance {run}", "type": "business",
                               "country": "MA"},
        )  # fmt: skip
        assert r.status_code == 202, r.text
        org_id = r.json()["org"]["id"]
        workspace_id = uuid.UUID(r.json()["workspace"]["id"])
        created_workspaces.append(workspace_id)
        base = f"/v1/workspaces/{workspace_id}"
        started = time.monotonic()
        while (state := (await http.get(base)).json()["state"]) != WorkspaceState.active:
            assert state != WorkspaceState.error
            assert time.monotonic() - started < PROVISION_BUDGET_S, state
            await asyncio.sleep(3)
        _log(f"workspace {workspace_id} active after {time.monotonic() - started:.0f} s")
        async with sessions() as db:
            workspace = await db.get(Workspace, workspace_id)
        assert workspace is not None and workspace.paperless_url is not None

        # The other roles: users made in the database, added by the Owner over the API.
        await owner.step_up()
        members: dict[str, httpx.AsyncClient] = {"owner": http}
        for role in ("admin", "member", "readonly"):
            email = f"m4-{run}-{role}@sijil.local"
            async with sessions() as db:
                db.add(
                    User(email=email, name=f"SAMPLE {role}", password_hash=hash_password(PASSWORD))
                )
                await db.commit()
            r = await _post(http, f"/v1/orgs/{org_id}/members", {"email": email, "role": role})
            assert r.status_code == 201, r.text
            members[role] = await _signed_in(email)
            clients.append(members[role])

        # 2. Four SAMPLE invoices through the API; 3. their fields written straight into the
        # index before ingest finishes (extraction then finds them and never runs the model).
        now = datetime.now(UTC)
        today = region_local_date(now, REGION)
        ids: list[uuid.UUID] = []
        for name in FILES:
            data = (FIXTURES / "invoices" / name).read_bytes()
            r = await http.post(
                f"{base}/documents", files={"file": (name, data, "application/pdf")},
                headers=_csrf(http),
            )  # fmt: skip
            assert r.status_code == 202, r.text
            ledger_id = uuid.UUID(r.json()["document_id"])
            ids.append(ledger_id)
            async with (await engines.engine(workspace)).begin() as conn:
                await write_extraction(conn, _fields(name, ledger_id, today.isoformat(), now))

        async def ledger(ledger_id: uuid.UUID) -> DocumentLedger:
            async with sessions() as db:
                row = await db.get(DocumentLedger, ledger_id, populate_existing=True)
            assert row is not None
            return row

        started = time.monotonic()
        while [i for i in ids if (await ledger(i)).ingest_state is not IngestState.consumed]:
            assert time.monotonic() - started < CONSUME_BUDGET_S, "not consumed in time"
            await asyncio.sleep(3)
        _log(f"4 documents consumed after {time.monotonic() - started:.0f} s")
        # Routed by the worker's own routing (the extraction worker may get there first).
        for ledger_id in ids:
            async with (await engines.engine(workspace)).connect() as conn:
                row = await read_fields(conn, ledger_id)
            assert row is not None
            await _route(sessions, engines, root, workspace, row, datetime.now(UTC))
        rows = [await ledger(i) for i in ids]
        assert all(r.state in (DocumentState.filed, DocumentState.needs_review) for r in rows)
        _log("routed: " + ", ".join(r.state.value for r in rows))
        sealed_a, sealed_b, removed, kept = rows

        # 4. Seal two.
        for row in (sealed_a, sealed_b):
            r = await http.post(f"{base}/documents/{row.id}/seal", headers=_csrf(http))
            assert r.status_code == 200, r.text
            assert r.json()["state"] == DocumentState.sealed

        # 5. The Owner removes an unsealed one, with a step-up.
        await owner.step_up()
        r = await _post(http, f"{base}/documents/{removed.id}/remove", {"reason": REMOVAL_REASON})
        assert r.status_code == 202, r.text
        removed_at = time.monotonic()
        async with sessions() as db:
            cred = await db.scalar(
                select(TenantCredential).where(
                    TenantCredential.workspace_id == workspace_id,
                    TenantCredential.kind == CredentialKind.admin_password,
                )
            )
        assert cred is not None
        admin_password = open_envelope(
            Envelope.from_bytes(cred.envelope), keys,
            f"{workspace_id}:{CredentialKind.admin_password}",
        ).decode()  # fmt: skip
        ref = ObjectRef(removed.object_key, removed.object_version_id)
        assert removed.paperless_doc_id is not None
        with attach_runner(docker_runtime.network_name(tenant_ref(workspace_id))):
            async with PaperlessClient(
                workspace.paperless_url, basic=(ADMIN_USERNAME, admin_password)
            ) as admin:

                async def in_paperless() -> bool:
                    """Live, in the trash, or another copy with the same checksum."""
                    assert removed.paperless_doc_id is not None
                    try:
                        await admin.get_document(removed.paperless_doc_id)
                    except PaperlessError as exc:
                        if exc.status != 404:
                            raise
                    else:
                        return True
                    trashed = {d.id for d in await admin.trashed_documents()}
                    return bool(
                        await admin.documents_with_checksum(removed.sha256)
                        or removed.paperless_doc_id in trashed
                    )

                while True:
                    async with (await engines.engine(workspace)).connect() as conn:
                        index_row = await read_fields(conn, removed.id)
                    left = {
                        "original": await root.head(ref) is not None,
                        "paperless": await in_paperless(),
                        "index": index_row is not None,
                    }
                    if not any(left.values()):
                        break
                    if time.monotonic() - removed_at > REMOVAL_BUDGET_S:
                        pytest.fail(f"still present after {REMOVAL_BUDGET_S} s: {left}")
                    await asyncio.sleep(2)
        _log(f"removed document gone {time.monotonic() - removed_at:.0f} s after the request")
        tomb = await ledger(removed.id)
        assert tomb.state is DocumentState.removed
        assert (tomb.sha256, tomb.object_key, tomb.object_version_id) == (
            removed.sha256, removed.object_key, removed.object_version_id,
        )  # fmt: skip
        assert tomb.original_removed_at is not None

        # 6. No role removes a sealed one.
        expected = {
            "owner": (409, "document.sealed_cannot_be_removed"),
            "admin": (403, "forbidden"),
            "member": (403, "forbidden"),
            "readonly": (403, "forbidden"),
        }
        await owner.step_up()
        for role, client in members.items():
            r = await _post(
                client, f"{base}/documents/{sealed_a.id}/remove", {"reason": REMOVAL_REASON}
            )
            assert (r.status_code, r.json()["error"]["code"]) == expected[role], (role, r.text)
        assert (await ledger(sealed_a.id)).state is DocumentState.sealed
        async with sessions() as db:
            refused = list(
                await db.scalars(
                    select(AuditEvent.data).where(
                        AuditEvent.workspace_id == workspace_id,
                        AuditEvent.action == "document.removal_refused",
                        AuditEvent.target_id == str(sealed_a.id),
                    )
                )
            )
        assert sorted(d["code"] for d in refused) == sorted(c for _, c in expected.values())

        # 7. A correction note on a sealed one; the seal stays as it was.
        seal = (await ledger(sealed_a.id)).fields_sha256
        ttc = MANIFEST[FILES[0]]["ttc"]
        corrected = f"{Decimal(ttc) - Decimal('50.00'):.2f}"
        r = await _post(
            http, f"{base}/documents/{sealed_a.id}/corrections",
            {"field": "total_ttc", "value": corrected, "reason": CORRECTION_REASON},
        )  # fmt: skip
        assert r.status_code == 201, r.text
        assert (await ledger(sealed_a.id)).fields_sha256 == seal

        # 8. Export the month and download it.
        await owner.step_up()
        period = f"{today:%Y-%m}"
        r = await _post(http, f"{base}/exports", {"period": period})
        assert r.status_code == 202, r.text
        export_id = r.json()["id"]
        started = time.monotonic()
        while (job := (await http.get(f"{base}/exports/{export_id}")).json())["state"] != "ready":
            assert job["state"] in ("queued", "building"), job
            assert time.monotonic() - started < EXPORT_BUDGET_S, job
            await asyncio.sleep(2)
        _log(f"export {period} ready after {time.monotonic() - started:.0f} s")
        r = await http.get(f"{base}/exports/{export_id}/download")
        assert r.status_code == 200, r.text
        archive = tmp_path / "export.zip"
        archive.write_bytes(r.content)
        assert hashlib.sha256(r.content).hexdigest() == job["sha256"]
        published = (await http.get(f"/v1/regions/{REGION}/export-key")).json()
        public = decode_key(published["public_key"])
        outcome = verify_zip(archive, public)
        assert outcome.ok, outcome.problems
        code, out = await _cli("export", "verify", str(archive), "--against-ledger")
        assert (code, out.strip()) == (0, "ok"), out
        report = read_report(archive)
        assert report is not None
        documents = {d["id"]: d for d in report["documents"]}
        assert set(documents) == {str(sealed_a.id), str(sealed_b.id), str(kept.id)}

        # 9. The CSV flags the corrected field; the removed document is not in it.
        with zipfile.ZipFile(archive) as zf:
            raw = zf.read("index.csv")
        assert raw.startswith(b"\xef\xbb\xbf")
        table = list(csv.DictReader(io.StringIO(raw[3:].decode("utf-8")), delimiter=";"))
        by_id = {line["document_id"]: line for line in table}
        assert set(by_id) == set(documents)
        assert str(removed.id) not in raw.decode("utf-8")
        assert by_id[str(sealed_a.id)]["corrected_fields"] == "total_ttc"
        assert by_id[str(sealed_a.id)]["total_ttc"] == corrected
        assert by_id[str(sealed_b.id)]["corrected_fields"] == ""

        # 10. One byte tampered: verification fails, naming the file.
        entry = documents[str(sealed_a.id)]["path"]
        tampered = tmp_path / "tampered.zip"
        _tampered(archive, tampered, entry)
        outcome = verify_zip(tampered, public)
        assert (outcome.ok, outcome.problems) == (False, [f"file.hash:{entry}"])
        code, out = await _cli("export", "verify", str(tampered))
        assert (code, out.strip()) == (1, f"file.hash:{entry}"), out

        # 11. The audit chain verifies.
        code, out = await _cli("audit", "verify", "--workspace", str(workspace_id))
        assert code == 0, out
        assert out.startswith("ok "), out
    finally:
        # Each step on its own; the tenant always goes (the workspace is marked closing by the
        # `created_workspaces` fixture). Originals stay: they are under retention.
        try:
            for client in clients:
                await client.aclose()
            await engines.aclose()
            await engine.dispose()
        finally:
            for created_id in created_workspaces:
                await docker_runtime.destroy(tenant_ref(created_id))
