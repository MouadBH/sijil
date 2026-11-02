"""`make seed`: twelve SAMPLE Moroccan invoices into a workspace, through the API and by email.

Runs in the compose `tests` runner (the control plane and the `smtp` service are reachable by
service name). Idempotent: it signs in as the fixed SAMPLE user (signing up only the first time,
so the signup rate limit and the three-organisation cap are never touched again), reuses the
SAMPLE organisation, and skips what is already archived. Prints the workspace id.

    python -m tests.support.seed
"""

import asyncio
import hashlib
import os
import smtplib
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from uuid import UUID

import httpx
from sqlalchemy import select

from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import (
    DocumentLedger,
    IngestState,
    Membership,
    Organisation,
    Workspace,
    WorkspaceState,
)

CP_URL = os.environ.get("SIJIL_IT_CP_URL", "http://control-plane:8000")
SMTP_HOST = os.environ.get("SIJIL_IT_SMTP_HOST", "smtp")
SMTP_PORT = 2525
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "invoices"
SAMPLE_EMAIL = "sample-owner@sijil.local"
SAMPLE_PASSWORD = "sample-Passw0rd-not-a-secret"  # noqa: S105 - SAMPLE data only
SAMPLE_ORG = "SAMPLE Société Atlas SARL"
PROVISION_BUDGET_S = 240
CONSUME_BUDGET_S = 180
VIA_API = 6  # sample-01..06 by upload, sample-07..12 by email
SETTLED = {IngestState.consumed, IngestState.failed, IngestState.unsupported}


@dataclass(frozen=True)
class Seeded:
    email: str
    workspace_id: UUID
    inbound_email: str
    fixtures: list[Path]
    uploaded: int
    emailed: int
    already_present: int


def invoices() -> list[Path]:
    found = sorted(FIXTURES.glob("sample-*.pdf"))
    if len(found) != 12:
        raise SystemExit(f"expected 12 SAMPLE invoices in {FIXTURES}, found {len(found)}")
    return found


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _csrf(http: httpx.AsyncClient) -> dict[str, str]:
    return {"X-CSRF-Token": http.cookies["sijil_csrf"]}


@asynccontextmanager
async def signed_in(
    email: str, *, password: str = SAMPLE_PASSWORD
) -> AsyncIterator[httpx.AsyncClient]:
    """A client with a session for `email`: logs in, or signs up if the account is new."""
    async with httpx.AsyncClient(base_url=CP_URL, timeout=60) as http:
        (await http.get("/v1/auth/csrf")).raise_for_status()
        creds = {"email": email, "password": password}
        r = await http.post("/v1/auth/login", json=creds, headers=_csrf(http))
        if r.status_code == 401:
            r = await http.post(
                "/v1/auth/signup", json=creds | {"name": "SAMPLE Owner"}, headers=_csrf(http)
            )
        r.raise_for_status()
        yield http


async def upload(http: httpx.AsyncClient, workspace_id: UUID, path: Path) -> httpx.Response:
    data = await asyncio.to_thread(path.read_bytes)
    return await http.post(
        f"/v1/workspaces/{workspace_id}/documents",
        files={"file": (path.name, data, "application/pdf")},
        headers=_csrf(http),
    )


def _send(address: str, path: Path) -> None:
    message = EmailMessage()
    message["From"] = "fournisseur@sample.example"
    message["To"] = address
    message["Subject"] = f"SAMPLE facture {path.stem}"
    message.set_content("SAMPLE: fictitious invoice attached.")
    message.add_attachment(
        path.read_bytes(), maintype="application", subtype="pdf", filename=path.name
    )
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as client:
        client.send_message(message, from_addr="fournisseur@sample.example")


async def seed(
    *,
    email: str = SAMPLE_EMAIL,
    org_name: str = SAMPLE_ORG,
    consume_budget_s: int = CONSUME_BUDGET_S,
    on_workspace: Callable[[UUID], None] | None = None,
) -> Seeded:
    """`on_workspace` is called with the workspace id as soon as it exists, before anything can
    fail, so a caller can always clean up the tenant a failed seed leaves behind."""
    files = invoices()
    engine = make_engine(os.environ["SIJIL_DATABASE_URL"])
    sessions = make_sessionmaker(engine)

    async def ledger() -> list[DocumentLedger]:
        async with sessions() as db:
            return list(
                await db.scalars(
                    select(DocumentLedger).where(DocumentLedger.workspace_id == workspace_id)
                )
            )

    try:
        async with signed_in(email) as http:
            me = await http.get("/v1/me")
            me.raise_for_status()
            user_id = UUID(me.json()["id"])
            async with sessions() as db:
                found = await db.scalar(
                    select(Workspace.id)
                    .join(Organisation, Organisation.id == Workspace.org_id)
                    .join(Membership, Membership.org_id == Organisation.id)
                    .where(Membership.user_id == user_id, Organisation.name == org_name)
                    .order_by(Workspace.created_at)
                )
            if found is None:
                r = await http.post(
                    "/v1/orgs",
                    json={"name": org_name, "type": "business", "country": "MA"},
                    headers=_csrf(http),
                )
                r.raise_for_status()
                workspace_id = UUID(r.json()["workspace"]["id"])
            else:
                workspace_id = found
            if on_workspace is not None:
                on_workspace(workspace_id)

            deadline = time.monotonic() + PROVISION_BUDGET_S
            while True:
                r = await http.get(f"/v1/workspaces/{workspace_id}")
                r.raise_for_status()
                info = r.json()
                if info["state"] == WorkspaceState.active:
                    break
                if info["state"] == WorkspaceState.error or time.monotonic() > deadline:
                    raise SystemExit(f"workspace {workspace_id} is {info['state']}")
                await asyncio.sleep(3)
            inbound = str(info["inbound_email"])

            have = {row.sha256 for row in await ledger()}
            uploaded = emailed = skipped = 0
            for index, path in enumerate(files):
                if _sha(path) in have:
                    skipped += 1
                elif index < VIA_API:
                    r = await upload(http, workspace_id, path)
                    if r.status_code not in (202, 409):
                        raise SystemExit(f"upload of {path.name} failed: {r.status_code} {r.text}")
                    uploaded += r.status_code == 202
                    skipped += r.status_code == 409
                else:
                    await asyncio.to_thread(_send, inbound, path)
                    emailed += 1

        wanted = {_sha(p) for p in files}
        deadline = time.monotonic() + consume_budget_s
        while True:
            rows = await ledger()
            if wanted <= {r.sha256 for r in rows} and all(r.ingest_state in SETTLED for r in rows):
                break
            if time.monotonic() > deadline:
                raise SystemExit(f"not all documents settled within {consume_budget_s} s")
            await asyncio.sleep(3)
        bad = [r.sha256[:12] for r in rows if r.ingest_state is not IngestState.consumed]
        if bad:
            raise SystemExit(f"documents not consumed: {bad}")
        return Seeded(email, workspace_id, inbound, files, uploaded, emailed, skipped)
    finally:
        await engine.dispose()


def main() -> None:
    started = time.monotonic()
    result = asyncio.run(seed())
    print(  # noqa: T201
        f"uploaded={result.uploaded} emailed={result.emailed} "
        f"already_present={result.already_present} in {time.monotonic() - started:.0f} s\n"
        f"user: {result.email}\ninbound address: {result.inbound_email}\n"
        f"workspace id: {result.workspace_id}"
    )


if __name__ == "__main__":
    main()
