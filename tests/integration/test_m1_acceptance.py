"""M1 acceptance: a person signs up, creates an organisation and gets a running, isolated
Paperless-ngx tenant within 180 s. Everything goes over HTTP to the control plane, like the web
app; the database is read only to prove the stored service token is sealed and usable.
"""

import asyncio
import os
import time
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from urllib.parse import parse_qs, urlparse

import httpx
import pyotp
import pytest
from sqlalchemy import select

from sijil_core.crypto import Envelope, LocalKeyService, open_envelope
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import CredentialKind, TenantCredential, Workspace
from sijil_core.paperless import PaperlessClient, PaperlessForbidden
from sijil_core.tenancy import tenant_ref
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import WorkerSettings

AttachRunner = Callable[[str], AbstractContextManager[None]]

pytestmark = pytest.mark.integration

CP_URL = os.environ.get("SIJIL_IT_CP_URL", "http://control-plane:8000")
PROVISION_BUDGET_S = 180
PASSWORD = "it-acceptance-Passw0rd"
# Long enough that a name-derived hostname would break DNS's 63-character label.
ORG_NAME = "Société Marocaine de Distribution et de Commerce SARL"


async def _post(http: httpx.AsyncClient, path: str, body: dict[str, object]) -> httpx.Response:
    return await http.post(path, json=body, headers={"X-CSRF-Token": http.cookies["sijil_csrf"]})


async def _sign_up_with_totp(http: httpx.AsyncClient) -> None:
    (await http.get("/v1/auth/csrf")).raise_for_status()
    email = f"it-{uuid.uuid4().hex[:10]}@example.com"
    r = await _post(http, "/v1/auth/signup", {"email": email, "password": PASSWORD, "name": "IT"})
    assert r.status_code == 201, r.text
    r = await _post(http, "/v1/auth/totp/enrol", {"password": PASSWORD})
    assert r.status_code == 200, r.text
    secret = parse_qs(urlparse(r.json()["otpauth_uri"]).query)["secret"][0]
    totp = pyotp.TOTP(secret)
    now = time.time()
    # Each time-step is spent once: confirm with the previous step, verify with the current.
    r = await _post(http, "/v1/auth/totp/confirm", {"code": totp.at(now - 30)})
    assert r.status_code == 200, r.text
    r = await _post(http, "/v1/auth/totp/verify", {"code": totp.at(now)})
    assert r.status_code == 200, r.text


async def _service_token(settings: WorkerSettings, workspace_id: uuid.UUID) -> tuple[str, str]:
    engine = make_engine(settings.database_url)
    try:
        async with make_sessionmaker(engine)() as session:
            workspace = await session.get(Workspace, workspace_id)
            assert workspace is not None
            assert workspace.paperless_url is not None
            row = await session.scalar(
                select(TenantCredential).where(
                    TenantCredential.workspace_id == workspace_id,
                    TenantCredential.kind == CredentialKind.service_token,
                )
            )
            assert row is not None
            token = open_envelope(
                Envelope.from_bytes(row.envelope),
                LocalKeyService(settings.master_key_bytes()),
                f"{workspace_id}:{CredentialKind.service_token}",
            ).decode()
            return workspace.paperless_url, token
    finally:
        await engine.dispose()


async def test_m1_acceptance(
    docker_runtime: DockerRuntime, worker_settings: WorkerSettings, attach_runner: AttachRunner
) -> None:
    ref: str | None = None
    try:
        async with httpx.AsyncClient(base_url=CP_URL, timeout=30) as http:
            await _sign_up_with_totp(http)

            started = time.monotonic()
            r = await _post(
                http, "/v1/orgs", {"name": ORG_NAME, "type": "business", "country": "MA"}
            )
            assert r.status_code == 202, r.text
            created = r.json()
            workspace_id = uuid.UUID(created["workspace"]["id"])
            ref = tenant_ref(workspace_id)

            job: dict[str, object] = {}
            while time.monotonic() - started < PROVISION_BUDGET_S:
                r = await http.get(f"/v1/jobs/{created['job_id']}")
                assert r.status_code == 200, r.text
                job = r.json()
                if job["status"] in ("succeeded", "failed"):
                    break
                await asyncio.sleep(2)
            elapsed = time.monotonic() - started
            print(f"provisioning took {elapsed:.1f} s")  # noqa: T201 - reported by -s runs
            assert job.get("status") == "succeeded", job
            assert elapsed < PROVISION_BUDGET_S

            r = await http.get(f"/v1/workspaces/{workspace_id}")
            assert r.status_code == 200, r.text
            assert r.json()["state"] == "active"

        url, token = await _service_token(worker_settings, workspace_id)
        assert url == f"http://{docker_runtime.paperless_name(ref)}:8000"
        with attach_runner(docker_runtime.network_name(ref)):
            async with PaperlessClient(url, token=token) as service:
                assert await service.api_version() >= 1
                with pytest.raises(PaperlessForbidden):
                    await service.delete_document(1)
    finally:
        if ref is not None:
            await docker_runtime.destroy(ref)
