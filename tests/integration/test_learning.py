"""Contract with the live Paperless-ngx 3.4.1 of a fresh workspace (`make itest`): what
`sijil_worker.learning` relies on for a learnt supplier's correspondent.

A correspondent made by `ensure_correspondent` with `matching_algorithm` 3 ("exact" in 3.4.1's
`MatchingModel`: the text contains the `match` string as a word) and the supplier's ICE is
assigned by Paperless to a newly consumed document whose text carries that ICE, and not one
whose ICE is absent. A decoy with the same algorithm whose two words are both in the text, but
never side by side in that order, is not matched: so 3 is the literal phrase, never ANY (1) or
ALL (2), which would match it. The service token (view/add/change, no delete) can create a
correspondent, rename it (only its name changes, its rule stays) and set it on a document.
The workspace is created for this test and marked `closing` at teardown: the shared SAMPLE
workspace is never touched.
"""

import asyncio
import time
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from sijil_core.crypto import Envelope, LocalKeyService, open_envelope
from sijil_core.db.engine import make_engine, make_sessionmaker
from sijil_core.db.models import CredentialKind, TenantCredential, Workspace
from sijil_core.paperless import PaperlessClient
from sijil_core.tenancy import tenant_ref
from sijil_worker.runtime.docker import DockerRuntime
from sijil_worker.settings import WorkerSettings
from sijil_worker.taxonomy import MATCH_LITERAL
from tests.support.polling import poll_get

pytestmark = pytest.mark.integration

AttachRunner = Callable[[str], AbstractContextManager[None]]

CP_URL = "http://control-plane:8000"
PASSWORD = "it-learning-Passw0rd"  # noqa: S105 - a throw-away SAMPLE account
SAMPLE = Path(__file__).parents[2] / "fixtures" / "invoices" / "sample-01-fr.pdf"
ICE = "001905837000041"  # SAMPLE: Atlas Télécom Services SARL (fictitious), printed in SAMPLE
ABSENT_ICE = "009999999000099"  # SAMPLE: printed on no fixture
# Both words are printed on sample-01-fr.pdf, never as this phrase (asserted below).
DECOY = "Casablanca SAMPLE"
PROVISION_BUDGET_S = 240
CONSUME_BUDGET_S = 180


def _csrf(http: httpx.AsyncClient) -> dict[str, str]:
    return {"X-CSRF-Token": http.cookies["sijil_csrf"]}


async def _fresh_workspace(http: httpx.AsyncClient, created: list[uuid.UUID]) -> uuid.UUID:
    run = uuid.uuid4().hex[:8]
    (await http.get("/v1/auth/csrf")).raise_for_status()
    r = await http.post(
        "/v1/auth/signup",
        json={"email": f"learn-{run}@sijil.local", "password": PASSWORD,
              "name": "SAMPLE Learning Owner"},
        headers=_csrf(http),
    )  # fmt: skip
    assert r.status_code == 201, r.text
    r = await http.post(
        "/v1/orgs",
        json={"name": f"SAMPLE Learning {run}", "type": "business", "country": "MA"},
        headers=_csrf(http),
    )
    assert r.status_code == 202, r.text
    workspace_id = uuid.UUID(r.json()["workspace"]["id"])
    created.append(workspace_id)
    started = time.monotonic()
    path = f"/v1/workspaces/{workspace_id}"
    while (state := (await poll_get(http, path)).json()["state"]) != "active":
        assert state != "error"
        assert time.monotonic() - started < PROVISION_BUDGET_S, state
        await asyncio.sleep(3)
    return workspace_id


async def test_literal_ice_correspondent_is_assigned_at_consumption(
    worker_settings: WorkerSettings,
    docker_runtime: DockerRuntime,
    attach_runner: AttachRunner,
    created_workspaces: list[uuid.UUID],
) -> None:
    async with httpx.AsyncClient(base_url=CP_URL, timeout=60) as http:
        workspace_id = await _fresh_workspace(http, created_workspaces)
    engine = make_engine(worker_settings.database_url)
    keys = LocalKeyService(worker_settings.master_key_bytes())
    try:
        async with make_sessionmaker(engine)() as db:
            workspace = await db.get(Workspace, workspace_id)
            credential = await db.scalar(
                select(TenantCredential).where(
                    TenantCredential.workspace_id == workspace_id,
                    TenantCredential.kind == CredentialKind.service_token,
                )
            )
    finally:
        await engine.dispose()
    assert workspace is not None and workspace.paperless_url is not None
    assert credential is not None
    token = open_envelope(
        Envelope.from_bytes(credential.envelope), keys,
        f"{workspace_id}:{CredentialKind.service_token}",
    ).decode()  # fmt: skip

    with attach_runner(docker_runtime.network_name(tenant_ref(workspace_id))):
        async with PaperlessClient(workspace.paperless_url, token=token) as svc:
            await svc.wait_ready(120)
            atlas = await svc.ensure_correspondent("Atlas Télécom SAMPLE", ICE, MATCH_LITERAL)
            # Reused by name, whatever the case, and never changed.
            assert await svc.ensure_correspondent("ATLAS TÉLÉCOM SAMPLE", "x", 0) == atlas
            absent = await svc.ensure_correspondent("SAMPLE Absent", ABSENT_ICE, MATCH_LITERAL)
            stored = (await svc._request("GET", f"/api/correspondents/{atlas}/")).json()  # noqa: SLF001
            assert (stored["match"], stored["matching_algorithm"]) == (ICE, 3)
            assert stored["is_insensitive"] is True
            decoy = await svc.ensure_correspondent("SAMPLE Decoy", DECOY, MATCH_LITERAL)

            # A trailing PDF comment makes the run's upload unique.
            content = SAMPLE.read_bytes() + f"\n%sijil-it-{uuid.uuid4()}\n".encode()
            task_id = await svc.upload(content, SAMPLE.name)
            deadline = time.monotonic() + CONSUME_BUDGET_S
            task = await svc.task(task_id)
            while task.status not in ("SUCCESS", "FAILURE") and time.monotonic() < deadline:
                await asyncio.sleep(2)
                task = await svc.task(task_id)
            assert task.status == "SUCCESS", task.status
            assert task.related_document is not None
            document = await svc.get_document(task.related_document)
            text = await svc.document_content(task.related_document)
            assert ICE in text
            words = " ".join(text.casefold().split())
            assert all(w.casefold() in words for w in DECOY.split())
            assert DECOY.casefold() not in words
            assert document["correspondent"] == atlas
            # Every correspondent whose rule matches the text: the ICE's, never the decoy's.
            matched = (await svc.document_suggestions(task.related_document))["correspondents"]
            assert matched == [atlas], (matched, decoy, absent)

            # A rename (the learning's, of a correspondent it created): the name only.
            await svc.rename_correspondent(atlas, "Atlas Télécom Services SAMPLE")
            renamed = await svc.correspondent(atlas)
            assert renamed is not None
            assert renamed["name"] == "Atlas Télécom Services SAMPLE"
            assert (renamed["match"], renamed["matching_algorithm"]) == (ICE, MATCH_LITERAL)
            assert await svc.correspondent(10_000_000) is None

            # The learning sets it on a document as well (the change right is enough).
            await svc.set_correspondent(task.related_document, absent)
            assert (await svc.get_document(task.related_document))["correspondent"] == absent
