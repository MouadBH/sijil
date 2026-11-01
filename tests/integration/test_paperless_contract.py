"""Contract tests against the real Paperless-ngx in compose. Run with `make itest`."""

import asyncio
import os
import time
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from sijil_core.paperless import (
    SERVICE_PERMISSIONS,
    SUPPORT_PERMISSIONS,
    PaperlessClient,
    PaperlessForbidden,
)

pytestmark = pytest.mark.integration

URL = os.environ.get("SIJIL_IT_T1_URL", "http://paperless-t1:8000")
ADMIN_PASSWORD = os.environ.get("SIJIL_IT_T1_ADMIN_PASSWORD", "")
SAMPLE = Path(__file__).parents[2] / "fixtures" / "invoices" / "sample-01-fr.pdf"
USER_PASSWORD = "it-Passw0rd-not-a-secret"  # noqa: S105


@pytest.fixture
async def admin() -> AsyncIterator[PaperlessClient]:
    async with PaperlessClient(URL, basic=("sijil-admin", ADMIN_PASSWORD)) as client:
        await client.wait_ready(180)
        yield client


async def test_api_reachable_and_versioned(admin: PaperlessClient) -> None:
    assert await admin.api_version() >= 1


async def test_service_user_cannot_delete(admin: PaperlessClient) -> None:
    await admin.ensure_user("sijil-it-service", USER_PASSWORD, SERVICE_PERMISSIONS)
    # idempotent: a second call updates rather than failing
    await admin.ensure_user("sijil-it-service", USER_PASSWORD, SERVICE_PERMISSIONS)
    assert await admin.user_permissions("sijil-it-service") == set(SERVICE_PERMISSIONS)
    token = await admin.obtain_token("sijil-it-service", USER_PASSWORD)
    async with PaperlessClient(URL, token=token) as svc:
        task_id = await svc.upload(SAMPLE.read_bytes(), SAMPLE.name)
        deadline = time.monotonic() + 120
        task = await svc.task(task_id)
        while task.status not in ("SUCCESS", "FAILURE") and time.monotonic() < deadline:
            await asyncio.sleep(2)
            task = await svc.task(task_id)
        assert task.status == "SUCCESS"
        assert task.related_document is not None
        doc = await svc.get_document(task.related_document)
        assert doc["id"] == task.related_document
        with pytest.raises(PaperlessForbidden):
            await svc.delete_document(task.related_document)


async def test_support_user_read_only(admin: PaperlessClient) -> None:
    await admin.ensure_user("sijil-it-support", USER_PASSWORD, SUPPORT_PERMISSIONS)
    perms = await admin.user_permissions("sijil-it-support")
    assert perms == set(SUPPORT_PERMISSIONS)
    assert all(p.startswith("view_") for p in perms)
