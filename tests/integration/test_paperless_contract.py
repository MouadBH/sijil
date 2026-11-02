"""Contract tests against the real Paperless-ngx in compose. Run with `make itest`."""

import asyncio
import hashlib
import os
import time
import uuid
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


async def _assert_no_extra_grants(admin: PaperlessClient, username: str) -> None:
    user = await admin._find_user(username)  # noqa: SLF001
    assert user is not None
    assert user["groups"] == []
    assert user["is_superuser"] is False
    assert user["is_staff"] is False


async def test_ensure_user_strips_preexisting_grants(admin: PaperlessClient) -> None:
    """A user already holding groups or superuser is stripped back on the next ensure_user."""
    user_id = await admin.ensure_user("sijil-it-strip", USER_PASSWORD, SUPPORT_PERMISSIONS)
    await admin._request(  # noqa: SLF001
        "PATCH", f"/api/users/{user_id}/", json={"is_superuser": True, "is_staff": True}
    )
    await admin.ensure_user("sijil-it-strip", USER_PASSWORD, SUPPORT_PERMISSIONS)
    await _assert_no_extra_grants(admin, "sijil-it-strip")


async def test_api_reachable_and_versioned(admin: PaperlessClient) -> None:
    assert await admin.api_version() >= 1


async def test_service_user_cannot_delete(admin: PaperlessClient) -> None:
    await admin.ensure_user("sijil-it-service", USER_PASSWORD, SERVICE_PERMISSIONS)
    # idempotent: a second call updates rather than failing
    await admin.ensure_user("sijil-it-service", USER_PASSWORD, SERVICE_PERMISSIONS)
    assert await admin.user_permissions("sijil-it-service") == set(SERVICE_PERMISSIONS)
    await _assert_no_extra_grants(admin, "sijil-it-service")
    token = await admin.obtain_token("sijil-it-service", USER_PASSWORD)
    async with PaperlessClient(URL, token=token) as svc:
        # Paperless rejects duplicate checksums: a trailing PDF comment makes each run unique.
        content = SAMPLE.read_bytes() + f"\n%sijil-it-{uuid.uuid4()}\n".encode()
        task_id = await svc.upload(content, SAMPLE.name)
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
        assert (await svc.get_document(task.related_document))["id"] == task.related_document


async def test_support_user_read_only(admin: PaperlessClient) -> None:
    await admin.ensure_user("sijil-it-support", USER_PASSWORD, SUPPORT_PERMISSIONS)
    perms = await admin.user_permissions("sijil-it-support")
    assert perms == set(SUPPORT_PERMISSIONS)
    assert all(p.startswith("view_") for p in perms)
    await _assert_no_extra_grants(admin, "sijil-it-support")


async def test_checksum_lookup_uses_the_filter_paperless_honours(admin: PaperlessClient) -> None:
    """Paperless 3.4.1 declares `checksum` with istartswith/iendswith/icontains/iexact only.

    A bare `checksum=` is not a filter there: django-filter ignores it and returns every
    document, so an "exact" lookup would adopt an unrelated document. Hence `checksum__iexact`.
    """
    content = SAMPLE.read_bytes() + f"\n%sijil-it-{uuid.uuid4()}\n".encode()
    sha256 = hashlib.sha256(content).hexdigest()
    task_id = await admin.upload(content, SAMPLE.name)
    deadline = time.monotonic() + 120
    task = await admin.task(task_id)
    while task.status not in ("SUCCESS", "FAILURE") and time.monotonic() < deadline:
        await asyncio.sleep(2)
        task = await admin.task(task_id)
    assert task.status == "SUCCESS"
    assert await admin.find_document_by_checksum(sha256) == task.related_document
    absent = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    assert await admin.find_document_by_checksum(absent) is None
    ignored = await admin._request(  # noqa: SLF001
        "GET", "/api/documents/", params={"checksum": absent, "page_size": 1}
    )
    assert ignored.json()["count"] >= 1  # the bare filter matched documents it should not


async def test_custom_fields_as_the_mirror_sets_them(admin: PaperlessClient) -> None:
    """3.4.1 accepts the mirror's values (monetary `MAD1500.00`, ISO date, boolean), finds a
    custom field by `name__iexact`, and keeps a document's other custom fields when ours are
    merged in (a PATCH of `custom_fields` replaces the whole list)."""
    await admin.ensure_user("sijil-it-service", USER_PASSWORD, SERVICE_PERMISSIONS)
    token = await admin.obtain_token("sijil-it-service", USER_PASSWORD)
    async with PaperlessClient(URL, token=token) as svc:
        content = SAMPLE.read_bytes() + f"\n%sijil-it-{uuid.uuid4()}\n".encode()
        task_id = await svc.upload(content, SAMPLE.name)
        deadline = time.monotonic() + 120
        task = await svc.task(task_id)
        while task.status not in ("SUCCESS", "FAILURE") and time.monotonic() < deadline:
            await asyncio.sleep(2)
            task = await svc.task(task_id)
        assert task.status == "SUCCESS"
        assert task.related_document is not None
        doc_id = task.related_document

        theirs = await svc.ensure_custom_field("it-tenant-note", "string")
        await svc.set_custom_fields(doc_id, {theirs: "kept"})
        ttc = await svc.ensure_custom_field("sijil_total_ttc", "monetary")
        assert await svc.ensure_custom_field("SIJIL_TOTAL_TTC", "monetary") == ttc
        day = await svc.ensure_custom_field("sijil_date", "date")
        sealed = await svc.ensure_custom_field("sijil_sealed", "boolean")
        await svc.set_custom_fields(doc_id, {ttc: "MAD1500.00", day: "2026-09-30", sealed: True})

        document = await svc.get_document(doc_id)
        stored = {f["field"]: f["value"] for f in document["custom_fields"]}
        assert stored == {theirs: "kept", ttc: "MAD1500.00", day: "2026-09-30", sealed: True}
