import asyncio
import json

import httpx
import pytest
import respx

from sijil_core.paperless import (
    SERVICE_PERMISSIONS,
    SUPPORT_PERMISSIONS,
    PaperlessClient,
    PaperlessError,
    PaperlessForbidden,
    PaperlessUnavailable,
)

BASE = "http://paperless.test"


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    async def instant(_: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", instant)


@respx.mock
async def test_obtain_token_posts_credentials() -> None:
    route = respx.post(f"{BASE}/api/token/").mock(
        return_value=httpx.Response(200, json={"token": "tok-1"})
    )
    async with PaperlessClient(BASE) as client:
        assert await client.obtain_token("svc", "pw-secret") == "tok-1"
    assert json.loads(route.calls.last.request.content) == {
        "username": "svc",
        "password": "pw-secret",
    }


@respx.mock
async def test_ensure_user_is_idempotent() -> None:
    respx.get(f"{BASE}/api/users/").mock(
        return_value=httpx.Response(200, json={"results": [{"id": 7, "username": "svc"}]})
    )
    patch = respx.patch(f"{BASE}/api/users/7/").mock(
        return_value=httpx.Response(200, json={"id": 7})
    )
    post = respx.post(f"{BASE}/api/users/").mock(return_value=httpx.Response(201, json={"id": 9}))
    async with PaperlessClient(BASE, basic=("a", "b")) as client:
        assert await client.ensure_user("svc", "pw", ["view_document"]) == 7
    assert patch.called
    assert not post.called
    body = json.loads(patch.calls.last.request.content)
    assert body["user_permissions"] == ["view_document"]
    assert body["password"] == "pw"


@respx.mock
async def test_ensure_user_creates_when_missing() -> None:
    respx.get(f"{BASE}/api/users/").mock(return_value=httpx.Response(200, json={"results": []}))
    post = respx.post(f"{BASE}/api/users/").mock(return_value=httpx.Response(201, json={"id": 9}))
    async with PaperlessClient(BASE, basic=("a", "b")) as client:
        assert await client.ensure_user("svc", "pw", ["view_document"]) == 9
    assert json.loads(post.calls.last.request.content)["username"] == "svc"


@respx.mock
async def test_delete_403_raises_forbidden() -> None:
    respx.delete(f"{BASE}/api/documents/3/").mock(return_value=httpx.Response(403))
    async with PaperlessClient(BASE, token="t") as client:
        with pytest.raises(PaperlessForbidden):
            await client.delete_document(3)


@respx.mock
async def test_retries_on_503_then_succeeds() -> None:
    route = respx.get(f"{BASE}/api/").mock(
        side_effect=[
            httpx.Response(503),
            httpx.ConnectError("down"),
            httpx.Response(200, json={}, headers={"X-Api-Version": "9"}),
        ]
    )
    async with PaperlessClient(BASE, token="t") as client:
        assert await client.api_version() == 9
    assert route.call_count == 3


@respx.mock
async def test_retries_give_up_as_unavailable() -> None:
    route = respx.get(f"{BASE}/api/").mock(return_value=httpx.Response(502))
    async with PaperlessClient(BASE, token="t") as client:
        with pytest.raises(PaperlessUnavailable):
            await client.api_version()
    assert route.call_count == 3


@respx.mock
async def test_no_retry_on_400() -> None:
    route = respx.post(f"{BASE}/api/token/").mock(return_value=httpx.Response(400, json={}))
    async with PaperlessClient(BASE) as client:
        with pytest.raises(PaperlessError) as exc:
            await client.obtain_token("svc", "pw-secret")
    assert route.call_count == 1
    assert "pw-secret" not in str(exc.value)


def test_service_permissions_have_no_delete() -> None:
    assert SERVICE_PERMISSIONS
    assert not [p for p in SERVICE_PERMISSIONS if p.startswith("delete_")]
    assert "view_paperlesstask" in SERVICE_PERMISSIONS
    assert "add_document" in SERVICE_PERMISSIONS


def test_support_permissions_view_only() -> None:
    assert SUPPORT_PERMISSIONS
    assert all(p.startswith("view_") for p in SUPPORT_PERMISSIONS)


@respx.mock
async def test_wait_ready_keeps_polling_through_500() -> None:
    route = respx.get(f"{BASE}/api/").mock(
        side_effect=[httpx.Response(500), httpx.Response(500), httpx.Response(302)]
    )
    async with PaperlessClient(BASE) as client:
        await client.wait_ready(60)
    assert route.call_count == 3


@respx.mock
async def test_wait_ready_keeps_polling_through_read_timeout() -> None:
    route = respx.get(f"{BASE}/api/").mock(
        side_effect=[httpx.ReadTimeout("slow"), httpx.Response(401)]
    )
    async with PaperlessClient(BASE) as client:
        await client.wait_ready(60)
    assert route.call_count == 2


@respx.mock
async def test_wait_ready_gives_up_on_persistent_500(monkeypatch: pytest.MonkeyPatch) -> None:
    respx.get(f"{BASE}/api/").mock(return_value=httpx.Response(500))
    clock = iter(range(0, 1000, 5))
    monkeypatch.setattr("sijil_core.paperless.time.monotonic", lambda: float(next(clock)))
    async with PaperlessClient(BASE) as client:
        with pytest.raises(PaperlessUnavailable):
            await client.wait_ready(12)


@respx.mock
async def test_ensure_user_strips_groups_and_superuser() -> None:
    respx.get(f"{BASE}/api/users/").mock(
        return_value=httpx.Response(200, json={"results": [{"id": 7, "username": "svc"}]})
    )
    patch = respx.patch(f"{BASE}/api/users/7/").mock(return_value=httpx.Response(200, json={}))
    async with PaperlessClient(BASE, basic=("a", "b")) as client:
        await client.ensure_user("svc", "pw", ["view_document"])
    body = json.loads(patch.calls.last.request.content)
    assert body["groups"] == []
    assert body["is_superuser"] is False
    assert body["is_staff"] is False


@respx.mock
async def test_ensure_user_create_has_no_extra_grants() -> None:
    respx.get(f"{BASE}/api/users/").mock(return_value=httpx.Response(200, json={"results": []}))
    post = respx.post(f"{BASE}/api/users/").mock(return_value=httpx.Response(201, json={"id": 9}))
    async with PaperlessClient(BASE, basic=("a", "b")) as client:
        await client.ensure_user("svc", "pw", ["view_document"])
    body = json.loads(post.calls.last.request.content)
    assert body["groups"] == []
    assert body["is_superuser"] is False
    assert body["is_staff"] is False


@pytest.mark.parametrize(
    "error",
    [
        httpx.ReadTimeout("slow"),
        httpx.WriteTimeout("slow"),
        httpx.RemoteProtocolError("hung up"),
        httpx.ReadError("reset"),
    ],
)
@respx.mock
async def test_transport_errors_are_unavailable_and_post_is_sent_once(
    error: httpx.TransportError,
) -> None:
    """The request may have reached Paperless: no in-client retry, but retryable upstream."""
    route = respx.post(f"{BASE}/api/token/").mock(side_effect=error)
    async with PaperlessClient(BASE) as client:
        with pytest.raises(PaperlessUnavailable) as exc:
            await client.obtain_token("svc", "pw-secret")
    assert route.call_count == 1
    assert "pw-secret" not in str(exc.value)


@pytest.mark.parametrize("status", [502, 503, 504])
@respx.mock
async def test_upload_is_never_retried_on_gateway_errors(status: int) -> None:
    route = respx.post(f"{BASE}/api/documents/post_document/").mock(
        return_value=httpx.Response(status)
    )
    async with PaperlessClient(BASE, token="t") as client:
        with pytest.raises(PaperlessUnavailable):
            await client.upload(b"%PDF-1", "a.pdf")
    assert route.call_count == 1  # the POST may have been applied: the caller resumes safely


@respx.mock
async def test_upload_retries_only_when_the_request_was_never_sent() -> None:
    route = respx.post(f"{BASE}/api/documents/post_document/").mock(
        side_effect=[httpx.ConnectError("refused"), httpx.Response(200, json="task-1")]
    )
    async with PaperlessClient(BASE, token="t") as client:
        assert await client.upload(b"%PDF-1", "a.pdf") == "task-1"
    assert route.call_count == 2
