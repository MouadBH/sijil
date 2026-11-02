import asyncio
import json

import httpx
import pytest
import respx

from sijil_core.paperless import (
    SERVICE_PERMISSIONS,
    SUPPORT_PERMISSIONS,
    PaperlessBadResponse,
    PaperlessClient,
    PaperlessDoc,
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


@respx.mock
async def test_documents_by_ids_maps_titles() -> None:
    route = respx.get(f"{BASE}/api/documents/").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": 4,
                        "title": "Facture",
                        "created": "2026-10-01",
                        "original_file_name": "f.pdf",
                        "content": "x",
                    }
                ]
            },
        )
    )
    async with PaperlessClient(BASE, token="t") as client:
        docs = await client.documents_by_ids([4, 9])
        assert await client.documents_by_ids([]) == {}
    assert set(docs) == {4}
    assert docs[4] == PaperlessDoc(4, "Facture", "2026-10-01", "f.pdf")
    params = route.calls.last.request.url.params
    assert params["id__in"] == "4,9"
    assert params["page_size"] == "2"
    assert route.call_count == 1


@respx.mock
async def test_documents_by_ids_bad_body_is_paperless_error() -> None:
    respx.get(f"{BASE}/api/documents/").mock(return_value=httpx.Response(200, json={"x": 1}))
    async with PaperlessClient(BASE, token="t") as client:
        with pytest.raises(PaperlessError):
            await client.documents_by_ids([1])


@respx.mock
async def test_500_raises_unavailable() -> None:
    route = respx.get(f"{BASE}/api/documents/").mock(return_value=httpx.Response(500))
    async with PaperlessClient(BASE, token="t") as client:
        with pytest.raises(PaperlessUnavailable) as exc:
            await client._request("GET", "/api/documents/")
    assert exc.value.status == 500
    assert route.call_count == 1  # 500 is mapped, not retried inside the client


# M3: content, suggestions, document types, title/type ------------------------------------


@respx.mock
async def test_document_content_reads_only_the_content_field() -> None:
    route = respx.get(f"{BASE}/api/documents/7/").mock(
        return_value=httpx.Response(200, json={"id": 7, "content": "FACTURE N° F-1"})
    )
    async with PaperlessClient(BASE, token="t") as client:
        assert await client.document_content(7) == "FACTURE N° F-1"
    assert route.calls.last.request.url.params["fields"] == "id,content"


@respx.mock
async def test_document_content_absent_is_empty() -> None:
    respx.get(f"{BASE}/api/documents/7/").mock(
        return_value=httpx.Response(200, json={"id": 7, "content": None})
    )
    async with PaperlessClient(BASE, token="t") as client:
        assert await client.document_content(7) == ""


@respx.mock
async def test_document_content_bad_body_is_bad_response() -> None:
    respx.get(f"{BASE}/api/documents/7/").mock(return_value=httpx.Response(200, json=[1]))
    async with PaperlessClient(BASE, token="t") as client:
        with pytest.raises(PaperlessBadResponse):
            await client.document_content(7)


@respx.mock
async def test_document_suggestions_keeps_id_lists_only() -> None:
    respx.get(f"{BASE}/api/documents/7/suggestions/").mock(
        return_value=httpx.Response(
            200,
            json={
                "correspondents": [],
                "tags": [3],
                "document_types": [5, 2],
                "storage_paths": [],
                "dates": ["2026-09-30"],
            },
        )
    )
    async with PaperlessClient(BASE, token="t") as client:
        suggestions = await client.document_suggestions(7)
    assert suggestions["document_types"] == [5, 2]
    assert suggestions["tags"] == [3]
    assert "dates" not in suggestions


@respx.mock
async def test_ensure_document_type_creates_when_missing() -> None:
    respx.get(f"{BASE}/api/document_types/").mock(
        return_value=httpx.Response(200, json={"results": []})
    )
    post = respx.post(f"{BASE}/api/document_types/").mock(
        return_value=httpx.Response(201, json={"id": 11})
    )
    async with PaperlessClient(BASE, token="t") as client:
        assert await client.ensure_document_type("invoice", r"\bfacture\b", 4) == 11
    assert json.loads(post.calls.last.request.content) == {
        "name": "invoice",
        "match": r"\bfacture\b",
        "matching_algorithm": 4,
        "is_insensitive": True,
    }


def _type_row(**row: object) -> httpx.Response:
    return httpx.Response(200, json={"results": [{"is_insensitive": True, **row}]})


@respx.mock
async def test_ensure_document_type_reuses_by_name_and_never_changes_its_rule() -> None:
    """A tenant admin may have edited the rule: an existing type is reused as it is."""
    respx.get(f"{BASE}/api/document_types/").mock(
        return_value=_type_row(id=4, name="Invoice", match="", matching_algorithm=0)
    )
    patch = respx.patch(f"{BASE}/api/document_types/4/")
    post = respx.post(f"{BASE}/api/document_types/")
    async with PaperlessClient(BASE, token="t") as client:
        assert await client.ensure_document_type("invoice", "x", 4) == 4
    assert not post.called
    assert not patch.called


@respx.mock
async def test_ensure_document_type_lost_create_race_returns_the_winner() -> None:
    """Two first extractions: the other one created the type between our GET and POST."""
    respx.get(f"{BASE}/api/document_types/").mock(
        side_effect=[
            httpx.Response(200, json={"results": []}),
            _type_row(id=9, name="invoice", match="x", matching_algorithm=4),
        ]
    )
    respx.post(f"{BASE}/api/document_types/").mock(
        return_value=httpx.Response(400, json={"name": ["already exists"]})
    )
    async with PaperlessClient(BASE, token="t") as client:
        assert await client.ensure_document_type("invoice", "x", 4) == 9


@respx.mock
async def test_ensure_document_type_create_refused_and_still_absent_raises() -> None:
    respx.get(f"{BASE}/api/document_types/").mock(
        return_value=httpx.Response(200, json={"results": []})
    )
    respx.post(f"{BASE}/api/document_types/").mock(return_value=httpx.Response(400, json={}))
    async with PaperlessClient(BASE, token="t") as client:
        with pytest.raises(PaperlessError):
            await client.ensure_document_type("invoice", "x", 4)


@respx.mock
async def test_ensure_document_type_create_unavailable_is_not_swallowed() -> None:
    listing = respx.get(f"{BASE}/api/document_types/").mock(
        return_value=httpx.Response(200, json={"results": []})
    )
    respx.post(f"{BASE}/api/document_types/").mock(return_value=httpx.Response(500))
    async with PaperlessClient(BASE, token="t") as client:
        with pytest.raises(PaperlessUnavailable):
            await client.ensure_document_type("invoice", "x", 4)
    assert listing.call_count == 1


def test_environment_proxies_are_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    """Document content must never go through a proxy taken from the environment."""
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example.com:3128")
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example.com:3128")
    client = PaperlessClient(BASE, token="t")
    assert client._http._trust_env is False
    assert not client._http._mounts


@respx.mock
async def test_set_title_and_type_patches_the_document() -> None:
    patch = respx.patch(f"{BASE}/api/documents/7/").mock(
        return_value=httpx.Response(200, json={"id": 7})
    )
    async with PaperlessClient(BASE, token="t") as client:
        await client.set_title_and_type(7, "2026-09-30 Atlas F-1", 11)
    assert json.loads(patch.calls.last.request.content) == {
        "title": "2026-09-30 Atlas F-1",
        "document_type": 11,
    }


@respx.mock
async def test_download_archive_or_original() -> None:
    route = respx.get(f"{BASE}/api/documents/7/download/").mock(
        return_value=httpx.Response(200, content=b"%PDF-1.7")
    )
    async with PaperlessClient(BASE, token="t") as client:
        assert await client.download(7) == b"%PDF-1.7"
        assert "original" not in route.calls.last.request.url.params
        await client.download(7, original=True)
        assert route.calls.last.request.url.params["original"] == "true"
