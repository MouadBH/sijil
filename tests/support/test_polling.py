"""`poll_get`: a GET that survives the keep-alive race once, and only once."""

import httpx
import pytest

from tests.support.polling import poll_get


def _client(failures: int, calls: list[str]) -> httpx.AsyncClient:
    """A client whose first `failures` requests fail as a pooled connection the server just
    closed does (`httpx.ReadError`); the next ones answer 200."""

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if len(calls) <= failures:
            raise httpx.ReadError("connection closed by the peer", request=request)
        return httpx.Response(200, json={"state": "active"})

    return httpx.AsyncClient(base_url="http://cp", transport=httpx.MockTransport(handler))


async def test_a_transport_error_is_retried_once() -> None:
    calls: list[str] = []
    async with _client(1, calls) as http:
        r = await poll_get(http, "/v1/workspaces/w")
    assert r.json() == {"state": "active"}
    assert calls == ["/v1/workspaces/w", "/v1/workspaces/w"]


async def test_a_second_transport_error_is_raised() -> None:
    calls: list[str] = []
    async with _client(2, calls) as http:
        with pytest.raises(httpx.ReadError):
            await poll_get(http, "/v1/workspaces/w")
    assert len(calls) == 2


async def test_an_answer_is_returned_as_is() -> None:
    calls: list[str] = []
    async with _client(0, calls) as http:
        r = await poll_get(http, "/v1/workspaces/w")
    assert r.status_code == 200
    assert calls == ["/v1/workspaces/w"]
