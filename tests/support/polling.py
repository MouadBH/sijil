"""Polling the control plane from the integration tests.

A test polls with one `httpx.AsyncClient`, whose pool reuses a keep-alive connection; uvicorn
closes an idle one after its keep-alive timeout, and a request sent on it just as it closes
fails with a transport error (`httpx.ReadError`) though the control plane is fine. A poll is a
GET, safe to send again: `poll_get` retries it once, on a new connection (httpx drops the
broken one from its pool). A second failure is a real one and is raised.
"""

from typing import Any

import httpx


async def poll_get(http: httpx.AsyncClient, url: str, **kwargs: Any) -> httpx.Response:
    try:
        return await http.get(url, **kwargs)
    except httpx.TransportError:
        return await http.get(url, **kwargs)
