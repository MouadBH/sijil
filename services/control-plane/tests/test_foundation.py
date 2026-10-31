from collections.abc import Callable

import httpx
import pytest
from fastapi import FastAPI
from pydantic import BaseModel

from sijil_core.region import RegionMismatch
from sijil_cp.errors import ApiError


class Body(BaseModel):
    email: str
    age: int


@pytest.fixture
def app(make_app: Callable[..., FastAPI]) -> FastAPI:
    app = make_app()

    @app.get("/v1/_t/api-error")
    async def api_error() -> None:
        raise ApiError("x.y", 409)

    @app.get("/v1/_t/region")
    async def region() -> None:
        raise RegionMismatch("ma-cas", "fr-par")

    @app.post("/v1/_t/echo")
    async def echo(body: Body) -> dict[str, str]:
        return {"email": body.email}

    return app


async def test_post_without_csrf_rejected(
    app: FastAPI, client_for: Callable[[FastAPI], httpx.AsyncClient]
) -> None:
    async with client_for(app) as c:
        r = await c.post("/v1/_t/echo", json={"email": "a", "age": 1})
    assert r.status_code == 403
    assert r.json() == {"error": {"code": "auth.csrf"}}


async def test_post_with_mismatched_csrf_rejected(
    app: FastAPI, client_for: Callable[[FastAPI], httpx.AsyncClient]
) -> None:
    async with client_for(app) as c:
        await c.get("/v1/auth/csrf")
        r = await c.post(
            "/v1/_t/echo", json={"email": "a", "age": 1}, headers={"X-CSRF-Token": "nope"}
        )
    assert r.status_code == 403
    assert r.json() == {"error": {"code": "auth.csrf"}}


async def test_header_without_cookie_rejected(
    app: FastAPI, client_for: Callable[[FastAPI], httpx.AsyncClient]
) -> None:
    async with client_for(app) as c:
        r = await c.post(
            "/v1/_t/echo", json={"email": "a", "age": 1}, headers={"X-CSRF-Token": "x"}
        )
    assert r.status_code == 403


async def test_post_with_matching_csrf_accepted(
    app: FastAPI, client_for: Callable[[FastAPI], httpx.AsyncClient]
) -> None:
    async with client_for(app) as c:
        token = (await c.get("/v1/auth/csrf")).json()["csrf"]
        r = await c.post(
            "/v1/_t/echo", json={"email": "a", "age": 1}, headers={"X-CSRF-Token": token}
        )
    assert r.status_code == 200


async def test_csrf_cookie_flags(
    make_app: Callable[..., FastAPI], client_for: Callable[[FastAPI], httpx.AsyncClient]
) -> None:
    async with client_for(make_app(cookie_secure="true")) as c:
        r = await c.get("/v1/auth/csrf")
    header = r.headers["set-cookie"]
    assert header.startswith(f"sijil_csrf={r.json()['csrf']}")
    assert "samesite=lax" in header.lower()
    assert "secure" in header.lower()
    assert "httponly" not in header.lower()
    assert len(r.json()["csrf"]) >= 43  # 32 bytes, base64url


async def test_csrf_cookie_not_secure_when_disabled(
    make_app: Callable[..., FastAPI], client_for: Callable[[FastAPI], httpx.AsyncClient]
) -> None:
    async with client_for(make_app(cookie_secure="false")) as c:
        r = await c.get("/v1/auth/csrf")
    assert "secure" not in r.headers["set-cookie"].lower()


async def test_api_error_shape(
    app: FastAPI, client_for: Callable[[FastAPI], httpx.AsyncClient]
) -> None:
    async with client_for(app) as c:
        r = await c.get("/v1/_t/api-error")
    assert r.status_code == 409
    assert r.json() == {"error": {"code": "x.y"}}


async def test_validation_error_has_code_not_prose(
    app: FastAPI, client_for: Callable[[FastAPI], httpx.AsyncClient]
) -> None:
    async with client_for(app) as c:
        token = (await c.get("/v1/auth/csrf")).json()["csrf"]
        r = await c.post("/v1/_t/echo", json={"age": "x"}, headers={"X-CSRF-Token": token})
    assert r.status_code == 422
    body = r.json()
    assert set(body["error"]) == {"code", "fields"}
    assert body["error"]["code"] == "validation"
    assert set(body["error"]["fields"]) == {"body.email", "body.age"}


async def test_region_mismatch_maps_to_421(
    app: FastAPI, client_for: Callable[[FastAPI], httpx.AsyncClient]
) -> None:
    async with client_for(app) as c:
        r = await c.get("/v1/_t/region")
    assert r.status_code == 421
    assert r.json() == {"error": {"code": "region.mismatch"}}


async def test_unknown_route_uses_envelope(
    app: FastAPI, client_for: Callable[[FastAPI], httpx.AsyncClient]
) -> None:
    async with client_for(app) as c:
        r = await c.get("/v1/does-not-exist")
    assert r.status_code == 404
    assert r.json() == {"error": {"code": "not_found"}}


async def test_wrong_method_uses_envelope_and_keeps_allow(
    app: FastAPI, client_for: Callable[[FastAPI], httpx.AsyncClient]
) -> None:
    async with client_for(app) as c:
        c.cookies.set("sijil_csrf", "x")
        r = await c.put("/v1/health", headers={"X-CSRF-Token": "x"})
    assert r.status_code == 405
    assert r.json() == {"error": {"code": "method_not_allowed"}}
    assert "GET" in r.headers["allow"]


async def test_unhandled_exception_is_internal_without_detail(app: FastAPI) -> None:
    @app.get("/v1/_t/boom")
    async def boom() -> None:
        raise RuntimeError("secret-detail")

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://cp") as c:
        r = await c.get("/v1/_t/boom")
    assert r.status_code == 500
    assert r.json() == {"error": {"code": "internal"}}
    assert "secret-detail" not in r.text


@pytest.mark.db
async def test_client_fixture_serves_ready_on_real_db_and_redis(client: httpx.AsyncClient) -> None:
    r = await client.get("/v1/health/ready")
    assert r.status_code == 200
