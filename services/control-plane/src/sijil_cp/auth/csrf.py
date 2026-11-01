"""CSRF double-submit: cookie `sijil_csrf` must equal header `X-CSRF-Token` on unsafe methods."""

import hmac
import secrets

from fastapi import APIRouter, Request, Response
from starlette.datastructures import Headers
from starlette.requests import HTTPConnection
from starlette.types import ASGIApp, Receive, Scope, Send

from sijil_cp.errors import error_response

COOKIE = "sijil_csrf"
HEADER = "X-CSRF-Token"
_UNSAFE = {"POST", "PUT", "PATCH", "DELETE"}

router = APIRouter(prefix="/auth")


def tokens_match(cookie: str | None, header: str | None) -> bool:
    if not cookie or not header:
        return False
    return hmac.compare_digest(cookie.encode(), header.encode())


class CsrfMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] == "http"
            and scope["method"] in _UNSAFE
            and scope["path"].startswith("/v1/")
        ):
            cookie = HTTPConnection(scope).cookies.get(COOKIE)
            header = Headers(scope=scope).get(HEADER)
            if not tokens_match(cookie, header):
                response = error_response("auth.csrf", 403)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


@router.get("/csrf")
async def issue_csrf(request: Request, response: Response) -> dict[str, str]:
    token = secrets.token_urlsafe(32)
    response.set_cookie(
        COOKIE,
        token,
        httponly=False,
        samesite="lax",
        secure=request.app.state.settings.cookie_secure,
        path="/",
    )
    return {"csrf": token}
