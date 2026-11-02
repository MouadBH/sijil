"""The error envelope: `{"error": {"code": "<machine.code>"}}`. Never prose."""

import logging
from typing import cast

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from sijil_core.region import RegionMismatch

logger = logging.getLogger(__name__)

_HTTP_CODES = {
    401: "auth.unauthenticated",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
}


class ApiError(Exception):
    """A machine code and its status. `body` keys sit next to `error` in the response, like
    `document_id` of `document.duplicate` (e.g. `{"error": {...}, "field": "total_ttc"}`)."""

    def __init__(self, code: str, status: int, body: dict[str, object] | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.status = status
        self.body = body or {}


def error_response(code: str, status: int, **extra: object) -> JSONResponse:
    return JSONResponse({"error": {"code": code, **extra}}, status_code=status)


async def _api_error(_: Request, exc: Exception) -> JSONResponse:
    exc = cast(ApiError, exc)
    if exc.body:
        return JSONResponse({"error": {"code": exc.code}, **exc.body}, status_code=exc.status)
    return error_response(exc.code, exc.status)


async def _region_mismatch(_: Request, exc: Exception) -> JSONResponse:
    exc = cast(RegionMismatch, exc)
    return error_response(exc.code, 421)


async def _validation(_: Request, exc: Exception) -> JSONResponse:
    exc = cast(RequestValidationError, exc)
    fields = [".".join(str(part) for part in e["loc"]) for e in exc.errors()]
    return error_response("validation", 422, fields=fields)


async def _http_error(_: Request, exc: Exception) -> JSONResponse:
    exc = cast(StarletteHTTPException, exc)
    code = _HTTP_CODES.get(exc.status_code, f"http.{exc.status_code}")
    response = error_response(code, exc.status_code)
    if exc.headers:
        response.headers.update(exc.headers)
    return response


async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
    logger.error("unhandled exception on %s %s", request.method, request.url.path, exc_info=exc)
    return error_response("internal", 500)


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ApiError, _api_error)
    app.add_exception_handler(RegionMismatch, _region_mismatch)
    app.add_exception_handler(RequestValidationError, _validation)
    app.add_exception_handler(StarletteHTTPException, _http_error)
    app.add_exception_handler(Exception, _unhandled)
