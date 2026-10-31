"""The error envelope: `{"error": {"code": "<machine.code>"}}`. Never prose."""

from typing import cast

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from sijil_core.region import RegionMismatch


class ApiError(Exception):
    def __init__(self, code: str, status: int) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


def error_response(code: str, status: int, **extra: object) -> JSONResponse:
    return JSONResponse({"error": {"code": code, **extra}}, status_code=status)


async def _api_error(_: Request, exc: Exception) -> JSONResponse:
    exc = cast(ApiError, exc)
    return error_response(exc.code, exc.status)


async def _region_mismatch(_: Request, exc: Exception) -> JSONResponse:
    exc = cast(RegionMismatch, exc)
    return error_response(exc.code, 421)


async def _validation(_: Request, exc: Exception) -> JSONResponse:
    exc = cast(RequestValidationError, exc)
    fields = [".".join(str(part) for part in e["loc"]) for e in exc.errors()]
    return error_response("validation", 422, fields=fields)


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ApiError, _api_error)
    app.add_exception_handler(RegionMismatch, _region_mismatch)
    app.add_exception_handler(RequestValidationError, _validation)
