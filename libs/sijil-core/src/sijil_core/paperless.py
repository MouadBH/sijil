"""REST client for Paperless-ngx. The only way Sijil talks to Paperless (never its code or DB).

Errors never carry credentials or response bodies: only the method, path and status.
"""

import asyncio
import time
from collections.abc import Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

import httpx

_MODELS = ("document", "tag", "correspondent", "documenttype", "customfield", "note", "savedview")

# The service user ingests and edits, but can never delete: this is the Paperless-side layer of
# the retention lock. `view_paperlesstask` lets it poll upload tasks.
SERVICE_PERMISSIONS: tuple[str, ...] = (
    *(f"{verb}_{model}" for verb in ("view", "add", "change") for model in _MODELS),
    "view_paperlesstask",
)
# Support staff can look, never touch.
SUPPORT_PERMISSIONS: tuple[str, ...] = (
    *(f"view_{model}" for model in _MODELS),
    "view_paperlesstask",
)

_RETRY_STATUS = frozenset({502, 503, 504})
_READY_ERRORS = frozenset({401, 403, 406})
_ATTEMPTS = 3
_BACKOFF_S = 0.5


class PaperlessError(Exception):
    """A Paperless call failed. The message never includes credentials or bodies."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class PaperlessForbidden(PaperlessError):  # noqa: N818
    """Paperless answered 403."""


class PaperlessUnavailable(PaperlessError):  # noqa: N818
    """Paperless could not be reached, timed out or dropped the connection, or kept answering
    502/503/504. Transient: worth retrying the whole (idempotent) operation later."""


class PaperlessUnsupportedType(PaperlessError):  # noqa: N818
    """Paperless refused the upload because it cannot consume this file type (HTTP 400,
    `{"document": ["File type text/xml not supported"]}` in 3.4.1). Final, never retried."""


@dataclass(frozen=True)
class PaperlessTask:
    task_id: str
    status: str
    related_document: int | None
    # Paperless's own failure text for a FAILURE task (`result_data.error_message`). Used to
    # classify the failure; callers must not log it or store it.
    error: str | None = None
    # False when Paperless has no record of the task id (never seen, or purged).
    known: bool = True


def _related_document(row: dict[str, Any]) -> int | None:
    # 3.4.1: `related_document_ids` and `result_data.document_id`; older: `related_document`.
    ids = row.get("related_document_ids") or []
    if ids:
        return int(ids[0])
    result = row.get("result_data")
    if isinstance(result, dict) and result.get("document_id") is not None:
        return int(result["document_id"])
    legacy = row.get("related_document")
    return int(str(legacy).split(",")[0]) if legacy else None


def _error_message(row: dict[str, Any]) -> str | None:
    result = row.get("result_data")
    if isinstance(result, dict) and result.get("error_message"):
        return str(result["error_message"])
    legacy = row.get("result")  # older versions put the text in `result`
    return str(legacy) if isinstance(legacy, str) and legacy else None


def _is_unsupported_type(response: httpx.Response) -> bool:
    try:
        body = response.json()
    except ValueError:
        return False
    messages = body.get("document") if isinstance(body, dict) else None
    return isinstance(messages, list) and any("not supported" in str(m) for m in messages)


@dataclass(frozen=True)
class PaperlessDoc:
    id: int
    title: str | None
    created: str | None
    original_file_name: str | None


class PaperlessClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        basic: tuple[str, str] | None = None,
        timeout: float = 10.0,
    ) -> None:
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Token {token}"
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers=headers,
            auth=httpx.BasicAuth(*basic) if basic else None,
            timeout=timeout,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        where = f"{method} {path}"
        for attempt in range(_ATTEMPTS):
            last = attempt == _ATTEMPTS - 1
            try:
                response = await self._http.request(method, path, **kwargs)
            except (httpx.ConnectError, httpx.ConnectTimeout):
                # Nothing was sent, so any method may be tried again.
                if last:
                    raise PaperlessUnavailable(f"{where}: connection failed") from None
            except httpx.TransportError as exc:
                # Timeouts and broken connections after sending: the request may have been
                # applied, so no retry here; callers (e.g. the provisioning task) may retry.
                raise PaperlessUnavailable(f"{where}: {type(exc).__name__}") from None
            except httpx.HTTPError as exc:
                raise PaperlessError(f"{where}: {type(exc).__name__}") from None
            else:
                if response.status_code not in _RETRY_STATUS:
                    return self._checked(where, response)
                if last:
                    raise PaperlessUnavailable(f"{where}: HTTP {response.status_code}")
            await asyncio.sleep(_BACKOFF_S * 2**attempt)
        raise AssertionError("unreachable")  # pragma: no cover

    @staticmethod
    def _checked(where: str, response: httpx.Response) -> httpx.Response:
        status = response.status_code
        if status == 403:
            raise PaperlessForbidden(f"{where}: HTTP 403", 403)
        if status >= 400:
            raise PaperlessError(f"{where}: HTTP {status}", status)
        return response

    async def api_version(self) -> int:
        # Paperless 3.4.1 redirects /api/ to /api/schema/view/, which only carries the header
        # for authenticated callers.
        # The schema view is HTML, so a JSON-only Accept header gets a 406.
        response = await self._request(
            "GET", "/api/", follow_redirects=True, headers={"Accept": "*/*"}
        )
        try:
            return int(response.headers["X-Api-Version"])
        except (KeyError, ValueError):
            raise PaperlessError("GET /api/: missing X-Api-Version") from None

    async def wait_ready(self, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                await self._request("GET", "/api/")
            except PaperlessError as exc:
                # 401/403/406 prove the app is answering; 5xx (e.g. migrations running) and
                # transport errors (status None) mean not ready yet.
                if exc.status in _READY_ERRORS:
                    return
            else:
                return
            if time.monotonic() >= deadline:
                raise PaperlessUnavailable(f"not ready after {timeout_s:.0f}s")
            await asyncio.sleep(2)

    async def obtain_token(self, username: str, password: str) -> str:
        response = await self._request(
            "POST", "/api/token/", json={"username": username, "password": password}
        )
        return str(response.json()["token"])

    async def _find_user(self, username: str) -> dict[str, Any] | None:
        response = await self._request("GET", "/api/users/", params={"username__iexact": username})
        for row in response.json()["results"]:
            if str(row["username"]).lower() == username.lower():
                return dict(row)
        return None

    async def ensure_user(self, username: str, password: str, permissions: Sequence[str]) -> int:
        existing = await self._find_user(username)
        # Exactly these grants and nothing inherited: no groups, never staff/superuser, so an
        # existing user cannot keep a delete_* right from elsewhere.
        grants: dict[str, Any] = {
            "user_permissions": list(permissions),
            "groups": [],
            "is_superuser": False,
            "is_staff": False,
        }
        if existing is not None:
            user_id = int(existing["id"])
            await self._request(
                "PATCH",
                f"/api/users/{user_id}/",
                json={**grants, "password": password, "is_active": True},
            )
            return user_id
        response = await self._request(
            "POST", "/api/users/", json={**grants, "username": username, "password": password}
        )
        return int(response.json()["id"])

    async def user_permissions(self, username: str) -> set[str]:
        user = await self._find_user(username)
        if user is None:
            raise PaperlessError("user not found")
        return {str(p) for p in user["user_permissions"]}

    async def upload(self, content: bytes, filename: str, title: str | None = None) -> str:
        """Queue `content` for consumption and return the Paperless task id.

        Raises PaperlessUnsupportedType when Paperless refuses the file type outright. Retries
        only connection failures (nothing was sent); gateway errors surface as Unavailable.
        """
        where = "POST /api/documents/post_document/"
        data = {"title": title} if title else None
        for attempt in range(_ATTEMPTS):
            last = attempt == _ATTEMPTS - 1
            try:
                response = await self._http.post(
                    "/api/documents/post_document/",
                    files={"document": (filename, content, "application/octet-stream")},
                    data=data,
                )
            except (httpx.ConnectError, httpx.ConnectTimeout):
                if last:
                    raise PaperlessUnavailable(f"{where}: connection failed") from None
            except httpx.TransportError as exc:
                raise PaperlessUnavailable(f"{where}: {type(exc).__name__}") from None
            except httpx.HTTPError as exc:
                raise PaperlessError(f"{where}: {type(exc).__name__}") from None
            else:
                if response.status_code == 400 and _is_unsupported_type(response):
                    raise PaperlessUnsupportedType(f"{where}: HTTP 400", 400)
                if response.status_code in _RETRY_STATUS:
                    # Not retried here: the POST may already have been applied, so the caller
                    # resumes by task id / checksum instead of uploading again.
                    raise PaperlessUnavailable(f"{where}: HTTP {response.status_code}")
                return str(self._checked(where, response).json())
            await asyncio.sleep(_BACKOFF_S * 2**attempt)
        raise AssertionError("unreachable")  # pragma: no cover

    async def find_document_by_checksum(self, sha256: str) -> int | None:
        """Id of the oldest live document whose original has this SHA-256, or None.

        Paperless 3.4.1 keys documents by the SHA-256 of the original and does not refuse
        duplicates, so this is what makes ingest idempotent.
        """
        response = await self._request(
            "GET",
            "/api/documents/",
            params={"checksum__iexact": sha256, "ordering": "id", "page_size": 1},
        )
        results = response.json().get("results") or []
        return int(results[0]["id"]) if results else None

    async def task(self, task_id: str) -> PaperlessTask:
        response = await self._request("GET", "/api/tasks/", params={"task_id": task_id})
        rows = response.json()
        if isinstance(rows, dict):
            rows = rows.get("results", [])
        if not rows:
            return PaperlessTask(
                task_id=task_id, status="PENDING", related_document=None, known=False
            )
        row = rows[0]
        return PaperlessTask(
            task_id=task_id,
            status=str(row["status"]).upper(),  # 3.4.1 reports lowercase ("success")
            related_document=_related_document(row),
            error=_error_message(row),
        )

    async def get_document(self, doc_id: int) -> dict[str, Any]:
        response = await self._request("GET", f"/api/documents/{doc_id}/")
        return dict(response.json())

    async def documents_by_ids(self, ids: Sequence[int]) -> dict[int, PaperlessDoc]:
        """Titles and names of the given documents in one request (ids missing in Paperless
        are simply absent). A malformed answer surfaces as PaperlessError."""
        wanted = sorted(set(ids))
        if not wanted:
            return {}
        response = await self._request(
            "GET",
            "/api/documents/",
            params={
                "id__in": ",".join(str(i) for i in wanted),
                "page_size": len(wanted),
                "fields": "id,title,created,original_file_name",
            },
        )
        try:
            return {
                int(r["id"]): PaperlessDoc(
                    id=int(r["id"]),
                    title=r.get("title"),
                    created=r.get("created"),
                    original_file_name=r.get("original_file_name"),
                )
                for r in response.json()["results"]
            }
        except (ValueError, KeyError, TypeError, AttributeError):
            raise PaperlessError("GET /api/documents/: unexpected body") from None

    async def delete_document(self, doc_id: int) -> None:
        await self._request("DELETE", f"/api/documents/{doc_id}/")
