"""A tenant Paperless as the purge meets it, for unit tests (respx) of the purge path.

It behaves as 3.4.1 does, which tests/integration/test_paperless_contract.py pins against the
real one: a delete moves a live document to the trash (404 when it is not live), emptying the
trash answers 400 unless every document named is in it, the checksum lookup sees live documents
only, lists are paginated. Every request is recorded with the credential it carried; after the
token request only the tenant admin's token is accepted (403 otherwise).
"""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import httpx
import respx

ADMIN_USERNAME = "sijil-admin"


class FakePaperless:
    def __init__(self, admin_password: str, admin_token: str) -> None:
        self.admin_password = admin_password
        self.admin_token = admin_token
        self.live: dict[int, tuple[str, str]] = {}  # id -> (checksum, original file name)
        self.trash: dict[int, tuple[str, str]] = {}
        self.tasks: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str, str | None, Any]] = []
        self.keep_in_trash = False  # an `empty` that answers 200 but keeps them
        self.down = False
        # A Paperless that ignores the checksum filter (an undeclared one, as 3.4.1 does with a
        # bare `checksum=`) and lists every live document.
        self.ignore_checksum_filter = False
        # Whether list rows (documents and trash) carry `checksum`; `original_file_name` always.
        self.lists_checksum = True

    def add(
        self, doc_id: int, sha256: str, *, name: str | None = None, trashed: bool = False
    ) -> None:
        """A document; its file name is ingest's `{sha256[:12]}.pdf` unless named otherwise."""
        entry = (sha256, name or f"{sha256[:12]}.pdf")
        (self.trash if trashed else self.live)[doc_id] = entry

    def methods(self) -> list[tuple[str, str]]:
        return [(method, path) for method, path, _, _ in self.calls]

    def _row(self, doc_id: int, entry: tuple[str, str]) -> dict[str, Any]:
        row: dict[str, Any] = {"id": doc_id, "original_file_name": entry[1]}
        if self.lists_checksum:
            row["checksum"] = entry[0]
        return row

    @staticmethod
    def _page(request: httpx.Request, rows: list[dict[str, Any]]) -> httpx.Response:
        size = int(request.url.params.get("page_size", 25))
        page = int(request.url.params.get("page", 1))
        chunk = rows[(page - 1) * size : page * size]
        more = page * size < len(rows)
        return httpx.Response(
            200,
            json={"count": len(rows), "next": f"page={page + 1}" if more else None,
                  "results": chunk},
        )  # fmt: skip

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else None
        auth = request.headers.get("authorization")
        self.calls.append((request.method, path, auth, body))
        if path == "/api/token/":
            ok = body == {"username": ADMIN_USERNAME, "password": self.admin_password}
            return httpx.Response(200 if ok else 400, json={"token": self.admin_token})
        if self.down:
            return httpx.Response(503)
        if auth != f"Token {self.admin_token}":
            return httpx.Response(403)
        if request.method == "GET" and path == "/api/documents/":
            wanted = request.url.params["checksum__iexact"].lower()
            rows = [
                self._row(i, entry)
                for i, entry in sorted(self.live.items())
                if self.ignore_checksum_filter or entry[0] == wanted
            ]
            return self._page(request, rows)
        if request.method == "DELETE" and path.startswith("/api/documents/"):
            doc_id = int(path.split("/")[3])
            if doc_id not in self.live:
                return httpx.Response(404)
            self.trash[doc_id] = self.live.pop(doc_id)
            return httpx.Response(204)
        if request.method == "GET" and path == "/api/trash/":
            trashed = [self._row(i, entry) for i, entry in sorted(self.trash.items())]
            return self._page(request, trashed)
        if request.method == "POST" and path == "/api/trash/":
            assert body is not None
            ids = body["documents"]
            if body["action"] != "empty" or any(i not in self.trash for i in ids):
                return httpx.Response(400, json={"documents": ["not yet deleted"]})
            if not self.keep_in_trash:
                for i in ids:
                    del self.trash[i]
            return httpx.Response(200, json={"result": "OK"})
        if request.method == "GET" and path == "/api/tasks/":
            task = self.tasks.get(request.url.params["task_id"])
            return httpx.Response(200, json=[task] if task else [])
        return httpx.Response(404)


@contextmanager
def fake_paperless(url: str, admin_password: str, admin_token: str) -> Iterator[FakePaperless]:
    """Every request to `url` answered by a FakePaperless (any other host is refused)."""
    found = FakePaperless(admin_password, admin_token)
    with respx.mock(assert_all_called=False) as router:
        router.route(url__startswith=url).mock(side_effect=found.handler)
        yield found
