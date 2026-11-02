"""Client for the in-region Ollama: the only place document text is sent for AI.

The base URL's host must be on an allow-list (`SIJIL_AI_ALLOWED_HOSTS`, the compose `ollama`
service by default), checked when the client is built, and proxies from the environment are
ignored: document text never leaves for another host (CLAUDE.md, hard rule 3).

Errors carry a machine code and at most an HTTP status, never Ollama's text or the model's
output (which is document content). `OllamaBadOutput.raw` keeps the output for evidence in the
workspace's own index database; it must not be logged.
"""

import json
from collections.abc import Collection, Mapping
from types import TracebackType
from typing import Any, Final, Self

import httpx

NUM_CTX: Final = 8192
# The answer is ten short fields (a few hundred tokens). A run that never closes its JSON (seen
# live: one generating until the 300 s timeout) is cut here and parses as bad output, so it
# never holds the one model for the whole timeout.
NUM_PREDICT: Final = 1024
# What Ollama 0.40 answers (HTTP 500) when the model cannot be loaded: too little memory, or
# the runner killed (OOM) while loading.
_LOAD_FAILURE_MARKERS: Final = (
    "requires more system memory",
    "runner process has terminated",
    "signal: killed",
    "out of memory",
    "failed to load model",
    "unable to allocate",
)


class OllamaError(Exception):
    """An Ollama call failed for a reason retrying will not fix (e.g. the model is missing)."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class OllamaUnavailable(OllamaError):  # noqa: N818
    """Ollama could not be reached, timed out, or answered 5xx. Transient."""

    def __init__(
        self, message: str, status: int | None = None, *, model_load_failed: bool = False
    ) -> None:
        super().__init__(message, status)
        # The model could not be loaded (too little memory, runner killed).
        self.model_load_failed = model_load_failed


class OllamaBadOutput(OllamaError):  # noqa: N818
    """The model's answer is not JSON, or not an object with the schema's required keys."""

    def __init__(self, message: str, raw: str | None = None) -> None:
        super().__init__(message)
        self.raw = raw


def _host_allowed(base_url: str, allowed_hosts: Collection[str]) -> bool:
    try:
        url = httpx.URL(base_url)
    except httpx.InvalidURL:
        return False
    if url.scheme not in ("http", "https") or not url.host:
        return False
    return url.host.lower() in {h.strip().lower() for h in allowed_hosts}


def _load_failed(response: httpx.Response) -> bool:
    try:
        body = response.json()
    except ValueError:
        return False
    error = body.get("error") if isinstance(body, dict) else None
    return isinstance(error, str) and any(m in error.lower() for m in _LOAD_FAILURE_MARKERS)


def _model_missing(response: httpx.Response) -> bool:
    """Ollama's 404 for an unpulled model: `{"error": "model '<name>' not found"}`."""
    try:
        body = response.json()
    except ValueError:
        return False
    error = body.get("error") if isinstance(body, dict) else None
    return isinstance(error, str) and "model" in error.lower() and "not found" in error.lower()


class OllamaClient:
    def __init__(
        self,
        base_url: str,
        allowed_hosts: Collection[str],
        *,
        timeout_s: float = 300,  # one CPU-bound run measured ~130 s under load
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not _host_allowed(base_url, allowed_hosts):
            raise ValueError("ai.host_not_allowed")
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_s,
            transport=transport,
            trust_env=False,  # never an environment proxy: content stays on the allowed host
            follow_redirects=False,
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
        try:
            response = await self._http.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise OllamaUnavailable(f"ai.unavailable: {type(exc).__name__}") from None
        status = response.status_code
        if status >= 500:
            raise OllamaUnavailable(
                f"ai.unavailable: HTTP {status}", status, model_load_failed=_load_failed(response)
            )
        if status == 404 and _model_missing(response):
            # Pulled late, or a wiped volume: wait for `make models` like for a down server.
            raise OllamaUnavailable("ai.unavailable: model not found", status)
        if status >= 400:
            raise OllamaError(f"ai.error: HTTP {status}", status)
        return response

    async def has_model(self, model: str) -> bool:
        """Whether `model` is pulled (`/api/tags`)."""
        response = await self._request("GET", "/api/tags")
        try:
            models = response.json().get("models") or []
            names = {str(m.get("name")) for m in models} | {str(m.get("model")) for m in models}
        except (ValueError, AttributeError, TypeError):
            raise OllamaError("ai.bad_response: /api/tags") from None
        return model in names

    async def chat_json(
        self, model: str, system: str, user: str, schema: Mapping[str, Any], seed: int
    ) -> dict[str, Any]:
        """One deterministic structured-output chat. Returns the parsed JSON object.

        Raises OllamaUnavailable (transient), OllamaBadOutput (not JSON, not an object, or a
        required key of `schema` missing) or OllamaError (anything else, e.g. model missing).
        """
        response = await self._request(
            "POST",
            "/api/chat",
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "stream": False,
                "think": False,
                "format": dict(schema),
                "options": {
                    "temperature": 0,
                    "seed": seed,
                    "num_ctx": NUM_CTX,
                    "num_predict": NUM_PREDICT,
                },
            },
        )
        try:
            envelope = response.json()
            content = envelope["message"]["content"]
        except (ValueError, KeyError, TypeError):
            raise OllamaBadOutput("ai.bad_output: no message") from None
        if not isinstance(content, str):
            raise OllamaBadOutput("ai.bad_output: no message")
        try:
            parsed = json.loads(content)
        except ValueError:
            raise OllamaBadOutput("ai.bad_output: not JSON", raw=content) from None
        if not isinstance(parsed, dict):
            raise OllamaBadOutput("ai.bad_output: not an object", raw=content)
        missing = [k for k in schema.get("required", ()) if k not in parsed]
        if missing:
            raise OllamaBadOutput("ai.bad_output: required key missing", raw=content)
        return parsed
