"""The Ollama client: host allow-list, request shape, and how failures are classified."""

import json
from collections.abc import Callable

import httpx
import pytest

from sijil_core.ollama import OllamaBadOutput, OllamaClient, OllamaError, OllamaUnavailable

SCHEMA = {
    "type": "object",
    "properties": {"number": {"type": ["string", "null"]}},
    "required": ["number"],
}


def client(handler: Callable[[httpx.Request], httpx.Response]) -> OllamaClient:
    return OllamaClient("http://ollama:11434", {"ollama"}, transport=httpx.MockTransport(handler))


def answer(content: str, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json={"message": {"role": "assistant", "content": content}})


@pytest.mark.parametrize(
    "url",
    [
        "https://api.example.com",
        "http://ollama.example.com:11434",
        "http://10.0.0.1:11434",
        "http://user@evil:11434",
        "not a url",
    ],
)
def test_host_not_allowed_is_refused_at_construction(url: str) -> None:
    with pytest.raises(ValueError, match=r"^ai\.host_not_allowed$"):
        OllamaClient(url, {"ollama"})


def test_allowed_host_is_case_insensitive() -> None:
    OllamaClient("http://OLLAMA:11434", {"ollama"})


async def test_chat_json_request_shape() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return answer('{"number": "F-1"}')

    async with client(handler) as ai:
        result = await ai.chat_json("qwen3:4b", "sys", "user text", SCHEMA, seed=2)
    assert result == {"number": "F-1"}
    (request,) = seen
    assert request.url.host == "ollama"
    assert request.url.path == "/api/chat"
    body = json.loads(request.content)
    assert body == {
        "model": "qwen3:4b",
        "messages": [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "user text"},
        ],
        "stream": False,
        "think": False,
        "format": SCHEMA,
        "options": {"temperature": 0, "seed": 2, "num_ctx": 8192},
    }


@pytest.mark.parametrize("status", [500, 502, 503, 504])
async def test_server_errors_are_unavailable(status: int) -> None:
    async with client(lambda _r: httpx.Response(status, json={"error": "x"})) as ai:
        with pytest.raises(OllamaUnavailable):
            await ai.chat_json("m", "s", "u", SCHEMA, seed=1)


async def test_connect_error_and_timeout_are_unavailable() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    def slow(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    for handler in (refuse, slow):
        async with client(handler) as ai:
            with pytest.raises(OllamaUnavailable):
                await ai.chat_json("m", "s", "u", SCHEMA, seed=1)


async def test_model_load_failure_is_recognised() -> None:
    def oom(_r: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500, json={"error": "model requires more system memory (5.1 GiB) than is available"}
        )

    async with client(oom) as ai:
        with pytest.raises(OllamaUnavailable) as exc:
            await ai.chat_json("m", "s", "u", SCHEMA, seed=1)
    assert exc.value.model_load_failed
    # The message is a machine code and status only, never Ollama's text.
    assert "memory" not in str(exc.value)


async def test_missing_model_is_a_final_error() -> None:
    async with client(lambda _r: httpx.Response(404, json={"error": "model not found"})) as ai:
        with pytest.raises(OllamaError) as exc:
            await ai.chat_json("m", "s", "u", SCHEMA, seed=1)
    assert not isinstance(exc.value, OllamaUnavailable | OllamaBadOutput)


@pytest.mark.parametrize(
    "content",
    ["not json", "[1, 2]", '"text"', "{}", '{"other": 1}'],
)
async def test_unparseable_or_mismatched_output_is_bad_output(content: str) -> None:
    async with client(lambda _r: answer(content)) as ai:
        with pytest.raises(OllamaBadOutput) as exc:
            await ai.chat_json("m", "s", "u", SCHEMA, seed=1)
    assert exc.value.raw == content


async def test_envelope_without_message_is_bad_output() -> None:
    async with client(lambda _r: httpx.Response(200, json={"done": True})) as ai:
        with pytest.raises(OllamaBadOutput):
            await ai.chat_json("m", "s", "u", SCHEMA, seed=1)


async def test_has_model_reads_tags() -> None:
    def tags(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/tags"
        return httpx.Response(200, json={"models": [{"name": "qwen3:4b"}, {"name": "x:1"}]})

    async with client(tags) as ai:
        assert await ai.has_model("qwen3:4b")
        assert not await ai.has_model("qwen3:8b")


def test_environment_proxies_are_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    """A proxy from the environment would send document text to another host."""
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example.com:3128")
    monkeypatch.setenv("ALL_PROXY", "http://proxy.example.com:3128")
    ai = OllamaClient("http://ollama:11434", {"ollama"})
    assert ai._http._trust_env is False
