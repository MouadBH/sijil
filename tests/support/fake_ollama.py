"""The in-region Ollama with no model behind it, for tests that run the workers' Ask and
extraction pipelines in `make check` or `make itest` (the real model is `make acceptance`'s).

Each `/api/chat` is answered by what its JSON schema (`format`) asks for: the Ask intent
(`kind`), the Ask writer (`answer`) or an extraction run (`supplier_ice`). Each `/api/embed`
input gets a deterministic vector of the embedding's dimensions. Every chat body is recorded,
so a test can read the prompts the pipeline sent. Nothing leaves the process.
"""

import hashlib
import json
from typing import Any

import httpx

from sijil_core.index import EMBEDDING_DIM
from sijil_core.ollama import OllamaClient

OLLAMA_URL = "http://ollama:11434"


def vector(text: str) -> list[float]:
    """A deterministic vector per text: mostly orthogonal to any other text's."""
    found = [0.001] * EMBEDDING_DIM
    found[int(hashlib.sha256(text.encode()).hexdigest(), 16) % EMBEDDING_DIM] = 1.0
    return found


class FakeOllama:
    """`intent`: the Ask intent's JSON. `answer`: the Ask writer's text. `extraction`: the
    fields one extraction run reads. A call whose answer was not given fails the test."""

    def __init__(
        self,
        *,
        intent: dict[str, Any] | None = None,
        answer: str | None = None,
        extraction: dict[str, str | None] | None = None,
    ) -> None:
        self.intent = intent
        self.answer = answer
        self.extraction = extraction
        self.chats: list[dict[str, Any]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if request.url.path == "/api/embed":
            return httpx.Response(200, json={"embeddings": [vector(t) for t in body["input"]]})
        assert request.url.path == "/api/chat", request.url.path
        self.chats.append(body)
        wanted = body["format"]["properties"]
        if "kind" in wanted:
            out: Any = self.intent
        elif "answer" in wanted:
            out = None if self.answer is None else {"answer": self.answer}
        else:
            out = self.extraction
        assert out is not None, f"no answer given for {sorted(wanted)}"
        return httpx.Response(200, json={"message": {"content": json.dumps(out)}})

    def client(self) -> OllamaClient:
        return OllamaClient(OLLAMA_URL, {"ollama"}, transport=httpx.MockTransport(self))

    def system_prompts(self) -> list[str]:
        return [str(chat["messages"][0]["content"]) for chat in self.chats]
