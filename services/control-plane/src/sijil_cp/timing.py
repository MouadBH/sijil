"""`Server-Timing: app;dur=<ms>` on every HTTP response: the time from the request reaching the
app to its response starting (dependencies, handler, database), as the server measured it. The
search budget (p95 < 500 ms over 100k documents, tests/perf) is read from it."""

import time

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class ServerTimingMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()

        async def timed(message: Message) -> None:
            if message["type"] == "http.response.start":
                elapsed = (time.perf_counter() - started) * 1000
                MutableHeaders(scope=message).append("Server-Timing", f"app;dur={elapsed:.1f}")
            await send(message)

        await self.app(scope, receive, timed)
