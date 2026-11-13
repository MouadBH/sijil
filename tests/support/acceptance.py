"""What the end-to-end acceptance tests share: the signed-up Owner with TOTP (each step-up spends
a time step never used before), the control plane's CSRF header and its CLI in the runner.
"""

import asyncio
import os
import sys
import time
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pyotp


def csrf(http: httpx.AsyncClient) -> dict[str, str]:
    return {"X-CSRF-Token": http.cookies["sijil_csrf"]}


async def post(http: httpx.AsyncClient, path: str, body: Any) -> httpx.Response:
    return await http.post(path, json=body, headers=csrf(http))


class Owner:
    """The signed-up Owner, with TOTP: each step-up spends a time step never used before."""

    def __init__(self, http: httpx.AsyncClient, password: str) -> None:
        self.http = http
        self.password = password
        self.totp: pyotp.TOTP | None = None
        self.last_step = 0

    async def sign_up(self, email: str, name: str) -> None:
        (await self.http.get("/v1/auth/csrf")).raise_for_status()
        body = {"email": email, "password": self.password, "name": name}
        r = await post(self.http, "/v1/auth/signup", body)
        assert r.status_code == 201, r.text
        r = await post(self.http, "/v1/auth/totp/enrol", {"password": self.password})
        assert r.status_code == 200, r.text
        self.totp = pyotp.TOTP(parse_qs(urlparse(r.json()["otpauth_uri"]).query)["secret"][0])
        # As in M1: well inside a step, confirm with the previous step and verify with this one.
        left = self.totp.interval - time.time() % self.totp.interval
        if left < 10:
            await asyncio.sleep(left + 0.5)
        now = time.time()
        r = await post(self.http, "/v1/auth/totp/confirm", {"code": self.totp.at(now - 30)})
        assert r.status_code == 200, r.text
        r = await post(self.http, "/v1/auth/totp/verify", {"code": self.totp.at(now)})
        assert r.status_code == 200, r.text
        self.last_step = int(now // 30)

    async def step_up(self) -> None:
        assert self.totp is not None
        # A code is accepted once per step: wait for the next step if this one is spent.
        await asyncio.sleep(max(0.0, (self.last_step + 1) * 30 - time.time() + 0.5))
        now = time.time()
        r = await post(self.http, "/v1/auth/totp/verify", {"code": self.totp.at(now)})
        assert r.status_code == 200, r.text
        self.last_step = int(now // 30)


async def cli(*args: str) -> tuple[int, str]:
    """`sijil-cp <args>` in the runner: its exit code and its output."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "sijil_cp.cli", *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        # The runner carries the worker's environment; the CLI also wants the API's one flag.
        env=os.environ | {"SIJIL_COOKIE_SECURE": "false"},
    )  # fmt: skip
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode()
