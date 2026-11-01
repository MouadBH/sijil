import asyncio
import hashlib
from collections.abc import Callable
from datetime import timedelta

import httpx
import pytest
from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.db.base import utcnow
from sijil_core.db.models import User, UserSession
from sijil_cp import deps
from sijil_cp.auth import passwords, ratelimit

pytestmark = pytest.mark.db

EMAIL = "mouad@example.com"
PASSWORD = "correct horse battery"
SESSION = "sijil_session"


async def signup(
    c: httpx.AsyncClient, email: str = EMAIL, password: str = PASSWORD
) -> httpx.Response:
    return await c.post(
        "/v1/auth/signup",
        json={"email": email, "password": password, "name": "Mouad", "locale": "fr"},
    )


async def login(
    c: httpx.AsyncClient, email: str = EMAIL, password: str = PASSWORD
) -> httpx.Response:
    return await c.post("/v1/auth/login", json={"email": email, "password": password})


def session_cookie(r: httpx.Response) -> str:
    for header in r.headers.get_list("set-cookie"):
        if header.startswith(f"{SESSION}="):
            return header
    raise AssertionError("no session cookie set")


async def test_signup_creates_user_and_session(client: httpx.AsyncClient, db: AsyncSession) -> None:
    r = await signup(client)
    assert r.status_code == 201
    user = r.json()["user"]
    assert user["email"] == EMAIL
    assert user["name"] == "Mouad"
    assert user["locale"] == "fr"
    assert user["digits"] == "latn"
    assert user["totp_enabled"] is False
    assert "password_hash" not in user

    token = r.cookies[SESSION]
    rows = (await db.scalars(select(UserSession))).all()
    assert len(rows) == 1
    assert rows[0].token_hash == hashlib.sha256(token.encode()).digest()
    assert rows[0].pending_totp is False

    me = await client.get("/v1/me")
    assert me.status_code == 200
    assert me.json() == user


async def test_password_never_stored_plain(client: httpx.AsyncClient, db: AsyncSession) -> None:
    assert (await signup(client)).status_code == 201
    stored = await db.scalar(select(User.password_hash).where(User.email == EMAIL))
    assert stored is not None
    assert stored.startswith("$argon2id$")
    assert PASSWORD not in stored


async def test_weak_password_rejected(client: httpx.AsyncClient, db: AsyncSession) -> None:
    for weak in ("a" * 11, "a" * 129):
        r = await signup(client, password=weak)
        assert r.status_code == 422
        assert r.json() == {"error": {"code": "auth.weak_password"}}
    assert (await db.scalars(select(User))).all() == []


async def test_concurrent_signup_same_email(
    make_db_app: Callable[..., FastAPI],
    csrf_client_for: Callable[..., httpx.AsyncClient],
    db: AsyncSession,
) -> None:
    app = make_db_app()
    async with csrf_client_for(app) as a, csrf_client_for(app) as b:
        ra, rb = await asyncio.gather(signup(a, EMAIL), signup(b, " Mouad@Example.COM "))
    assert {ra.status_code, rb.status_code} == {201, 409}
    loser = ra if ra.status_code == 409 else rb
    assert loser.json() == {"error": {"code": "user.email_taken"}}
    assert SESSION not in loser.cookies
    assert len((await db.scalars(select(User))).all()) == 1


async def test_login_email_normalised(client: httpx.AsyncClient, db: AsyncSession) -> None:
    assert (await signup(client)).status_code == 201
    r = await login(client, email="  Mouad@Example.COM ")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
    assert (await client.get("/v1/me")).json()["email"] == EMAIL


async def test_unknown_email_and_wrong_password_indistinguishable(
    client: httpx.AsyncClient, db: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert (await signup(client)).status_code == 201
    calls: list[str | None] = []
    real = passwords.verify_password

    def spy(stored: str | None, pw: str) -> bool:
        calls.append(stored)
        return real(stored, pw)

    monkeypatch.setattr(passwords, "verify_password", spy)

    unknown = await login(client, email="nobody@example.com")
    assert len(calls) == 1
    assert calls[0] is None
    calls.clear()
    wrong = await login(client, password="not the password!")
    assert len(calls) == 1
    assert calls[0] is not None

    assert unknown.status_code == wrong.status_code == 401
    assert unknown.json() == wrong.json() == {"error": {"code": "auth.invalid_credentials"}}
    assert SESSION not in unknown.cookies
    assert SESSION not in wrong.cookies


async def test_session_cookie_flags(
    make_db_app: Callable[..., FastAPI],
    csrf_client_for: Callable[..., httpx.AsyncClient],
    db: AsyncSession,
) -> None:
    app = make_db_app(cookie_secure="true")
    async with csrf_client_for(app, base_url="https://cp") as c:
        for r in (await signup(c), await login(c)):
            assert r.status_code in (200, 201)
            parts = [p.strip().lower() for p in session_cookie(r).split(";")]
            assert "httponly" in parts
            assert "secure" in parts
            assert "samesite=lax" in parts
            assert "path=/" in parts


async def test_session_token_rotates_on_login(
    make_db_app: Callable[..., FastAPI],
    csrf_client_for: Callable[..., httpx.AsyncClient],
    db: AsyncSession,
) -> None:
    app = make_db_app()
    async with csrf_client_for(app) as c:
        first = (await signup(c)).cookies[SESSION]
        second = (await login(c)).cookies[SESSION]
        third = (await login(c)).cookies[SESSION]
    assert len({first, second, third}) == 3
    # The token presented on login is retired: a planted (fixated) token is useless.
    async with csrf_client_for(app) as stale:
        stale.cookies.set(SESSION, first)
        assert (await stale.get("/v1/me")).status_code == 401
    async with csrf_client_for(app) as fresh:
        fresh.cookies.set(SESSION, third)
        assert (await fresh.get("/v1/me")).status_code == 200


async def test_revoked_session_rejected(client: httpx.AsyncClient, db: AsyncSession) -> None:
    assert (await signup(client)).status_code == 201
    listed = (await client.get("/v1/auth/sessions")).json()
    assert len(listed) == 1
    assert listed[0]["current"] is True
    assert set(listed[0]) == {"id", "created_at", "last_seen_at", "ip", "user_agent", "current"}

    r = await client.delete(f"/v1/auth/sessions/{listed[0]['id']}")
    assert r.status_code == 204
    me = await client.get("/v1/me")
    assert me.status_code == 401
    assert me.json() == {"error": {"code": "auth.unauthenticated"}}


async def test_cannot_revoke_another_users_session(
    make_db_app: Callable[..., FastAPI],
    csrf_client_for: Callable[..., httpx.AsyncClient],
    db: AsyncSession,
) -> None:
    app = make_db_app()
    async with csrf_client_for(app) as alice, csrf_client_for(app) as bob:
        await signup(alice, "alice@example.com")
        await signup(bob, "bob@example.com")
        bobs = (await bob.get("/v1/auth/sessions")).json()[0]["id"]
        r = await alice.delete(f"/v1/auth/sessions/{bobs}")
        assert r.status_code == 404
        assert r.json() == {"error": {"code": "not_found"}}
        assert (await bob.get("/v1/me")).status_code == 200


async def test_logout_revokes_session(client: httpx.AsyncClient, db: AsyncSession) -> None:
    token = (await signup(client)).cookies[SESSION]
    r = await client.post("/v1/auth/logout")
    assert r.status_code == 204
    revoked = await db.scalar(
        select(UserSession.revoked_at).where(
            UserSession.token_hash == hashlib.sha256(token.encode()).digest()
        )
    )
    assert revoked is not None
    client.cookies.set(SESSION, token)
    assert (await client.get("/v1/me")).status_code == 401


async def test_idle_session_expires(
    make_db_app: Callable[..., FastAPI],
    csrf_client_for: Callable[..., httpx.AsyncClient],
    db: AsyncSession,
) -> None:
    app = make_db_app()
    async with csrf_client_for(app) as c:
        assert (await signup(c)).status_code == 201
        assert (await c.get("/v1/me")).status_code == 200
        later = utcnow() + timedelta(minutes=121)
        app.dependency_overrides[deps.get_now] = lambda: later
        r = await c.get("/v1/me")
    assert r.status_code == 401
    assert r.json() == {"error": {"code": "auth.unauthenticated"}}


async def test_absolute_session_expiry(
    make_db_app: Callable[..., FastAPI],
    csrf_client_for: Callable[..., httpx.AsyncClient],
    db: AsyncSession,
) -> None:
    app = make_db_app()
    start = utcnow()
    async with csrf_client_for(app) as c:
        assert (await signup(c)).status_code == 201
        # Active every 100 minutes: never idle, but 12 h after creation it ends anyway.
        for minutes in range(100, 720, 100):
            app.dependency_overrides[deps.get_now] = lambda m=minutes: start + timedelta(minutes=m)
            assert (await c.get("/v1/me")).status_code == 200
        app.dependency_overrides[deps.get_now] = lambda: start + timedelta(minutes=721)
        assert (await c.get("/v1/me")).status_code == 401


async def test_login_rate_limited_after_10_failures(
    client: httpx.AsyncClient, db: AsyncSession
) -> None:
    assert (await signup(client)).status_code == 201
    for _ in range(10):
        assert (await login(client, password="wrong password!!")).status_code == 401
    r = await login(client)  # even the right password
    assert r.status_code == 429
    assert r.json() == {"error": {"code": "auth.rate_limited"}}
    # Same email in another case is the same key.
    assert (await login(client, email=" MOUAD@example.com")).status_code == 429
    # Another account from the same IP is not locked out.
    assert (await signup(client, "other@example.com")).status_code == 201
    assert (await login(client, "other@example.com")).status_code == 200


async def test_parallel_login_burst_is_rate_limited(
    make_db_app: Callable[..., FastAPI],
    csrf_client_for: Callable[..., httpx.AsyncClient],
    db: AsyncSession,
) -> None:
    app = make_db_app()
    async with csrf_client_for(app) as c:
        assert (await signup(c)).status_code == 201
    clients = [csrf_client_for(app) for _ in range(20)]
    try:
        results = await asyncio.gather(*(login(cl, password="wrong password!!") for cl in clients))
    finally:
        for cl in clients:
            await cl.aclose()
    codes = [r.status_code for r in results]
    assert set(codes) <= {401, 429}
    assert codes.count(401) <= ratelimit.LOGIN_LIMIT
    assert codes.count(429) >= 20 - ratelimit.LOGIN_LIMIT


async def test_login_failure_counter_expires(
    client: httpx.AsyncClient, db: AsyncSession, redis: Redis
) -> None:
    assert (await signup(client)).status_code == 201
    assert (await login(client, password="wrong password!!")).status_code == 401
    ttl = await redis.ttl(ratelimit.login_key(EMAIL, "127.0.0.1"))
    assert 0 < ttl <= ratelimit.LOGIN_WINDOW_S


async def test_login_with_totp_returns_pending_session(
    client: httpx.AsyncClient, db: AsyncSession
) -> None:
    assert (await signup(client)).status_code == 201
    await db.execute(update(User).where(User.email == EMAIL).values(totp_enabled=True))
    await db.commit()
    r = await login(client)
    assert r.status_code == 200
    assert r.json() == {"status": "totp_required"}
    pending = await db.scalar(
        select(UserSession).where(
            UserSession.token_hash == hashlib.sha256(r.cookies[SESSION].encode()).digest()
        )
    )
    assert pending is not None
    assert pending.pending_totp is True
    assert (await client.get("/v1/me")).status_code == 401


async def test_csrf_required_on_login(
    make_db_app: Callable[..., FastAPI],
    client_for: Callable[[FastAPI], httpx.AsyncClient],
    db: AsyncSession,
) -> None:
    async with client_for(make_db_app()) as c:
        for r in (await signup(c), await login(c)):
            assert r.status_code == 403
            assert r.json() == {"error": {"code": "auth.csrf"}}
    assert (await db.scalars(select(User))).all() == []


async def test_signup_rate_limited_per_ip(client: httpx.AsyncClient, redis: Redis) -> None:
    for i in range(ratelimit.SIGNUP_LIMIT):
        assert (await signup(client, f"user{i}@example.com")).status_code == 201
    r = await signup(client, "one-too-many@example.com")
    assert r.status_code == 429
    assert r.json() == {"error": {"code": "auth.rate_limited"}}
    ttl = await redis.ttl(ratelimit.ip_key("signup", "127.0.0.1"))
    assert 0 < ttl <= ratelimit.SIGNUP_WINDOW_S == 3600
