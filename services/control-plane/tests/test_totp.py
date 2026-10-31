import hashlib
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta
from typing import Annotated
from urllib.parse import parse_qs, urlparse

import httpx
import pyotp
import pytest
from fastapi import APIRouter, Depends, FastAPI
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.crypto import Envelope, open_envelope
from sijil_core.db.base import utcnow
from sijil_core.db.models import (
    Membership,
    Organisation,
    OrgType,
    RecoveryCode,
    Role,
    User,
    UserSession,
    Workspace,
)
from sijil_cp import deps
from sijil_cp.auth import totp

pytestmark = pytest.mark.db

EMAIL = "mouad@example.com"
PASSWORD = "correct horse battery"
SESSION = "sijil_session"
STEP = timedelta(seconds=30)


class Clock:
    """A frozen request clock that tests move by hand."""

    def __init__(self) -> None:
        now = utcnow()
        # Five seconds into a step, so ±1 step is unambiguous.
        self.now = datetime.fromtimestamp((int(now.timestamp()) // 30) * 30 + 5, tz=now.tzinfo)

    def advance(self, delta: timedelta) -> datetime:
        self.now += delta
        return self.now


@pytest.fixture
def clock() -> Clock:
    return Clock()


def _probe_router() -> APIRouter:
    """Test-only routes that exercise the TOTP dependencies directly."""
    router = APIRouter(prefix="/v1")

    @router.get("/orgs/_probe")
    async def compliance_probe(
        user: Annotated[User, Depends(deps.require_totp_compliance)],
    ) -> dict[str, str]:
        return {"user": str(user.id)}

    @router.get("/_probe/step-up")
    async def step_up_probe(
        session: Annotated[UserSession, Depends(deps.require_recent_totp())],
    ) -> dict[str, str]:
        return {"session": str(session.id)}

    return router


@pytest.fixture
def app(make_db_app: Callable[..., FastAPI], clock: Clock) -> FastAPI:
    app = make_db_app()
    app.include_router(_probe_router())
    app.dependency_overrides[deps.get_now] = lambda: clock.now
    return app


@pytest.fixture
async def c(
    app: FastAPI, csrf_client_for: Callable[..., httpx.AsyncClient], db: AsyncSession
) -> AsyncIterator[httpx.AsyncClient]:
    async with csrf_client_for(app) as client:
        yield client


async def signup(c: httpx.AsyncClient, email: str = EMAIL) -> None:
    r = await c.post(
        "/v1/auth/signup",
        json={"email": email, "password": PASSWORD, "name": "Mouad", "locale": "fr"},
    )
    assert r.status_code == 201


async def login(c: httpx.AsyncClient, email: str = EMAIL) -> httpx.Response:
    return await c.post("/v1/auth/login", json={"email": email, "password": PASSWORD})


def code_at(secret: str, when: datetime) -> str:
    return pyotp.TOTP(secret).at(when)


def wrong_code(secret: str, clock: "Clock") -> str:
    """A well-formed code outside the accepted window."""
    return code_at(secret, clock.now + 5 * STEP)


async def enrol(c: httpx.AsyncClient) -> str:
    r = await c.post("/v1/auth/totp/enrol")
    assert r.status_code == 200
    uri = urlparse(str(r.json()["otpauth_uri"]))
    assert uri.scheme == "otpauth"
    params = parse_qs(uri.query)
    assert params["issuer"] == ["Sijil"]
    return params["secret"][0]


async def enable_totp(c: httpx.AsyncClient, clock: Clock) -> tuple[str, list[str]]:
    """Sign up, enrol and confirm. Leaves the clock one step past the confirmed code."""
    await signup(c)
    secret = await enrol(c)
    r = await c.post("/v1/auth/totp/confirm", json={"code": code_at(secret, clock.now)})
    assert r.status_code == 200
    clock.advance(STEP)
    return secret, r.json()["recovery_codes"]


async def verify(c: httpx.AsyncClient, **body: str) -> httpx.Response:
    return await c.post("/v1/auth/totp/verify", json=body)


def error(r: httpx.Response) -> str:
    code: str = r.json()["error"]["code"]
    return code


async def test_enrol_confirm_enables_totp_and_returns_10_codes(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    await signup(c)
    secret = await enrol(c)
    user = await db.scalar(select(User).where(User.email == EMAIL))
    assert user is not None
    assert user.totp_enabled is False
    assert user.totp_pending is not None
    assert (await c.get("/v1/me")).json()["totp_enabled"] is False

    r = await c.post("/v1/auth/totp/confirm", json={"code": code_at(secret, clock.now)})
    assert r.status_code == 200
    codes = r.json()["recovery_codes"]
    assert len(codes) == 10
    assert len(set(codes)) == 10
    alphabet = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567")
    assert all(len(code) == 10 and set(code) <= alphabet for code in codes)

    await db.refresh(user)
    assert user.totp_enabled is True
    assert user.totp_pending is None
    assert user.totp_secret is not None
    assert (await c.get("/v1/me")).json()["totp_enabled"] is True
    stored = (await db.scalars(select(RecoveryCode).where(RecoveryCode.user_id == user.id))).all()
    assert len(stored) == 10
    for row in stored:
        assert all(code.encode() not in row.code_hmac for code in codes)


async def test_confirm_with_wrong_code_keeps_totp_off(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    await signup(c)
    secret = await enrol(c)
    wrong = code_at(secret, clock.now + timedelta(minutes=5))
    r = await c.post("/v1/auth/totp/confirm", json={"code": wrong})
    assert r.status_code == 401
    assert error(r) == "auth.totp_invalid"
    assert (await c.get("/v1/me")).json()["totp_enabled"] is False


async def test_login_requires_totp_when_enabled(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, _ = await enable_totp(c, clock)
    r = await login(c)
    assert r.json() == {"status": "totp_required"}
    me = await c.get("/v1/me")
    assert me.status_code == 401
    assert error(me) == "auth.totp_required"

    r = await verify(c, code=code_at(secret, clock.now))
    assert r.status_code == 200
    assert (await c.get("/v1/me")).status_code == 200


async def test_totp_code_cannot_be_reused(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, _ = await enable_totp(c, clock)
    await login(c)
    code = code_at(secret, clock.now)
    assert (await verify(c, code=code)).status_code == 200

    # Same code, same 30 s window, from a fresh login: rejected.
    await login(c)
    r = await verify(c, code=code)
    assert r.status_code == 401
    assert error(r) == "auth.totp_invalid"
    # The previous step (still inside valid_window) is also spent.
    r = await verify(c, code=code_at(secret, clock.now - STEP))
    assert r.status_code == 401
    # The next step works.
    clock.advance(STEP)
    assert (await verify(c, code=code_at(secret, clock.now))).status_code == 200


async def test_confirm_code_cannot_be_replayed_on_login(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    await signup(c)
    secret = await enrol(c)
    code = code_at(secret, clock.now)
    assert (await c.post("/v1/auth/totp/confirm", json={"code": code})).status_code == 200
    await login(c)
    assert (await verify(c, code=code)).status_code == 401


async def test_wrong_code_rejected(c: httpx.AsyncClient, clock: Clock, db: AsyncSession) -> None:
    secret, _ = await enable_totp(c, clock)
    await login(c)
    for wrong in (code_at(secret, clock.now + 3 * STEP), "000000x", "", "1" * 64):
        r = await verify(c, code=wrong)
        assert r.status_code == 401
        assert error(r) == "auth.totp_invalid"
    assert (await c.get("/v1/me")).status_code == 401


async def test_verify_needs_exactly_one_of_code_and_recovery_code(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, codes = await enable_totp(c, clock)
    await login(c)
    assert (await verify(c)).status_code == 422
    r = await verify(c, code=code_at(secret, clock.now), recovery_code=codes[0])
    assert r.status_code == 422


async def test_recovery_code_single_use(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    _, codes = await enable_totp(c, clock)
    await login(c)
    # Case and separators are forgiven.
    spaced = f" {codes[0][:5].lower()}-{codes[0][5:]} "
    assert (await verify(c, recovery_code=spaced)).status_code == 200
    assert (await c.get("/v1/me")).status_code == 200

    await login(c)
    r = await verify(c, recovery_code=codes[0])
    assert r.status_code == 401
    assert error(r) == "auth.totp_invalid"
    assert (await verify(c, recovery_code=codes[1])).status_code == 200
    used = await db.scalar(
        select(func.count()).select_from(RecoveryCode).where(RecoveryCode.used_at.is_not(None))
    )
    assert used == 2


async def test_recovery_codes_are_per_user(
    app: FastAPI,
    csrf_client_for: Callable[..., httpx.AsyncClient],
    clock: Clock,
    db: AsyncSession,
) -> None:
    async with csrf_client_for(app) as alice, csrf_client_for(app) as bob:
        _, alice_codes = await enable_totp(alice, clock)
        await signup(bob, "bob@example.com")
        secret = await enrol(bob)
        r = await bob.post("/v1/auth/totp/confirm", json={"code": code_at(secret, clock.now)})
        assert r.status_code == 200
        await login(bob, "bob@example.com")
        assert (await verify(bob, recovery_code=alice_codes[0])).status_code == 401


async def test_session_token_rotates_on_totp_verify(
    app: FastAPI,
    c: httpx.AsyncClient,
    csrf_client_for: Callable[..., httpx.AsyncClient],
    clock: Clock,
    db: AsyncSession,
) -> None:
    secret, _ = await enable_totp(c, clock)
    pending = (await login(c)).cookies[SESSION]
    r = await verify(c, code=code_at(secret, clock.now))
    full = r.cookies[SESSION]
    assert full != pending
    row = await db.scalar(
        select(UserSession).where(UserSession.token_hash == hashlib.sha256(full.encode()).digest())
    )
    assert row is not None
    assert row.pending_totp is False
    assert row.totp_verified_at == clock.now
    # The pending token is retired.
    async with csrf_client_for(app) as stale:
        stale.cookies.set(SESSION, pending)
        assert (await verify(stale, code=code_at(secret, clock.now + STEP))).status_code == 401
    # A step-up on a full session rotates again.
    clock.advance(STEP)
    again = (await verify(c, code=code_at(secret, clock.now))).cookies[SESSION]
    assert again not in (pending, full)
    async with csrf_client_for(app) as stale:
        stale.cookies.set(SESSION, full)
        assert (await stale.get("/v1/me")).status_code == 401


async def test_step_up_keeps_absolute_expiry(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, _ = await enable_totp(c, clock)
    await login(c)
    first = (await verify(c, code=code_at(secret, clock.now))).cookies[SESSION]
    expires = await db.scalar(
        select(UserSession.expires_at).where(
            UserSession.token_hash == hashlib.sha256(first.encode()).digest()
        )
    )
    clock.advance(timedelta(hours=1))
    second = (await verify(c, code=code_at(secret, clock.now))).cookies[SESSION]
    again = await db.scalar(
        select(UserSession.expires_at).where(
            UserSession.token_hash == hashlib.sha256(second.encode()).digest()
        )
    )
    assert again == expires


async def test_totp_secret_stored_encrypted(
    app: FastAPI, c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, _ = await enable_totp(c, clock)
    user = await db.scalar(select(User).where(User.email == EMAIL))
    assert user is not None
    assert user.totp_secret is not None
    assert secret.encode() not in user.totp_secret
    env = Envelope.from_bytes(user.totp_secret)
    assert env.key_id == "platform"
    keys = app.state.keys
    assert open_envelope(env, keys, f"user:{user.id}:totp").decode() == secret
    with pytest.raises(Exception):  # noqa: B017, PT011
        open_envelope(env, keys, "user:someone-else:totp")


async def test_pending_session_expires_after_10_minutes(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, _ = await enable_totp(c, clock)
    await login(c)
    clock.advance(timedelta(minutes=11))
    r = await verify(c, code=code_at(secret, clock.now))
    assert r.status_code == 401
    assert error(r) == "auth.unauthenticated"


async def test_verify_rate_limited_after_5_failures(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, _ = await enable_totp(c, clock)
    await login(c)
    for _ in range(5):
        assert (await verify(c, code=wrong_code(secret, clock))).status_code == 401
    r = await verify(c, code=code_at(secret, clock.now))  # even a valid code
    assert r.status_code == 429
    assert error(r) == "auth.rate_limited"
    # A new login does not reset the per-user counter.
    await login(c)
    assert (await verify(c, code=code_at(secret, clock.now))).status_code == 429


async def test_verify_success_clears_failures(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, _ = await enable_totp(c, clock)
    await login(c)
    for _ in range(4):
        assert (await verify(c, code=wrong_code(secret, clock))).status_code == 401
    assert (await verify(c, code=code_at(secret, clock.now))).status_code == 200
    clock.advance(STEP)
    for _ in range(4):
        assert (await verify(c, code=wrong_code(secret, clock))).status_code == 401
    assert (await verify(c, code=code_at(secret, clock.now))).status_code == 200


async def test_step_up_required_after_10_minutes(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, _ = await enable_totp(c, clock)
    await login(c)
    assert (await verify(c, code=code_at(secret, clock.now))).status_code == 200
    clock.advance(timedelta(minutes=9))
    assert (await c.get("/v1/_probe/step-up")).status_code == 200
    clock.advance(timedelta(minutes=2))
    r = await c.get("/v1/_probe/step-up")
    assert r.status_code == 403
    assert error(r) == "auth.step_up_required"
    assert (await verify(c, code=code_at(secret, clock.now))).status_code == 200
    assert (await c.get("/v1/_probe/step-up")).status_code == 200


async def test_step_up_required_for_user_without_totp(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    await signup(c)
    r = await c.get("/v1/_probe/step-up")
    assert r.status_code == 403
    assert error(r) == "auth.step_up_required"
    # No TOTP means no way to obtain step-up through verify either.
    assert (await verify(c, code="000000")).status_code == 401
    assert (await c.get("/v1/_probe/step-up")).status_code == 403


async def _require_totp_org(db: AsyncSession, *, via_workspace: bool = False) -> Organisation:
    user = await db.scalar(select(User).where(User.email == EMAIL))
    assert user is not None
    org = Organisation(
        name="Atlas", type=OrgType.business, country="MA", region="ma-cas", require_totp=True
    )
    db.add(org)
    await db.flush()
    if via_workspace:
        ws = Workspace(
            org_id=org.id,
            name="Atlas",
            slug="atlas",
            db_name="ws_atlas",
            index_db_name="ws_atlas_index",
            kms_key_id="ws-atlas",
        )
        db.add(ws)
        await db.flush()
        db.add(Membership(user_id=user.id, workspace_id=ws.id, role=Role.member))
    else:
        db.add(Membership(user_id=user.id, org_id=org.id, role=Role.member))
    await db.commit()
    return org


async def test_owner_enforcement_blocks_users_without_totp(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    await signup(c)
    assert (await c.get("/v1/orgs/_probe")).status_code == 200
    await _require_totp_org(db)
    r = await c.get("/v1/orgs/_probe")
    assert r.status_code == 403
    assert error(r) == "auth.totp_enrolment_required"
    # Enrolling is still possible, and once enabled the user gets through.
    secret = await enrol(c)
    assert (
        await c.post("/v1/auth/totp/confirm", json={"code": code_at(secret, clock.now)})
    ).status_code == 200
    assert (await c.get("/v1/orgs/_probe")).status_code == 200


async def test_enforcement_applies_through_workspace_membership(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    await signup(c)
    await _require_totp_org(db, via_workspace=True)
    r = await c.get("/v1/orgs/_probe")
    assert r.status_code == 403
    assert error(r) == "auth.totp_enrolment_required"


async def test_disable_totp_with_step_up(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, _ = await enable_totp(c, clock)
    # Without a recent verification: refused.
    r = await c.post("/v1/auth/totp/disable")
    assert r.status_code == 403
    assert error(r) == "auth.step_up_required"
    assert (await verify(c, code=code_at(secret, clock.now))).status_code == 200
    assert (await c.post("/v1/auth/totp/disable")).status_code == 204
    user = await db.scalar(select(User).where(User.email == EMAIL))
    assert user is not None
    await db.refresh(user)
    assert user.totp_enabled is False
    assert user.totp_secret is None
    count = await db.scalar(
        select(func.count()).select_from(RecoveryCode).where(RecoveryCode.user_id == user.id)
    )
    assert count == 0
    # The earlier verification no longer counts as step-up: the user has no TOTP now.
    r = await c.get("/v1/_probe/step-up")
    assert r.status_code == 403
    assert error(r) == "auth.step_up_required"
    assert (await login(c)).json() == {"status": "ok"}


async def test_cannot_disable_totp_when_enforced(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, _ = await enable_totp(c, clock)
    await _require_totp_org(db)
    assert (await verify(c, code=code_at(secret, clock.now))).status_code == 200
    r = await c.post("/v1/auth/totp/disable")
    assert r.status_code == 409
    assert error(r) == "auth.totp_enforced"
    assert (await c.get("/v1/me")).json()["totp_enabled"] is True


async def test_reenrol_when_enabled_needs_step_up(
    c: httpx.AsyncClient, clock: Clock, db: AsyncSession
) -> None:
    secret, _ = await enable_totp(c, clock)
    r = await c.post("/v1/auth/totp/enrol")
    assert r.status_code == 403
    assert error(r) == "auth.step_up_required"
    assert (await verify(c, code=code_at(secret, clock.now))).status_code == 200
    assert (await c.post("/v1/auth/totp/enrol")).status_code == 200


def test_verify_unit_window_and_replay() -> None:
    secret = totp.new_secret()
    now = Clock().now
    step = int(now.timestamp()) // 30
    t = pyotp.TOTP(secret)
    assert totp.verify(secret, t.at(now), None, now) == step
    assert totp.verify(secret, t.at(now - STEP), None, now) == step - 1
    assert totp.verify(secret, t.at(now + STEP), None, now) == step + 1
    assert totp.verify(secret, t.at(now + 2 * STEP), None, now) is None
    assert totp.verify(secret, t.at(now), step, now) is None
    assert totp.verify(secret, t.at(now + STEP), step, now) == step + 1
    assert "issuer=Sijil" in totp.provisioning_uri(secret, EMAIL)
