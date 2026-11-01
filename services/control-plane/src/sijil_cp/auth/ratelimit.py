"""Fixed-window attempt counters in Redis, and every abuse limit of the control plane."""

import hashlib
from uuid import UUID

from redis.asyncio import Redis

LOGIN_LIMIT = 10
LOGIN_WINDOW_S = 15 * 60
TOTP_LIMIT = 5
TOTP_WINDOW_S = 15 * 60
# Each organisation provisions a whole tenant stack: bound what open signup can ask for.
SIGNUP_LIMIT = 10  # per client IP
SIGNUP_WINDOW_S = 60 * 60
ORG_CREATE_LIMIT = 10  # per client IP
ORG_CREATE_WINDOW_S = 60 * 60
MAX_OWNED_ORGS = 3  # organisations a user may own


def login_key(email: str, ip: str) -> str:
    # Hashed so Redis never holds addresses in clear.
    digest = hashlib.sha256(f"{email.strip().lower()}\x00{ip}".encode()).hexdigest()
    return f"ratelimit:login:{digest}"


def ip_key(action: str, ip: str) -> str:
    digest = hashlib.sha256(f"{action}\x00{ip}".encode()).hexdigest()
    return f"ratelimit:{action}:{digest}"


def totp_key(user_id: UUID) -> str:
    # Per user, not per session: a fresh login must not reset the counter.
    return f"ratelimit:totp:{user_id}"


async def hit(redis: Redis, key: str, limit: int, window_s: int) -> bool:
    """Count one attempt atomically, before doing the work; True while within the limit.

    Counting first means a burst of parallel requests can't all slip past a read-only check.
    The window starts with the first attempt (EXPIRE NX) and is not extended by later ones.
    """
    async with redis.pipeline(transaction=True) as pipe:
        pipe.incr(key)
        pipe.expire(key, window_s, nx=True)
        count, _ = await pipe.execute()
    return int(count) <= limit


async def clear(redis: Redis, key: str) -> None:
    await redis.delete(key)
