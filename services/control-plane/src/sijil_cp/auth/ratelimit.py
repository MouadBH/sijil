"""Fixed-window attempt counters in Redis."""

import hashlib

from redis.asyncio import Redis

LOGIN_LIMIT = 10
LOGIN_WINDOW_S = 15 * 60


def login_key(email: str, ip: str) -> str:
    # Hashed so Redis never holds addresses in clear.
    digest = hashlib.sha256(f"{email.strip().lower()}\x00{ip}".encode()).hexdigest()
    return f"ratelimit:login:{digest}"


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
