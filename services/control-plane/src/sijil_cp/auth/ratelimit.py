"""Fixed-window failure counters in Redis."""

import hashlib

from redis.asyncio import Redis

LOGIN_LIMIT = 10
LOGIN_WINDOW_S = 15 * 60


def login_key(email: str, ip: str) -> str:
    # Hashed so Redis never holds addresses in clear.
    digest = hashlib.sha256(f"{email.strip().lower()}\x00{ip}".encode()).hexdigest()
    return f"ratelimit:login:{digest}"


async def too_many(redis: Redis, key: str, limit: int, window_s: int) -> bool:
    # window_s is part of the interface; the window itself lives in the key's TTL.
    count = await redis.get(key)
    return count is not None and int(count) >= limit


async def record_failure(redis: Redis, key: str, window_s: int) -> None:
    async with redis.pipeline(transaction=True) as pipe:
        pipe.incr(key)
        pipe.expire(key, window_s, nx=True)
        await pipe.execute()


async def clear(redis: Redis, key: str) -> None:
    await redis.delete(key)
