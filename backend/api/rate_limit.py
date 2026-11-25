import os
from dataclasses import dataclass
from typing import Protocol

from redis.asyncio import Redis


_INCREMENT_WITH_EXPIRY = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
"""


class RateLimiter(Protocol):
    async def allow(self, key: str, *, limit: int, window_seconds: int) -> bool: ...

    async def aclose(self) -> None: ...


class RedisRateLimiter:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    @classmethod
    def from_env(cls) -> "RedisRateLimiter":
        url = os.getenv("REDIS_URL")
        if not url:
            raise RuntimeError("REDIS_URL must be configured")
        redis = Redis.from_url(
            url,
            socket_connect_timeout=float(
                os.getenv("REDIS_CONNECT_TIMEOUT_SECONDS", "1")
            ),
            socket_timeout=float(os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", "1")),
            max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", "20")),
            decode_responses=True,
        )
        return cls(redis)

    async def allow(self, key: str, *, limit: int, window_seconds: int) -> bool:
        script = self._redis.register_script(_INCREMENT_WITH_EXPIRY)
        count = await script(keys=[key], args=[window_seconds])
        return int(count) <= limit

    async def aclose(self) -> None:
        await self._redis.aclose()


@dataclass(frozen=True)
class RatePolicy:
    name: str
    limit: int
    window_seconds: int


def rate_policy(method: str, path: str) -> RatePolicy | None:
    if method == "POST" and path == "/api/auth/login":
        return RatePolicy("login", limit=10, window_seconds=60)
    if method == "POST" and path == "/api/auth/register":
        return RatePolicy("register", limit=5, window_seconds=3600)
    if method == "POST" and path == "/api/auth/refresh":
        return RatePolicy("refresh", limit=20, window_seconds=60)
    if method == "POST" and path == "/api/files/upload":
        return RatePolicy("upload", limit=10, window_seconds=3600)
    if method == "GET" and path == "/api/files":
        return RatePolicy("list-files", limit=120, window_seconds=60)
    if method == "GET" and path.endswith("/download"):
        return RatePolicy("download", limit=120, window_seconds=60)
    if path.startswith("/api/nodes/"):
        return RatePolicy("admin-api", limit=60, window_seconds=60)
    return None
