"""Token-bucket rate limiter for outbound REST calls.

Exists specifically to avoid tripping Kalshi's own server-side rate limit during a burst
(e.g. a WS backlog replay). Raising our own connection pool limits alone just shifts the
bottleneck from "blocked on our pool" to "429 from the exchange," which is worse: 429s come
back fast, so removing our self-imposed throttle just lets us hit the real limit harder and
faster, with no useful backoff. This proactively paces requests instead of reacting to 429s.
"""
from __future__ import annotations

import asyncio
import time


class TokenBucketRateLimiter:
    def __init__(self, rate_per_second: float, burst: "int | None" = None) -> None:
        if rate_per_second <= 0:
            raise ValueError(f"rate_per_second must be > 0, got {rate_per_second}")
        self._rate = rate_per_second
        self._capacity = burst if burst is not None else max(1, int(rate_per_second))
        self._tokens = float(self._capacity)
        self._updated_at = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Block until a token is available, pacing calls to at most `rate_per_second`."""
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._updated_at
                self._updated_at = now
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait_s = (1.0 - self._tokens) / self._rate
            # Sleep outside the lock so concurrent waiters don't serialize on it.
            await asyncio.sleep(wait_s)
