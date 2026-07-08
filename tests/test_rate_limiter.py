import asyncio
import time

import pytest

from src.rate_limiter import TokenBucketRateLimiter


def test_rejects_non_positive_rate():
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(rate_per_second=0)
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(rate_per_second=-1)


def test_default_burst_matches_rate():
    limiter = TokenBucketRateLimiter(rate_per_second=7.5)
    assert limiter._capacity == 7  # int(7.5)


async def test_burst_capacity_allows_immediate_acquires():
    limiter = TokenBucketRateLimiter(rate_per_second=5, burst=5)
    start = time.monotonic()
    for _ in range(5):
        await limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.1, f"expected near-instant burst acquires, took {elapsed:.3f}s"


async def test_exceeding_burst_paces_at_configured_rate():
    limiter = TokenBucketRateLimiter(rate_per_second=10, burst=1)
    await limiter.acquire()  # consumes the one burst token immediately
    start = time.monotonic()
    await limiter.acquire()  # must wait ~1/10s for the next token to refill
    elapsed = time.monotonic() - start
    assert 0.05 <= elapsed <= 0.3, f"expected ~0.1s wait, took {elapsed:.3f}s"


async def test_concurrent_acquires_beyond_burst_are_paced_not_all_immediate():
    limiter = TokenBucketRateLimiter(rate_per_second=20, burst=1)
    start = time.monotonic()
    await asyncio.gather(*(limiter.acquire() for _ in range(5)))
    elapsed = time.monotonic() - start
    # 1 token free immediately, remaining 4 paced at 20/s -> at least ~0.2s total
    assert elapsed >= 0.15, f"expected pacing to take at least ~0.2s, took {elapsed:.3f}s"
