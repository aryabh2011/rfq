import asyncio

from unittest.mock import AsyncMock

from src.config import BotConfig
from src.kalshi_client import OrderbookLevel, OrderbookSnapshot, RFQBroadcast
from src.rfq_main import RFQBot


def _book(ticker: str) -> OrderbookSnapshot:
    return OrderbookSnapshot(
        ticker=ticker,
        yes_levels=[OrderbookLevel(price=0.55, size=100)],
        no_levels=[OrderbookLevel(price=0.40, size=100)],
    )


async def test_concurrent_rfq_processing_is_bounded_by_semaphore(kalshi_config):
    bot_config = BotConfig(max_concurrent_rfq_processing=3)
    bot = RFQBot(kalshi_config, bot_config)
    bot._bankroll._value = 10_000.0

    in_flight = 0
    max_in_flight = 0
    lock = asyncio.Lock()

    async def slow_get_orderbook(ticker: str) -> OrderbookSnapshot:
        nonlocal in_flight, max_in_flight
        async with lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.05)
        async with lock:
            in_flight -= 1
        return _book(ticker)

    bot._http.get_orderbook = slow_get_orderbook
    bot._http.create_quote = AsyncMock(return_value="quote-id")

    events = [
        RFQBroadcast(
            rfq_id=f"rfq-{i}", market_ticker=f"COMBO-{i}",
            leg_tickers=[f"LEG-A-{i}", f"LEG-B-{i}"],
            contracts=None, target_cost_dollars=0.5,
        )
        for i in range(10)
    ]

    await asyncio.gather(*(bot._handle_rfq(event) for event in events))

    # The semaphore caps concurrent RFQs, not concurrent HTTP calls directly -- each RFQ here
    # fires 2 concurrent leg fetches, so the true ceiling is semaphore_size * legs_per_rfq = 6.
    assert max_in_flight <= 6, f"expected at most 3 RFQs x 2 legs = 6 concurrent fetches, saw {max_in_flight}"
    assert max_in_flight >= 6, "expected the cap to actually be reached with 10 concurrent RFQs"
