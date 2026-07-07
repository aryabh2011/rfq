"""Async orchestration loop for the unhedged RFQ pennying bot.

Flow per inbound RFQ: fetch fresh leg VWMids -> compute a safe penny quote ->
gate it through the inventory risk manager -> sign and POST it back to Kalshi,
all under a hard response-time deadline. Each RFQ is handled as an independent
task so one slow leg-book fetch never blocks the WS stream or other RFQs.

Run with:  python -m src.rfq_main
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import signal

import websockets

from src.config import BotConfig, KalshiConfig
from src.execution.rfq_penny_engine import MinimalistPennyingEngine
from src.execution.rfq_risk_gate import RFQInventoryManager
from src.kalshi_client import KalshiHttpClient, KalshiWebSocketClient, RFQEvent, compute_vwmid

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

MAX_RECONNECT_BACKOFF_S = 30.0


class RFQBot:
    def __init__(self, kalshi_config: KalshiConfig, bot_config: BotConfig) -> None:
        self._http = KalshiHttpClient(kalshi_config)
        self._ws_client = KalshiWebSocketClient(kalshi_config)
        self._engine = MinimalistPennyingEngine(fee_buffer=bot_config.fee_buffer)
        self._risk_gate = RFQInventoryManager(
            get_bankroll=lambda: bot_config.bankroll_usd,
            max_exposure_pct_per_contract=bot_config.max_exposure_pct_per_contract,
            max_exposure_pct_per_prefix=bot_config.max_exposure_pct_per_prefix,
        )
        self._response_deadline_s = bot_config.rfq_response_deadline_s
        self._stopping = False

    def stop(self) -> None:
        self._stopping = True

    async def run(self) -> None:
        backoff_s = 1.0
        while not self._stopping:
            try:
                ws = await self._ws_client.connect()
                backoff_s = 1.0
                try:
                    async for event in self._ws_client.stream_rfq_events(ws):
                        asyncio.create_task(self._handle_rfq(event))
                finally:
                    await ws.close()
            except (websockets.exceptions.WebSocketException, OSError) as exc:
                if self._stopping:
                    break
                logger.warning("WS connection lost (%s); reconnecting in %.1fs.", exc, backoff_s)
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, MAX_RECONNECT_BACKOFF_S)
        await self._http.aclose()

    async def _handle_rfq(self, event: RFQEvent) -> None:
        try:
            await asyncio.wait_for(self._process_rfq(event), timeout=self._response_deadline_s)
        except asyncio.TimeoutError:
            logger.warning(
                "Skipping RFQ %s: exceeded %.2fs response deadline (network latency).",
                event.rfq_id, self._response_deadline_s,
            )
        except Exception:
            logger.exception("Skipping RFQ %s: unhandled error while processing.", event.rfq_id)

    async def _process_rfq(self, event: RFQEvent) -> None:
        books = await asyncio.gather(
            *(self._http.get_orderbook(ticker) for ticker in event.leg_tickers)
        )
        leg_vwmids = [compute_vwmid(book) for book in books]
        if any(mid is None for mid in leg_vwmids):
            logger.info("Skipping RFQ %s: a leg has no tradeable market (empty/crossed book).", event.rfq_id)
            return

        quote_price = self._engine.calculate_safe_quote(
            leg_mid_prices=leg_vwmids,
            best_competitor_quote=event.best_competitor_quote,
            side=event.side,
        )
        if quote_price is None:
            logger.info(
                "Skipping RFQ %s: no safe quote (competitor margin squeezed below fee-adjusted floor, "
                "or floor breaches the absolute ceiling).",
                event.rfq_id,
            )
            return

        approved = await self._risk_gate.try_reserve(event.ticker, quote_price, event.contracts)
        if not approved:
            logger.warning("Skipping RFQ %s: rejected by inventory risk gate (exposure limit).", event.rfq_id)
            return

        try:
            await self._http.submit_rfq_quote(event.rfq_id, event.ticker, event.side, quote_price, event.contracts)
            logger.info(
                "Quoted RFQ %s: %s %s @ %.2f x%d",
                event.rfq_id, event.ticker, event.side, quote_price, event.contracts,
            )
        except Exception:
            self._risk_gate.release(event.ticker, quote_price, event.contracts)
            raise


async def main() -> None:
    kalshi_config = KalshiConfig()
    bot_config = BotConfig()
    bot = RFQBot(kalshi_config, bot_config)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    run_task = asyncio.create_task(bot.run())
    await stop_event.wait()

    logger.info("Shutdown signal received; stopping RFQ bot.")
    bot.stop()
    run_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await run_task


if __name__ == "__main__":
    asyncio.run(main())
