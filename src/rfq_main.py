"""Async orchestration loop for the unhedged RFQ market-making bot.

Kalshi's RFQs are sealed-bid: makers never see competing quotes, so the bot posts
its own defensible two-sided price (a bid to buy YES, a bid to buy NO) computed
directly from the model-free anchor -- there is no competitor price to react to.

Startup: reconcile the risk gate's ledger against live positions before accepting
any RFQs -- refuses to start if that reconciliation is ambiguous, rather than
silently assuming a clean slate (see `_reconcile_startup_liability`).

Flow per inbound RFQ: dedupe -> reject same-event (SGP) combos -> fetch fresh leg
VWMids -> compute a two-sided anchored quote -> size contracts-offered per side
from the RFQ's own contracts/target_cost -> reserve the worse-case liability ->
submit. If the requester accepts one side (`quote_accepted`), we must confirm
within the exchange's window (3-30s) -- that handler is time-critical and runs
as its own task, never queued behind anything else. If nothing is accepted within
a timeout, the reservation is released; if a quote is accepted+confirmed, the
reservation is trimmed down to the real accepted-side liability and kept until
the position is later reconciled (this bot does not track settlement/close).

Run with:  python -m src.rfq_main
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import time
from dataclasses import dataclass

import websockets

from src.config import BotConfig, KalshiConfig
from src.execution.rfq_anchor_engine import MinimalistPennyingEngine
from src.execution.rfq_risk_gate import RFQInventoryManager, contract_liability, legs_share_event
from src.kalshi_client import (
    KalshiHttpClient,
    KalshiWebSocketClient,
    QuoteAccepted,
    QuoteExecuted,
    RFQBroadcast,
    compute_vwmid,
    extract_position_liabilities,
)
from src.notifications import Notifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

MAX_RECONNECT_BACKOFF_S = 30.0
SEEN_RFQ_PRUNE_INTERVAL_S = 300.0


class StartupReconciliationError(RuntimeError):
    """Raised when the risk gate's ledger can't be confidently seeded from live positions."""


@dataclass
class PendingQuote:
    """Bookkeeping for a submitted quote awaiting acceptance."""
    rfq_id: str
    market_ticker: str
    yes_bid: float
    no_bid: float
    yes_contracts: float
    no_contracts: float
    worst_case_liability: float
    confirmed: bool = False


class BankrollTracker:
    """Periodically refreshes live Kalshi cash balance; keeps the last known value on fetch failure."""

    def __init__(self, http_client: KalshiHttpClient, fallback_usd: float, refresh_interval_s: float) -> None:
        self._http = http_client
        self._value = fallback_usd
        self._refresh_interval_s = refresh_interval_s

    @property
    def value(self) -> float:
        return self._value

    async def run(self) -> None:
        while True:
            try:
                self._value = await self._http.get_balance_usd()
                logger.info("Bankroll refreshed: $%.2f", self._value)
            except Exception:
                logger.exception("Bankroll refresh failed; keeping last known value $%.2f.", self._value)
            await asyncio.sleep(self._refresh_interval_s)


class RFQBot:
    def __init__(self, kalshi_config: KalshiConfig, bot_config: BotConfig) -> None:
        self._http = KalshiHttpClient(kalshi_config)
        self._ws_client = KalshiWebSocketClient(kalshi_config)
        self._engine = MinimalistPennyingEngine(
            fee_buffer=bot_config.fee_buffer,
            fee_buffer_per_extra_leg=bot_config.fee_buffer_per_extra_leg,
            base_legs=bot_config.fee_buffer_base_legs,
        )
        self._bankroll = BankrollTracker(
            self._http, bot_config.fallback_bankroll_usd, bot_config.bankroll_refresh_interval_s
        )
        self._risk_gate = RFQInventoryManager(
            get_bankroll=lambda: self._bankroll.value,
            max_exposure_pct_per_contract=bot_config.max_exposure_pct_per_contract,
            max_exposure_pct_per_prefix=bot_config.max_exposure_pct_per_prefix,
            min_notional_cap_per_contract_usd=bot_config.min_notional_cap_per_contract_usd,
            min_notional_cap_per_prefix_usd=bot_config.min_notional_cap_per_prefix_usd,
        )
        self._notifier = Notifier(bot_config.alert_webhook_url)
        self._response_deadline_s = bot_config.rfq_response_deadline_s
        self._quote_acceptance_timeout_s = bot_config.quote_acceptance_timeout_s
        self._seen_rfq_ttl_s = bot_config.seen_rfq_ttl_s
        self._mgp_only = bot_config.mgp_only
        self._min_leg_quote_size = bot_config.min_leg_quote_size
        self._stop_loss_floor_usd = bot_config.stop_loss_floor_usd
        self._halted = False
        self._stopping = False
        self._seen_rfq_ids: dict[str, float] = {}
        self._pending_quotes: dict[str, PendingQuote] = {}

    def stop(self) -> None:
        self._stopping = True

    async def _reconcile_startup_liability(self) -> None:
        """Seed the risk gate from live positions before accepting any RFQs.

        Fail-closed: if positions can't be fetched or fully parsed, refuse to start rather
        than let the bot begin quoting believing it holds zero risk when it might not.
        """
        try:
            positions = await self._http.get_positions()
        except Exception as exc:
            await self._notifier.notify(
                "RFQ bot STARTUP FAILED: could not fetch live positions for reconciliation. Refusing to start."
            )
            raise StartupReconciliationError("Could not fetch live positions for startup reconciliation.") from exc

        liabilities, fully_parsed = extract_position_liabilities(positions)
        if not fully_parsed:
            await self._notifier.notify(
                "RFQ bot STARTUP FAILED: one or more open positions had an unparseable exposure "
                "field during reconciliation. Refusing to start until this is resolved manually."
            )
            raise StartupReconciliationError(
                "One or more open positions could not be confidently parsed during startup "
                "reconciliation -- refusing to start rather than assume zero existing exposure."
            )

        self._risk_gate.seed(liabilities)
        logger.info(
            "Startup reconciliation complete: seeded risk gate with $%.2f of real liability across %d tickers.",
            sum(liabilities.values()), len(liabilities),
        )

    async def run(self) -> None:
        try:
            await self._reconcile_startup_liability()

            background_tasks = [
                asyncio.create_task(self._bankroll.run()),
                asyncio.create_task(self._prune_seen_rfq_ids()),
            ]
            try:
                backoff_s = 1.0
                ws_outage_alert_fired = False
                while not self._stopping:
                    try:
                        ws = await self._ws_client.connect()
                        backoff_s = 1.0
                        ws_outage_alert_fired = False
                        try:
                            async for event in self._ws_client.stream_events(ws):
                                self._dispatch(event)
                        finally:
                            await ws.close()
                    except (websockets.exceptions.WebSocketException, OSError) as exc:
                        if self._stopping:
                            break
                        logger.warning("WS connection lost (%s); reconnecting in %.1fs.", exc, backoff_s)
                        if backoff_s >= MAX_RECONNECT_BACKOFF_S and not ws_outage_alert_fired:
                            ws_outage_alert_fired = True
                            await self._notifier.notify(
                                f"RFQ bot: WS reconnect backoff has maxed out ({exc}); "
                                "the bot has been unable to reconnect for a while."
                            )
                        await asyncio.sleep(backoff_s)
                        backoff_s = min(backoff_s * 2, MAX_RECONNECT_BACKOFF_S)
            finally:
                for task in background_tasks:
                    task.cancel()
                for task in background_tasks:
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
        finally:
            await self._http.aclose()

    def _dispatch(self, event) -> None:
        if isinstance(event, RFQBroadcast):
            asyncio.create_task(self._handle_rfq(event))
        elif isinstance(event, QuoteAccepted):
            # Time-critical: must confirm within the exchange's 3-30s window. Its own task,
            # never queued behind orderbook fetches or other in-flight RFQ processing.
            asyncio.create_task(self._handle_quote_accepted(event))
        elif isinstance(event, QuoteExecuted):
            asyncio.create_task(self._handle_quote_executed(event))

    async def _prune_seen_rfq_ids(self) -> None:
        while True:
            await asyncio.sleep(SEEN_RFQ_PRUNE_INTERVAL_S)
            cutoff = time.time() - self._seen_rfq_ttl_s
            stale = [rfq_id for rfq_id, seen_at in self._seen_rfq_ids.items() if seen_at < cutoff]
            for rfq_id in stale:
                del self._seen_rfq_ids[rfq_id]

    async def _handle_rfq(self, event: RFQBroadcast) -> None:
        if event.rfq_id in self._seen_rfq_ids:
            logger.debug("Ignoring duplicate RFQ event for %s (already handled).", event.rfq_id)
            return
        self._seen_rfq_ids[event.rfq_id] = time.time()

        if self._halted:
            logger.warning("Skipping RFQ %s: bot is halted (stop-loss previously triggered).", event.rfq_id)
            return
        if self._stop_loss_floor_usd > 0 and self._bankroll.value <= self._stop_loss_floor_usd:
            self._halted = True
            logger.critical(
                "STOP-LOSS TRIGGERED: cash balance $%.2f <= floor $%.2f. Halting all new RFQ quoting "
                "for the rest of this process's life (restart to resume). Open positions are untouched.",
                self._bankroll.value, self._stop_loss_floor_usd,
            )
            await self._notifier.notify(
                f"RFQ bot STOP-LOSS TRIGGERED: cash balance ${self._bankroll.value:.2f} <= "
                f"floor ${self._stop_loss_floor_usd:.2f}. All new quoting halted; restart to resume."
            )
            return

        try:
            await asyncio.wait_for(self._process_rfq(event), timeout=self._response_deadline_s)
        except asyncio.TimeoutError:
            logger.warning(
                "Skipping RFQ %s: exceeded %.2fs response deadline (network latency).",
                event.rfq_id, self._response_deadline_s,
            )
        except Exception:
            logger.exception("Skipping RFQ %s: unhandled error while processing.", event.rfq_id)

    async def _process_rfq(self, event: RFQBroadcast) -> None:
        if self._mgp_only and legs_share_event(event.leg_tickers):
            logger.info(
                "Skipping RFQ %s: legs share an underlying event (SGP) -- MGP-only mode only "
                "quotes cross-event combos, since same-event legs are typically correlated and "
                "violate the pricing engine's independence assumption.",
                event.rfq_id,
            )
            return

        books = await asyncio.gather(
            *(self._http.get_orderbook(ticker) for ticker in event.leg_tickers)
        )
        leg_vwmids = [compute_vwmid(book, min_size=self._min_leg_quote_size) for book in books]
        if any(mid is None for mid in leg_vwmids):
            logger.info(
                "Skipping RFQ %s: a leg has no tradeable market (empty/crossed/too-thin book).",
                event.rfq_id,
            )
            return

        quote = self._engine.calculate_anchored_quote(leg_vwmids)
        if quote is None:
            logger.info("Skipping RFQ %s: no safe two-sided quote (margin swallows the estimate).", event.rfq_id)
            return
        yes_bid, no_bid = quote

        if event.contracts is not None:
            yes_contracts = no_contracts = event.contracts
        else:
            target = event.target_cost_dollars or 0.0
            yes_contracts = (target / yes_bid) if yes_bid > 0 else 0.0
            no_contracts = (target / no_bid) if no_bid > 0 else 0.0

        if yes_contracts <= 0 and no_contracts <= 0:
            logger.info("Skipping RFQ %s: no priceable side has any contracts to offer.", event.rfq_id)
            return

        yes_liability = contract_liability(yes_bid, yes_contracts) if yes_bid > 0 else 0.0
        no_liability = contract_liability(no_bid, no_contracts) if no_bid > 0 else 0.0
        worst_case_liability = max(yes_liability, no_liability)

        approved = await self._risk_gate.try_reserve(event.market_ticker, worst_case_liability)
        if not approved:
            logger.warning("Skipping RFQ %s: rejected by inventory risk gate (exposure limit).", event.rfq_id)
            return

        try:
            quote_id = await self._http.create_quote(event.rfq_id, yes_bid, no_bid, yes_contracts, no_contracts)
        except Exception:
            self._risk_gate.release(event.market_ticker, worst_case_liability)
            raise

        self._pending_quotes[quote_id] = PendingQuote(
            rfq_id=event.rfq_id,
            market_ticker=event.market_ticker,
            yes_bid=yes_bid,
            no_bid=no_bid,
            yes_contracts=yes_contracts,
            no_contracts=no_contracts,
            worst_case_liability=worst_case_liability,
        )
        logger.info(
            "Quoted RFQ %s: %s yes_bid=%.2f(x%.2f) no_bid=%.2f(x%.2f), worst-case liability $%.2f reserved.",
            event.rfq_id, event.market_ticker, yes_bid, yes_contracts, no_bid, no_contracts, worst_case_liability,
        )
        asyncio.create_task(self._expire_if_not_accepted(quote_id))

    async def _expire_if_not_accepted(self, quote_id: str) -> None:
        await asyncio.sleep(self._quote_acceptance_timeout_s)
        pending = self._pending_quotes.get(quote_id)
        if pending is None or pending.confirmed:
            return  # already resolved (accepted+confirmed, or already cleaned up)
        del self._pending_quotes[quote_id]
        logger.info(
            "Quote %s (RFQ %s) not accepted within %.0fs; releasing reserved liability.",
            quote_id, pending.rfq_id, self._quote_acceptance_timeout_s,
        )
        self._risk_gate.release(pending.market_ticker, pending.worst_case_liability)

    async def _handle_quote_accepted(self, event: QuoteAccepted) -> None:
        pending = self._pending_quotes.get(event.quote_id)
        if pending is None:
            logger.warning(
                "Quote %s (RFQ %s) was accepted but we have no record of it (already expired/resolved?) "
                "-- not confirming, since capacity for it was already released.",
                event.quote_id, event.rfq_id,
            )
            return
        if pending.confirmed:
            logger.debug("Ignoring duplicate quote_accepted for already-confirmed quote %s.", event.quote_id)
            return

        try:
            await self._http.confirm_quote(event.rfq_id, event.quote_id)
        except Exception:
            logger.exception(
                "Failed to confirm quote %s (RFQ %s) within the acceptance window -- the trade likely "
                "did not execute. Releasing reserved liability.",
                event.quote_id, event.rfq_id,
            )
            self._pending_quotes.pop(event.quote_id, None)
            self._risk_gate.release(pending.market_ticker, pending.worst_case_liability)
            await self._notifier.notify(
                f"RFQ bot: failed to confirm accepted quote {event.quote_id} in time -- trade likely lost."
            )
            return

        accepted_price = pending.yes_bid if event.accepted_side == "yes" else pending.no_bid
        actual_liability = contract_liability(accepted_price, event.contracts_accepted)
        excess = pending.worst_case_liability - actual_liability
        if excess > 0:
            self._risk_gate.release(pending.market_ticker, excess)

        pending.worst_case_liability = actual_liability
        pending.confirmed = True
        logger.info(
            "Confirmed quote %s (RFQ %s): accepted_side=%s contracts=%.2f, real liability $%.2f.",
            event.quote_id, event.rfq_id, event.accepted_side, event.contracts_accepted, actual_liability,
        )

    async def _handle_quote_executed(self, event: QuoteExecuted) -> None:
        pending = self._pending_quotes.pop(event.quote_id, None)
        if pending is not None:
            logger.info(
                "RFQ %s quote %s executed as order %s; $%.2f liability is now a confirmed real position.",
                event.rfq_id, event.quote_id, event.order_id, pending.worst_case_liability,
            )


async def main() -> None:
    kalshi_config = KalshiConfig.from_env()
    bot_config = BotConfig()

    logger.info("Starting RFQ bot in %s mode.", "DEMO" if kalshi_config.demo_mode else "LIVE")
    if not kalshi_config.demo_mode:
        logger.warning(
            "LIVE trading mode -- real capital at risk. stop_loss_floor=$%.2f (basis: cash balance).",
            bot_config.stop_loss_floor_usd,
        )

    bot = RFQBot(kalshi_config, bot_config)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    run_task = asyncio.create_task(bot.run())
    stop_wait_task = asyncio.create_task(stop_event.wait())
    done, _pending = await asyncio.wait({run_task, stop_wait_task}, return_when=asyncio.FIRST_COMPLETED)

    if run_task in done and not stop_event.is_set():
        # run() exited on its own -- e.g. startup reconciliation refused to proceed.
        # Surface the failure loudly instead of hanging forever waiting for a stop signal.
        stop_wait_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stop_wait_task
        exc = run_task.exception()
        if exc is not None:
            logger.critical("RFQ bot exited unexpectedly: %s", exc)
            raise exc
        return

    logger.info("Shutdown signal received; stopping RFQ bot.")
    bot.stop()
    run_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await run_task


if __name__ == "__main__":
    asyncio.run(main())
