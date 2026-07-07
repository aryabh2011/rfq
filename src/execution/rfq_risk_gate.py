"""Unhedged-inventory risk gate for RFQ market making.

Since the pennying engine never hedges, this module is the only thing standing
between a mispriced/adverse-selected fill and real capital loss. It enforces a
per-contract exposure cap and a per-underlying-event concentration cap so a
burst of correlated RFQs (e.g. every quarter of the same game) can't stack
liability past what the bankroll can absorb.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict

logger = logging.getLogger(__name__)


def default_event_prefix(ticker: str) -> str:
    """Strip the final outcome-specific segment, leaving the underlying event id.

    E.g. "KXNFLGAME-24DEC25KCMIA-KC" -> "KXNFLGAME-24DEC25KCMIA", so all
    outcomes of a single game (or a single macro event) share one concentration
    bucket. Pass a custom `prefix_extractor` if your ticker scheme differs.
    """
    parts = ticker.split("-")
    return "-".join(parts[:-1]) if len(parts) > 1 else ticker


def contract_liability(price: float, contracts: int) -> float:
    """Worst-case dollar liability for a seller: premium collected is `price`, max payout is $1/contract."""
    return (1.0 - price) * contracts


@dataclass
class RFQInventoryManager:
    """Tracks open unhedged liability and gates new quotes against bankroll-relative caps."""

    get_bankroll: Callable[[], float]
    max_exposure_pct_per_contract: float = 0.01
    max_exposure_pct_per_prefix: float = 0.03
    prefix_extractor: Callable[[str], str] = default_event_prefix

    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _liability_by_ticker: Dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _liability_by_prefix: Dict[str, float] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if not 0.0 < self.max_exposure_pct_per_contract <= 1.0:
            raise ValueError("max_exposure_pct_per_contract must be in (0, 1]")
        if self.max_exposure_pct_per_prefix < self.max_exposure_pct_per_contract:
            raise ValueError("max_exposure_pct_per_prefix must be >= max_exposure_pct_per_contract")

    async def try_reserve(self, ticker: str, price: float, contracts: int) -> bool:
        """Atomically check both caps and reserve liability if the quote fits. Returns approval."""
        liability = contract_liability(price, contracts)
        prefix = self.prefix_extractor(ticker)
        bankroll = self.get_bankroll()
        contract_cap = bankroll * self.max_exposure_pct_per_contract
        prefix_cap = bankroll * self.max_exposure_pct_per_prefix

        async with self._lock:
            existing_ticker_liability = self._liability_by_ticker.get(ticker, 0.0)
            existing_prefix_liability = self._liability_by_prefix.get(prefix, 0.0)

            if existing_ticker_liability + liability > contract_cap:
                logger.warning(
                    "Rejecting quote for %s: liability $%.2f would exceed per-contract cap "
                    "$%.2f (%.2f%% of $%.2f bankroll).",
                    ticker, existing_ticker_liability + liability, contract_cap,
                    self.max_exposure_pct_per_contract * 100, bankroll,
                )
                return False

            if existing_prefix_liability + liability > prefix_cap:
                logger.warning(
                    "Rejecting quote for %s: prefix '%s' liability $%.2f would exceed concentration "
                    "cap $%.2f (%.2f%% of $%.2f bankroll).",
                    ticker, prefix, existing_prefix_liability + liability, prefix_cap,
                    self.max_exposure_pct_per_prefix * 100, bankroll,
                )
                return False

            self._liability_by_ticker[ticker] = existing_ticker_liability + liability
            self._liability_by_prefix[prefix] = existing_prefix_liability + liability
            logger.info(
                "Reserved $%.2f liability for %s (prefix '%s'); running totals: contract=$%.2f/%.2f prefix=$%.2f/%.2f",
                liability, ticker, prefix,
                self._liability_by_ticker[ticker], contract_cap,
                self._liability_by_prefix[prefix], prefix_cap,
            )
            return True

    def release(self, ticker: str, price: float, contracts: int) -> None:
        """Free previously reserved liability, e.g. after settlement or a failed quote submission."""
        liability = contract_liability(price, contracts)
        prefix = self.prefix_extractor(ticker)
        self._liability_by_ticker[ticker] = max(0.0, self._liability_by_ticker.get(ticker, 0.0) - liability)
        self._liability_by_prefix[prefix] = max(0.0, self._liability_by_prefix.get(prefix, 0.0) - liability)
        logger.info("Released $%.2f liability for %s (prefix '%s').", liability, ticker, prefix)
