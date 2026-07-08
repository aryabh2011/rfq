"""Unhedged-inventory risk gate for RFQ market making.

Since the bot never hedges, this module is the only thing standing between a
mispriced/adverse-selected fill and real capital loss. It enforces a per-contract
exposure cap and a per-underlying-event concentration cap so a burst of correlated
RFQs (e.g. every quarter of the same game) can't stack liability past what the
bankroll can absorb.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, Sequence

logger = logging.getLogger(__name__)


def default_event_prefix(ticker: str) -> str:
    """Strip the final outcome-specific segment, leaving the underlying event id.

    E.g. "KXNFLGAME-24DEC25KCMIA-KC" -> "KXNFLGAME-24DEC25KCMIA", so all
    outcomes of a single game (or a single macro event) share one concentration
    bucket. Pass a custom `prefix_extractor` if your ticker scheme differs.
    """
    parts = ticker.split("-")
    return "-".join(parts[:-1]) if len(parts) > 1 else ticker


def legs_share_event(
    leg_tickers: Sequence[str], prefix_extractor: Callable[[str], str] = default_event_prefix
) -> bool:
    """True if two or more legs of a combo belong to the same underlying event.

    Same-event legs (a same-game parlay) are typically correlated, which violates
    the pricing engine's independence assumption and is a well-known adverse-selection
    vector (SGP correlation arbitrage). Cross-event legs (a multi-game parlay) are a much
    safer fit for that assumption -- this is the model-free, zero-research gate used to
    restrict quoting to MGP-style combos only.
    """
    prefixes = [prefix_extractor(ticker) for ticker in leg_tickers]
    return len(set(prefixes)) < len(prefixes)


def contract_liability(price: float, contracts: float) -> float:
    """Worst-case dollar liability for a BUYER: pay `price` now, lose the entire amount if
    it resolves against you. (The bot buys YES or NO from the RFQ requester -- Kalshi's RFQ
    quotes are two-sided bids, not offers to sell -- so this is buyer risk, not seller risk:
    a seller's worst case would be `1 - price`, but that's not our role here.)
    """
    return price * contracts


@dataclass
class RFQInventoryManager:
    """Tracks open unhedged liability and gates new quotes against bankroll-relative caps."""

    get_bankroll: Callable[[], float]
    max_exposure_pct_per_contract: float = 0.01
    max_exposure_pct_per_prefix: float = 0.03
    # Absolute dollar floor under each cap: at a small bankroll, a pure percentage cap can
    # round down to cents (e.g. 1% of $52 is $0.52), which is smaller than a single realistic
    # contract's liability and would reject nearly every quote. The floor keeps the bot
    # functional at small scale; once bankroll * pct exceeds it, the percentage takes back
    # over automatically -- no need to remember to raise these again as the bankroll grows.
    min_notional_cap_per_contract_usd: float = 0.0
    min_notional_cap_per_prefix_usd: float = 0.0
    prefix_extractor: Callable[[str], str] = default_event_prefix

    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _liability_by_ticker: Dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _liability_by_prefix: Dict[str, float] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if not 0.0 < self.max_exposure_pct_per_contract <= 1.0:
            raise ValueError("max_exposure_pct_per_contract must be in (0, 1]")
        if self.max_exposure_pct_per_prefix < self.max_exposure_pct_per_contract:
            raise ValueError("max_exposure_pct_per_prefix must be >= max_exposure_pct_per_contract")
        if self.min_notional_cap_per_contract_usd < 0.0 or self.min_notional_cap_per_prefix_usd < 0.0:
            raise ValueError("min_notional_cap_*_usd must be >= 0")
        if self.min_notional_cap_per_prefix_usd < self.min_notional_cap_per_contract_usd:
            raise ValueError("min_notional_cap_per_prefix_usd must be >= min_notional_cap_per_contract_usd")

    async def try_reserve(self, ticker: str, liability: float) -> bool:
        """Atomically check both caps and reserve `liability` dollars if it fits. Returns approval.

        Takes a raw dollar liability rather than (price, contracts): a two-sided RFQ quote
        has a different price *and* a different contracts-offered count on each side, so the
        caller (which knows both) computes the worst-case liability across both sides before
        calling this -- the risk gate itself doesn't need to know how liability was derived.
        """
        prefix = self.prefix_extractor(ticker)
        bankroll = self.get_bankroll()
        contract_cap = max(bankroll * self.max_exposure_pct_per_contract, self.min_notional_cap_per_contract_usd)
        prefix_cap = max(bankroll * self.max_exposure_pct_per_prefix, self.min_notional_cap_per_prefix_usd)

        async with self._lock:
            existing_ticker_liability = self._liability_by_ticker.get(ticker, 0.0)
            existing_prefix_liability = self._liability_by_prefix.get(prefix, 0.0)

            if existing_ticker_liability + liability > contract_cap:
                logger.warning(
                    "Rejecting quote for %s: liability $%.2f would exceed per-contract cap $%.2f "
                    "(max of %.2f%% of $%.2f bankroll and $%.2f floor).",
                    ticker, existing_ticker_liability + liability, contract_cap,
                    self.max_exposure_pct_per_contract * 100, bankroll, self.min_notional_cap_per_contract_usd,
                )
                return False

            if existing_prefix_liability + liability > prefix_cap:
                logger.warning(
                    "Rejecting quote for %s: prefix '%s' liability $%.2f would exceed concentration "
                    "cap $%.2f (max of %.2f%% of $%.2f bankroll and $%.2f floor).",
                    ticker, prefix, existing_prefix_liability + liability, prefix_cap,
                    self.max_exposure_pct_per_prefix * 100, bankroll, self.min_notional_cap_per_prefix_usd,
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

    def release(self, ticker: str, liability: float) -> None:
        """Free previously reserved `liability` dollars, e.g. after settlement or a failed submission."""
        prefix = self.prefix_extractor(ticker)
        self._liability_by_ticker[ticker] = max(0.0, self._liability_by_ticker.get(ticker, 0.0) - liability)
        self._liability_by_prefix[prefix] = max(0.0, self._liability_by_prefix.get(prefix, 0.0) - liability)
        logger.info("Released $%.2f liability for %s (prefix '%s').", liability, ticker, prefix)

    def seed(self, liability_by_ticker: Dict[str, float]) -> None:
        """Overwrite the ledger with known real liability (dollars per ticker).

        Used once at startup, before any new quotes are accepted, to reconcile against
        live positions -- otherwise a restarted process would start believing it holds
        zero risk regardless of what it actually still has open on the exchange.
        """
        self._liability_by_ticker = dict(liability_by_ticker)
        self._liability_by_prefix = {}
        for ticker, liability in liability_by_ticker.items():
            prefix = self.prefix_extractor(ticker)
            self._liability_by_prefix[prefix] = self._liability_by_prefix.get(prefix, 0.0) + liability
        logger.info(
            "Risk gate seeded with $%.2f of real liability across %d tickers (%d prefixes).",
            sum(liability_by_ticker.values()), len(liability_by_ticker), len(self._liability_by_prefix),
        )
