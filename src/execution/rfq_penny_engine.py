"""Model-free linear anchor for pennying Kalshi RFQ parlay quotes.

No copula, no correlation prior: independence is used only to derive a
conservative *floor*, and the rarest-leg inequality is used to derive an
absolute *ceiling*. Both are model-free (true for any joint distribution,
not just the independent case).
"""
from __future__ import annotations

import logging
from math import prod
from typing import Literal, Optional, Sequence

logger = logging.getLogger(__name__)

Side = Literal["yes", "no"]

MIN_PRICE = 0.01
MAX_PRICE = 0.99
PENNY_INCREMENT = 0.01


class MinimalistPennyingEngine:
    """Computes a fee- and toxicity-adjusted pennying quote for a parlay RFQ."""

    def __init__(self, fee_buffer: float = 0.02) -> None:
        if not 0.0 <= fee_buffer < 1.0:
            raise ValueError(f"fee_buffer must be in [0, 1), got {fee_buffer}")
        self.fee_buffer = fee_buffer

    def calculate_safe_quote(
        self,
        leg_mid_prices: Sequence[float],
        best_competitor_quote: float,
        side: Side,
    ) -> Optional[float]:
        """Return a safe quote to penny `best_competitor_quote`, or None if unsafe to quote."""
        if side not in ("yes", "no"):
            raise ValueError(f"side must be 'yes' or 'no', got {side!r}")
        if not leg_mid_prices:
            logger.warning("Rejecting RFQ quote: no leg mid prices supplied.")
            return None
        if any(not (0.0 < p < 1.0) for p in leg_mid_prices):
            logger.warning("Rejecting RFQ quote: leg mid prices out of (0,1) bounds: %s", leg_mid_prices)
            return None
        if not (0.0 < best_competitor_quote < 1.0):
            logger.warning("Rejecting RFQ quote: competitor quote %.4f out of (0,1) bounds.", best_competitor_quote)
            return None

        independent_joint_prob = prod(leg_mid_prices)
        rarest_leg = min(leg_mid_prices)

        if side == "yes":
            return self._quote_yes(independent_joint_prob, rarest_leg, best_competitor_quote)
        return self._quote_no(independent_joint_prob, rarest_leg, best_competitor_quote)

    def _quote_yes(
        self, independent_joint_prob: float, rarest_leg: float, best_competitor_quote: float
    ) -> Optional[float]:
        # A joint AND event can never be more likely than its rarest leg.
        absolute_ceiling = min(rarest_leg, MAX_PRICE)
        hard_floor = max(independent_joint_prob + self.fee_buffer, MIN_PRICE)

        if hard_floor >= absolute_ceiling:
            logger.info(
                "Skipping YES quote: hard_floor %.4f >= absolute_ceiling %.4f "
                "(independent_joint_prob=%.4f, rarest_leg=%.4f) -- market too thin/toxic to quote.",
                hard_floor, absolute_ceiling, independent_joint_prob, rarest_leg,
            )
            return None

        target_quote = best_competitor_quote - PENNY_INCREMENT
        safe_quote = max(target_quote, hard_floor)

        if safe_quote >= absolute_ceiling:
            logger.info(
                "Skipping YES quote: competitor %.4f leaves no room under ceiling %.4f "
                "after floor enforcement (safe_quote=%.4f).",
                best_competitor_quote, absolute_ceiling, safe_quote,
            )
            return None

        if target_quote < hard_floor:
            logger.info(
                "Competitor YES quote %.4f squeezed below our floor %.4f; quoting floor instead of pennying.",
                best_competitor_quote, hard_floor,
            )

        return round(safe_quote, 2)

    def _quote_no(
        self, independent_joint_prob: float, rarest_leg: float, best_competitor_quote: float
    ) -> Optional[float]:
        independent_no_baseline = 1.0 - independent_joint_prob
        # Mirror of the YES ceiling: since P(all yes) <= rarest_leg, P(not all yes) >= 1 - rarest_leg.
        # This is a hard physical floor independent of the fee-adjusted margin floor below.
        absolute_no_floor_physical = 1.0 - rarest_leg
        hard_floor = max(independent_no_baseline + self.fee_buffer, absolute_no_floor_physical, MIN_PRICE)
        absolute_ceiling = MAX_PRICE

        if hard_floor >= absolute_ceiling:
            logger.info(
                "Skipping NO quote: hard_floor %.4f >= absolute_ceiling %.4f "
                "(independent_no_baseline=%.4f, rarest_leg=%.4f) -- market too thin/toxic to quote.",
                hard_floor, absolute_ceiling, independent_no_baseline, rarest_leg,
            )
            return None

        target_quote = best_competitor_quote - PENNY_INCREMENT
        safe_quote = max(target_quote, hard_floor)

        if safe_quote >= absolute_ceiling:
            logger.info(
                "Skipping NO quote: competitor %.4f leaves no room under ceiling %.4f "
                "after floor enforcement (safe_quote=%.4f).",
                best_competitor_quote, absolute_ceiling, safe_quote,
            )
            return None

        if target_quote < hard_floor:
            logger.info(
                "Competitor NO quote %.4f squeezed below our floor %.4f; quoting floor instead of pennying.",
                best_competitor_quote, hard_floor,
            )

        return round(safe_quote, 2)
