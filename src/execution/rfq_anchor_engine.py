"""Model-free linear anchor for two-sided Kalshi RFQ quotes.

Kalshi's RFQs are a sealed-bid auction: makers cannot see each other's quotes, so
there is nothing to "penny." Each maker must post its own defensible two-sided
price -- a bid to buy YES and a bid to buy NO on the same combo -- derived purely
from its own fair-value estimate. This is standard practice for any RFQ/OTC market
where quotes are private (bond desks, FX, etc.): you don't compete by shading a
visible price, you compete by being the most consistently accurate quoter.

No copula, no correlation prior: independence gives the fair-value estimate, and
the rarest-leg inequality still bounds it (a joint AND can never be more likely
than its rarest leg), though that bound is automatically satisfied by construction
once a margin is subtracted -- see calculate_anchored_quote for why.
"""
from __future__ import annotations

import logging
import math
from math import prod
from typing import Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

MIN_PRICE = 0.01


def _round_down_to_cents(value: float) -> float:
    """Round down to the nearest whole cent, snapping out float noise first.

    Used for bid prices: we must never submit a bid ABOVE what we calculated as
    safe. A naive round(x, 2) can round UP across a half-cent boundary due to
    float representation noise (e.g. 0.415 stored as 0.41500000000000004),
    overpaying by up to half a cent. Snapping to 6 decimal places in cent-space
    first recovers the intended value before rounding down.
    """
    cents = round(value * 100, 6)
    return math.floor(cents) / 100.0


class MinimalistPennyingEngine:
    """Computes a fee-adjusted, model-free two-sided anchor quote for a parlay RFQ.

    (Class name kept for continuity with prior discussion of this bot; it no longer
    "pennies" anything -- see module docstring for why that mechanism doesn't exist
    on Kalshi's RFQ product.)
    """

    def __init__(self, fee_buffer: float = 0.02, fee_buffer_per_extra_leg: float = 0.0, base_legs: int = 2) -> None:
        if not 0.0 <= fee_buffer < 1.0:
            raise ValueError(f"fee_buffer must be in [0, 1), got {fee_buffer}")
        if fee_buffer_per_extra_leg < 0.0:
            raise ValueError(f"fee_buffer_per_extra_leg must be >= 0, got {fee_buffer_per_extra_leg}")
        if base_legs < 1:
            raise ValueError(f"base_legs must be >= 1, got {base_legs}")
        self.fee_buffer = fee_buffer
        # Independence error compounds as legs are added, and the base fee_buffer doesn't
        # scale on its own. This lets the margin widen with leg count -- defaults to 0.0
        # (no scaling) since there's no empirical basis here for the "right" scaling
        # constant; that's a deliberate no-guess default, not an oversight.
        self.fee_buffer_per_extra_leg = fee_buffer_per_extra_leg
        self.base_legs = base_legs

    def _effective_fee_buffer(self, num_legs: int) -> float:
        extra_legs = max(0, num_legs - self.base_legs)
        return self.fee_buffer + self.fee_buffer_per_extra_leg * extra_legs

    def calculate_anchored_quote(self, leg_mid_prices: Sequence[float]) -> Optional[Tuple[float, float]]:
        """Return (yes_bid, no_bid) to submit directly as the RFQ quote, or None if unpriceable.

        yes_bid = fair value (independent product) minus margin: the most we're willing
        to pay to buy YES. no_bid mirrors it for NO. Because both are the *same* fair
        estimate minus the *same* margin on complementary probabilities, yes_bid + no_bid
        = 1 - 2*margin, which automatically satisfies Kalshi's required yes_bid+no_bid<=1
        coherence constraint, and both bids are automatically below the rarest-leg ceiling
        (since the independent product of probabilities in [0,1] is never greater than
        their minimum) -- no separate ceiling check is needed on this side of the trade.

        Either side is submitted as 0.0 (decline that side) if margin swallows the whole
        estimate; if both would be declined, there's nothing safe to quote at all.
        """
        if not leg_mid_prices:
            logger.warning("Rejecting RFQ quote: no leg mid prices supplied.")
            return None
        if any(not (0.0 < p < 1.0) for p in leg_mid_prices):
            logger.warning("Rejecting RFQ quote: leg mid prices out of (0,1) bounds: %s", leg_mid_prices)
            return None

        independent_joint_prob = prod(leg_mid_prices)
        fee_buffer = self._effective_fee_buffer(len(leg_mid_prices))

        yes_bid_raw = independent_joint_prob - fee_buffer
        no_bid_raw = (1.0 - independent_joint_prob) - fee_buffer

        yes_bid = _round_down_to_cents(yes_bid_raw) if yes_bid_raw >= MIN_PRICE else 0.0
        no_bid = _round_down_to_cents(no_bid_raw) if no_bid_raw >= MIN_PRICE else 0.0

        if yes_bid <= 0.0 and no_bid <= 0.0:
            logger.info(
                "Skipping RFQ quote: margin swallows the entire estimate on both sides "
                "(independent_joint_prob=%.4f, fee_buffer=%.4f) -- nothing safe to bid.",
                independent_joint_prob, fee_buffer,
            )
            return None

        return yes_bid, no_bid
