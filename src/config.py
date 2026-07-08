"""Runtime configuration, sourced from environment variables / a local .env file
(see .env.example). Credentials must never be hardcoded here or checked into a
tracked config file -- they belong in the gitignored `.env` only.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlsplit

from dotenv import load_dotenv

load_dotenv()


def _env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    return float(val) if val is not None else default


def _env_bool(name: str, default: bool) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() not in ("false", "0", "no", "")


def _env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    return int(val) if val is not None else default


def _env_str(name: str, default: Optional[str]) -> Optional[str]:
    val = os.environ.get(name)
    return val if val else default


def _split_base_url(url: str) -> tuple[str, str]:
    """Split a full base URL into (scheme://host, path_prefix).

    Lets REST/WS request paths be built and RSA-PSS signed against the exact
    same path string, regardless of how much prefix the configured host URL
    already includes (avoids the httpx base_url + leading-slash join gotcha).
    """
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}", parts.path.rstrip("/")


@dataclass(frozen=True)
class KalshiConfig:
    demo_mode: bool
    api_key_id: str
    private_key_path: str
    rest_host: str
    rest_path_prefix: str
    ws_host: str
    ws_path: str

    @staticmethod
    def from_env() -> "KalshiConfig":
        # Defaults to demo mode no matter what any other bot's config says --
        # a fresh, less battle-tested execution path should never inherit `live` implicitly.
        demo_mode = _env_bool("KALSHI_DEMO_MODE", default=True)
        if demo_mode:
            api_key_id = os.environ["KALSHI_DEMO_API_KEY_ID"]
            private_key_path = os.environ["KALSHI_DEMO_API_KEY_FILE"]
            base_url = os.environ.get("KALSHI_DEMO_BASE_URL", "https://external-api.demo.kalshi.co/trade-api/v2")
            ws_url = os.environ.get("KALSHI_DEMO_WS_URL", "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2")
        else:
            api_key_id = os.environ["KALSHI_LIVE_API_KEY_ID"]
            private_key_path = os.environ["KALSHI_LIVE_API_KEY_FILE"]
            base_url = os.environ.get("KALSHI_BASE_URL", "https://external-api.kalshi.com/trade-api/v2")
            ws_url = os.environ.get("KALSHI_WS_URL", "wss://external-api-ws.kalshi.com/trade-api/ws/v2")

        rest_host, rest_path_prefix = _split_base_url(base_url)
        ws_host, ws_path = _split_base_url(ws_url)
        return KalshiConfig(
            demo_mode=demo_mode,
            api_key_id=api_key_id,
            private_key_path=private_key_path,
            rest_host=rest_host,
            rest_path_prefix=rest_path_prefix,
            ws_host=ws_host,
            ws_path=ws_path,
        )


@dataclass(frozen=True)
class BotConfig:
    fee_buffer: float = field(default_factory=lambda: _env_float("RFQ_FEE_BUFFER", 0.02))
    # MGP-only: refuse combos where two or more legs share the same underlying event (e.g. a
    # same-game parlay). Those legs are typically correlated, which the pricing engine's
    # independence-based estimate doesn't account for -- a well-known adverse-selection vector.
    # Cross-event combos (multi-game parlays) are a much safer fit for that assumption.
    mgp_only: bool = field(default_factory=lambda: _env_bool("RFQ_MGP_ONLY", True))
    # Extra fee_buffer added per leg beyond `fee_buffer_base_legs`, since independence error
    # compounds as legs are added. Defaults to 0.0 (no scaling) -- there's no empirical basis
    # here for the "right" scaling constant, so this is a deliberate no-guess default, not an
    # oversight. Set explicitly once you have a view on it.
    fee_buffer_per_extra_leg: float = field(default_factory=lambda: _env_float("RFQ_FEE_BUFFER_PER_EXTRA_LEG", 0.0))
    fee_buffer_base_legs: int = field(default_factory=lambda: _env_int("RFQ_FEE_BUFFER_BASE_LEGS", 2))
    # Reject a leg's price if its top-of-book size on either side is thinner than this --
    # guards against a single stale/leftover resting order being trusted as real liquidity.
    min_leg_quote_size: int = field(default_factory=lambda: _env_int("RFQ_MIN_LEG_QUOTE_SIZE", 5))
    max_exposure_pct_per_contract: float = field(
        default_factory=lambda: _env_float("RFQ_MAX_EXPOSURE_PCT_PER_CONTRACT", 0.01)
    )
    max_exposure_pct_per_prefix: float = field(
        default_factory=lambda: _env_float("RFQ_MAX_EXPOSURE_PCT_PER_PREFIX", 0.03)
    )
    # Absolute dollar floor under the two caps above. At a small bankroll, a pure percentage
    # (e.g. 1% of $52 = $0.52) can be smaller than a single realistic contract's liability and
    # reject nearly everything. 0.0 = no floor (pure percentage, original behavior) -- set these
    # deliberately for your actual bankroll; there's no universal "right" default here.
    min_notional_cap_per_contract_usd: float = field(
        default_factory=lambda: _env_float("RFQ_MIN_NOTIONAL_CAP_PER_CONTRACT_USD", 0.0)
    )
    min_notional_cap_per_prefix_usd: float = field(
        default_factory=lambda: _env_float("RFQ_MIN_NOTIONAL_CAP_PER_PREFIX_USD", 0.0)
    )
    rfq_response_deadline_s: float = field(
        default_factory=lambda: _env_float("RFQ_RESPONSE_DEADLINE_S", 0.75)
    )
    # How long to wait for a `quote_accepted` event before giving up on a submitted quote and
    # releasing its reserved liability. This is a safety-net timeout, not the exchange's own
    # confirmation window (which is a much shorter 3-30s, starts only after acceptance, and is
    # handled immediately/unconditionally the instant `quote_accepted` arrives).
    quote_acceptance_timeout_s: float = field(
        default_factory=lambda: _env_float("RFQ_QUOTE_ACCEPTANCE_TIMEOUT_S", 120.0)
    )
    # How long to remember an rfq_id for duplicate-event suppression (WS redelivery on reconnect).
    seen_rfq_ttl_s: float = field(default_factory=lambda: _env_float("RFQ_SEEN_TTL_S", 3600.0))
    # Used only until the first live balance fetch succeeds, and as a fallback if refreshes fail.
    fallback_bankroll_usd: float = field(default_factory=lambda: _env_float("RFQ_BANKROLL_USD", 100.0))
    bankroll_refresh_interval_s: float = field(
        default_factory=lambda: _env_float("RFQ_BANKROLL_REFRESH_INTERVAL_S", 30.0)
    )
    # Sticky capital-preservation halt: once live cash balance <= this floor, the bot stops
    # quoting new RFQs for the rest of the process's life (no auto-recovery). 0 disables it.
    # Basis is cash balance only -- this bot does not mark open positions to market.
    stop_loss_floor_usd: float = field(default_factory=lambda: _env_float("RFQ_STOP_LOSS_FLOOR_USD", 0.0))
    # Slack-compatible webhook URL for critical alerts (stop-loss trigger, startup
    # reconciliation failure, prolonged WS outage, repeated fill-check failures). None disables
    # alerting entirely -- it's a convenience layer, never a hard dependency.
    alert_webhook_url: Optional[str] = field(default_factory=lambda: _env_str("RFQ_ALERT_WEBHOOK_URL", None))
