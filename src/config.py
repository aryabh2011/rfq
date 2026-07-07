"""Runtime configuration, sourced from environment variables (see .env.example)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    return float(val) if val is not None else default


@dataclass(frozen=True)
class KalshiConfig:
    api_key_id: str = field(default_factory=lambda: os.environ["KALSHI_API_KEY_ID"])
    private_key_path: str = field(default_factory=lambda: os.environ["KALSHI_PRIVATE_KEY_PATH"])
    rest_host: str = field(
        default_factory=lambda: os.environ.get("KALSHI_REST_HOST", "https://api.elections.kalshi.com")
    )
    ws_host: str = field(
        default_factory=lambda: os.environ.get("KALSHI_WS_HOST", "wss://api.elections.kalshi.com")
    )


@dataclass(frozen=True)
class BotConfig:
    fee_buffer: float = field(default_factory=lambda: _env_float("RFQ_FEE_BUFFER", 0.02))
    max_exposure_pct_per_contract: float = field(
        default_factory=lambda: _env_float("RFQ_MAX_EXPOSURE_PCT_PER_CONTRACT", 0.01)
    )
    max_exposure_pct_per_prefix: float = field(
        default_factory=lambda: _env_float("RFQ_MAX_EXPOSURE_PCT_PER_PREFIX", 0.03)
    )
    rfq_response_deadline_s: float = field(
        default_factory=lambda: _env_float("RFQ_RESPONSE_DEADLINE_S", 0.75)
    )
    bankroll_usd: float = field(default_factory=lambda: _env_float("RFQ_BANKROLL_USD", 10_000.0))
