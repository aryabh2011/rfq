"""Minimal Kalshi REST + WebSocket client: RSA-PSS request signing, orderbook
reads, RFQ/quote lifecycle streaming, and two-sided quote submission.

Schema notes (verified against the real demo API and https://docs.kalshi.com,
not guessed): prices are decimal-dollar strings (e.g. "0.4200"), not integer
cents; contract counts are fixed-point strings (e.g. "13.00") and can be
fractional; RFQs are a sealed-bid auction -- makers cannot see each other's
quotes, so a maker's job is to post its own defensible two-sided price, not to
react to a visible competitor.
"""
from __future__ import annotations

import base64
import json
import logging
import time
from dataclasses import dataclass
from typing import AsyncIterator, Dict, List, Optional, Tuple, Union

import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from src.config import KalshiConfig
from src.rate_limiter import TokenBucketRateLimiter

logger = logging.getLogger(__name__)


def _load_private_key(path: str) -> rsa.RSAPrivateKey:
    with open(path, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise TypeError(f"Key at {path} is not an RSA private key.")
    return key


def sign_request(private_key: rsa.RSAPrivateKey, timestamp_ms: str, method: str, path: str) -> str:
    message = f"{timestamp_ms}{method.upper()}{path}".encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


@dataclass(frozen=True)
class OrderbookLevel:
    price: float
    size: float


@dataclass(frozen=True)
class OrderbookSnapshot:
    ticker: str
    yes_levels: List[OrderbookLevel]
    no_levels: List[OrderbookLevel]


@dataclass(frozen=True)
class RFQBroadcast:
    """A new RFQ, broadcast to all makers. `contracts` and `target_cost_dollars` are
    mutually exclusive on the wire -- an RFQ specifies a fixed size OR a dollar budget,
    never both -- so exactly one of these is populated."""
    rfq_id: str
    market_ticker: str
    leg_tickers: List[str]
    contracts: Optional[float]
    target_cost_dollars: Optional[float]


@dataclass(frozen=True)
class QuoteAccepted:
    """The requester accepted one side of a quote we submitted. Must confirm within the
    exchange's confirmation window (3-30s) or the acceptance is voided."""
    quote_id: str
    rfq_id: str
    accepted_side: str
    contracts_accepted: float


@dataclass(frozen=True)
class QuoteExecuted:
    """Final confirmation that an accepted-and-confirmed quote resulted in a real order."""
    quote_id: str
    rfq_id: str
    order_id: str


CommunicationsEvent = Union[RFQBroadcast, QuoteAccepted, QuoteExecuted]

DEFAULT_MIN_QUOTE_SIZE = 5


def _parse_levels(raw_levels: Optional[list]) -> List[OrderbookLevel]:
    """Each level is a two-element string array: [price_dollars, count_fp]."""
    levels: List[OrderbookLevel] = []
    for entry in raw_levels or []:
        try:
            price_str, count_str = entry
            levels.append(OrderbookLevel(price=float(price_str), size=float(count_str)))
        except (ValueError, TypeError):
            logger.warning("Skipping unparseable orderbook level: %s", entry)
    return levels


def compute_vwmid(book: OrderbookSnapshot, min_size: float = DEFAULT_MIN_QUOTE_SIZE) -> Optional[float]:
    """Volume-weighted YES mid, derived from Kalshi's dual bid-only order book.

    Kalshi only posts resting bids for both the YES and NO side of a market
    (there is no separate ask book): the implied YES ask is `1 - best_no_bid`.
    The mid is a microprice, weighting each side's price by the opposing
    side's resting size so it leans toward whichever side has less depth.

    `min_size` is a liquidity gate: since every RFQ triggers a fresh REST fetch, the
    snapshot itself is never stale, but the *resting orders in it* can be -- a single
    leftover 1-lot at a stale price would otherwise be trusted exactly as much as a deep,
    active book. Reject anything thinner than `min_size` on either side rather than price
    off it.
    """
    best_yes_bid = max(book.yes_levels, key=lambda lvl: lvl.price, default=None)
    best_no_bid = max(book.no_levels, key=lambda lvl: lvl.price, default=None)
    if best_yes_bid is None or best_no_bid is None:
        return None

    if best_yes_bid.size < min_size or best_no_bid.size < min_size:
        logger.warning(
            "Insufficient top-of-book liquidity for %s (yes_size=%.2f, no_size=%.2f, min=%.2f); skipping.",
            book.ticker, best_yes_bid.size, best_no_bid.size, min_size,
        )
        return None

    implied_yes_ask = 1.0 - best_no_bid.price
    if implied_yes_ask <= best_yes_bid.price:
        logger.warning("Crossed/locked book for %s (bid=%.2f, implied_ask=%.2f); skipping.",
                        book.ticker, best_yes_bid.price, implied_yes_ask)
        return None

    total_size = best_yes_bid.size + best_no_bid.size
    if total_size == 0:
        return (best_yes_bid.price + implied_yes_ask) / 2.0
    return (best_yes_bid.price * best_no_bid.size + implied_yes_ask * best_yes_bid.size) / total_size


def extract_position_liabilities(positions: List[dict]) -> Tuple[Dict[str, float], bool]:
    """Convert Kalshi position records into {ticker: worst-case dollar liability}.

    Used for startup reconciliation: seed the risk gate with what we actually hold before
    accepting any new quotes, instead of assuming a blank slate after every restart.

    NOTE: uses the `market_exposure` field (cents), Kalshi's documented worst-case-loss
    figure for a position -- verify this field name against current docs before relying on
    it for real capital. Returns `fully_parsed=False` (rather than silently treating missing
    data as zero exposure) if any position record can't be read, so the caller can refuse to
    proceed on an ambiguous reconciliation instead of starting with an under-counted ledger.
    """
    liabilities: Dict[str, float] = {}
    fully_parsed = True
    for position in positions:
        ticker = position.get("ticker")
        exposure_cents = position.get("market_exposure")
        if not ticker or exposure_cents is None:
            logger.warning("Unparseable position record during reconciliation: %s", position)
            fully_parsed = False
            continue
        liabilities[ticker] = liabilities.get(ticker, 0.0) + abs(exposure_cents) / 100.0
    return liabilities, fully_parsed


class KalshiHttpClient:
    """Signed REST access for orderbook reads and RFQ quote lifecycle actions."""

    def __init__(
        self,
        config: KalshiConfig,
        timeout_s: float = 2.0,
        max_connections: int = 200,
        max_keepalive_connections: int = 50,
        max_read_requests_per_second: float = 24.0,
        max_write_requests_per_second: float = 24.0,
    ) -> None:
        # httpx's default pool (100 connections) can be exhausted by a burst of RFQs each
        # firing 1-2 concurrent orderbook fetches -- observed directly: a WS backlog replay
        # of 600+ RFQs on fresh subscription caused real ConnectTimeouts waiting for a pool
        # slot. Raised generously here as a second line of defense; the primary defense is
        # the rate limiters below, since a bigger pool alone just shifts the failure from our
        # own ConnectTimeout to a 429 from Kalshi's server-side rate limit (also observed).
        #
        # Kalshi's rate limits are two independent token buckets (read/write), confirmed via
        # docs.kalshi.com/getting_started/rate_limits: Basic tier is 200/100 tokens/sec,
        # Advanced (free, one API call, no approval -- see upgrade_api_usage_level) is
        # 300/300. Most requests cost 10 tokens, so that's ~30 req/s each on Advanced; these
        # defaults sit at 80% of that as a safety margin, and are safely under Basic tier too
        # in case the upgrade call ever fails.
        self._config = config
        self._private_key = _load_private_key(config.private_key_path)
        limits = httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_keepalive_connections)
        self._client = httpx.AsyncClient(base_url=config.rest_host, timeout=timeout_s, limits=limits)
        self._read_rate_limiter = TokenBucketRateLimiter(max_read_requests_per_second)
        self._write_rate_limiter = TokenBucketRateLimiter(max_write_requests_per_second)

    def _path(self, suffix: str) -> str:
        return self._config.rest_path_prefix + suffix

    def _auth_headers(self, method: str, full_path: str) -> dict:
        timestamp_ms = str(int(time.time() * 1000))
        signature = sign_request(self._private_key, timestamp_ms, method, full_path)
        return {
            "KALSHI-ACCESS-KEY": self._config.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        }

    @staticmethod
    def _warn_if_rate_limited(response: httpx.Response) -> None:
        if response.status_code == 429:
            logger.warning("Rate limited by Kalshi (429) on %s %s", response.request.method, response.request.url)

    async def _get(self, path: str, headers: dict, params: Optional[dict] = None) -> httpx.Response:
        await self._read_rate_limiter.acquire()
        response = await self._client.get(path, params=params, headers=headers)
        self._warn_if_rate_limited(response)
        return response

    async def _post(self, path: str, json_body: dict, headers: dict) -> httpx.Response:
        await self._write_rate_limiter.acquire()
        response = await self._client.post(path, json=json_body, headers=headers)
        self._warn_if_rate_limited(response)
        return response

    async def _put(self, path: str, json_body: dict, headers: dict) -> httpx.Response:
        await self._write_rate_limiter.acquire()
        response = await self._client.put(path, json=json_body, headers=headers)
        self._warn_if_rate_limited(response)
        return response

    async def get_orderbook(self, ticker: str, depth: int = 10) -> OrderbookSnapshot:
        path = self._path(f"/markets/{ticker}/orderbook")
        headers = self._auth_headers("GET", path)
        response = await self._get(path, headers, params={"depth": depth})
        response.raise_for_status()
        book = response.json().get("orderbook_fp") or {}
        return OrderbookSnapshot(
            ticker=ticker,
            yes_levels=_parse_levels(book.get("yes_dollars")),
            no_levels=_parse_levels(book.get("no_dollars")),
        )

    async def create_quote(
        self,
        rfq_id: str,
        yes_bid: float,
        no_bid: float,
        yes_contracts_offered: float,
        no_contracts_offered: float,
    ) -> str:
        """Submit a two-sided quote in response to an RFQ. Returns the new quote's id.

        A quote is implicitly for the full RFQ amount split across whichever contracts
        count each side's price implies -- there is no separate "size" parameter beyond
        the two `*_contracts_offered` fields.
        """
        path = self._path("/communications/quotes")
        body = {
            "rfq_id": rfq_id,
            "yes_bid": f"{yes_bid:.2f}",
            "no_bid": f"{no_bid:.2f}",
            "yes_contracts_offered_fp": f"{yes_contracts_offered:.2f}",
            "no_contracts_offered_fp": f"{no_contracts_offered:.2f}",
            "rest_remainder": False,
        }
        headers = self._auth_headers("POST", path)
        response = await self._post(path, body, headers)
        response.raise_for_status()
        return response.json()["id"]

    async def confirm_quote(self, rfq_id: str, quote_id: str) -> None:
        """Confirm an accepted quote. Must happen within the exchange's confirmation
        window (3-30s depending on market volatility) or the acceptance is voided."""
        path = self._path(f"/communications/rfqs/{rfq_id}/quotes/{quote_id}/confirm")
        headers = self._auth_headers("PUT", path)
        response = await self._put(path, {}, headers)
        response.raise_for_status()

    async def upgrade_api_usage_level(self) -> None:
        """Upgrade to the Advanced API rate-limit tier (300/300 read/write tokens-per-sec vs
        Basic's 200/100) -- free, no approval needed, and confirmed idempotent (calling it
        again when already at or above Advanced just succeeds again with no error). Safe to
        call unconditionally on every startup rather than tracking whether it's needed.
        """
        path = self._path("/account/api_usage_level/upgrade")
        headers = self._auth_headers("POST", path)
        response = await self._post(path, {}, headers)
        response.raise_for_status()

    async def get_balance_usd(self) -> float:
        """Live settled cash balance, in dollars. Used to size risk caps and drive the stop-loss halt."""
        path = self._path("/portfolio/balance")
        headers = self._auth_headers("GET", path)
        response = await self._get(path, headers)
        response.raise_for_status()
        return response.json()["balance"] / 100.0

    async def get_positions(self) -> List[dict]:
        """Open market positions. Used for startup reconciliation against real, live exposure."""
        path = self._path("/portfolio/positions")
        headers = self._auth_headers("GET", path)
        response = await self._get(path, headers)
        response.raise_for_status()
        body = response.json()
        return body.get("market_positions") or body.get("positions") or []

    async def get_fills(self, ticker: str, min_ts: int) -> List[dict]:
        """Fills for `ticker` at or after `min_ts` (unix seconds). Not on the primary
        quote-confirmation path (that's event-driven via quote_accepted/quote_executed
        now) -- kept as a secondary cross-check available for reconciliation.
        """
        path = self._path("/portfolio/fills")
        headers = self._auth_headers("GET", path)
        response = await self._get(path, headers, params={"ticker": ticker, "min_ts": min_ts})
        response.raise_for_status()
        return response.json().get("fills", [])

    async def aclose(self) -> None:
        await self._client.aclose()


class KalshiWebSocketClient:
    """Authenticated WebSocket connection streaming Kalshi's `communications` channel:
    RFQ broadcasts, plus lifecycle events for quotes we've submitted."""

    def __init__(self, config: KalshiConfig) -> None:
        self._config = config
        self._private_key = _load_private_key(config.private_key_path)

    def _auth_headers(self) -> dict:
        timestamp_ms = str(int(time.time() * 1000))
        signature = sign_request(self._private_key, timestamp_ms, "GET", self._config.ws_path)
        return {
            "KALSHI-ACCESS-KEY": self._config.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        }

    async def connect(self) -> websockets.WebSocketClientProtocol:
        url = self._config.ws_host + self._config.ws_path
        ws = await websockets.connect(url, extra_headers=self._auth_headers(), ping_interval=10)
        await ws.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["communications"]}}))
        logger.info("Connected and subscribed to Kalshi communications channel.")
        return ws

    async def stream_events(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterator[CommunicationsEvent]:
        async for raw_message in ws:
            try:
                message = json.loads(raw_message)
            except json.JSONDecodeError:
                logger.warning("Dropping malformed WS message (not JSON): %.200s", raw_message)
                continue
            event = self._parse_message(message)
            if event is not None:
                yield event

    @staticmethod
    def _parse_message(message: dict) -> Optional[CommunicationsEvent]:
        msg_type = message.get("type")
        body = message.get("msg") or {}
        try:
            if msg_type == "rfq_created":
                legs = body.get("mve_selected_legs") or []
                leg_tickers = [leg["market_ticker"] for leg in legs] or [body["market_ticker"]]
                contracts = float(body["contracts_fp"]) if body.get("contracts_fp") is not None else None
                target_cost_dollars = (
                    float(body["target_cost_dollars"]) if body.get("target_cost_dollars") is not None else None
                )
                if (contracts is None or contracts <= 0) and (target_cost_dollars is None or target_cost_dollars <= 0):
                    logger.warning(
                        "Dropping RFQ %s: neither a positive contracts_fp nor target_cost_dollars present.",
                        body.get("id"),
                    )
                    return None
                return RFQBroadcast(
                    rfq_id=str(body["id"]),
                    market_ticker=body["market_ticker"],
                    leg_tickers=leg_tickers,
                    contracts=contracts,
                    target_cost_dollars=target_cost_dollars,
                )
            if msg_type == "quote_accepted":
                return QuoteAccepted(
                    quote_id=str(body["quote_id"]),
                    rfq_id=str(body["rfq_id"]),
                    accepted_side=body["accepted_side"],
                    contracts_accepted=float(body.get("contracts_accepted_fp", 0.0)),
                )
            if msg_type == "quote_executed":
                return QuoteExecuted(
                    quote_id=str(body["quote_id"]),
                    rfq_id=str(body["rfq_id"]),
                    order_id=str(body.get("order_id", "")),
                )
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Dropping unparseable %s message (%s): %s", msg_type, exc, body)
            return None
        # subscribed / rfq_deleted / quote_created (echo of our own or others' submissions,
        # not actionable for a sealed-bid maker) / anything else -- not handled here.
        return None
