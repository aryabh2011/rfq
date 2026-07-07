"""Minimal Kalshi REST + WebSocket client: RSA-PSS request signing, orderbook
reads, RFQ event streaming, and quote submission.

NOTE: the RFQ WebSocket channel name and message schema below follow Kalshi's
documented `rfq` channel shape as of this writing. Kalshi's RFQ API is a
newer surface than the core trading API -- verify field names against the
current API docs before trusting this in production.
"""
from __future__ import annotations

import base64
import json
import logging
import time
from dataclasses import dataclass
from typing import AsyncIterator, List, Optional

import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from src.config import KalshiConfig

logger = logging.getLogger(__name__)

API_PREFIX = "/trade-api/v2"
WS_PATH = "/trade-api/ws/v2"


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
    size: int


@dataclass(frozen=True)
class OrderbookSnapshot:
    ticker: str
    yes_levels: List[OrderbookLevel]
    no_levels: List[OrderbookLevel]


@dataclass(frozen=True)
class RFQEvent:
    rfq_id: str
    ticker: str
    leg_tickers: List[str]
    side: str
    best_competitor_quote: float
    contracts: int


def compute_vwmid(book: OrderbookSnapshot) -> Optional[float]:
    """Volume-weighted YES mid, derived from Kalshi's dual bid-only order book.

    Kalshi only posts resting bids for both the YES and NO side of a market
    (there is no separate ask book): the implied YES ask is `1 - best_no_bid`.
    The mid is a microprice, weighting each side's price by the opposing
    side's resting size so it leans toward whichever side has less depth.
    """
    best_yes_bid = max(book.yes_levels, key=lambda lvl: lvl.price, default=None)
    best_no_bid = max(book.no_levels, key=lambda lvl: lvl.price, default=None)
    if best_yes_bid is None or best_no_bid is None:
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


class KalshiHttpClient:
    """Signed REST access for orderbook reads and RFQ quote submission."""

    def __init__(self, config: KalshiConfig, timeout_s: float = 2.0) -> None:
        self._config = config
        self._private_key = _load_private_key(config.private_key_path)
        self._client = httpx.AsyncClient(base_url=config.rest_host, timeout=timeout_s)

    def _auth_headers(self, method: str, full_path: str) -> dict:
        timestamp_ms = str(int(time.time() * 1000))
        signature = sign_request(self._private_key, timestamp_ms, method, full_path)
        return {
            "KALSHI-ACCESS-KEY": self._config.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        }

    async def get_orderbook(self, ticker: str, depth: int = 10) -> OrderbookSnapshot:
        path = f"{API_PREFIX}/markets/{ticker}/orderbook"
        headers = self._auth_headers("GET", path)
        response = await self._client.get(path, params={"depth": depth}, headers=headers)
        response.raise_for_status()
        book = response.json()["orderbook"]
        return OrderbookSnapshot(
            ticker=ticker,
            yes_levels=[OrderbookLevel(price=price / 100.0, size=size) for price, size in book.get("yes") or []],
            no_levels=[OrderbookLevel(price=price / 100.0, size=size) for price, size in book.get("no") or []],
        )

    async def submit_rfq_quote(self, rfq_id: str, ticker: str, side: str, price: float, contracts: int) -> dict:
        path = f"{API_PREFIX}/rfqs/{rfq_id}/quotes"
        body = {
            "ticker": ticker,
            "side": side,
            "price": round(price * 100),
            "count": contracts,
        }
        headers = self._auth_headers("POST", path)
        response = await self._client.post(path, json=body, headers=headers)
        response.raise_for_status()
        return response.json()

    async def aclose(self) -> None:
        await self._client.aclose()


class KalshiWebSocketClient:
    """Authenticated WebSocket connection streaming RFQ broadcast events."""

    def __init__(self, config: KalshiConfig) -> None:
        self._config = config
        self._private_key = _load_private_key(config.private_key_path)

    def _auth_headers(self) -> dict:
        timestamp_ms = str(int(time.time() * 1000))
        signature = sign_request(self._private_key, timestamp_ms, "GET", WS_PATH)
        return {
            "KALSHI-ACCESS-KEY": self._config.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        }

    async def connect(self) -> websockets.WebSocketClientProtocol:
        url = self._config.ws_host + WS_PATH
        ws = await websockets.connect(url, extra_headers=self._auth_headers(), ping_interval=10)
        await ws.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["rfq"]}}))
        logger.info("Connected and subscribed to Kalshi RFQ channel.")
        return ws

    async def stream_rfq_events(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterator[RFQEvent]:
        async for raw_message in ws:
            try:
                message = json.loads(raw_message)
            except json.JSONDecodeError:
                logger.warning("Dropping malformed WS message (not JSON): %.200s", raw_message)
                continue
            event = self._parse_rfq_event(message)
            if event is not None:
                yield event

    @staticmethod
    def _parse_rfq_event(message: dict) -> Optional[RFQEvent]:
        if message.get("type") not in ("rfq_created", "rfq_updated", "rfq"):
            return None
        body = message.get("msg", message)
        try:
            return RFQEvent(
                rfq_id=str(body["rfq_id"]),
                ticker=body["ticker"],
                leg_tickers=list(body["leg_tickers"]),
                side=body["side"],
                best_competitor_quote=float(body["best_quote"]) / 100.0,
                contracts=int(body["contracts"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Dropping unparseable RFQ message (%s): %s", exc, body)
            return None
