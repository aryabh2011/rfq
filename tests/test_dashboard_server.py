import asyncio
import json

from aiohttp.test_utils import TestClient, TestServer

from src.activity_feed import ActivityFeed
from src.dashboard_server import DashboardServer


async def _make_client(feed: ActivityFeed, state: dict) -> TestClient:
    server = DashboardServer(feed, lambda: state, host="127.0.0.1", port=0)
    client = TestClient(TestServer(server._app))
    await client.start_server()
    return client


async def test_index_serves_html():
    client = await _make_client(ActivityFeed(), {})
    try:
        resp = await client.get("/")
        assert resp.status == 200
        assert "RFQ Bot" in await resp.text()
    finally:
        await client.close()


async def test_state_endpoint_returns_snapshot():
    state = {
        "mode": "LIVE", "bankroll_usd": 52.48, "halted": False,
        "total_reserved_liability": 10.0, "open_ticker_count": 1, "pending_quotes": 2,
    }
    client = await _make_client(ActivityFeed(), state)
    try:
        resp = await client.get("/state")
        assert resp.status == 200
        assert await resp.json() == state
    finally:
        await client.close()


async def test_events_endpoint_replays_history_on_connect():
    feed = ActivityFeed()
    feed.publish("rfq_quoted", rfq_id="abc")
    client = await _make_client(feed, {})
    try:
        resp = await client.get("/events")
        assert resp.status == 200
        line = await asyncio.wait_for(resp.content.readline(), timeout=2.0)
        assert line.startswith(b"data: ")
        payload = json.loads(line[len(b"data: "):])
        assert payload == {"type": "rfq_quoted", "rfq_id": "abc", "ts": payload["ts"]}
    finally:
        resp.close()
        await client.close()


async def test_events_endpoint_streams_new_events_live():
    feed = ActivityFeed()
    client = await _make_client(feed, {})
    try:
        resp = await client.get("/events")
        assert resp.status == 200
        await asyncio.sleep(0.05)  # let the handler reach queue.get() before we publish
        feed.publish("quote_confirmed", quote_id="q1", rfq_id="r1", accepted_side="yes", contracts=1.0, liability=0.5)
        line = await asyncio.wait_for(resp.content.readline(), timeout=2.0)
        payload = json.loads(line[len(b"data: "):])
        assert payload["type"] == "quote_confirmed"
        assert payload["quote_id"] == "q1"
    finally:
        resp.close()
        await client.close()
