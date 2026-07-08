"""Local, read-only web dashboard for watching bot activity live.

Binds to localhost only by default -- deliberately, since this exposes real
trading activity and (state snapshot) bankroll figures. Strictly observational:
it only ever reads from an ActivityFeed and a state-snapshot callable, and has
no route that can call back into the bot's trading logic. If this server hangs
or crashes, it must never take the bot down with it -- it runs as an independent
background task, started and stopped around the bot's own lifecycle.
"""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from aiohttp import web

from src.activity_feed import ActivityFeed

logger = logging.getLogger(__name__)

_TEMPLATE_PATH = Path(__file__).parent / "dashboard" / "index.html"


class DashboardServer:
    def __init__(
        self,
        feed: ActivityFeed,
        get_state: Callable[[], Dict[str, Any]],
        host: str = "127.0.0.1",
        port: int = 8765,
    ) -> None:
        self._feed = feed
        self._get_state = get_state
        self._host = host
        self._port = port
        self._app = web.Application()
        self._app.router.add_get("/", self._handle_index)
        self._app.router.add_get("/state", self._handle_state)
        self._app.router.add_get("/events", self._handle_events)
        self._runner: Optional[web.AppRunner] = None

    async def _handle_index(self, request: web.Request) -> web.Response:
        return web.Response(text=_TEMPLATE_PATH.read_text(), content_type="text/html")

    async def _handle_state(self, request: web.Request) -> web.Response:
        return web.json_response(self._get_state())

    async def _handle_events(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)

        for event in self._feed.history():
            await response.write(f"data: {json.dumps(event)}\n\n".encode())

        queue = self._feed.subscribe()
        try:
            while True:
                event = await queue.get()
                await response.write(f"data: {json.dumps(event)}\n\n".encode())
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        finally:
            self._feed.unsubscribe(queue)
        return response

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        logger.info("Dashboard running at http://%s:%d", self._host, self._port)

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
