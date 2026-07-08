"""In-memory pub-sub for bot activity, feeding the local read-only dashboard.

Purely an observability side-channel -- it must never be able to affect, block, or
slow down the bot's actual trading logic. Publishing is synchronous and non-blocking;
a slow or stuck dashboard client drops events rather than backing up the publisher.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any, Deque, Dict, List

DEFAULT_HISTORY_SIZE = 200
SUBSCRIBER_QUEUE_SIZE = 1000


class ActivityFeed:
    def __init__(self, history_size: int = DEFAULT_HISTORY_SIZE) -> None:
        self._history: Deque[Dict[str, Any]] = deque(maxlen=history_size)
        self._subscribers: List[asyncio.Queue] = []

    def publish(self, event_type: str, **fields: Any) -> None:
        event = {"type": event_type, "ts": time.time(), **fields}
        self._history.append(event)
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # a slow/stuck dashboard client must never back-pressure the bot

    def history(self) -> List[Dict[str, Any]]:
        return list(self._history)

    def subscribe(self) -> "asyncio.Queue[Dict[str, Any]]":
        queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=SUBSCRIBER_QUEUE_SIZE)
        self._subscribers.append(queue)
        return queue

    def unsubscribe(self, queue: "asyncio.Queue[Dict[str, Any]]") -> None:
        if queue in self._subscribers:
            self._subscribers.remove(queue)
