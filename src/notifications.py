"""Best-effort external alerting for events a human should see outside the log stream.

Posts a Slack-compatible webhook payload (`{"text": ...}`) -- most alerting relays
(Slack, Mattermost, many Discord-to-Slack bridges) accept this shape. For a native
Discord webhook, swap the payload key to `"content"`. If no URL is configured, this
is a no-op: alerting is a convenience layer, never a dependency for the bot to run.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class Notifier:
    def __init__(self, webhook_url: Optional[str], timeout_s: float = 5.0) -> None:
        self._webhook_url = webhook_url
        self._timeout_s = timeout_s

    async def notify(self, message: str) -> None:
        if not self._webhook_url:
            return
        try:
            async with httpx.AsyncClient(timeout=self._timeout_s) as client:
                response = await client.post(self._webhook_url, json={"text": message})
                response.raise_for_status()
        except Exception:
            logger.warning("Failed to deliver alert webhook (message was: %s)", message, exc_info=True)
