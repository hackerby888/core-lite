from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from app.config import AlertingConfig

logger = logging.getLogger(__name__)


class AlertManager:
    """Sends webhook alerts with rate limiting."""

    def __init__(self, config: AlertingConfig) -> None:
        self._config = config
        self._last_alert_times: dict[str, float] = {}
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def send_alert(
        self,
        level: str,
        event: str,
        details: Optional[dict] = None,
    ) -> None:
        """Send an alert if enabled and not rate-limited."""
        if not self._config.enabled or not self._config.webhook_url:
            logger.info(
                f"Alert ({level}): {event} - {details or {}}"
            )
            return

        # Rate limiting per event type
        now = time.monotonic()
        last_time = self._last_alert_times.get(event, 0)
        if now - last_time < self._config.rate_limit_seconds:
            logger.debug(
                f"Alert rate-limited: {event} "
                f"(last sent {now - last_time:.0f}s ago)"
            )
            return

        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "event": event,
            "details": details or {},
        }

        try:
            session = await self._get_session()
            async with session.post(
                self._config.webhook_url,
                json=payload,
            ) as resp:
                if resp.status < 300:
                    self._last_alert_times[event] = now
                    logger.info(f"Alert sent: {event} ({level})")
                else:
                    logger.warning(
                        f"Alert webhook returned {resp.status}: {event}"
                    )
        except Exception as e:
            logger.warning(f"Failed to send alert: {e}")
