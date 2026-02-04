from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import aiohttp

from app.downloaders.base import BaseDownloader

logger = logging.getLogger(__name__)


class HttpDownloader(BaseDownloader):
    """Downloads files via HTTP GET with retry logic."""

    def __init__(
        self,
        timeout: int = 3600,
        retry_count: int = 5,
        retry_delay: int = 60,
    ) -> None:
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._retry_count = retry_count
        self._retry_delay = retry_delay
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def download(self, url: str, dest_path: Path) -> Path:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        last_error: Exception | None = None

        for attempt in range(self._retry_count + 1):
            try:
                session = await self._get_session()
                logger.info(
                    f"Downloading {url} (attempt {attempt + 1}/{self._retry_count + 1})"
                )
                async with session.get(url) as resp:
                    if resp.status != 200:
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message=f"HTTP {resp.status}",
                        )
                    total = int(resp.headers.get("Content-Length", 0))
                    downloaded = 0
                    with open(dest_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(
                            1024 * 1024
                        ):
                            f.write(chunk)
                            downloaded += len(chunk)
                    if total and downloaded != total:
                        raise IOError(
                            f"Incomplete download: {downloaded}/{total} bytes"
                        )
                    logger.info(
                        f"Downloaded {downloaded} bytes to {dest_path}"
                    )
                    return dest_path

            except Exception as e:
                last_error = e
                logger.warning(
                    f"Download attempt {attempt + 1} failed: {e}"
                )
                # Clean up partial file
                if dest_path.exists():
                    dest_path.unlink()
                if attempt < self._retry_count:
                    logger.info(f"Retrying in {self._retry_delay}s...")
                    await asyncio.sleep(self._retry_delay)

        raise RuntimeError(
            f"Download failed after {self._retry_count + 1} attempts: {last_error}"
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
