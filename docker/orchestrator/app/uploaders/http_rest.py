from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

import aiohttp

from app.models import UploadResult
from app.uploaders.base import BaseUploader

logger = logging.getLogger(__name__)


class HttpRestUploader(BaseUploader):
    """Uploads snapshots via HTTP POST to a REST endpoint."""

    def __init__(
        self,
        upload_url: str,
        auth_token: str = "",
        timeout: int = 1800,
    ) -> None:
        self._upload_url = upload_url.rstrip("/")
        self._auth_token = auth_token
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self._auth_token:
            headers["Authorization"] = f"Bearer {self._auth_token}"
        return headers

    async def upload(
        self,
        file_path: Path,
        metadata: dict,
        remote_key: str,
    ) -> UploadResult:
        start = time.monotonic()
        file_size = file_path.stat().st_size

        session = await self._get_session()
        data = aiohttp.FormData()
        data.add_field(
            "file",
            open(file_path, "rb"),
            filename=file_path.name,
            content_type="application/zip",
        )
        data.add_field("metadata", str(metadata))
        data.add_field("remote_key", remote_key)

        try:
            async with session.post(
                f"{self._upload_url}/upload",
                data=data,
                headers=self._headers(),
            ) as resp:
                duration = time.monotonic() - start
                if resp.status < 300:
                    body = await resp.json()
                    return UploadResult(
                        success=True,
                        remote_url=body.get("url", ""),
                        bytes_uploaded=file_size,
                        duration_seconds=duration,
                    )
                else:
                    text = await resp.text()
                    return UploadResult(
                        success=False,
                        error_message=f"HTTP {resp.status}: {text}",
                        duration_seconds=duration,
                    )
        except Exception as e:
            return UploadResult(
                success=False,
                error_message=str(e),
                duration_seconds=time.monotonic() - start,
            )

    async def put_small_file(self, remote_key: str, content: bytes) -> bool:
        try:
            session = await self._get_session()
            async with session.put(
                f"{self._upload_url}/files/{remote_key}",
                data=content,
                headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                return resp.status < 300
        except Exception as e:
            logger.warning(f"put_small_file failed: {e}")
            return False

    async def get_small_file(self, remote_key: str) -> Optional[bytes]:
        try:
            session = await self._get_session()
            async with session.get(
                f"{self._upload_url}/files/{remote_key}",
                headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    return await resp.read()
                return None
        except Exception:
            return None

    async def delete_file(self, remote_key: str) -> bool:
        try:
            session = await self._get_session()
            async with session.delete(
                f"{self._upload_url}/files/{remote_key}",
                headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                return resp.status < 300
        except Exception as e:
            logger.warning(f"delete_file failed: {e}")
            return False

    async def check_health(self) -> bool:
        if not self._upload_url:
            return False
        try:
            session = await self._get_session()
            async with session.head(
                self._upload_url,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                return resp.status < 500
        except Exception:
            return False

    def get_name(self) -> str:
        return "http_rest"

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
