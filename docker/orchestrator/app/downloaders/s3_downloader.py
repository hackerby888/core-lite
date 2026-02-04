from __future__ import annotations

import logging
from pathlib import Path

from app.downloaders.base import BaseDownloader

logger = logging.getLogger(__name__)


class S3Downloader(BaseDownloader):
    """Downloads files from S3-compatible storage."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        endpoint_url: str = "",
        region: str = "us-east-1",
        access_key: str = "",
        secret_key: str = "",
    ) -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._endpoint_url = endpoint_url
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key

    async def download(self, url: str, dest_path: Path) -> Path:
        # url is used as the S3 key in this context
        try:
            import aioboto3
        except ImportError:
            raise RuntimeError(
                "aioboto3 is required for S3 downloads. "
                "Install with: pip install qubic-orchestrator[s3]"
            )

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        key = self._prefix + url.lstrip("/")

        session = aioboto3.Session()
        kwargs: dict = {"region_name": self._region}
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        if self._access_key:
            kwargs["aws_access_key_id"] = self._access_key
            kwargs["aws_secret_access_key"] = self._secret_key

        async with session.client("s3", **kwargs) as s3:
            logger.info(f"Downloading s3://{self._bucket}/{key} to {dest_path}")
            await s3.download_file(self._bucket, key, str(dest_path))
            logger.info(f"S3 download complete: {dest_path}")

        return dest_path

    async def close(self) -> None:
        pass
