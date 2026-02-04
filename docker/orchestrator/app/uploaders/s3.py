from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

from app.models import UploadResult
from app.uploaders.base import BaseUploader

logger = logging.getLogger(__name__)


class S3Uploader(BaseUploader):
    """Uploads snapshots to S3-compatible storage."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "snapshots/",
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

    def _client_kwargs(self) -> dict:
        kwargs: dict = {"region_name": self._region}
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        if self._access_key:
            kwargs["aws_access_key_id"] = self._access_key
            kwargs["aws_secret_access_key"] = self._secret_key
        return kwargs

    def _key(self, remote_key: str) -> str:
        return self._prefix + remote_key

    def _get_aioboto3(self):
        try:
            import aioboto3
            return aioboto3
        except ImportError:
            raise RuntimeError(
                "aioboto3 is required for S3 uploads. "
                "Install with: pip install qubic-orchestrator[s3]"
            )

    async def upload(
        self,
        file_path: Path,
        metadata: dict,
        remote_key: str,
    ) -> UploadResult:
        try:
            aioboto3 = self._get_aioboto3()
        except RuntimeError as e:
            return UploadResult(success=False, error_message=str(e))

        start = time.monotonic()
        file_size = file_path.stat().st_size
        key = self._key(remote_key)

        try:
            session = aioboto3.Session()
            async with session.client("s3", **self._client_kwargs()) as s3:
                logger.info(
                    f"Uploading {file_path.name} to "
                    f"s3://{self._bucket}/{key} ({file_size} bytes)"
                )
                await s3.upload_file(str(file_path), self._bucket, key)

            duration = time.monotonic() - start
            remote_url = f"s3://{self._bucket}/{key}"
            logger.info(f"Upload complete: {remote_url} ({duration:.1f}s)")
            return UploadResult(
                success=True,
                remote_url=remote_url,
                bytes_uploaded=file_size,
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
            aioboto3 = self._get_aioboto3()
            session = aioboto3.Session()
            async with session.client("s3", **self._client_kwargs()) as s3:
                await s3.put_object(
                    Bucket=self._bucket,
                    Key=self._key(remote_key),
                    Body=content,
                )
            return True
        except Exception as e:
            logger.warning(f"put_small_file failed: {e}")
            return False

    async def get_small_file(self, remote_key: str) -> Optional[bytes]:
        try:
            aioboto3 = self._get_aioboto3()
            session = aioboto3.Session()
            async with session.client("s3", **self._client_kwargs()) as s3:
                resp = await s3.get_object(
                    Bucket=self._bucket,
                    Key=self._key(remote_key),
                )
                return await resp["Body"].read()
        except Exception:
            return None

    async def delete_file(self, remote_key: str) -> bool:
        try:
            aioboto3 = self._get_aioboto3()
            session = aioboto3.Session()
            async with session.client("s3", **self._client_kwargs()) as s3:
                await s3.delete_object(
                    Bucket=self._bucket,
                    Key=self._key(remote_key),
                )
            return True
        except Exception as e:
            logger.warning(f"delete_file failed: {e}")
            return False

    async def check_health(self) -> bool:
        try:
            aioboto3 = self._get_aioboto3()
            session = aioboto3.Session()
            async with session.client("s3", **self._client_kwargs()) as s3:
                await s3.head_bucket(Bucket=self._bucket)
                return True
        except Exception:
            return False

    def get_name(self) -> str:
        return "s3"
