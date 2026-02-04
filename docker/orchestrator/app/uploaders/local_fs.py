from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path
from typing import Optional

from app.models import UploadResult
from app.uploaders.base import BaseUploader

logger = logging.getLogger(__name__)


class LocalFsUploader(BaseUploader):
    """Copies snapshots to a local directory (for testing)."""

    def __init__(self, dest_dir: str = "/tmp/snapshots") -> None:
        self._dest_dir = Path(dest_dir)

    def _full_path(self, remote_key: str) -> Path:
        return self._dest_dir / remote_key

    async def upload(
        self,
        file_path: Path,
        metadata: dict,
        remote_key: str,
    ) -> UploadResult:
        start = time.monotonic()
        try:
            dest = self._full_path(remote_key)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file_path, dest)

            duration = time.monotonic() - start
            file_size = file_path.stat().st_size
            logger.info(f"Copied snapshot to {dest} ({file_size} bytes)")
            return UploadResult(
                success=True,
                remote_url=str(dest),
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
            dest = self._full_path(remote_key)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)
            return True
        except Exception as e:
            logger.warning(f"put_small_file failed: {e}")
            return False

    async def get_small_file(self, remote_key: str) -> Optional[bytes]:
        try:
            path = self._full_path(remote_key)
            if path.exists():
                return path.read_bytes()
            return None
        except Exception:
            return None

    async def delete_file(self, remote_key: str) -> bool:
        try:
            path = self._full_path(remote_key)
            if path.exists():
                path.unlink()
            return True
        except Exception as e:
            logger.warning(f"delete_file failed: {e}")
            return False

    async def list_remote_dir(self, remote_prefix: str) -> list[str]:
        try:
            path = self._full_path(remote_prefix)
            if path.is_dir():
                return [e.name for e in path.iterdir()]
            return []
        except Exception:
            return []

    async def delete_remote_dir(self, remote_prefix: str) -> bool:
        try:
            path = self._full_path(remote_prefix)
            if path.is_dir():
                shutil.rmtree(path)
            return True
        except Exception as e:
            logger.warning(f"delete_remote_dir failed: {e}")
            return False

    async def check_health(self) -> bool:
        try:
            self._dest_dir.mkdir(parents=True, exist_ok=True)
            return True
        except Exception:
            return False

    def get_name(self) -> str:
        return "local_fs"
