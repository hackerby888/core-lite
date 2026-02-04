from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Protocol, Tuple, runtime_checkable

from app.models import UploadResult


class BaseUploader(ABC):
    """Abstract base for snapshot uploaders."""

    @abstractmethod
    async def upload(
        self,
        file_path: Path,
        metadata: dict,
        remote_key: str,
    ) -> UploadResult:
        """Upload a snapshot archive to remote storage.

        Args:
            file_path: Local path to the archive file.
            metadata: Dict with keys: epoch, tick, timestamp, checksum.
            remote_key: Remote path (e.g., "198/ep198-t43101500-snap.zip").

        Returns:
            UploadResult with success status and details.
        """
        ...

    @abstractmethod
    async def put_small_file(self, remote_key: str, content: bytes) -> bool:
        """Write a small file (lock, metadata JSON, index) to remote storage."""
        ...

    @abstractmethod
    async def get_small_file(self, remote_key: str) -> Optional[bytes]:
        """Read a small file from remote storage. Returns None if not found."""
        ...

    @abstractmethod
    async def delete_file(self, remote_key: str) -> bool:
        """Delete a file from remote storage."""
        ...

    @abstractmethod
    async def check_health(self) -> bool:
        """Check if the upload destination is reachable."""
        ...

    @abstractmethod
    def get_name(self) -> str:
        """Human-readable name for this uploader."""
        ...

    async def list_remote_dir(self, remote_prefix: str) -> list[str]:
        """List entries in a remote directory.

        Args:
            remote_prefix: Relative path within dest (e.g., "" for root,
                           "198" for an epoch directory).

        Returns:
            List of entry names (files and dirs) in the directory.
            Returns empty list if unsupported or on error.
        """
        return []

    async def delete_remote_dir(self, remote_prefix: str) -> bool:
        """Delete a remote directory tree.

        Returns True on success, False if unsupported or on error.
        """
        return False

    async def close(self) -> None:
        """Clean up resources."""
        pass


@runtime_checkable
class RemotePackagingUploader(Protocol):
    """Protocol for uploaders that sync a snapshot directory and package remotely.

    Uploaders implementing this skip local ZIP packaging; instead they
    rsync the epoch snapshot directory to a persistent remote directory
    and then create the ZIP archive on the remote server.
    """

    async def sync_and_package(
        self,
        snap_dir: Path,
        epoch: int,
        tick: int,
    ) -> UploadResult:
        """Sync snapshot directory to remote, then create ZIP archive."""
        ...

    async def get_remote_checksum(
        self, remote_key: str
    ) -> Tuple[str, int]:
        """Compute SHA-256 checksum and file size of a remote file.

        Returns (hex_checksum, size_bytes).
        """
        ...
