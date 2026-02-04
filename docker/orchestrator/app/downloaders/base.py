from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class BaseDownloader(ABC):
    """Abstract base for downloading epoch files and snapshots."""

    @abstractmethod
    async def download(self, url: str, dest_path: Path) -> Path:
        """Download a file from the given URL to dest_path.

        Returns the path to the downloaded file.
        Raises on failure after retries.
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources."""
        ...
