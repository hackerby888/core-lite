from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.config import DownloaderConfig

from app.downloaders.base import BaseDownloader


def create_downloader(config: DownloaderConfig) -> BaseDownloader:
    if config.type == "http":
        from app.downloaders.http_downloader import HttpDownloader

        return HttpDownloader(
            timeout=config.timeout_seconds,
            retry_count=config.retry_count,
            retry_delay=config.retry_delay_seconds,
        )
    elif config.type == "s3":
        from app.downloaders.s3_downloader import S3Downloader

        return S3Downloader(
            bucket=config.s3_bucket,
            prefix=config.s3_prefix,
            endpoint_url=config.s3_endpoint_url,
            region=config.s3_region,
            access_key=config.s3_access_key,
            secret_key=config.s3_secret_key,
        )
    else:
        raise ValueError(f"Unknown downloader type: {config.type}")
