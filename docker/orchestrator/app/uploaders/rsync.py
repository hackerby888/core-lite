from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Tuple

from app.models import UploadResult
from app.uploaders.scp import ScpUploader

logger = logging.getLogger(__name__)


class RsyncUploader(ScpUploader):
    """Uploads snapshots via rsync (delta transfer) + remote ZIP packaging.

    Inherits all SSH/SCP infrastructure from ScpUploader.
    Overrides the upload flow to:
      1. rsync the epoch snapshot directory to a persistent remote directory
      2. Create ZIP archive on the remote server via SSH
      3. Compute checksum and size remotely
    """

    def __init__(
        self,
        host: str,
        user: str = "",
        port: int = 22,
        dest_path: str = "/snapshots",
        key_file: str = "",
        timeout: int = 1800,
        bandwidth_limit: int = 0,
        compress: bool = False,
    ) -> None:
        super().__init__(
            host=host,
            user=user,
            port=port,
            dest_path=dest_path,
            key_file=key_file,
            timeout=timeout,
        )
        self._bandwidth_limit = bandwidth_limit  # KB/s, 0 = unlimited
        self._compress = compress

    def _rsync_opts(self) -> list[str]:
        """Build rsync option list using the same SSH transport."""
        ssh_cmd = "ssh " + " ".join(self._ssh_opts())
        opts = [
            "-a",           # archive mode (recursive, preserves permissions)
            "--delete",     # remove files on dest that don't exist on source
            "--inplace",    # update files in place (avoid temp copies of large files)
            "--stats",      # show transfer statistics
            "-e", ssh_cmd,  # use SSH with our options
        ]
        if self._compress:
            opts.append("-z")
        if self._bandwidth_limit > 0:
            opts.extend(["--bwlimit", str(self._bandwidth_limit)])
        return opts

    def _remote_staging_path(self, epoch: int) -> str:
        """Persistent remote directory for rsync'd files."""
        return f"{self._dest_path}/{epoch}/snapshot-files"

    async def sync_and_package(
        self,
        snap_dir: Path,
        epoch: int,
        tick: int,
    ) -> UploadResult:
        """Rsync snapshot directory to remote, then ZIP remotely."""
        start = time.monotonic()
        remote_staging = self._remote_staging_path(epoch)
        archive_name = f"ep{epoch}-t{tick}-snap.zip"
        remote_archive = f"{self._dest_path}/{epoch}/{archive_name}"

        try:
            # Step 1: Ensure remote staging directory exists
            code, _, stderr = await self._run_ssh(
                "mkdir", "-p", remote_staging,
                timeout=30,
            )
            if code != 0:
                return UploadResult(
                    success=False,
                    error_message=(
                        f"Failed to create remote staging dir: "
                        f"{stderr.strip()}"
                    ),
                    duration_seconds=time.monotonic() - start,
                )

            # Step 2: Rsync snapshot directory
            rsync_result = await self._rsync_directory(
                snap_dir, remote_staging
            )
            if not rsync_result.success:
                return UploadResult(
                    success=False,
                    error_message=f"rsync failed: {rsync_result.error_message}",
                    duration_seconds=time.monotonic() - start,
                )

            logger.info(
                f"rsync complete: {rsync_result.bytes_uploaded} bytes "
                f"transferred ({rsync_result.duration_seconds:.1f}s)"
            )

            # Step 3: Create ZIP on remote server
            # Use a temp file and atomic mv to avoid partial downloads
            zip_start = time.monotonic()
            remote_tmp = f"{remote_archive}.tmp"
            zip_cmd = (
                f"cd {remote_staging} && "
                f"zip -r {remote_tmp} . && "
                f"mv -f {remote_tmp} {remote_archive}"
            )
            code, stdout, stderr = await self._run_ssh(
                zip_cmd,
                timeout=self._timeout,
            )
            zip_duration = time.monotonic() - zip_start
            if code != 0:
                return UploadResult(
                    success=False,
                    error_message=(
                        f"Remote zip failed (exit {code}): "
                        f"{stderr.strip()}"
                    ),
                    duration_seconds=time.monotonic() - start,
                )

            logger.info(
                f"Remote ZIP created: {remote_archive} "
                f"({zip_duration:.1f}s)"
            )

            duration = time.monotonic() - start
            return UploadResult(
                success=True,
                remote_url=f"{self._target()}:{remote_archive}",
                bytes_uploaded=rsync_result.bytes_uploaded,
                duration_seconds=duration,
            )

        except asyncio.TimeoutError as e:
            return UploadResult(
                success=False,
                error_message=f"Timeout: {e}",
                duration_seconds=time.monotonic() - start,
            )
        except Exception as e:
            return UploadResult(
                success=False,
                error_message=str(e),
                duration_seconds=time.monotonic() - start,
            )

    async def _rsync_directory(
        self,
        snap_dir: Path,
        remote_staging: str,
    ) -> UploadResult:
        """Rsync a local directory to the remote staging path."""
        start = time.monotonic()

        # Trailing / on source syncs directory contents (not the dir itself)
        source = str(snap_dir).rstrip("/") + "/"
        rsync_target = f"{self._target()}:{remote_staging}/"

        cmd = ["rsync"] + self._rsync_opts() + [source, rsync_target]

        logger.info(f"Running rsync: {source} -> {rsync_target}")
        logger.debug(f"rsync command: {' '.join(cmd)}")

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(),
                    timeout=self._timeout,
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                return UploadResult(
                    success=False,
                    error_message=(
                        f"rsync timed out after {self._timeout}s"
                    ),
                    duration_seconds=time.monotonic() - start,
                )

            duration = time.monotonic() - start

            if proc.returncode == 0:
                bytes_transferred = self._parse_rsync_bytes(
                    stdout.decode() + stderr.decode()
                )
                return UploadResult(
                    success=True,
                    bytes_uploaded=bytes_transferred,
                    duration_seconds=duration,
                )
            else:
                err = stderr.decode().strip()
                return UploadResult(
                    success=False,
                    error_message=(
                        f"rsync exit {proc.returncode}: {err}"
                    ),
                    duration_seconds=duration,
                )
        except Exception as e:
            return UploadResult(
                success=False,
                error_message=str(e),
                duration_seconds=time.monotonic() - start,
            )

    @staticmethod
    def _parse_rsync_bytes(output: str) -> int:
        """Parse 'Total bytes sent: N' from --stats (actual wire bytes)."""
        for line in output.splitlines():
            stripped = line.strip()
            if stripped.startswith("Total bytes sent:"):
                parts = stripped.split(":")
                if len(parts) >= 2:
                    num_str = parts[1].strip().split()[0].replace(",", "")
                    try:
                        return int(num_str)
                    except ValueError:
                        pass
        return 0

    async def get_remote_checksum(
        self, remote_key: str
    ) -> Tuple[str, int]:
        """Compute SHA-256 checksum and file size of a remote file."""
        remote_path = self._remote_path(remote_key)
        cmd = (
            f"sha256sum {remote_path} && stat -c '%s' {remote_path}"
        )
        code, stdout, stderr = await self._run_ssh(
            cmd,
            timeout=300,
        )
        if code != 0:
            raise RuntimeError(
                f"Failed to compute remote checksum: {stderr.strip()}"
            )

        lines = stdout.strip().splitlines()
        checksum = lines[0].split()[0]
        size_bytes = int(lines[1].strip())
        return checksum, size_bytes

    def get_name(self) -> str:
        return "rsync"
