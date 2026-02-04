from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

from app.models import UploadResult
from app.uploaders.base import BaseUploader

logger = logging.getLogger(__name__)


class ScpUploader(BaseUploader):
    """Uploads snapshots via SCP/SSH to a remote host."""

    def __init__(
        self,
        host: str,
        user: str = "",
        port: int = 22,
        dest_path: str = "/snapshots",
        key_file: str = "",
        timeout: int = 1800,
    ) -> None:
        self._host = host
        self._user = user
        self._port = port
        self._dest_path = dest_path.rstrip("/")
        self._key_file = key_file
        self._timeout = timeout

    def _ssh_opts(self) -> list[str]:
        opts = [
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "BatchMode=yes",
            "-p", str(self._port),
        ]
        if self._key_file:
            opts.extend(["-i", self._key_file])
        return opts

    def _scp_opts(self) -> list[str]:
        opts = [
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "BatchMode=yes",
            "-P", str(self._port),
        ]
        if self._key_file:
            opts.extend(["-i", self._key_file])
        return opts

    def _target(self) -> str:
        if self._user:
            return f"{self._user}@{self._host}"
        return self._host

    def _remote_path(self, remote_key: str) -> str:
        return f"{self._dest_path}/{remote_key}"

    async def _run_ssh(self, *args: str, timeout: int = 30) -> tuple[int, str, str]:
        cmd = ["ssh"] + self._ssh_opts() + [self._target()] + list(args)
        logger.debug(f"Running SSH command: {' '.join(cmd)}")
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise asyncio.TimeoutError(
                f"SSH command timed out after {timeout}s: {' '.join(args)}"
            )
        return proc.returncode, stdout.decode(), stderr.decode()

    async def _ensure_remote_dir(self, remote_key: str) -> None:
        remote_dir = str(Path(self._remote_path(remote_key)).parent)
        try:
            code, _, stderr = await self._run_ssh("mkdir", "-p", remote_dir)
            if code != 0:
                logger.warning(
                    f"Failed to create remote dir {remote_dir}: exit {code}: {stderr.strip()}"
                )
        except Exception as e:
            logger.warning(f"Failed to create remote dir {remote_dir}: {e}")

    async def upload(
        self,
        file_path: Path,
        metadata: dict,
        remote_key: str,
    ) -> UploadResult:
        start = time.monotonic()
        file_size = file_path.stat().st_size

        await self._ensure_remote_dir(remote_key)

        scp_target = f"{self._target()}:{self._remote_path(remote_key)}"
        cmd = ["scp"] + self._scp_opts() + [str(file_path), scp_target]

        logger.info(f"Uploading {file_path.name} via SCP to {scp_target} ({file_size} bytes)")
        logger.debug(f"SCP command: {' '.join(cmd)}")

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                _, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=self._timeout
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                return UploadResult(
                    success=False,
                    error_message=f"SCP timed out after {self._timeout}s",
                    duration_seconds=time.monotonic() - start,
                )
            duration = time.monotonic() - start

            if proc.returncode == 0:
                logger.info(f"SCP upload complete: {scp_target} ({duration:.1f}s)")
                return UploadResult(
                    success=True,
                    remote_url=scp_target,
                    bytes_uploaded=file_size,
                    duration_seconds=duration,
                )
            else:
                err = stderr.decode().strip()
                return UploadResult(
                    success=False,
                    error_message=f"scp exit {proc.returncode}: {err}",
                    duration_seconds=duration,
                )
        except Exception as e:
            return UploadResult(
                success=False,
                error_message=str(e),
                duration_seconds=time.monotonic() - start,
            )

    async def put_small_file(self, remote_key: str, content: bytes) -> bool:
        await self._ensure_remote_dir(remote_key)
        remote = self._remote_path(remote_key)
        # Pipe content through ssh to write on remote
        cmd = ["ssh"] + self._ssh_opts() + [
            self._target(),
            f"cat > {remote}",
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                _, stderr = await asyncio.wait_for(
                    proc.communicate(input=content), timeout=30
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                logger.warning(
                    f"put_small_file timed out for {remote_key} after 30s"
                )
                return False
            if proc.returncode != 0:
                err = stderr.decode().strip()
                logger.warning(
                    f"put_small_file failed for {remote_key}: exit {proc.returncode}: {err}"
                )
            return proc.returncode == 0
        except Exception as e:
            logger.warning(f"put_small_file failed for {remote_key}: {e}")
            return False

    async def get_small_file(self, remote_key: str) -> Optional[bytes]:
        remote = self._remote_path(remote_key)
        try:
            code, stdout, _ = await self._run_ssh("cat", remote)
            if code == 0:
                return stdout.encode() if isinstance(stdout, str) else stdout
            return None
        except Exception:
            return None

    async def delete_file(self, remote_key: str) -> bool:
        remote = self._remote_path(remote_key)
        try:
            code, _, _ = await self._run_ssh("rm", "-f", remote)
            return code == 0
        except Exception as e:
            logger.warning(f"delete_file failed for {remote_key}: {e}")
            return False

    async def list_remote_dir(self, remote_prefix: str) -> list[str]:
        remote = self._remote_path(remote_prefix)
        try:
            code, stdout, _ = await self._run_ssh("ls", "-1", remote)
            if code == 0:
                return [n for n in stdout.strip().splitlines() if n]
            return []
        except Exception as e:
            logger.debug(f"list_remote_dir failed for {remote_prefix}: {e}")
            return []

    async def delete_remote_dir(self, remote_prefix: str) -> bool:
        remote = self._remote_path(remote_prefix)
        try:
            code, _, _ = await self._run_ssh("rm", "-rf", remote)
            return code == 0
        except Exception as e:
            logger.warning(f"delete_remote_dir failed for {remote_prefix}: {e}")
            return False

    async def check_health(self) -> bool:
        try:
            code, _, _ = await self._run_ssh("true")
            return code == 0
        except Exception:
            return False

    def get_name(self) -> str:
        return "scp"
