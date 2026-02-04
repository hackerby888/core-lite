from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from aiohttp import web

from app.config import OrchestratorConfig
from app.epoch_service import EpochService
from app.models import NodeHealth, OrchestratorMode
from app.process_manager import KEY_DESCRIPTIONS, ProcessManager
from app.snapshot_cycle import SnapshotCycle
from app.watchdog import Watchdog

logger = logging.getLogger(__name__)


class ManagementAPI:
    """HTTP management API for the orchestrator.

    Provides endpoints for health checks, status queries,
    node restart, and snapshot triggering.
    """

    def __init__(
        self,
        config: OrchestratorConfig,
        process_manager: ProcessManager,
        watchdog: Optional[Watchdog],
        snapshot_cycle: Optional[SnapshotCycle],
        qubic_args: list[str],
        start_time: float,
        local_version: tuple[int, int] | None = None,
        version_health: NodeHealth | None = None,
    ) -> None:
        self._config = config
        self._process_manager = process_manager
        self._watchdog = watchdog
        self._snapshot_cycle = snapshot_cycle
        self._qubic_args = qubic_args
        self._start_time = start_time
        self._local_version = local_version
        self._version_health = version_health
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

    def _build_app(self) -> web.Application:
        """Create the aiohttp application with routes."""
        app = web.Application()
        app.router.add_get("/health", self._handle_health)
        app.router.add_get("/status", self._handle_status)
        app.router.add_post("/restart", self._handle_restart)
        app.router.add_post("/trigger-snapshot", self._handle_trigger_snapshot)
        app.router.add_post("/send-key", self._handle_send_key)
        app.router.add_get("/keys", self._handle_list_keys)
        return app

    async def start(self) -> None:
        """Start the HTTP management server."""
        app = self._build_app()
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()

        host = self._config.management_api_host
        port = self._config.management_api_port
        self._site = web.TCPSite(self._runner, host, port)
        await self._site.start()
        logger.info(f"Management API listening on {host}:{port}")

    async def stop(self) -> None:
        """Shut down the HTTP management server."""
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()
        logger.info("Management API stopped")

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------
    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({
            "status": "ok",
            "uptime_seconds": round(time.monotonic() - self._start_time, 1),
        })

    async def _handle_status(self, request: web.Request) -> web.Response:
        uptime = round(time.monotonic() - self._start_time, 1)

        # Node state
        node: dict[str, Any] = {
            "pid": self._process_manager.get_pid(),
            "is_running": self._process_manager.is_running(),
        }

        if self._watchdog is not None:
            state = self._watchdog.state
            node["health"] = state.health.value
            node["restart_count"] = state.restart_count
            node["last_restart_time"] = state.last_restart_time
            node["consecutive_stuck_polls"] = state.consecutive_stuck_polls

            if state.last_tick_info is not None:
                ti = state.last_tick_info
                node["tick"] = {
                    "epoch": ti.epoch,
                    "tick": ti.tick,
                    "initial_tick": ti.initial_tick,
                    "aligned_votes": ti.aligned_votes,
                    "misaligned_votes": ti.misaligned_votes,
                    "main_aux_status": ti.main_aux_status,
                    "is_saving_snapshot": ti.is_saving_snapshot,
                }
            else:
                node["tick"] = None
        elif self._version_health is not None:
            node["health"] = self._version_health.value
            node["tick"] = None
        else:
            node["health"] = "starting"
            node["tick"] = None

        result: dict[str, Any] = {
            "mode": self._config.mode.value,
            "uptime_seconds": uptime,
            "node": node,
        }

        # Version information
        result["version"] = {
            "local": (
                EpochService.format_version(self._local_version)
                if self._local_version
                else None
            ),
            "compatible": self._version_health is None,
        }

        # Snapshot cycle info (source mode)
        if self._snapshot_cycle is not None:
            result["snapshot_cycle"] = {
                "enabled": True,
                "interval_seconds": self._config.source.snapshot_interval_seconds,
                "last_snapshot_epoch": self._snapshot_cycle.last_snapshot_epoch,
                "cycle_running": self._snapshot_cycle.is_cycle_running,
                "trigger_pending": self._snapshot_cycle.is_trigger_pending,
            }

        return web.json_response(result)

    async def _handle_restart(self, request: web.Request) -> web.Response:
        # Check if node is saving a snapshot
        if self._watchdog is not None:
            if self._watchdog.state.health == NodeHealth.SAVING_SNAPSHOT:
                return web.json_response(
                    {
                        "status": "error",
                        "message": "Cannot restart while node is saving a snapshot",
                    },
                    status=409,
                )

        try:
            proc = await self._process_manager.restart(self._qubic_args)

            # Reset watchdog state so it doesn't immediately flag as crashed
            if self._watchdog is not None:
                self._watchdog.state.health = NodeHealth.STARTING
                self._watchdog.state.consecutive_stuck_polls = 0
                self._watchdog.state.consecutive_misaligned_polls = 0
                self._watchdog.state.last_tick_change_time = time.monotonic()

            logger.info(
                f"Node restarted via management API (new PID: {proc.pid})"
            )
            return web.json_response({
                "status": "ok",
                "message": "Node restart initiated",
                "pid": proc.pid,
            })

        except Exception as e:
            logger.error(
                f"Restart via management API failed: {e}", exc_info=True
            )
            return web.json_response(
                {
                    "status": "error",
                    "message": f"Restart failed: {e}",
                },
                status=503,
            )

    async def _handle_trigger_snapshot(
        self, request: web.Request
    ) -> web.Response:
        if self._config.mode != OrchestratorMode.SOURCE:
            return web.json_response(
                {
                    "status": "error",
                    "message": "Snapshot trigger is only available in source mode",
                },
                status=400,
            )

        if self._snapshot_cycle is None:
            return web.json_response(
                {
                    "status": "error",
                    "message": "Snapshot cycle is not initialized",
                },
                status=503,
            )

        accepted = self._snapshot_cycle.trigger_immediate()
        if not accepted:
            return web.json_response(
                {
                    "status": "error",
                    "message": "A snapshot cycle is already in progress or pending",
                },
                status=409,
            )

        logger.info("Snapshot cycle triggered via management API")
        return web.json_response({
            "status": "ok",
            "message": "Snapshot cycle triggered",
        })

    async def _handle_send_key(
        self, request: web.Request
    ) -> web.Response:
        """Send a key press to the Qubic process via stdin."""
        try:
            body = await request.json()
        except Exception:
            return web.json_response(
                {"status": "error", "message": "Invalid JSON body"},
                status=400,
            )

        key_name = body.get("key", "").lower().strip()
        if not key_name:
            return web.json_response(
                {"status": "error", "message": "Missing 'key' field"},
                status=400,
            )

        if key_name not in KEY_DESCRIPTIONS:
            return web.json_response(
                {
                    "status": "error",
                    "message": f"Unknown key: {key_name}",
                    "available_keys": list(KEY_DESCRIPTIONS.keys()),
                },
                status=400,
            )

        success = await self._process_manager.send_key(key_name)
        if not success:
            return web.json_response(
                {"status": "error", "message": "Failed to send key (process not running?)"},
                status=503,
            )

        desc = KEY_DESCRIPTIONS.get(key_name, "")
        logger.info(f"Key {key_name.upper()} sent via management API ({desc})")
        return web.json_response({
            "status": "ok",
            "message": f"Sent key {key_name.upper()}: {desc}",
        })

    async def _handle_list_keys(
        self, request: web.Request
    ) -> web.Response:
        """List all available key commands."""
        return web.json_response({
            "status": "ok",
            "keys": KEY_DESCRIPTIONS,
        })
