from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from app.config import OrchestratorConfig, SourceConfig
from app.management_api import ManagementAPI
from app.models import NodeHealth, NodeState, OrchestratorMode, TickInfo
from app.process_manager import ProcessManager
from app.snapshot_cycle import SnapshotCycle
from app.watchdog import Watchdog


def _make_tick_info(epoch: int = 198, tick: int = 43101500) -> TickInfo:
    return TickInfo(
        epoch=epoch,
        tick=tick,
        initial_tick=43101000,
        aligned_votes=500,
        misaligned_votes=176,
        main_aux_status=1,
        is_saving_snapshot=False,
    )


def _make_config(mode: OrchestratorMode = OrchestratorMode.SOURCE) -> MagicMock:
    config = MagicMock(spec=OrchestratorConfig)
    config.mode = mode
    config.management_api_port = 8080
    config.management_api_host = "127.0.0.1"
    config.source = SourceConfig(snapshot_interval_seconds=3600)
    return config


def _make_process_manager(running: bool = True, pid: int = 42) -> MagicMock:
    pm = MagicMock(spec=ProcessManager)
    pm.is_running.return_value = running
    pm.get_pid.return_value = pid
    pm.get_return_code.return_value = None if running else 0

    mock_proc = MagicMock()
    mock_proc.pid = pid + 1
    pm.restart = AsyncMock(return_value=mock_proc)
    return pm


def _make_watchdog(
    health: NodeHealth = NodeHealth.HEALTHY,
    tick_info: TickInfo | None = None,
) -> MagicMock:
    wd = MagicMock(spec=Watchdog)
    state = NodeState(health=health)
    state.last_tick_info = tick_info or _make_tick_info()
    state.restart_count = 0
    state.last_restart_time = 0.0
    state.consecutive_stuck_polls = 0
    state.consecutive_misaligned_polls = 0
    wd.state = state
    return wd


def _make_snapshot_cycle(
    running: bool = False,
    pending: bool = False,
    trigger_returns: bool = True,
) -> MagicMock:
    sc = MagicMock(spec=SnapshotCycle)
    sc.is_cycle_running = running
    sc.is_trigger_pending = pending
    sc.last_snapshot_epoch = None
    sc.trigger_immediate.return_value = trigger_returns
    return sc


def _make_api(
    mode: OrchestratorMode = OrchestratorMode.SOURCE,
    process_manager: MagicMock | None = None,
    watchdog: MagicMock | None = None,
    snapshot_cycle: MagicMock | None = None,
) -> ManagementAPI:
    config = _make_config(mode)
    pm = process_manager or _make_process_manager()
    wd = watchdog if watchdog is not None else _make_watchdog()
    sc = snapshot_cycle if snapshot_cycle is not None else _make_snapshot_cycle()

    if mode == OrchestratorMode.NORMAL:
        sc = None

    return ManagementAPI(
        config=config,
        process_manager=pm,
        watchdog=wd,
        snapshot_cycle=sc,
        qubic_args=["--peers", "1.2.3.4"],
        start_time=time.monotonic() - 100,
    )


@pytest_asyncio.fixture
async def client():
    """Create a test client for the management API."""
    api = _make_api()
    app = api._build_app()
    async with TestClient(TestServer(app)) as c:
        # Attach api ref for assertions
        c._api = api
        yield c


class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_health_returns_ok(self, client):
        resp = await client.get("/health")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ok"

    @pytest.mark.asyncio
    async def test_health_includes_uptime(self, client):
        resp = await client.get("/health")
        data = await resp.json()
        assert data["uptime_seconds"] > 0


class TestStatusEndpoint:
    @pytest.mark.asyncio
    async def test_status_source_mode(self):
        api = _make_api(mode=OrchestratorMode.SOURCE)
        app = api._build_app()
        async with TestClient(TestServer(app)) as c:
            resp = await c.get("/status")
            assert resp.status == 200
            data = await resp.json()
            assert data["mode"] == "source"
            assert "snapshot_cycle" in data
            assert data["snapshot_cycle"]["enabled"] is True

    @pytest.mark.asyncio
    async def test_status_normal_mode(self):
        api = _make_api(mode=OrchestratorMode.NORMAL)
        app = api._build_app()
        async with TestClient(TestServer(app)) as c:
            resp = await c.get("/status")
            data = await resp.json()
            assert data["mode"] == "normal"
            assert "snapshot_cycle" not in data

    @pytest.mark.asyncio
    async def test_status_shows_node_health(self, client):
        resp = await client.get("/status")
        data = await resp.json()
        assert data["node"]["health"] == "healthy"
        assert data["node"]["is_running"] is True
        assert data["node"]["pid"] == 42

    @pytest.mark.asyncio
    async def test_status_shows_tick_info(self, client):
        resp = await client.get("/status")
        data = await resp.json()
        tick = data["node"]["tick"]
        assert tick["epoch"] == 198
        assert tick["tick"] == 43101500

    @pytest.mark.asyncio
    async def test_status_without_watchdog(self):
        api = ManagementAPI(
            config=_make_config(),
            process_manager=_make_process_manager(),
            watchdog=None,
            snapshot_cycle=_make_snapshot_cycle(),
            qubic_args=[],
            start_time=time.monotonic() - 10,
        )
        app = api._build_app()
        async with TestClient(TestServer(app)) as c:
            resp = await c.get("/status")
            data = await resp.json()
            assert data["node"]["health"] == "starting"
            assert data["node"]["tick"] is None


class TestRestartEndpoint:
    @pytest.mark.asyncio
    async def test_restart_success(self):
        pm = _make_process_manager()
        api = _make_api(process_manager=pm)
        app = api._build_app()
        async with TestClient(TestServer(app)) as c:
            resp = await c.post("/restart")
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "ok"
            assert "pid" in data
            pm.restart.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_restart_blocked_during_snapshot_save(self):
        wd = _make_watchdog(health=NodeHealth.SAVING_SNAPSHOT)
        api = _make_api(watchdog=wd)
        app = api._build_app()
        async with TestClient(TestServer(app)) as c:
            resp = await c.post("/restart")
            assert resp.status == 409
            data = await resp.json()
            assert data["status"] == "error"
            assert "saving" in data["message"].lower()

    @pytest.mark.asyncio
    async def test_restart_failure(self):
        pm = _make_process_manager()
        pm.restart = AsyncMock(side_effect=RuntimeError("binary not found"))
        api = _make_api(process_manager=pm)
        app = api._build_app()
        async with TestClient(TestServer(app)) as c:
            resp = await c.post("/restart")
            assert resp.status == 503
            data = await resp.json()
            assert data["status"] == "error"

    @pytest.mark.asyncio
    async def test_restart_resets_watchdog_state(self):
        wd = _make_watchdog(health=NodeHealth.STUCK)
        wd.state.consecutive_stuck_polls = 5
        api = _make_api(watchdog=wd)
        app = api._build_app()
        async with TestClient(TestServer(app)) as c:
            resp = await c.post("/restart")
            assert resp.status == 200
            assert wd.state.health == NodeHealth.STARTING
            assert wd.state.consecutive_stuck_polls == 0


class TestTriggerSnapshotEndpoint:
    @pytest.mark.asyncio
    async def test_trigger_in_source_mode(self):
        sc = _make_snapshot_cycle(trigger_returns=True)
        api = _make_api(mode=OrchestratorMode.SOURCE, snapshot_cycle=sc)
        app = api._build_app()
        async with TestClient(TestServer(app)) as c:
            resp = await c.post("/trigger-snapshot")
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "ok"
            sc.trigger_immediate.assert_called_once()

    @pytest.mark.asyncio
    async def test_trigger_in_normal_mode(self):
        api = _make_api(mode=OrchestratorMode.NORMAL)
        app = api._build_app()
        async with TestClient(TestServer(app)) as c:
            resp = await c.post("/trigger-snapshot")
            assert resp.status == 400
            data = await resp.json()
            assert data["status"] == "error"
            assert "source mode" in data["message"].lower()

    @pytest.mark.asyncio
    async def test_trigger_while_cycle_running(self):
        sc = _make_snapshot_cycle(trigger_returns=False)
        api = _make_api(mode=OrchestratorMode.SOURCE, snapshot_cycle=sc)
        app = api._build_app()
        async with TestClient(TestServer(app)) as c:
            resp = await c.post("/trigger-snapshot")
            assert resp.status == 409
            data = await resp.json()
            assert data["status"] == "error"
