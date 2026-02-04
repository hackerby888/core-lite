import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.process_manager import ProcessManager


class TestProcessManager:
    def test_not_running_initially(self):
        pm = ProcessManager(
            binary_path=Path("/qubic/Qubic"),
            node_client=AsyncMock(),
            working_dir=Path("/qubic"),
        )
        assert pm.is_running() is False
        assert pm.get_pid() is None
        assert pm.get_return_code() is None

    @pytest.mark.asyncio
    async def test_start_and_stop(self, tmp_path):
        # Use a simple command that runs for a bit
        script = tmp_path / "mock_node.sh"
        script.write_text("#!/bin/bash\nwhile true; do sleep 0.1; done")
        script.chmod(0o755)

        node_client = AsyncMock()
        node_client.shutdown.return_value = True

        pm = ProcessManager(
            binary_path=script,
            node_client=node_client,
            working_dir=tmp_path,
        )

        proc = await pm.start([])
        assert pm.is_running()
        assert pm.get_pid() is not None

        # Stop should work (will go through escalation since mock doesn't respond to /shutdown)
        exit_code = await pm.stop(timeout=1.0)
        assert not pm.is_running()
