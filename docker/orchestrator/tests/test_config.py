import os

import pytest

from app.config import OrchestratorConfig, WatchdogConfig, SourceConfig
from app.models import OrchestratorMode


class TestOrchestratorConfig:
    def test_defaults(self):
        config = OrchestratorConfig()
        assert config.mode == OrchestratorMode.NORMAL
        assert config.data_dir == "/qubic"
        assert config.binary_path == "/qubic/Qubic"
        assert config.security_tick == 32
        assert config.watchdog.enabled is True
        assert config.source.snapshot_interval_seconds == 3600

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("QUBIC_MODE", "source")
        monkeypatch.setenv("QUBIC_PEERS", "1.2.3.4,5.6.7.8")
        monkeypatch.setenv("QUBIC_SECURITY_TICK", "64")
        config = OrchestratorConfig()
        assert config.mode == OrchestratorMode.SOURCE
        assert config.peers == "1.2.3.4,5.6.7.8"
        assert config.security_tick == 64

    def test_get_peers_list(self):
        config = OrchestratorConfig(peers="1.2.3.4, 5.6.7.8 , 9.10.11.12")
        peers = config.get_peers_list()
        assert peers == ["1.2.3.4", "5.6.7.8", "9.10.11.12"]

    def test_get_peers_list_empty(self):
        config = OrchestratorConfig(peers="")
        assert config.get_peers_list() == []

    def test_build_qubic_args_minimal(self):
        config = OrchestratorConfig(peers="", security_tick=32)
        args = config.build_qubic_args()
        assert "--security-tick" in args
        assert "32" in args
        assert "--peers" not in args

    def test_build_qubic_args_full(self):
        config = OrchestratorConfig(
            peers="1.2.3.4",
            security_tick=16,
            threads=4,
            operator_seed="a" * 55,
            http_passcode="1-2-3-4",
        )
        args = config.build_qubic_args()
        assert args == [
            "--peers", "1.2.3.4",
            "--security-tick", "16",
            "--threads", "4",
            "--operator-seed", "a" * 55,
            "--http-passcode", "1-2-3-4",
        ]


    def test_env_overrides_yaml(self, monkeypatch, tmp_path):
        """ENV vars must override YAML values (not the other way around)."""
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text("mode: normal\nsecurity_tick: 32\n")

        monkeypatch.setenv("QUBIC_MODE", "source")
        monkeypatch.setenv("QUBIC_SECURITY_TICK", "64")

        from app.config import _load_yaml_config

        OrchestratorConfig._yaml_config = _load_yaml_config(str(yaml_file))
        config = OrchestratorConfig()

        assert config.mode == OrchestratorMode.SOURCE
        assert config.security_tick == 64

        # Cleanup
        OrchestratorConfig._yaml_config = {}

    def test_env_overrides_yaml_nested(self, monkeypatch, tmp_path):
        """Nested ENV vars (QUBIC_SOURCE__*) must override YAML values."""
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(
            "source:\n  scp_host: yaml-host\n  scp_user: yaml-user\n"
        )

        monkeypatch.setenv("QUBIC_SOURCE__SCP_HOST", "env-host")

        from app.config import _load_yaml_config

        OrchestratorConfig._yaml_config = _load_yaml_config(str(yaml_file))
        config = OrchestratorConfig()

        assert config.source.scp_host == "env-host"
        # YAML value should be used when no env override
        assert config.source.scp_user == "yaml-user"

        # Cleanup
        OrchestratorConfig._yaml_config = {}

    def test_cli_overrides_env_and_yaml(self, monkeypatch, tmp_path):
        """Init kwargs (CLI) must override both ENV and YAML."""
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text("security_tick: 16\n")

        monkeypatch.setenv("QUBIC_SECURITY_TICK", "32")

        from app.config import _load_yaml_config

        OrchestratorConfig._yaml_config = _load_yaml_config(str(yaml_file))
        # CLI override passed as init kwarg
        config = OrchestratorConfig(security_tick=64)

        assert config.security_tick == 64

        # Cleanup
        OrchestratorConfig._yaml_config = {}


class TestWatchdogConfig:
    def test_defaults(self):
        config = WatchdogConfig()
        assert config.poll_interval_seconds == 15
        assert config.stuck_threshold_seconds == 300
        assert config.max_restarts == 5


class TestSourceConfig:
    def test_defaults(self):
        config = SourceConfig()
        assert config.snapshot_interval_seconds == 3600
        assert config.uploader_type == "scp"
        assert config.upload_retry_count == 3
