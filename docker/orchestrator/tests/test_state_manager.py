import zipfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from app.downloaders.base import BaseDownloader
from app.state_manager import StateManager


class MockDownloader(BaseDownloader):
    async def download(self, url: str, dest_path: Path) -> Path:
        # Create a mock zip file
        with zipfile.ZipFile(dest_path, "w") as zf:
            zf.writestr("spectrum.000", "spectrum data")
            zf.writestr("universe.000", "universe data")
            zf.writestr("contract0000.000", "contract data")
        return dest_path

    async def close(self) -> None:
        pass


class TestStateManager:
    def test_get_local_epoch_none(self, tmp_data_dir):
        sm = StateManager(tmp_data_dir, MockDownloader())
        assert sm.get_local_epoch() is None

    def test_get_local_epoch(self, tmp_data_dir):
        (tmp_data_dir / "spectrum.198").write_text("data")
        sm = StateManager(tmp_data_dir, MockDownloader())
        assert sm.get_local_epoch() == 198

    def test_get_local_epoch_multiple(self, tmp_data_dir):
        (tmp_data_dir / "spectrum.197").write_text("old")
        (tmp_data_dir / "spectrum.198").write_text("new")
        sm = StateManager(tmp_data_dir, MockDownloader())
        assert sm.get_local_epoch() == 198

    def test_has_valid_state_files_true(self, tmp_data_dir):
        (tmp_data_dir / "spectrum.198").write_text("data")
        (tmp_data_dir / "universe.198").write_text("data")
        sm = StateManager(tmp_data_dir, MockDownloader())
        assert sm.has_valid_state_files(198) is True

    def test_has_valid_state_files_missing(self, tmp_data_dir):
        (tmp_data_dir / "spectrum.198").write_text("data")
        sm = StateManager(tmp_data_dir, MockDownloader())
        assert sm.has_valid_state_files(198) is False

    def test_has_valid_state_files_empty(self, tmp_data_dir):
        (tmp_data_dir / "spectrum.198").write_text("")
        (tmp_data_dir / "universe.198").write_text("data")
        sm = StateManager(tmp_data_dir, MockDownloader())
        assert sm.has_valid_state_files(198) is False

    @pytest.mark.asyncio
    async def test_download_epoch_files(self, tmp_data_dir):
        sm = StateManager(tmp_data_dir, MockDownloader())
        result = await sm.download_epoch_files(
            198, "https://example.com/ep198-full.zip"
        )
        assert result is True
        assert (tmp_data_dir / "spectrum.198").exists()
        assert (tmp_data_dir / "universe.198").exists()
        assert (tmp_data_dir / "contract0000.198").exists()

    def test_list_state_files(self, tmp_data_dir):
        (tmp_data_dir / "spectrum.198").write_text("data")
        (tmp_data_dir / "universe.198").write_text("data")
        (tmp_data_dir / "contract0000.198").write_text("data")
        (tmp_data_dir / "system").write_text("data")
        sm = StateManager(tmp_data_dir, MockDownloader())
        files = sm.list_state_files(198)
        names = [f.name for f in files]
        assert "spectrum.198" in names
        assert "universe.198" in names
        assert "contract0000.198" in names
        assert "system" in names

    def test_cleanup_old_epochs(self, tmp_data_dir):
        (tmp_data_dir / "spectrum.196").write_text("old")
        (tmp_data_dir / "spectrum.197").write_text("prev")
        (tmp_data_dir / "spectrum.198").write_text("current")
        sm = StateManager(tmp_data_dir, MockDownloader())
        sm.cleanup_old_epochs(198, keep=1)
        assert not (tmp_data_dir / "spectrum.196").exists()
        assert (tmp_data_dir / "spectrum.197").exists()
        assert (tmp_data_dir / "spectrum.198").exists()

    def test_package_snapshot_legacy_name(self, tmp_data_dir):
        (tmp_data_dir / "spectrum.198").write_text("data")
        (tmp_data_dir / "universe.198").write_text("data")
        (tmp_data_dir / "system").write_text("system")
        ep_dir = tmp_data_dir / "ep198"
        ep_dir.mkdir()
        (ep_dir / "system.snp").write_text("snp")

        sm = StateManager(tmp_data_dir, MockDownloader())
        dest = tmp_data_dir / "out"
        dest.mkdir()
        archive = sm.package_snapshot(198, dest)
        assert archive.name == "ep198-full.zip"
        assert archive.exists()

    def test_package_snapshot_tick_name(self, tmp_data_dir):
        (tmp_data_dir / "spectrum.198").write_text("data")
        (tmp_data_dir / "universe.198").write_text("data")
        (tmp_data_dir / "system").write_text("system")
        ep_dir = tmp_data_dir / "ep198"
        ep_dir.mkdir()
        (ep_dir / "system.snp").write_text("snp")

        sm = StateManager(tmp_data_dir, MockDownloader())
        dest = tmp_data_dir / "out"
        dest.mkdir()
        archive = sm.package_snapshot(198, dest, tick=43101500)
        assert archive.name == "ep198-t43101500-snap.zip"
        assert archive.exists()

        # Verify it's a valid zip
        with zipfile.ZipFile(archive) as zf:
            names = zf.namelist()
            assert any("spectrum.198" in n for n in names)

    def test_package_snapshot_parallel_zip_valid(self, tmp_data_dir):
        """Verify parallel ZIP is valid and extractable with correct content."""
        # Create multiple files of varying sizes to exercise parallelism
        (tmp_data_dir / "spectrum.198").write_bytes(b"A" * 100_000)
        (tmp_data_dir / "universe.198").write_bytes(b"B" * 200_000)
        (tmp_data_dir / "contract0000.198").write_bytes(b"C" * 50_000)
        (tmp_data_dir / "system").write_bytes(b"D" * 10_000)
        ep_dir = tmp_data_dir / "ep198"
        ep_dir.mkdir()
        (ep_dir / "system.snp").write_bytes(b"E" * 5_000)

        sm = StateManager(tmp_data_dir, MockDownloader())
        dest = tmp_data_dir / "out"
        dest.mkdir()
        archive = sm.package_snapshot(198, dest, tick=99999)
        assert archive.exists()

        # Verify zipfile can open and validate CRCs
        with zipfile.ZipFile(archive, "r") as zf:
            bad = zf.testzip()
            assert bad is None, f"CRC mismatch in {bad}"

            names = zf.namelist()
            assert "spectrum.198" in names
            assert "universe.198" in names
            assert "contract0000.198" in names
            assert "system" in names

            # Check actual content
            assert zf.read("spectrum.198") == b"A" * 100_000
            assert zf.read("universe.198") == b"B" * 200_000
            assert zf.read("contract0000.198") == b"C" * 50_000
            assert zf.read("system") == b"D" * 10_000

        # Verify it can be extracted to a fresh directory
        extract_dir = tmp_data_dir / "extracted"
        extract_dir.mkdir()
        with zipfile.ZipFile(archive, "r") as zf:
            zf.extractall(extract_dir)
        assert (extract_dir / "spectrum.198").read_bytes() == b"A" * 100_000
        assert (extract_dir / "universe.198").read_bytes() == b"B" * 200_000

    def test_compute_checksum(self, tmp_data_dir):
        test_file = tmp_data_dir / "test.bin"
        test_file.write_bytes(b"hello world")
        checksum = StateManager.compute_checksum(test_file)
        assert len(checksum) == 64  # SHA-256 hex digest
        assert checksum == (
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        )
