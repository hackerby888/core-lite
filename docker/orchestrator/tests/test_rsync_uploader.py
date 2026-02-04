from __future__ import annotations

import pytest

from app.uploaders.base import RemotePackagingUploader
from app.uploaders.rsync import RsyncUploader


class TestRsyncOpts:
    def test_basic_opts(self):
        up = RsyncUploader(host="example.com", user="admin", port=22)
        opts = up._rsync_opts()
        assert "-a" in opts
        assert "--delete" in opts
        assert "--inplace" in opts
        assert "--stats" in opts
        assert "-z" not in opts  # compress disabled by default (binary data)

    def test_compress_enabled(self):
        up = RsyncUploader(host="example.com", compress=True)
        opts = up._rsync_opts()
        assert "-z" in opts

    def test_no_compress(self):
        up = RsyncUploader(host="example.com", compress=False)
        opts = up._rsync_opts()
        assert "-z" not in opts

    def test_bandwidth_limit(self):
        up = RsyncUploader(host="example.com", bandwidth_limit=1000)
        opts = up._rsync_opts()
        assert "--bwlimit" in opts
        idx = opts.index("--bwlimit")
        assert opts[idx + 1] == "1000"

    def test_no_bandwidth_limit_when_zero(self):
        up = RsyncUploader(host="example.com", bandwidth_limit=0)
        opts = up._rsync_opts()
        assert "--bwlimit" not in opts

    def test_ssh_command_in_opts(self):
        up = RsyncUploader(
            host="example.com", user="admin", port=2222,
            key_file="/path/to/key",
        )
        opts = up._rsync_opts()
        idx = opts.index("-e")
        ssh_cmd = opts[idx + 1]
        assert "ssh" in ssh_cmd
        assert "-p" in ssh_cmd
        assert "2222" in ssh_cmd
        assert "-i" in ssh_cmd
        assert "/path/to/key" in ssh_cmd


class TestRemoteStagingPath:
    def test_basic(self):
        up = RsyncUploader(host="example.com", dest_path="/snapshots")
        assert up._remote_staging_path(198) == "/snapshots/198/snapshot-files"

    def test_trailing_slash_in_dest(self):
        up = RsyncUploader(host="example.com", dest_path="/snapshots/")
        # ScpUploader strips trailing /
        assert up._remote_staging_path(198) == "/snapshots/198/snapshot-files"


class TestParseRsyncBytes:
    def test_parse_normal_output(self):
        output = """
Number of files: 10
Number of created files: 0
Number of deleted files: 0
Number of regular files transferred: 5
Total file size: 2,345,678,901 bytes
Total transferred file size: 1,234,567 bytes
Literal data: 234,567 bytes
Matched data: 1,000,000 bytes
Total bytes sent: 250,000
Total bytes received: 1,234
"""
        assert RsyncUploader._parse_rsync_bytes(output) == 250000

    def test_parse_zero_transfer(self):
        output = """
Total bytes sent: 0
"""
        assert RsyncUploader._parse_rsync_bytes(output) == 0

    def test_parse_no_stats(self):
        assert RsyncUploader._parse_rsync_bytes("no stats here") == 0

    def test_parse_empty(self):
        assert RsyncUploader._parse_rsync_bytes("") == 0

    def test_parse_large_number(self):
        output = "Total bytes sent: 1,234,567,890,123"
        assert RsyncUploader._parse_rsync_bytes(output) == 1234567890123


class TestGetName:
    def test_name(self):
        up = RsyncUploader(host="example.com")
        assert up.get_name() == "rsync"


class TestProtocolCompliance:
    def test_implements_remote_packaging(self):
        up = RsyncUploader(host="example.com")
        assert isinstance(up, RemotePackagingUploader)
