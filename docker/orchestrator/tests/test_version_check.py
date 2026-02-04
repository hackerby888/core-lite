"""Tests for version parsing, formatting, and compatibility logic."""

from __future__ import annotations

import pytest

from app.epoch_service import EpochService
from app.models import EpochInfo


class TestParseVersion:
    def test_valid(self):
        assert EpochService.parse_version("1.276") == (1, 276)

    def test_valid_three_part(self):
        # "1.276.0" — extra parts are ignored
        assert EpochService.parse_version("1.276.0") == (1, 276)

    def test_zero(self):
        assert EpochService.parse_version("0.0") == (0, 0)

    def test_large_numbers(self):
        assert EpochService.parse_version("2.999") == (2, 999)

    def test_whitespace(self):
        assert EpochService.parse_version("  1.280  ") == (1, 280)

    def test_empty_string(self):
        assert EpochService.parse_version("") is None

    def test_single_number(self):
        assert EpochService.parse_version("1") is None

    def test_non_numeric(self):
        assert EpochService.parse_version("abc.def") is None

    def test_partial_non_numeric(self):
        assert EpochService.parse_version("1.abc") is None


class TestFormatVersion:
    def test_basic(self):
        assert EpochService.format_version((1, 276)) == "1.276"

    def test_zero(self):
        assert EpochService.format_version((0, 0)) == "0.0"

    def test_large(self):
        assert EpochService.format_version((2, 999)) == "2.999"


class TestIsVersionCompatible:
    def test_compatible_equal(self):
        assert EpochService.is_version_compatible((1, 280), (1, 280)) is True

    def test_compatible_newer(self):
        assert EpochService.is_version_compatible((1, 281), (1, 280)) is True

    def test_incompatible(self):
        assert EpochService.is_version_compatible((1, 276), (1, 280)) is False

    def test_version_a_takes_precedence(self):
        assert EpochService.is_version_compatible((2, 0), (1, 999)) is True

    def test_version_a_lower(self):
        assert EpochService.is_version_compatible((1, 999), (2, 0)) is False

    def test_no_minimum(self):
        # min_version=None → always compatible (no check performed)
        assert EpochService.is_version_compatible((1, 276), None) is True

    def test_no_minimum_no_local(self):
        assert EpochService.is_version_compatible(None, None) is True

    def test_no_local_with_minimum(self):
        # local=None but minimum is set → incompatible
        assert EpochService.is_version_compatible(None, (1, 280)) is False


class TestReadLocalVersion:
    def test_valid_file(self, tmp_path):
        vf = tmp_path / "version.txt"
        vf.write_text("1.276\n")
        assert EpochService.read_local_version(str(vf)) == (1, 276)

    def test_missing_file(self, tmp_path):
        vf = tmp_path / "version.txt"
        assert EpochService.read_local_version(str(vf)) is None

    def test_empty_file(self, tmp_path):
        vf = tmp_path / "version.txt"
        vf.write_text("")
        assert EpochService.read_local_version(str(vf)) is None

    def test_invalid_content(self, tmp_path):
        vf = tmp_path / "version.txt"
        vf.write_text("not-a-version")
        assert EpochService.read_local_version(str(vf)) is None


class TestEpochInfoMinVersion:
    def test_with_min_version(self):
        info = EpochInfo(
            epoch=199,
            initial_tick=43200000,
            peers=["1.2.3.4"],
            min_version=(1, 280),
        )
        assert info.min_version == (1, 280)

    def test_without_min_version(self):
        info = EpochInfo(
            epoch=199,
            initial_tick=43200000,
            peers=[],
        )
        assert info.min_version is None
