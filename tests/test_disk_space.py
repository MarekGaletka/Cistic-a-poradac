"""Tests for disk_space module."""

from __future__ import annotations

from collections import namedtuple
from pathlib import Path
from unittest.mock import patch

from godmode_media_library.disk_space import check_disk_space

DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


# ---------------------------------------------------------------------------
# Sufficient space
# ---------------------------------------------------------------------------


def test_sufficient_space(tmp_path):
    """When free space exceeds needed, returns True."""
    with patch("shutil.disk_usage", return_value=DiskUsage(1_000_000, 0, 1_000_000)):
        assert check_disk_space(tmp_path, file_size=100_000) is True


def test_exact_boundary_sufficient(tmp_path):
    """Free == needed (file_size * margin) should be True."""
    # 1000 * 1.1 = 1100
    with patch("shutil.disk_usage", return_value=DiskUsage(5000, 0, 1100)):
        assert check_disk_space(tmp_path, file_size=1000) is True


# ---------------------------------------------------------------------------
# Insufficient space
# ---------------------------------------------------------------------------


def test_insufficient_space(tmp_path):
    with patch("shutil.disk_usage", return_value=DiskUsage(1000, 900, 100)):
        assert check_disk_space(tmp_path, file_size=500) is False


def test_insufficient_with_custom_margin(tmp_path):
    # 500 * 2.0 = 1000, free=999 => insufficient
    with patch("shutil.disk_usage", return_value=DiskUsage(2000, 1001, 999)):
        assert check_disk_space(tmp_path, file_size=500, margin=2.0) is False


def test_zero_free_space(tmp_path):
    with patch("shutil.disk_usage", return_value=DiskUsage(1000, 1000, 0)):
        assert check_disk_space(tmp_path, file_size=1) is False


# ---------------------------------------------------------------------------
# Graceful fallback on errors
# ---------------------------------------------------------------------------


def test_oserror_returns_true(tmp_path):
    """On OSError, function should not block the operation."""
    with patch("shutil.disk_usage", side_effect=OSError("permission denied")):
        assert check_disk_space(tmp_path, file_size=999_999_999) is True


# ---------------------------------------------------------------------------
# Ancestor directory walk-up
# ---------------------------------------------------------------------------


def test_nonexistent_dest_walks_up(tmp_path):
    """If dest_dir doesn't exist, walks up to an existing ancestor."""
    deep = tmp_path / "a" / "b" / "c"
    with patch("shutil.disk_usage", return_value=DiskUsage(1_000_000, 0, 500_000)):
        assert check_disk_space(deep, file_size=100) is True


def test_completely_nonexistent_path_returns_true():
    """Edge case: path whose root is unreachable returns True (fallback)."""
    # Use a path that resolves to root after walk-up; the function
    # returns True when it reaches filesystem root without finding an
    # existing dir.
    fake = Path("/") / "nonexistent_volume_xyz_123"
    # If parent == check_path, it returns True (root sentinel)
    # But /nonexistent_volume_xyz_123 parent is /, which exists, so it'll
    # call disk_usage on /. Mock it:
    with patch("shutil.disk_usage", return_value=DiskUsage(1_000_000, 0, 900_000)):
        assert check_disk_space(fake, file_size=100) is True


# ---------------------------------------------------------------------------
# Margin calculation
# ---------------------------------------------------------------------------


def test_default_margin_is_ten_percent(tmp_path):
    """Default margin=1.1: file_size=1000 needs 1100 bytes."""
    # 1099 free < 1100 needed => False
    with patch("shutil.disk_usage", return_value=DiskUsage(5000, 0, 1099)):
        assert check_disk_space(tmp_path, file_size=1000) is False
    # 1100 free >= 1100 needed => True
    with patch("shutil.disk_usage", return_value=DiskUsage(5000, 0, 1100)):
        assert check_disk_space(tmp_path, file_size=1000) is True


def test_zero_file_size(tmp_path):
    """A zero-byte file always fits."""
    with patch("shutil.disk_usage", return_value=DiskUsage(1000, 999, 1)):
        assert check_disk_space(tmp_path, file_size=0) is True
