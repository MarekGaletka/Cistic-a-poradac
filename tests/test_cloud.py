"""Tests for cloud storage support."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from godmode_media_library.cloud import (
    check_rclone,
    format_cloud_guide,
    list_remotes,
    mount_command,
    resolve_root,
)


def test_check_rclone_not_installed():
    with patch("shutil.which", return_value=None):
        assert check_rclone() is False


def test_check_rclone_installed():
    with patch("shutil.which", return_value="/usr/bin/rclone"):
        assert check_rclone() is True


def test_list_remotes_no_rclone():
    with patch("shutil.which", return_value=None):
        assert list_remotes() == []


def test_list_remotes_parses_output():
    mock_output = "gdrive: drive\ns3:     s3\n"
    with patch("shutil.which", return_value="/usr/bin/rclone"), \
         patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = mock_output
        remotes = list_remotes()
        assert len(remotes) == 2
        assert remotes[0].name == "gdrive"
        assert remotes[0].type == "drive"
        assert remotes[1].name == "s3"
        assert remotes[1].type == "s3"


def test_resolve_root_local_path(tmp_path):
    result = resolve_root(str(tmp_path))
    assert result == tmp_path


def test_resolve_root_home_relative(tmp_path):
    with patch.object(Path, "expanduser", return_value=tmp_path):
        result = resolve_root("~/test")
        assert result == tmp_path


def test_resolve_root_nonexistent():
    with pytest.raises(ValueError, match="does not exist"):
        resolve_root("/nonexistent/path/abc123")


def test_resolve_root_remote_not_mounted():
    with pytest.raises(ValueError, match="not mounted"):
        resolve_root("gdrive:Photos")


def test_resolve_root_remote_mounted(tmp_path):
    mount = tmp_path / "mnt" / "gdrive" / "Photos"
    mount.mkdir(parents=True)
    with patch.object(Path, "home", return_value=tmp_path):
        result = resolve_root("gdrive:Photos")
        assert result == mount


def test_mount_command():
    cmd = mount_command("gdrive")
    assert "rclone mount gdrive:" in cmd
    assert "--daemon" in cmd
    assert "--vfs-cache-mode full" in cmd


def test_mount_command_custom_path():
    cmd = mount_command("s3", "/mnt/s3")
    assert "rclone mount s3: /mnt/s3" in cmd


def test_format_cloud_guide():
    guide = format_cloud_guide()
    assert "rclone" in guide
    assert "Google Drive" in guide
    assert "iCloud" in guide
