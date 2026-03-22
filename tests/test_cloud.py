"""Tests for cloud storage support."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from godmode_media_library.cloud import (
    PROVIDERS,
    RcloneRemote,
    check_rclone,
    default_sync_dir,
    detect_native_cloud_paths,
    format_cloud_guide,
    get_cloud_status,
    list_remotes,
    mount_command,
    provider_setup_guide,
    resolve_root,
)


def test_check_rclone_not_installed():
    with patch("godmode_media_library.cloud.shutil.which", return_value=None):
        assert check_rclone() is False


def test_check_rclone_installed():
    with patch("godmode_media_library.cloud.shutil.which", return_value="/usr/bin/rclone"):
        assert check_rclone() is True


def test_list_remotes_no_rclone():
    with patch("godmode_media_library.cloud.shutil.which", return_value=None):
        assert list_remotes() == []


def test_list_remotes_parses_output():
    mock_output = "gdrive: drive\nmega:   mega\npcloud: pcloud\n"
    with patch("godmode_media_library.cloud.shutil.which", return_value="/usr/bin/rclone"), \
         patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = mock_output
        remotes = list_remotes()
        assert len(remotes) == 3
        assert remotes[0].name == "gdrive"
        assert remotes[0].type == "drive"
        assert remotes[0].provider_label == "Google Drive"
        assert remotes[1].name == "mega"
        assert remotes[1].provider_label == "MEGA"
        assert remotes[2].name == "pcloud"
        assert remotes[2].provider_label == "pCloud"


def test_rclone_remote_autodetect_provider():
    r = RcloneRemote(name="test", type="mega")
    assert r.provider_label == "MEGA"

    r2 = RcloneRemote(name="test", type="drive")
    assert r2.provider_label == "Google Drive"

    r3 = RcloneRemote(name="test", type="unknown_type")
    assert r3.provider_label == "Unknown_Type"


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
    assert "MEGA" in guide
    assert "pCloud" in guide


# ── New tests ──


def test_providers_dict():
    assert "mega" in PROVIDERS
    assert "pcloud" in PROVIDERS
    assert "drive" in PROVIDERS
    assert "google photos" in PROVIDERS
    assert "onedrive" in PROVIDERS
    assert "dropbox" in PROVIDERS


def test_provider_setup_guide():
    guide = provider_setup_guide("mega")
    assert guide["provider"] == "MEGA"
    assert len(guide["steps"]) == 4
    assert "command" in guide["steps"][0]


def test_provider_setup_guide_unknown():
    guide = provider_setup_guide("nonexistent")
    assert "error" in guide


def test_default_sync_dir():
    d = default_sync_dir()
    assert "gml" in str(d)
    assert "cloud" in str(d)


def test_detect_native_cloud_paths_mega(tmp_path):
    mega_dir = tmp_path / "MEGA"
    mega_dir.mkdir()
    with patch.object(Path, "home", return_value=tmp_path):
        paths = detect_native_cloud_paths()
        names = [p["name"] for p in paths]
        assert "MEGA" in names


def test_detect_native_cloud_paths_pcloud(tmp_path):
    pcloud_dir = tmp_path / "pCloudDrive"
    pcloud_dir.mkdir()
    with patch.object(Path, "home", return_value=tmp_path):
        paths = detect_native_cloud_paths()
        names = [p["name"] for p in paths]
        assert "pCloud" in names


def test_detect_native_cloud_paths_dropbox(tmp_path):
    dropbox_dir = tmp_path / "Dropbox"
    dropbox_dir.mkdir()
    with patch.object(Path, "home", return_value=tmp_path):
        paths = detect_native_cloud_paths()
        names = [p["name"] for p in paths]
        assert "Dropbox" in names


def test_detect_native_cloud_paths_empty(tmp_path):
    with patch.object(Path, "home", return_value=tmp_path):
        paths = detect_native_cloud_paths()
        # No cloud dirs exist in tmp_path
        assert isinstance(paths, list)


def test_get_cloud_status_no_rclone():
    with patch("godmode_media_library.cloud.check_rclone", return_value=False), \
         patch("godmode_media_library.cloud.detect_native_cloud_paths", return_value=[]):
        status = get_cloud_status()
        assert status["rclone_installed"] is False
        assert status["rclone_version"] is None
        assert isinstance(status["sources"], list)
        assert "providers" in status


def test_get_cloud_status_with_rclone():
    mock_remotes = [RcloneRemote(name="mega", type="mega")]
    with patch("godmode_media_library.cloud.check_rclone", return_value=True), \
         patch("godmode_media_library.cloud.rclone_version", return_value="1.67.0"), \
         patch("godmode_media_library.cloud.list_remotes", return_value=mock_remotes), \
         patch("godmode_media_library.cloud.detect_native_cloud_paths", return_value=[]), \
         patch("godmode_media_library.cloud._is_mount_active", return_value=False):
        status = get_cloud_status()
        assert status["rclone_installed"] is True
        assert status["rclone_version"] == "1.67.0"
        assert len(status["sources"]) == 1
        assert status["sources"][0]["name"] == "mega"
        assert status["sources"][0]["provider"] == "MEGA"
