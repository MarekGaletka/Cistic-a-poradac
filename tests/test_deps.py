from __future__ import annotations

from unittest.mock import MagicMock, patch

from godmode_media_library.deps import (
    DependencyStatus,
    check_all,
    check_exiftool,
    check_ffmpeg,
    check_ffprobe,
    check_pillow,
    check_rclone,
    format_report,
)


def test_check_exiftool_found():
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = "12.76\n"
    with patch("godmode_media_library.deps.shutil.which", return_value="/usr/bin/exiftool"), \
         patch("subprocess.run", return_value=mock_proc):
        status = check_exiftool()
    assert status.available is True
    assert status.version == "12.76"
    assert status.name == "ExifTool"


def test_check_exiftool_not_found():
    with patch("godmode_media_library.deps._which", return_value=None):
        status = check_exiftool()
    assert status.available is False
    assert status.install_hint is not None


def test_check_ffprobe_found():
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = "ffprobe version 6.1 Copyright (c) ...\n"
    with patch("godmode_media_library.deps.shutil.which", return_value="/usr/bin/ffprobe"), \
         patch("subprocess.run", return_value=mock_proc):
        status = check_ffprobe()
    assert status.available is True
    assert status.version is not None


def test_check_ffprobe_not_found():
    with patch("godmode_media_library.deps._which", return_value=None):
        status = check_ffprobe()
    assert status.available is False
    assert "ffmpeg" in status.install_hint.lower()


def test_check_ffmpeg_found():
    with patch("godmode_media_library.deps.shutil.which", return_value="/usr/bin/ffmpeg"):
        status = check_ffmpeg()
    assert status.available is True


def test_check_ffmpeg_not_found():
    with patch("godmode_media_library.deps._which", return_value=None):
        status = check_ffmpeg()
    assert status.available is False


def test_check_rclone_not_found():
    with patch("godmode_media_library.deps._which", return_value=None):
        status = check_rclone()
    assert status.available is False
    assert "rclone.org" in status.install_hint


def test_check_pillow():
    status = check_pillow()
    # Pillow is in dev deps, should be available in test env
    assert isinstance(status, DependencyStatus)
    assert status.name == "Pillow"


def test_check_all_returns_list():
    with patch("godmode_media_library.deps._which", return_value=None):
        statuses = check_all()
    assert isinstance(statuses, list)
    assert len(statuses) == 8
    names = {s.name for s in statuses}
    assert "ExifTool" in names
    assert "ffprobe (FFmpeg)" in names
    assert "Pillow" in names


def test_format_report_all_missing():
    statuses = [
        DependencyStatus(name="Tool1", available=False, install_hint="install tool1"),
        DependencyStatus(name="Tool2", available=False, install_hint="install tool2"),
    ]
    report = format_report(statuses)
    assert "Missing" in report
    assert "Tool1" in report
    assert "install tool1" in report


def test_format_report_all_available():
    statuses = [
        DependencyStatus(name="Tool1", available=True, version="1.0"),
    ]
    report = format_report(statuses)
    assert "Available" in report
    assert "Tool1" in report
    assert "1.0" in report
    assert "All dependencies available" in report


def test_format_report_mixed():
    statuses = [
        DependencyStatus(name="ExifTool", available=True, version="12.76"),
        DependencyStatus(name="rclone", available=False, install_hint="https://rclone.org"),
    ]
    report = format_report(statuses)
    assert "ExifTool" in report
    assert "12.76" in report
    assert "rclone" in report
    assert "Missing" in report
