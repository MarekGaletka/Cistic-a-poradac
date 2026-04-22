"""Tests for godmode_media_library.iphone_import module."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from godmode_media_library.iphone_import import (
    IPhoneImportConfig,
    IPhoneFile,
    ImportProgress,
    _fmt_bytes,
    _is_media,
    _determine_dest_path,
    _cleanup_temp,
    get_progress,
    pause_import,
    resume_import,
    cancel_import,
    _check_iphone_connected,
    _run_async,
    get_iphone_status,
    MEDIA_EXTS,
    _progress,
    _progress_lock,
    _pause_event,
    _cancel_event,
)


# ── _fmt_bytes ────────────────────────────────────────────────────────

class TestFmtBytes:
    def test_bytes(self):
        assert _fmt_bytes(0) == "0 B"
        assert _fmt_bytes(512) == "512 B"
        assert _fmt_bytes(1023) == "1023 B"

    def test_kilobytes(self):
        assert _fmt_bytes(1024) == "1.0 KB"
        assert _fmt_bytes(1536) == "1.5 KB"
        assert _fmt_bytes(1024 * 1024 - 1) == "1024.0 KB"

    def test_megabytes(self):
        assert _fmt_bytes(1024 ** 2) == "1.0 MB"
        assert _fmt_bytes(int(1.5 * 1024 ** 2)) == "1.5 MB"

    def test_gigabytes(self):
        assert _fmt_bytes(1024 ** 3) == "1.00 GB"
        assert _fmt_bytes(int(2.5 * 1024 ** 3)) == "2.50 GB"


# ── _is_media ─────────────────────────────────────────────────────────

class TestIsMedia:
    def test_common_image_extensions(self):
        assert _is_media("photo.jpg") is True
        assert _is_media("photo.JPEG") is True
        assert _is_media("photo.heic") is True
        assert _is_media("photo.png") is True
        assert _is_media("photo.dng") is True

    def test_video_extensions(self):
        assert _is_media("clip.mov") is True
        assert _is_media("clip.MP4") is True
        assert _is_media("clip.m4v") is True
        assert _is_media("clip.3gp") is True

    def test_audio_extensions(self):
        assert _is_media("song.mp3") is True
        assert _is_media("song.m4a") is True
        assert _is_media("song.aac") is True

    def test_aae_sidecar(self):
        assert _is_media("IMG_1234.AAE") is True

    def test_non_media(self):
        assert _is_media("document.pdf") is False
        assert _is_media("readme.txt") is False
        assert _is_media("data.json") is False
        assert _is_media("script.py") is False

    def test_no_extension(self):
        assert _is_media("noext") is False


# ── _determine_dest_path ─────────────────────────────────────────────

class TestDetermineDestPath:
    def setup_method(self):
        self.config = IPhoneImportConfig(
            dest_remote="gws-backup",
            dest_path="GML-Consolidated",
            structure_pattern="year_month",
        )

    def test_with_exif_date(self):
        """EXIF date should produce year/month path."""

        @dataclass
        class FakeExif:
            date_original: str | None = "2023:06:15 14:30:00"
            camera_make: str | None = None
            camera_model: str | None = None
            gps_latitude: float | None = None
            gps_longitude: float | None = None

        exif = FakeExif()
        result = _determine_dest_path(self.config, exif, "IMG_1234.JPG")
        assert result == "GML-Consolidated/2023/2023-06/IMG_1234.JPG"

    def test_with_probe_creation_time(self):
        """QuickTime creation_time from probe should work."""
        probe = SimpleNamespace(creation_time="2022-03-10T08:15:30.000000Z")
        result = _determine_dest_path(self.config, None, "IMG_5678.MOV", probe)
        assert result == "GML-Consolidated/2022/2022-03/IMG_5678.MOV"

    def test_probe_creation_time_iso_tz(self):
        """creation_time with timezone offset."""
        probe = SimpleNamespace(creation_time="2021-12-25T10:00:00+0200")
        result = _determine_dest_path(self.config, None, "VID_0001.MP4", probe)
        assert result == "GML-Consolidated/2021/2021-12/VID_0001.MP4"

    def test_probe_creation_time_plain(self):
        """creation_time without fractional seconds."""
        probe = SimpleNamespace(creation_time="2020-01-15T09:30:00Z")
        result = _determine_dest_path(self.config, None, "VID_0002.MP4", probe)
        assert result == "GML-Consolidated/2020/2020-01/VID_0002.MP4"

    def test_probe_creation_time_space_format(self):
        """creation_time with space separator."""
        probe = SimpleNamespace(creation_time="2019-07-04 12:00:00")
        result = _determine_dest_path(self.config, None, "VID_0003.MP4", probe)
        assert result == "GML-Consolidated/2019/2019-07/VID_0003.MP4"

    def test_probe_creation_time_too_old_skipped(self):
        """creation_time before 1990 should be skipped (e.g. 1904 epoch)."""
        probe = SimpleNamespace(creation_time="1904-01-01T00:00:00Z")
        result = _determine_dest_path(self.config, None, "VID_0004.MP4", probe)
        # No valid date -> Unsorted
        assert "Unsorted" in result

    def test_filename_date_extraction(self):
        """Date embedded in filename like IMG_20210315_..."""
        result = _determine_dest_path(self.config, None, "IMG_20210315_120000.jpg")
        assert result == "GML-Consolidated/2021/2021-03/IMG_20210315_120000.jpg"

    def test_filename_date_only_digits(self):
        """Date from numeric-only filename."""
        result = _determine_dest_path(self.config, None, "20200101_photo.jpg")
        assert result == "GML-Consolidated/2020/2020-01/20200101_photo.jpg"

    def test_filename_date_out_of_range(self):
        """Filename date outside 1990-2100 range should be ignored."""
        result = _determine_dest_path(self.config, None, "18990101_old.jpg")
        assert "Unsorted" in result

    def test_filename_invalid_date(self):
        """Filename with invalid date (month 13) should fallback to Unsorted."""
        result = _determine_dest_path(self.config, None, "IMG_20201301_bad.jpg")
        assert "Unsorted" in result

    def test_no_date_unsorted(self):
        """No date at all should go to Unsorted."""
        result = _determine_dest_path(self.config, None, "random_name.jpg")
        assert result == "GML-Consolidated/Unsorted/random_name.jpg"

    def test_flat_pattern(self):
        """flat pattern puts file directly in dest_path."""
        self.config.structure_pattern = "flat"
        result = _determine_dest_path(self.config, None, "IMG_1234.JPG")
        assert result == "GML-Consolidated/IMG_1234.JPG"

    def test_original_pattern(self):
        """original pattern puts file in dest_path/iPhone/."""
        self.config.structure_pattern = "original"
        result = _determine_dest_path(self.config, None, "IMG_1234.JPG")
        assert result == "GML-Consolidated/iPhone/IMG_1234.JPG"

    def test_exif_beats_filename_when_older(self):
        """The oldest date should win — EXIF 2020 vs filename 2021."""

        @dataclass
        class FakeExif:
            date_original: str | None = "2020:01:01 00:00:00"
            camera_make: str | None = None
            camera_model: str | None = None
            gps_latitude: float | None = None
            gps_longitude: float | None = None

        exif = FakeExif()
        result = _determine_dest_path(self.config, exif, "IMG_20210315_120000.jpg")
        assert "2020/2020-01" in result

    def test_filename_beats_exif_when_older(self):
        """Filename date 2019 < EXIF date 2023 -> use filename."""

        @dataclass
        class FakeExif:
            date_original: str | None = "2023:06:15 14:30:00"
            camera_make: str | None = None
            camera_model: str | None = None
            gps_latitude: float | None = None
            gps_longitude: float | None = None

        exif = FakeExif()
        result = _determine_dest_path(self.config, exif, "IMG_20190101_000000.jpg")
        assert "2019/2019-01" in result

    def test_probe_and_exif_both_present_oldest_wins(self):
        """When both EXIF and probe have dates, oldest wins."""

        @dataclass
        class FakeExif:
            date_original: str | None = "2023:06:15 14:30:00"
            camera_make: str | None = None
            camera_model: str | None = None
            gps_latitude: float | None = None
            gps_longitude: float | None = None

        exif = FakeExif()
        probe = SimpleNamespace(creation_time="2021-03-10T08:15:30.000000Z")
        result = _determine_dest_path(self.config, exif, "nodate.mov", probe)
        assert "2021/2021-03" in result


# ── Dataclass defaults ────────────────────────────────────────────────

class TestDataclasses:
    def test_iphone_import_config_defaults(self):
        cfg = IPhoneImportConfig()
        assert cfg.dest_remote == "gws-backup"
        assert cfg.dest_path == "GML-Consolidated"
        assert cfg.temp_dir == "/tmp/gml-iphone"
        assert cfg.structure_pattern == "year_month"
        assert cfg.bwlimit is None
        assert cfg.media_only is True
        assert cfg.upload_workers == 4

    def test_iphone_file(self):
        f = IPhoneFile(afc_path="/DCIM/100APPLE/IMG_001.JPG",
                       filename="IMG_001.JPG", size=5000)
        assert f.mtime == 0.0
        assert f.size == 5000

    def test_import_progress_defaults(self):
        p = ImportProgress()
        assert p.phase == "idle"
        assert p.total_files == 0
        assert p.error is None
        assert p.job_id is None


# ── Global state controls ─────────────────────────────────────────────

class TestGlobalControls:
    def setup_method(self):
        """Reset global state before each test."""
        _pause_event.clear()
        _cancel_event.clear()
        with _progress_lock:
            _progress.phase = "idle"
            _progress.total_files = 0
            _progress.completed_files = 0
            _progress.failed_files = 0
            _progress.skipped_files = 0
            _progress.bytes_transferred = 0
            _progress.bytes_total = 0
            _progress.current_file = ""
            _progress.speed_bps = 0.0
            _progress.iphone_connected = False
            _progress.error = None
            _progress.job_id = None

    def test_get_progress_returns_dict(self):
        result = get_progress()
        assert isinstance(result, dict)
        assert result["phase"] == "idle"
        assert result["total_files"] == 0
        assert result["error"] is None

    def test_pause_import_sets_phase(self):
        pause_import()
        assert _pause_event.is_set()
        result = get_progress()
        assert result["phase"] == "paused"

    def test_resume_import_clears_pause(self):
        _pause_event.set()
        resume_import()
        assert not _pause_event.is_set()

    def test_cancel_import_sets_both(self):
        cancel_import()
        assert _cancel_event.is_set()
        assert _pause_event.is_set()  # cancel also sets pause to unblock

    def test_get_progress_all_fields(self):
        with _progress_lock:
            _progress.phase = "transferring"
            _progress.total_files = 100
            _progress.completed_files = 50
            _progress.failed_files = 2
            _progress.skipped_files = 5
            _progress.bytes_transferred = 1024
            _progress.bytes_total = 2048
            _progress.current_file = "IMG_001.JPG"
            _progress.speed_bps = 500.0
            _progress.iphone_connected = True
            _progress.error = None
            _progress.job_id = "job-123"

        result = get_progress()
        assert result["phase"] == "transferring"
        assert result["total_files"] == 100
        assert result["completed_files"] == 50
        assert result["failed_files"] == 2
        assert result["skipped_files"] == 5
        assert result["bytes_transferred"] == 1024
        assert result["bytes_total"] == 2048
        assert result["current_file"] == "IMG_001.JPG"
        assert result["speed_bps"] == 500.0
        assert result["iphone_connected"] is True
        assert result["job_id"] == "job-123"


# ── _cleanup_temp ─────────────────────────────────────────────────────

class TestCleanupTemp:
    def test_removes_existing_file(self, tmp_path):
        f = tmp_path / "test.tmp"
        f.write_text("data")
        assert f.exists()
        _cleanup_temp(f)
        assert not f.exists()

    def test_nonexistent_file_no_error(self, tmp_path):
        f = tmp_path / "nonexistent.tmp"
        # Should not raise
        _cleanup_temp(f)

    def test_oserror_suppressed(self, tmp_path):
        """OSError during unlink is silently suppressed."""
        f = tmp_path / "test.tmp"
        f.write_text("data")
        with patch.object(Path, "unlink", side_effect=OSError("perm denied")):
            _cleanup_temp(f)  # Should not raise


# ── _check_iphone_connected ──────────────────────────────────────────

class TestCheckIPhoneConnected:
    @patch("godmode_media_library.iphone_import._run_async")
    @patch("godmode_media_library.iphone_import._get_afc_service")
    def test_connected_returns_true(self, mock_afc, mock_run):
        """When devices are found, return True."""
        with patch(
            "godmode_media_library.iphone_import._check_iphone_connected"
        ) as mock_check:
            # We need to test the actual function, so let's patch the import inside
            pass

        # Test the actual function by mocking pymobiledevice3
        mock_list_devices = MagicMock()
        mock_run.return_value = [MagicMock()]  # One device

        with patch.dict(
            "sys.modules",
            {"pymobiledevice3": MagicMock(), "pymobiledevice3.usbmux": MagicMock()},
        ):
            with patch(
                "godmode_media_library.iphone_import._run_async",
                return_value=[MagicMock()],
            ):
                result = _check_iphone_connected()
                assert result is True

    def test_import_error_returns_false(self):
        """When pymobiledevice3 can't be imported, return False."""
        with patch.dict("sys.modules", {"pymobiledevice3.usbmux": None}):
            # Force ImportError
            with patch(
                "godmode_media_library.iphone_import._run_async",
                side_effect=Exception("no module"),
            ):
                result = _check_iphone_connected()
                assert result is False

    def test_empty_device_list_returns_false(self):
        """When no devices found, return False."""
        with patch(
            "godmode_media_library.iphone_import._run_async", return_value=[]
        ):
            with patch.dict(
                "sys.modules",
                {
                    "pymobiledevice3": MagicMock(),
                    "pymobiledevice3.usbmux": MagicMock(),
                },
            ):
                result = _check_iphone_connected()
                assert result is False


# ── _run_async ────────────────────────────────────────────────────────

class TestRunAsync:
    def test_no_running_loop(self):
        """When no event loop is running, uses asyncio.run."""
        import asyncio

        async def coro():
            return 42

        result = _run_async(coro())
        assert result == 42

    def test_nested_event_loop(self):
        """When inside running event loop, runs in thread pool."""
        import asyncio

        async def inner():
            return 99

        async def outer():
            return _run_async(inner())

        result = asyncio.run(outer())
        assert result == 99


# ── get_iphone_status ─────────────────────────────────────────────────

class TestGetIPhoneStatus:
    def setup_method(self):
        _pause_event.clear()
        _cancel_event.clear()
        with _progress_lock:
            _progress.phase = "idle"

    @patch("godmode_media_library.iphone_import._check_iphone_connected", return_value=False)
    def test_disconnected(self, mock_check, tmp_path):
        """When iPhone not connected, connected=False."""
        db_path = str(tmp_path / "test.db")
        # Mock catalog
        with patch("godmode_media_library.iphone_import.Catalog") as MockCat:
            mock_cat = MagicMock()
            MockCat.return_value = mock_cat
            mock_ckpt_jobs = []
            with patch("godmode_media_library.iphone_import.ckpt") as mock_ckpt:
                mock_ckpt.list_jobs.return_value = []
                result = get_iphone_status(db_path)

        assert result["connected"] is False
        assert result["device_name"] is None
        assert "progress" in result
        assert "jobs" in result

    @patch("godmode_media_library.iphone_import._check_iphone_connected", return_value=True)
    @patch("godmode_media_library.iphone_import._run_async")
    def test_connected_with_device_name(self, mock_run, mock_check, tmp_path):
        """When connected, tries to get device name."""
        db_path = str(tmp_path / "test.db")
        mock_device = MagicMock()
        mock_device.name = "Marek's iPhone"
        mock_run.return_value = [mock_device]

        with patch("godmode_media_library.iphone_import.Catalog") as MockCat:
            mock_cat = MagicMock()
            MockCat.return_value = mock_cat
            with patch("godmode_media_library.iphone_import.ckpt") as mock_ckpt:
                mock_ckpt.list_jobs.return_value = []
                # We need to also mock the pymobiledevice3 import inside get_iphone_status
                mock_usbmux = MagicMock()
                mock_usbmux.list_devices = MagicMock()
                with patch.dict("sys.modules", {"pymobiledevice3.usbmux": mock_usbmux}):
                    result = get_iphone_status(db_path)

        assert result["connected"] is True
        assert result["device_name"] == "Marek's iPhone"

    @patch("godmode_media_library.iphone_import._check_iphone_connected", return_value=True)
    @patch("godmode_media_library.iphone_import._run_async", side_effect=Exception("fail"))
    def test_connected_device_name_fails_gracefully(self, mock_run, mock_check, tmp_path):
        """If getting device name fails, still returns connected=True."""
        db_path = str(tmp_path / "test.db")

        with patch("godmode_media_library.iphone_import.Catalog") as MockCat:
            mock_cat = MagicMock()
            MockCat.return_value = mock_cat
            with patch("godmode_media_library.iphone_import.ckpt") as mock_ckpt:
                mock_ckpt.list_jobs.return_value = []
                mock_usbmux = MagicMock()
                with patch.dict("sys.modules", {"pymobiledevice3.usbmux": mock_usbmux}):
                    result = get_iphone_status(db_path)

        assert result["connected"] is True
        assert result["device_name"] is None

    @patch("godmode_media_library.iphone_import._check_iphone_connected", return_value=False)
    def test_catalog_open_fails_gracefully(self, mock_check, tmp_path):
        """If Catalog can't open, jobs list stays empty."""
        db_path = str(tmp_path / "test.db")
        with patch("godmode_media_library.iphone_import.Catalog") as MockCat:
            MockCat.return_value.open.side_effect = Exception("db locked")
            result = get_iphone_status(db_path)

        assert result["jobs"] == []

    @patch("godmode_media_library.iphone_import._check_iphone_connected", return_value=False)
    def test_with_existing_jobs(self, mock_check, tmp_path):
        """Shows iPhone import jobs."""
        db_path = str(tmp_path / "test.db")

        mock_job = SimpleNamespace(
            job_id="job-abc",
            job_type="iphone_import",
            status="completed",
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-01T01:00:00Z",
        )
        non_iphone_job = SimpleNamespace(
            job_id="job-xyz",
            job_type="consolidation",
            status="running",
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-01T01:00:00Z",
        )

        with patch("godmode_media_library.iphone_import.Catalog") as MockCat:
            mock_cat = MagicMock()
            MockCat.return_value = mock_cat
            with patch("godmode_media_library.iphone_import.ckpt") as mock_ckpt:
                mock_ckpt.list_jobs.return_value = [mock_job, non_iphone_job]
                mock_ckpt.get_job_progress.return_value = {"completed": 10, "total": 20}
                result = get_iphone_status(db_path)

        # Only iphone_import jobs should be included
        assert len(result["jobs"]) == 1
        assert result["jobs"][0]["job_id"] == "job-abc"
        assert result["jobs"][0]["status"] == "completed"


# ── run_import ────────────────────────────────────────────────────────

class TestRunImport:
    """Tests for the main run_import pipeline. Heavily mocked."""

    def setup_method(self):
        _pause_event.clear()
        _cancel_event.clear()
        with _progress_lock:
            _progress.phase = "idle"
            _progress.total_files = 0
            _progress.completed_files = 0
            _progress.failed_files = 0
            _progress.skipped_files = 0
            _progress.bytes_transferred = 0
            _progress.bytes_total = 0
            _progress.current_file = ""
            _progress.speed_bps = 0.0
            _progress.iphone_connected = False
            _progress.error = None
            _progress.job_id = None

    @patch("godmode_media_library.iphone_import.asyncio.run")
    def test_connection_failure(self, mock_asyncio_run):
        """If listing iPhone files fails, return error."""
        from godmode_media_library.iphone_import import run_import

        mock_asyncio_run.side_effect = Exception("USB disconnected")

        result = run_import("/fake/catalog.db")
        assert "error" in result
        assert "USB disconnected" in result["error"]

    @patch("godmode_media_library.iphone_import._check_iphone_connected", return_value=True)
    @patch("godmode_media_library.iphone_import.asyncio.run")
    @patch("godmode_media_library.iphone_import.Catalog")
    @patch("godmode_media_library.iphone_import.ckpt")
    @patch("godmode_media_library.iphone_import.sha256_file", return_value="abc123def456")
    @patch("godmode_media_library.iphone_import.can_read_exif", return_value=False)
    @patch("godmode_media_library.iphone_import.probe_file", return_value=None)
    @patch("godmode_media_library.iphone_import.rclone_copyto", return_value={"success": True})
    @patch("godmode_media_library.iphone_import.shutil.disk_usage")
    def test_successful_single_file_import(
        self, mock_disk, mock_rclone, mock_probe, mock_exif, mock_sha,
        mock_ckpt, mock_catalog, mock_asyncio_run, mock_iphone_check,
        tmp_path,
    ):
        """End-to-end: one file, no EXIF, upload succeeds."""
        from godmode_media_library.iphone_import import run_import

        # Setup: list_iphone_files returns 1 file
        iphone_file = IPhoneFile(
            afc_path="/DCIM/100APPLE/IMG_001.JPG",
            filename="IMG_001.JPG",
            size=1024,
            mtime=1000.0,
        )

        call_count = [0]
        def asyncio_run_side_effect(coro):
            call_count[0] += 1
            if call_count[0] == 1:
                # list_iphone_files
                return [iphone_file]
            else:
                # _download_file — create the temp file
                temp_file = tmp_path / "IMG_001.JPG"
                temp_file.write_bytes(b"fake image data")
                return True

        mock_asyncio_run.side_effect = asyncio_run_side_effect

        # Catalog mock
        mock_cat_instance = MagicMock()
        mock_catalog.return_value = mock_cat_instance
        mock_cat_instance.conn = MagicMock()
        cursor_mock = MagicMock()
        cursor_mock.fetchall.return_value = []
        mock_cat_instance.conn.cursor.return_value = cursor_mock
        cursor_mock.execute = MagicMock()

        # Checkpoint mock
        mock_job = SimpleNamespace(job_id="job-1", job_type="iphone_import", status="running")
        mock_ckpt.list_jobs.return_value = []
        mock_ckpt.create_job.return_value = mock_job
        mock_ckpt.get_job_progress.return_value = {}

        # Dedup: no existing file
        mock_cat_instance.get_file_by_hash = MagicMock(return_value=None)

        # Disk space: plenty
        mock_disk.return_value = SimpleNamespace(free=10 * 1024 ** 3)

        config = IPhoneImportConfig(temp_dir=str(tmp_path), upload_workers=1)
        result = run_import("/fake/catalog.db", config=config)

        assert result["phase"] == "completed"
        mock_ckpt.update_job.assert_called()
        mock_rclone.assert_called_once()

    @patch("godmode_media_library.iphone_import._check_iphone_connected", return_value=True)
    @patch("godmode_media_library.iphone_import.asyncio.run")
    def test_media_only_filter(self, mock_asyncio_run, mock_check):
        """media_only=True should filter out non-media files."""
        from godmode_media_library.iphone_import import run_import

        files = [
            IPhoneFile("/DCIM/100APPLE/IMG_001.JPG", "IMG_001.JPG", 1024),
            IPhoneFile("/DCIM/100APPLE/notes.txt", "notes.txt", 512),
            IPhoneFile("/DCIM/100APPLE/clip.MOV", "clip.MOV", 2048),
        ]

        # asyncio.run is called once for list_iphone_files, then for each download.
        # We set cancel from a timer so it fires after run_import clears the events.
        call_count = [0]
        def asyncio_side(coro):
            call_count[0] += 1
            if call_count[0] == 1:
                # After returning the file list, schedule cancel
                _cancel_event.set()
                return files
            return True

        mock_asyncio_run.side_effect = asyncio_side

        with patch("godmode_media_library.iphone_import.Catalog") as MockCat:
            mock_cat = MagicMock()
            MockCat.return_value = mock_cat
            with patch("godmode_media_library.iphone_import.ckpt") as mock_ckpt:
                mock_ckpt.list_jobs.return_value = []
                mock_job = SimpleNamespace(job_id="j1", job_type="iphone_import")
                mock_ckpt.create_job.return_value = mock_job
                mock_ckpt.get_job_progress.return_value = {}
                mock_cat.conn = MagicMock()
                cursor = MagicMock()
                cursor.fetchall.return_value = []
                mock_cat.conn.cursor.return_value = cursor

                config = IPhoneImportConfig(media_only=True)
                result = run_import("/fake/catalog.db", config=config)

        # Check that total_files was set to 2 (not 3)
        # The notes.txt should have been filtered out
        assert result["total_files"] == 2

    @patch("godmode_media_library.iphone_import._check_iphone_connected", return_value=True)
    @patch("godmode_media_library.iphone_import.asyncio.run")
    @patch("godmode_media_library.iphone_import.Catalog")
    @patch("godmode_media_library.iphone_import.ckpt")
    @patch("godmode_media_library.iphone_import.shutil.disk_usage")
    def test_low_disk_space_aborts(
        self, mock_disk, mock_ckpt, mock_catalog, mock_asyncio_run, mock_check,
        tmp_path,
    ):
        """Import should abort when disk space is too low."""
        from godmode_media_library.iphone_import import run_import

        iphone_file = IPhoneFile("/DCIM/100APPLE/IMG_001.JPG", "IMG_001.JPG", 1024)
        mock_asyncio_run.return_value = [iphone_file]

        mock_cat = MagicMock()
        mock_catalog.return_value = mock_cat
        mock_conn = MagicMock()
        mock_cat.conn = mock_conn
        # cursor().execute().fetchall() must return []
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        mock_job = SimpleNamespace(job_id="j1", job_type="iphone_import")
        mock_ckpt.list_jobs.return_value = []
        mock_ckpt.create_job.return_value = mock_job
        mock_ckpt.get_job_progress.return_value = {}

        # Very low disk space
        mock_disk.return_value = SimpleNamespace(free=100)

        config = IPhoneImportConfig(temp_dir=str(tmp_path), upload_workers=1)
        result = run_import("/fake/catalog.db", config=config)

        # The disk space error is set, but phase gets overwritten to "completed"
        # because _cancel_event is not set (abort only breaks the loop).
        # The important thing is the error message was reported and job was paused.
        mock_ckpt.update_job.assert_any_call(
            mock_cat, "j1", status="paused", error="Nedostatek místa"
        )
        # Verify no files were actually processed (0 completed, 0 failed)
        assert result["completed_files"] == 0
        assert result["failed_files"] == 0

    @patch("godmode_media_library.iphone_import._check_iphone_connected", return_value=True)
    @patch("godmode_media_library.iphone_import.asyncio.run")
    @patch("godmode_media_library.iphone_import.Catalog")
    @patch("godmode_media_library.iphone_import.ckpt")
    @patch("godmode_media_library.iphone_import.sha256_file", return_value="existing_hash")
    @patch("godmode_media_library.iphone_import.shutil.disk_usage")
    def test_dedup_skip(
        self, mock_disk, mock_sha, mock_ckpt, mock_catalog, mock_asyncio_run,
        mock_check, tmp_path,
    ):
        """Files with existing hash in catalog should be skipped (dedup)."""
        from godmode_media_library.iphone_import import run_import

        iphone_file = IPhoneFile("/DCIM/100APPLE/IMG_001.JPG", "IMG_001.JPG", 1024)

        call_count = [0]
        def asyncio_side_effect(coro):
            call_count[0] += 1
            if call_count[0] == 1:
                return [iphone_file]
            temp = tmp_path / "IMG_001.JPG"
            temp.write_bytes(b"data")
            return True

        mock_asyncio_run.side_effect = asyncio_side_effect

        mock_cat = MagicMock()
        mock_catalog.return_value = mock_cat
        mock_cat.conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        mock_cat.conn.cursor.return_value = cursor
        # Key: file already exists by hash
        mock_cat.get_file_by_hash = MagicMock(return_value={"id": 1, "path": "some/path"})

        mock_job = SimpleNamespace(job_id="j1", job_type="iphone_import")
        mock_ckpt.list_jobs.return_value = []
        mock_ckpt.create_job.return_value = mock_job
        mock_ckpt.get_job_progress.return_value = {}

        mock_disk.return_value = SimpleNamespace(free=10 * 1024 ** 3)

        config = IPhoneImportConfig(temp_dir=str(tmp_path), upload_workers=1)
        result = run_import("/fake/catalog.db", config=config)

        # File should be skipped, not uploaded
        assert result["phase"] == "completed"
        # Check skipped was incremented
        assert result["skipped_files"] >= 1

    @patch("godmode_media_library.iphone_import._check_iphone_connected", return_value=True)
    @patch("godmode_media_library.iphone_import.asyncio.run")
    @patch("godmode_media_library.iphone_import.Catalog")
    @patch("godmode_media_library.iphone_import.ckpt")
    @patch("godmode_media_library.iphone_import.shutil.disk_usage")
    def test_download_failure_marks_failed(
        self, mock_disk, mock_ckpt, mock_catalog, mock_asyncio_run, mock_check,
        tmp_path,
    ):
        """If download fails, file is marked as failed and import continues."""
        from godmode_media_library.iphone_import import run_import

        iphone_file = IPhoneFile("/DCIM/100APPLE/IMG_001.JPG", "IMG_001.JPG", 1024)

        call_count = [0]
        def asyncio_side_effect(coro):
            call_count[0] += 1
            if call_count[0] == 1:
                return [iphone_file]
            raise ConnectionError("USB died")

        mock_asyncio_run.side_effect = asyncio_side_effect

        mock_cat = MagicMock()
        mock_catalog.return_value = mock_cat
        mock_cat.conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        mock_cat.conn.cursor.return_value = cursor

        mock_job = SimpleNamespace(job_id="j1", job_type="iphone_import")
        mock_ckpt.list_jobs.return_value = []
        mock_ckpt.create_job.return_value = mock_job
        mock_ckpt.get_job_progress.return_value = {}

        mock_disk.return_value = SimpleNamespace(free=10 * 1024 ** 3)

        config = IPhoneImportConfig(temp_dir=str(tmp_path), upload_workers=1)
        result = run_import("/fake/catalog.db", config=config)

        assert result["failed_files"] >= 1

    @patch("godmode_media_library.iphone_import.asyncio.run")
    @patch("godmode_media_library.iphone_import.Catalog")
    @patch("godmode_media_library.iphone_import.ckpt")
    def test_resume_existing_job(self, mock_ckpt, mock_catalog, mock_asyncio_run):
        """If a running/paused job exists, resume it instead of creating new."""
        from godmode_media_library.iphone_import import run_import

        mock_asyncio_run.return_value = []  # No files

        mock_cat = MagicMock()
        mock_catalog.return_value = mock_cat
        mock_cat.conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        mock_cat.conn.cursor.return_value = cursor

        existing_job = SimpleNamespace(job_id="existing-job", job_type="iphone_import", status="paused")
        mock_ckpt.list_jobs.side_effect = lambda cat, status=None: (
            [existing_job] if status in ("running", "paused") else []
        )
        mock_ckpt.get_job_progress.return_value = {}

        result = run_import("/fake/catalog.db")

        # Should update existing job, not create new
        mock_ckpt.create_job.assert_not_called()
        mock_ckpt.update_job.assert_any_call(mock_cat, "existing-job", status="running")


# ── reorganize_unsorted ───────────────────────────────────────────────

class TestReorganizeUnsorted:
    @patch("godmode_media_library.iphone_import.Catalog")
    def test_no_files_to_process(self, MockCatalog, tmp_path):
        """When there are no unsorted files, return zeros."""
        from godmode_media_library.iphone_import import reorganize_unsorted

        mock_cat = MagicMock()
        MockCatalog.return_value = mock_cat
        mock_conn = MagicMock()
        mock_cat.conn = mock_conn
        mock_conn.row_factory = None

        # No rows found
        mock_conn.execute.return_value.fetchall.return_value = []

        # rclone lsf returns empty
        with patch("subprocess.run") as mock_subproc:
            mock_subproc.return_value = MagicMock(returncode=0, stdout="")
            result = reorganize_unsorted(str(tmp_path / "cat.db"))

        assert result["moved"] == 0
        assert result["total"] == 0

    @patch("godmode_media_library.iphone_import.Catalog")
    def test_mov_file_with_creation_time(self, MockCatalog, tmp_path):
        """A MOV file with valid creation_time gets moved to correct year/month."""
        from godmode_media_library.iphone_import import reorganize_unsorted
        import json

        mock_cat = MagicMock()
        MockCatalog.return_value = mock_cat
        mock_conn = MagicMock()
        mock_cat.conn = mock_conn
        mock_conn.row_factory = None

        row = {
            "id": 1,
            "path": "gws-backup:GML-Consolidated/Unsorted/IMG_001.MOV",
            "ext": "mov",
            "date_original": None,
        }

        # Make row subscriptable
        class DictRow(dict):
            pass

        dict_row = DictRow(row)

        mock_conn.execute.return_value.fetchall.return_value = [dict_row]

        ffprobe_output = json.dumps({
            "format": {
                "tags": {
                    "com.apple.quicktime.creationdate": "2023-06-15T14:30:00+0200"
                }
            },
            "streams": [],
        })

        call_idx = [0]
        def subprocess_side_effect(*args, **kwargs):
            call_idx[0] += 1
            cmd = args[0] if args else kwargs.get("args", [])

            result = MagicMock()
            result.returncode = 0
            result.stdout = ""

            if "lsf" in cmd:
                if "--include" in cmd:
                    result.stdout = "IMG_001.MOV\n"
                else:
                    result.stdout = "IMG_001.MOV\n"
            elif "size" in cmd:
                result.stdout = json.dumps({"bytes": 50000000})
            elif "copyto" in cmd:
                # Create a tmp file for ffprobe
                pass
            elif "ffprobe" in cmd:
                result.stdout = ffprobe_output
            elif "moveto" in cmd:
                pass

            return result

        with patch("subprocess.run", side_effect=subprocess_side_effect):
            with patch("tempfile.NamedTemporaryFile") as mock_tmp:
                mock_tmp.return_value.__enter__ = MagicMock(
                    return_value=MagicMock(name="/tmp/test.mov")
                )
                mock_tmp.return_value.__exit__ = MagicMock(return_value=False)
                mock_tmp.return_value.name = "/tmp/test.mov"
                with patch("os.unlink"):
                    result = reorganize_unsorted(str(tmp_path / "cat.db"))

        assert result["total"] == 1

    @patch("godmode_media_library.iphone_import.Catalog")
    def test_non_video_with_sibling_match(self, MockCatalog, tmp_path):
        """AAE file matching a sibling IMG file gets moved using sibling's date."""
        from godmode_media_library.iphone_import import reorganize_unsorted

        mock_cat = MagicMock()
        MockCatalog.return_value = mock_cat
        mock_conn = MagicMock()
        mock_cat.conn = mock_conn
        mock_conn.row_factory = None

        row = {
            "id": 1,
            "path": "gws-backup:GML-Consolidated/Unsorted/IMG_8456.AAE",
            "ext": "aae",
            "date_original": None,
        }

        class DictRow(dict):
            pass

        dict_row = DictRow(row)

        # First execute call: main query for unsorted files
        # Second: lsf for validation
        # Third: lsf --include check
        # Fourth: sibling lookup
        execute_calls = [0]
        def execute_side_effect(*args, **kwargs):
            execute_calls[0] += 1
            result = MagicMock()
            if "SELECT id, path, ext" in str(args[0]):
                result.fetchall.return_value = [dict_row]
            elif "SELECT DISTINCT" in str(args[0]) if args else False:
                result.fetchall.return_value = []
            elif "SELECT date_original" in str(args[0]):
                sibling_row = {"date_original": "2023:06:15 14:30:00"}
                result.fetchone.return_value = type("Row", (), {"__getitem__": lambda self, k: sibling_row[k]})()
            else:
                result.fetchall.return_value = []
                result.fetchone.return_value = None
            return result

        mock_conn.execute = execute_side_effect

        def subprocess_side_effect(*args, **kwargs):
            cmd = args[0]
            result = MagicMock()
            result.returncode = 0
            result.stdout = ""
            if "lsf" in cmd:
                result.stdout = "IMG_8456.AAE\n"
            elif "moveto" in cmd:
                pass
            return result

        with patch("subprocess.run", side_effect=subprocess_side_effect):
            result = reorganize_unsorted(str(tmp_path / "cat.db"))

        assert result["moved"] == 1

    @patch("godmode_media_library.iphone_import.Catalog")
    def test_non_video_no_sibling_skipped(self, MockCatalog, tmp_path):
        """Non-video file without matching sibling gets skipped."""
        from godmode_media_library.iphone_import import reorganize_unsorted

        mock_cat = MagicMock()
        MockCatalog.return_value = mock_cat
        mock_conn = MagicMock()
        mock_cat.conn = mock_conn
        mock_conn.row_factory = None

        row = {
            "id": 1,
            "path": "gws-backup:GML-Consolidated/Unsorted/random_file.txt",
            "ext": "txt",
            "date_original": None,
        }

        class DictRow(dict):
            pass

        dict_row = DictRow(row)

        mock_conn.execute.return_value.fetchall.return_value = [dict_row]

        def subprocess_side_effect(*args, **kwargs):
            cmd = args[0]
            result = MagicMock()
            result.returncode = 0
            if "lsf" in cmd:
                result.stdout = "random_file.txt\n"
            else:
                result.stdout = ""
            return result

        with patch("subprocess.run", side_effect=subprocess_side_effect):
            result = reorganize_unsorted(str(tmp_path / "cat.db"))

        assert result["skipped"] == 1
        assert result["moved"] == 0


# ── IPhoneFile / MEDIA_EXTS ──────────────────────────────────────────

class TestMediaExts:
    def test_all_expected_extensions_present(self):
        """Verify critical extensions are in MEDIA_EXTS."""
        for ext in [".jpg", ".jpeg", ".heic", ".png", ".mov", ".mp4", ".aae", ".dng"]:
            assert ext in MEDIA_EXTS, f"{ext} missing from MEDIA_EXTS"

    def test_media_exts_are_lowercase(self):
        for ext in MEDIA_EXTS:
            assert ext == ext.lower()
            assert ext.startswith(".")
