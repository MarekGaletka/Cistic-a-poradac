"""Unit tests for distributed_backup.py — backup planning, redundancy, restore."""

from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library.distributed_backup import (
    BackupManifestEntry,
    BackupPlan,
    BackupStats,
    BackupTarget,
    _compute_file_priority,
)


# ── BackupTarget ─────────────────────────────────────────────────────


class TestBackupTarget:
    def test_available_bytes_with_reserve(self):
        t = BackupTarget(remote_name="gdrive", free_bytes=1_000_000_000)
        # Should keep 500MB reserve
        assert t.available_bytes == 500_000_000

    def test_available_bytes_below_reserve(self):
        t = BackupTarget(remote_name="gdrive", free_bytes=100_000_000)
        assert t.available_bytes == 0  # max(0, 100M - 500M) = 0

    def test_available_bytes_zero_free(self):
        t = BackupTarget(remote_name="gdrive", free_bytes=0)
        assert t.available_bytes == 0

    def test_defaults(self):
        t = BackupTarget(remote_name="test")
        assert t.remote_path == "GML-Backup"
        assert t.enabled is True
        assert t.priority == 0
        assert t.encrypted is False


# ── BackupPlan ───────────────────────────────────────────────────────


class TestBackupPlan:
    def test_empty_plan(self):
        plan = BackupPlan()
        assert plan.total_files == 0
        assert plan.total_bytes == 0
        assert plan.entries == []
        assert plan.overflow_files == 0

    def test_plan_with_entries(self):
        plan = BackupPlan(
            entries=[{"file_id": 1, "size": 1000}],
            total_files=1,
            total_bytes=1000,
            targets_used=["gdrive"],
        )
        assert plan.total_files == 1
        assert len(plan.targets_used) == 1


# ── BackupStats ──────────────────────────────────────────────────────


class TestBackupStats:
    def test_defaults(self):
        stats = BackupStats()
        assert stats.total_files_in_catalog == 0
        assert stats.backup_coverage_pct == 0.0
        assert stats.last_backup_at is None
        assert stats.files_by_remote == {}


# ── _compute_file_priority ───────────────────────────────────────────


class TestComputeFilePriority:
    def test_raw_photo_with_gps_is_high_priority(self):
        row = {
            "ext": "cr2",
            "metadata_richness": 0.9,
            "gps_latitude": 50.0,
            "gps_longitude": 14.0,
            "date_original": "2023-06-15",
            "quality_category": "",
        }
        score = _compute_file_priority(row)
        # Should be very low (high priority): 200 - 80 - 50 - 20 - 15 = 35
        assert score < 50

    def test_screenshot_is_low_priority(self):
        row = {
            "ext": "png",
            "metadata_richness": 0.0,
            "gps_latitude": None,
            "gps_longitude": None,
            "date_original": None,
            "quality_category": "screenshot",
        }
        score = _compute_file_priority(row)
        # 200 - 40 + 150 = 310
        assert score >= 300

    def test_blurry_photo_penalized(self):
        row = {
            "ext": "jpg",
            "metadata_richness": 0.1,
            "gps_latitude": None,
            "gps_longitude": None,
            "date_original": None,
            "quality_category": "blurry",
        }
        score = _compute_file_priority(row)
        # 200 - 80 + 50 = 170
        assert score > 100

    def test_video_with_date_medium_priority(self):
        row = {
            "ext": "mp4",
            "metadata_richness": 0.5,
            "gps_latitude": None,
            "gps_longitude": None,
            "date_original": "2023-01-01",
            "quality_category": "",
        }
        score = _compute_file_priority(row)
        # 200 - 60 - 25 - 15 = 100
        assert 50 <= score <= 150

    def test_empty_row_gets_default(self):
        row = {}
        score = _compute_file_priority(row)
        assert score == 200  # base default

    def test_meme_is_low_priority(self):
        row = {
            "ext": "jpg",
            "metadata_richness": 0.0,
            "gps_latitude": None,
            "gps_longitude": None,
            "date_original": None,
            "quality_category": "meme",
        }
        score = _compute_file_priority(row)
        assert score >= 250  # 200 - 80 + 150 = 270

    def test_rich_metadata_boosts_priority(self):
        row_rich = {
            "ext": "jpg",
            "metadata_richness": 0.8,
            "gps_latitude": None,
            "gps_longitude": None,
            "date_original": None,
            "quality_category": "",
        }
        row_poor = {
            "ext": "jpg",
            "metadata_richness": 0.1,
            "gps_latitude": None,
            "gps_longitude": None,
            "date_original": None,
            "quality_category": "",
        }
        assert _compute_file_priority(row_rich) < _compute_file_priority(row_poor)


# ── BackupManifestEntry ─────────────────────────────────────────────


class TestBackupManifestEntry:
    def test_creation(self):
        entry = BackupManifestEntry(
            file_id=42,
            path="/photos/img.jpg",
            sha256="abc123",
            size=1024,
            remote_name="gdrive",
            remote_path="GML-Backup/2023/06",
            backed_up_at="2023-06-15T10:00:00",
        )
        assert entry.file_id == 42
        assert entry.verified is False
        assert entry.verified_at is None

    def test_verified_entry(self):
        entry = BackupManifestEntry(
            file_id=1,
            path="/a.jpg",
            sha256="def",
            size=100,
            remote_name="s3",
            remote_path="backup/",
            backed_up_at="2023-01-01T00:00:00",
            verified=True,
            verified_at="2023-02-01T00:00:00",
        )
        assert entry.verified is True
        assert entry.verified_at is not None
