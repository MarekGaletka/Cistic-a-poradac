"""Unit tests for distributed_backup.py — backup planning, execution, verification, auto-heal.

Expands coverage from ~56% to 70%+.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library.distributed_backup import (
    BackupManifestEntry,
    BackupPlan,
    BackupStats,
    BackupTarget,
    _compute_file_priority,
    create_backup_plan,
    ensure_backup_tables,
    execute_backup_plan,
    get_backup_stats,
    get_files_for_backup,
    get_manifest_for_file,
    get_targets,
    remove_backup_entry,
    set_target_enabled,
    set_target_priority,
    verify_backups,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_catalog(tmp_path):
    """Create an in-memory catalog mock with real SQLite connection."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL,
            sha256 TEXT,
            size INTEGER NOT NULL,
            ext TEXT,
            date_original TEXT,
            gps_latitude REAL,
            gps_longitude REAL,
            metadata_richness REAL,
            quality_category TEXT
        )
    """)
    conn.commit()
    cat = MagicMock()
    cat.conn = conn
    return cat


# ---------------------------------------------------------------------------
# BackupTarget
# ---------------------------------------------------------------------------


class TestBackupTarget:
    def test_available_bytes_with_reserve(self):
        t = BackupTarget(remote_name="gdrive", free_bytes=1_000_000_000)
        assert t.available_bytes == 500_000_000

    def test_available_bytes_below_reserve(self):
        t = BackupTarget(remote_name="gdrive", free_bytes=100_000_000)
        assert t.available_bytes == 0

    def test_available_bytes_zero_free(self):
        t = BackupTarget(remote_name="gdrive", free_bytes=0)
        assert t.available_bytes == 0

    def test_defaults(self):
        t = BackupTarget(remote_name="test")
        assert t.remote_path == "GML-Backup"
        assert t.enabled is True
        assert t.priority == 0
        assert t.encrypted is False


# ---------------------------------------------------------------------------
# BackupPlan / BackupStats / BackupManifestEntry
# ---------------------------------------------------------------------------


class TestBackupPlan:
    def test_empty_plan(self):
        plan = BackupPlan()
        assert plan.total_files == 0
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


class TestBackupStats:
    def test_defaults(self):
        stats = BackupStats()
        assert stats.total_files_in_catalog == 0
        assert stats.last_backup_at is None
        assert stats.files_by_remote == {}


class TestBackupManifestEntry:
    def test_creation(self):
        entry = BackupManifestEntry(
            file_id=42,
            path="/a.jpg",
            sha256="abc",
            size=1024,
            remote_name="gdrive",
            remote_path="GML-Backup/2023",
            backed_up_at="2023-06-15T10:00:00",
        )
        assert entry.file_id == 42
        assert entry.verified is False

    def test_verified_entry(self):
        entry = BackupManifestEntry(
            file_id=1,
            path="/a.jpg",
            sha256="def",
            size=100,
            remote_name="s3",
            remote_path="backup/",
            backed_up_at="2023-01-01",
            verified=True,
            verified_at="2023-02-01",
        )
        assert entry.verified is True


# ---------------------------------------------------------------------------
# _compute_file_priority
# ---------------------------------------------------------------------------


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
        assert _compute_file_priority(row) < 50

    def test_screenshot_is_low_priority(self):
        row = {
            "ext": "png",
            "metadata_richness": 0.0,
            "gps_latitude": None,
            "gps_longitude": None,
            "date_original": None,
            "quality_category": "screenshot",
        }
        assert _compute_file_priority(row) >= 300

    def test_blurry_photo_penalized(self):
        row = {
            "ext": "jpg",
            "metadata_richness": 0.1,
            "gps_latitude": None,
            "gps_longitude": None,
            "date_original": None,
            "quality_category": "blurry",
        }
        assert _compute_file_priority(row) > 100

    def test_empty_row_gets_default(self):
        assert _compute_file_priority({}) == 200

    def test_meme_is_low_priority(self):
        row = {
            "ext": "jpg",
            "metadata_richness": 0.0,
            "gps_latitude": None,
            "gps_longitude": None,
            "date_original": None,
            "quality_category": "meme",
        }
        assert _compute_file_priority(row) >= 250

    def test_medium_metadata_boost(self):
        row = {"ext": "jpg", "metadata_richness": 0.5, "quality_category": ""}
        score = _compute_file_priority(row)
        # 200 - 80 - 25 = 95
        assert score < 200

    def test_video_extensions(self):
        for ext in ("mp4", "mov", "avi", "mkv", "m4v"):
            row = {"ext": ext, "quality_category": ""}
            score = _compute_file_priority(row)
            assert score == 200 - 60  # base - video boost


# ---------------------------------------------------------------------------
# ensure_backup_tables
# ---------------------------------------------------------------------------


class TestEnsureBackupTables:
    def test_creates_tables(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        # Verify tables exist
        tables = mock_catalog.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = {t[0] for t in tables}
        assert "backup_targets" in table_names
        assert "backup_manifest" in table_names

    def test_idempotent(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        ensure_backup_tables(mock_catalog)  # Should not error


# ---------------------------------------------------------------------------
# get_targets / set_target_enabled / set_target_priority
# ---------------------------------------------------------------------------


class TestTargetManagement:
    def test_get_targets_empty(self, mock_catalog):
        targets = get_targets(mock_catalog)
        assert targets == []

    def test_get_targets_with_data(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_targets (remote_name, remote_path, enabled, priority, "
            "total_bytes, used_bytes, free_bytes, encrypted, crypt_remote) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("gdrive", "GML-Backup", 1, 0, 15_000_000_000, 5_000_000_000, 10_000_000_000, 0, ""),
        )
        mock_catalog.conn.commit()

        targets = get_targets(mock_catalog)
        assert len(targets) == 1
        assert targets[0].remote_name == "gdrive"
        assert targets[0].total_bytes == 15_000_000_000
        assert targets[0].enabled is True

    def test_set_target_enabled(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute("INSERT INTO backup_targets (remote_name) VALUES (?)", ("gdrive",))
        mock_catalog.conn.commit()

        set_target_enabled(mock_catalog, "gdrive", False)
        targets = get_targets(mock_catalog)
        assert targets[0].enabled is False

        set_target_enabled(mock_catalog, "gdrive", True)
        targets = get_targets(mock_catalog)
        assert targets[0].enabled is True

    def test_set_target_priority(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute("INSERT INTO backup_targets (remote_name) VALUES (?)", ("gdrive",))
        mock_catalog.conn.commit()

        set_target_priority(mock_catalog, "gdrive", 5)
        targets = get_targets(mock_catalog)
        assert targets[0].priority == 5


# ---------------------------------------------------------------------------
# get_files_for_backup
# ---------------------------------------------------------------------------


class TestGetFilesForBackup:
    def test_empty_catalog(self, mock_catalog):
        files = get_files_for_backup(mock_catalog)
        assert files == []

    def test_returns_unbacked_files(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO files (path, sha256, size, ext) VALUES (?, ?, ?, ?)",
            ("/tmp/photo.jpg", "abc123", 1024, "jpg"),
        )
        mock_catalog.conn.commit()

        files = get_files_for_backup(mock_catalog)
        assert len(files) == 1
        assert files[0]["path"] == "/tmp/photo.jpg"
        assert "priority" in files[0]

    def test_excludes_backed_up_files(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO files (path, sha256, size, ext) VALUES (?, ?, ?, ?)",
            ("/tmp/photo.jpg", "abc123", 1024, "jpg"),
        )
        file_id = mock_catalog.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        mock_catalog.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (file_id, "/tmp/photo.jpg", "abc123", 1024, "gdrive", "GML-Backup", "2023-01-01"),
        )
        mock_catalog.conn.commit()

        files = get_files_for_backup(mock_catalog)
        assert files == []

    def test_excludes_files_without_sha256(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO files (path, sha256, size, ext) VALUES (?, ?, ?, ?)",
            ("/tmp/photo.jpg", None, 1024, "jpg"),
        )
        mock_catalog.conn.commit()

        files = get_files_for_backup(mock_catalog)
        assert files == []

    def test_limit(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        for i in range(5):
            mock_catalog.conn.execute(
                "INSERT INTO files (path, sha256, size, ext) VALUES (?, ?, ?, ?)",
                (f"/tmp/photo{i}.jpg", f"sha{i}", 1024, "jpg"),
            )
        mock_catalog.conn.commit()

        files = get_files_for_backup(mock_catalog, limit=2)
        assert len(files) == 2

    def test_sorted_by_priority(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        # High priority: raw with GPS
        mock_catalog.conn.execute(
            "INSERT INTO files (path, sha256, size, ext, gps_latitude, gps_longitude, metadata_richness, date_original) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("/tmp/photo.cr2", "sha1", 2048, "cr2", 50.0, 14.0, 0.9, "2023-06-15"),
        )
        # Low priority: screenshot
        mock_catalog.conn.execute(
            "INSERT INTO files (path, sha256, size, ext, quality_category) VALUES (?, ?, ?, ?, ?)",
            ("/tmp/screen.png", "sha2", 512, "png", "screenshot"),
        )
        mock_catalog.conn.commit()

        files = get_files_for_backup(mock_catalog)
        assert len(files) == 2
        assert files[0]["ext"] == "cr2"  # Higher priority first
        assert files[1]["quality_category"] == "screenshot"


# ---------------------------------------------------------------------------
# create_backup_plan
# ---------------------------------------------------------------------------


class TestCreateBackupPlan:
    def test_no_targets(self, mock_catalog):
        plan = create_backup_plan(mock_catalog)
        assert plan.total_files == 0
        assert plan.entries == []

    def test_no_files(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_targets (remote_name, enabled, free_bytes, total_bytes) VALUES (?, ?, ?, ?)",
            ("gdrive", 1, 10_000_000_000, 15_000_000_000),
        )
        mock_catalog.conn.commit()

        plan = create_backup_plan(mock_catalog)
        assert plan.total_files == 0

    def test_basic_plan(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_targets (remote_name, enabled, free_bytes, total_bytes) VALUES (?, ?, ?, ?)",
            ("gdrive", 1, 10_000_000_000, 15_000_000_000),
        )
        mock_catalog.conn.execute(
            "INSERT INTO files (path, sha256, size, ext, date_original) VALUES (?, ?, ?, ?, ?)",
            ("/tmp/photo.jpg", "abc", 1024, "jpg", "2023-06-15"),
        )
        mock_catalog.conn.commit()

        plan = create_backup_plan(mock_catalog)
        assert plan.total_files == 1
        assert "gdrive" in plan.targets_used

    def test_overflow_when_no_space(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        # Target with very little free space
        mock_catalog.conn.execute(
            "INSERT INTO backup_targets (remote_name, enabled, free_bytes, total_bytes) VALUES (?, ?, ?, ?)",
            ("gdrive", 1, 500_000_001, 1_000_000_000),  # 1 byte available after reserve
        )
        # File bigger than available
        mock_catalog.conn.execute(
            "INSERT INTO files (path, sha256, size, ext) VALUES (?, ?, ?, ?)",
            ("/tmp/big.mp4", "sha1", 100_000_000, "mp4"),
        )
        mock_catalog.conn.commit()

        plan = create_backup_plan(mock_catalog)
        assert plan.overflow_files == 1

    def test_date_based_path(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_targets (remote_name, remote_path, enabled, free_bytes, total_bytes) VALUES (?, ?, ?, ?, ?)",
            ("gdrive", "GML-Backup", 1, 10_000_000_000, 15_000_000_000),
        )
        mock_catalog.conn.execute(
            "INSERT INTO files (path, sha256, size, ext, date_original) VALUES (?, ?, ?, ?, ?)",
            ("/tmp/photo.jpg", "abc", 1024, "jpg", "2023-06-15"),
        )
        mock_catalog.conn.commit()

        plan = create_backup_plan(mock_catalog)
        assert plan.entries[0]["target_path"] == "GML-Backup/2023-06"

    def test_unsorted_path_no_date(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_targets (remote_name, remote_path, enabled, free_bytes, total_bytes) VALUES (?, ?, ?, ?, ?)",
            ("gdrive", "GML-Backup", 1, 10_000_000_000, 15_000_000_000),
        )
        mock_catalog.conn.execute(
            "INSERT INTO files (path, sha256, size, ext) VALUES (?, ?, ?, ?)",
            ("/tmp/photo.jpg", "abc", 1024, "jpg"),
        )
        mock_catalog.conn.commit()

        plan = create_backup_plan(mock_catalog)
        assert "unsorted" in plan.entries[0]["target_path"]


# ---------------------------------------------------------------------------
# execute_backup_plan
# ---------------------------------------------------------------------------


class TestExecuteBackupPlan:
    def test_empty_plan(self, mock_catalog):
        result = execute_backup_plan(mock_catalog, plan=BackupPlan())
        assert result["uploaded"] == 0
        assert result["message"] == "No files to back up"

    def test_dry_run(self, mock_catalog, tmp_path):
        ensure_backup_tables(mock_catalog)
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"test")

        plan = BackupPlan(
            entries=[
                {
                    "file_id": 1,
                    "path": str(f),
                    "size": 4,
                    "sha256": "abc",
                    "priority": 100,
                    "target_remote": "gdrive",
                    "target_path": "GML-Backup/2023-06",
                }
            ],
            total_files=1,
            total_bytes=4,
            targets_used=["gdrive"],
        )

        result = execute_backup_plan(mock_catalog, plan=plan, dry_run=True)
        assert result["uploaded"] == 1
        assert result["dry_run"] is True

    def test_file_not_found_skipped(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        plan = BackupPlan(
            entries=[
                {
                    "file_id": 1,
                    "path": "/nonexistent/photo.jpg",
                    "size": 1024,
                    "sha256": "abc",
                    "priority": 100,
                    "target_remote": "gdrive",
                    "target_path": "GML-Backup/2023-06",
                }
            ],
            total_files=1,
            total_bytes=1024,
            targets_used=["gdrive"],
        )

        result = execute_backup_plan(mock_catalog, plan=plan)
        assert result["skipped"] == 1
        assert result["uploaded"] == 0

    def test_upload_success(self, mock_catalog, tmp_path):
        ensure_backup_tables(mock_catalog)
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"test_content")

        plan = BackupPlan(
            entries=[
                {
                    "file_id": 1,
                    "path": str(f),
                    "size": 12,
                    "sha256": "abc123",
                    "priority": 100,
                    "target_remote": "gdrive",
                    "target_path": "GML-Backup/2023-06",
                }
            ],
            total_files=1,
            total_bytes=12,
            targets_used=["gdrive"],
        )

        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stderr = ""

        with (
            patch("godmode_media_library.distributed_backup._rclone_bin", return_value="rclone"),
            patch("subprocess.run", return_value=mock_proc),
        ):
            result = execute_backup_plan(mock_catalog, plan=plan)

        assert result["uploaded"] == 1
        assert result["errors"] == 0

        # Verify manifest entry was created
        row = mock_catalog.conn.execute("SELECT * FROM backup_manifest WHERE file_id = 1").fetchone()
        assert row is not None

    def test_upload_failure(self, mock_catalog, tmp_path):
        ensure_backup_tables(mock_catalog)
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"test")

        plan = BackupPlan(
            entries=[
                {
                    "file_id": 1,
                    "path": str(f),
                    "size": 4,
                    "sha256": "abc",
                    "priority": 100,
                    "target_remote": "gdrive",
                    "target_path": "GML-Backup/2023-06",
                }
            ],
            total_files=1,
            total_bytes=4,
            targets_used=["gdrive"],
        )

        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.stderr = "permission denied"

        with (
            patch("godmode_media_library.distributed_backup._rclone_bin", return_value="rclone"),
            patch("subprocess.run", return_value=mock_proc),
        ):
            result = execute_backup_plan(mock_catalog, plan=plan)

        assert result["errors"] == 1
        assert result["uploaded"] == 0

    def test_upload_exception(self, mock_catalog, tmp_path):
        ensure_backup_tables(mock_catalog)
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"test")

        plan = BackupPlan(
            entries=[
                {
                    "file_id": 1,
                    "path": str(f),
                    "size": 4,
                    "sha256": "abc",
                    "priority": 100,
                    "target_remote": "gdrive",
                    "target_path": "GML-Backup/2023-06",
                }
            ],
            total_files=1,
            total_bytes=4,
            targets_used=["gdrive"],
        )

        with (
            patch("godmode_media_library.distributed_backup._rclone_bin", return_value="rclone"),
            patch("subprocess.run", side_effect=OSError("rclone not found")),
        ):
            result = execute_backup_plan(mock_catalog, plan=plan)

        assert result["errors"] == 1

    def test_crypt_remote_upload(self, mock_catalog, tmp_path):
        ensure_backup_tables(mock_catalog)
        # Add target with crypt overlay
        mock_catalog.conn.execute(
            "INSERT INTO backup_targets (remote_name, remote_path, enabled, free_bytes, "
            "total_bytes, encrypted, crypt_remote) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("gdrive", "GML-Backup", 1, 10_000_000_000, 15_000_000_000, 1, "gdrive-crypt"),
        )
        mock_catalog.conn.commit()

        f = tmp_path / "photo.jpg"
        f.write_bytes(b"test")

        plan = BackupPlan(
            entries=[
                {
                    "file_id": 1,
                    "path": str(f),
                    "size": 4,
                    "sha256": "abc",
                    "priority": 100,
                    "target_remote": "gdrive",
                    "target_path": "GML-Backup/2023-06",
                }
            ],
            total_files=1,
            total_bytes=4,
            targets_used=["gdrive"],
        )

        mock_proc = MagicMock()
        mock_proc.returncode = 0

        with (
            patch("godmode_media_library.distributed_backup._rclone_bin", return_value="rclone"),
            patch("subprocess.run", return_value=mock_proc) as mock_run,
        ):
            result = execute_backup_plan(mock_catalog, plan=plan)

        assert result["uploaded"] == 1
        assert result["encrypted_files"] == 1
        # Verify the crypt remote was used in the rclone command
        cmd_args = mock_run.call_args[0][0]
        assert "gdrive-crypt" in cmd_args[3]

    def test_progress_fn_called(self, mock_catalog, tmp_path):
        ensure_backup_tables(mock_catalog)
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"test")

        plan = BackupPlan(
            entries=[
                {
                    "file_id": 1,
                    "path": str(f),
                    "size": 4,
                    "sha256": "abc",
                    "priority": 100,
                    "target_remote": "gdrive",
                    "target_path": "GML-Backup/2023-06",
                }
            ],
            total_files=1,
            total_bytes=4,
            targets_used=["gdrive"],
        )

        progress_calls = []
        mock_proc = MagicMock()
        mock_proc.returncode = 0

        with (
            patch("godmode_media_library.distributed_backup._rclone_bin", return_value="rclone"),
            patch("subprocess.run", return_value=mock_proc),
        ):
            execute_backup_plan(
                mock_catalog,
                plan=plan,
                progress_fn=lambda p: progress_calls.append(p),
            )

        assert len(progress_calls) >= 2  # uploading + complete
        assert progress_calls[-1]["phase"] == "complete"


# ---------------------------------------------------------------------------
# get_backup_stats
# ---------------------------------------------------------------------------


class TestGetBackupStats:
    def test_empty_stats(self, mock_catalog):
        stats = get_backup_stats(mock_catalog)
        assert stats.total_files_in_catalog == 0
        assert stats.backed_up_files == 0
        assert stats.backup_coverage_pct == 0.0

    def test_stats_with_data(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO files (path, sha256, size, ext) VALUES (?, ?, ?, ?)",
            ("/tmp/photo.jpg", "abc", 1024, "jpg"),
        )
        file_id = mock_catalog.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        mock_catalog.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (file_id, "/tmp/photo.jpg", "abc", 1024, "gdrive", "GML-Backup", "2023-06-15"),
        )
        mock_catalog.conn.commit()

        stats = get_backup_stats(mock_catalog)
        assert stats.total_files_in_catalog == 1
        assert stats.backed_up_files == 1
        assert stats.backup_coverage_pct == 100.0
        assert "gdrive" in stats.files_by_remote


# ---------------------------------------------------------------------------
# get_manifest_for_file
# ---------------------------------------------------------------------------


class TestGetManifestForFile:
    def test_no_entries(self, mock_catalog):
        entries = get_manifest_for_file(mock_catalog, 999)
        assert entries == []

    def test_with_entries(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (42, "/tmp/photo.jpg", "abc", 1024, "gdrive", "GML-Backup/2023", "2023-06-15"),
        )
        mock_catalog.conn.commit()

        entries = get_manifest_for_file(mock_catalog, 42)
        assert len(entries) == 1
        assert entries[0]["remote"] == "gdrive"
        assert entries[0]["verified"] is False


# ---------------------------------------------------------------------------
# remove_backup_entry
# ---------------------------------------------------------------------------


class TestRemoveBackupEntry:
    def test_remove_entry(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (42, "/tmp/photo.jpg", "abc", 1024, "gdrive", "GML-Backup/2023", "2023-06-15"),
        )
        mock_catalog.conn.commit()

        result = remove_backup_entry(mock_catalog, 42, "gdrive")
        assert result is True

        entries = get_manifest_for_file(mock_catalog, 42)
        assert entries == []


# ---------------------------------------------------------------------------
# verify_backups
# ---------------------------------------------------------------------------


class TestVerifyBackups:
    def test_verify_empty(self, mock_catalog):
        result = verify_backups(mock_catalog)
        assert result["verified"] == 0
        assert result["total_checked"] == 0

    def test_verify_success(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, "/tmp/photo.jpg", "abc", 1024, "gdrive", "GML-Backup/2023", "2023-06-15"),
        )
        mock_catalog.conn.commit()

        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "  1024 photo.jpg"

        with (
            patch("godmode_media_library.distributed_backup._rclone_bin", return_value="rclone"),
            patch("subprocess.run", return_value=mock_proc),
        ):
            result = verify_backups(mock_catalog)

        assert result["verified"] == 1
        assert result["missing"] == 0

    def test_verify_missing_file(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, "/tmp/photo.jpg", "abc", 1024, "gdrive", "GML-Backup/2023", "2023-06-15"),
        )
        mock_catalog.conn.commit()

        mock_proc = MagicMock()
        mock_proc.returncode = 3
        mock_proc.stdout = ""

        with (
            patch("godmode_media_library.distributed_backup._rclone_bin", return_value="rclone"),
            patch("subprocess.run", return_value=mock_proc),
        ):
            result = verify_backups(mock_catalog)

        assert result["missing"] == 1
        assert result["verified"] == 0

    def test_verify_with_remote_filter(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, "/tmp/a.jpg", "abc", 1024, "gdrive", "GML-Backup", "2023-01-01"),
        )
        mock_catalog.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (2, "/tmp/b.jpg", "def", 2048, "s3", "GML-Backup", "2023-01-01"),
        )
        mock_catalog.conn.commit()

        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "  1024 a.jpg"

        with (
            patch("godmode_media_library.distributed_backup._rclone_bin", return_value="rclone"),
            patch("subprocess.run", return_value=mock_proc),
        ):
            result = verify_backups(mock_catalog, remote_name="gdrive")

        assert result["total_checked"] == 1

    def test_verify_exception(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, "/tmp/photo.jpg", "abc", 1024, "gdrive", "GML-Backup", "2023-01-01"),
        )
        mock_catalog.conn.commit()

        with (
            patch("godmode_media_library.distributed_backup._rclone_bin", return_value="rclone"),
            patch("subprocess.run", side_effect=OSError("rclone not found")),
        ):
            result = verify_backups(mock_catalog)

        assert result["errors"] == 1

    def test_verify_progress_fn(self, mock_catalog):
        ensure_backup_tables(mock_catalog)
        mock_catalog.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, "/tmp/photo.jpg", "abc", 1024, "gdrive", "GML-Backup", "2023-01-01"),
        )
        mock_catalog.conn.commit()

        progress_calls = []
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "  1024 photo.jpg"

        with (
            patch("godmode_media_library.distributed_backup._rclone_bin", return_value="rclone"),
            patch("subprocess.run", return_value=mock_proc),
        ):
            verify_backups(
                mock_catalog,
                progress_fn=lambda p: progress_calls.append(p),
            )

        assert len(progress_calls) == 1
        assert progress_calls[0]["phase"] == "verifying"
