"""Tests for consolidation route endpoints (web/routes/consolidation.py).

Covers: status, preview, start, pause, resume, failed, catalog-stats,
available-disks, sync-to-disk, sync-disk.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

try:
    from fastapi.testclient import TestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

from godmode_media_library.catalog import Catalog

pytestmark = pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi not installed")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_db(tmp_path):
    """Create a minimal catalog DB."""
    db_path = tmp_path / "test_routes.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()
    return db_path


@pytest.fixture
def catalog_db_with_files(tmp_path):
    """Create a catalog DB pre-populated with sample files for stats tests."""
    db_path = tmp_path / "test_routes_files.db"
    cat = Catalog(db_path)
    cat.open()
    conn = cat._conn
    now_iso = "2024-01-15T12:00:00+00:00"
    files = [
        ("/photos/2024/01/img001.jpg", 1024000, ".jpg", "abc123", 1704067200.0, "2024:01:01 12:00:00"),
        ("/photos/2024/01/img002.jpg", 2048000, ".jpg", "abc124", 1704067200.0, "2024:01:02 12:00:00"),
        ("/photos/2024/06/img003.png", 512000, ".png", "abc125", 1719792000.0, "2024:06:15 12:00:00"),
        ("/videos/2023/clip.mp4", 50000000, ".mp4", "vid001", 1672531200.0, "2023:01:01 00:00:00"),
        ("/docs/report.pdf", 300000, ".pdf", "doc001", 1704067200.0, None),
        ("/misc/archive.zip", 10000000, ".zip", "zip001", 1704067200.0, None),
        # Duplicate hash for dup group test
        ("/photos/2024/01/img001_copy.jpg", 1024000, ".jpg", "abc123", 1704067200.0, "2024:01:01 12:00:00"),
    ]
    for path, size, ext, sha, mtime, date_orig in files:
        conn.execute(
            "INSERT INTO files (path, size, ext, sha256, mtime, ctime, first_seen, last_scanned, date_original) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (path, size, ext, sha, mtime, mtime, now_iso, now_iso, date_orig),
        )
    conn.commit()
    cat.close()
    return db_path


@pytest.fixture
def client(catalog_db):
    """Test client with an empty catalog."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_db)
    return TestClient(app)


@pytest.fixture
def client_with_files(catalog_db_with_files):
    """Test client with a catalog containing sample files."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_db_with_files)
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET /api/consolidation/status
# ---------------------------------------------------------------------------


class TestConsolidationStatus:
    def test_status_returns_200(self, client):
        resp = client.get("/api/consolidation/status")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    def test_status_has_no_active_job_by_default(self, client):
        resp = client.get("/api/consolidation/status")
        data = resp.json()
        assert data.get("has_active_job") is not True


# ---------------------------------------------------------------------------
# POST /api/consolidation/preview
# ---------------------------------------------------------------------------


class TestConsolidationPreview:
    def test_preview_returns_task_id(self, client):
        with patch(
            "godmode_media_library.consolidation.preview_consolidation",
            return_value={"total_files": 10, "total_size": 5000},
        ):
            resp = client.post(
                "/api/consolidation/preview",
                json={"source_remotes": ["gdrive:"], "local_roots": [], "dest_remote": "backup", "dest_path": "Consolidated"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "previewing"

    def test_preview_with_defaults(self, client):
        with patch(
            "godmode_media_library.consolidation.preview_consolidation",
            return_value={},
        ):
            resp = client.post("/api/consolidation/preview", json={})
        assert resp.status_code == 200
        assert "task_id" in resp.json()

    def test_preview_rejects_path_traversal_in_dest_path(self, client):
        resp = client.post(
            "/api/consolidation/preview",
            json={"dest_path": "../etc/passwd"},
        )
        assert resp.status_code == 422

    def test_preview_rejects_null_bytes_in_dest_remote(self, client):
        resp = client.post(
            "/api/consolidation/preview",
            json={"dest_remote": "bad\x00remote"},
        )
        assert resp.status_code == 422

    def test_preview_rejects_newline_in_dest_path(self, client):
        resp = client.post(
            "/api/consolidation/preview",
            json={"dest_path": "path\ninjection"},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /api/consolidation/start
# ---------------------------------------------------------------------------


class TestConsolidationStart:
    def test_start_returns_task_id(self, client):
        with patch(
            "godmode_media_library.consolidation.get_consolidation_status",
            return_value={"has_active_job": False},
        ), patch(
            "godmode_media_library.consolidation.run_consolidation",
            return_value={"status": "done"},
        ):
            resp = client.post(
                "/api/consolidation/start",
                json={"source_remotes": ["gdrive:"], "dest_remote": "backup", "dest_path": "Out"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_start_dry_run_flag(self, client):
        with patch(
            "godmode_media_library.consolidation.get_consolidation_status",
            return_value={"has_active_job": False},
        ), patch(
            "godmode_media_library.consolidation.run_consolidation",
            return_value={},
        ):
            resp = client.post(
                "/api/consolidation/start",
                json={"dry_run": True, "dest_remote": "bak", "dest_path": "DryOut"},
            )
        assert resp.status_code == 200
        assert resp.json()["dry_run"] is True

    def test_start_rejects_path_traversal(self, client):
        resp = client.post(
            "/api/consolidation/start",
            json={"dest_path": "../../etc/shadow"},
        )
        assert resp.status_code == 422

    def test_start_rejects_empty_dest_remote(self, client):
        resp = client.post(
            "/api/consolidation/start",
            json={"dest_remote": ""},
        )
        assert resp.status_code == 422

    def test_start_rejects_invalid_bwlimit(self, client):
        resp = client.post(
            "/api/consolidation/start",
            json={"bwlimit": "not_a_limit!!"},
        )
        assert resp.status_code == 422

    def test_start_accepts_valid_bwlimit(self, client):
        with patch(
            "godmode_media_library.consolidation.get_consolidation_status",
            return_value={"has_active_job": False},
        ), patch(
            "godmode_media_library.consolidation.run_consolidation",
            return_value={},
        ):
            resp = client.post(
                "/api/consolidation/start",
                json={"bwlimit": "10M", "dest_remote": "bak", "dest_path": "Out"},
            )
        assert resp.status_code == 200

    def test_start_rejects_verify_pct_out_of_range(self, client):
        resp = client.post(
            "/api/consolidation/start",
            json={"verify_pct": 200},
        )
        assert resp.status_code == 422

    def test_start_blocked_when_job_active(self, client):
        with patch(
            "godmode_media_library.consolidation.get_consolidation_status",
            return_value={"has_active_job": True},
        ):
            resp = client.post(
                "/api/consolidation/start",
                json={"dest_remote": "bak", "dest_path": "Out"},
            )
        assert resp.status_code == 409


# ---------------------------------------------------------------------------
# POST /api/consolidation/pause
# ---------------------------------------------------------------------------


class TestConsolidationPause:
    def test_pause_when_no_job(self, client):
        resp = client.post("/api/consolidation/pause")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    def test_pause_returns_paused_true_when_active(self, client):
        with patch(
            "godmode_media_library.consolidation.pause_consolidation",
            return_value={"paused": True, "job_id": "test123"},
        ):
            resp = client.post("/api/consolidation/pause")
        assert resp.status_code == 200
        assert resp.json()["paused"] is True


# ---------------------------------------------------------------------------
# POST /api/consolidation/resume
# ---------------------------------------------------------------------------


class TestConsolidationResume:
    def test_resume_returns_task_id(self, client):
        with patch(
            "godmode_media_library.consolidation.resume_consolidation",
            return_value={"status": "completed"},
        ):
            resp = client.post("/api/consolidation/resume")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "resuming"

    def test_resume_when_nothing_to_resume(self, client):
        with patch(
            "godmode_media_library.consolidation.resume_consolidation",
            side_effect=RuntimeError("No paused job found"),
        ):
            resp = client.post("/api/consolidation/resume")
        # The endpoint itself returns 200 (background task handles errors)
        assert resp.status_code == 200
        assert "task_id" in resp.json()


# ---------------------------------------------------------------------------
# GET /api/consolidation/failed
# ---------------------------------------------------------------------------


class TestConsolidationFailed:
    def test_failed_empty(self, client):
        resp = client.get("/api/consolidation/failed")
        assert resp.status_code == 200
        data = resp.json()
        assert "failed_files" in data
        assert isinstance(data["failed_files"], list)
        assert len(data["failed_files"]) == 0

    def test_failed_with_results(self, client):
        mock_report = [
            {"job_id": "j1", "file_hash": "aaa", "source": "/src/a.jpg", "error": "timeout"},
            {"job_id": "j1", "file_hash": "bbb", "source": "/src/b.jpg", "error": "disk full"},
        ]
        with patch(
            "godmode_media_library.consolidation.get_failed_files_report",
            return_value=mock_report,
        ):
            resp = client.get("/api/consolidation/failed")
        assert resp.status_code == 200
        assert len(resp.json()["failed_files"]) == 2


# ---------------------------------------------------------------------------
# GET /api/consolidation/catalog-stats
# ---------------------------------------------------------------------------


class TestConsolidationCatalogStats:
    def test_stats_empty_catalog(self, client):
        resp = client.get("/api/consolidation/catalog-stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_files"] == 0
        assert data["total_size"] == 0
        assert isinstance(data["categories"], list)
        assert isinstance(data["by_extension"], list)
        assert isinstance(data["by_year"], list)
        assert data["duplicate_groups"] == 0

    def test_stats_with_files(self, client_with_files):
        resp = client_with_files.get("/api/consolidation/catalog-stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_files"] == 7
        assert data["total_size"] > 0
        assert len(data["categories"]) > 0
        category_names = [c["category"] for c in data["categories"]]
        assert "Media" in category_names
        assert data["duplicate_groups"] >= 1
        assert len(data["by_extension"]) > 0
        ext_names = [e["extension"] for e in data["by_extension"]]
        assert "jpg" in ext_names
        assert len(data["by_year"]) > 0
        year_values = [y["year"] for y in data["by_year"]]
        assert "2024" in year_values

    def test_stats_categories_contain_documents(self, client_with_files):
        resp = client_with_files.get("/api/consolidation/catalog-stats")
        data = resp.json()
        category_names = [c["category"] for c in data["categories"]]
        assert "Documents" in category_names

    def test_stats_categories_contain_software(self, client_with_files):
        resp = client_with_files.get("/api/consolidation/catalog-stats")
        data = resp.json()
        category_names = [c["category"] for c in data["categories"]]
        assert "Software" in category_names


# ---------------------------------------------------------------------------
# GET /api/consolidation/available-disks
# ---------------------------------------------------------------------------


class TestConsolidationAvailableDisks:
    def test_available_disks_returns_list(self, client):
        resp = client.get("/api/consolidation/available-disks")
        assert resp.status_code == 200
        data = resp.json()
        assert "disks" in data
        assert isinstance(data["disks"], list)

    def test_available_disks_excludes_system_volumes(self, client):
        """System volumes like 'Macintosh HD' should be excluded."""
        mock_entries = []
        for name in ["Macintosh HD", "ExternalDrive", "Recovery", "USBStick"]:
            entry = MagicMock()
            entry.name = name
            entry.is_dir.return_value = True
            entry.__str__ = lambda s, n=name: f"/Volumes/{n}"
            entry.__lt__ = lambda s, o: s.name < o.name  # Support sorted()
            mock_entries.append(entry)

        mock_usage = MagicMock()
        mock_usage.total = 1000000000000
        mock_usage.free = 500000000000
        mock_usage.used = 500000000000

        with patch("godmode_media_library.web.routes.consolidation.Path") as MockPath:
            volumes_dir = MagicMock()
            volumes_dir.is_dir.return_value = True
            volumes_dir.iterdir.return_value = iter(mock_entries)
            MockPath.return_value = volumes_dir
            with patch("godmode_media_library.web.routes.consolidation.shutil.disk_usage", return_value=mock_usage):
                resp = client.get("/api/consolidation/available-disks")

        assert resp.status_code == 200
        data = resp.json()
        disk_names = [d["name"] for d in data["disks"]]
        assert "Macintosh HD" not in disk_names
        assert "Recovery" not in disk_names
        # ExternalDrive and USBStick should be present
        assert "ExternalDrive" in disk_names
        assert "USBStick" in disk_names

    def test_available_disks_no_volumes_dir(self, client):
        """When /Volumes doesn't exist, return empty list."""
        with patch("godmode_media_library.web.routes.consolidation.Path") as MockPath:
            volumes_dir = MagicMock()
            volumes_dir.is_dir.return_value = False
            MockPath.return_value = volumes_dir
            resp = client.get("/api/consolidation/available-disks")
        assert resp.status_code == 200
        assert resp.json()["disks"] == []


# ---------------------------------------------------------------------------
# POST /api/consolidation/sync-to-disk
# ---------------------------------------------------------------------------


class TestConsolidationSyncToDisk:
    def test_sync_to_disk_missing_disk_path(self, client):
        """disk_path is required and must be non-empty."""
        resp = client.post(
            "/api/consolidation/sync-to-disk",
            json={"disk_path": ""},
        )
        assert resp.status_code == 422

    def test_sync_to_disk_path_traversal(self, client):
        resp = client.post(
            "/api/consolidation/sync-to-disk",
            json={"disk_path": "/Volumes/../etc/passwd"},
        )
        assert resp.status_code == 422

    def test_sync_to_disk_null_bytes(self, client):
        resp = client.post(
            "/api/consolidation/sync-to-disk",
            json={"disk_path": "/Volumes/disk\x00inject"},
        )
        assert resp.status_code == 422

    def test_sync_to_disk_nonexistent_path(self, client, tmp_path):
        resp = client.post(
            "/api/consolidation/sync-to-disk",
            json={"disk_path": str(tmp_path / "nonexistent_disk")},
        )
        assert resp.status_code == 400

    def test_sync_to_disk_valid_path(self, client, tmp_path):
        disk_dir = tmp_path / "external_disk"
        disk_dir.mkdir()
        with patch(
            "godmode_media_library.web.routes.consolidation._run_rclone_sync"
        ):
            resp = client.post(
                "/api/consolidation/sync-to-disk",
                json={"disk_path": str(disk_dir)},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "syncing"
        assert data["dest"] == str(disk_dir)

    def test_sync_to_disk_file_not_dir(self, client, tmp_path):
        """disk_path must be a directory, not a file."""
        file_path = tmp_path / "not_a_dir.txt"
        file_path.write_text("hello")
        resp = client.post(
            "/api/consolidation/sync-to-disk",
            json={"disk_path": str(file_path)},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /api/consolidation/sync-disk
# ---------------------------------------------------------------------------


class TestConsolidationSyncDisk:
    def test_sync_disk_missing_disk_path(self, client):
        resp = client.post(
            "/api/consolidation/sync-disk",
            json={"disk_path": ""},
        )
        assert resp.status_code == 422

    def test_sync_disk_path_traversal(self, client):
        resp = client.post(
            "/api/consolidation/sync-disk",
            json={"disk_path": "/Volumes/../../secret"},
        )
        assert resp.status_code == 422

    def test_sync_disk_nonexistent_path(self, client, tmp_path):
        resp = client.post(
            "/api/consolidation/sync-disk",
            json={"disk_path": str(tmp_path / "no_such_disk")},
        )
        assert resp.status_code == 400

    def test_sync_disk_valid_path(self, client, tmp_path):
        disk_dir = tmp_path / "sync_target"
        disk_dir.mkdir()
        with patch(
            "godmode_media_library.web.routes.consolidation._run_rclone_sync"
        ):
            resp = client.post(
                "/api/consolidation/sync-disk",
                json={"disk_path": str(disk_dir), "delete_extra": False},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "syncing"
        assert data["delete_extra"] is False

    def test_sync_disk_default_delete_extra(self, client, tmp_path):
        disk_dir = tmp_path / "sync_default"
        disk_dir.mkdir()
        with patch(
            "godmode_media_library.web.routes.consolidation._run_rclone_sync"
        ):
            resp = client.post(
                "/api/consolidation/sync-disk",
                json={"disk_path": str(disk_dir)},
            )
        assert resp.status_code == 200
        assert resp.json()["delete_extra"] is True

    def test_sync_disk_null_bytes_in_dest_remote(self, client):
        resp = client.post(
            "/api/consolidation/sync-disk",
            json={"disk_path": "/Volumes/disk", "dest_remote": "bad\x00remote"},
        )
        assert resp.status_code == 422

    def test_sync_disk_file_not_dir(self, client, tmp_path):
        file_path = tmp_path / "file.txt"
        file_path.write_text("data")
        resp = client.post(
            "/api/consolidation/sync-disk",
            json={"disk_path": str(file_path)},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Pydantic model validation (edge cases)
# ---------------------------------------------------------------------------


class TestRequestModelValidation:
    def test_consolidation_start_bwlimit_valid_variants(self, client):
        """Various valid bwlimit formats should pass validation."""
        with patch(
            "godmode_media_library.consolidation.get_consolidation_status",
            return_value={"has_active_job": False},
        ), patch(
            "godmode_media_library.consolidation.run_consolidation",
            return_value={},
        ):
            for bwlimit in ["512K", "10M", "1G", "100", "0"]:
                resp = client.post(
                    "/api/consolidation/start",
                    json={"bwlimit": bwlimit, "dest_remote": "bak", "dest_path": "Out"},
                )
                assert resp.status_code == 200, f"bwlimit={bwlimit} should be valid"

    def test_consolidation_start_bwlimit_invalid_variants(self, client):
        for bwlimit in ["10 M", "abc", "10MB", "1.5G", "-10M"]:
            resp = client.post(
                "/api/consolidation/start",
                json={"bwlimit": bwlimit, "dest_remote": "bak", "dest_path": "Out"},
            )
            assert resp.status_code == 422, f"bwlimit={bwlimit} should be rejected"

    def test_consolidation_start_verify_pct_boundary(self, client):
        with patch(
            "godmode_media_library.consolidation.get_consolidation_status",
            return_value={"has_active_job": False},
        ), patch(
            "godmode_media_library.consolidation.run_consolidation",
            return_value={},
        ):
            for pct in [0, 100]:
                resp = client.post(
                    "/api/consolidation/start",
                    json={"verify_pct": pct, "dest_remote": "bak", "dest_path": "Out"},
                )
                assert resp.status_code == 200, f"verify_pct={pct} should be valid"

        for pct in [-1, 101]:
            resp = client.post(
                "/api/consolidation/start",
                json={"verify_pct": pct, "dest_remote": "bak", "dest_path": "Out"},
            )
            assert resp.status_code == 422, f"verify_pct={pct} should be rejected"

    def test_sync_to_disk_long_path_rejected(self, client):
        """Paths over 500 chars should be rejected by Pydantic max_length."""
        long_path = "/Volumes/" + "a" * 500
        resp = client.post(
            "/api/consolidation/sync-to-disk",
            json={"disk_path": long_path},
        )
        assert resp.status_code == 422
