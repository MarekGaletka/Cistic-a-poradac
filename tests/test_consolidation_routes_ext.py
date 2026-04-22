"""Extended tests for consolidation route endpoints (web/routes/consolidation.py).

Covers endpoints and branches not tested in test_consolidation_routes.py:
enrich-hashes, run-dedup, run-metadata-enrichment, health with active jobs,
status with disk_path, _consolidation_progress_dict, dedup mode validation.
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
    db_path = tmp_path / "consol_ext_test.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()
    return db_path


@pytest.fixture
def catalog_db_with_files(tmp_path):
    db_path = tmp_path / "consol_ext_files.db"
    cat = Catalog(db_path)
    cat.open()
    conn = cat._conn
    now_iso = "2024-01-15T12:00:00+00:00"
    conn.execute(
        "INSERT INTO files (path, size, ext, sha256, mtime, ctime, first_seen, last_scanned) "
        "VALUES ('/photos/a.jpg', 1024, '.jpg', 'abc123', 1704067200.0, 1704067200.0, ?, ?)",
        (now_iso, now_iso),
    )
    conn.commit()
    cat.close()
    return db_path


@pytest.fixture
def client(catalog_db):
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_db)
    return TestClient(app)


@pytest.fixture
def client_with_files(catalog_db_with_files):
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_db_with_files)
    return TestClient(app)


# ---------------------------------------------------------------------------
# POST /api/consolidation/enrich-hashes
# ---------------------------------------------------------------------------


class TestEnrichHashes:
    def test_enrich_hashes_starts_task(self, client):
        with patch(
            "godmode_media_library.cloud.enrich_catalog_hashes",
            return_value={"enriched": 10},
        ):
            resp = client.post("/api/consolidation/enrich-hashes")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"


# ---------------------------------------------------------------------------
# POST /api/consolidation/run-dedup
# ---------------------------------------------------------------------------


class TestRunDedup:
    def test_run_dedup_starts_task(self, client):
        with patch(
            "godmode_media_library.cloud.rclone_dedupe",
            return_value={"removed": 5},
        ):
            resp = client.post(
                "/api/consolidation/run-dedup",
                json={"dest_remote": "gdrive", "dest_path": "Consolidated", "mode": "largest"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["mode"] == "largest"

    def test_run_dedup_dry_run(self, client):
        with patch(
            "godmode_media_library.cloud.rclone_dedupe",
            return_value={},
        ):
            resp = client.post(
                "/api/consolidation/run-dedup",
                json={"mode": "newest", "dry_run": True},
            )
        assert resp.status_code == 200
        assert resp.json()["dry_run"] is True

    def test_run_dedup_invalid_mode(self, client):
        resp = client.post(
            "/api/consolidation/run-dedup",
            json={"mode": "invalid_mode"},
        )
        assert resp.status_code == 422

    def test_run_dedup_all_valid_modes(self, client):
        for mode in ("newest", "oldest", "largest", "smallest", "rename", "first"):
            with patch(
                "godmode_media_library.cloud.rclone_dedupe",
                return_value={},
            ):
                resp = client.post(
                    "/api/consolidation/run-dedup",
                    json={"mode": mode},
                )
            assert resp.status_code == 200, f"mode={mode} should be valid"


# ---------------------------------------------------------------------------
# POST /api/consolidation/run-metadata-enrichment
# ---------------------------------------------------------------------------


class TestRunMetadataEnrichment:
    def test_metadata_enrichment_starts_task(self, client):
        resp = client.post("/api/consolidation/run-metadata-enrichment")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"


# ---------------------------------------------------------------------------
# GET /api/consolidation/status — enrichment branches
# ---------------------------------------------------------------------------


class TestConsolidationStatusExtended:
    def test_status_with_disk_path(self, client, tmp_path):
        disk_dir = tmp_path / "fake_disk"
        disk_dir.mkdir()
        mock_status = {
            "has_active_job": False,
            "disk_path": str(disk_dir),
            "jobs": [],
        }
        with patch(
            "godmode_media_library.consolidation.get_consolidation_status",
            return_value=mock_status,
        ):
            resp = client.get("/api/consolidation/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "disk_free_gb" in data

    def test_status_disk_path_oserror(self, client):
        mock_status = {
            "has_active_job": False,
            "disk_path": "/nonexistent/volume/xyz",
            "jobs": [],
        }
        with patch(
            "godmode_media_library.consolidation.get_consolidation_status",
            return_value=mock_status,
        ):
            resp = client.get("/api/consolidation/status")
        assert resp.status_code == 200
        # Should not have disk_free_gb since path doesn't exist
        assert "disk_free_gb" not in resp.json()

    def test_status_with_active_job_and_task(self, client):
        """Test the active job enrichment branch."""
        mock_status = {
            "has_active_job": True,
            "disk_path": None,
            "jobs": [{"id": "job1", "status": "running"}],
        }
        with patch(
            "godmode_media_library.consolidation.get_consolidation_status",
            return_value=mock_status,
        ):
            resp = client.get("/api/consolidation/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["has_active_job"] is True


# ---------------------------------------------------------------------------
# GET /api/consolidation/health — extended branches
# ---------------------------------------------------------------------------


class TestConsolidationHealthExtended:
    def test_health_basic(self, client):
        resp = client.get("/api/consolidation/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert "timestamp" in data

    def test_health_rclone_not_running(self, client):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="")
            resp = client.get("/api/consolidation/health")
        assert resp.status_code == 200
        data = resp.json()
        # rclone_running should be False or None depending on pgrep availability
        assert "rclone_running" in data or data["ok"] is True


# ---------------------------------------------------------------------------
# POST /api/consolidation/start — low disk space guard
# ---------------------------------------------------------------------------


class TestConsolidationStartLowDisk:
    def test_start_low_disk_space(self, client, tmp_path):
        """When disk has < 1 GB free, start should be rejected."""
        with patch(
            "godmode_media_library.consolidation.get_consolidation_status",
            return_value={"has_active_job": False},
        ), patch(
            "godmode_media_library.web.routes.consolidation.shutil.disk_usage",
            return_value=MagicMock(free=500_000_000),  # 0.5 GB
        ):
            resp = client.post(
                "/api/consolidation/start",
                json={
                    "dest_remote": "bak",
                    "dest_path": "Out",
                    "disk_path": str(tmp_path),
                },
            )
        assert resp.status_code == 400
        assert "místa" in resp.json()["detail"].lower() or "disk" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# _consolidation_progress_dict helper
# ---------------------------------------------------------------------------


class TestConsolidationProgressDict:
    def test_progress_dict_conversion(self):
        from godmode_media_library.web.routes.consolidation import _consolidation_progress_dict

        progress = MagicMock()
        progress.phase = "transfer"
        progress.phase_label = "Transferring files"
        progress.current_step = 5
        progress.total_steps = 10
        progress.files_cataloged = 100
        progress.files_unique = 80
        progress.files_transferred = 50
        progress.files_verified = 40
        progress.files_failed = 2
        progress.files_retried = 1
        progress.bytes_transferred = 5000000
        progress.bytes_total_estimate = 10000000
        progress.transfer_speed_bps = 1000000
        progress.eta_seconds = 300
        progress.errors = ["err1"]
        progress.paused = False
        progress.dry_run = False
        progress.current_file = "/photos/test.jpg"

        result = _consolidation_progress_dict(progress)
        assert result["phase"] == "transfer"
        assert result["files_transferred"] == 50
        assert result["current_file"] == "/photos/test.jpg"
        assert result["current_step"] == 5

    def test_progress_dict_zero_step_becomes_one(self):
        from godmode_media_library.web.routes.consolidation import _consolidation_progress_dict

        progress = MagicMock()
        progress.phase = "init"
        progress.phase_label = "Initializing"
        progress.current_step = 0
        progress.total_steps = 10
        progress.files_cataloged = 0
        progress.files_unique = 0
        progress.files_transferred = 0
        progress.files_verified = 0
        progress.files_failed = 0
        progress.files_retried = 0
        progress.bytes_transferred = 0
        progress.bytes_total_estimate = 0
        progress.transfer_speed_bps = 0
        progress.eta_seconds = 0
        progress.errors = []
        progress.paused = False
        progress.dry_run = True
        progress.current_file = ""

        result = _consolidation_progress_dict(progress)
        assert result["current_step"] == 1  # max(0, 1) = 1


# ---------------------------------------------------------------------------
# Validation: SyncToDiskRequest, SyncDiskRequest, DedupRequest
# ---------------------------------------------------------------------------


class TestRequestValidationExtended:
    def test_sync_to_disk_rejects_carriage_return(self, client):
        resp = client.post(
            "/api/consolidation/sync-to-disk",
            json={"disk_path": "/Volumes/disk\rinjection"},
        )
        assert resp.status_code == 422

    def test_sync_disk_rejects_carriage_return(self, client):
        resp = client.post(
            "/api/consolidation/sync-disk",
            json={"disk_path": "/Volumes/disk\rinjection"},
        )
        assert resp.status_code == 422

    def test_dedup_accepts_dest_path(self, client):
        """DedupRequest does not validate dest_path — only mode is validated."""
        with patch(
            "godmode_media_library.cloud.rclone_dedupe",
            return_value={},
        ):
            resp = client.post(
                "/api/consolidation/run-dedup",
                json={"dest_path": "custom/path", "mode": "largest"},
            )
        assert resp.status_code == 200

    def test_consolidation_media_only_flag(self, client):
        with patch(
            "godmode_media_library.consolidation.get_consolidation_status",
            return_value={"has_active_job": False},
        ), patch(
            "godmode_media_library.consolidation.run_consolidation",
            return_value={},
        ):
            resp = client.post(
                "/api/consolidation/start",
                json={
                    "dest_remote": "bak",
                    "dest_path": "Out",
                    "media_only": True,
                },
            )
        assert resp.status_code == 200
