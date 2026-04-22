"""Tests for iPhone route endpoints (web/routes/iphone.py).

Targets coverage from ~25% to 60%+.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

try:
    from fastapi.testclient import TestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

pytestmark = pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi not installed")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_db(tmp_path):
    """Create a minimal catalog DB."""
    from godmode_media_library.catalog import Catalog

    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()
    return db_path


@pytest.fixture
def client(catalog_db):
    """Test client with iPhone route mocks disabled (real router, mocked imports)."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_db)
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET /api/iphone/status
# ---------------------------------------------------------------------------


class TestIPhoneStatus:
    def test_status_returns_dict(self, client, catalog_db):
        mock_status = {
            "connected": False,
            "device_name": None,
            "last_import": None,
            "total_imported": 0,
        }
        with patch(
            "godmode_media_library.iphone_import.get_iphone_status",
            return_value=mock_status,
        ):
            resp = client.get("/api/iphone/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["connected"] is False

    def test_status_connected(self, client, catalog_db):
        mock_status = {
            "connected": True,
            "device_name": "Marek's iPhone",
            "last_import": "2026-04-20T10:00:00",
            "total_imported": 1234,
        }
        with patch(
            "godmode_media_library.iphone_import.get_iphone_status",
            return_value=mock_status,
        ):
            resp = client.get("/api/iphone/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["connected"] is True
        assert data["device_name"] == "Marek's iPhone"


# ---------------------------------------------------------------------------
# GET /api/iphone/progress
# ---------------------------------------------------------------------------


class TestIPhoneProgress:
    def test_progress_idle(self, client):
        mock_progress = {"phase": "idle", "transferred": 0, "total": 0, "pct": 0}
        with patch(
            "godmode_media_library.iphone_import.get_progress",
            return_value=mock_progress,
        ):
            resp = client.get("/api/iphone/progress")
        assert resp.status_code == 200
        data = resp.json()
        assert data["phase"] == "idle"

    def test_progress_transferring(self, client):
        mock_progress = {
            "phase": "transferring",
            "transferred": 50,
            "total": 100,
            "pct": 50,
        }
        with patch(
            "godmode_media_library.iphone_import.get_progress",
            return_value=mock_progress,
        ):
            resp = client.get("/api/iphone/progress")
        assert resp.status_code == 200
        data = resp.json()
        assert data["phase"] == "transferring"
        assert data["pct"] == 50


# ---------------------------------------------------------------------------
# POST /api/iphone/start
# ---------------------------------------------------------------------------


class TestIPhoneStart:
    def test_start_no_iphone(self, client):
        with patch(
            "godmode_media_library.iphone_import._check_iphone_connected",
            return_value=False,
        ):
            resp = client.post("/api/iphone/start", json={})
        assert resp.status_code == 404

    def test_start_already_running(self, client):
        with (
            patch(
                "godmode_media_library.iphone_import._check_iphone_connected",
                return_value=True,
            ),
            patch(
                "godmode_media_library.iphone_import.get_progress",
                return_value={"phase": "transferring"},
            ),
        ):
            resp = client.post("/api/iphone/start", json={})
        assert resp.status_code == 409

    def test_start_success(self, client):
        mock_run = MagicMock(return_value={"completed": 10, "skipped": 0})
        with (
            patch(
                "godmode_media_library.iphone_import._check_iphone_connected",
                return_value=True,
            ),
            patch(
                "godmode_media_library.iphone_import.get_progress",
                return_value={"phase": "idle"},
            ),
            patch(
                "godmode_media_library.iphone_import.IPhoneImportConfig",
            ),
            patch(
                "godmode_media_library.iphone_import.run_import",
                mock_run,
            ),
        ):
            resp = client.post(
                "/api/iphone/start",
                json={
                    "dest_remote": "gws-backup",
                    "dest_path": "GML-Consolidated",
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_start_with_custom_params(self, client):
        with (
            patch(
                "godmode_media_library.iphone_import._check_iphone_connected",
                return_value=True,
            ),
            patch(
                "godmode_media_library.iphone_import.get_progress",
                return_value={"phase": "idle"},
            ),
            patch("godmode_media_library.iphone_import.IPhoneImportConfig"),
            patch(
                "godmode_media_library.iphone_import.run_import",
                return_value={"completed": 0},
            ),
        ):
            resp = client.post(
                "/api/iphone/start",
                json={
                    "dest_remote": "my-remote",
                    "dest_path": "my-path",
                    "structure_pattern": "year",
                    "bwlimit": "10M",
                    "media_only": False,
                },
            )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# POST /api/iphone/pause
# ---------------------------------------------------------------------------


class TestIPhonePause:
    def test_pause_not_running(self, client):
        with patch(
            "godmode_media_library.iphone_import.get_progress",
            return_value={"phase": "idle"},
        ):
            resp = client.post("/api/iphone/pause")
        assert resp.status_code == 409

    def test_pause_success(self, client):
        with (
            patch(
                "godmode_media_library.iphone_import.get_progress",
                return_value={"phase": "transferring"},
            ),
            patch("godmode_media_library.iphone_import.pause_import") as mock_pause,
        ):
            resp = client.post("/api/iphone/pause")
        assert resp.status_code == 200
        assert resp.json()["status"] == "paused"
        mock_pause.assert_called_once()

    def test_pause_during_listing(self, client):
        with (
            patch(
                "godmode_media_library.iphone_import.get_progress",
                return_value={"phase": "listing"},
            ),
            patch("godmode_media_library.iphone_import.pause_import"),
        ):
            resp = client.post("/api/iphone/pause")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# POST /api/iphone/resume
# ---------------------------------------------------------------------------


class TestIPhoneResume:
    def test_resume_paused_in_memory(self, client):
        """When phase is paused and thread is alive, resume_import is called."""
        import godmode_media_library.web.routes.iphone as iphone_mod

        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True
        original = iphone_mod._import_thread
        iphone_mod._import_thread = mock_thread
        try:
            with (
                patch(
                    "godmode_media_library.iphone_import.get_progress",
                    return_value={"phase": "paused"},
                ),
                patch(
                    "godmode_media_library.iphone_import.resume_import"
                ) as mock_resume,
                patch("godmode_media_library.iphone_import.IPhoneImportConfig"),
                patch("godmode_media_library.iphone_import.run_import"),
            ):
                resp = client.post("/api/iphone/resume")
            assert resp.status_code == 200
            assert resp.json()["status"] == "resumed"
            mock_resume.assert_called_once()
        finally:
            iphone_mod._import_thread = original

    def test_resume_restart_pipeline(self, client):
        """When not paused or thread dead, restart from checkpoint."""
        import godmode_media_library.web.routes.iphone as iphone_mod

        original = iphone_mod._import_thread
        iphone_mod._import_thread = None
        try:
            with (
                patch(
                    "godmode_media_library.iphone_import.get_progress",
                    return_value={"phase": "completed"},
                ),
                patch("godmode_media_library.iphone_import.IPhoneImportConfig"),
                patch(
                    "godmode_media_library.iphone_import.run_import",
                    return_value={"completed": 5},
                ),
            ):
                resp = client.post("/api/iphone/resume")
            assert resp.status_code == 200
            data = resp.json()
            assert "task_id" in data
            assert data["status"] == "resumed"
        finally:
            iphone_mod._import_thread = original


# ---------------------------------------------------------------------------
# GET /api/iphone/list
# ---------------------------------------------------------------------------


class TestIPhoneList:
    def test_list_no_iphone(self, client):
        with patch(
            "godmode_media_library.iphone_import._check_iphone_connected",
            return_value=False,
        ):
            resp = client.get("/api/iphone/list")
        assert resp.status_code == 404

    def test_list_success(self, client):
        mock_file = SimpleNamespace(
            filename="IMG_0001.jpg",
            afc_path="/DCIM/100APPLE/IMG_0001.jpg",
            size=1024000,
        )
        mock_file2 = SimpleNamespace(
            filename="IMG_0002.mp4",
            afc_path="/DCIM/100APPLE/IMG_0002.mp4",
            size=5000000,
        )

        async def mock_list():
            return [mock_file, mock_file2]

        with (
            patch(
                "godmode_media_library.iphone_import._check_iphone_connected",
                return_value=True,
            ),
            patch(
                "godmode_media_library.iphone_import.list_iphone_files",
                side_effect=mock_list,
            ),
            patch(
                "godmode_media_library.iphone_import._is_media",
                return_value=True,
            ),
        ):
            resp = client.get("/api/iphone/list")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_files"] == 2
        assert data["media_files"] == 2

    def test_list_error(self, client):
        async def mock_list_error():
            raise RuntimeError("Connection lost")

        with (
            patch(
                "godmode_media_library.iphone_import._check_iphone_connected",
                return_value=True,
            ),
            patch(
                "godmode_media_library.iphone_import.list_iphone_files",
                side_effect=mock_list_error,
            ),
        ):
            resp = client.get("/api/iphone/list")
        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# POST /api/iphone/reorganize
# ---------------------------------------------------------------------------


class TestIPhoneReorganize:
    def test_reorganize_starts_task(self, client):
        with patch(
            "godmode_media_library.iphone_import.reorganize_unsorted",
            return_value={"moved": 10, "errors": 0},
        ):
            resp = client.post("/api/iphone/reorganize")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"


# ---------------------------------------------------------------------------
# POST /api/iphone/auto-import (toggle)
# ---------------------------------------------------------------------------


class TestAutoImport:
    def test_auto_import_enable(self, client):
        import godmode_media_library.web.routes.iphone as iphone_mod

        # Ensure disabled initially
        iphone_mod._auto_import_enabled = False
        iphone_mod._auto_import_thread = None

        # Mock _check_iphone_connected to prevent the daemon thread from doing real work
        with patch(
            "godmode_media_library.iphone_import._check_iphone_connected",
            return_value=False,
        ):
            resp = client.post("/api/iphone/auto-import")
        assert resp.status_code == 200
        data = resp.json()
        assert data["auto_import"] is True
        assert data["status"] == "enabled"

        # Disable to clean up
        iphone_mod._auto_import_enabled = False

    def test_auto_import_disable(self, client):
        import godmode_media_library.web.routes.iphone as iphone_mod

        iphone_mod._auto_import_enabled = True

        resp = client.post("/api/iphone/auto-import")
        assert resp.status_code == 200
        data = resp.json()
        assert data["auto_import"] is False
        assert data["status"] == "disabled"

    def test_auto_import_status(self, client):
        import godmode_media_library.web.routes.iphone as iphone_mod

        iphone_mod._auto_import_enabled = False

        resp = client.get("/api/iphone/auto-import")
        assert resp.status_code == 200
        data = resp.json()
        assert data["auto_import"] is False
        assert "check_interval" in data
        assert "cooldown" in data

    def test_auto_import_status_when_enabled(self, client):
        import godmode_media_library.web.routes.iphone as iphone_mod

        iphone_mod._auto_import_enabled = True
        try:
            resp = client.get("/api/iphone/auto-import")
            assert resp.status_code == 200
            data = resp.json()
            assert data["auto_import"] is True
        finally:
            iphone_mod._auto_import_enabled = False


# ---------------------------------------------------------------------------
# IPhoneStartRequest validation
# ---------------------------------------------------------------------------


class TestIPhoneStartRequestValidation:
    def test_empty_dest_remote_rejected(self, client):
        """dest_remote must be non-empty (min_length=1)."""
        with (
            patch(
                "godmode_media_library.iphone_import._check_iphone_connected",
                return_value=True,
            ),
            patch(
                "godmode_media_library.iphone_import.get_progress",
                return_value={"phase": "idle"},
            ),
        ):
            resp = client.post(
                "/api/iphone/start",
                json={"dest_remote": "", "dest_path": "path"},
            )
        assert resp.status_code == 422

    def test_empty_dest_path_rejected(self, client):
        with (
            patch(
                "godmode_media_library.iphone_import._check_iphone_connected",
                return_value=True,
            ),
            patch(
                "godmode_media_library.iphone_import.get_progress",
                return_value={"phase": "idle"},
            ),
        ):
            resp = client.post(
                "/api/iphone/start",
                json={"dest_remote": "remote", "dest_path": ""},
            )
        assert resp.status_code == 422
