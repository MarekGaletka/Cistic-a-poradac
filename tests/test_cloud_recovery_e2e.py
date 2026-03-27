"""Comprehensive E2E tests for Cloud and Recovery features.

Covers cloud sources, browse, sync, mount, connect, and all recovery
endpoints (quarantine, integrity, deep-scan, repair, photorec, signal).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

try:
    from fastapi.testclient import TestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

pytestmark = pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi not installed")


# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture
def client(tmp_path):
    """Minimal test client — no pre-populated catalog needed for most cloud/recovery tests."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=tmp_path / "test.db")
    # Expose quarantine root so recovery endpoints can find it
    app.state.quarantine_root = str(tmp_path / "quarantine")
    return TestClient(app)


@pytest.fixture
def quarantine_dir(tmp_path):
    """Create a quarantine directory with sample files and a manifest."""
    qroot = tmp_path / "quarantine"
    qroot.mkdir()

    # Create a few quarantined files
    (qroot / "photo1.jpg").write_bytes(b"\xff\xd8\xff\xe0fake-jpeg-data")
    (qroot / "video1.mp4").write_bytes(b"\x00\x00\x00\x1cftypisom" + b"\x00" * 50)
    (qroot / "song.mp3").write_bytes(b"ID3" + b"\x00" * 30)

    manifest = {
        str(qroot / "photo1.jpg"): {
            "original_path": str(tmp_path / "originals" / "photo1.jpg"),
            "quarantine_date": "2026-03-20T10:00:00",
        },
        str(qroot / "video1.mp4"): {
            "original_path": str(tmp_path / "originals" / "video1.mp4"),
            "quarantine_date": "2026-03-21T11:00:00",
        },
        str(qroot / "song.mp3"): {
            "original_path": str(tmp_path / "originals" / "song.mp3"),
            "quarantine_date": "2026-03-22T12:00:00",
        },
    }
    (qroot / "manifest.json").write_text(json.dumps(manifest))
    return qroot


@pytest.fixture
def client_with_quarantine(tmp_path, quarantine_dir):
    """Test client whose app.state.quarantine_root points at the prepared quarantine."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=tmp_path / "test.db")
    app.state.quarantine_root = str(quarantine_dir)
    return TestClient(app)


# ════════════════════════════════════════════════════════════════════════
# 1. Cloud Sources
# ════════════════════════════════════════════════════════════════════════


class TestCloudSources:
    """GET /api/cloud/remotes, /api/cloud/status, /api/cloud/native."""

    def test_list_remotes_rclone_not_installed(self, client):
        # Clear endpoint cache so mock takes effect
        from godmode_media_library.web import api as _api
        _api._remotes_cache.update({"data": None, "ts": 0.0})

        with patch("godmode_media_library.cloud.check_rclone", return_value=False):
            resp = client.get("/api/cloud/remotes")
        assert resp.status_code == 200
        data = resp.json()
        assert data["installed"] is False
        assert data["remotes"] == []
        assert data["version"] is None

    def test_list_remotes_rclone_installed(self, client):
        from godmode_media_library.cloud import RcloneRemote

        mock_remotes = [
            RcloneRemote(name="gdrive", type="drive"),
            RcloneRemote(name="mega", type="mega"),
        ]
        with (
            patch("godmode_media_library.cloud.check_rclone", return_value=True),
            patch("godmode_media_library.cloud.rclone_version", return_value="1.68.0"),
            patch("godmode_media_library.cloud.list_remotes", return_value=mock_remotes),
        ):
            resp = client.get("/api/cloud/remotes")
        assert resp.status_code == 200
        data = resp.json()
        assert data["installed"] is True
        assert data["version"] == "1.68.0"
        assert len(data["remotes"]) == 2
        names = [r["name"] for r in data["remotes"]]
        assert "gdrive" in names
        assert "mega" in names

    def test_cloud_status_returns_sources_and_providers(self, client):
        with (
            patch("godmode_media_library.cloud.check_rclone", return_value=False),
            patch("godmode_media_library.cloud.detect_native_cloud_paths", return_value=[]),
        ):
            resp = client.get("/api/cloud/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "sources" in data
        assert "providers" in data

    def test_cloud_native_no_paths(self, client):
        with patch("godmode_media_library.cloud.detect_native_cloud_paths", return_value=[]):
            resp = client.get("/api/cloud/native")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["paths"] == []

    def test_cloud_native_with_paths(self, client, tmp_path):
        native_dir = tmp_path / "iCloud"
        native_dir.mkdir()
        (native_dir / "photo.jpg").write_bytes(b"data")
        mock_paths = [
            {"name": "iCloud", "path": str(native_dir), "type": "native_sync", "icon": "icon"}
        ]
        with patch("godmode_media_library.cloud.detect_native_cloud_paths", return_value=mock_paths):
            resp = client.get("/api/cloud/native")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["paths"][0]["name"] == "iCloud"


# ════════════════════════════════════════════════════════════════════════
# 2. Cloud Providers
# ════════════════════════════════════════════════════════════════════════


class TestCloudProviders:
    """GET /api/cloud/providers, /api/cloud/providers/{key}."""

    def test_list_providers(self, client):
        resp = client.get("/api/cloud/providers")
        assert resp.status_code == 200
        data = resp.json()
        assert "providers" in data
        # At minimum these three should exist
        for key in ("mega", "pcloud", "drive"):
            assert key in data["providers"]

    def test_provider_guide_mega(self, client):
        resp = client.get("/api/cloud/providers/mega")
        assert resp.status_code == 200
        data = resp.json()
        assert data["provider"] == "MEGA"
        assert "steps" in data
        assert len(data["steps"]) > 0

    def test_provider_guide_not_found(self, client):
        resp = client.get("/api/cloud/providers/nonexistent_provider")
        assert resp.status_code == 404

    def test_provider_fields_known(self, client):
        resp = client.get("/api/cloud/provider-fields/mega")
        assert resp.status_code == 200
        data = resp.json()
        assert "provider" in data
        assert "fields" in data

    def test_provider_fields_unknown(self, client):
        resp = client.get("/api/cloud/provider-fields/bogus_provider")
        assert resp.status_code == 404


# ════════════════════════════════════════════════════════════════════════
# 3. Cloud Browse
# ════════════════════════════════════════════════════════════════════════


class TestCloudBrowse:
    """GET /api/cloud/remote/{name}/browse."""

    def test_browse_root(self, client):
        mock_items = [
            {"Path": "folder1", "Name": "folder1", "IsDir": True, "Size": 0},
            {"Path": "pic.jpg", "Name": "pic.jpg", "IsDir": False, "Size": 1024},
        ]
        with patch("godmode_media_library.cloud.rclone_ls", return_value=mock_items):
            resp = client.get("/api/cloud/remote/mega/browse")
        assert resp.status_code == 200
        data = resp.json()
        assert data["remote"] == "mega"
        assert data["path"] == ""
        assert len(data["items"]) == 2

    def test_browse_subdirectory(self, client):
        mock_items = [{"Path": "sub/pic.jpg", "Name": "pic.jpg", "IsDir": False, "Size": 512}]
        with patch("godmode_media_library.cloud.rclone_ls", return_value=mock_items):
            resp = client.get("/api/cloud/remote/mega/browse", params={"path": "sub"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["path"] == "sub"
        assert len(data["items"]) == 1

    def test_browse_nonexistent_remote_rclone_error(self, client):
        with patch(
            "godmode_media_library.cloud.rclone_ls",
            side_effect=RuntimeError("Remote not found"),
        ):
            resp = client.get("/api/cloud/remote/nonexistent/browse")
        assert resp.status_code == 500

    def test_remote_about(self, client):
        mock_about = {"total": 50_000_000_000, "used": 12_000_000_000, "free": 38_000_000_000}
        with patch("godmode_media_library.cloud.rclone_about", return_value=mock_about):
            resp = client.get("/api/cloud/remote/mega/about")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 50_000_000_000
        assert data["used"] == 12_000_000_000


# ════════════════════════════════════════════════════════════════════════
# 4. Cloud Sync
# ════════════════════════════════════════════════════════════════════════


class TestCloudSync:
    """POST /api/cloud/sync."""

    def test_start_sync_returns_task_id(self, client):
        from godmode_media_library.cloud import SyncResult

        mock_result = SyncResult(
            remote="mega",
            remote_path="",
            local_path="/tmp/sync",
            files_transferred=10,
            errors=0,
            elapsed_seconds=5.0,
        )
        with (
            patch("godmode_media_library.cloud.rclone_copy", return_value=mock_result),
            patch("godmode_media_library.cloud.default_sync_dir", return_value=Path("/tmp/sync")),
        ):
            resp = client.post("/api/cloud/sync", json={"remote": "mega"})
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_sync_with_dry_run(self, client):
        from godmode_media_library.cloud import SyncResult

        mock_result = SyncResult(
            remote="mega", remote_path="", local_path="/tmp/sync",
            files_transferred=0, errors=0, elapsed_seconds=0.5,
        )
        with (
            patch("godmode_media_library.cloud.rclone_copy", return_value=mock_result),
            patch("godmode_media_library.cloud.default_sync_dir", return_value=Path("/tmp/sync")),
        ):
            resp = client.post(
                "/api/cloud/sync",
                json={"remote": "mega", "dry_run": True},
            )
        assert resp.status_code == 200
        assert resp.json()["status"] == "started"

    def test_sync_missing_remote_field(self, client):
        resp = client.post("/api/cloud/sync", json={})
        assert resp.status_code == 422  # Validation error


# ════════════════════════════════════════════════════════════════════════
# 5. Cloud Mount / Unmount
# ════════════════════════════════════════════════════════════════════════


class TestCloudMount:
    """POST /api/cloud/mount, /api/cloud/unmount."""

    def test_mount_success(self, client, tmp_path):
        mount_path = str(tmp_path / "mnt" / "mega")
        with patch("godmode_media_library.cloud.rclone_mount", return_value=(mount_path, True)):
            resp = client.post("/api/cloud/mount", json={"remote": "mega"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["mount_path"] == mount_path

    def test_mount_failure(self, client):
        with patch(
            "godmode_media_library.cloud.rclone_mount",
            side_effect=RuntimeError("mount failed"),
        ):
            resp = client.post("/api/cloud/mount", json={"remote": "mega"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is False

    def test_unmount_success(self, client):
        with patch("godmode_media_library.cloud.rclone_unmount", return_value=True):
            resp = client.post(
                "/api/cloud/unmount",
                json={"remote": "mega", "mount_point": "/tmp/mnt/mega"},
            )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_unmount_nonexistent(self, client):
        with patch("godmode_media_library.cloud.rclone_unmount", return_value=False):
            resp = client.post(
                "/api/cloud/unmount",
                json={"remote": "mega", "mount_point": "/nonexistent/path"},
            )
        assert resp.status_code == 200
        assert resp.json()["success"] is False


# ════════════════════════════════════════════════════════════════════════
# 6. Cloud Connect / Delete / Test
# ════════════════════════════════════════════════════════════════════════


class TestCloudConnect:
    """POST /api/cloud/connect, DELETE /api/cloud/remote/{name}, POST /api/cloud/test/{name}."""

    def test_connect_new_remote_success(self, client):
        with patch(
            "godmode_media_library.cloud.create_remote",
            return_value={"success": True, "message": "Remote created", "oauth": False},
        ):
            resp = client.post(
                "/api/cloud/connect",
                json={
                    "provider_key": "mega",
                    "name": "my_mega",
                    "credentials": {"user": "a@b.com", "pass": "secret"},
                },
            )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_connect_remote_failure(self, client):
        with patch(
            "godmode_media_library.cloud.create_remote",
            return_value={"success": False, "message": "Invalid credentials"},
        ):
            resp = client.post(
                "/api/cloud/connect",
                json={"provider_key": "mega", "name": "bad_mega", "credentials": {}},
            )
        assert resp.status_code == 400

    def test_delete_remote_success(self, client):
        with patch(
            "godmode_media_library.cloud.delete_remote",
            return_value={"success": True, "message": "Deleted"},
        ):
            resp = client.delete("/api/cloud/remote/my_mega")
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_delete_remote_failure(self, client):
        with patch(
            "godmode_media_library.cloud.delete_remote",
            return_value={"success": False, "message": "Not found"},
        ):
            resp = client.delete("/api/cloud/remote/nonexistent")
        assert resp.status_code == 400

    def test_test_remote_ok(self, client):
        with patch(
            "godmode_media_library.cloud.test_remote",
            return_value={"success": True, "message": "Connection OK"},
        ):
            resp = client.post("/api/cloud/test/mega")
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_test_remote_unreachable(self, client):
        with patch(
            "godmode_media_library.cloud.test_remote",
            return_value={"success": False, "message": "Timeout"},
        ):
            resp = client.post("/api/cloud/test/mega")
        assert resp.status_code == 200
        assert resp.json()["success"] is False


# ════════════════════════════════════════════════════════════════════════
# 7. Cloud OAuth
# ════════════════════════════════════════════════════════════════════════


class TestCloudOAuth:
    """GET /api/cloud/oauth/status/{name}, POST /api/cloud/oauth/finalize."""

    def test_oauth_status_pending(self, client):
        with patch(
            "godmode_media_library.cloud.get_oauth_status",
            return_value={"status": "pending"},
        ):
            resp = client.get("/api/cloud/oauth/status/gdrive")
        assert resp.status_code == 200
        assert resp.json()["status"] == "pending"

    def test_oauth_status_not_found(self, client):
        with patch(
            "godmode_media_library.cloud.get_oauth_status",
            return_value={"status": "not_found"},
        ):
            resp = client.get("/api/cloud/oauth/status/missing")
        assert resp.status_code == 200
        assert resp.json()["status"] == "not_found"

    def test_oauth_finalize_success(self, client):
        with patch(
            "godmode_media_library.cloud.finalize_oauth",
            return_value={"success": True, "message": "Remote configured"},
        ):
            resp = client.post(
                "/api/cloud/oauth/finalize",
                json={"provider_key": "drive", "name": "gdrive", "credentials": {"token": "abc123"}},
            )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_oauth_finalize_missing_token(self, client):
        resp = client.post(
            "/api/cloud/oauth/finalize",
            json={"provider_key": "drive", "name": "gdrive", "credentials": {}},
        )
        assert resp.status_code == 400

    def test_oauth_finalize_failure(self, client):
        with patch(
            "godmode_media_library.cloud.finalize_oauth",
            return_value={"success": False, "message": "Token expired"},
        ):
            resp = client.post(
                "/api/cloud/oauth/finalize",
                json={"provider_key": "drive", "name": "gdrive", "credentials": {"token": "bad"}},
            )
        assert resp.status_code == 400


# ════════════════════════════════════════════════════════════════════════
# 8. Quarantine Management
# ════════════════════════════════════════════════════════════════════════


class TestQuarantine:
    """GET /api/recovery/quarantine, POST .../restore, POST .../delete."""

    def test_list_empty_quarantine(self, client):
        resp = client.get("/api/recovery/quarantine")
        assert resp.status_code == 200
        data = resp.json()
        assert data["entries"] == []
        assert data["total"] == 0
        assert data["total_size"] == 0

    def test_list_populated_quarantine(self, client_with_quarantine):
        resp = client_with_quarantine.get("/api/recovery/quarantine")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert data["total_size"] > 0
        exts = {e["ext"] for e in data["entries"]}
        assert ".jpg" in exts
        assert ".mp4" in exts
        assert ".mp3" in exts

    def test_quarantine_entries_have_required_fields(self, client_with_quarantine):
        resp = client_with_quarantine.get("/api/recovery/quarantine")
        for entry in resp.json()["entries"]:
            assert "path" in entry
            assert "original_path" in entry
            assert "size" in entry
            assert "ext" in entry
            assert "quarantine_date" in entry
            assert "category" in entry

    def test_restore_quarantine_files(self, client_with_quarantine, tmp_path):
        # Get the list first
        entries = client_with_quarantine.get("/api/recovery/quarantine").json()["entries"]
        paths_to_restore = [entries[0]["path"]]

        restore_dest = str(tmp_path / "restored")
        resp = client_with_quarantine.post(
            "/api/recovery/quarantine/restore",
            json={"paths": paths_to_restore, "restore_to": restore_dest},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "restored" in data

    def test_restore_empty_list(self, client_with_quarantine):
        resp = client_with_quarantine.post(
            "/api/recovery/quarantine/restore",
            json={"paths": []},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("restored", 0) == 0

    def test_restore_nonexistent_file(self, client_with_quarantine):
        resp = client_with_quarantine.post(
            "/api/recovery/quarantine/restore",
            json={"paths": ["/nonexistent/file.jpg"]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("restored", 0) == 0
        assert len(data.get("errors", [])) > 0

    def test_delete_quarantine_files(self, client_with_quarantine):
        entries = client_with_quarantine.get("/api/recovery/quarantine").json()["entries"]
        path_to_delete = entries[0]["path"]

        resp = client_with_quarantine.post(
            "/api/recovery/quarantine/delete",
            json={"paths": [path_to_delete]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("deleted", 0) == 1

        # Verify it is actually gone
        after = client_with_quarantine.get("/api/recovery/quarantine").json()
        assert after["total"] == 2

    def test_delete_nonexistent_quarantine_file(self, client_with_quarantine):
        resp = client_with_quarantine.post(
            "/api/recovery/quarantine/delete",
            json={"paths": ["/does/not/exist.jpg"]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("deleted", 0) == 0

    def test_delete_empty_list(self, client_with_quarantine):
        resp = client_with_quarantine.post(
            "/api/recovery/quarantine/delete",
            json={"paths": []},
        )
        assert resp.status_code == 200
        assert resp.json().get("deleted", 0) == 0


# ════════════════════════════════════════════════════════════════════════
# 9. Integrity Check
# ════════════════════════════════════════════════════════════════════════


class TestIntegrityCheck:
    """POST /api/recovery/integrity-check, /api/recovery/repair."""

    def test_start_integrity_check(self, client):
        from godmode_media_library.recovery import IntegrityResult

        mock_result = IntegrityResult(
            total_checked=100, healthy=95, corrupted=5, repaired=0, errors=[]
        )
        with patch("godmode_media_library.recovery.check_integrity", return_value=mock_result):
            resp = client.post("/api/recovery/integrity-check")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_integrity_check_task_tracking(self, client):
        """Verify the task can be polled after creation."""
        from godmode_media_library.recovery import IntegrityResult

        mock_result = IntegrityResult(
            total_checked=50, healthy=50, corrupted=0, repaired=0, errors=[]
        )
        with patch("godmode_media_library.recovery.check_integrity", return_value=mock_result):
            start_resp = client.post("/api/recovery/integrity-check")
        task_id = start_resp.json()["task_id"]

        task_resp = client.get(f"/api/tasks/{task_id}")
        assert task_resp.status_code == 200
        task_data = task_resp.json()
        assert task_data["id"] == task_id
        assert task_data["command"] == "integrity-check"

    def test_repair_existing_file(self, client, tmp_path):
        # Create a dummy file that repair_file can find
        test_file = tmp_path / "broken.jpg"
        test_file.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)

        with patch(
            "godmode_media_library.recovery.repair_file",
            return_value={"success": True, "message": "Repaired"},
        ):
            resp = client.post("/api/recovery/repair", json={"path": str(test_file)})
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_repair_nonexistent_file(self, client):
        with patch(
            "godmode_media_library.recovery.repair_file",
            return_value={"success": False, "error": "Soubor neexistuje"},
        ):
            resp = client.post("/api/recovery/repair", json={"path": "/no/such/file.jpg"})
        assert resp.status_code == 200
        assert resp.json()["success"] is False


# ════════════════════════════════════════════════════════════════════════
# 10. Deep Scan
# ════════════════════════════════════════════════════════════════════════


class TestDeepScan:
    """POST /api/recovery/deep-scan."""

    def test_start_deep_scan(self, client):
        from godmode_media_library.recovery import DeepScanResult

        mock_result = DeepScanResult(
            locations_scanned=5,
            files_found=42,
            total_size=1_000_000,
            files=[{"path": "/hidden/pic.jpg", "size": 1024}],
            locations=[{"name": "Trash", "path": "~/.Trash", "count": 10}],
        )
        with patch("godmode_media_library.recovery.deep_scan", return_value=mock_result):
            resp = client.post("/api/recovery/deep-scan")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_deep_scan_task_tracking(self, client):
        from godmode_media_library.recovery import DeepScanResult

        mock_result = DeepScanResult(
            locations_scanned=3, files_found=10, total_size=5000,
            files=[], locations=[],
        )
        with patch("godmode_media_library.recovery.deep_scan", return_value=mock_result):
            start = client.post("/api/recovery/deep-scan")
        task_id = start.json()["task_id"]

        task = client.get(f"/api/tasks/{task_id}")
        assert task.status_code == 200
        assert task.json()["command"] == "deep-scan"


# ════════════════════════════════════════════════════════════════════════
# 11. Recover Files
# ════════════════════════════════════════════════════════════════════════


class TestRecoverFiles:
    """POST /api/recovery/recover-files."""

    def test_recover_files_success(self, client, tmp_path):
        src = tmp_path / "found" / "pic.jpg"
        src.parent.mkdir(parents=True, exist_ok=True)
        src.write_bytes(b"image-data")
        dest = str(tmp_path / "recovered")

        with patch(
            "godmode_media_library.recovery.recover_files",
            return_value={"recovered": 1, "errors": [], "total_size": 10},
        ):
            resp = client.post(
                "/api/recovery/recover-files",
                json={"paths": [str(src)], "destination": dest},
            )
        assert resp.status_code == 200
        assert resp.json()["recovered"] == 1

    def test_recover_files_nonexistent_source(self, client, tmp_path):
        with patch(
            "godmode_media_library.recovery.recover_files",
            return_value={"recovered": 0, "errors": ["File not found: /nope.jpg"], "total_size": 0},
        ):
            resp = client.post(
                "/api/recovery/recover-files",
                json={"paths": ["/nope.jpg"], "destination": str(tmp_path / "dest")},
            )
        assert resp.status_code == 200
        assert resp.json()["recovered"] == 0
        assert len(resp.json()["errors"]) > 0


# ════════════════════════════════════════════════════════════════════════
# 12. PhotoRec
# ════════════════════════════════════════════════════════════════════════


class TestPhotoRec:
    """GET /api/recovery/photorec/status, GET /api/recovery/disks, POST /api/recovery/photorec/run."""

    def test_photorec_not_installed(self, client):
        with patch(
            "godmode_media_library.recovery.check_photorec",
            return_value={"available": False, "version": None, "install_hint": "brew install testdisk"},
        ):
            resp = client.get("/api/recovery/photorec/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is False

    def test_photorec_installed(self, client):
        with patch(
            "godmode_media_library.recovery.check_photorec",
            return_value={"available": True, "version": "7.2"},
        ):
            resp = client.get("/api/recovery/photorec/status")
        assert resp.status_code == 200
        assert resp.json()["available"] is True
        assert resp.json()["version"] == "7.2"

    def test_list_disks(self, client):
        mock_disks = [
            {"name": "disk0", "size": "500GB", "type": "internal"},
            {"name": "disk1", "size": "1TB", "type": "external"},
        ]
        with patch("godmode_media_library.recovery.list_disks", return_value=mock_disks):
            resp = client.get("/api/recovery/disks")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["disks"]) == 2

    def test_photorec_run_not_installed(self, client):
        with patch(
            "godmode_media_library.recovery.check_photorec",
            return_value={"available": False, "version": None},
        ):
            resp = client.post(
                "/api/recovery/photorec/run",
                json={"source": "/dev/disk1"},
            )
        assert resp.status_code == 400


# ════════════════════════════════════════════════════════════════════════
# 13. App Mine / Available Apps
# ════════════════════════════════════════════════════════════════════════


class TestAppRecovery:
    """GET /api/recovery/apps, POST /api/recovery/app-mine."""

    def test_get_available_apps(self, client):
        mock_apps = [
            {"id": "whatsapp", "name": "WhatsApp", "icon": "icon", "color": "#25D366",
             "category": "messaging", "available": False, "encrypted": True,
             "decryptable": False, "note": ""},
        ]
        with patch("godmode_media_library.recovery.get_available_apps", return_value=mock_apps):
            resp = client.get("/api/recovery/apps")
        assert resp.status_code == 200
        data = resp.json()
        assert "apps" in data
        assert len(data["apps"]) == 1
        assert data["apps"][0]["id"] == "whatsapp"

    def test_start_app_mine(self, client):
        """Starting app mine returns a task_id."""
        # We don't need to mock the actual mining since it runs in background
        resp = client.post("/api/recovery/app-mine", json={"app_ids": ["whatsapp"]})
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"


# ════════════════════════════════════════════════════════════════════════
# 14. Signal Decryption
# ════════════════════════════════════════════════════════════════════════


class TestSignalRecovery:
    """GET /api/recovery/signal/status, POST /api/recovery/signal/decrypt."""

    def test_signal_status(self, client):
        with patch(
            "godmode_media_library.recovery.check_signal_decrypt",
            return_value={"available": False, "reason": "Signal data not found"},
        ):
            resp = client.get("/api/recovery/signal/status")
        assert resp.status_code == 200
        assert resp.json()["available"] is False

    def test_signal_decrypt_start(self, client, tmp_path):
        resp = client.post(
            "/api/recovery/signal/decrypt",
            json={"destination": str(tmp_path / "signal_out")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"


# ════════════════════════════════════════════════════════════════════════
# 15. Cloud Backup
# ════════════════════════════════════════════════════════════════════════


class TestCloudBackup:
    """POST /api/cloud/backup."""

    def test_backup_with_explicit_sources(self, client, tmp_path):
        src_dir = tmp_path / "photos"
        src_dir.mkdir()
        (src_dir / "pic.jpg").write_bytes(b"img")

        from godmode_media_library.cloud import SyncResult

        mock_result = SyncResult(
            remote="mega", remote_path="GML-Backup/photos",
            local_path=str(src_dir), files_transferred=1, errors=0, elapsed_seconds=1.0,
        )
        with patch("godmode_media_library.cloud.rclone_upload", return_value=mock_result):
            resp = client.post(
                "/api/cloud/backup",
                json={"remote": "mega", "source_paths": [str(src_dir)]},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_backup_no_sources_no_scans(self, client):
        """When no sources given and no scans in catalog, should 400."""
        resp = client.post("/api/cloud/backup", json={"remote": "mega"})
        assert resp.status_code == 400


# ════════════════════════════════════════════════════════════════════════
# 16. Task Tracking Integration
# ════════════════════════════════════════════════════════════════════════


class TestTaskTracking:
    """GET /api/tasks/{id} — verify task lifecycle for background operations."""

    def test_task_not_found(self, client):
        resp = client.get("/api/tasks/nonexistent-id")
        assert resp.status_code == 404

    def test_task_has_expected_fields(self, client):
        """Start a background op and check the task has standard fields."""
        resp = client.post("/api/recovery/deep-scan")
        task_id = resp.json()["task_id"]

        task = client.get(f"/api/tasks/{task_id}").json()
        assert "id" in task
        assert "command" in task
        assert "status" in task
        assert "started_at" in task

    def test_multiple_tasks_independent(self, client):
        """Two background tasks get independent IDs."""
        r1 = client.post("/api/recovery/deep-scan")
        r2 = client.post("/api/recovery/integrity-check")
        assert r1.json()["task_id"] != r2.json()["task_id"]


# ════════════════════════════════════════════════════════════════════════
# 17. Integrity Score (combined endpoint)
# ════════════════════════════════════════════════════════════════════════


class TestIntegrityScore:
    """GET /api/integrity-score."""

    def test_integrity_score_empty_catalog(self, client):
        resp = client.get("/api/integrity-score")
        assert resp.status_code == 200
        data = resp.json()
        assert "score" in data
        assert "grade" in data
        assert data["score"] == 0
        assert data["grade"] == "N/A"
