"""Tests for the web API endpoints.

Covers: auth (Bearer, X-API-Token, query param), auth-failure rate limiting,
key read endpoints, path safety on delete, input validation, security headers.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

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
def catalog_with_files(tmp_path):
    """Create a catalog with some test files."""
    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()

    # Create test files on disk
    root = tmp_path / "media"
    root.mkdir()
    (root / "photo1.jpg").write_bytes(b"content1")
    (root / "photo2.jpg").write_bytes(b"content2")
    (root / "dup1.jpg").write_bytes(b"duplicate")
    (root / "dup2.jpg").write_bytes(b"duplicate")

    # Scan them into catalog
    from godmode_media_library.scanner import incremental_scan

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
    ):
        incremental_scan(cat, [root])
    cat.close()
    return db_path


@pytest.fixture
def empty_catalog(tmp_path):
    """Return path to an empty (freshly-created) catalog DB."""
    db_path = tmp_path / "empty.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()
    return db_path


@pytest.fixture
def client(catalog_with_files):
    """Create a test client with a populated catalog (no auth)."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_with_files)
    return TestClient(app)


@pytest.fixture
def client_empty(empty_catalog):
    """Test client backed by an empty catalog (no auth)."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=empty_catalog)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def app_with_auth(catalog_with_files):
    """App with a known API token."""
    from godmode_media_library.web.app import _auth_failures, create_app

    env = {"GML_API_TOKEN": "test-secret-token", "GML_RATE_LIMIT": "0"}
    with patch.dict(os.environ, env, clear=False):
        _auth_failures.clear()
        app = create_app(catalog_path=catalog_with_files)
        yield app
        _auth_failures.clear()


@pytest.fixture
def client_no_token(app_with_auth):
    """TestClient that does NOT send a token (for testing rejection)."""
    return TestClient(app_with_auth, raise_server_exceptions=False)


@pytest.fixture
def authed_client(app_with_auth):
    """TestClient that sends the correct Bearer token."""
    c = TestClient(app_with_auth, raise_server_exceptions=False)
    c.headers["Authorization"] = "Bearer test-secret-token"
    return c


# ===========================================================================
# Auth tests
# ===========================================================================


class TestAuth:
    """Authentication via Bearer token, X-API-Token header, query param."""

    def test_no_token_returns_401(self, client_no_token):
        resp = client_no_token.get("/api/stats")
        assert resp.status_code == 401
        assert "Invalid or missing" in resp.json()["detail"]

    def test_wrong_token_returns_401(self, client_no_token):
        resp = client_no_token.get(
            "/api/stats", headers={"Authorization": "Bearer wrong"}
        )
        assert resp.status_code == 401

    def test_valid_bearer_token(self, authed_client):
        resp = authed_client.get("/api/stats")
        assert resp.status_code == 200

    def test_valid_x_api_token_header(self, client_no_token):
        resp = client_no_token.get(
            "/api/stats", headers={"X-API-Token": "test-secret-token"}
        )
        assert resp.status_code == 200

    def test_valid_query_param_token(self, client_no_token):
        resp = client_no_token.get("/api/stats?token=test-secret-token")
        assert resp.status_code == 200

    def test_auth_not_required_for_docs(self, client_no_token):
        """OpenAPI docs are outside /api/ — no auth needed."""
        resp = client_no_token.get("/openapi.json")
        assert resp.status_code == 200

    def test_auth_failure_rate_limit_after_10(self, client_no_token):
        """After 10 bad attempts within the window the IP gets 429."""
        for _ in range(10):
            resp = client_no_token.get(
                "/api/stats", headers={"Authorization": "Bearer bad"}
            )
            assert resp.status_code == 401

        # 11th attempt should be rate-limited
        resp = client_no_token.get(
            "/api/stats", headers={"Authorization": "Bearer bad"}
        )
        assert resp.status_code == 429
        assert "Retry-After" in resp.headers

    def test_valid_token_works_after_some_failures(self, client_no_token):
        """A few failures below the threshold don't block valid requests."""
        for _ in range(5):
            client_no_token.get(
                "/api/stats", headers={"Authorization": "Bearer x"}
            )
        resp = client_no_token.get(
            "/api/stats",
            headers={"Authorization": "Bearer test-secret-token"},
        )
        assert resp.status_code == 200

    def test_auth_not_required_when_env_unset(self, catalog_with_files):
        """Without GML_API_TOKEN env var, API is open."""
        from godmode_media_library.web.app import create_app

        env = {"GML_API_TOKEN": ""}
        with patch.dict(os.environ, env, clear=False):
            app = create_app(catalog_path=catalog_with_files)
        c = TestClient(app)
        resp = c.get("/api/stats")
        assert resp.status_code == 200


# ===========================================================================
# Security headers
# ===========================================================================


class TestSecurityHeaders:
    def test_security_headers_present(self, client):
        resp = client.get("/api/stats")
        assert resp.headers.get("x-content-type-options") == "nosniff"
        assert resp.headers.get("x-frame-options") == "DENY"
        assert resp.headers.get("referrer-policy") == "strict-origin-when-cross-origin"


# ===========================================================================
# Stats / categories
# ===========================================================================


class TestStatsEndpoint:
    def test_get_stats_populated(self, client):
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_files"] == 4
        assert isinstance(data["top_extensions"], list)

    def test_get_stats_empty_catalog(self, client_empty):
        resp = client_empty.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_files"] == 0

    def test_get_categories(self, client):
        resp = client.get("/api/categories")
        assert resp.status_code == 200
        data = resp.json()
        assert "categories" in data
        assert "images" in data["categories"]


# ===========================================================================
# Files endpoint
# ===========================================================================


class TestFilesEndpoint:
    def test_get_files(self, client):
        resp = client.get("/api/files")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0
        assert "files" in data

    def test_get_files_empty(self, client_empty):
        resp = client_empty.get("/api/files")
        assert resp.status_code == 200
        data = resp.json()
        assert data["files"] == []
        assert data["count"] == 0
        assert data["has_more"] is False

    def test_get_files_with_ext_filter(self, client):
        resp = client.get("/api/files?ext=jpg")
        assert resp.status_code == 200
        assert resp.json()["count"] > 0

    def test_get_files_with_size_filter(self, client):
        resp = client.get("/api/files?min_size=0&max_size=1")
        assert resp.status_code == 200
        assert isinstance(resp.json()["files"], list)

    def test_get_files_limit_over_max_rejected(self, client):
        """limit > 10000 should be rejected by FastAPI validation."""
        resp = client.get("/api/files?limit=99999")
        assert resp.status_code == 422

    def test_get_files_pagination(self, client):
        d1 = client.get("/api/files?limit=2&offset=0").json()
        d2 = client.get("/api/files?limit=2&offset=2").json()
        assert d1["count"] == 2
        assert d1["has_more"] is True
        paths1 = {f["path"] for f in d1["files"]}
        paths2 = {f["path"] for f in d2["files"]}
        assert paths1.isdisjoint(paths2)

    def test_get_files_gps_filter(self, client):
        resp = client.get("/api/files?has_gps=true")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0  # no GPS in test data

    def test_get_file_detail(self, client):
        path = client.get("/api/files").json()["files"][0]["path"]
        resp = client.get(f"/api/files{path}")
        assert resp.status_code == 200
        assert "file" in resp.json()

    def test_file_detail_not_found(self, client):
        resp = client.get("/api/files/nonexistent/path.jpg")
        assert resp.status_code == 404


# ===========================================================================
# Delete — path safety
# ===========================================================================


class TestDeleteSafety:
    """Ensure delete endpoint rejects paths outside managed roots."""

    def test_delete_outside_managed_roots(self, client, tmp_path):
        outsider = tmp_path / "evil.txt"
        outsider.write_text("precious data")
        resp = client.post("/api/files/delete", json={"paths": [str(outsider)]})
        data = resp.json()
        assert data["deleted"] == 0
        assert data["skipped"] == 1
        assert "outside managed roots" in data["errors"][0].lower()
        assert outsider.exists()

    def test_delete_system_directory_blocked(self, client):
        resp = client.post("/api/files/delete", json={"paths": ["/etc/passwd"]})
        data = resp.json()
        assert data["deleted"] == 0
        assert data["skipped"] == 1

    def test_delete_null_byte_path(self, client):
        resp = client.post(
            "/api/files/delete", json={"paths": ["/tmp/evil\x00.txt"]}
        )
        data = resp.json()
        assert data["deleted"] == 0
        assert data["skipped"] >= 1

    def test_delete_nonexistent_file(self, client, catalog_with_files):
        """Delete of a file that's within roots but doesn't exist on disk."""
        # Get a managed root from the catalog
        cat = Catalog(catalog_with_files)
        cat.open()
        paths = cat.all_paths()
        cat.close()
        first_path = next(iter(paths))
        root = str(first_path.rsplit("/", 1)[0])
        fake_path = root + "/nonexistent.jpg"
        resp = client.post("/api/files/delete", json={"paths": [fake_path]})
        data = resp.json()
        assert data["deleted"] == 0
        assert data["skipped"] == 1


# ===========================================================================
# Consolidation status
# ===========================================================================


class TestConsolidationStatus:
    def test_consolidation_status_returns_dict(self, client):
        resp = client.get("/api/consolidation/status")
        assert resp.status_code == 200
        assert isinstance(resp.json(), dict)


# ===========================================================================
# Misc endpoints
# ===========================================================================


class TestMiscEndpoints:
    def test_roots(self, client):
        resp = client.get("/api/roots")
        assert resp.status_code == 200

    def test_deps(self, client):
        resp = client.get("/api/deps")
        assert resp.status_code == 200
        data = resp.json()
        assert "dependencies" in data
        names = {d["name"] for d in data["dependencies"]}
        assert "ExifTool" in names

    def test_tasks_list_empty(self, client):
        resp = client.get("/api/tasks")
        assert resp.status_code == 200

    def test_task_not_found(self, client):
        resp = client.get("/api/tasks/nonexistent-uuid")
        assert resp.status_code == 404

    def test_duplicates(self, client):
        resp = client.get("/api/duplicates")
        assert resp.status_code == 200
        data = resp.json()
        assert "groups" in data
        assert "total_groups" in data

    def test_similar(self, client):
        resp = client.get("/api/similar?threshold=10")
        assert resp.status_code == 200
        assert "pairs" in resp.json()

    def test_openapi_schema(self, client):
        resp = client.get("/openapi.json")
        assert resp.status_code == 200
        data = resp.json()
        assert "/api/stats" in data["paths"]

    def test_thumbnail_not_found(self, client):
        resp = client.get("/api/thumbnail/nonexistent/path.jpg")
        assert resp.status_code == 404


# ===========================================================================
# Input validation
# ===========================================================================


class TestInputValidation:
    def test_rename_missing_body(self, client):
        resp = client.post("/api/files/rename")
        assert resp.status_code == 422

    def test_move_missing_body(self, client):
        resp = client.post("/api/files/move")
        assert resp.status_code == 422

    def test_quarantine_missing_body(self, client):
        resp = client.post("/api/files/quarantine")
        assert resp.status_code == 422

    def test_delete_missing_body(self, client):
        resp = client.post("/api/files/delete")
        assert resp.status_code == 422


# ===========================================================================
# Path traversal
# ===========================================================================


class TestPathTraversal:
    def test_thumbnail_path_traversal_blocked(self, client):
        resp = client.get("/api/thumbnail/etc/passwd")
        assert resp.status_code == 404

    def test_thumbnail_dot_dot_traversal(self, client):
        # Literal /../ is resolved by Starlette router — use encoded variant
        resp = client.get("/api/thumbnail/..%2F..%2Fetc%2Fpasswd")
        assert resp.status_code in (404, 400, 422)


# ===========================================================================
# Task progress / eviction
# ===========================================================================


class TestTaskManagement:
    def test_task_progress_field(self, client):
        from godmode_media_library.web.api import _create_task, _update_progress

        task = _create_task("test")
        _update_progress(task.id, {"phase": "hashing", "total": 100, "processed": 42})
        resp = client.get(f"/api/tasks/{task.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["progress"]["phase"] == "hashing"
        assert data["progress"]["processed"] == 42

    def test_task_eviction(self):
        """Old completed tasks should be evicted."""
        from godmode_media_library.web.api import (
            _create_task,
            _evict_old_tasks,
            _finish_task,
            _tasks,
            _tasks_lock,
        )

        with _tasks_lock:
            saved_tasks = dict(_tasks)
            _tasks.clear()

        try:
            old_ids = []
            for i in range(5):
                task = _create_task(f"evict-test-{i}")
                _finish_task(task.id, result={"ok": True})
                with _tasks_lock:
                    _tasks[task.id]._created_ts -= 7200
                old_ids.append(task.id)

            fresh = _create_task("fresh")
            with _tasks_lock:
                _evict_old_tasks()
                assert fresh.id in _tasks
                for old_id in old_ids:
                    assert old_id not in _tasks
        finally:
            with _tasks_lock:
                _tasks.clear()
                _tasks.update(saved_tasks)


# ===========================================================================
# Background task endpoints
# ===========================================================================


class TestBackgroundTasks:
    def test_post_scan_returns_task(self, client):
        resp = client.post("/api/scan", json={"workers": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_post_pipeline_returns_task(self, client):
        resp = client.post("/api/pipeline", json={"workers": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_post_verify_returns_task(self, client):
        resp = client.post("/api/verify")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"


# ===========================================================================
# WebSocket
# ===========================================================================


class TestWebSocket:
    def test_websocket_task_status(self, client):
        from godmode_media_library.web.api import _create_task, _finish_task

        task = _create_task("ws-test")
        _finish_task(task.id, result={"ok": True})

        with client.websocket_connect(f"/api/ws/tasks/{task.id}") as ws:
            data = ws.receive_json()
            assert data["id"] == task.id
            assert data["status"] == "completed"

    def test_websocket_task_not_found(self, client):
        with client.websocket_connect("/api/ws/tasks/nonexistent") as ws:
            data = ws.receive_json()
            assert "error" in data


# ===========================================================================
# Catalog vacuum (not API, but was here before)
# ===========================================================================


def test_catalog_vacuum(tmp_path):
    """Catalog.vacuum() should run without error."""
    db_path = tmp_path / "test.db"
    with Catalog(db_path) as cat:
        cat.vacuum()


# ===========================================================================
# _sanitize_path unit tests
# ===========================================================================


class TestSanitizePath:
    def test_null_byte_rejected(self, client):
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": "/tmp/foo\x00bar.jpg", "new_name": "ok.jpg"}]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["skipped"] == 1
        assert "null bytes" in data["errors"][0].lower() or data["skipped"] == 1

    def test_excessively_long_path_rejected(self, client):
        long_path = "/media/" + "a" * 5000 + ".jpg"
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": long_path, "new_name": "ok.jpg"}]},
        )
        data = resp.json()
        assert data["skipped"] == 1

    def test_dot_dot_only_path_rejected(self, client):
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": "..", "new_name": "ok.jpg"}]},
        )
        data = resp.json()
        assert data["skipped"] == 1


# ===========================================================================
# Rename — path traversal and validation
# ===========================================================================


class TestRenameValidation:
    def test_rename_traversal_in_new_name(self, client):
        """new_name with path separators or '..' should be rejected."""
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": "/some/file.jpg", "new_name": "../evil.jpg"}]},
        )
        data = resp.json()
        assert data["renamed"] == 0
        assert data["skipped"] == 1
        assert "traversal" in data["errors"][0].lower() or "separator" in data["errors"][0].lower()

    def test_rename_slash_in_new_name(self, client):
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": "/some/file.jpg", "new_name": "sub/evil.jpg"}]},
        )
        data = resp.json()
        assert data["renamed"] == 0
        assert data["skipped"] >= 1

    def test_rename_backslash_in_new_name(self, client):
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": "/some/file.jpg", "new_name": "sub\\evil.jpg"}]},
        )
        data = resp.json()
        assert data["renamed"] == 0
        assert data["skipped"] >= 1

    def test_rename_empty_new_name(self, client):
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": "/some/file.jpg", "new_name": ""}]},
        )
        data = resp.json()
        assert data["renamed"] == 0
        assert data["skipped"] >= 1

    def test_rename_file_not_found(self, client):
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": "/nonexistent/file.jpg", "new_name": "ok.jpg"}]},
        )
        data = resp.json()
        assert data["renamed"] == 0
        assert data["skipped"] == 1

    def test_rename_success(self, client, catalog_with_files):
        """Rename a real file."""
        files_resp = client.get("/api/files")
        first = files_resp.json()["files"][0]
        path = first["path"]
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": path, "new_name": "renamed_photo.jpg"}]},
        )
        data = resp.json()
        assert data["renamed"] == 1
        assert data["skipped"] == 0


# ===========================================================================
# Move — validation
# ===========================================================================


class TestMoveValidation:
    def test_move_to_system_dir_blocked(self, client):
        resp = client.post(
            "/api/files/move",
            json={"paths": ["/some/file.jpg"], "destination": "/etc/evil"},
        )
        assert resp.status_code == 403

    def test_move_file_not_found(self, client, catalog_with_files):
        """Move with a valid destination but nonexistent source file."""
        cat = Catalog(catalog_with_files)
        cat.open()
        paths = cat.all_paths()
        cat.close()
        first_path = next(iter(paths))
        root = first_path.rsplit("/", 1)[0]
        resp = client.post(
            "/api/files/move",
            json={"paths": ["/nonexistent/file.jpg"], "destination": root},
        )
        data = resp.json()
        assert data["moved"] == 0
        assert data["skipped"] >= 1


# ===========================================================================
# Quarantine — validation
# ===========================================================================


class TestQuarantineValidation:
    def test_quarantine_invalid_path(self, client):
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": ["/nonexistent/file\x00.jpg"]},
        )
        data = resp.json()
        assert data["moved"] == 0
        assert data["skipped"] >= 1

    def test_quarantine_file_not_found(self, client):
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": ["/nonexistent/file.jpg"]},
        )
        data = resp.json()
        assert data["moved"] == 0
        assert data["skipped"] >= 1

    def test_quarantine_system_root_blocked(self, client):
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": ["/some/file.jpg"], "quarantine_root": "/etc"},
        )
        assert resp.status_code == 403


# ===========================================================================
# Favorites
# ===========================================================================


class TestFavorites:
    def test_list_favorites_empty(self, client):
        resp = client.get("/api/files/favorites")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["favorites"] == []

    def test_toggle_favorite(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        # Toggle on
        resp = client.post("/api/files/favorite", json={"path": path})
        assert resp.status_code == 200
        assert resp.json()["is_favorite"] is True
        # Verify in list
        resp = client.get("/api/files/favorites")
        assert path in resp.json()["favorites"]
        # Toggle off
        resp = client.post("/api/files/favorite", json={"path": path})
        assert resp.json()["is_favorite"] is False


# ===========================================================================
# Notes
# ===========================================================================


class TestNotes:
    def test_get_note_empty(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        # Strip leading slash for URL path
        url_path = path.lstrip("/")
        resp = client.get(f"/api/files/{url_path}/note")
        assert resp.status_code == 200
        data = resp.json()
        assert data["note"] is None

    def test_set_and_get_note(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        url_path = path.lstrip("/")
        # Set note
        resp = client.put(f"/api/files/{url_path}/note", json={"note": "Test note"})
        assert resp.status_code == 200
        assert resp.json()["saved"] is True
        # Get note
        resp = client.get(f"/api/files/{url_path}/note")
        assert resp.json()["note"] == "Test note"

    def test_delete_note(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        url_path = path.lstrip("/")
        # Set then delete
        client.put(f"/api/files/{url_path}/note", json={"note": "To delete"})
        resp = client.request("DELETE", f"/api/files/{url_path}/note")
        assert resp.status_code == 200


# ===========================================================================
# Ratings
# ===========================================================================


class TestRatings:
    def test_set_rating(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        url_path = path.lstrip("/")
        resp = client.put(f"/api/files/{url_path}/rating", json={"rating": 4})
        assert resp.status_code == 200
        assert resp.json()["rating"] == 4

    def test_set_rating_invalid_too_high(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        url_path = path.lstrip("/")
        resp = client.put(f"/api/files/{url_path}/rating", json={"rating": 6})
        assert resp.status_code == 400

    def test_set_rating_invalid_too_low(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        url_path = path.lstrip("/")
        resp = client.put(f"/api/files/{url_path}/rating", json={"rating": 0})
        assert resp.status_code == 400

    def test_delete_rating(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        url_path = path.lstrip("/")
        client.put(f"/api/files/{url_path}/rating", json={"rating": 3})
        resp = client.request("DELETE", f"/api/files/{url_path}/rating")
        assert resp.status_code == 200


# ===========================================================================
# Tags
# ===========================================================================


class TestTags:
    def test_list_tags_empty(self, client):
        resp = client.get("/api/tags")
        assert resp.status_code == 200
        assert "tags" in resp.json()

    def test_create_and_list_tag(self, client):
        resp = client.post("/api/tags", json={"name": "TestTag", "color": "#ff0000"})
        assert resp.status_code == 200
        tag = resp.json()
        assert tag["name"] == "TestTag"
        # List should include it
        resp = client.get("/api/tags")
        names = [t["name"] for t in resp.json()["tags"]]
        assert "TestTag" in names

    def test_create_duplicate_tag(self, client):
        client.post("/api/tags", json={"name": "UniqueTag"})
        resp = client.post("/api/tags", json={"name": "UniqueTag"})
        assert resp.status_code == 409

    def test_delete_tag(self, client):
        resp = client.post("/api/tags", json={"name": "ToDelete"})
        tag_id = resp.json()["id"]
        resp = client.request("DELETE", f"/api/tags/{tag_id}")
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

    def test_tag_files(self, client):
        # Create a tag
        tag_resp = client.post("/api/tags", json={"name": "FileTag"})
        tag_id = tag_resp.json()["id"]
        # Get a file path
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        # Tag the file
        resp = client.post("/api/files/tag", json={"paths": [path], "tag_id": tag_id})
        assert resp.status_code == 200
        assert resp.json()["tagged"] >= 1

    def test_untag_files(self, client):
        tag_resp = client.post("/api/tags", json={"name": "UntagMe"})
        tag_id = tag_resp.json()["id"]
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        client.post("/api/files/tag", json={"paths": [path], "tag_id": tag_id})
        resp = client.request(
            "DELETE", "/api/files/tag", json={"paths": [path], "tag_id": tag_id}
        )
        assert resp.status_code == 200


# ===========================================================================
# Browse filesystem
# ===========================================================================


class TestBrowse:
    def test_browse_home(self, client):
        resp = client.get("/api/browse")
        assert resp.status_code == 200
        data = resp.json()
        assert "current" in data
        assert "entries" in data
        assert "bookmarks" in data

    def test_browse_blocked_path(self, client):
        resp = client.get("/api/browse?path=/etc")
        assert resp.status_code == 403

    def test_browse_nonexistent_path(self, client):
        resp = client.get("/api/browse?path=/nonexistent_path_xyz")
        assert resp.status_code == 404


# ===========================================================================
# Roots management
# ===========================================================================


class TestRootsManagement:
    def test_save_and_get_roots(self, client, tmp_path):
        root_dir = str(tmp_path)
        resp = client.post("/api/roots", json={"roots": [root_dir]})
        assert resp.status_code == 200
        assert resp.json()["saved"] is True
        # Verify
        resp = client.get("/api/roots")
        assert root_dir in resp.json()["roots"]

    def test_remove_root(self, client, tmp_path):
        root_dir = str(tmp_path)
        client.post("/api/roots", json={"roots": [root_dir]})
        resp = client.request("DELETE", "/api/roots", json={"path": root_dir})
        assert resp.status_code == 200
        assert resp.json()["removed"] is True

    def test_save_roots_filters_nonexistent(self, client):
        resp = client.post(
            "/api/roots", json={"roots": ["/nonexistent_path_xyz_123"]}
        )
        assert resp.status_code == 200
        # Nonexistent path should be filtered out
        assert len(resp.json()["roots"]) == 0


# ===========================================================================
# Sources
# ===========================================================================


class TestSources:
    def test_get_sources(self, client):
        resp = client.get("/api/sources")
        assert resp.status_code == 200
        data = resp.json()
        assert "sources" in data
        assert "thumbnail_cache" in data


# ===========================================================================
# Memories
# ===========================================================================


class TestMemories:
    def test_get_memories(self, client):
        resp = client.get("/api/memories")
        assert resp.status_code == 200
        data = resp.json()
        assert "date" in data
        assert "memories" in data
        assert isinstance(data["memories"], list)


# ===========================================================================
# System info
# ===========================================================================


class TestSystemInfo:
    def test_get_system_info(self, client):
        resp = client.get("/api/system-info")
        assert resp.status_code == 200
        data = resp.json()
        assert "python_version" in data
        assert "platform" in data
        assert "catalog_path" in data
        assert "total_files" in data


# ===========================================================================
# Duplicate group detail
# ===========================================================================


class TestDuplicateGroup:
    def test_get_duplicate_group_not_found(self, client):
        resp = client.get("/api/duplicates/nonexistent-group-id")
        assert resp.status_code == 404

    def test_get_duplicate_diff_not_found(self, client):
        resp = client.get("/api/duplicates/nonexistent-group-id/diff")
        assert resp.status_code == 404


# ===========================================================================
# File detail enrichment
# ===========================================================================


class TestFileDetail:
    def test_file_detail_has_metadata(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        resp = client.get(f"/api/files{path}")
        assert resp.status_code == 200
        data = resp.json()
        assert "file" in data
        assert "metadata" in data
        assert "tags" in data
        assert "rating" in data


# ===========================================================================
# Consolidation status
# ===========================================================================


class TestConsolidationStatusExtended:
    def test_consolidation_status_has_keys(self, client):
        resp = client.get("/api/consolidation/status")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)


# ===========================================================================
# Files filters (sorting, extended params)
# ===========================================================================


class TestFilesFilters:
    def test_files_sort_by_size(self, client):
        resp = client.get("/api/files?sort=size&order=desc")
        assert resp.status_code == 200
        files = resp.json()["files"]
        if len(files) >= 2:
            assert files[0]["size"] >= files[1]["size"]

    def test_files_path_contains(self, client):
        resp = client.get("/api/files?path_contains=photo")
        assert resp.status_code == 200
        for f in resp.json()["files"]:
            assert "photo" in f["path"].lower()

    def test_files_date_range(self, client):
        resp = client.get("/api/files?date_from=2000-01-01&date_to=2099-12-31")
        assert resp.status_code == 200

    def test_files_favorites_only(self, client):
        resp = client.get("/api/files?favorites_only=true")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0  # no favorites set

    def test_files_has_notes(self, client):
        resp = client.get("/api/files?has_notes=true")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

    def test_files_min_rating(self, client):
        resp = client.get("/api/files?min_rating=3")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0


# ===========================================================================
# Restore files
# ===========================================================================


class TestRestoreFiles:
    def test_restore_not_in_quarantine(self, client):
        resp = client.post(
            "/api/files/restore",
            json={"paths": ["/nonexistent/file.jpg"]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["restored"] == 0
        assert len(data["errors"]) >= 1

    def test_restore_invalid_path(self, client):
        resp = client.post(
            "/api/files/restore",
            json={"paths": ["/some/path\x00evil.jpg"]},
        )
        data = resp.json()
        assert data["restored"] == 0


# ===========================================================================
# Scenario endpoints (mocked)
# ===========================================================================


class TestScenarioEndpoints:
    def test_list_scenarios(self, client):
        resp = client.get("/api/scenarios")
        assert resp.status_code == 200
        assert "scenarios" in resp.json()

    def test_get_scenario_templates(self, client):
        resp = client.get("/api/scenarios/templates")
        assert resp.status_code == 200
        assert "templates" in resp.json()

    def test_get_step_types(self, client):
        resp = client.get("/api/scenarios/step-types")
        assert resp.status_code == 200
        assert "step_types" in resp.json()

    def test_get_scenario_not_found(self, client):
        resp = client.get("/api/scenarios/nonexistent-id")
        assert resp.status_code == 404

    def test_create_scenario(self, client):
        resp = client.post(
            "/api/scenarios",
            json={"name": "Test Scenario", "description": "A test"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "id" in data
        assert data["name"] == "Test Scenario"

    def test_delete_scenario_not_found(self, client):
        resp = client.request("DELETE", "/api/scenarios/nonexistent-id")
        assert resp.status_code == 404

    def test_update_scenario_not_found(self, client):
        resp = client.put(
            "/api/scenarios/nonexistent-id",
            json={"name": "Updated"},
        )
        assert resp.status_code == 404

    def test_duplicate_scenario_not_found(self, client):
        resp = client.post("/api/scenarios/nonexistent-id/duplicate")
        assert resp.status_code == 404

    def test_run_scenario_not_found(self, client):
        resp = client.post("/api/scenarios/nonexistent-id/run")
        assert resp.status_code == 404


# ===========================================================================
# Recovery endpoints
# ===========================================================================


class TestRecoveryEndpoints:
    def test_recovery_apps(self, client):
        resp = client.get("/api/recovery/apps")
        assert resp.status_code == 200
        assert "apps" in resp.json()

    def test_recovery_quarantine_list(self, client):
        resp = client.get("/api/recovery/quarantine")
        assert resp.status_code == 200
        data = resp.json()
        assert "entries" in data
        assert "total" in data

    def test_recovery_deep_scan(self, client):
        resp = client.post("/api/recovery/deep-scan")
        assert resp.status_code == 200
        assert "task_id" in resp.json()

    def test_recovery_integrity_check(self, client):
        resp = client.post("/api/recovery/integrity-check")
        assert resp.status_code == 200
        assert "task_id" in resp.json()

    def test_recovery_photorec_status(self, client):
        resp = client.get("/api/recovery/photorec/status")
        assert resp.status_code == 200
        assert "available" in resp.json()

    def test_recovery_disks(self, client):
        resp = client.get("/api/recovery/disks")
        assert resp.status_code == 200
        assert "disks" in resp.json()

    def test_recovery_quarantine_restore_invalid(self, client):
        resp = client.post(
            "/api/recovery/quarantine/restore",
            json={"paths": ["/nonexistent/file.jpg"]},
        )
        assert resp.status_code == 200

    def test_recovery_quarantine_delete_invalid(self, client):
        resp = client.post(
            "/api/recovery/quarantine/delete",
            json={"paths": ["/nonexistent/file.jpg"]},
        )
        assert resp.status_code == 200

    def test_recovery_quarantine_restore_blocked_dest(self, client):
        resp = client.post(
            "/api/recovery/quarantine/restore",
            json={"paths": ["/some/file.jpg"], "restore_to": "/dev"},
        )
        assert resp.status_code == 403

    def test_recovery_app_mine(self, client):
        resp = client.post("/api/recovery/app-mine", json={})
        assert resp.status_code == 200
        assert "task_id" in resp.json()


# ===========================================================================
# Shares
# ===========================================================================


class TestShares:
    def test_list_shares_empty(self, client):
        resp = client.get("/api/shares")
        assert resp.status_code == 200
        assert "shares" in resp.json()

    def test_create_share_file_not_found(self, client):
        resp = client.post(
            "/api/shares",
            json={"path": "/nonexistent/file.jpg"},
        )
        assert resp.status_code == 404

    def test_shares_for_file(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        resp = client.get(f"/api/shares/file?path={path}")
        assert resp.status_code == 200
        assert "shares" in resp.json()


# ===========================================================================
# Backfill metadata
# ===========================================================================


class TestBackfillMetadata:
    def test_backfill_metadata(self, client):
        resp = client.post("/api/backfill-metadata")
        assert resp.status_code == 200
        data = resp.json()
        assert "fs_dates_filled" in data


# ===========================================================================
# Stream endpoint
# ===========================================================================


class TestStreamEndpoint:
    def test_stream_not_in_catalog(self, client):
        resp = client.get("/api/stream/nonexistent/path.jpg")
        assert resp.status_code == 404

    def test_stream_file_in_catalog(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        url_path = path.lstrip("/")
        resp = client.get(f"/api/stream/{url_path}")
        assert resp.status_code == 200


# ===========================================================================
# Dedup rules config
# ===========================================================================


class TestDedupRulesConfig:
    def test_get_dedup_rules(self, client):
        resp = client.get("/api/config/dedup-rules")
        assert resp.status_code == 200
        data = resp.json()
        assert "strategy" in data

    def test_put_dedup_rules(self, client, tmp_path):
        with patch(
            "godmode_media_library.config._global_config_path",
            return_value=tmp_path / "config.toml",
        ):
            resp = client.put(
                "/api/config/dedup-rules",
                json={
                    "strategy": "richness",
                    "similarity_threshold": 15,
                    "auto_resolve": False,
                    "merge_metadata": True,
                    "quarantine_path": "",
                    "exclude_extensions": [],
                    "exclude_paths": [],
                    "min_file_size_kb": 0,
                },
            )
            assert resp.status_code == 200
            assert resp.json()["status"] == "ok"


# ===========================================================================
# Reorganize sources
# ===========================================================================


class TestReorganize:
    def test_reorganize_sources(self, client):
        resp = client.get("/api/reorganize/sources")
        assert resp.status_code == 200
        assert "sources" in resp.json()

    def test_reorganize_execute_no_plan(self, client):
        resp = client.post(
            "/api/reorganize/execute",
            json={"plan_id": "nonexistent-plan-id"},
        )
        assert resp.status_code == 404


# ===========================================================================
# Backup endpoints
# ===========================================================================


class TestBackupEndpoints:
    def test_backup_stats(self, client):
        resp = client.get("/api/backup/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_files" in data
        assert "coverage_pct" in data

    def test_backup_targets(self, client):
        resp = client.get("/api/backup/targets")
        assert resp.status_code == 200
        assert "targets" in resp.json()

    def test_backup_plan(self, client):
        resp = client.post("/api/backup/plan")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_files" in data

    def test_backup_manifest(self, client):
        resp = client.get("/api/backup/manifest")
        assert resp.status_code == 200
        data = resp.json()
        assert "entries" in data
        assert "total" in data

    def test_backup_execute(self, client):
        resp = client.post("/api/backup/execute", json={"dry_run": True})
        assert resp.status_code == 200
        assert "task_id" in resp.json()

    def test_backup_verify(self, client):
        resp = client.post("/api/backup/verify")
        assert resp.status_code == 200
        assert "task_id" in resp.json()


# ===========================================================================
# Tag suggestions
# ===========================================================================


class TestTagSuggestions:
    def test_suggest_tags_file_not_found(self, client):
        resp = client.get("/api/tags/suggest?path=/nonexistent/file.jpg")
        assert resp.status_code == 404

    def test_suggest_tags_for_file(self, client):
        files_resp = client.get("/api/files")
        path = files_resp.json()["files"][0]["path"]
        resp = client.get(f"/api/tags/suggest?path={path}")
        assert resp.status_code == 200
        assert "suggestions" in resp.json()
