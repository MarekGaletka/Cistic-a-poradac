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
        resp = client.get("/api/thumbnail/../../../etc/passwd")
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
