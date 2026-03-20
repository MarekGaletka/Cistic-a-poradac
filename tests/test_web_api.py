"""Tests for the web API endpoints."""

from __future__ import annotations

from unittest.mock import patch

import pytest

try:
    from fastapi.testclient import TestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

from godmode_media_library.catalog import Catalog

pytestmark = pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi not installed")


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
def client(catalog_with_files):
    """Create a test client with a populated catalog."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_with_files)
    return TestClient(app)


def test_get_stats(client):
    resp = client.get("/api/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "total_files" in data
    assert data["total_files"] == 4
    # top_extensions should be a list of [ext, count] pairs
    assert isinstance(data["top_extensions"], list)
    if data["top_extensions"]:
        assert isinstance(data["top_extensions"][0], list)
        assert len(data["top_extensions"][0]) == 2


def test_get_files(client):
    resp = client.get("/api/files")
    assert resp.status_code == 200
    data = resp.json()
    assert "files" in data
    assert data["count"] > 0


def test_get_files_with_filter(client):
    resp = client.get("/api/files?ext=jpg")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] > 0


def test_get_file_detail(catalog_with_files, client):
    # First get a file path from the list
    resp = client.get("/api/files")
    files = resp.json()["files"]
    assert len(files) > 0
    path = files[0]["path"]
    # Remove leading / for URL
    resp2 = client.get(f"/api/files{path}")
    assert resp2.status_code == 200
    data = resp2.json()
    assert "file" in data


def test_get_duplicates(client):
    resp = client.get("/api/duplicates")
    assert resp.status_code == 200
    data = resp.json()
    assert "groups" in data
    assert "total_groups" in data
    # Should have at least 1 group (dup1.jpg and dup2.jpg)
    assert data["total_groups"] >= 1


def test_get_deps(client):
    resp = client.get("/api/deps")
    assert resp.status_code == 200
    data = resp.json()
    assert "dependencies" in data
    names = {d["name"] for d in data["dependencies"]}
    assert "ExifTool" in names
    assert "Pillow" in names


def test_get_similar(client):
    resp = client.get("/api/similar?threshold=10")
    assert resp.status_code == 200
    data = resp.json()
    assert "pairs" in data


def test_get_tasks_not_found(client):
    resp = client.get("/api/tasks/nonexistent")
    assert resp.status_code == 404


def test_file_not_found(client):
    resp = client.get("/api/files/nonexistent/path.jpg")
    assert resp.status_code == 404


def test_thumbnail_not_found(client):
    resp = client.get("/api/thumbnail/nonexistent/path.jpg")
    assert resp.status_code == 404


def test_get_files_with_size_filter(client):
    resp = client.get("/api/files?min_size=0&max_size=1")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data["files"], list)


def test_get_files_with_gps_filter(client):
    resp = client.get("/api/files?has_gps=true")
    assert resp.status_code == 200
    data = resp.json()
    # No files have GPS in test data
    assert data["count"] == 0


def test_get_files_with_phash_filter(client):
    resp = client.get("/api/files?has_phash=true")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data["files"], list)


def test_task_progress_field(client):
    """Verify task response includes progress field."""
    from godmode_media_library.web.api import _create_task, _update_progress

    task = _create_task("test")
    _update_progress(task.id, {"phase": "hashing", "total": 100, "processed": 42})
    resp = client.get(f"/api/tasks/{task.id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["progress"] is not None
    assert data["progress"]["phase"] == "hashing"
    assert data["progress"]["processed"] == 42


def test_get_files_with_pagination(client):
    # Get first page
    resp1 = client.get("/api/files?limit=2&offset=0")
    assert resp1.status_code == 200
    d1 = resp1.json()
    assert d1["count"] == 2
    assert d1["has_more"] is True

    # Get second page
    resp2 = client.get("/api/files?limit=2&offset=2")
    assert resp2.status_code == 200
    d2 = resp2.json()
    assert d2["count"] == 2
    assert d2["has_more"] is False

    # Pages should have different files
    paths1 = {f["path"] for f in d1["files"]}
    paths2 = {f["path"] for f in d2["files"]}
    assert paths1.isdisjoint(paths2)


# ── Security tests ────────────────────────────────────


def test_thumbnail_path_traversal_blocked(client):
    """Thumbnail endpoint must not serve files outside the catalog."""
    # Attempt to access /etc/passwd via path traversal
    resp = client.get("/api/thumbnail/etc/passwd")
    assert resp.status_code == 404


def test_thumbnail_dot_dot_traversal(client):
    """Path traversal with .. must be blocked."""
    resp = client.get("/api/thumbnail/../../../etc/passwd")
    # FastAPI normalizes the path, but our catalog check should still block it
    assert resp.status_code in (404, 400, 422)


def test_security_headers(client):
    """Response should include security headers."""
    resp = client.get("/api/stats")
    assert resp.headers.get("x-content-type-options") == "nosniff"
    assert resp.headers.get("x-frame-options") == "DENY"
    assert resp.headers.get("referrer-policy") == "strict-origin-when-cross-origin"


# ── POST endpoint tests ──────────────────────────────


def test_post_scan_no_roots(client):
    """POST /scan without roots should start and report error in task."""
    resp = client.post("/api/scan", json={"workers": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert "task_id" in data
    assert data["status"] == "started"


def test_post_scan_with_roots(catalog_with_files, client):
    """POST /scan with valid roots should start a scan task."""
    # Get the media root from the catalog
    cat = Catalog(catalog_with_files)
    cat.open()
    paths = cat.all_paths()
    cat.close()
    # Extract root directory from first path
    first_path = next(iter(paths))
    root = str(first_path.rsplit("/", 1)[0])

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
    ):
        resp = client.post("/api/scan", json={"roots": [root], "workers": 1, "extract_exiftool": False})
    assert resp.status_code == 200
    assert resp.json()["status"] == "started"


def test_post_pipeline_starts(client):
    """POST /pipeline should return task_id."""
    resp = client.post("/api/pipeline", json={"workers": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert "task_id" in data
    assert data["status"] == "started"


# ── Task eviction tests ──────────────────────────────


def test_task_eviction():
    """Old completed tasks should be evicted."""
    from godmode_media_library.web.api import (
        _create_task,
        _evict_old_tasks,
        _finish_task,
        _tasks,
        _tasks_lock,
    )

    # Save existing tasks and clear for isolated test
    with _tasks_lock:
        saved_tasks = dict(_tasks)
        _tasks.clear()

    try:
        # Create and finish some tasks
        old_ids = []
        for i in range(5):
            task = _create_task(f"evict-test-{i}")
            _finish_task(task.id, result={"ok": True})
            with _tasks_lock:
                _tasks[task.id]._created_ts -= 7200  # 2 hours ago
            old_ids.append(task.id)

        # Create a fresh task
        fresh = _create_task("fresh")

        # Eviction should remove old tasks but keep fresh one
        with _tasks_lock:
            _evict_old_tasks()
            assert fresh.id in _tasks
            # Old tasks should be gone
            for old_id in old_ids:
                assert old_id not in _tasks
    finally:
        # Restore original tasks
        with _tasks_lock:
            _tasks.clear()
            _tasks.update(saved_tasks)


# ── Verify endpoint tests ────────────────────────────


def test_post_verify(client):
    """POST /verify should start a verify task."""
    resp = client.post("/api/verify")
    assert resp.status_code == 200
    data = resp.json()
    assert "task_id" in data
    assert data["status"] == "started"


# ── WebSocket endpoint test ──────────────────────────


def test_websocket_task_status(client):
    """WebSocket endpoint should return task status."""
    from godmode_media_library.web.api import _create_task, _finish_task

    task = _create_task("ws-test")
    _finish_task(task.id, result={"ok": True})

    with client.websocket_connect(f"/api/ws/tasks/{task.id}") as ws:
        data = ws.receive_json()
        assert data["id"] == task.id
        assert data["status"] == "completed"
        assert data["result"]["ok"] is True


def test_websocket_task_not_found(client):
    """WebSocket should report task not found."""
    with client.websocket_connect("/api/ws/tasks/nonexistent") as ws:
        data = ws.receive_json()
        assert "error" in data
