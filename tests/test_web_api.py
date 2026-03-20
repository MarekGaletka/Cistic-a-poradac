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
    with patch("godmode_media_library.scanner.probe_file", return_value=None), \
         patch("godmode_media_library.scanner.read_exif", return_value=None), \
         patch("godmode_media_library.scanner.dhash", return_value=None), \
         patch("godmode_media_library.scanner.video_dhash", return_value=None):
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
