"""Tests for file sharing feature."""

from __future__ import annotations

import hashlib

import pytest
from fastapi.testclient import TestClient

from godmode_media_library.catalog import Catalog
from godmode_media_library.web.app import create_app


@pytest.fixture
def cat(tmp_path):
    db = tmp_path / "test.db"
    c = Catalog(db)
    c.open()
    # Insert a test file
    c.conn.execute(
        "INSERT INTO files (path, size, mtime, ctime, ext, first_seen, last_scanned) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("/tmp/photo.jpg", 1024, 1000.0, 1000.0, ".jpg", "2025-01-01", "2025-01-01"),
    )
    c.conn.commit()
    yield c
    c.close()


# ── Catalog-level tests ──


def test_shares_table_exists(cat):
    tables = {r[0] for r in cat.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "shares" in tables


def test_create_share_basic(cat):
    share = cat.create_share("/tmp/photo.jpg")
    assert share["token"]
    assert len(share["token"]) == 32
    assert share["has_password"] is False
    assert share["expires_at"] is None
    assert share["max_downloads"] is None
    assert share["download_count"] == 0
    assert share["path"] == "/tmp/photo.jpg"


def test_create_share_with_options(cat):
    share = cat.create_share(
        "/tmp/photo.jpg",
        label="Test share",
        password="secret123",
        expires_hours=24,
        max_downloads=5,
    )
    assert share["label"] == "Test share"
    assert share["has_password"] is True
    assert share["expires_at"] is not None
    assert share["max_downloads"] == 5


def test_create_share_nonexistent_file(cat):
    with pytest.raises(ValueError, match="not found"):
        cat.create_share("/nonexistent/file.jpg")


def test_get_share_by_token(cat):
    share = cat.create_share("/tmp/photo.jpg", label="my share")
    token = share["token"]
    found = cat.get_share_by_token(token)
    assert found is not None
    assert found["token"] == token
    assert found["path"] == "/tmp/photo.jpg"
    assert found["label"] == "my share"


def test_get_share_by_token_not_found(cat):
    assert cat.get_share_by_token("nonexistent_token") is None


def test_get_share_by_token_expired(cat):
    share = cat.create_share("/tmp/photo.jpg", expires_hours=1)
    # Manually set expires_at to past
    cat.conn.execute(
        "UPDATE shares SET expires_at = '2020-01-01T00:00:00+00:00' WHERE id = ?",
        (share["id"],),
    )
    cat.conn.commit()
    found = cat.get_share_by_token(share["token"])
    assert found is not None
    assert found.get("expired") is True


def test_get_shares_for_file(cat):
    cat.create_share("/tmp/photo.jpg", label="share1")
    cat.create_share("/tmp/photo.jpg", label="share2")
    shares = cat.get_shares_for_file("/tmp/photo.jpg")
    assert len(shares) == 2


def test_get_all_shares(cat):
    cat.create_share("/tmp/photo.jpg", label="s1")
    cat.create_share("/tmp/photo.jpg", label="s2")
    shares = cat.get_all_shares()
    assert len(shares) == 2


def test_delete_share(cat):
    share = cat.create_share("/tmp/photo.jpg")
    cat.delete_share(share["id"])
    assert cat.get_share_by_token(share["token"]) is None


def test_increment_download(cat):
    share = cat.create_share("/tmp/photo.jpg")
    assert share["download_count"] == 0
    cat.increment_download(share["id"])
    cat.increment_download(share["id"])
    found = cat.get_share_by_token(share["token"])
    assert found["download_count"] == 2


def test_cleanup_expired_shares(cat):
    cat.create_share("/tmp/photo.jpg", label="active")
    cat.create_share("/tmp/photo.jpg", label="expired")
    # Manually expire one
    cat.conn.execute(
        "UPDATE shares SET expires_at = '2020-01-01T00:00:00+00:00' WHERE id = (SELECT id FROM shares WHERE label = 'expired')"
    )
    cat.conn.commit()
    cleaned = cat.cleanup_expired_shares()
    assert cleaned == 1
    remaining = cat.get_all_shares()
    assert len(remaining) == 1


def test_multiple_shares_same_file(cat):
    s1 = cat.create_share("/tmp/photo.jpg", label="link1")
    s2 = cat.create_share("/tmp/photo.jpg", label="link2")
    assert s1["token"] != s2["token"]


def test_max_downloads_reached(cat):
    share = cat.create_share("/tmp/photo.jpg", max_downloads=2)
    cat.increment_download(share["id"])
    cat.increment_download(share["id"])
    found = cat.get_share_by_token(share["token"])
    assert found.get("max_downloads_reached") is True


def test_password_hash_stored(cat):
    share = cat.create_share("/tmp/photo.jpg", password="test123")
    found = cat.get_share_by_token(share["token"])
    expected = hashlib.sha256(b"test123").hexdigest()
    assert found["password_hash"] == expected


# ── API tests ──


@pytest.fixture
def client(tmp_path):
    db = tmp_path / "api_test.db"
    app = create_app(catalog_path=db)
    c = TestClient(app)
    # Insert a test file
    cat = Catalog(db)
    cat.open()
    cat.conn.execute(
        "INSERT INTO files (path, size, mtime, ctime, ext, first_seen, last_scanned) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("/tmp/test.jpg", 2048, 1000.0, 1000.0, ".jpg", "2025-01-01", "2025-01-01"),
    )
    cat.conn.commit()
    cat.close()
    return c


def test_api_create_share(client):
    resp = client.post("/api/shares", json={"path": "/tmp/test.jpg"})
    assert resp.status_code == 200
    data = resp.json()
    assert "token" in data
    assert len(data["token"]) == 32


def test_api_create_share_not_found(client):
    resp = client.post("/api/shares", json={"path": "/nonexistent.jpg"})
    assert resp.status_code == 404


def test_api_list_shares(client):
    client.post("/api/shares", json={"path": "/tmp/test.jpg"})
    resp = client.get("/api/shares")
    assert resp.status_code == 200
    assert len(resp.json()["shares"]) == 1


def test_api_shares_for_file(client):
    client.post("/api/shares", json={"path": "/tmp/test.jpg"})
    resp = client.get("/api/shares/file", params={"path": "/tmp/test.jpg"})
    assert resp.status_code == 200
    assert len(resp.json()["shares"]) == 1


def test_api_delete_share(client):
    resp = client.post("/api/shares", json={"path": "/tmp/test.jpg"})
    share_id = resp.json()["id"]
    del_resp = client.delete(f"/api/shares/{share_id}")
    assert del_resp.status_code == 200
    list_resp = client.get("/api/shares")
    assert len(list_resp.json()["shares"]) == 0


def test_api_shared_invalid_token(client):
    resp = client.get("/shared/invalid_token_here/info")
    assert resp.status_code == 404


def test_api_tag_suggest(client):
    resp = client.get("/api/tags/suggest", params={"path": "/tmp/test.jpg"})
    assert resp.status_code == 200
    data = resp.json()
    assert "suggestions" in data
    assert isinstance(data["suggestions"], list)
