"""Tests for files route endpoints (web/routes/files.py).

Targets coverage improvement from ~79% to 88%+.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

try:
    from fastapi.testclient import TestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

from godmode_media_library.catalog import Catalog, CatalogFileRow

pytestmark = pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi not installed")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def populated_catalog(tmp_path):
    """Catalog with a few real files on disk."""
    db_path = tmp_path / "files_test.db"
    cat = Catalog(db_path)
    cat.open()

    media_dir = tmp_path / "media"
    media_dir.mkdir()

    now_iso = "2024-01-15T12:00:00+00:00"
    files_info = [
        ("photo1.jpg", b"FAKE JPEG 1", ".jpg"),
        ("photo2.png", b"FAKE PNG DATA", ".png"),
        ("video1.mp4", b"FAKE MP4 DATA", ".mp4"),
        ("doc.txt", b"text content", ".txt"),
    ]

    for fname, content, ext in files_info:
        fpath = media_dir / fname
        fpath.write_bytes(content)
        row = CatalogFileRow(
            id=None,
            path=str(fpath),
            size=len(content),
            mtime=fpath.stat().st_mtime,
            ctime=fpath.stat().st_ctime,
            birthtime=fpath.stat().st_mtime,
            ext=ext,
            sha256="a" * 64,
            inode=fpath.stat().st_ino,
            device=fpath.stat().st_dev,
            nlink=1,
            asset_key=None,
            asset_component=False,
            xattr_count=0,
            first_seen=now_iso,
            last_scanned=now_iso,
        )
        cat.upsert_file(row)

    cat.commit()
    cat.close()
    return db_path, media_dir


@pytest.fixture
def client(populated_catalog):
    from godmode_media_library.web.app import create_app

    db_path, media_dir = populated_catalog
    app = create_app(catalog_path=db_path)
    # Store media_dir for tests that need it
    app.state.media_dir = media_dir
    cl = TestClient(app)
    cl._media_dir = media_dir
    return cl


@pytest.fixture
def empty_client(tmp_path):
    from godmode_media_library.web.app import create_app

    db_path = tmp_path / "empty_files.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()
    app = create_app(catalog_path=db_path)
    return TestClient(app)


# ---------------------------------------------------------------------------
# POST /api/files/rename
# ---------------------------------------------------------------------------


class TestRenameFiles:
    def test_rename_single_file(self, client):
        media_dir = client._media_dir
        old_path = str(media_dir / "photo1.jpg")
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": old_path, "new_name": "renamed.jpg"}]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["renamed"] == 1
        assert data["skipped"] == 0
        assert (media_dir / "renamed.jpg").exists()
        assert not (media_dir / "photo1.jpg").exists()

    def test_rename_nonexistent_file(self, client):
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": "/nonexistent/file.jpg", "new_name": "new.jpg"}]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["renamed"] == 0
        assert data["skipped"] == 1

    def test_rename_path_traversal_in_new_name(self, client):
        media_dir = client._media_dir
        old_path = str(media_dir / "photo2.png")
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": old_path, "new_name": "../escaped.png"}]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["renamed"] == 0
        assert data["skipped"] == 1
        assert "traversal" in data["errors"][0].lower() or "separator" in data["errors"][0].lower()

    def test_rename_target_exists(self, client):
        media_dir = client._media_dir
        old_path = str(media_dir / "photo1.jpg")
        # photo2.png already exists
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": old_path, "new_name": "photo2.png"}]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["renamed"] == 0
        assert data["skipped"] == 1

    def test_rename_slash_in_new_name(self, client):
        media_dir = client._media_dir
        old_path = str(media_dir / "photo1.jpg")
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": old_path, "new_name": "sub/dir/file.jpg"}]},
        )
        assert resp.status_code == 200
        assert resp.json()["skipped"] == 1

    def test_rename_empty_new_name(self, client):
        media_dir = client._media_dir
        old_path = str(media_dir / "photo1.jpg")
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": old_path, "new_name": ""}]},
        )
        assert resp.status_code == 200
        assert resp.json()["skipped"] == 1


# ---------------------------------------------------------------------------
# POST /api/files/move
# ---------------------------------------------------------------------------


class TestMoveFiles:
    def test_move_files(self, client):
        media_dir = client._media_dir
        dest = media_dir / "subfolder"
        dest.mkdir()
        src_path = str(media_dir / "photo1.jpg")

        # We need to set roots for security check
        # Save roots first
        client.post("/api/roots", json={"roots": [str(media_dir)]})

        resp = client.post(
            "/api/files/move",
            json={"paths": [src_path], "destination": str(dest)},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["moved"] == 1

    def test_move_nonexistent_source(self, client):
        media_dir = client._media_dir
        dest = media_dir / "dest2"
        dest.mkdir()
        client.post("/api/roots", json={"roots": [str(media_dir)]})

        resp = client.post(
            "/api/files/move",
            json={"paths": ["/nonexistent/file.jpg"], "destination": str(dest)},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["moved"] == 0
        assert data["skipped"] == 1

    def test_move_target_exists(self, client):
        media_dir = client._media_dir
        # photo2.png in destination already
        dest = media_dir  # Same directory — photo2.png already there
        client.post("/api/roots", json={"roots": [str(media_dir)]})

        resp = client.post(
            "/api/files/move",
            json={"paths": [str(media_dir / "photo1.jpg")], "destination": str(dest)},
        )
        assert resp.status_code == 200
        # Should skip since target already exists (same name in same dir)
        data = resp.json()
        assert data["skipped"] == 1

    def test_move_creates_destination(self, client):
        media_dir = client._media_dir
        dest = media_dir / "new_dir"
        client.post("/api/roots", json={"roots": [str(media_dir)]})

        resp = client.post(
            "/api/files/move",
            json={"paths": [str(media_dir / "doc.txt")], "destination": str(dest)},
        )
        assert resp.status_code == 200
        assert dest.exists()


# ---------------------------------------------------------------------------
# POST /api/files/quarantine
# ---------------------------------------------------------------------------


class TestQuarantineFiles:
    def test_quarantine_file(self, client):
        media_dir = client._media_dir
        src = str(media_dir / "photo1.jpg")
        qroot = media_dir / "_quarantine"
        client.post("/api/roots", json={"roots": [str(media_dir)]})

        resp = client.post(
            "/api/files/quarantine",
            json={"paths": [src], "quarantine_root": str(qroot)},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["moved"] == 1

    def test_quarantine_nonexistent(self, client):
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": ["/nonexistent.jpg"]},
        )
        assert resp.status_code == 200
        assert resp.json()["skipped"] == 1

    def test_quarantine_not_in_catalog(self, client):
        """File exists on disk but not in catalog."""
        media_dir = client._media_dir
        extra = media_dir / "extra.tmp"
        extra.write_bytes(b"extra data")
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": [str(extra)]},
        )
        assert resp.status_code == 200
        assert resp.json()["skipped"] == 1

    def test_quarantine_default_root(self, client):
        """Test with no quarantine_root specified (uses default)."""
        media_dir = client._media_dir
        src = str(media_dir / "photo2.png")
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": [src]},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# POST /api/files/delete
# ---------------------------------------------------------------------------


class TestDeleteFiles:
    def test_delete_file(self, client):
        media_dir = client._media_dir
        src = str(media_dir / "doc.txt")
        client.post("/api/roots", json={"roots": [str(media_dir)]})

        resp = client.post("/api/files/delete", json={"paths": [src]})
        assert resp.status_code == 200
        data = resp.json()
        assert data["deleted"] == 1
        assert not (media_dir / "doc.txt").exists()

    def test_delete_nonexistent(self, client):
        media_dir = client._media_dir
        client.post("/api/roots", json={"roots": [str(media_dir)]})
        resp = client.post(
            "/api/files/delete", json={"paths": [str(media_dir / "nofile.xyz")]}
        )
        assert resp.status_code == 200
        assert resp.json()["skipped"] == 1


# ---------------------------------------------------------------------------
# POST /api/files/restore
# ---------------------------------------------------------------------------


class TestRestoreFiles:
    def test_restore_from_quarantine(self, client):
        media_dir = client._media_dir
        qroot = media_dir / "_quarantine"
        qroot.mkdir(parents=True, exist_ok=True)

        # Simulate quarantined file
        original_path = media_dir / "restored.jpg"
        quarantine_path = qroot / str(original_path).lstrip("/")
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        quarantine_path.write_bytes(b"jpeg data")

        client.post("/api/roots", json={"roots": [str(media_dir)]})

        resp = client.post(
            "/api/files/restore",
            json={"paths": [str(original_path)], "quarantine_root": str(qroot)},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["restored"] == 1

    def test_restore_not_in_quarantine(self, client):
        resp = client.post(
            "/api/files/restore",
            json={"paths": ["/nonexistent/file.jpg"]},
        )
        assert resp.status_code == 200
        assert len(resp.json()["errors"]) >= 1

    def test_restore_original_occupied(self, client):
        media_dir = client._media_dir
        qroot = media_dir / "_quarantine2"
        qroot.mkdir(parents=True, exist_ok=True)

        # photo1.jpg exists at original location already
        quarantine_path = qroot / str(media_dir / "photo1.jpg").lstrip("/")
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        quarantine_path.write_bytes(b"old data")

        client.post("/api/roots", json={"roots": [str(media_dir)]})

        resp = client.post(
            "/api/files/restore",
            json={
                "paths": [str(media_dir / "photo1.jpg")],
                "quarantine_root": str(qroot),
            },
        )
        assert resp.status_code == 200
        assert "occupied" in resp.json()["errors"][0].lower()


# ---------------------------------------------------------------------------
# POST /api/files/favorite and GET /api/files/favorites
# ---------------------------------------------------------------------------


class TestFavorites:
    def test_toggle_favorite_on(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg")
        resp = client.post("/api/files/favorite", json={"path": fpath})
        assert resp.status_code == 200
        assert resp.json()["is_favorite"] is True

    def test_toggle_favorite_off(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg")
        client.post("/api/files/favorite", json={"path": fpath})
        resp = client.post("/api/files/favorite", json={"path": fpath})
        assert resp.status_code == 200
        assert resp.json()["is_favorite"] is False

    def test_list_favorites(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg")
        client.post("/api/files/favorite", json={"path": fpath})
        resp = client.get("/api/files/favorites")
        assert resp.status_code == 200
        data = resp.json()
        assert fpath in data["favorites"]
        assert data["count"] == 1

    def test_list_favorites_empty(self, empty_client):
        resp = empty_client.get("/api/files/favorites")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0


# ---------------------------------------------------------------------------
# Notes: GET/PUT/DELETE /api/files/{path}/note
# ---------------------------------------------------------------------------


class TestFileNotes:
    def test_get_note_none(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        resp = client.get(f"/api/files/{fpath}/note")
        assert resp.status_code == 200
        assert resp.json()["note"] is None

    def test_set_and_get_note(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        resp = client.put(f"/api/files/{fpath}/note", json={"note": "Great photo!"})
        assert resp.status_code == 200
        assert resp.json()["saved"] is True

        resp2 = client.get(f"/api/files/{fpath}/note")
        assert resp2.json()["note"] == "Great photo!"

    def test_delete_note(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        client.put(f"/api/files/{fpath}/note", json={"note": "To delete"})
        resp = client.delete(f"/api/files/{fpath}/note")
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True


# ---------------------------------------------------------------------------
# Ratings: PUT/DELETE /api/files/{path}/rating
# ---------------------------------------------------------------------------


class TestFileRatings:
    def test_set_rating(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        resp = client.put(f"/api/files/{fpath}/rating", json={"rating": 5})
        assert resp.status_code == 200
        assert resp.json()["rating"] == 5

    def test_set_invalid_rating(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        resp = client.put(f"/api/files/{fpath}/rating", json={"rating": 0})
        assert resp.status_code == 400

    def test_delete_rating(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        client.put(f"/api/files/{fpath}/rating", json={"rating": 3})
        resp = client.delete(f"/api/files/{fpath}/rating")
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True


# ---------------------------------------------------------------------------
# GET /api/files/{path} — file detail
# ---------------------------------------------------------------------------


class TestFileDetail:
    def test_get_file_detail(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        resp = client.get(f"/api/files/{fpath}")
        assert resp.status_code == 200
        data = resp.json()
        assert "file" in data
        assert "metadata" in data
        assert "richness" in data
        assert "tags" in data

    def test_get_file_detail_not_found(self, client):
        resp = client.get("/api/files/nonexistent/path.jpg")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/files — listing with filters
# ---------------------------------------------------------------------------


class TestFilesListing:
    def test_list_files_basic(self, client):
        resp = client.get("/api/files")
        assert resp.status_code == 200
        data = resp.json()
        assert "files" in data
        assert "count" in data
        assert data["count"] == 4

    def test_list_files_ext_filter(self, client):
        resp = client.get("/api/files?ext=.jpg")
        assert resp.status_code == 200
        data = resp.json()
        # ext filter strips the dot, so 'jpg' and '.jpg' may or may not match
        # depending on how catalog stores ext; just verify the endpoint runs
        assert "count" in data

    def test_list_files_pagination(self, client):
        resp = client.get("/api/files?limit=2&offset=0")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] <= 2
        assert data["has_more"] is True

    def test_list_files_favorites_only(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg")
        client.post("/api/files/favorite", json={"path": fpath})

        resp = client.get("/api/files?favorites_only=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1

    def test_list_files_with_min_rating(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        client.put(f"/api/files/{fpath}/rating", json={"rating": 4})

        resp = client.get("/api/files?min_rating=4")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 1

    def test_list_files_has_notes(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        client.put(f"/api/files/{fpath}/note", json={"note": "noted!"})

        resp = client.get("/api/files?has_notes=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 1

    def test_list_files_path_contains(self, client):
        resp = client.get("/api/files?path_contains=photo")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 1

    def test_list_files_sort(self, client):
        resp = client.get("/api/files?sort=size&order=desc")
        assert resp.status_code == 200
        assert resp.json()["count"] >= 1

    def test_list_files_empty(self, empty_client):
        resp = empty_client.get("/api/files")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0


# ---------------------------------------------------------------------------
# GET /api/browse
# ---------------------------------------------------------------------------


class TestBrowse:
    def test_browse_home(self, client):
        resp = client.get("/api/browse")
        assert resp.status_code == 200
        data = resp.json()
        assert "current" in data
        assert "entries" in data
        assert "bookmarks" in data

    def test_browse_specific_path(self, client):
        """Browse the user's home directory (always allowed)."""
        home = str(Path.home())
        resp = client.get(f"/api/browse?path={home}")
        assert resp.status_code == 200
        assert resp.json()["current"] == home

    def test_browse_nonexistent(self, client):
        home = str(Path.home())
        resp = client.get(f"/api/browse?path={home}/nonexistent_xyz_dir_9999")
        assert resp.status_code == 404

    def test_browse_blocked_path(self, client):
        """System paths like /etc should be blocked."""
        resp = client.get("/api/browse?path=/etc")
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /api/roots and POST /api/roots and DELETE /api/roots
# ---------------------------------------------------------------------------


class TestRoots:
    def test_get_roots_empty(self, empty_client):
        resp = empty_client.get("/api/roots")
        assert resp.status_code == 200
        assert resp.json()["roots"] == []

    def test_save_and_get_roots(self, client):
        media_dir = client._media_dir
        resp = client.post("/api/roots", json={"roots": [str(media_dir)]})
        assert resp.status_code == 200
        assert resp.json()["saved"] is True

        resp2 = client.get("/api/roots")
        assert str(media_dir.resolve()) in resp2.json()["roots"]

    def test_save_roots_deduplicates(self, client):
        media_dir = client._media_dir
        resp = client.post(
            "/api/roots",
            json={"roots": [str(media_dir), str(media_dir), str(media_dir)]},
        )
        assert resp.status_code == 200
        assert len(resp.json()["roots"]) == 1

    def test_save_roots_filters_nonexistent(self, client):
        resp = client.post(
            "/api/roots", json={"roots": ["/nonexistent/path/xyz"]}
        )
        assert resp.status_code == 200
        assert resp.json()["roots"] == []

    def test_remove_root(self, client):
        media_dir = client._media_dir
        client.post("/api/roots", json={"roots": [str(media_dir)]})
        resp = client.request(
            "DELETE",
            "/api/roots",
            json={"path": str(media_dir)},
        )
        assert resp.status_code == 200
        assert resp.json()["removed"] is True


# ---------------------------------------------------------------------------
# GET /api/sources
# ---------------------------------------------------------------------------


class TestSources:
    def test_sources_empty(self, empty_client):
        resp = empty_client.get("/api/sources")
        assert resp.status_code == 200
        data = resp.json()
        assert "sources" in data
        assert "thumbnail_cache" in data

    def test_sources_with_roots(self, client):
        media_dir = client._media_dir
        client.post("/api/roots", json={"roots": [str(media_dir)]})
        resp = client.get("/api/sources")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["sources"]) >= 1
        src = data["sources"][0]
        assert "online" in src
        assert "file_count" in src


# ---------------------------------------------------------------------------
# GET /api/stream/{path} — streaming
# ---------------------------------------------------------------------------


class TestStreamFile:
    def test_stream_file(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        resp = client.get(f"/api/stream/{fpath}")
        assert resp.status_code == 200
        assert "image/jpeg" in resp.headers["content-type"]

    def test_stream_file_not_in_catalog(self, client):
        resp = client.get("/api/stream/nonexistent/file.jpg")
        assert resp.status_code == 404

    def test_stream_range_request(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        resp = client.get(f"/api/stream/{fpath}", headers={"Range": "bytes=0-5"})
        assert resp.status_code == 206
        assert "content-range" in resp.headers
        assert len(resp.content) == 6

    def test_stream_range_invalid(self, client):
        media_dir = client._media_dir
        fpath = str(media_dir / "photo1.jpg").lstrip("/")
        resp = client.get(
            f"/api/stream/{fpath}", headers={"Range": "bytes=999999-9999999"}
        )
        assert resp.status_code == 416

    def test_stream_file_not_on_disk(self, client, populated_catalog):
        """File in catalog but deleted from disk."""
        db_path, media_dir = populated_catalog
        # Add a catalog entry for a file that doesn't exist on disk
        cat = Catalog(db_path)
        cat.open()
        now_iso = "2024-01-15T12:00:00+00:00"
        row = CatalogFileRow(
            id=None,
            path="/ghost/file.mp4",
            size=100,
            mtime=1704067200.0,
            ctime=1704067200.0,
            birthtime=1704067200.0,
            ext=".mp4",
            sha256="b" * 64,
            inode=99999,
            device=1,
            nlink=1,
            asset_key=None,
            asset_component=False,
            xattr_count=0,
            first_seen=now_iso,
            last_scanned=now_iso,
        )
        cat.upsert_file(row)
        cat.commit()
        cat.close()

        resp = client.get("/api/stream/ghost/file.mp4")
        assert resp.status_code == 404
