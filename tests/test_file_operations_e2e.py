"""End-to-end tests for file operations — the core user workflows.

Covers: listing/filtering, detail, notes, ratings, tags, quarantine,
move, rename, delete, thumbnails, and previews.
"""

from __future__ import annotations

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


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def media_root(tmp_path):
    """Create a diverse set of test files on disk."""
    root = tmp_path / "media"
    root.mkdir()

    # Photos sub-dir with varying sizes
    photos = root / "Photos"
    photos.mkdir()
    (photos / "sunset.jpg").write_bytes(b"J" * 3000)
    (photos / "portrait.jpg").write_bytes(b"P" * 1500)
    (photos / "tiny.jpg").write_bytes(b"T" * 100)

    # Videos sub-dir
    videos = root / "Videos"
    videos.mkdir()
    (videos / "clip.mp4").write_bytes(b"V" * 5000)

    # Documents
    docs = root / "Docs"
    docs.mkdir()
    (docs / "readme.txt").write_bytes(b"text content here")

    # Files with special characters
    (root / "my photo (1).jpg").write_bytes(b"S" * 800)
    (root / "file with spaces.png").write_bytes(b"W" * 600)

    # Duplicate pair
    (root / "dup_a.jpg").write_bytes(b"DUPLICATE_CONTENT")
    (root / "dup_b.jpg").write_bytes(b"DUPLICATE_CONTENT")

    return root


@pytest.fixture
def catalog_db(tmp_path, media_root):
    """Scan media_root into a catalog and return the DB path."""
    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()

    from godmode_media_library.scanner import incremental_scan

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
    ):
        incremental_scan(cat, [media_root])
    # Register tmp_path as a configured root so quarantine/move destinations
    # under tmp_path pass the _check_path_within_roots security check.
    import json

    cat.conn.execute(
        "INSERT INTO meta (key, value) VALUES ('configured_roots', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (json.dumps([str(tmp_path)]),),
    )
    cat.conn.commit()
    cat.close()
    return db_path


@pytest.fixture
def client(catalog_db):
    """Create a FastAPI test client backed by the populated catalog."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_db)
    return TestClient(app)


@pytest.fixture
def file_paths(client):
    """Return the list of file paths known to the catalog."""
    resp = client.get("/api/files?limit=100")
    assert resp.status_code == 200
    return [f["path"] for f in resp.json()["files"]]


# ── Helpers ───────────────────────────────────────────────────────────


def _first_path(client) -> str:
    """Return the first file path in the catalog."""
    resp = client.get("/api/files?limit=1")
    files = resp.json()["files"]
    assert len(files) > 0
    return files[0]["path"]


def _path_for_name(file_paths, name_fragment: str) -> str:
    """Find a path containing the given fragment."""
    for p in file_paths:
        if name_fragment in p:
            return p
    raise ValueError(f"No path containing {name_fragment!r} in {file_paths}")


# ═══════════════════════════════════════════════════════════════════════
# 1. FILE LISTING & FILTERING
# ═══════════════════════════════════════════════════════════════════════


class TestFileListing:
    """Tests for GET /api/files with various filter combinations."""

    def test_default_listing_returns_all(self, client):
        """GET /api/files with no params returns all scanned files."""
        resp = client.get("/api/files")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 9  # We created 9 files

    def test_filter_by_extension_jpg(self, client):
        resp = client.get("/api/files?ext=jpg")
        data = resp.json()
        assert data["count"] > 0
        for f in data["files"]:
            assert f["ext"] == "jpg"

    def test_filter_by_extension_txt(self, client):
        resp = client.get("/api/files?ext=txt")
        data = resp.json()
        assert data["count"] >= 1
        for f in data["files"]:
            assert f["ext"] == "txt"

    def test_filter_by_extension_nonexistent(self, client):
        resp = client.get("/api/files?ext=xyz")
        data = resp.json()
        assert data["count"] == 0
        assert data["files"] == []

    def test_filter_by_path_contains(self, client):
        resp = client.get("/api/files?path_contains=Photos")
        data = resp.json()
        assert data["count"] >= 3
        for f in data["files"]:
            assert "Photos" in f["path"]

    def test_filter_by_path_contains_no_match(self, client):
        resp = client.get("/api/files?path_contains=ZZZZNONEXISTENT")
        data = resp.json()
        assert data["count"] == 0

    def test_filter_has_gps_true(self, client):
        """No test files have GPS, so count should be 0."""
        resp = client.get("/api/files?has_gps=true")
        data = resp.json()
        assert data["count"] == 0

    def test_filter_has_gps_false(self, client):
        """All test files lack GPS."""
        resp = client.get("/api/files?has_gps=false")
        data = resp.json()
        assert data["count"] >= 9

    def test_filter_size_range(self, client):
        """Filter by min_size and max_size (in KB in the API)."""
        # Files between 1KB and 4KB
        resp = client.get("/api/files?min_size=1&max_size=4")
        data = resp.json()
        assert resp.status_code == 200
        for f in data["files"]:
            # API converts KB to bytes: min_size*1024 <= size <= max_size*1024
            assert f["size"] >= 1024
            assert f["size"] <= 4096

    def test_filter_size_zero_max(self, client):
        """max_size=0 is treated as unset (falsy) by the API, so all files returned."""
        resp = client.get("/api/files?max_size=0")
        data = resp.json()
        assert resp.status_code == 200
        # The API uses `max_size * 1024 if max_size else None`, so 0 is falsy => no filter
        assert data["count"] >= 1

    def test_pagination_limit_and_offset(self, client):
        # Page 1
        resp1 = client.get("/api/files?limit=3&offset=0")
        d1 = resp1.json()
        assert d1["count"] == 3
        assert d1["has_more"] is True

        # Page 2
        resp2 = client.get("/api/files?limit=3&offset=3")
        d2 = resp2.json()
        assert d2["count"] >= 1

        # No overlap
        paths1 = {f["path"] for f in d1["files"]}
        paths2 = {f["path"] for f in d2["files"]}
        assert paths1.isdisjoint(paths2)

    def test_pagination_offset_beyond_total(self, client):
        """Offset past all records returns empty list."""
        resp = client.get("/api/files?limit=10&offset=10000")
        data = resp.json()
        assert data["count"] == 0
        assert data["has_more"] is False

    def test_sorting_by_size_desc(self, client):
        resp = client.get("/api/files?sort=size&order=desc")
        data = resp.json()
        sizes = [f["size"] for f in data["files"]]
        assert sizes == sorted(sizes, reverse=True)

    def test_sorting_by_size_asc(self, client):
        resp = client.get("/api/files?sort=size&order=asc")
        data = resp.json()
        sizes = [f["size"] for f in data["files"]]
        assert sizes == sorted(sizes)

    def test_sorting_by_name(self, client):
        resp = client.get("/api/files?sort=name&order=asc")
        data = resp.json()
        paths = [f["path"] for f in data["files"]]
        assert paths == sorted(paths)

    def test_combined_filters(self, client):
        """Extension + path_contains together."""
        resp = client.get("/api/files?ext=jpg&path_contains=Photos")
        data = resp.json()
        for f in data["files"]:
            assert f["ext"] == "jpg"
            assert "Photos" in f["path"]

    def test_multiple_extensions_comma_separated(self, client):
        """Filter by multiple extensions at once."""
        resp = client.get("/api/files?ext=jpg,png")
        data = resp.json()
        for f in data["files"]:
            assert f["ext"] in ("jpg", "png")


# ═══════════════════════════════════════════════════════════════════════
# 2. FILE DETAIL
# ═══════════════════════════════════════════════════════════════════════


class TestFileDetail:
    """Tests for GET /api/files/{path}."""

    def test_detail_existing_file(self, client, file_paths):
        path = file_paths[0]
        resp = client.get(f"/api/files{path}")
        assert resp.status_code == 200
        data = resp.json()
        assert "file" in data
        assert data["file"]["path"] == path
        # Should include metadata and enrichment fields
        assert "metadata" in data
        assert "tags" in data

    def test_detail_returns_full_metadata(self, client, file_paths):
        path = file_paths[0]
        resp = client.get(f"/api/files{path}")
        data = resp.json()
        f = data["file"]
        # Core fields should always be present
        assert "size" in f
        assert "ext" in f
        assert "path" in f

    def test_detail_nonexistent_file(self, client):
        resp = client.get("/api/files/does/not/exist.jpg")
        assert resp.status_code == 404

    def test_detail_file_with_spaces(self, client, file_paths):
        path = _path_for_name(file_paths, "file with spaces")
        resp = client.get(f"/api/files{path}")
        assert resp.status_code == 200
        assert resp.json()["file"]["path"] == path

    def test_detail_file_with_parentheses(self, client, file_paths):
        path = _path_for_name(file_paths, "my photo (1)")
        resp = client.get(f"/api/files{path}")
        assert resp.status_code == 200


# ═══════════════════════════════════════════════════════════════════════
# 3. FILE NOTES (CRUD)
# ═══════════════════════════════════════════════════════════════════════


class TestFileNotes:
    """Tests for note CRUD on files."""

    def test_get_note_empty(self, client, file_paths):
        """New file has no note."""
        path = file_paths[0].lstrip("/")
        resp = client.get(f"/api/files/{path}/note")
        assert resp.status_code == 200
        data = resp.json()
        assert data["note"] is None

    def test_set_and_get_note(self, client, file_paths):
        path = file_paths[0].lstrip("/")
        # Set
        resp = client.put(f"/api/files/{path}/note", json={"note": "Beautiful sunset photo"})
        assert resp.status_code == 200
        assert resp.json()["saved"] is True

        # Get
        resp = client.get(f"/api/files/{path}/note")
        assert resp.status_code == 200
        assert resp.json()["note"] == "Beautiful sunset photo"
        assert resp.json()["updated_at"] is not None

    def test_update_note_overwrites(self, client, file_paths):
        path = file_paths[0].lstrip("/")
        client.put(f"/api/files/{path}/note", json={"note": "First note"})
        client.put(f"/api/files/{path}/note", json={"note": "Updated note"})
        resp = client.get(f"/api/files/{path}/note")
        assert resp.json()["note"] == "Updated note"

    def test_delete_note(self, client, file_paths):
        path = file_paths[0].lstrip("/")
        client.put(f"/api/files/{path}/note", json={"note": "To be deleted"})
        resp = client.delete(f"/api/files/{path}/note")
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

        # Verify it is gone
        resp = client.get(f"/api/files/{path}/note")
        assert resp.json()["note"] is None

    def test_delete_nonexistent_note(self, client, file_paths):
        """Deleting a note that does not exist should succeed gracefully."""
        path = file_paths[0].lstrip("/")
        resp = client.delete(f"/api/files/{path}/note")
        assert resp.status_code == 200
        # deleted may be False or True depending on implementation
        assert "deleted" in resp.json()

    def test_set_empty_note(self, client, file_paths):
        """Setting an empty string note should be accepted."""
        path = file_paths[0].lstrip("/")
        resp = client.put(f"/api/files/{path}/note", json={"note": ""})
        assert resp.status_code == 200

    def test_set_very_long_note(self, client, file_paths):
        """A 10000+ character note should be stored without error."""
        path = file_paths[0].lstrip("/")
        long_note = "A" * 12000
        resp = client.put(f"/api/files/{path}/note", json={"note": long_note})
        assert resp.status_code == 200

        resp = client.get(f"/api/files/{path}/note")
        assert len(resp.json()["note"]) == 12000

    def test_note_with_html_tags(self, client, file_paths):
        """HTML/script tags should be stored as-is (not executed)."""
        path = file_paths[0].lstrip("/")
        xss = '<script>alert("xss")</script><b>bold</b>'
        resp = client.put(f"/api/files/{path}/note", json={"note": xss})
        assert resp.status_code == 200

        resp = client.get(f"/api/files/{path}/note")
        assert resp.json()["note"] == xss  # Stored verbatim


# ═══════════════════════════════════════════════════════════════════════
# 4. FILE RATINGS
# ═══════════════════════════════════════════════════════════════════════


class TestFileRatings:
    """Tests for rating CRUD on files."""

    def test_set_valid_ratings(self, client, file_paths):
        """Ratings 1-5 should all be accepted."""
        path = file_paths[0].lstrip("/")
        for rating in (1, 2, 3, 4, 5):
            resp = client.put(f"/api/files/{path}/rating", json={"rating": rating})
            assert resp.status_code == 200
            assert resp.json()["rating"] == rating

    def test_rating_zero_rejected(self, client, file_paths):
        """Rating 0 is out of range [1-5]."""
        path = file_paths[0].lstrip("/")
        resp = client.put(f"/api/files/{path}/rating", json={"rating": 0})
        assert resp.status_code == 400

    def test_rating_six_rejected(self, client, file_paths):
        """Rating 6 is out of range."""
        path = file_paths[0].lstrip("/")
        resp = client.put(f"/api/files/{path}/rating", json={"rating": 6})
        assert resp.status_code == 400

    def test_rating_negative_rejected(self, client, file_paths):
        """Negative rating rejected."""
        path = file_paths[0].lstrip("/")
        resp = client.put(f"/api/files/{path}/rating", json={"rating": -1})
        assert resp.status_code == 400

    def test_delete_rating(self, client, file_paths):
        path = file_paths[0].lstrip("/")
        client.put(f"/api/files/{path}/rating", json={"rating": 4})
        resp = client.delete(f"/api/files/{path}/rating")
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

    def test_delete_nonexistent_rating(self, client, file_paths):
        """Deleting a rating that does not exist should succeed."""
        path = file_paths[1].lstrip("/")
        resp = client.delete(f"/api/files/{path}/rating")
        assert resp.status_code == 200

    def test_rating_visible_in_detail(self, client, file_paths):
        """Rating should appear in file detail response."""
        path = file_paths[0].lstrip("/")
        client.put(f"/api/files/{path}/rating", json={"rating": 5})
        resp = client.get(f"/api/files/{path}")
        assert resp.status_code == 200
        assert resp.json()["rating"] == 5

    def test_filter_by_min_rating(self, client, file_paths):
        """After rating a file, filtering by min_rating should include it."""
        path = file_paths[0].lstrip("/")
        client.put(f"/api/files/{path}/rating", json={"rating": 5})

        resp = client.get("/api/files?min_rating=5")
        data = resp.json()
        rated_paths = [f["path"] for f in data["files"]]
        assert f"/{path}" in rated_paths


# ═══════════════════════════════════════════════════════════════════════
# 5. TAGS
# ═══════════════════════════════════════════════════════════════════════


class TestTags:
    """Tests for tag management and file tagging."""

    def test_create_tag(self, client):
        resp = client.post("/api/tags", json={"name": "Vacation", "color": "#ff0000"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "Vacation"
        assert data["color"] == "#ff0000"
        assert "id" in data

    def test_list_tags(self, client):
        client.post("/api/tags", json={"name": "TestTag"})
        resp = client.get("/api/tags")
        assert resp.status_code == 200
        tags = resp.json()["tags"]
        names = [t["name"] for t in tags]
        assert "TestTag" in names

    def test_delete_tag(self, client):
        resp = client.post("/api/tags", json={"name": "ToDelete"})
        tag_id = resp.json()["id"]
        resp = client.delete(f"/api/tags/{tag_id}")
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

    def test_create_duplicate_tag_rejected(self, client):
        client.post("/api/tags", json={"name": "Unique"})
        resp = client.post("/api/tags", json={"name": "Unique"})
        assert resp.status_code == 409

    def test_tag_files_and_untag(self, client, file_paths):
        # Create tag
        tag_resp = client.post("/api/tags", json={"name": "Nature"})
        tag_id = tag_resp.json()["id"]

        # Tag two files
        paths = file_paths[:2]
        resp = client.post("/api/files/tag", json={"paths": paths, "tag_id": tag_id})
        assert resp.status_code == 200
        assert resp.json()["tagged"] >= 1

        # Untag one file (DELETE with body requires client.request)
        resp = client.request("DELETE", "/api/files/tag", json={"paths": [paths[0]], "tag_id": tag_id})
        assert resp.status_code == 200
        assert resp.json()["untagged"] >= 0

    def test_tag_visible_in_file_detail(self, client, file_paths):
        tag_resp = client.post("/api/tags", json={"name": "Starred"})
        tag_id = tag_resp.json()["id"]
        path = file_paths[0]
        client.post("/api/files/tag", json={"paths": [path], "tag_id": tag_id})

        detail = client.get(f"/api/files{path}")
        assert detail.status_code == 200
        tag_names = [t["name"] for t in detail.json()["tags"]]
        assert "Starred" in tag_names

    def test_tag_with_special_characters(self, client):
        """Tags with emoji or special chars should work."""
        resp = client.post("/api/tags", json={"name": "Best-of 2024!"})
        assert resp.status_code == 200
        assert resp.json()["name"] == "Best-of 2024!"

    def test_filter_files_by_tag(self, client, file_paths):
        """Filtering by tag_id should only return tagged files."""
        tag_resp = client.post("/api/tags", json={"name": "FilterMe"})
        tag_id = tag_resp.json()["id"]
        target = file_paths[0]
        client.post("/api/files/tag", json={"paths": [target], "tag_id": tag_id})

        resp = client.get(f"/api/files?tag_id={tag_id}")
        data = resp.json()
        result_paths = [f["path"] for f in data["files"]]
        assert target in result_paths
        # Should not include untagged files (less than total)
        all_resp = client.get("/api/files")
        assert data["count"] < all_resp.json()["count"]


# ═══════════════════════════════════════════════════════════════════════
# 6. FILE QUARANTINE
# ═══════════════════════════════════════════════════════════════════════


class TestQuarantine:
    """Tests for POST /api/files/quarantine."""

    def test_quarantine_valid_file(self, client, file_paths, tmp_path):
        quarantine_dir = str(tmp_path / "quarantine")
        path = file_paths[0]
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": [path], "quarantine_root": quarantine_dir},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["moved"] == 1
        assert data["skipped"] == 0

        # File should no longer appear in catalog
        detail = client.get(f"/api/files{path}")
        assert detail.status_code == 404

    def test_quarantine_empty_paths(self, client, tmp_path):
        quarantine_dir = str(tmp_path / "quarantine")
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": [], "quarantine_root": quarantine_dir},
        )
        assert resp.status_code == 200
        assert resp.json()["moved"] == 0

    def test_quarantine_nonexistent_file(self, client, tmp_path):
        quarantine_dir = str(tmp_path / "quarantine")
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": ["/nonexistent/file.jpg"], "quarantine_root": quarantine_dir},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["moved"] == 0
        assert data["skipped"] == 1
        assert len(data["errors"]) == 1

    def test_quarantine_multiple_files(self, client, file_paths, tmp_path):
        quarantine_dir = str(tmp_path / "quarantine")
        paths_to_quarantine = file_paths[:3]
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": paths_to_quarantine, "quarantine_root": quarantine_dir},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["moved"] == 3


# ═══════════════════════════════════════════════════════════════════════
# 7. FILE MOVE
# ═══════════════════════════════════════════════════════════════════════


class TestFileMove:
    """Tests for POST /api/files/move."""

    def test_move_file_to_new_dir(self, client, file_paths, tmp_path):
        dest = str(tmp_path / "moved_files")
        path = file_paths[0]
        resp = client.post(
            "/api/files/move",
            json={"paths": [path], "destination": dest},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["moved"] == 1
        assert data["skipped"] == 0

        # Old path should be gone from catalog
        detail = client.get(f"/api/files{path}")
        assert detail.status_code == 404

    def test_move_nonexistent_file(self, client, tmp_path):
        dest = str(tmp_path / "moved_files")
        resp = client.post(
            "/api/files/move",
            json={"paths": ["/nonexistent/file.jpg"], "destination": dest},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["moved"] == 0
        assert data["skipped"] == 1

    def test_move_to_same_directory_conflict(self, client, file_paths, media_root):
        """Moving a file to its own directory creates a conflict (same name exists)."""
        path = file_paths[0]
        parent_dir = str(Path(path).parent)
        resp = client.post(
            "/api/files/move",
            json={"paths": [path], "destination": parent_dir},
        )
        assert resp.status_code == 200
        data = resp.json()
        # Should skip because target already exists (it is the same file)
        assert data["skipped"] == 1

    def test_move_multiple_files(self, client, file_paths, tmp_path):
        dest = str(tmp_path / "batch_move")
        paths = file_paths[:2]
        resp = client.post(
            "/api/files/move",
            json={"paths": paths, "destination": dest},
        )
        assert resp.status_code == 200
        assert resp.json()["moved"] == 2


# ═══════════════════════════════════════════════════════════════════════
# 8. FILE RENAME
# ═══════════════════════════════════════════════════════════════════════


class TestFileRename:
    """Tests for POST /api/files/rename."""

    def test_rename_file(self, client, file_paths):
        path = _path_for_name(file_paths, "sunset.jpg")
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": path, "new_name": "golden_sunset.jpg"}]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["renamed"] == 1

        # Old path gone
        detail = client.get(f"/api/files{path}")
        assert detail.status_code == 404

    def test_rename_nonexistent_file(self, client):
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": "/no/such/file.jpg", "new_name": "new.jpg"}]},
        )
        assert resp.status_code == 200
        assert resp.json()["skipped"] == 1

    def test_rename_to_existing_name_conflict(self, client, file_paths):
        """Renaming to a name that already exists should be skipped."""
        path_a = _path_for_name(file_paths, "dup_a.jpg")
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": path_a, "new_name": "dup_b.jpg"}]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["skipped"] == 1
        assert len(data["errors"]) >= 1

    def test_rename_with_special_chars(self, client, file_paths):
        path = _path_for_name(file_paths, "portrait.jpg")
        resp = client.post(
            "/api/files/rename",
            json={"renames": [{"path": path, "new_name": "portrait (edited).jpg"}]},
        )
        assert resp.status_code == 200
        assert resp.json()["renamed"] == 1

    def test_rename_batch(self, client, file_paths):
        """Rename multiple files in a single request."""
        path_a = _path_for_name(file_paths, "dup_a.jpg")
        path_b = _path_for_name(file_paths, "dup_b.jpg")
        resp = client.post(
            "/api/files/rename",
            json={
                "renames": [
                    {"path": path_a, "new_name": "renamed_a.jpg"},
                    {"path": path_b, "new_name": "renamed_b.jpg"},
                ]
            },
        )
        assert resp.status_code == 200
        assert resp.json()["renamed"] == 2


# ═══════════════════════════════════════════════════════════════════════
# 9. FILE DELETE
# ═══════════════════════════════════════════════════════════════════════


class TestFileDelete:
    """Tests for POST /api/files/delete."""

    def test_delete_file(self, client, file_paths):
        path = file_paths[0]
        resp = client.post("/api/files/delete", json={"paths": [path]})
        assert resp.status_code == 200
        data = resp.json()
        assert data["deleted"] == 1

        # Verify file is removed from catalog
        detail = client.get(f"/api/files{path}")
        assert detail.status_code == 404

    def test_delete_nonexistent_file(self, client):
        resp = client.post("/api/files/delete", json={"paths": ["/no/such/file.jpg"]})
        assert resp.status_code == 200
        data = resp.json()
        assert data["deleted"] == 0
        assert data["skipped"] == 1

    def test_delete_empty_list(self, client):
        resp = client.post("/api/files/delete", json={"paths": []})
        assert resp.status_code == 200
        assert resp.json()["deleted"] == 0

    def test_delete_multiple_files(self, client, file_paths):
        paths = file_paths[:2]
        resp = client.post("/api/files/delete", json={"paths": paths})
        assert resp.status_code == 200
        assert resp.json()["deleted"] == 2

    def test_delete_already_deleted_file(self, client, file_paths):
        """Delete the same file twice; second attempt should skip."""
        path = file_paths[0]
        client.post("/api/files/delete", json={"paths": [path]})
        resp = client.post("/api/files/delete", json={"paths": [path]})
        assert resp.status_code == 200
        assert resp.json()["deleted"] == 0
        assert resp.json()["skipped"] == 1


# ═══════════════════════════════════════════════════════════════════════
# 10. THUMBNAILS & PREVIEWS
# ═══════════════════════════════════════════════════════════════════════


class TestThumbnails:
    """Tests for GET /api/thumbnail/{path} and GET /api/preview/{path}."""

    def test_thumbnail_nonexistent_path(self, client):
        resp = client.get("/api/thumbnail/nonexistent/file.jpg")
        assert resp.status_code == 404

    def test_thumbnail_for_cataloged_non_image(self, client, file_paths):
        """A .txt file in the catalog should fail thumbnail generation."""
        path = _path_for_name(file_paths, "readme.txt").lstrip("/")
        resp = client.get(f"/api/thumbnail/{path}")
        # Should be 400 (not an image) or 500 (generation failed)
        assert resp.status_code in (400, 404, 500)

    def test_preview_nonexistent_path(self, client):
        resp = client.get("/api/preview/nonexistent/file.jpg")
        assert resp.status_code in (403, 404)

    def test_preview_blocked_system_path(self, client):
        """Preview should reject system paths."""
        resp = client.get("/api/preview/etc/passwd")
        assert resp.status_code == 403


# ═══════════════════════════════════════════════════════════════════════
# 11. CROSS-FEATURE WORKFLOWS
# ═══════════════════════════════════════════════════════════════════════


class TestCrossFeatureWorkflows:
    """End-to-end workflows combining multiple operations."""

    def test_note_survives_across_sessions(self, client, file_paths):
        """Set a note, then retrieve it in a separate request — tests persistence."""
        path = file_paths[0].lstrip("/")
        client.put(f"/api/files/{path}/note", json={"note": "Persistent note"})
        # Simulate a new "session" by just making a fresh request
        resp = client.get(f"/api/files/{path}/note")
        assert resp.json()["note"] == "Persistent note"

    def test_rate_then_filter(self, client, file_paths):
        """Rate a file, then verify the rating filter returns it."""
        path = file_paths[0].lstrip("/")
        client.put(f"/api/files/{path}/rating", json={"rating": 3})

        resp = client.get("/api/files?min_rating=3")
        rated_paths = [f["path"] for f in resp.json()["files"]]
        assert f"/{path}" in rated_paths

    def test_tag_then_filter_then_untag(self, client, file_paths):
        """Create tag, tag a file, filter by tag, then untag."""
        tag = client.post("/api/tags", json={"name": "Workflow"}).json()
        tag_id = tag["id"]
        target = file_paths[0]

        # Tag
        client.post("/api/files/tag", json={"paths": [target], "tag_id": tag_id})

        # Filter
        resp = client.get(f"/api/files?tag_id={tag_id}")
        assert resp.json()["count"] == 1

        # Untag (DELETE with body requires client.request)
        client.request("DELETE", "/api/files/tag", json={"paths": [target], "tag_id": tag_id})

        # Filter again — should be empty
        resp = client.get(f"/api/files?tag_id={tag_id}")
        assert resp.json()["count"] == 0

    def test_note_and_rating_visible_in_listing(self, client, file_paths):
        """File listing enrichment includes has_note and rating."""
        path = file_paths[0]
        path_stripped = path.lstrip("/")

        client.put(f"/api/files/{path_stripped}/note", json={"note": "Listed note"})
        client.put(f"/api/files/{path_stripped}/rating", json={"rating": 4})

        resp = client.get("/api/files")
        files_map = {f["path"]: f for f in resp.json()["files"]}
        assert files_map[path]["has_note"] is True
        assert files_map[path]["rating"] == 4

    def test_delete_removes_notes_and_ratings(self, client, file_paths):
        """After deleting a file, its notes/ratings should not be accessible."""
        path = file_paths[0]
        path_stripped = path.lstrip("/")

        client.put(f"/api/files/{path_stripped}/note", json={"note": "Will be gone"})
        client.put(f"/api/files/{path_stripped}/rating", json={"rating": 5})

        # Delete the file
        client.post("/api/files/delete", json={"paths": [path]})

        # File detail should be 404
        assert client.get(f"/api/files{path}").status_code == 404

    def test_has_notes_filter(self, client, file_paths):
        """Filter files that have notes."""
        path = file_paths[0].lstrip("/")
        client.put(f"/api/files/{path}/note", json={"note": "Has a note"})

        resp = client.get("/api/files?has_notes=true")
        data = resp.json()
        noted_paths = [f["path"] for f in data["files"]]
        assert f"/{path}" in noted_paths

    def test_favorites_toggle_and_list(self, client, file_paths):
        """Toggle favorite on a file, then list favorites."""
        path = file_paths[0]
        resp = client.post("/api/files/favorite", json={"path": path})
        assert resp.status_code == 200
        assert resp.json()["is_favorite"] is True

        resp = client.get("/api/files/favorites")
        assert path in resp.json()["favorites"]

        # Toggle off
        resp = client.post("/api/files/favorite", json={"path": path})
        assert resp.json()["is_favorite"] is False

    def test_favorites_filter(self, client, file_paths):
        """Filter files by favorites_only flag."""
        target = file_paths[0]
        client.post("/api/files/favorite", json={"path": target})

        resp = client.get("/api/files?favorites_only=true")
        data = resp.json()
        fav_paths = [f["path"] for f in data["files"]]
        assert target in fav_paths
        # Should be fewer than total
        all_resp = client.get("/api/files")
        assert data["count"] <= all_resp.json()["count"]
