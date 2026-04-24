"""Tests for the duplicate and similarity route endpoints.

Covers: GET /api/duplicates, GET /api/duplicates/{group_id},
GET /api/duplicates/{group_id}/diff, GET /api/similar,
POST /api/duplicates/{group_id}/quarantine,
POST /api/duplicates/{group_id}/merge.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
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
# Helpers
# ---------------------------------------------------------------------------


def _insert_file(cat: Catalog, path: str, size: int = 100, sha256: str = "abc123", phash: str | None = None) -> int:
    """Insert a file row directly and return its id."""
    now = datetime.now(timezone.utc).isoformat()
    cur = cat.conn.execute(
        """INSERT INTO files (path, size, mtime, ctime, birthtime, ext, sha256, phash,
                              first_seen, last_scanned)
           VALUES (?, ?, 1.0, 1.0, 1.0, ?, ?, ?, ?, ?)""",
        (path, size, Path(path).suffix, sha256, phash, now, now),
    )
    cat.conn.commit()
    return cur.lastrowid


def _insert_duplicate_group(cat: Catalog, group_id: str, file_ids: list[int]) -> None:
    """Insert a duplicate group with the given file ids."""
    for fid in file_ids:
        cat.conn.execute(
            "INSERT INTO duplicates (group_id, file_id) VALUES (?, ?)",
            (group_id, fid),
        )
    cat.conn.commit()


def _insert_file_metadata(cat: Catalog, file_id: int, meta: dict) -> None:
    """Insert file metadata for a file."""
    now = datetime.now(timezone.utc).isoformat()
    cat.conn.execute(
        "INSERT OR REPLACE INTO file_metadata (file_id, raw_json, extracted_at) VALUES (?, ?, ?)",
        (file_id, json.dumps(meta), now),
    )
    cat.conn.commit()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def empty_db(tmp_path):
    """Return path to an empty catalog DB."""
    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()
    return db_path


@pytest.fixture
def client_empty(empty_db):
    """Test client backed by an empty catalog."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=empty_db)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def db_with_duplicates(tmp_path):
    """Create a catalog with duplicate groups and real files on disk."""
    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()

    # Create real files on disk for quarantine/merge tests
    media = tmp_path / "media"
    media.mkdir()
    file_a = media / "photo_a.jpg"
    file_b = media / "photo_b.jpg"
    file_c = media / "photo_c.jpg"
    file_a.write_bytes(b"content_a")
    file_b.write_bytes(b"content_b")
    file_c.write_bytes(b"content_c")

    id_a = _insert_file(cat, str(file_a), size=9, sha256="deadbeef")
    id_b = _insert_file(cat, str(file_b), size=9, sha256="deadbeef")
    id_c = _insert_file(cat, str(file_c), size=9, sha256="deadbeef")

    _insert_duplicate_group(cat, "group1", [id_a, id_b, id_c])

    # Add metadata for diff tests
    _insert_file_metadata(cat, id_a, {"EXIF:Make": "Canon", "EXIF:Model": "EOS R5"})
    _insert_file_metadata(cat, id_b, {"EXIF:Make": "Canon", "EXIF:Model": "EOS R6"})
    _insert_file_metadata(cat, id_c, {"EXIF:Make": "Canon"})

    cat.close()
    return db_path, tmp_path


@pytest.fixture
def client_with_dups(db_with_duplicates):
    """Test client backed by a catalog with duplicate groups."""
    from godmode_media_library.web.app import create_app

    db_path, _ = db_with_duplicates
    app = create_app(catalog_path=db_path)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def db_with_phashes(tmp_path):
    """Create a catalog with files that have perceptual hashes."""
    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()

    # Two files with identical phash (distance 0) and one different
    _insert_file(cat, "/media/img1.jpg", sha256="aaa", phash="abcdef1234567890")
    _insert_file(cat, "/media/img2.jpg", sha256="bbb", phash="abcdef1234567890")
    _insert_file(cat, "/media/img3.jpg", sha256="ccc", phash="ffffffffffffffff")

    cat.close()
    return db_path


@pytest.fixture
def client_with_phashes(db_with_phashes):
    """Test client backed by a catalog with phash data."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=db_with_phashes)
    return TestClient(app, raise_server_exceptions=False)


# ===========================================================================
# GET /api/duplicates
# ===========================================================================


class TestGetDuplicates:
    def test_empty_catalog(self, client_empty):
        resp = client_empty.get("/api/duplicates")
        assert resp.status_code == 200
        data = resp.json()
        assert data["groups"] == []
        assert data["total_groups"] == 0

    def test_with_groups(self, client_with_dups):
        resp = client_with_dups.get("/api/duplicates")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_groups"] == 1
        assert len(data["groups"]) == 1

        group = data["groups"][0]
        assert group["group_id"] == "group1"
        assert group["file_count"] == 3
        assert group["total_size"] == 27  # 9 * 3
        assert len(group["files"]) == 3

    def test_limit_parameter(self, client_with_dups):
        resp = client_with_dups.get("/api/duplicates?limit=0")
        assert resp.status_code == 200
        data = resp.json()
        assert data["groups"] == []
        # total_groups still reports the real count
        assert data["total_groups"] == 1


# ===========================================================================
# GET /api/duplicates/{group_id}
# ===========================================================================


class TestGetDuplicateGroup:
    def test_found(self, client_with_dups):
        resp = client_with_dups.get("/api/duplicates/group1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["group_id"] == "group1"
        assert len(data["files"]) == 3
        # Each file has path and metadata keys
        for f in data["files"]:
            assert "path" in f
            assert "metadata" in f

    def test_not_found(self, client_with_dups):
        resp = client_with_dups.get("/api/duplicates/nonexistent")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()


# ===========================================================================
# GET /api/duplicates/{group_id}/diff
# ===========================================================================


class TestGetDuplicateDiff:
    def test_found(self, client_with_dups):
        resp = client_with_dups.get("/api/duplicates/group1/diff")
        assert resp.status_code == 200
        data = resp.json()
        assert data["group_id"] == "group1"
        assert "unanimous" in data
        assert "partial" in data
        assert "conflicts" in data
        assert "scores" in data

    def test_not_found(self, client_with_dups):
        resp = client_with_dups.get("/api/duplicates/nonexistent/diff")
        assert resp.status_code == 404

    def test_single_file_group(self, tmp_path):
        """A group with < 2 files returns 404."""
        from godmode_media_library.web.app import create_app

        db_path = tmp_path / "single.db"
        cat = Catalog(db_path)
        cat.open()
        fid = _insert_file(cat, "/media/solo.jpg", sha256="solo")
        _insert_duplicate_group(cat, "solo_group", [fid])
        _insert_file_metadata(cat, fid, {"EXIF:Make": "Nikon"})
        cat.close()

        app = create_app(catalog_path=db_path)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/duplicates/solo_group/diff")
        assert resp.status_code == 404
        assert "< 2" in resp.json()["detail"]


# ===========================================================================
# GET /api/similar
# ===========================================================================


class TestGetSimilar:
    def test_empty_catalog(self, client_empty):
        resp = client_empty.get("/api/similar")
        assert resp.status_code == 200
        data = resp.json()
        assert data["pairs"] == []
        assert data["total_pairs"] == 0

    def test_with_phashes(self, client_with_phashes):
        resp = client_with_phashes.get("/api/similar")
        assert resp.status_code == 200
        data = resp.json()
        # img1 and img2 have identical phash -> distance 0, should be found
        assert data["total_pairs"] >= 1
        found_pair = False
        for p in data["pairs"]:
            assert "path_a" in p
            assert "path_b" in p
            assert "distance" in p
            paths = {p["path_a"], p["path_b"]}
            if "/media/img1.jpg" in paths and "/media/img2.jpg" in paths:
                assert p["distance"] == 0
                found_pair = True
        assert found_pair, "Expected identical-phash pair not found"

    def test_threshold_zero(self, client_with_phashes):
        """threshold=0 should only return exact matches."""
        resp = client_with_phashes.get("/api/similar?threshold=0")
        assert resp.status_code == 200
        data = resp.json()
        for p in data["pairs"]:
            assert p["distance"] == 0

    def test_limit_parameter(self, client_with_phashes):
        resp = client_with_phashes.get("/api/similar?limit=1")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["pairs"]) <= 1


# ===========================================================================
# POST /api/duplicates/{group_id}/quarantine
# ===========================================================================


class TestQuarantineDuplicateGroup:
    def test_success(self, db_with_duplicates):
        """Quarantine all but the keeper."""
        from godmode_media_library.web.app import create_app

        db_path, tmp_path = db_with_duplicates
        keep_path = str(tmp_path / "media" / "photo_a.jpg")

        app = create_app(catalog_path=db_path)
        client = TestClient(app, raise_server_exceptions=False)

        # Patch _DEFAULT_QUARANTINE_ROOT to use temp dir and disk space check
        quarantine_dir = tmp_path / "quarantine"
        with (
            patch(
                "godmode_media_library.web.routes.duplicates._DEFAULT_QUARANTINE_ROOT",
                quarantine_dir,
            ),
            patch(
                "godmode_media_library.web.routes.duplicates.check_disk_space",
                return_value=True,
            ),
        ):
            resp = client.post(
                "/api/duplicates/group1/quarantine",
                json={"keep_path": keep_path},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["quarantined"] == 2
        assert data["kept"] == keep_path
        # Keeper file should still exist
        assert Path(keep_path).exists()

    def test_not_found(self, client_with_dups):
        resp = client_with_dups.post(
            "/api/duplicates/nonexistent/quarantine",
            json={"keep_path": "/some/path.jpg"},
        )
        assert resp.status_code == 404

    def test_keep_path_not_in_group(self, db_with_duplicates):
        """keep_path that doesn't belong to the group returns 400."""
        from godmode_media_library.web.app import create_app

        db_path, _ = db_with_duplicates
        app = create_app(catalog_path=db_path)
        client = TestClient(app, raise_server_exceptions=False)

        resp = client.post(
            "/api/duplicates/group1/quarantine",
            json={"keep_path": "/not/in/group.jpg"},
        )
        assert resp.status_code == 400
        assert "keep_path" in resp.json()["detail"].lower()

    def test_missing_body(self, client_with_dups):
        """POST without body should return 422 (validation error)."""
        resp = client_with_dups.post("/api/duplicates/group1/quarantine")
        assert resp.status_code == 422

    def test_file_not_on_disk(self, tmp_path):
        """Files missing from disk produce errors but don't crash."""
        from godmode_media_library.web.app import create_app

        db_path = tmp_path / "test.db"
        cat = Catalog(db_path)
        cat.open()

        # Insert files that don't exist on disk
        id_a = _insert_file(cat, "/nonexistent/a.jpg", sha256="same")
        id_b = _insert_file(cat, "/nonexistent/b.jpg", sha256="same")
        _insert_duplicate_group(cat, "gX", [id_a, id_b])
        cat.close()

        app = create_app(catalog_path=db_path)
        client = TestClient(app, raise_server_exceptions=False)

        quarantine_dir = tmp_path / "quarantine"
        with patch(
            "godmode_media_library.web.routes.duplicates._DEFAULT_QUARANTINE_ROOT",
            quarantine_dir,
        ):
            resp = client.post(
                "/api/duplicates/gX/quarantine",
                json={"keep_path": "/nonexistent/a.jpg"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["quarantined"] == 0
        assert "errors" in data
        assert len(data["errors"]) == 1

    def test_insufficient_disk_space(self, db_with_duplicates):
        """When disk space is insufficient, files are skipped with errors."""
        from godmode_media_library.web.app import create_app

        db_path, tmp_path = db_with_duplicates
        keep_path = str(tmp_path / "media" / "photo_a.jpg")

        app = create_app(catalog_path=db_path)
        client = TestClient(app, raise_server_exceptions=False)

        quarantine_dir = tmp_path / "quarantine"
        with (
            patch(
                "godmode_media_library.web.routes.duplicates._DEFAULT_QUARANTINE_ROOT",
                quarantine_dir,
            ),
            patch(
                "godmode_media_library.web.routes.duplicates.check_disk_space",
                return_value=False,
            ),
        ):
            resp = client.post(
                "/api/duplicates/group1/quarantine",
                json={"keep_path": keep_path},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["quarantined"] == 0
        assert "errors" in data
        assert any("disk space" in e.lower() for e in data["errors"])


# ===========================================================================
# POST /api/duplicates/{group_id}/merge
# ===========================================================================


class TestMergeDuplicateGroup:
    def test_success(self, db_with_duplicates):
        """Merge metadata and quarantine non-keepers."""
        from godmode_media_library.web.app import create_app

        db_path, tmp_path = db_with_duplicates
        keep_path = str(tmp_path / "media" / "photo_a.jpg")

        app = create_app(catalog_path=db_path)
        client = TestClient(app, raise_server_exceptions=False)

        quarantine_dir = tmp_path / "quarantine"
        with (
            patch(
                "godmode_media_library.web.routes.duplicates._DEFAULT_QUARANTINE_ROOT",
                quarantine_dir,
            ),
            patch(
                "godmode_media_library.web.routes.duplicates.check_disk_space",
                return_value=True,
            ),
        ):
            resp = client.post(
                "/api/duplicates/group1/merge",
                json={"keep_path": keep_path},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert "merged" in data
        assert data["quarantined"] == 2
        assert data["kept"] == keep_path

    def test_not_found(self, client_with_dups):
        resp = client_with_dups.post(
            "/api/duplicates/nonexistent/merge",
            json={"keep_path": "/some/path.jpg"},
        )
        assert resp.status_code == 404

    def test_keep_path_not_in_group(self, db_with_duplicates):
        """keep_path not in the group returns 400."""
        from godmode_media_library.web.app import create_app

        db_path, _ = db_with_duplicates
        app = create_app(catalog_path=db_path)
        client = TestClient(app, raise_server_exceptions=False)

        resp = client.post(
            "/api/duplicates/group1/merge",
            json={"keep_path": "/not/in/group.jpg"},
        )
        assert resp.status_code == 400
        assert "keep_path" in resp.json()["detail"].lower()

    def test_missing_body(self, client_with_dups):
        """POST without body returns 422."""
        resp = client_with_dups.post("/api/duplicates/group1/merge")
        assert resp.status_code == 422

    def test_merge_with_metadata_error(self, db_with_duplicates):
        """If metadata merge raises, errors are captured but quarantine proceeds."""
        from godmode_media_library.web.app import create_app

        db_path, tmp_path = db_with_duplicates
        keep_path = str(tmp_path / "media" / "photo_a.jpg")

        app = create_app(catalog_path=db_path)
        client = TestClient(app, raise_server_exceptions=False)

        quarantine_dir = tmp_path / "quarantine"
        with (
            patch(
                "godmode_media_library.web.routes.duplicates._DEFAULT_QUARANTINE_ROOT",
                quarantine_dir,
            ),
            patch(
                "godmode_media_library.web.routes.duplicates.check_disk_space",
                return_value=True,
            ),
            patch(
                "godmode_media_library.metadata_richness.compute_group_diff",
                side_effect=RuntimeError("diff failed"),
            ),
        ):
            resp = client.post(
                "/api/duplicates/group1/merge",
                json={"keep_path": keep_path},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["merged"] is False
        assert data["quarantined"] == 2

    def test_merge_files_not_on_disk(self, tmp_path):
        """When files are missing from disk, errors are reported."""
        from godmode_media_library.web.app import create_app

        db_path = tmp_path / "test.db"
        cat = Catalog(db_path)
        cat.open()

        id_a = _insert_file(cat, "/ghost/a.jpg", sha256="same")
        id_b = _insert_file(cat, "/ghost/b.jpg", sha256="same")
        _insert_duplicate_group(cat, "gY", [id_a, id_b])
        _insert_file_metadata(cat, id_a, {"EXIF:Make": "Sony"})
        _insert_file_metadata(cat, id_b, {"EXIF:Make": "Sony"})
        cat.close()

        app = create_app(catalog_path=db_path)
        client = TestClient(app, raise_server_exceptions=False)

        quarantine_dir = tmp_path / "quarantine"
        with patch(
            "godmode_media_library.web.routes.duplicates._DEFAULT_QUARANTINE_ROOT",
            quarantine_dir,
        ):
            resp = client.post(
                "/api/duplicates/gY/merge",
                json={"keep_path": "/ghost/a.jpg"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["quarantined"] == 0
        assert "errors" in data


# ===========================================================================
# Edge cases
# ===========================================================================


class TestEdgeCases:
    def test_quarantine_dest_helper(self):
        """_quarantine_dest preserves path structure."""
        from godmode_media_library.web.routes.duplicates import _quarantine_dest

        root = Path("/tmp/quarantine")
        result = _quarantine_dest(root, Path("/Users/me/photos/pic.jpg"))
        assert result == root / "Users/me/photos/pic.jpg"

    def test_quarantine_dest_strips_leading_slash(self):
        from godmode_media_library.web.routes.duplicates import _quarantine_dest

        root = Path("/q")
        result = _quarantine_dest(root, Path("/a/b.jpg"))
        assert str(result) == "/q/a/b.jpg"
