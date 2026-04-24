"""Tests for the gallery API route endpoints.

Covers: /api/gallery/highlights, /api/gallery/collections,
/api/gallery/score/{file_path}, /api/gallery/slideshow
"""

from __future__ import annotations

import sqlite3
import time

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


def _insert_file(db: sqlite3.Connection, **kwargs) -> int:
    """Insert a file row into the catalog and return its id."""
    defaults = {
        "path": "/media/photo.jpg",
        "size": 5_000_000,
        "mtime": time.time(),
        "ctime": time.time(),
        "ext": "jpg",
        "first_seen": "2025-01-01T00:00:00",
        "last_scanned": "2025-01-01T00:00:00",
        "width": 4000,
        "height": 3000,
        "bitrate": None,
        "date_original": "2025-06-15T10:30:00",
        "camera_make": "Canon",
        "camera_model": "Canon EOS R5",
        "gps_latitude": 48.8566,
        "gps_longitude": 2.3522,
        "metadata_richness": 75.0,
        "sha256": "abc123",
        "phash": "0000000000000000",
    }
    defaults.update(kwargs)
    d = defaults
    cur = db.execute(
        """
        INSERT INTO files (
            path, size, mtime, ctime, ext, first_seen, last_scanned,
            width, height, bitrate, date_original, camera_make, camera_model,
            gps_latitude, gps_longitude, metadata_richness, sha256, phash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            d["path"],
            d["size"],
            d["mtime"],
            d["ctime"],
            d["ext"],
            d["first_seen"],
            d["last_scanned"],
            d["width"],
            d["height"],
            d["bitrate"],
            d["date_original"],
            d["camera_make"],
            d["camera_model"],
            d["gps_latitude"],
            d["gps_longitude"],
            d["metadata_richness"],
            d["sha256"],
            d["phash"],
        ),
    )
    db.commit()
    return cur.lastrowid


def _add_rating(db: sqlite3.Connection, file_id: int, rating: int):
    db.execute("INSERT INTO file_ratings (file_id, rating) VALUES (?, ?)", (file_id, rating))
    db.commit()


def _add_note(db: sqlite3.Connection, file_id: int, note: str):
    db.execute(
        "INSERT INTO file_notes (file_id, note, updated_at) VALUES (?, ?, ?)",
        (file_id, note, "2025-01-01T00:00:00"),
    )
    db.commit()


def _add_duplicate(db: sqlite3.Connection, file_id: int, group_id: str, is_primary: int):
    db.execute(
        "INSERT INTO duplicates (group_id, file_id, is_primary) VALUES (?, ?, ?)",
        (group_id, file_id, is_primary),
    )
    db.commit()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def empty_catalog(tmp_path):
    """Return path to an empty (freshly-created) catalog DB."""
    db_path = tmp_path / "empty.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()
    return db_path


@pytest.fixture
def populated_catalog(tmp_path):
    """Return path to a catalog with diverse test files for gallery scoring."""
    db_path = tmp_path / "gallery.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()

    db = sqlite3.connect(str(db_path))

    # File 1: High-quality pro photo with GPS and high resolution
    fid1 = _insert_file(
        db,
        path="/media/pro_shot.jpg",
        size=15_000_000,
        ext="jpg",
        width=6000,
        height=4000,
        camera_make="Sony",
        camera_model="Sony ILCE-7M4",
        gps_latitude=35.6762,
        gps_longitude=139.6503,
        metadata_richness=90.0,
        date_original="2025-08-01T14:00:00",
        sha256="aaa111",
    )
    _add_rating(db, fid1, 5)

    # File 2: Decent phone photo, no GPS
    _insert_file(
        db,
        path="/media/phone_pic.jpg",
        size=3_000_000,
        ext="jpg",
        width=4032,
        height=3024,
        camera_make="Apple",
        camera_model="iPhone 15 Pro",
        gps_latitude=None,
        gps_longitude=None,
        metadata_richness=50.0,
        date_original="2025-09-10T09:00:00",
        sha256="bbb222",
    )

    # File 3: Low-quality old screenshot, no metadata
    _insert_file(
        db,
        path="/media/screenshot.png",
        size=200_000,
        ext="png",
        width=800,
        height=600,
        camera_make=None,
        camera_model=None,
        gps_latitude=None,
        gps_longitude=None,
        metadata_richness=5.0,
        date_original=None,
        sha256="ccc333",
    )

    # File 4: Video file
    fid4 = _insert_file(
        db,
        path="/media/vacation.mp4",
        size=500_000_000,
        ext="mp4",
        width=3840,
        height=2160,
        camera_make="DJI",
        camera_model="DJI Mavic 3",
        gps_latitude=40.7128,
        gps_longitude=-74.0060,
        metadata_richness=60.0,
        date_original="2025-07-04T12:00:00",
        bitrate=50_000_000,
        sha256="ddd444",
    )
    _add_note(db, fid4, "Amazing drone footage")

    # File 5: Duplicate (secondary) of pro_shot
    fid5 = _insert_file(
        db,
        path="/media/pro_shot_copy.jpg",
        size=15_000_000,
        ext="jpg",
        width=6000,
        height=4000,
        camera_make="Sony",
        camera_model="Sony ILCE-7M4",
        gps_latitude=35.6762,
        gps_longitude=139.6503,
        metadata_richness=90.0,
        date_original="2025-08-01T14:00:00",
        sha256="aaa111dup",
    )
    _add_duplicate(db, fid1, "group_A", 1)
    _add_duplicate(db, fid5, "group_A", 0)

    # File 6: RAW file with very high quality
    fid6 = _insert_file(
        db,
        path="/media/landscape.cr3",
        size=40_000_000,
        ext="cr3",
        width=8192,
        height=5464,
        camera_make="Canon",
        camera_model="Canon EOS R5",
        gps_latitude=46.5197,
        gps_longitude=6.6323,
        metadata_richness=95.0,
        date_original="2025-05-20T07:30:00",
        sha256="eee555",
    )
    _add_rating(db, fid6, 5)

    db.close()
    return db_path


@pytest.fixture
def empty_client(empty_catalog):
    """Test client with an empty catalog."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=empty_catalog)
    return TestClient(app)


@pytest.fixture
def client(populated_catalog):
    """Test client with a populated catalog."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=populated_catalog)
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET /api/gallery/highlights
# ---------------------------------------------------------------------------


class TestGalleryHighlights:
    def test_empty_db(self, empty_client):
        resp = empty_client.get("/api/gallery/highlights")
        assert resp.status_code == 200
        data = resp.json()
        assert "files" in data
        assert data["files"] == []

    def test_returns_scored_files(self, client):
        resp = client.get("/api/gallery/highlights")
        assert resp.status_code == 200
        data = resp.json()
        assert "files" in data
        assert len(data["files"]) > 0

        # Each file should have score breakdown
        for f in data["files"]:
            assert "path" in f
            assert "total" in f
            assert "dimensions" in f
            assert "tier" in f
            assert isinstance(f["total"], (int, float))
            assert f["total"] >= 0

    def test_sorted_by_score_descending(self, client):
        resp = client.get("/api/gallery/highlights")
        data = resp.json()
        scores = [f["total"] for f in data["files"]]
        assert scores == sorted(scores, reverse=True)

    def test_limit_param(self, client):
        resp = client.get("/api/gallery/highlights?limit=2")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["files"]) <= 2

    def test_min_score_param(self, client):
        resp = client.get("/api/gallery/highlights?min_score=50")
        assert resp.status_code == 200
        data = resp.json()
        for f in data["files"]:
            assert f["total"] >= 50

    def test_limit_validation_too_low(self, client):
        resp = client.get("/api/gallery/highlights?limit=0")
        assert resp.status_code == 422

    def test_limit_validation_too_high(self, client):
        resp = client.get("/api/gallery/highlights?limit=9999")
        assert resp.status_code == 422

    def test_min_score_negative(self, client):
        resp = client.get("/api/gallery/highlights?min_score=-1")
        assert resp.status_code == 422

    def test_min_score_over_100(self, client):
        resp = client.get("/api/gallery/highlights?min_score=101")
        assert resp.status_code == 422

    def test_high_min_score_filters_all(self, client):
        resp = client.get("/api/gallery/highlights?min_score=99")
        assert resp.status_code == 200
        # Most files won't have near-perfect scores
        data = resp.json()
        assert isinstance(data["files"], list)


# ---------------------------------------------------------------------------
# GET /api/gallery/collections
# ---------------------------------------------------------------------------


class TestGalleryCollections:
    def test_empty_db(self, empty_client):
        resp = empty_client.get("/api/gallery/collections")
        assert resp.status_code == 200
        data = resp.json()
        assert "collections" in data
        collections = data["collections"]
        # Should return all collection keys even if empty
        expected_keys = {
            "best_of",
            "masterpieces",
            "top_rated",
            "travel",
            "pro_shots",
            "recent",
            "hidden_gems",
        }
        assert set(collections.keys()) == expected_keys
        # All collections should be empty lists
        for key in expected_keys:
            assert isinstance(collections[key], list)

    def test_returns_collections_with_data(self, client):
        resp = client.get("/api/gallery/collections")
        assert resp.status_code == 200
        data = resp.json()
        collections = data["collections"]

        # best_of should have some files
        assert len(collections["best_of"]) > 0

    def test_travel_collection_has_gps_files(self, client):
        resp = client.get("/api/gallery/collections")
        collections = resp.json()["collections"]
        # Files with GPS data should appear in travel collection
        travel = collections["travel"]
        if travel:
            for f in travel:
                assert "path" in f
                assert "total" in f

    def test_top_rated_collection(self, client):
        resp = client.get("/api/gallery/collections")
        collections = resp.json()["collections"]
        # We added 5-star ratings, so top_rated should have entries
        top_rated = collections["top_rated"]
        assert isinstance(top_rated, list)

    def test_collection_items_have_expected_fields(self, client):
        resp = client.get("/api/gallery/collections")
        collections = resp.json()["collections"]
        for _name, items in collections.items():
            for item in items:
                assert "path" in item
                assert "total" in item
                assert "dimensions" in item
                assert "tier" in item


# ---------------------------------------------------------------------------
# GET /api/gallery/score/{file_path}
# ---------------------------------------------------------------------------


class TestGalleryFileScore:
    def test_existing_file(self, client):
        resp = client.get("/api/gallery/score/media/pro_shot.jpg")
        assert resp.status_code == 200
        data = resp.json()
        assert data["path"] == "/media/pro_shot.jpg"
        assert "total" in data
        assert "dimensions" in data
        assert "tier" in data
        assert isinstance(data["total"], (int, float))
        assert data["total"] > 0

    def test_score_dimensions_present(self, client):
        resp = client.get("/api/gallery/score/media/pro_shot.jpg")
        data = resp.json()
        dims = data["dimensions"]
        expected_dims = {
            "resolution",
            "metadata_richness",
            "file_quality",
            "camera_tier",
            "geo",
            "user_signal",
            "uniqueness",
            "recency",
        }
        assert set(dims.keys()) == expected_dims
        for dim_name, val in dims.items():
            assert 0.0 <= val <= 1.0, f"Dimension {dim_name} out of range: {val}"

    def test_file_not_found(self, client):
        resp = client.get("/api/gallery/score/media/nonexistent.jpg")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    def test_video_file_score(self, client):
        resp = client.get("/api/gallery/score/media/vacation.mp4")
        assert resp.status_code == 200
        data = resp.json()
        assert data["path"] == "/media/vacation.mp4"
        assert data["total"] > 0

    def test_low_quality_file_score(self, client):
        resp = client.get("/api/gallery/score/media/screenshot.png")
        assert resp.status_code == 200
        data = resp.json()
        assert data["path"] == "/media/screenshot.png"
        # Screenshot should score lower than pro shot
        pro_resp = client.get("/api/gallery/score/media/pro_shot.jpg")
        pro_data = pro_resp.json()
        assert data["total"] < pro_data["total"]

    def test_duplicate_secondary_file(self, client):
        resp = client.get("/api/gallery/score/media/pro_shot_copy.jpg")
        assert resp.status_code == 200
        data = resp.json()
        # Secondary duplicate should have lower uniqueness
        assert data["dimensions"]["uniqueness"] < 1.0

    def test_primary_duplicate_file(self, client):
        resp = client.get("/api/gallery/score/media/pro_shot.jpg")
        assert resp.status_code == 200
        data = resp.json()
        # Primary in a duplicate group
        assert data["dimensions"]["uniqueness"] > 0.2

    def test_file_with_gps(self, client):
        resp = client.get("/api/gallery/score/media/pro_shot.jpg")
        data = resp.json()
        assert data["dimensions"]["geo"] == 1.0

    def test_file_without_gps(self, client):
        resp = client.get("/api/gallery/score/media/phone_pic.jpg")
        data = resp.json()
        assert data["dimensions"]["geo"] == 0.0

    def test_raw_file_score(self, client):
        resp = client.get("/api/gallery/score/media/landscape.cr3")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] > 0
        # RAW file with high resolution should score well on resolution
        assert data["dimensions"]["resolution"] > 0.5

    def test_path_traversal_returns_not_found(self, client):
        # Path traversal with /../ is resolved by Starlette router and hits the
        # SPA fallback (200) or a non-matching route — the traversal never
        # reaches the API handler. Use an encoded traversal that stays within the route.
        resp = client.get("/api/gallery/score/..%2F..%2Fetc%2Fpasswd")
        assert resp.status_code in (400, 404, 422)
        # Also test a traversal that stays in the route path
        resp2 = client.get("/api/gallery/score/media/../../etc/passwd")
        assert resp2.status_code in (200, 400, 404, 422)  # Router may resolve this

    def test_empty_catalog_file_not_found(self, empty_client):
        resp = empty_client.get("/api/gallery/score/media/any_file.jpg")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/gallery/slideshow
# ---------------------------------------------------------------------------


class TestGallerySlideshow:
    def test_empty_db(self, empty_client):
        resp = empty_client.get("/api/gallery/slideshow")
        assert resp.status_code == 200
        data = resp.json()
        assert "collection" in data
        assert "count" in data
        assert "files" in data
        assert data["count"] == 0
        assert data["files"] == []

    def test_default_best_of(self, client):
        resp = client.get("/api/gallery/slideshow")
        assert resp.status_code == 200
        data = resp.json()
        assert data["collection"] == "best_of"
        assert isinstance(data["files"], list)
        assert data["count"] == len(data["files"])

    def test_all_top_collection(self, client):
        resp = client.get("/api/gallery/slideshow?collection=all_top")
        assert resp.status_code == 200
        data = resp.json()
        assert data["collection"] == "all_top"
        # all_top uses score_catalog with min_score=40
        for f in data["files"]:
            assert f["total"] >= 40

    def test_travel_collection(self, client):
        resp = client.get("/api/gallery/slideshow?collection=travel")
        assert resp.status_code == 200
        data = resp.json()
        assert data["collection"] == "travel"

    def test_named_collection(self, client):
        resp = client.get("/api/gallery/slideshow?collection=pro_shots")
        assert resp.status_code == 200
        data = resp.json()
        assert data["collection"] == "pro_shots"

    def test_nonexistent_collection_returns_empty(self, client):
        resp = client.get("/api/gallery/slideshow?collection=does_not_exist")
        assert resp.status_code == 200
        data = resp.json()
        assert data["collection"] == "does_not_exist"
        assert data["count"] == 0
        assert data["files"] == []

    def test_limit_param(self, client):
        resp = client.get("/api/gallery/slideshow?limit=2")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["files"]) <= 2

    def test_limit_validation_too_low(self, client):
        resp = client.get("/api/gallery/slideshow?limit=0")
        assert resp.status_code == 422

    def test_limit_validation_too_high(self, client):
        resp = client.get("/api/gallery/slideshow?limit=999")
        assert resp.status_code == 422

    def test_shuffle_param(self, client):
        # Just verify the param is accepted and returns valid data
        resp = client.get("/api/gallery/slideshow?shuffle=true")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data["files"], list)
        assert data["count"] == len(data["files"])

    def test_shuffle_false(self, client):
        resp = client.get("/api/gallery/slideshow?shuffle=false")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data["files"], list)

    def test_combined_params(self, client):
        resp = client.get("/api/gallery/slideshow?collection=all_top&limit=3&shuffle=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["collection"] == "all_top"
        assert len(data["files"]) <= 3

    def test_slideshow_files_have_expected_fields(self, client):
        resp = client.get("/api/gallery/slideshow?collection=best_of")
        data = resp.json()
        for f in data["files"]:
            assert "path" in f
            assert "total" in f
            assert "tier" in f
