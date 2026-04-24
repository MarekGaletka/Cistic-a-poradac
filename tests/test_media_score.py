"""Tests for media_score.py — quality scoring engine."""

from __future__ import annotations

import sqlite3
import time

import pytest

from godmode_media_library.media_score import (
    MediaScore,
    _score_camera,
    _score_file_quality,
    _score_geo,
    _score_recency,
    _score_resolution,
    _score_richness,
    _score_uniqueness,
    _score_user_signal,
    _tier_label,
    score_catalog,
    score_file,
)

# ── _tier_label ──────────────────────────────────────────────────────


class TestTierLabel:
    def test_masterpiece(self):
        assert _tier_label(85) == "masterpiece"
        assert _tier_label(100) == "masterpiece"

    def test_excellent(self):
        assert _tier_label(70) == "excellent"
        assert _tier_label(84.9) == "excellent"

    def test_good(self):
        assert _tier_label(50) == "good"

    def test_average(self):
        assert _tier_label(30) == "average"

    def test_poor(self):
        assert _tier_label(0) == "poor"
        assert _tier_label(29.9) == "poor"


# ── _score_recency (colon replacement) ───────────────────────────────


class TestScoreRecency:
    def test_yyyy_colon_format(self):
        """EXIF dates use colons: '2024:01:15 12:30:00' must be parsed."""
        score = _score_recency("2024:01:15 12:30:00", None)
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0

    def test_iso_format(self):
        score = _score_recency("2024-01-15T12:30:00", None)
        assert 0.0 <= score <= 1.0

    def test_none_date_uses_mtime(self):
        recent_mtime = time.time() - 60  # 1 minute ago
        score = _score_recency(None, recent_mtime)
        assert score == 1.0

    def test_both_none_returns_neutral(self):
        assert _score_recency(None, None) == 0.3

    def test_old_date_low_score(self):
        score = _score_recency("2005:06:01 00:00:00", None)
        assert score <= 0.45

    def test_very_recent_date_high_score(self):
        from datetime import datetime, timedelta

        d = datetime.now() - timedelta(days=30)
        # Use EXIF format (colons in date) which is what _score_recency expects
        recent = d.strftime("%Y:%m:%d %H:%M:%S")
        score = _score_recency(recent, None)
        assert score >= 0.85

    def test_invalid_date_falls_back_to_mtime(self):
        recent_mtime = time.time() - 100
        score = _score_recency("not-a-date", recent_mtime)
        assert score == 1.0  # recent mtime


# ── _score_camera (word boundaries) ─────────────────────────────────


class TestScoreCamera:
    def test_pro_camera_exact(self):
        assert _score_camera("Canon", "EOS R") == 1.0

    def test_iphone_15_pro(self):
        assert _score_camera("Apple", "iPhone 15 Pro") == 0.7

    def test_unknown_camera_neutral(self):
        assert _score_camera(None, None) == 0.3

    def test_known_brand_unknown_model(self):
        assert _score_camera("Canon", "PowerShot SX999") == 0.6

    def test_word_boundary_matching(self):
        """Camera tier lookup uses word-boundary regex matching."""
        # 'nikon d8' key exists with score 1.0 but the regex uses (?<!\S)
        # so "nikon d850" should match "nikon d8" only if d8 is at word start
        score_d850 = _score_camera("Nikon", "D850")
        # Matches known brand "nikon" -> 0.6 if "nikon d8" regex doesn't match
        assert score_d850 >= 0.6

    def test_dji_drone(self):
        assert _score_camera("DJI", "Mavic 3") == 0.8

    def test_gopro(self):
        assert _score_camera("GoPro", "HERO12") == 0.55

    def test_basic_iphone(self):
        assert _score_camera("Apple", "iPhone SE") == 0.3


# ── _score_resolution ────────────────────────────────────────────────


class TestScoreResolution:
    def test_high_res_20mp(self):
        assert _score_resolution(5000, 4000, "jpg") == 1.0

    def test_medium_res_12mp(self):
        assert _score_resolution(4000, 3000, "jpg") == 0.85

    def test_low_res(self):
        # 640*480 = 0.3 MP -> falls in 0.1 bucket (< 0.5 MP)
        assert _score_resolution(640, 480, "jpg") == 0.1

    def test_zero_dimensions_image(self):
        assert _score_resolution(0, 0, "jpg") == 0.0

    def test_zero_dimensions_video(self):
        assert _score_resolution(0, 0, "mp4") == 0.3

    def test_none_dimensions_image(self):
        assert _score_resolution(None, None, "png") == 0.0

    def test_none_dimensions_video(self):
        assert _score_resolution(None, None, "mov") == 0.3


# ── _score_file_quality ──────────────────────────────────────────────


class TestScoreFileQuality:
    def test_video_large_file(self):
        assert _score_file_quality(600 * 1024 * 1024, None, None, "mp4") == 1.0

    def test_video_small_file(self):
        assert _score_file_quality(1 * 1024 * 1024, None, None, "mov") == 0.2

    def test_image_high_bpp(self):
        # RAW-like: 24+ bpp
        size = 24 * 4000 * 3000 // 8  # 24 bpp
        assert _score_file_quality(size, 4000, 3000, "cr2") == 1.0

    def test_image_zero_dimensions(self):
        assert _score_file_quality(10000, 0, 0, "jpg") == 0.3


# ── _score_user_signal ───────────────────────────────────────────────


class TestScoreUserSignal:
    def test_max_rating(self):
        score = _score_user_signal(5, False, 0, False)
        assert score == pytest.approx(0.6)

    def test_favorite_adds(self):
        score = _score_user_signal(None, True, 0, False)
        assert score == pytest.approx(0.25)

    def test_combined_capped_at_one(self):
        score = _score_user_signal(5, True, 10, True)
        assert score == 1.0

    def test_no_signal(self):
        assert _score_user_signal(None, False, 0, False) == 0.0


# ── _score_uniqueness, _score_geo, _score_richness ───────────────────


class TestMiscScorers:
    def test_uniqueness_no_group(self):
        assert _score_uniqueness(None, None) == 1.0

    def test_uniqueness_primary(self):
        assert _score_uniqueness(1, True) == 0.8

    def test_uniqueness_duplicate(self):
        assert _score_uniqueness(1, False) == 0.2

    def test_geo_with_coords(self):
        assert _score_geo(49.0, 14.0) == 1.0

    def test_geo_without_coords(self):
        assert _score_geo(None, None) == 0.0

    def test_richness_normal(self):
        assert _score_richness(40.0) == pytest.approx(0.5)

    def test_richness_capped(self):
        assert _score_richness(200.0) == 1.0

    def test_richness_none(self):
        assert _score_richness(None) == 0.0


# ── score_file (overall) ─────────────────────────────────────────────


class TestScoreFile:
    def test_basic_scoring(self):
        row = {
            "path": "/photos/test.jpg",
            "ext": "jpg",
            "size": 5_000_000,
            "width": 4000,
            "height": 3000,
            "camera_make": "Canon",
            "camera_model": "EOS R",
            "gps_latitude": 49.0,
            "gps_longitude": 14.0,
            "metadata_richness": 60.0,
            "rating": 4,
            "is_favorite": True,
            "tag_count": 3,
            "has_note": True,
            "duplicate_group_id": None,
            "is_primary": None,
            "date_original": "2024:06:15 10:00:00",
            "mtime": None,
        }
        ms = score_file(row)
        assert isinstance(ms, MediaScore)
        assert 0 <= ms.total <= 100
        assert ms.tier in ("masterpiece", "excellent", "good", "average", "poor")
        assert ms.path == "/photos/test.jpg"

    def test_minimal_row(self):
        row = {"path": "/x.jpg"}
        ms = score_file(row)
        assert ms.total >= 0

    def test_to_dict(self):
        ms = MediaScore(path="/a.jpg", total=72.5, dimensions={"resolution": 0.85}, tier="excellent")
        d = ms.to_dict()
        assert d["total"] == 72.5
        assert d["tier"] == "excellent"


# ── score_catalog (with mock sqlite) ─────────────────────────────────


class TestScoreCatalog:
    def test_score_catalog_basic(self, tmp_path):
        db_path = tmp_path / "catalog.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
            CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                path TEXT, ext TEXT, size INTEGER, mtime REAL,
                width INTEGER, height INTEGER, bitrate INTEGER,
                date_original TEXT, camera_make TEXT, camera_model TEXT,
                gps_latitude REAL, gps_longitude REAL,
                metadata_richness REAL, sha256 TEXT, phash TEXT
            );
            CREATE TABLE duplicates (file_id INTEGER, group_id INTEGER, is_primary INTEGER);
            CREATE TABLE file_ratings (file_id INTEGER, rating INTEGER);
            CREATE TABLE file_notes (file_id INTEGER, note TEXT);
            CREATE TABLE file_tags (file_id INTEGER, tag_id INTEGER);
            INSERT INTO files (id, path, ext, size, width, height)
                VALUES (1, '/test.jpg', 'jpg', 5000000, 4000, 3000);
        """)
        conn.commit()
        conn.close()

        results = score_catalog(db_path, limit=10)
        assert len(results) == 1
        assert results[0].path == "/test.jpg"

    def test_score_catalog_empty(self, tmp_path):
        db_path = tmp_path / "catalog.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
            CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                path TEXT, ext TEXT, size INTEGER, mtime REAL,
                width INTEGER, height INTEGER, bitrate INTEGER,
                date_original TEXT, camera_make TEXT, camera_model TEXT,
                gps_latitude REAL, gps_longitude REAL,
                metadata_richness REAL, sha256 TEXT, phash TEXT
            );
            CREATE TABLE duplicates (file_id INTEGER, group_id INTEGER, is_primary INTEGER);
            CREATE TABLE file_ratings (file_id INTEGER, rating INTEGER);
            CREATE TABLE file_notes (file_id INTEGER, note TEXT);
            CREATE TABLE file_tags (file_id INTEGER, tag_id INTEGER);
        """)
        conn.commit()
        conn.close()

        results = score_catalog(db_path, limit=10)
        assert results == []

    def test_score_catalog_min_score_filter(self, tmp_path):
        db_path = tmp_path / "catalog.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
            CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                path TEXT, ext TEXT, size INTEGER, mtime REAL,
                width INTEGER, height INTEGER, bitrate INTEGER,
                date_original TEXT, camera_make TEXT, camera_model TEXT,
                gps_latitude REAL, gps_longitude REAL,
                metadata_richness REAL, sha256 TEXT, phash TEXT
            );
            CREATE TABLE duplicates (file_id INTEGER, group_id INTEGER, is_primary INTEGER);
            CREATE TABLE file_ratings (file_id INTEGER, rating INTEGER);
            CREATE TABLE file_notes (file_id INTEGER, note TEXT);
            CREATE TABLE file_tags (file_id INTEGER, tag_id INTEGER);
            INSERT INTO files (id, path, ext, size, width, height)
                VALUES (1, '/tiny.jpg', 'jpg', 100, 10, 10);
        """)
        conn.commit()
        conn.close()

        results = score_catalog(db_path, min_score=90)
        assert len(results) == 0


# ── Additional coverage tests ──────────────────────────────────────


class TestScoreResolutionEdgeCases:
    """Cover resolution thresholds at 4MP, 2MP, 0.5MP boundaries."""

    def test_4mp(self):
        # 2000*2000 = 4 MP
        assert _score_resolution(2000, 2000, "jpg") == 0.55

    def test_2mp(self):
        # ~2 MP
        assert _score_resolution(1600, 1250, "jpg") == 0.4

    def test_half_mp(self):
        # 1000*500 = 0.5 MP
        assert _score_resolution(1000, 500, "jpg") == 0.25

    def test_8mp(self):
        # 4000*2000 = 8 MP exactly
        assert _score_resolution(4000, 2000, "jpg") == 0.7


class TestScoreFileQualityEdgeCases:
    """Cover video size thresholds and image bpp thresholds."""

    def test_video_100mb(self):
        assert _score_file_quality(100 * 1024 * 1024, None, None, "mp4") == 0.8

    def test_video_20mb(self):
        assert _score_file_quality(20 * 1024 * 1024, None, None, "mov") == 0.6

    def test_video_5mb(self):
        assert _score_file_quality(5 * 1024 * 1024, None, None, "avi") == 0.4

    def test_image_8bpp(self):
        # 8 bpp => size = 8 * W * H / 8 = W * H
        size = 4000 * 3000  # 8 bpp
        assert _score_file_quality(size, 4000, 3000, "jpg") == 0.85

    def test_image_4bpp(self):
        size = 4 * 4000 * 3000 // 8  # 4 bpp
        assert _score_file_quality(size, 4000, 3000, "jpg") == 0.7

    def test_image_2bpp(self):
        size = 2 * 4000 * 3000 // 8  # 2 bpp
        assert _score_file_quality(size, 4000, 3000, "jpg") == 0.55

    def test_image_1bpp(self):
        size = 1 * 4000 * 3000 // 8  # 1 bpp
        assert _score_file_quality(size, 4000, 3000, "jpg") == 0.4

    def test_image_low_bpp(self):
        # 0.5 bpp
        size = 4000 * 3000 // 16
        assert _score_file_quality(size, 4000, 3000, "jpg") == 0.2

    def test_image_none_width(self):
        assert _score_file_quality(10000, None, 3000, "jpg") == 0.3


class TestScoreCameraEdgeCases:
    """Cover unknown brand fallback and more word boundary cases."""

    def test_completely_unknown_device(self):
        assert _score_camera("SomeRandomBrand", "Model999") == 0.35

    def test_make_only(self):
        assert _score_camera("Sony", None) == 0.6

    def test_model_only(self):
        assert _score_camera(None, "iPhone 15 Pro") == 0.7

    def test_hasselblad(self):
        assert _score_camera("Hasselblad", "X2D") == 1.0

    def test_leica(self):
        assert _score_camera("Leica", "M11") == 0.95

    def test_iphone_se_word_boundary(self):
        """iPhone SE should not match iPhone 15 Pro."""
        assert _score_camera("Apple", "iPhone SE") == 0.3

    def test_fujifilm_unknown_model(self):
        assert _score_camera("Fujifilm", "SomeModel") == 0.6


class TestScoreRecencyEdgeCases:
    """Cover recency age brackets."""

    def test_two_year_old(self):
        # ~1.5 years ago
        from datetime import datetime, timedelta

        d = datetime.now() - timedelta(days=550)
        date_str = d.strftime("%Y:%m:%d %H:%M:%S")
        score = _score_recency(date_str, None)
        assert score == 0.85

    def test_five_year_old(self):
        from datetime import datetime, timedelta

        d = datetime.now() - timedelta(days=4 * 365)
        date_str = d.strftime("%Y:%m:%d %H:%M:%S")
        score = _score_recency(date_str, None)
        assert score == 0.65

    def test_eight_year_old(self):
        from datetime import datetime, timedelta

        d = datetime.now() - timedelta(days=8 * 365)
        date_str = d.strftime("%Y:%m:%d %H:%M:%S")
        score = _score_recency(date_str, None)
        assert score == 0.45

    def test_z_suffix_iso(self):
        """Test that Z timezone suffix is handled."""
        score = _score_recency("2024:01:15 12:30:00Z", None)
        assert 0.0 <= score <= 1.0


class TestScoreFileExtFromPath:
    """Cover ext extraction from path when ext key is missing."""

    def test_ext_from_path(self):
        row = {"path": "/photos/test.cr2", "size": 50_000_000, "width": 6000, "height": 4000}
        ms = score_file(row)
        assert ms.total > 0
        # Should detect cr2 as image extension

    def test_ext_with_dot_prefix(self):
        row = {"path": "/test.jpg", "ext": ".jpg"}
        ms = score_file(row)
        assert ms.total >= 0


class TestScoreCatalogHeapOverflow:
    """Cover the heap replacement path when more files than limit."""

    def test_heap_replacement(self, tmp_path):
        db_path = tmp_path / "catalog.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
            CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                path TEXT, ext TEXT, size INTEGER, mtime REAL,
                width INTEGER, height INTEGER, bitrate INTEGER,
                date_original TEXT, camera_make TEXT, camera_model TEXT,
                gps_latitude REAL, gps_longitude REAL,
                metadata_richness REAL, sha256 TEXT, phash TEXT
            );
            CREATE TABLE duplicates (file_id INTEGER, group_id INTEGER, is_primary INTEGER);
            CREATE TABLE file_ratings (file_id INTEGER, rating INTEGER);
            CREATE TABLE file_notes (file_id INTEGER, note TEXT);
            CREATE TABLE file_tags (file_id INTEGER, tag_id INTEGER);
        """)
        # Insert 5 files with varying quality
        for i in range(5):
            conn.execute(
                "INSERT INTO files (id, path, ext, size, width, height, metadata_richness) VALUES (?, ?, 'jpg', ?, ?, ?, ?)",
                (i + 1, f"/photo_{i}.jpg", (i + 1) * 1_000_000, (i + 1) * 1000, (i + 1) * 750, i * 20.0),
            )
        conn.commit()
        conn.close()

        # Limit to 2 — should trigger heap replacement
        results = score_catalog(db_path, limit=2)
        assert len(results) == 2
        # Results should be sorted descending by score
        assert results[0].total >= results[1].total

    def test_media_only_false(self, tmp_path):
        db_path = tmp_path / "catalog.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
            CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                path TEXT, ext TEXT, size INTEGER, mtime REAL,
                width INTEGER, height INTEGER, bitrate INTEGER,
                date_original TEXT, camera_make TEXT, camera_model TEXT,
                gps_latitude REAL, gps_longitude REAL,
                metadata_richness REAL, sha256 TEXT, phash TEXT
            );
            CREATE TABLE duplicates (file_id INTEGER, group_id INTEGER, is_primary INTEGER);
            CREATE TABLE file_ratings (file_id INTEGER, rating INTEGER);
            CREATE TABLE file_notes (file_id INTEGER, note TEXT);
            CREATE TABLE file_tags (file_id INTEGER, tag_id INTEGER);
            INSERT INTO files (id, path, ext, size, width, height)
                VALUES (1, '/doc.pdf', 'pdf', 5000, NULL, NULL);
        """)
        conn.commit()
        conn.close()

        results = score_catalog(db_path, media_only=False, limit=10)
        assert len(results) == 1
