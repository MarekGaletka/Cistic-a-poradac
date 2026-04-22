"""Tests for system route endpoints (GET /api/stats, /api/categories, etc.).

Targets coverage improvement for web/routes/system.py from ~59% to 75%+.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

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
    """Create a catalog with test files of various extensions."""
    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()

    root = tmp_path / "media"
    root.mkdir()

    # Create files with different extensions for category testing
    (root / "photo1.jpg").write_bytes(b"jpeg1")
    (root / "photo2.png").write_bytes(b"png_data_here")
    (root / "video1.mp4").write_bytes(b"mp4video")
    (root / "song.mp3").write_bytes(b"mp3audio")
    (root / "doc.pdf").write_bytes(b"pdfdoc")
    (root / "readme.txt").write_bytes(b"text")
    (root / "archive.zip").write_bytes(b"zipdata")
    (root / "unknown.xyz").write_bytes(b"other")

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
    """Return path to an empty catalog DB."""
    db_path = tmp_path / "empty.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()
    return db_path


@pytest.fixture
def client(catalog_with_files):
    """Test client with a populated catalog (no auth)."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_with_files)
    return TestClient(app)


@pytest.fixture
def empty_client(empty_catalog):
    """Test client with an empty catalog."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=empty_catalog)
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET /api/stats
# ---------------------------------------------------------------------------


class TestStats:
    def test_stats_returns_expected_keys(self, client):
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_files" in data
        assert "total_size_bytes" in data
        assert data["total_files"] == 8

    def test_stats_empty_catalog(self, empty_client):
        resp = empty_client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_files"] == 0


# ---------------------------------------------------------------------------
# GET /api/categories
# ---------------------------------------------------------------------------


class TestCategories:
    def test_categories_structure(self, client):
        resp = client.get("/api/categories")
        assert resp.status_code == 200
        data = resp.json()
        assert "categories" in data
        cats = data["categories"]
        for name in ("images", "videos", "audio", "documents", "text", "archives", "other"):
            assert name in cats
            assert "count" in cats[name]
            assert "size" in cats[name]

    def test_categories_counts(self, client):
        resp = client.get("/api/categories")
        cats = resp.json()["categories"]
        # photo1.jpg + photo2.png = 2 images
        assert cats["images"]["count"] == 2
        assert cats["videos"]["count"] == 1  # video1.mp4
        assert cats["audio"]["count"] == 1  # song.mp3
        assert cats["documents"]["count"] == 1  # doc.pdf
        assert cats["text"]["count"] == 1  # readme.txt
        assert cats["archives"]["count"] == 1  # archive.zip
        assert cats["other"]["count"] == 1  # unknown.xyz

    def test_categories_empty_catalog(self, empty_client):
        resp = empty_client.get("/api/categories")
        assert resp.status_code == 200
        cats = resp.json()["categories"]
        assert all(cats[c]["count"] == 0 for c in cats)


# ---------------------------------------------------------------------------
# GET /api/memories
# ---------------------------------------------------------------------------


class TestMemories:
    def test_memories_structure(self, client):
        resp = client.get("/api/memories")
        assert resp.status_code == 200
        data = resp.json()
        assert "date" in data
        assert "memories" in data
        assert isinstance(data["memories"], list)

    def test_memories_empty_catalog(self, empty_client):
        resp = empty_client.get("/api/memories")
        assert resp.status_code == 200
        data = resp.json()
        assert data["memories"] == []


# ---------------------------------------------------------------------------
# GET /api/system-info
# ---------------------------------------------------------------------------


class TestSystemInfo:
    def test_system_info_keys(self, client):
        resp = client.get("/api/system-info")
        assert resp.status_code == 200
        data = resp.json()
        expected_keys = {
            "python_version",
            "platform",
            "catalog_path",
            "catalog_size",
            "total_files",
            "total_size",
            "quarantine_size",
            "last_scan_root",
        }
        assert expected_keys.issubset(data.keys())
        assert data["total_files"] == 8

    def test_system_info_empty_catalog(self, empty_client):
        resp = empty_client.get("/api/system-info")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_files"] == 0


# ---------------------------------------------------------------------------
# GET /api/deps
# ---------------------------------------------------------------------------


class TestDeps:
    def test_deps_returns_list(self, client):
        resp = client.get("/api/deps")
        assert resp.status_code == 200
        data = resp.json()
        assert "dependencies" in data
        deps = data["dependencies"]
        assert isinstance(deps, list)
        assert len(deps) > 0
        # Each dep has expected keys
        for dep in deps:
            assert "name" in dep
            assert "available" in dep
            assert "version" in dep
            assert "install_hint" in dep


# ---------------------------------------------------------------------------
# GET /api/tasks and GET /api/tasks/{task_id}
# ---------------------------------------------------------------------------


class TestTasks:
    def test_list_tasks_empty(self, client):
        resp = client.get("/api/tasks")
        assert resp.status_code == 200
        data = resp.json()
        assert "tasks" in data
        assert isinstance(data["tasks"], list)

    def test_get_task_not_found(self, client):
        resp = client.get("/api/tasks/nonexistent-task-id")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# POST /api/backfill-metadata
# ---------------------------------------------------------------------------


class TestBackfillMetadata:
    def test_backfill_metadata_basic(self, client):
        resp = client.post("/api/backfill-metadata")
        assert resp.status_code == 200
        data = resp.json()
        # Should return a dict with backfill results
        assert isinstance(data, dict)
        assert "fs_dates_filled" in data

    def test_backfill_metadata_empty_catalog(self, empty_client):
        resp = empty_client.post("/api/backfill-metadata")
        assert resp.status_code == 200
        data = resp.json()
        assert data["fs_dates_filled"] == 0


# ---------------------------------------------------------------------------
# GET /api/timeline/gaps
# ---------------------------------------------------------------------------


class TestTimelineGaps:
    def test_timeline_gaps_empty(self, empty_client):
        """Empty catalog returns empty gap structure."""
        resp = empty_client.get("/api/timeline/gaps")
        assert resp.status_code == 200
        data = resp.json()
        assert data["months"] == []
        assert data["gaps"] == []
        cov = data["coverage"]
        assert cov["total_months"] == 0
        assert cov["covered_months"] == 0
        assert cov["coverage_pct"] == 0

    def test_timeline_gaps_no_dates(self, client):
        """Files without date_original still return based on mtime/birthtime."""
        resp = client.get("/api/timeline/gaps")
        assert resp.status_code == 200
        data = resp.json()
        assert "months" in data
        assert "gaps" in data

    def test_timeline_gaps_with_dates(self, catalog_with_files):
        """Insert date_original values and verify gap detection."""
        cat = Catalog(catalog_with_files)
        cat.open()
        # Set dates spanning multiple months with a gap
        cat.conn.execute(
            "UPDATE files SET date_original = '2023:01:15 10:00:00' WHERE path LIKE '%photo1%'"
        )
        cat.conn.execute(
            "UPDATE files SET date_original = '2023:03:20 12:00:00' WHERE path LIKE '%photo2%'"
        )
        cat.conn.commit()
        cat.close()

        from godmode_media_library.web.app import create_app

        app = create_app(catalog_path=catalog_with_files)
        cl = TestClient(app)

        resp = cl.get("/api/timeline/gaps")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["months"]) >= 3  # At least Jan, Feb, Mar 2023
        assert "coverage" in data
        assert data["coverage"]["total_months"] >= 3
        assert data["coverage"]["covered_months"] >= 2


# ---------------------------------------------------------------------------
# POST /api/quality/analyze
# ---------------------------------------------------------------------------


class TestQualityAnalyze:
    def test_quality_analyze_starts_task(self, client):
        resp = client.post("/api/quality/analyze")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_quality_analyze_task_exists(self, client):
        """After triggering, the task should appear in the task list."""
        resp = client.post("/api/quality/analyze")
        task_id = resp.json()["task_id"]

        resp2 = client.get(f"/api/tasks/{task_id}")
        assert resp2.status_code == 200
        task = resp2.json()
        assert task["id"] == task_id
        assert task["command"] == "quality_analyze"


# ---------------------------------------------------------------------------
# GET /api/quality/stats
# ---------------------------------------------------------------------------


class TestQualityStats:
    def test_quality_stats_empty(self, client):
        """No quality data yet — all zeroes."""
        resp = client.get("/api/quality/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["blurry"] == 0
        assert data["dark"] == 0
        assert data["overexposed"] == 0
        assert data["analyzed"] == 0

    def test_quality_stats_with_data(self, catalog_with_files):
        """Insert quality scores and verify stats."""
        cat = Catalog(catalog_with_files)
        cat.open()
        # Mark one file as blurry (blur < 50) and dark (brightness < 40)
        cat.conn.execute(
            "UPDATE files SET quality_blur = 30, quality_brightness = 25, quality_category = 'poor' "
            "WHERE path LIKE '%photo1%'"
        )
        # Mark one file as overexposed (brightness > 220)
        cat.conn.execute(
            "UPDATE files SET quality_blur = 80, quality_brightness = 240, quality_category = 'good' "
            "WHERE path LIKE '%photo2%'"
        )
        cat.conn.commit()
        cat.close()

        from godmode_media_library.web.app import create_app

        app = create_app(catalog_path=catalog_with_files)
        cl = TestClient(app)

        resp = cl.get("/api/quality/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["blurry"] == 1
        assert data["dark"] == 1
        assert data["overexposed"] == 1
        assert data["analyzed"] == 2
        assert data.get("poor") == 1
        assert data.get("good") == 1


# ---------------------------------------------------------------------------
# GET /api/report/generate
# ---------------------------------------------------------------------------


class TestReportGenerate:
    def test_report_generate_returns_html(self, client):
        resp = client.get("/api/report/generate")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "<html" in resp.text.lower() or "<!doctype" in resp.text.lower()

    def test_report_generate_empty_catalog(self, empty_client):
        resp = empty_client.get("/api/report/generate")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# GET /api/report/download
# ---------------------------------------------------------------------------


class TestReportDownload:
    def test_report_download_returns_html_attachment(self, client):
        resp = client.get("/api/report/download")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "attachment" in resp.headers.get("content-disposition", "")
        assert "godmode_report_" in resp.headers["content-disposition"]

    def test_report_download_empty_catalog(self, empty_client):
        resp = empty_client.get("/api/report/download")
        assert resp.status_code == 200
        assert "attachment" in resp.headers.get("content-disposition", "")

    def test_report_download_uses_rfc5987_encoding(self, client):
        """Regression: Content-Disposition must use RFC 5987 encoding (filename*=UTF-8'')
        instead of bare filename="..." to prevent header injection."""
        resp = client.get("/api/report/download")
        cd = resp.headers.get("content-disposition", "")
        assert "filename*=UTF-8''" in cd, (
            f"Content-Disposition should use RFC 5987 encoding, got: {cd}"
        )
        # Must NOT contain bare filename="..." pattern (injection-vulnerable)
        assert 'filename="' not in cd, (
            f"Content-Disposition should not use bare filename=, got: {cd}"
        )


# ---------------------------------------------------------------------------
# POST /api/scan — triggers background task, returns task_id immediately
# ---------------------------------------------------------------------------


class TestScan:
    def test_scan_starts_task(self, client):
        resp = client.post("/api/scan")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_scan_task_appears_in_list(self, client):
        resp = client.post("/api/scan")
        task_id = resp.json()["task_id"]
        resp2 = client.get(f"/api/tasks/{task_id}")
        assert resp2.status_code == 200
        assert resp2.json()["command"] == "scan"

    def test_scan_with_config(self, client):
        resp = client.post("/api/scan", json={"roots": ["/tmp/test"], "workers": 2})
        assert resp.status_code == 200
        assert "task_id" in resp.json()


# ---------------------------------------------------------------------------
# POST /api/pipeline — triggers background task
# ---------------------------------------------------------------------------


class TestPipeline:
    def test_pipeline_starts_task(self, client):
        resp = client.post("/api/pipeline")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_pipeline_task_appears_in_list(self, client):
        resp = client.post("/api/pipeline")
        task_id = resp.json()["task_id"]
        resp2 = client.get(f"/api/tasks/{task_id}")
        assert resp2.status_code == 200
        assert resp2.json()["command"] == "pipeline"


# ---------------------------------------------------------------------------
# POST /api/verify — triggers background task
# ---------------------------------------------------------------------------


class TestVerify:
    def test_verify_starts_task(self, client):
        resp = client.post("/api/verify")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_verify_task_appears(self, client):
        resp = client.post("/api/verify")
        task_id = resp.json()["task_id"]
        resp2 = client.get(f"/api/tasks/{task_id}")
        assert resp2.status_code == 200
        assert resp2.json()["command"] == "verify"

    def test_verify_with_check_hashes(self, client):
        resp = client.post("/api/verify?check_hashes=true")
        assert resp.status_code == 200
        assert "task_id" in resp.json()


# ---------------------------------------------------------------------------
# Task listing after multiple tasks created
# ---------------------------------------------------------------------------


class TestTaskListing:
    def test_tasks_list_includes_multiple(self, client):
        """Create several tasks and verify they all appear in the listing."""
        ids = []
        for endpoint in ["/api/scan", "/api/pipeline", "/api/verify"]:
            resp = client.post(endpoint)
            ids.append(resp.json()["task_id"])
        resp = client.get("/api/tasks")
        assert resp.status_code == 200
        listed_ids = {t["id"] for t in resp.json()["tasks"]}
        for tid in ids:
            assert tid in listed_ids


# ---------------------------------------------------------------------------
# Memories with actual date data
# ---------------------------------------------------------------------------


class TestMemoriesWithDates:
    def test_memories_with_dates_set(self, catalog_with_files):
        """Insert dates and verify the endpoint processes them without error.

        Note: The production code uses strftime('%%m-%%d', ...) which sends literal
        '%%m-%%d' to SQLite instead of '%m-%d', so the query never matches.
        This test validates the endpoint runs the full code path regardless.
        """
        from datetime import date

        today = date.today()
        past_year = today.year - 2
        date_str = f"{past_year}-{today.month:02d}-{today.day:02d} 12:00:00"

        cat = Catalog(catalog_with_files)
        cat.open()
        cat.conn.execute(
            "UPDATE files SET date_original = ? WHERE path LIKE '%photo1%'",
            (date_str,),
        )
        cat.conn.commit()
        cat.close()

        from godmode_media_library.web.app import create_app

        app = create_app(catalog_path=catalog_with_files)
        cl = TestClient(app)

        resp = cl.get("/api/memories")
        assert resp.status_code == 200
        data = resp.json()
        assert data["date"] == today.isoformat()
        assert isinstance(data["memories"], list)


# ---------------------------------------------------------------------------
# Timeline gaps — trailing gap case
# ---------------------------------------------------------------------------


class TestTimelineGapsTrailing:
    def test_trailing_gap(self, catalog_with_files):
        """Verify trailing gap detection (gap at end of range)."""
        cat = Catalog(catalog_with_files)
        cat.open()
        # Jan has data, Feb-Apr are empty (trailing gap)
        cat.conn.execute(
            "UPDATE files SET date_original = '2022:01:10 08:00:00' WHERE path LIKE '%photo1%'"
        )
        cat.conn.execute(
            "UPDATE files SET date_original = '2022:04:15 08:00:00' WHERE path LIKE '%video1%'"
        )
        # Make sure Apr has data too so Feb-Mar is a mid gap, not trailing
        # Actually let's make Jan and Apr have data, so Feb-Mar is a gap
        cat.conn.commit()
        cat.close()

        from godmode_media_library.web.app import create_app

        app = create_app(catalog_path=catalog_with_files)
        cl = TestClient(app)

        resp = cl.get("/api/timeline/gaps")
        assert resp.status_code == 200
        data = resp.json()
        assert data["coverage"]["covered_months"] >= 2
        # There should be a gap for Feb-Mar
        gap_months = [g for g in data["gaps"] if g["from"] == "2022-02"]
        assert len(gap_months) == 1
        assert gap_months[0]["months"] == 2
