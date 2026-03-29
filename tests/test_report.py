"""Tests for report.py — HTML report generation."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library.report import (
    _bar_html,
    _build_recommendations,
    _compute_coverage,
    _fmt_size,
    _pct,
    _render_html,
    generate_report,
    generate_report_html,
)


# ── _fmt_size ────────────────────────────────────────────────────────

class TestFmtSize:
    def test_bytes(self):
        assert _fmt_size(500) == "500 B"

    def test_kilobytes(self):
        assert _fmt_size(2048) == "2.0 KB"

    def test_megabytes(self):
        assert _fmt_size(5 * 1024 * 1024) == "5.0 MB"

    def test_gigabytes(self):
        assert _fmt_size(3 * 1024**3) == "3.00 GB"

    def test_zero(self):
        assert _fmt_size(0) == "0 B"


# ── _pct ─────────────────────────────────────────────────────────────

class TestPct:
    def test_zero_total(self):
        assert _pct(10, 0) == "0%"

    def test_half(self):
        assert _pct(50, 100) == "50%"

    def test_full(self):
        assert _pct(100, 100) == "100%"


# ── _bar_html ────────────────────────────────────────────────────────

class TestBarHtml:
    def test_returns_html_string(self):
        result = _bar_html("Photos", 50, 100)
        assert "bar-row" in result
        assert "Photos" in result
        assert "50" in result

    def test_zero_max_value(self):
        result = _bar_html("Empty", 0, 0)
        assert "width:0%" in result


# ── _compute_coverage ────────────────────────────────────────────────

class TestComputeCoverage:
    def test_empty_months(self):
        result = _compute_coverage([], None, None)
        assert result["percentage"] == 0
        assert result["covered_months"] == 0

    def test_full_coverage(self):
        months = ["2024-01", "2024-02", "2024-03"]
        result = _compute_coverage(months, "2024-01-01", "2024-03-31")
        assert result["percentage"] == 100.0
        assert result["covered_months"] == 3
        assert result["gaps"] == []

    def test_gap_detection(self):
        months = ["2024-01", "2024-04"]
        result = _compute_coverage(months, "2024-01-01", "2024-04-30")
        assert result["covered_months"] == 2
        assert result["total_months"] == 4
        assert len(result["gaps"]) == 1
        assert "2024-02" in result["gaps"][0]

    def test_single_month(self):
        result = _compute_coverage(["2024-06"], "2024-06-01", "2024-06-30")
        assert result["percentage"] == 100.0
        assert result["total_months"] == 1

    def test_invalid_format(self):
        result = _compute_coverage(["bad"], "bad", "bad")
        assert result["percentage"] == 0


# ── _build_recommendations ───────────────────────────────────────────

class TestBuildRecommendations:
    def test_clean_library(self):
        data = {
            "duplicates": {"removable": 0},
            "metadata": {"total_files": 100, "exif_date_count": 100, "hashed_count": 100},
            "quality": {"screenshots": 10},
            "coverage": {"gaps": []},
        }
        recs = _build_recommendations(data)
        assert len(recs) == 1
        assert recs[0]["severity"] == "ok"

    def test_duplicates_warning(self):
        data = {
            "duplicates": {"removable": 50, "savings_bytes": 1024 * 1024 * 500},
            "metadata": {"total_files": 100, "exif_date_count": 100, "hashed_count": 100},
            "quality": {"screenshots": 0},
            "coverage": {"gaps": []},
        }
        recs = _build_recommendations(data)
        assert any(r["severity"] == "warning" for r in recs)

    def test_missing_dates_info(self):
        data = {
            "duplicates": {"removable": 0},
            "metadata": {"total_files": 100, "exif_date_count": 50, "hashed_count": 100},
            "quality": {"screenshots": 0},
            "coverage": {"gaps": []},
        }
        recs = _build_recommendations(data)
        assert any("bez data" in r["text"] for r in recs)

    def test_many_screenshots(self):
        data = {
            "duplicates": {"removable": 0},
            "metadata": {"total_files": 100, "exif_date_count": 100, "hashed_count": 100},
            "quality": {"screenshots": 200},
            "coverage": {"gaps": []},
        }
        recs = _build_recommendations(data)
        assert any("screenshot" in r["text"] for r in recs)

    def test_coverage_gaps(self):
        data = {
            "duplicates": {"removable": 0},
            "metadata": {"total_files": 100, "exif_date_count": 100, "hashed_count": 100},
            "quality": {"screenshots": 0},
            "coverage": {"gaps": ["2020-01 -- 2020-03", "2020-06 -- 2020-08", "2021-01 -- 2021-02", "2022-01 -- 2022-03"]},
        }
        recs = _build_recommendations(data)
        assert any("mezer" in r["text"] for r in recs)


# ── _render_html ─────────────────────────────────────────────────────

class TestRenderHtml:
    @staticmethod
    def _minimal_data():
        return {
            "overview": {
                "total_files": 42,
                "total_size": 1024 * 1024 * 100,
                "date_range_original": (None, None),
                "date_range_mtime": (None, None),
                "last_scan": "2024-06-01",
                "sources": [],
            },
            "duplicates": {
                "groups": 2,
                "total_files": 5,
                "removable": 3,
                "savings_bytes": 1024 * 50,
                "after_files": 39,
                "after_size": 1024 * 1024 * 95,
            },
            "metadata": {
                "exif_date_count": 30,
                "gps_count": 10,
                "camera_count": 20,
                "hashed_count": 42,
                "top_cameras": [("Canon EOS R", 15), ("iPhone 15", 10)],
                "avg_richness": 0.65,
                "total_files": 42,
            },
            "coverage": {"first": "2020-01", "last": "2024-06", "percentage": 85.0, "gaps": [], "total_months": 54, "covered_months": 46},
            "quality": {"photos": 30, "videos": 5, "screenshots": 3, "other": 4, "low_quality": 2},
            "faces": {},
            "cloud": [],
            "recommendations": [{"icon": "x", "text": "All good", "detail": "No action", "severity": "ok"}],
        }

    def test_returns_valid_html(self):
        html = _render_html(self._minimal_data())
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html

    def test_contains_file_count(self):
        html = _render_html(self._minimal_data())
        assert "42" in html

    def test_no_oom_with_streaming(self):
        """Rendering should work fine with moderate data (no OOM)."""
        data = self._minimal_data()
        data["overview"]["total_files"] = 500_000
        html = _render_html(data)
        assert isinstance(html, str)
        assert len(html) > 100


# ── generate_report (mocked Catalog) ────────────────────────────────

class TestGenerateReport:
    def test_generate_report_writes_file(self, tmp_path):
        out = tmp_path / "report.html"
        with patch("godmode_media_library.report._collect_data") as mock_collect, \
             patch("godmode_media_library.catalog.Catalog") as MockCatalog:
            mock_cat = MagicMock()
            MockCatalog.return_value = mock_cat
            # Patch the local import inside generate_report
            with patch.dict("sys.modules", {"godmode_media_library.catalog": MagicMock(Catalog=MockCatalog)}):
                mock_collect.return_value = TestRenderHtml._minimal_data()
                result = generate_report(tmp_path / "catalog.db", out)

        assert Path(result).exists()
        content = Path(result).read_text()
        assert "<!DOCTYPE html>" in content

    def test_generate_report_html_returns_string(self, tmp_path):
        with patch("godmode_media_library.report._collect_data") as mock_collect, \
             patch("godmode_media_library.catalog.Catalog") as MockCatalog:
            mock_cat = MagicMock()
            MockCatalog.return_value = mock_cat
            with patch.dict("sys.modules", {"godmode_media_library.catalog": MagicMock(Catalog=MockCatalog)}):
                mock_collect.return_value = TestRenderHtml._minimal_data()
                html = generate_report_html(tmp_path / "catalog.db")

        assert isinstance(html, str)
        assert "<!DOCTYPE html>" in html

    def test_generate_report_auto_path(self, tmp_path):
        with patch("godmode_media_library.report._collect_data") as mock_collect, \
             patch("godmode_media_library.catalog.Catalog") as MockCatalog:
            mock_cat = MagicMock()
            MockCatalog.return_value = mock_cat
            with patch.dict("sys.modules", {"godmode_media_library.catalog": MagicMock(Catalog=MockCatalog)}):
                mock_collect.return_value = TestRenderHtml._minimal_data()
                result = generate_report(tmp_path / "catalog.db")

        assert "godmode_report_" in result
        assert result.endswith(".html")


# ── XSS escaping tests ──────────────────────────────────────────────

class TestXSSEscaping:
    """Verify that user-controlled data is HTML-escaped in reports."""

    def test_source_path_xss_escaped(self):
        data = TestRenderHtml._minimal_data()
        data["overview"]["sources"] = [
            {"path": "<script>alert('xss')</script>", "file_count": 1, "last_scan": "2024-01-01", "scan_count": 1}
        ]
        result = _render_html(data)
        assert "<script>" not in result
        assert "&lt;script&gt;" in result

    def test_camera_model_xss_escaped(self):
        data = TestRenderHtml._minimal_data()
        data["metadata"]["top_cameras"] = [('<img onerror="alert(1)">', 10)]
        result = _render_html(data)
        # The raw <img> tag must not appear unescaped
        assert '<img onerror=' not in result
        assert "&lt;img" in result

    def test_recommendation_text_xss_escaped(self):
        data = TestRenderHtml._minimal_data()
        data["recommendations"] = [
            {"icon": "x", "text": "<b>bold</b>", "detail": "<i>italic</i>", "severity": "ok"}
        ]
        result = _render_html(data)
        assert "&lt;b&gt;" in result
        assert "&lt;i&gt;" in result

    def test_cloud_remote_xss_escaped(self):
        data = TestRenderHtml._minimal_data()
        data["cloud"] = [{"name": "<script>x</script>", "type": "<b>hack</b>"}]
        result = _render_html(data)
        assert "<script>x</script>" not in result


# ── Render with all sections populated ──────────────────────────────

class TestRenderHtmlFullSections:
    """Test rendering with all optional sections populated."""

    def test_faces_section_rendered(self):
        data = TestRenderHtml._minimal_data()
        data["faces"] = {"total_faces": 150, "total_persons": 12}
        result = _render_html(data)
        assert "150" in result
        assert "12" in result
        assert "Obliceje" in result

    def test_cloud_section_rendered(self):
        data = TestRenderHtml._minimal_data()
        data["cloud"] = [{"name": "gdrive", "type": "drive"}]
        result = _render_html(data)
        assert "gdrive" in result
        assert "Cloudove zdroje" in result

    def test_cloud_section_with_string_remotes(self):
        data = TestRenderHtml._minimal_data()
        data["cloud"] = ["remote1", "remote2"]
        result = _render_html(data)
        assert "remote1" in result

    def test_coverage_gaps_rendered(self):
        data = TestRenderHtml._minimal_data()
        data["coverage"]["gaps"] = ["2020-03 -- 2020-05", "2021-01 -- 2021-03"]
        result = _render_html(data)
        assert "2020-03" in result
        assert "Mezery" in result

    def test_quality_section_with_low_quality(self):
        data = TestRenderHtml._minimal_data()
        data["quality"]["low_quality"] = 42
        result = _render_html(data)
        assert "42" in result
        assert "Nizka kvalita" in result

    def test_no_coverage_section_when_empty(self):
        data = TestRenderHtml._minimal_data()
        data["coverage"] = {"first": None, "last": None, "percentage": 0, "gaps": [], "total_months": 0, "covered_months": 0}
        result = _render_html(data)
        assert "Casove pokryti" not in result

    def test_mtime_date_range_fallback(self):
        data = TestRenderHtml._minimal_data()
        data["overview"]["date_range_original"] = (None, None)
        data["overview"]["date_range_mtime"] = (1700000000.0, 1700100000.0)
        result = _render_html(data)
        assert "(mtime)" in result

    def test_mtime_date_range_invalid(self):
        data = TestRenderHtml._minimal_data()
        data["overview"]["date_range_original"] = (None, None)
        data["overview"]["date_range_mtime"] = ("invalid", None)
        result = _render_html(data)
        # Should fall back to "-"
        assert isinstance(result, str)

    def test_empty_quality_section(self):
        data = TestRenderHtml._minimal_data()
        data["quality"] = {}
        result = _render_html(data)
        assert "Kvalita" not in result

    def test_no_cameras(self):
        data = TestRenderHtml._minimal_data()
        data["metadata"]["top_cameras"] = []
        result = _render_html(data)
        assert "Top 5 fotoaparatu" not in result


# ── _compute_coverage edge cases ────────────────────────────────────

class TestComputeCoverageEdge:
    def test_trailing_gap(self):
        # _compute_coverage uses the months list itself (first/last) to define the range
        months = ["2024-01", "2024-05"]
        result = _compute_coverage(months, "2024-01-01", "2024-05-31")
        assert result["total_months"] == 5
        assert result["covered_months"] == 2
        assert len(result["gaps"]) >= 1
        # Gap in the middle: 2024-02 -- 2024-04
        assert any("2024-02" in g for g in result["gaps"])

    def test_duplicate_months(self):
        months = ["2024-01", "2024-01", "2024-02", "2024-02"]
        result = _compute_coverage(months, "2024-01-01", "2024-02-28")
        assert result["covered_months"] == 2
        assert result["percentage"] == 100.0


# ── _build_recommendations edge cases ───────────────────────────────

class TestBuildRecommendationsEdge:
    def test_missing_hashes(self):
        data = {
            "duplicates": {"removable": 0},
            "metadata": {"total_files": 100, "exif_date_count": 100, "hashed_count": 50},
            "quality": {"screenshots": 0},
            "coverage": {"gaps": []},
        }
        recs = _build_recommendations(data)
        assert any("SHA-256" in r["text"] for r in recs)

    def test_empty_metadata(self):
        data = {
            "duplicates": {"removable": 0},
            "metadata": {"total_files": 0, "exif_date_count": 0, "hashed_count": 0},
            "quality": {"screenshots": 0},
            "coverage": {"gaps": []},
        }
        recs = _build_recommendations(data)
        # With total_files=0, no metadata recs generated; should get "ok"
        assert any(r["severity"] == "ok" for r in recs)


# ── _bar_html edge cases ────────────────────────────────────────────

class TestBarHtmlEdge:
    def test_custom_color(self):
        result = _bar_html("Test", 30, 60, "#ff0000")
        assert "#ff0000" in result
        assert "width:50%" in result

    def test_value_formatting_with_comma(self):
        result = _bar_html("Big", 1000, 2000)
        assert "1,000" in result


# ── _fmt_size edge cases ────────────────────────────────────────────

class TestFmtSizeEdge:
    def test_boundary_kb(self):
        assert _fmt_size(1024) == "1.0 KB"

    def test_boundary_mb(self):
        assert _fmt_size(1024**2) == "1.0 MB"

    def test_boundary_gb(self):
        assert _fmt_size(1024**3) == "1.00 GB"


# ── _collect_data with real catalog ────────────────────────────────

from godmode_media_library.report import _collect_data


class TestCollectData:
    """Test _collect_data against a real catalog with scanned files."""

    def _scan_files(self, db_path, media):
        from godmode_media_library.catalog import Catalog
        from godmode_media_library.scanner import incremental_scan

        with Catalog(db_path) as cat:
            with patch("godmode_media_library.scanner.probe_file", return_value=None), \
                 patch("godmode_media_library.scanner.read_exif", return_value=None), \
                 patch("godmode_media_library.scanner.dhash", return_value=None), \
                 patch("godmode_media_library.scanner.video_dhash", return_value=None):
                incremental_scan(cat, [media])

    def test_collect_data_basic(self, tmp_path):
        from godmode_media_library.catalog import Catalog

        db_path = tmp_path / "catalog.db"
        media = tmp_path / "media"
        media.mkdir()
        (media / "photo1.jpg").write_bytes(b"JPEG1" * 100)
        (media / "photo2.jpg").write_bytes(b"JPEG2" * 100)
        (media / "video.mp4").write_bytes(b"MP4V" * 100)

        self._scan_files(db_path, media)

        # Mock cloud.list_remotes to avoid rclone dependency
        mock_cloud = MagicMock()
        mock_cloud.list_remotes.return_value = {"remotes": []}
        with Catalog(db_path) as cat:
            with patch.dict("sys.modules", {"godmode_media_library.cloud": mock_cloud}):
                data = _collect_data(cat)

        assert data["overview"]["total_files"] == 3
        assert data["overview"]["total_size"] > 0
        assert isinstance(data["duplicates"]["groups"], int)
        assert isinstance(data["metadata"]["exif_date_count"], int)
        assert isinstance(data["quality"], dict)
        assert isinstance(data["recommendations"], list)

    def test_collect_data_with_duplicates(self, tmp_path):
        from godmode_media_library.catalog import Catalog

        db_path = tmp_path / "catalog.db"
        media = tmp_path / "media"
        media.mkdir()
        content = b"DUPLICATE_CONTENT" * 200
        (media / "orig.jpg").write_bytes(content)
        (media / "copy.jpg").write_bytes(content)

        self._scan_files(db_path, media)

        mock_cloud = MagicMock()
        mock_cloud.list_remotes.return_value = {"remotes": []}
        with Catalog(db_path) as cat:
            with patch.dict("sys.modules", {"godmode_media_library.cloud": mock_cloud}):
                data = _collect_data(cat)

        assert data["duplicates"]["groups"] >= 1
        assert data["duplicates"]["removable"] >= 1
        assert data["duplicates"]["savings_bytes"] > 0

    def test_collect_data_empty_catalog(self, tmp_path):
        from godmode_media_library.catalog import Catalog

        db_path = tmp_path / "catalog.db"
        mock_cloud = MagicMock()
        mock_cloud.list_remotes.return_value = {"remotes": []}
        with Catalog(db_path) as cat:
            with patch.dict("sys.modules", {"godmode_media_library.cloud": mock_cloud}):
                data = _collect_data(cat)

        assert data["overview"]["total_files"] == 0
        assert data["duplicates"]["groups"] == 0
        assert data["metadata"]["exif_date_count"] == 0

    def test_full_report_generation_with_real_catalog(self, tmp_path):
        """End-to-end: scan files, collect data, render HTML."""
        from godmode_media_library.catalog import Catalog

        db_path = tmp_path / "catalog.db"
        media = tmp_path / "media"
        media.mkdir()
        (media / "a.jpg").write_bytes(b"AAA" * 100)

        self._scan_files(db_path, media)

        mock_cloud = MagicMock()
        mock_cloud.list_remotes.return_value = {"remotes": []}
        with Catalog(db_path) as cat:
            with patch.dict("sys.modules", {"godmode_media_library.cloud": mock_cloud}):
                data = _collect_data(cat)
            html_str = _render_html(data)

        assert "<!DOCTYPE html>" in html_str
        assert "GOD MODE" in html_str
