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
