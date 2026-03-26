"""Tests for _build_dest_path() and _make_collision_safe() helpers (Session 5, item 5.3)."""

import pytest

from godmode_media_library.consolidation import _build_dest_path, _make_collision_safe
from godmode_media_library.consolidation_types import StructurePattern


class TestBuildDestPath:
    def test_year_month_structure(self):
        result = _build_dest_path(
            "dest",
            "IMG_001.jpg",
            "abc123",
            "2024-06-15T10:30:00",
            StructurePattern.YEAR_MONTH,
        )
        assert result == "dest/2024/06/IMG_001.jpg"

    def test_year_only_structure(self):
        result = _build_dest_path(
            "dest",
            "IMG_001.jpg",
            "abc123",
            "2024-06-15T10:30:00",
            StructurePattern.YEAR,
        )
        assert result == "dest/2024/IMG_001.jpg"

    def test_flat_structure(self):
        result = _build_dest_path(
            "dest",
            "IMG_001.jpg",
            "abc123",
            "2024-06-15T10:30:00",
            StructurePattern.FLAT,
        )
        assert result == "dest/IMG_001.jpg"

    def test_no_mod_time_uses_unknown(self):
        result = _build_dest_path(
            "dest",
            "IMG_001.jpg",
            "abc123",
            None,
            StructurePattern.YEAR_MONTH,
        )
        assert result == "dest/unknown/00/IMG_001.jpg"

    def test_empty_filename_uses_hash(self):
        result = _build_dest_path(
            "dest",
            "",
            "abcdef123456789",
            None,
            StructurePattern.FLAT,
        )
        assert "unnamed_abcdef123456" in result

    def test_whitespace_filename_uses_hash(self):
        result = _build_dest_path(
            "dest",
            "   ",
            "abcdef123456789",
            None,
            StructurePattern.FLAT,
        )
        assert "unnamed_" in result

    def test_empty_filename_no_hash(self):
        result = _build_dest_path(
            "dest",
            "",
            "",
            None,
            StructurePattern.FLAT,
        )
        assert "unnamed_file" in result

    def test_date_format_without_time(self):
        result = _build_dest_path(
            "dest",
            "f.jpg",
            "h",
            "2023-01-05",
            StructurePattern.YEAR_MONTH,
        )
        assert result == "dest/2023/01/f.jpg"

    def test_date_format_with_space_separator(self):
        result = _build_dest_path(
            "dest",
            "f.jpg",
            "h",
            "2023-12-25 08:00:00",
            StructurePattern.YEAR_MONTH,
        )
        assert result == "dest/2023/12/f.jpg"

    def test_invalid_date_falls_back_to_unknown(self):
        result = _build_dest_path(
            "dest",
            "f.jpg",
            "h",
            "not-a-date",
            StructurePattern.YEAR_MONTH,
        )
        assert "unknown/00" in result


class TestMakeCollisionSafe:
    def test_adds_hash_suffix(self):
        result = _make_collision_safe("dest/2024/06/IMG_001.jpg", "abcdef123456")
        assert result == "dest/2024/06/IMG_001_abcdef.jpg"

    def test_preserves_extension(self):
        result = _make_collision_safe("dest/video.mp4", "abcdef123456")
        assert result.endswith(".mp4")
        assert "_abcdef" in result

    def test_no_hash_uses_md5_of_path(self):
        result = _make_collision_safe("dest/file.jpg", "")
        # Should still produce a valid collision-safe path
        assert "_" in result.split("/")[-1]
        assert result.endswith(".jpg")

    def test_collision_extends_hash(self):
        existing = {"dest/IMG_001_abcdef.jpg"}
        result = _make_collision_safe(
            "dest/IMG_001.jpg",
            "abcdef123456789abcdef123456789ab",
            existing_paths=existing,
        )
        # Should have a longer hash prefix since the 6-char one collided
        assert result not in existing
        assert result.endswith(".jpg")

    def test_counter_fallback_on_full_hash_collision(self):
        full_hash = "abcdef"
        existing = {f"dest/IMG_{full_hash}.jpg"}
        result = _make_collision_safe("dest/IMG.jpg", full_hash, existing_paths=existing)
        assert result not in existing

    def test_no_existing_paths_returns_simple_suffix(self):
        result = _make_collision_safe("dir/photo.heic", "deadbeef1234")
        assert result == "dir/photo_deadbe.heic"

    def test_file_without_extension(self):
        result = _make_collision_safe("dest/README", "abc123def456")
        assert "abc123" in result
