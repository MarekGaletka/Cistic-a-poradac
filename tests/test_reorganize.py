"""Tests for reorganize.py — media file reorganization engine."""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library.reorganize import (
    ReorganizeConfig,
    ReorganizeFileEntry,
    ReorganizePlan,
    ReorganizeResult,
    _compute_destination,
    _human_size,
    _icon_for,
    _looks_like_iphone,
    _resolve_collision,
    _should_exclude,
    execute_reorganization,
    plan_reorganization,
)


# ── _should_exclude ──────────────────────────────────────────────────

class TestShouldExclude:
    def test_dotfile_pattern(self):
        assert _should_exclude(Path("/photos/.DS_Store"), [".DS_Store"]) is True

    def test_no_match(self):
        assert _should_exclude(Path("/photos/image.jpg"), [".DS_Store"]) is False

    def test_substring_match(self):
        assert _should_exclude(Path("/photos/thumbs/cache/img.jpg"), ["thumbs"]) is True

    def test_wildcard_dot(self):
        assert _should_exclude(Path("/x/.hidden"), [".*"]) is True

    def test_non_dotfile_with_wildcard(self):
        assert _should_exclude(Path("/x/normal.jpg"), [".*"]) is False


# ── _icon_for ────────────────────────────────────────────────────────

class TestIconFor:
    def test_mac(self):
        assert _icon_for("mac") == "folder"

    def test_external(self):
        assert _icon_for("external") == "hard-drive"

    def test_iphone(self):
        assert _icon_for("iphone") == "smartphone"

    def test_icloud(self):
        assert _icon_for("icloud") == "cloud"

    def test_unknown(self):
        assert _icon_for("alien") == "folder"


# ── _looks_like_iphone ──────────────────────────────────────────────

class TestLooksLikeIphone:
    def test_has_dcim(self, tmp_path):
        (tmp_path / "DCIM").mkdir()
        assert _looks_like_iphone(tmp_path) is True

    def test_no_dcim(self, tmp_path):
        assert _looks_like_iphone(tmp_path) is False


# ── _human_size ──────────────────────────────────────────────────────

class TestHumanSize:
    def test_bytes(self):
        assert "B" in _human_size(500)

    def test_kilobytes(self):
        assert "KB" in _human_size(2048)

    def test_megabytes(self):
        assert "MB" in _human_size(5 * 1024 * 1024)

    def test_gigabytes(self):
        assert "GB" in _human_size(3 * 1024**3)


# ── _resolve_collision ───────────────────────────────────────────────

class TestResolveCollision:
    def test_no_collision(self, tmp_path):
        target = tmp_path / "photo.jpg"
        reserved = set()
        result = _resolve_collision(target, reserved)
        assert result == target
        assert target in reserved

    def test_collision_with_reserved(self, tmp_path):
        target = tmp_path / "photo.jpg"
        reserved = {target}
        result = _resolve_collision(target, reserved)
        assert result == tmp_path / "photo_1.jpg"

    def test_collision_with_existing_file(self, tmp_path):
        target = tmp_path / "photo.jpg"
        target.write_text("exists")
        reserved = set()
        result = _resolve_collision(target, reserved)
        assert result == tmp_path / "photo_1.jpg"

    def test_multiple_collisions(self, tmp_path):
        target = tmp_path / "photo.jpg"
        reserved = {target, tmp_path / "photo_1.jpg", tmp_path / "photo_2.jpg"}
        result = _resolve_collision(target, reserved)
        assert result == tmp_path / "photo_3.jpg"


# ── _compute_destination ─────────────────────────────────────────────

class TestComputeDestination:
    def _make_entry(self, name="photo.jpg", category="images"):
        entry = ReorganizeFileEntry(source_path=Path(f"/src/{name}"))
        entry.file_category = category
        return entry

    @patch("godmode_media_library.reorganize._origin_timestamp", return_value=1718400000.0)
    def test_year_month(self, mock_ts, tmp_path):
        entry = self._make_entry()
        result = _compute_destination(entry, tmp_path, "year_month")
        assert tmp_path in result.parents or result.parent == tmp_path
        assert "photo.jpg" in result.name

    @patch("godmode_media_library.reorganize._origin_timestamp", return_value=1718400000.0)
    def test_year_type(self, mock_ts, tmp_path):
        entry = self._make_entry(category="images")
        result = _compute_destination(entry, tmp_path, "year_type")
        assert "images" in str(result)

    @patch("godmode_media_library.reorganize._origin_timestamp", return_value=1718400000.0)
    def test_flat(self, mock_ts, tmp_path):
        entry = self._make_entry()
        result = _compute_destination(entry, tmp_path, "flat")
        assert result == tmp_path / "photo.jpg"

    @patch("godmode_media_library.reorganize._origin_timestamp", return_value=0.0)
    def test_zero_timestamp(self, mock_ts, tmp_path):
        entry = self._make_entry()
        result = _compute_destination(entry, tmp_path, "year_month")
        # Should fall back to 2000-01-01
        assert "2000" in str(result)


# ── plan_reorganization (mocked filesystem) ──────────────────────────

class TestPlanReorganization:
    @patch("godmode_media_library.reorganize.sha256_file", return_value="abc123")
    @patch("godmode_media_library.reorganize.iter_files")
    def test_basic_plan(self, mock_iter, mock_sha, tmp_path):
        # Create source files
        src = tmp_path / "source"
        src.mkdir()
        f1 = src / "photo1.jpg"
        f1.write_bytes(b"\xff\xd8" + b"\x00" * 1000)
        f2 = src / "photo2.jpg"
        f2.write_bytes(b"\xff\xd8" + b"\x00" * 2000)

        mock_iter.return_value = iter([f1, f2])

        config = ReorganizeConfig(
            sources=[src],
            destination=tmp_path / "dest",
            dry_run=True,
            deduplicate=False,
            workers=1,
        )
        plan = plan_reorganization(config)
        assert plan.total_files == 2
        assert plan.unique_files == 2

    @patch("godmode_media_library.reorganize.sha256_file")
    @patch("godmode_media_library.reorganize.iter_files")
    def test_dedup_detects_duplicates(self, mock_iter, mock_sha, tmp_path):
        src = tmp_path / "source"
        src.mkdir()
        f1 = src / "a.jpg"
        f1.write_bytes(b"\x00" * 500)
        f2 = src / "b.jpg"
        f2.write_bytes(b"\x00" * 500)

        mock_iter.return_value = iter([f1, f2])
        mock_sha.return_value = "same_hash"

        config = ReorganizeConfig(
            sources=[src],
            destination=tmp_path / "dest",
            dry_run=True,
            deduplicate=True,
            workers=1,
        )
        plan = plan_reorganization(config)
        assert plan.duplicate_files == 1
        assert plan.unique_files == 1

    @patch("godmode_media_library.reorganize.sha256_file", return_value="x")
    @patch("godmode_media_library.reorganize.iter_files")
    def test_progress_callback(self, mock_iter, mock_sha, tmp_path):
        src = tmp_path / "source"
        src.mkdir()
        f = src / "pic.jpg"
        f.write_bytes(b"\x00" * 100)
        mock_iter.return_value = iter([f])

        progress_calls = []
        config = ReorganizeConfig(
            sources=[src],
            destination=tmp_path / "dest",
            dry_run=True,
            deduplicate=False,
            workers=1,
        )
        plan = plan_reorganization(config, progress_fn=lambda d: progress_calls.append(d))
        assert len(progress_calls) > 0
        assert progress_calls[0]["phase"] == "discovery"


# ── execute_reorganization ───────────────────────────────────────────

class TestExecuteReorganization:
    def test_dry_run_no_copies(self, tmp_path):
        config = ReorganizeConfig(
            sources=[tmp_path / "src"],
            destination=tmp_path / "dest",
            dry_run=True,
        )
        entry = ReorganizeFileEntry(
            source_path=tmp_path / "src" / "photo.jpg",
            destination_path=tmp_path / "dest" / "photo.jpg",
            file_size=1000,
        )
        plan = ReorganizePlan(config=config, entries=[entry], total_files=1, unique_files=1)

        result = execute_reorganization(plan)
        assert result.files_copied == 0
        assert result.files_skipped == 1
        assert not (tmp_path / "dest" / "photo.jpg").exists()

    def test_actual_copy(self, tmp_path):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        src_file = src_dir / "photo.jpg"
        src_file.write_bytes(b"\xff\xd8" + b"\x00" * 100)

        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        config = ReorganizeConfig(
            sources=[src_dir],
            destination=dest_dir,
            dry_run=False,
        )
        entry = ReorganizeFileEntry(
            source_path=src_file,
            destination_path=dest_dir / "photo.jpg",
            file_size=src_file.stat().st_size,
        )
        plan = ReorganizePlan(config=config, entries=[entry], total_files=1, unique_files=1)

        result = execute_reorganization(plan)
        assert result.files_copied == 1
        assert (dest_dir / "photo.jpg").exists()

    def test_missing_source_error(self, tmp_path):
        config = ReorganizeConfig(
            sources=[tmp_path / "src"],
            destination=tmp_path / "dest",
            dry_run=False,
        )
        (tmp_path / "dest").mkdir()
        entry = ReorganizeFileEntry(
            source_path=tmp_path / "src" / "gone.jpg",
            destination_path=tmp_path / "dest" / "gone.jpg",
            file_size=100,
        )
        plan = ReorganizePlan(config=config, entries=[entry], total_files=1, unique_files=1)

        result = execute_reorganization(plan)
        assert result.files_skipped == 1
        assert len(result.errors) == 1

    def test_delete_originals(self, tmp_path):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        src_file = src_dir / "photo.jpg"
        src_file.write_bytes(b"\xff" * 50)

        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        config = ReorganizeConfig(
            sources=[src_dir],
            destination=dest_dir,
            dry_run=False,
            delete_originals=True,
            deduplicate=False,
        )
        entry = ReorganizeFileEntry(
            source_path=src_file,
            destination_path=dest_dir / "photo.jpg",
            file_size=src_file.stat().st_size,
        )
        plan = ReorganizePlan(config=config, entries=[entry], total_files=1, unique_files=1)

        result = execute_reorganization(plan)
        assert result.files_copied == 1
        assert result.originals_deleted == 1
        assert not src_file.exists()
