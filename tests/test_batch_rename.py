"""Tests for batch_rename module."""

from __future__ import annotations

from pathlib import Path

import pytest

from godmode_media_library.batch_rename import (
    RenameAction,
    RenameResult,
    apply_renames,
    plan_renames,
)


# ---------------------------------------------------------------------------
# plan_renames
# ---------------------------------------------------------------------------


def test_plan_renames_sequential_number(tmp_path):
    files = [tmp_path / "a.jpg", tmp_path / "b.jpg"]
    for f in files:
        f.touch()
    actions = plan_renames(files, "photo_{n:03d}")
    assert len(actions) == 2
    assert actions[0].new_name == "photo_001.jpg"
    assert actions[1].new_name == "photo_002.jpg"


def test_plan_renames_preserves_extension(tmp_path):
    f = tmp_path / "video.mp4"
    f.touch()
    actions = plan_renames([f], "clip_{n}")
    assert actions[0].new_name.endswith(".mp4")


def test_plan_renames_with_ext_in_pattern(tmp_path):
    f = tmp_path / "shot.png"
    f.touch()
    actions = plan_renames([f], "img_{n}.png")
    # Extension already in pattern — should not double up
    assert actions[0].new_name == "img_1.png"


def test_plan_renames_uses_original_name_placeholder(tmp_path):
    f = tmp_path / "sunset.jpg"
    f.touch()
    actions = plan_renames([f], "{name}_copy")
    assert "sunset" in actions[0].new_name


def test_plan_renames_date_extraction(tmp_path):
    f = tmp_path / "IMG_2024-06-15_shot.jpg"
    f.touch()
    actions = plan_renames([f], "{date}_{n}")
    assert "20240615" in actions[0].new_name


def test_plan_renames_parent_placeholder(tmp_path):
    sub = tmp_path / "vacation"
    sub.mkdir()
    f = sub / "pic.jpg"
    f.touch()
    actions = plan_renames([f], "{parent}_{n}")
    assert "vacation" in actions[0].new_name


def test_plan_renames_start_number(tmp_path):
    f = tmp_path / "a.jpg"
    f.touch()
    actions = plan_renames([f], "photo_{n}", start_number=10)
    assert "10" in actions[0].new_name


def test_plan_renames_empty_list():
    actions = plan_renames([], "photo_{n}")
    assert actions == []


# ---------------------------------------------------------------------------
# apply_renames — dry run
# ---------------------------------------------------------------------------


def test_dry_run_does_not_rename(tmp_path):
    f = tmp_path / "original.txt"
    f.write_text("data")
    action = RenameAction(original=f, new_name="new.txt", new_path=tmp_path / "new.txt")
    result = apply_renames([action], dry_run=True)
    assert result.renamed == 1
    assert f.exists()  # file NOT actually moved


# ---------------------------------------------------------------------------
# apply_renames — real renames
# ---------------------------------------------------------------------------


def test_apply_renames_moves_file(tmp_path):
    f = tmp_path / "old.txt"
    f.write_text("hello")
    action = RenameAction(original=f, new_name="new.txt", new_path=tmp_path / "new.txt")
    result = apply_renames([action])
    assert result.renamed == 1
    assert (tmp_path / "new.txt").read_text() == "hello"
    assert not f.exists()


def test_collision_adds_suffix(tmp_path):
    """When target already exists, a _1, _2 ... suffix is appended."""
    f1 = tmp_path / "src.txt"
    f1.write_text("source")
    existing = tmp_path / "target.txt"
    existing.write_text("already here")
    action = RenameAction(original=f1, new_name="target.txt", new_path=tmp_path / "target.txt")
    result = apply_renames([action])
    assert result.renamed == 1
    # The resolved path should have a _1 suffix
    assert (tmp_path / "target_1.txt").exists()


def test_collision_increments_suffix(tmp_path):
    """If _1 also exists, bump to _2."""
    src = tmp_path / "src.txt"
    src.write_text("data")
    (tmp_path / "target.txt").write_text("x")
    (tmp_path / "target_1.txt").write_text("x")
    action = RenameAction(original=src, new_name="target.txt", new_path=tmp_path / "target.txt")
    result = apply_renames([action])
    assert result.renamed == 1
    assert (tmp_path / "target_2.txt").exists()


def test_source_missing_is_skipped(tmp_path):
    missing = tmp_path / "gone.txt"
    action = RenameAction(original=missing, new_name="new.txt", new_path=tmp_path / "new.txt")
    result = apply_renames([action])
    assert result.skipped == 1
    assert len(result.errors) == 1


def test_conflict_two_sources_same_target(tmp_path):
    """Two actions mapping to the same target — second is skipped as conflict."""
    f1 = tmp_path / "a.txt"
    f2 = tmp_path / "b.txt"
    f1.write_text("a")
    f2.write_text("b")
    target = tmp_path / "out.txt"
    actions = [
        RenameAction(original=f1, new_name="out.txt", new_path=target),
        RenameAction(original=f2, new_name="out.txt", new_path=target),
    ]
    result = apply_renames(actions)
    assert result.renamed == 1
    assert result.skipped == 1
