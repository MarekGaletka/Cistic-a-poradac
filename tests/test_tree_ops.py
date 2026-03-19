from __future__ import annotations

import os
from pathlib import Path

from godmode_media_library.tree_ops import (
    _date_bucket,
    _file_category,
    _sanitize_segment,
    apply_tree_plan,
    create_tree_plan,
    write_tree_plan,
)


def test_date_bucket_day():
    # 2024-01-15 in some timezone
    import datetime as dt

    ts = dt.datetime(2024, 1, 15, 12, 0, 0).timestamp()
    bucket = _date_bucket(ts, "day")
    assert bucket == "2024/01/15"


def test_date_bucket_month():
    import datetime as dt

    ts = dt.datetime(2024, 3, 20, 12, 0, 0).timestamp()
    bucket = _date_bucket(ts, "month")
    assert bucket == "2024/03"


def test_file_category():
    assert _file_category("jpg") == "images"
    assert _file_category("mp4") == "videos"
    assert _file_category("dng") == "raw"
    assert _file_category("pdf") == "documents"
    assert _file_category("zip") == "archives"
    assert _file_category("xyz") == "other"
    assert _file_category("mp3") == "audio"


def test_sanitize_segment():
    assert _sanitize_segment("hello world") == "hello world"
    assert _sanitize_segment('file:name*"test"') == "file_name__test_"
    assert _sanitize_segment("  spaced  ") == "spaced"
    assert _sanitize_segment("") == "Unknown"
    assert _sanitize_segment("a\tb") == "a_b"


def test_create_tree_plan_time_mode(tmp_path: Path):
    src = tmp_path / "source"
    src.mkdir()
    (src / "photo.jpg").write_bytes(b"JPEG" * 100)
    target = tmp_path / "target"

    rows = create_tree_plan(
        roots=[src],
        target_root=target,
        mode="time",
        granularity="day",
    )
    assert len(rows) >= 1
    for row in rows:
        assert row.mode == "time"
        # Bucket should be YYYY/MM/DD
        parts = row.bucket.split("/")
        assert len(parts) == 3
        assert len(parts[0]) == 4  # year


def test_apply_tree_plan_hardlink(tmp_path: Path):
    # Create a source file and a plan TSV
    src = tmp_path / "source"
    src.mkdir()
    src_file = src / "photo.jpg"
    src_file.write_bytes(b"JPEG_DATA" * 50)

    dst = tmp_path / "target" / "by_time" / "2024" / "01" / "15"

    plan_path = tmp_path / "plan.tsv"
    from godmode_media_library.utils import write_tsv

    write_tsv(
        plan_path,
        ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
        [("u1", str(src_file), str(dst / "photo.jpg"), "time", "2024/01/15", "", "0")],
    )

    log_path = tmp_path / "log.tsv"
    applied, skipped = apply_tree_plan(
        plan_path=plan_path,
        operation="hardlink",
        dry_run=False,
        collision_policy="rename",
        log_path=log_path,
    )

    assert applied == 1
    assert skipped == 0
    dest_file = dst / "photo.jpg"
    assert dest_file.exists()
    # Verify it's a hardlink (same inode)
    assert os.stat(src_file).st_ino == os.stat(dest_file).st_ino


def test_apply_tree_plan_skip_missing(tmp_path: Path):
    plan_path = tmp_path / "plan.tsv"
    from godmode_media_library.utils import write_tsv

    write_tsv(
        plan_path,
        ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
        [("u1", str(tmp_path / "nonexistent.jpg"), str(tmp_path / "dst" / "photo.jpg"), "time", "2024/01", "", "0")],
    )

    log_path = tmp_path / "log.tsv"
    applied, skipped = apply_tree_plan(
        plan_path=plan_path,
        operation="hardlink",
        dry_run=False,
        collision_policy="skip",
        log_path=log_path,
    )

    assert applied == 0
    assert skipped == 1


def test_apply_tree_plan_move(tmp_path: Path):
    """Move operation relocates file to new location."""
    src = tmp_path / "source"
    src.mkdir()
    src_file = src / "image.jpg"
    src_file.write_bytes(b"IMAGE_DATA" * 50)

    dst = tmp_path / "target" / "2024" / "01"

    plan_path = tmp_path / "plan.tsv"
    from godmode_media_library.utils import write_tsv

    write_tsv(
        plan_path,
        ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
        [("u1", str(src_file), str(dst / "image.jpg"), "time", "2024/01", "", "0")],
    )

    log_path = tmp_path / "log.tsv"
    applied, skipped = apply_tree_plan(
        plan_path=plan_path,
        operation="move",
        dry_run=False,
        collision_policy="skip",
        log_path=log_path,
    )

    assert applied == 1
    assert skipped == 0
    assert (dst / "image.jpg").exists()
    assert not src_file.exists()  # moved away


def test_apply_tree_plan_collision_skip(tmp_path: Path):
    """Existing destination + skip policy results in skipped."""
    src = tmp_path / "source"
    src.mkdir()
    src_file = src / "photo.jpg"
    src_file.write_bytes(b"SRC_DATA" * 50)

    dst_dir = tmp_path / "target"
    dst_dir.mkdir(parents=True)
    dst_file = dst_dir / "photo.jpg"
    dst_file.write_bytes(b"EXISTING_DATA" * 50)

    plan_path = tmp_path / "plan.tsv"
    from godmode_media_library.utils import write_tsv

    write_tsv(
        plan_path,
        ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
        [("u1", str(src_file), str(dst_file), "time", "2024/01", "", "0")],
    )

    log_path = tmp_path / "log.tsv"
    applied, skipped = apply_tree_plan(
        plan_path=plan_path,
        operation="copy",
        dry_run=False,
        collision_policy="skip",
        log_path=log_path,
    )

    assert applied == 0
    assert skipped == 1
    # Original destination unchanged
    assert dst_file.read_bytes() == b"EXISTING_DATA" * 50


def test_apply_tree_plan_collision_rename(tmp_path: Path):
    """Existing destination + rename policy creates a renamed copy."""
    src = tmp_path / "source"
    src.mkdir()
    src_file = src / "photo.jpg"
    src_file.write_bytes(b"NEW_DATA" * 50)

    dst_dir = tmp_path / "target"
    dst_dir.mkdir(parents=True)
    dst_file = dst_dir / "photo.jpg"
    dst_file.write_bytes(b"EXISTING_DATA" * 50)

    plan_path = tmp_path / "plan.tsv"
    from godmode_media_library.utils import write_tsv

    write_tsv(
        plan_path,
        ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
        [("u1", str(src_file), str(dst_file), "time", "2024/01", "", "0")],
    )

    log_path = tmp_path / "log.tsv"
    applied, skipped = apply_tree_plan(
        plan_path=plan_path,
        operation="copy",
        dry_run=False,
        collision_policy="rename",
        log_path=log_path,
    )

    assert applied == 1
    assert skipped == 0
    # Original destination still exists
    assert dst_file.read_bytes() == b"EXISTING_DATA" * 50
    # Renamed copy should exist
    renamed = dst_dir / "photo (1).jpg"
    assert renamed.exists()
    assert renamed.read_bytes() == b"NEW_DATA" * 50


def test_date_bucket_year():
    import datetime as dt

    ts = dt.datetime(2024, 6, 15, 12, 0, 0).timestamp()
    bucket = _date_bucket(ts, "year")
    assert bucket == "2024"


def test_create_tree_plan_type_mode(tmp_path: Path):
    src = tmp_path / "source"
    src.mkdir()
    (src / "photo.jpg").write_bytes(b"JPEG" * 100)
    (src / "video.mp4").write_bytes(b"MP4V" * 100)
    (src / "notes.txt").write_bytes(b"TEXT" * 100)
    target = tmp_path / "target"

    rows = create_tree_plan(
        roots=[src],
        target_root=target,
        mode="type",
    )
    assert len(rows) >= 3
    buckets = {row.bucket for row in rows}
    # Should categorize into images, videos, documents
    assert any("images" in b for b in buckets)
    assert any("videos" in b for b in buckets)
    assert any("documents" in b for b in buckets)


def test_write_tree_plan(tmp_path: Path):
    src = tmp_path / "source"
    src.mkdir()
    (src / "photo.jpg").write_bytes(b"JPEG" * 100)
    target = tmp_path / "target"

    rows = create_tree_plan(
        roots=[src],
        target_root=target,
        mode="time",
    )
    plan_path = tmp_path / "plan.tsv"
    write_tree_plan(plan_path, rows)
    assert plan_path.exists()

    from godmode_media_library.utils import read_tsv_dict

    written = read_tsv_dict(plan_path)
    assert len(written) == len(rows)
    assert "source_path" in written[0]
    assert "destination_path" in written[0]


def test_apply_tree_plan_symlink(tmp_path: Path):
    src = tmp_path / "source"
    src.mkdir()
    src_file = src / "photo.jpg"
    src_file.write_bytes(b"DATA" * 50)

    dst = tmp_path / "target" / "linked"

    plan_path = tmp_path / "plan.tsv"
    from godmode_media_library.utils import write_tsv

    write_tsv(
        plan_path,
        ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
        [("u1", str(src_file), str(dst / "photo.jpg"), "time", "2024/01", "", "0")],
    )

    log_path = tmp_path / "log.tsv"
    applied, skipped = apply_tree_plan(
        plan_path=plan_path,
        operation="symlink",
        dry_run=False,
        collision_policy="skip",
        log_path=log_path,
    )

    assert applied == 1
    assert skipped == 0
    dest_file = dst / "photo.jpg"
    assert dest_file.is_symlink()


def test_apply_tree_plan_copy(tmp_path: Path):
    src = tmp_path / "source"
    src.mkdir()
    src_file = src / "photo.jpg"
    src_file.write_bytes(b"COPYDATA" * 50)

    dst = tmp_path / "target" / "copied"

    plan_path = tmp_path / "plan.tsv"
    from godmode_media_library.utils import write_tsv

    write_tsv(
        plan_path,
        ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
        [("u1", str(src_file), str(dst / "photo.jpg"), "time", "2024/01", "", "0")],
    )

    log_path = tmp_path / "log.tsv"
    applied, skipped = apply_tree_plan(
        plan_path=plan_path,
        operation="copy",
        dry_run=False,
        collision_policy="skip",
        log_path=log_path,
    )

    assert applied == 1
    assert skipped == 0
    assert (dst / "photo.jpg").exists()
    assert src_file.exists()  # original still exists for copy
