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


# ── Collision tracking & reserved set accumulation ───────────────────

import pytest
from godmode_media_library.tree_ops import (
    _allocate_destination,
    _bucket_for,
    _bucket_for_type_unit,
    _load_labels,
    _origin_ts,
    _pick_anchor,
    _unit_id,
)
from godmode_media_library.models import FileRecord


class TestAllocateDestination:
    def test_no_collision(self, tmp_path: Path):
        reserved = set()
        reserved_norm = set()
        dest = tmp_path / "photo.jpg"
        result = _allocate_destination(dest, reserved, reserved_norm)
        assert result == dest
        assert dest in reserved

    def test_collision_with_reserved(self, tmp_path: Path):
        reserved = set()
        reserved_norm = set()
        dest = tmp_path / "photo.jpg"
        # First allocation
        r1 = _allocate_destination(dest, reserved, reserved_norm)
        assert r1 == dest
        # Second allocation for same dest -> should rename
        r2 = _allocate_destination(dest, reserved, reserved_norm)
        assert r2 != dest
        assert r2.name == "photo (1).jpg"
        assert r2 in reserved

    def test_collision_with_existing_file(self, tmp_path: Path):
        reserved = set()
        reserved_norm = set()
        dest = tmp_path / "photo.jpg"
        dest.write_bytes(b"exists")
        result = _allocate_destination(dest, reserved, reserved_norm)
        assert result.name == "photo (1).jpg"

    def test_multiple_collisions(self, tmp_path: Path):
        reserved = set()
        reserved_norm = set()
        dest = tmp_path / "photo.jpg"
        # Allocate same dest 5 times
        results = []
        for _ in range(5):
            results.append(_allocate_destination(dest, reserved, reserved_norm))
        names = [r.name for r in results]
        assert names[0] == "photo.jpg"
        assert names[1] == "photo (1).jpg"
        assert names[2] == "photo (2).jpg"
        assert len(set(names)) == 5

    def test_reserved_norm_none_builds_from_reserved(self, tmp_path: Path):
        """When reserved_norm is None, it builds from reserved set."""
        dest = tmp_path / "photo.jpg"
        reserved = {dest}
        result = _allocate_destination(dest, reserved, None)
        assert result.name == "photo (1).jpg"


class TestOriginTs:
    def test_uses_birthtime_if_available(self, make_file_record):
        rec = make_file_record(birthtime=1000.0, mtime=2000.0)
        assert _origin_ts(rec) == 1000.0

    def test_uses_mtime_if_no_birthtime(self, make_file_record):
        rec = make_file_record(birthtime=None, mtime=2000.0)
        assert _origin_ts(rec) == 2000.0

    def test_uses_mtime_if_birthtime_zero(self, make_file_record):
        rec = make_file_record(birthtime=0.0, mtime=2000.0)
        assert _origin_ts(rec) == 2000.0


class TestUnitId:
    def test_deterministic(self):
        assert _unit_id("key1") == _unit_id("key1")

    def test_different_keys_differ(self):
        assert _unit_id("key1") != _unit_id("key2")

    def test_length(self):
        assert len(_unit_id("x")) == 24


class TestLoadLabels:
    def test_none_path(self):
        result = _load_labels(None)
        assert result == {}

    def test_with_labels_file(self, tmp_path: Path):
        labels_file = tmp_path / "labels.tsv"
        labels_file.write_text(
            "path\tpeople\tplace\n"
            f"{tmp_path}/photo.jpg\tJohn\tPrague\n",
            encoding="utf-8",
        )
        result = _load_labels(labels_file)
        assert len(result) == 1
        key = list(result.keys())[0]
        assert result[key]["people"] == "John"
        assert result[key]["place"] == "Prague"


class TestPickAnchor:
    def test_prefers_raw_over_image(self, make_file_record):
        raw = make_file_record(path=Path("/tmp/photo.dng"), ext="dng")
        jpg = make_file_record(path=Path("/tmp/photo.jpg"), ext="jpg")
        assert _pick_anchor([jpg, raw]) == raw

    def test_prefers_image_over_video(self, make_file_record):
        jpg = make_file_record(path=Path("/tmp/photo.jpg"), ext="jpg")
        mp4 = make_file_record(path=Path("/tmp/photo.mp4"), ext="mp4")
        assert _pick_anchor([mp4, jpg]) == jpg


class TestBucketFor:
    def test_time_mode(self, make_file_record):
        rec = make_file_record()
        bucket = _bucket_for(rec, "time", "day", {}, "Unknown")
        parts = bucket.split("/")
        assert len(parts) == 3

    def test_modified_mode(self, make_file_record):
        rec = make_file_record()
        bucket = _bucket_for(rec, "modified", "month", {}, "Unknown")
        parts = bucket.split("/")
        assert len(parts) == 2

    def test_type_mode(self, make_file_record):
        rec = make_file_record(ext="jpg")
        bucket = _bucket_for(rec, "type", "day", {}, "Unknown")
        assert "images" in bucket

    def test_people_mode_with_labels(self, make_file_record):
        rec = make_file_record(path=Path("/tmp/photo.jpg"))
        labels = {Path("/tmp/photo.jpg"): {"people": "Alice", "place": "NYC"}}
        bucket = _bucket_for(rec, "people", "day", labels, "Unknown")
        assert bucket == "Alice"

    def test_people_mode_no_label(self, make_file_record):
        rec = make_file_record()
        bucket = _bucket_for(rec, "people", "day", {}, "Neznamy")
        assert bucket == "Neznamy"

    def test_place_mode(self, make_file_record):
        rec = make_file_record(path=Path("/tmp/photo.jpg"))
        labels = {Path("/tmp/photo.jpg"): {"people": "", "place": "Berlin"}}
        bucket = _bucket_for(rec, "place", "day", labels, "Unknown")
        assert bucket == "Berlin"

    def test_unsupported_mode(self, make_file_record):
        rec = make_file_record()
        with pytest.raises(ValueError, match="Unsupported mode"):
            _bucket_for(rec, "invalid_mode", "day", {}, "Unknown")


class TestBucketForTypeUnit:
    def test_live_photo(self, make_file_record):
        members = [
            make_file_record(ext="jpg", path=Path("/tmp/IMG.jpg")),
            make_file_record(ext="mov", path=Path("/tmp/IMG.mov")),
        ]
        bucket = _bucket_for_type_unit(members)
        assert "live_photo" in bucket

    def test_raw_plus_preview(self, make_file_record):
        members = [
            make_file_record(ext="dng", path=Path("/tmp/IMG.dng")),
            make_file_record(ext="jpg", path=Path("/tmp/IMG.jpg")),
        ]
        bucket = _bucket_for_type_unit(members)
        assert "raw_plus_preview" in bucket

    def test_video_bundle(self, make_file_record):
        members = [
            make_file_record(ext="mp4", path=Path("/tmp/vid.mp4")),
            make_file_record(ext="mov", path=Path("/tmp/vid.mov")),
        ]
        bucket = _bucket_for_type_unit(members)
        assert "video_bundle" in bucket

    def test_mixed(self, make_file_record):
        members = [
            make_file_record(ext="pdf", path=Path("/tmp/doc.pdf")),
            make_file_record(ext="txt", path=Path("/tmp/doc.txt")),
        ]
        bucket = _bucket_for_type_unit(members)
        assert "mixed" in bucket

    def test_single_type(self, make_file_record):
        members = [make_file_record(ext="jpg", path=Path("/tmp/photo.jpg"))]
        bucket = _bucket_for_type_unit(members)
        assert "images" in bucket


class TestApplyTreePlanOverwrite:
    def test_overwrite_policy(self, tmp_path: Path):
        src = tmp_path / "source"
        src.mkdir()
        src_file = src / "photo.jpg"
        src_file.write_bytes(b"NEW_DATA" * 50)

        dst_dir = tmp_path / "target"
        dst_dir.mkdir(parents=True)
        dst_file = dst_dir / "photo.jpg"
        dst_file.write_bytes(b"OLD_DATA" * 50)

        plan_path = tmp_path / "plan.tsv"
        from godmode_media_library.utils import write_tsv
        write_tsv(
            plan_path,
            ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
            [("u1", str(src_file), str(dst_file), "time", "2024/01", "", "0")],
        )

        log_path = tmp_path / "log.tsv"
        applied, skipped = apply_tree_plan(
            plan_path=plan_path, operation="copy", dry_run=False,
            collision_policy="overwrite", log_path=log_path,
        )
        assert applied == 1
        assert dst_file.read_bytes() == b"NEW_DATA" * 50

    def test_unsupported_operation_raises(self, tmp_path: Path):
        src = tmp_path / "source"
        src.mkdir()
        src_file = src / "photo.jpg"
        src_file.write_bytes(b"DATA" * 50)

        plan_path = tmp_path / "plan.tsv"
        from godmode_media_library.utils import write_tsv
        write_tsv(
            plan_path,
            ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
            [("u1", str(src_file), str(tmp_path / "target" / "photo.jpg"), "time", "2024/01", "", "0")],
        )

        log_path = tmp_path / "log.tsv"
        with pytest.raises(ValueError, match="Unsupported operation"):
            apply_tree_plan(
                plan_path=plan_path, operation="invalid_op", dry_run=False,
                collision_policy="skip", log_path=log_path,
            )

    def test_dry_run_no_changes(self, tmp_path: Path):
        src = tmp_path / "source"
        src.mkdir()
        src_file = src / "photo.jpg"
        src_file.write_bytes(b"DATA" * 50)

        plan_path = tmp_path / "plan.tsv"
        from godmode_media_library.utils import write_tsv
        write_tsv(
            plan_path,
            ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
            [("u1", str(src_file), str(tmp_path / "target" / "photo.jpg"), "time", "2024/01", "", "0")],
        )

        log_path = tmp_path / "log.tsv"
        applied, skipped = apply_tree_plan(
            plan_path=plan_path, operation="move", dry_run=True,
            collision_policy="skip", log_path=log_path,
        )
        assert applied == 1
        assert src_file.exists()  # not actually moved


class TestCreateTreePlanModes:
    def test_people_mode(self, tmp_path: Path):
        src = tmp_path / "source"
        src.mkdir()
        f = src / "photo.jpg"
        f.write_bytes(b"JPEG" * 100)
        labels = tmp_path / "labels.tsv"
        labels.write_text(
            f"path\tpeople\tplace\n{f.resolve()}\tAlice\tPrague\n",
            encoding="utf-8",
        )
        target = tmp_path / "target"
        rows = create_tree_plan(
            roots=[src], target_root=target, mode="people",
            labels_tsv=labels,
        )
        assert len(rows) >= 1
        assert any("Alice" in r.bucket for r in rows)

    def test_modified_mode(self, tmp_path: Path):
        src = tmp_path / "source"
        src.mkdir()
        (src / "photo.jpg").write_bytes(b"JPEG" * 100)
        target = tmp_path / "target"
        rows = create_tree_plan(
            roots=[src], target_root=target, mode="modified", granularity="year",
        )
        assert len(rows) >= 1
        for r in rows:
            assert r.mode == "modified"
