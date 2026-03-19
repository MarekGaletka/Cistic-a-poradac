from __future__ import annotations

from pathlib import Path

from godmode_media_library.models import DuplicateRow, FileRecord, PlanPolicy
from godmode_media_library.planning import _score, create_plan, write_plan_files


def _rec(path: str, birthtime: float = 1700000000.0, xattr: int = 0, component: bool = False, size: int = 1024) -> FileRecord:
    return FileRecord(
        path=Path(path),
        size=size,
        mtime=1700000000.0,
        ctime=1700000000.0,
        birthtime=birthtime,
        ext="jpg",
        meaningful_xattr_count=xattr,
        asset_key=None,
        asset_component=component,
    )


def test_score_prefer_roots():
    policy = PlanPolicy(prefer_roots=("/preferred/root",))
    rec_preferred = _rec("/preferred/root/photo.jpg")
    rec_other = _rec("/other/root/photo.jpg")
    assert _score(rec_preferred, policy) > _score(rec_other, policy)


def test_score_earliest_origin():
    policy = PlanPolicy(prefer_earliest_origin_time=True)
    rec_old = _rec("/photos/old.jpg", birthtime=1600000000.0)
    rec_new = _rec("/photos/new.jpg", birthtime=1700000000.0)
    assert _score(rec_old, policy) > _score(rec_new, policy)


def test_create_plan_basic():
    digest = "a" * 64
    dupes = [
        DuplicateRow(digest=digest, size=1024, path=Path("/a/photo.jpg")),
        DuplicateRow(digest=digest, size=1024, path=Path("/b/photo.jpg")),
    ]
    inventory = {
        Path("/a/photo.jpg"): _rec("/a/photo.jpg", birthtime=1600000000.0),
        Path("/b/photo.jpg"): _rec("/b/photo.jpg", birthtime=1700000000.0),
    }
    policy = PlanPolicy()
    plan_rows, manual_rows = create_plan(dupes, inventory, policy)

    assert len(plan_rows) == 1
    assert len(manual_rows) == 0
    row = plan_rows[0]
    # The older file should be kept
    assert row.keep_path == Path("/a/photo.jpg")
    assert row.move_path == Path("/b/photo.jpg")


def test_create_plan_asset_component_protected():
    digest = "b" * 64
    dupes = [
        DuplicateRow(digest=digest, size=1024, path=Path("/a/live.jpg")),
        DuplicateRow(digest=digest, size=1024, path=Path("/b/live.jpg")),
    ]
    inventory = {
        Path("/a/live.jpg"): _rec("/a/live.jpg", component=True),
        Path("/b/live.jpg"): _rec("/b/live.jpg", component=True),
    }
    policy = PlanPolicy(protect_asset_components=True)
    plan_rows, manual_rows = create_plan(dupes, inventory, policy)

    assert len(plan_rows) == 0
    assert len(manual_rows) == 2
    assert all(r.reason == "asset_component_protected" for r in manual_rows)


def test_create_plan_missing_inventory():
    digest = "c" * 64
    dupes = [
        DuplicateRow(digest=digest, size=1024, path=Path("/a/gone.jpg")),
        DuplicateRow(digest=digest, size=1024, path=Path("/b/gone.jpg")),
    ]
    # Only one record in inventory — the other is missing
    inventory = {
        Path("/a/gone.jpg"): _rec("/a/gone.jpg"),
    }
    policy = PlanPolicy()
    plan_rows, manual_rows = create_plan(dupes, inventory, policy)

    assert len(plan_rows) == 0
    assert len(manual_rows) == 2
    assert all(r.reason == "missing_inventory_record" for r in manual_rows)


def test_write_plan_files_creates_tsvs(tmp_path: Path):
    from godmode_media_library.models import ManualReviewRow, PlanRow

    plan_rows = [
        PlanRow(
            digest="a" * 64,
            size=1024,
            keep_path=Path("/a/photo.jpg"),
            move_path=Path("/b/photo.jpg"),
            reason="score_based_primary_selection",
            keep_score=100.0,
            move_score=50.0,
        )
    ]
    manual_rows = [
        ManualReviewRow(
            digest="b" * 64,
            size=2048,
            path=Path("/c/photo.jpg"),
            reason="asset_component_protected",
        )
    ]
    write_plan_files(tmp_path, plan_rows, manual_rows)

    assert (tmp_path / "plan_quarantine.tsv").exists()
    assert (tmp_path / "manual_review.tsv").exists()
