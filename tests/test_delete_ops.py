from __future__ import annotations

from pathlib import Path

from godmode_media_library.delete_ops import apply_delete_plan, create_delete_plan
from godmode_media_library.utils import write_tsv


def test_create_delete_plan_basic(tmp_path: Path):
    root = tmp_path / "root"
    root.mkdir()
    f1 = root / "photo1.jpg"
    f2 = root / "photo2.jpg"
    f1.write_bytes(b"CONTENT_A" * 100)
    f2.write_bytes(b"CONTENT_B" * 100)

    # Create a select_paths file pointing to both
    select = tmp_path / "select.txt"
    select.write_text(f"{f1}\n{f2}\n", encoding="utf-8")

    plan_path = tmp_path / "delete_plan.tsv"
    summary_path = tmp_path / "delete_summary.json"

    result = create_delete_plan(
        roots=[root],
        plan_path=plan_path,
        summary_path=summary_path,
        select_paths=select,
    )
    assert result.selected_seed_paths == 2
    assert result.expanded_paths_total >= 2
    assert plan_path.exists()
    assert summary_path.exists()


def test_create_delete_plan_asset_expansion(tmp_path: Path):
    root = tmp_path / "root"
    root.mkdir()
    # Create a Live Photo pair (jpg + mov with same stem)
    jpg = root / "IMG_001.jpg"
    mov = root / "IMG_001.mov"
    jpg.write_bytes(b"JPEG" * 100)
    mov.write_bytes(b"MOVV" * 100)

    # Only select the jpg
    select = tmp_path / "select.txt"
    select.write_text(f"{jpg.resolve()}\n", encoding="utf-8")

    plan_path = tmp_path / "delete_plan.tsv"
    summary_path = tmp_path / "delete_summary.json"

    result = create_delete_plan(
        roots=[root],
        plan_path=plan_path,
        summary_path=summary_path,
        select_paths=select,
        include_asset_sets=True,
    )
    # The mov should be expanded as an asset sibling
    assert result.expanded_paths_total >= 2
    assert result.expanded_by_asset >= 1


def test_apply_delete_plan_moves_primary(tmp_path: Path):
    root = tmp_path / "root"
    root.mkdir()
    f1 = root / "photo.jpg"
    f1.write_bytes(b"DATA" * 50)

    plan_path = tmp_path / "plan.tsv"
    # Simulate a delete plan with move_primary action
    from godmode_media_library.delete_ops import _inode_id, _inode_key

    ikey = _inode_key(f1)
    iid = _inode_id(ikey) if ikey else "test_inode"

    write_tsv(
        plan_path,
        ["inode_id", "path", "action", "primary_path", "asset_key", "selected_seed",
         "unit_size", "nlink_expected", "nlink_scanned", "external_links", "note"],
        [(iid, str(f1), "move_primary", str(f1), "", "1",
          str(f1.stat().st_size), "1", "1", "0", "quarantine_primary_copy")],
    )

    quarantine = tmp_path / "quarantine"
    log_path = tmp_path / "log.tsv"

    result = apply_delete_plan(
        plan_path=plan_path,
        quarantine_root=quarantine,
        log_path=log_path,
        dry_run=False,
    )
    assert result.moved_primary == 1
    assert not f1.exists()


def test_apply_delete_plan_dry_run(tmp_path: Path):
    root = tmp_path / "root"
    root.mkdir()
    f1 = root / "photo.jpg"
    f1.write_bytes(b"DATA" * 50)

    plan_path = tmp_path / "plan.tsv"
    from godmode_media_library.delete_ops import _inode_id, _inode_key

    ikey = _inode_key(f1)
    iid = _inode_id(ikey) if ikey else "test_inode"

    write_tsv(
        plan_path,
        ["inode_id", "path", "action", "primary_path", "asset_key", "selected_seed",
         "unit_size", "nlink_expected", "nlink_scanned", "external_links", "note"],
        [(iid, str(f1), "move_primary", str(f1), "", "1",
          str(f1.stat().st_size), "1", "1", "0", "quarantine_primary_copy")],
    )

    quarantine = tmp_path / "quarantine"
    log_path = tmp_path / "log.tsv"

    result = apply_delete_plan(
        plan_path=plan_path,
        quarantine_root=quarantine,
        log_path=log_path,
        dry_run=True,
    )
    assert result.moved_primary == 1
    # File should still exist in dry run
    assert f1.exists()


def test_apply_delete_plan_unlink_alias(tmp_path: Path):
    """Test that unlink_alias action removes the file."""
    import os

    root = tmp_path / "root"
    root.mkdir()
    f1 = root / "photo.jpg"
    f1.write_bytes(b"DATA" * 50)
    # Create a hardlink alias
    f2 = root / "photo_link.jpg"
    os.link(f1, f2)

    from godmode_media_library.delete_ops import _inode_id, _inode_key

    ikey = _inode_key(f1)
    iid = _inode_id(ikey) if ikey else "test_inode"

    plan_path = tmp_path / "plan.tsv"
    write_tsv(
        plan_path,
        ["inode_id", "path", "action", "primary_path", "asset_key", "selected_seed",
         "unit_size", "nlink_expected", "nlink_scanned", "external_links", "note"],
        [(iid, str(f2), "unlink_alias", str(f1), "", "0",
          str(f1.stat().st_size), "2", "2", "0", "remove_extra_hardlink_alias")],
    )

    quarantine = tmp_path / "quarantine"
    log_path = tmp_path / "log.tsv"

    result = apply_delete_plan(
        plan_path=plan_path,
        quarantine_root=quarantine,
        log_path=log_path,
        dry_run=False,
    )
    assert result.unlinked_aliases == 1
    assert not f2.exists()
    assert f1.exists()


def test_apply_delete_plan_manual_review(tmp_path: Path):
    """Test that manual_review_external_links action is counted."""
    plan_path = tmp_path / "plan.tsv"
    write_tsv(
        plan_path,
        ["inode_id", "path", "action", "primary_path", "asset_key", "selected_seed",
         "unit_size", "nlink_expected", "nlink_scanned", "external_links", "note"],
        [("iid", str(tmp_path / "photo.jpg"), "manual_review_external_links", str(tmp_path / "photo.jpg"), "", "1",
          "100", "3", "1", "2", "nlink=3;scanned_links=1")],
    )

    log_path = tmp_path / "log.tsv"
    result = apply_delete_plan(
        plan_path=plan_path,
        quarantine_root=tmp_path / "quarantine",
        log_path=log_path,
        dry_run=False,
    )
    assert result.manual_review == 1
    assert result.moved_primary == 0


def test_apply_delete_plan_skip_missing(tmp_path: Path):
    """Test that missing paths are skipped."""
    plan_path = tmp_path / "plan.tsv"
    write_tsv(
        plan_path,
        ["inode_id", "path", "action", "primary_path", "asset_key", "selected_seed",
         "unit_size", "nlink_expected", "nlink_scanned", "external_links", "note"],
        [("iid", str(tmp_path / "nonexistent.jpg"), "move_primary", str(tmp_path / "nonexistent.jpg"), "", "1",
          "100", "1", "1", "0", "quarantine_primary_copy")],
    )

    log_path = tmp_path / "log.tsv"
    result = apply_delete_plan(
        plan_path=plan_path,
        quarantine_root=tmp_path / "quarantine",
        log_path=log_path,
        dry_run=False,
    )
    assert result.skipped == 1
    assert result.moved_primary == 0
