from __future__ import annotations

from pathlib import Path

from godmode_media_library.audit import (
    collect_file_records,
    duplicate_group_summary,
    exact_duplicates,
    load_exact_duplicates,
    load_inventory,
    write_audit_run,
)
from godmode_media_library.models import DuplicateRow


def test_collect_file_records(tmp_media_tree: Path):
    records = collect_file_records([tmp_media_tree])
    paths = {r.path.name for r in records}
    # Should find all files across nested dirs
    assert "photo1.jpg" in paths
    assert "photo2.jpg" in paths
    assert "photo3.jpg" in paths
    assert "photo1.mov" in paths
    assert "photo1.aae" in paths
    assert "readme.pdf" in paths
    assert ".DS_Store" in paths
    assert "._photo1.jpg" in paths

    # Verify correct extensions
    for rec in records:
        if rec.path.name == "photo1.jpg":
            assert rec.ext == "jpg"
        if rec.path.name == "readme.pdf":
            assert rec.ext == "pdf"


def test_exact_duplicates_finds_duplicates(tmp_media_tree: Path):
    records = collect_file_records([tmp_media_tree])
    dupes = exact_duplicates(records, min_size_bytes=0, dedup_exts={"jpg"})
    # photo1.jpg and photo3.jpg have the same content
    dupe_paths = {str(d.path.name) for d in dupes}
    assert "photo1.jpg" in dupe_paths
    assert "photo3.jpg" in dupe_paths
    # photo2.jpg should not be in duplicates (different content)
    assert "photo2.jpg" not in dupe_paths


def test_exact_duplicates_min_size_filter(tmp_media_tree: Path):
    records = collect_file_records([tmp_media_tree])
    # Set min_size very high — nothing should qualify
    dupes = exact_duplicates(records, min_size_bytes=999_999_999, dedup_exts={"jpg"})
    assert len(dupes) == 0


def test_exact_duplicates_ext_filter(tmp_media_tree: Path):
    records = collect_file_records([tmp_media_tree])
    # Only look at "png" — no png files exist
    dupes = exact_duplicates(records, min_size_bytes=0, dedup_exts={"png"})
    assert len(dupes) == 0


def test_duplicate_group_summary():
    digest = "a" * 64
    rows = [
        DuplicateRow(digest=digest, size=1000, path=Path("/a/img1.jpg")),
        DuplicateRow(digest=digest, size=1000, path=Path("/b/img1.jpg")),
        DuplicateRow(digest=digest, size=1000, path=Path("/c/img1.jpg")),
    ]
    summary = duplicate_group_summary(rows)
    assert len(summary) == 1
    count, size, reclaimable, d = summary[0]
    assert count == 3
    assert size == 1000
    assert reclaimable == 2000  # (3-1) * 1000
    assert d == digest


def test_write_audit_run_creates_files(tmp_media_tree: Path, tmp_path: Path):
    out_dir = tmp_path / "audit_output"
    run_dir = write_audit_run(
        roots=[tmp_media_tree],
        out_dir=out_dir,
        min_size_bytes=0,
        run_name="test_run",
    )
    assert run_dir.is_dir()
    expected_files = [
        "file_inventory.tsv",
        "extension_counts.tsv",
        "files_over_threshold.tsv",
        "exact_duplicates.tsv",
        "duplicate_groups_summary.tsv",
        "asset_sets.tsv",
        "summary.json",
    ]
    for fname in expected_files:
        assert (run_dir / fname).exists(), f"Missing: {fname}"


def test_load_inventory_roundtrip(tmp_media_tree: Path, tmp_path: Path):
    out_dir = tmp_path / "audit_inv"
    run_dir = write_audit_run(
        roots=[tmp_media_tree],
        out_dir=out_dir,
        min_size_bytes=0,
        run_name="inv_test",
    )
    inventory = load_inventory(run_dir / "file_inventory.tsv")
    assert len(inventory) > 0
    # All keys should be Path objects
    for key in inventory:
        assert isinstance(key, Path)
    # Spot-check a record
    for _path, rec in inventory.items():
        assert rec.size >= 0
        assert rec.ext != "" or rec.path.suffix == ""


def test_load_exact_duplicates_roundtrip(tmp_media_tree: Path, tmp_path: Path):
    out_dir = tmp_path / "audit_dupes"
    run_dir = write_audit_run(
        roots=[tmp_media_tree],
        out_dir=out_dir,
        min_size_bytes=0,
        run_name="dupe_test",
    )
    dupes = load_exact_duplicates(run_dir / "exact_duplicates.tsv")
    assert isinstance(dupes, list)
    for d in dupes:
        assert isinstance(d, DuplicateRow)
        assert isinstance(d.path, Path)
