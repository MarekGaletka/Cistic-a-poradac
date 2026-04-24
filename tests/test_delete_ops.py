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
        [
            "inode_id",
            "path",
            "action",
            "primary_path",
            "asset_key",
            "selected_seed",
            "unit_size",
            "nlink_expected",
            "nlink_scanned",
            "external_links",
            "note",
        ],
        [(iid, str(f1), "move_primary", str(f1), "", "1", str(f1.stat().st_size), "1", "1", "0", "quarantine_primary_copy")],
    )

    quarantine = tmp_path / "quarantine"
    log_path = tmp_path / "log.tsv"

    result = apply_delete_plan(
        plan_path=plan_path,
        quarantine_root=quarantine,
        log_path=log_path,
        dry_run=False,
        yes=True,
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
        [
            "inode_id",
            "path",
            "action",
            "primary_path",
            "asset_key",
            "selected_seed",
            "unit_size",
            "nlink_expected",
            "nlink_scanned",
            "external_links",
            "note",
        ],
        [(iid, str(f1), "move_primary", str(f1), "", "1", str(f1.stat().st_size), "1", "1", "0", "quarantine_primary_copy")],
    )

    quarantine = tmp_path / "quarantine"
    log_path = tmp_path / "log.tsv"

    result = apply_delete_plan(
        plan_path=plan_path,
        quarantine_root=quarantine,
        log_path=log_path,
        dry_run=True,
        yes=True,
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

    file_size = str(f1.stat().st_size)
    plan_path = tmp_path / "plan.tsv"
    header = [
        "inode_id",
        "path",
        "action",
        "primary_path",
        "asset_key",
        "selected_seed",
        "unit_size",
        "nlink_expected",
        "nlink_scanned",
        "external_links",
        "note",
    ]
    # unlink_alias now requires the primary to have been moved first,
    # so include a move_primary row for f1 before unlink_alias for f2.
    write_tsv(
        plan_path,
        header,
        [
            (iid, str(f1), "move_primary", str(f1), "", "0", file_size, "2", "2", "0", "quarantine_primary_copy"),
            (iid, str(f2), "unlink_alias", str(f1), "", "0", file_size, "0", "2", "0", "remove_extra_hardlink_alias"),
        ],
    )

    quarantine = tmp_path / "quarantine"
    log_path = tmp_path / "log.tsv"

    result = apply_delete_plan(
        plan_path=plan_path,
        quarantine_root=quarantine,
        log_path=log_path,
        dry_run=False,
        yes=True,
    )
    assert result.moved_primary == 1
    assert result.unlinked_aliases == 1
    assert not f2.exists()
    assert not f1.exists()  # f1 was moved to quarantine


def test_apply_delete_plan_manual_review(tmp_path: Path):
    """Test that manual_review_external_links action is counted."""
    plan_path = tmp_path / "plan.tsv"
    write_tsv(
        plan_path,
        [
            "inode_id",
            "path",
            "action",
            "primary_path",
            "asset_key",
            "selected_seed",
            "unit_size",
            "nlink_expected",
            "nlink_scanned",
            "external_links",
            "note",
        ],
        [
            (
                "iid",
                str(tmp_path / "photo.jpg"),
                "manual_review_external_links",
                str(tmp_path / "photo.jpg"),
                "",
                "1",
                "100",
                "3",
                "1",
                "2",
                "nlink=3;scanned_links=1",
            )
        ],
    )

    log_path = tmp_path / "log.tsv"
    result = apply_delete_plan(
        plan_path=plan_path,
        quarantine_root=tmp_path / "quarantine",
        log_path=log_path,
        dry_run=False,
        yes=True,
    )
    assert result.manual_review == 1
    assert result.moved_primary == 0


def test_apply_delete_plan_skip_missing(tmp_path: Path):
    """Test that missing paths are skipped."""
    plan_path = tmp_path / "plan.tsv"
    write_tsv(
        plan_path,
        [
            "inode_id",
            "path",
            "action",
            "primary_path",
            "asset_key",
            "selected_seed",
            "unit_size",
            "nlink_expected",
            "nlink_scanned",
            "external_links",
            "note",
        ],
        [
            (
                "iid",
                str(tmp_path / "nonexistent.jpg"),
                "move_primary",
                str(tmp_path / "nonexistent.jpg"),
                "",
                "1",
                "100",
                "1",
                "1",
                "0",
                "quarantine_primary_copy",
            )
        ],
    )

    log_path = tmp_path / "log.tsv"
    result = apply_delete_plan(
        plan_path=plan_path,
        quarantine_root=tmp_path / "quarantine",
        log_path=log_path,
        dry_run=False,
        yes=True,
    )
    assert result.skipped == 1
    assert result.moved_primary == 0


# ── Quarantine path tests ────────────────────────────────────────────

import pytest

from godmode_media_library.delete_ops import (
    _allocate_dest,
    _format_bytes,
    _inode_id,
    _inode_key,
    _load_selected_paths,
    _pick_primary,
    _quarantine_path,
)


class TestQuarantinePath:
    def test_basic_path(self, tmp_path: Path):
        qroot = tmp_path / "quarantine"
        qroot.mkdir()
        result = _quarantine_path(qroot, Path("/home/user/photo.jpg"))
        assert str(result).startswith(str(qroot))
        assert "photo.jpg" in str(result)

    def test_rejects_dotdot(self, tmp_path: Path):
        qroot = tmp_path / "quarantine"
        qroot.mkdir()
        with pytest.raises(ValueError, match="Path traversal"):
            _quarantine_path(qroot, Path("/home/../etc/passwd"))

    def test_windows_drive_path(self, tmp_path: Path):
        qroot = tmp_path / "quarantine"
        qroot.mkdir()
        result = _quarantine_path(qroot, Path("C:/Users/test/photo.jpg"))
        assert "_drive_" in str(result)
        assert "photo.jpg" in str(result)

    def test_path_without_drive(self, tmp_path: Path):
        qroot = tmp_path / "quarantine"
        qroot.mkdir()
        result = _quarantine_path(qroot, Path("/usr/local/file.txt"))
        assert str(result).startswith(str(qroot))


class TestAllocateDest:
    def test_returns_dest_when_free(self, tmp_path: Path):
        dest = tmp_path / "photo.jpg"
        result = _allocate_dest(dest)
        assert result == dest

    def test_increments_suffix_when_exists(self, tmp_path: Path):
        dest = tmp_path / "photo.jpg"
        dest.write_bytes(b"data")
        result = _allocate_dest(dest)
        assert result.name == "photo.dup1.jpg"
        assert result.parent == tmp_path

    def test_bounded_loop(self, tmp_path: Path):
        """Verify collision avoidance stops after max attempts."""
        dest = tmp_path / "photo.jpg"
        dest.write_bytes(b"data")
        # Create first few .dup files
        for i in range(1, 4):
            (tmp_path / f"photo.dup{i}.jpg").write_bytes(b"data")
        result = _allocate_dest(dest)
        assert result.name == "photo.dup4.jpg"


class TestInodeHelpers:
    def test_inode_key_existing_file(self, tmp_path: Path):
        f = tmp_path / "file.txt"
        f.write_bytes(b"hello")
        key = _inode_key(f)
        assert key is not None
        assert len(key) == 2

    def test_inode_key_missing_file(self, tmp_path: Path):
        key = _inode_key(tmp_path / "nonexistent")
        assert key is None

    def test_inode_id_deterministic(self):
        key = (12345, 67890)
        id1 = _inode_id(key)
        id2 = _inode_id(key)
        assert id1 == id2
        assert len(id1) == 24


class TestFormatBytes:
    def test_bytes(self):
        assert _format_bytes(100) == "100 B"

    def test_kib(self):
        assert _format_bytes(2048) == "2.0 KiB"

    def test_mib(self):
        assert _format_bytes(3 * 1024**2) == "3.0 MiB"

    def test_gib(self):
        assert _format_bytes(5 * 1024**3) == "5.00 GiB"


class TestLoadSelectedPaths:
    def test_from_file(self, tmp_path: Path):
        sel = tmp_path / "selected.txt"
        f1 = tmp_path / "photo.jpg"
        f1.write_bytes(b"data")
        sel.write_text(f"# comment\n{f1}\n\n", encoding="utf-8")
        selected, warnings = _load_selected_paths(sel, None)
        assert len(selected) >= 1
        assert len(warnings) == 0

    def test_empty_returns_warning(self, tmp_path: Path):
        sel = tmp_path / "selected.txt"
        sel.write_text("# only comments\n", encoding="utf-8")
        selected, warnings = _load_selected_paths(sel, None)
        assert len(selected) == 0
        assert len(warnings) == 1

    def test_none_inputs(self):
        selected, warnings = _load_selected_paths(None, None)
        assert len(selected) == 0
        assert len(warnings) == 1

    def test_recommendations_tsv(self, tmp_path: Path):
        rec = tmp_path / "recs.tsv"
        f1 = tmp_path / "photo.jpg"
        f1.write_bytes(b"data")
        rec.write_text(
            "action\trequires_manual_review\tpath\n"
            f"quarantine_candidate\t0\t{f1}\n"
            f"quarantine_candidate\t1\t{f1}\n"  # manual review = skip
            f"keep\t0\t{f1}\n",  # wrong action = skip
            encoding="utf-8",
        )
        selected, warnings = _load_selected_paths(None, rec)
        assert len(selected) == 1


class TestPickPrimary:
    def test_prefers_root(self, tmp_path: Path):
        p1 = tmp_path / "a" / "photo.jpg"
        p2 = tmp_path / "b" / "photo.jpg"
        p1.parent.mkdir(parents=True)
        p2.parent.mkdir(parents=True)
        p1.write_bytes(b"data")
        p2.write_bytes(b"data")
        primary = _pick_primary([p1, p2], prefer_roots=(str(tmp_path / "b"),))
        assert primary == p2

    def test_no_prefer_roots(self, tmp_path: Path):
        p1 = tmp_path / "photo.jpg"
        p1.write_bytes(b"data")
        primary = _pick_primary([p1], prefer_roots=())
        assert primary == p1


class TestApplyDeletePlanUnlinkNlinkCheck:
    """Test nlink safety checks in unlink_alias action."""

    def test_unlink_alias_skipped_without_primary_move(self, tmp_path: Path):
        """unlink_alias skipped when primary not moved first."""
        import os

        root = tmp_path / "root"
        root.mkdir()
        f1 = root / "photo.jpg"
        f1.write_bytes(b"DATA" * 50)
        f2 = root / "link.jpg"
        os.link(f1, f2)

        from godmode_media_library.delete_ops import _inode_id, _inode_key

        ikey = _inode_key(f1)
        iid = _inode_id(ikey)

        plan_path = tmp_path / "plan.tsv"
        write_tsv(
            plan_path,
            [
                "inode_id",
                "path",
                "action",
                "primary_path",
                "asset_key",
                "selected_seed",
                "unit_size",
                "nlink_expected",
                "nlink_scanned",
                "external_links",
                "note",
            ],
            [(iid, str(f2), "unlink_alias", str(f1), "", "0", "200", "2", "2", "0", "")],
        )
        log_path = tmp_path / "log.tsv"
        result = apply_delete_plan(
            plan_path=plan_path,
            quarantine_root=tmp_path / "q",
            log_path=log_path,
            dry_run=False,
            yes=True,
        )
        assert result.skipped == 1
        assert result.unlinked_aliases == 0
        assert f2.exists()

    def test_unknown_action_skipped(self, tmp_path: Path):
        """Unknown actions in the plan are skipped."""
        root = tmp_path / "root"
        root.mkdir()
        f1 = root / "file.txt"
        f1.write_bytes(b"data")

        plan_path = tmp_path / "plan.tsv"
        write_tsv(
            plan_path,
            [
                "inode_id",
                "path",
                "action",
                "primary_path",
                "asset_key",
                "selected_seed",
                "unit_size",
                "nlink_expected",
                "nlink_scanned",
                "external_links",
                "note",
            ],
            [("iid", str(f1), "unknown_action", str(f1), "", "0", "100", "1", "1", "0", "")],
        )
        log_path = tmp_path / "log.tsv"
        result = apply_delete_plan(
            plan_path=plan_path,
            quarantine_root=tmp_path / "q",
            log_path=log_path,
            dry_run=False,
            yes=True,
        )
        assert result.skipped == 1
