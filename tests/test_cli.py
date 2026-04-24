from __future__ import annotations

from pathlib import Path

import pytest

from godmode_media_library.cli import (
    build_parser,
    cmd_apply,
    cmd_audit,
    cmd_config_show,
    cmd_delete_apply,
    cmd_labels_template,
    cmd_plan,
    cmd_promote,
    cmd_prune_recommend,
    cmd_restore,
    cmd_tree_apply,
    cmd_tree_plan,
    main,
)
from godmode_media_library.utils import write_tsv


def test_build_parser():
    parser = build_parser()
    assert parser is not None
    assert parser.prog == "gml"


def test_parser_audit_args():
    parser = build_parser()
    args = parser.parse_args(["audit", "--roots", "/tmp/photos", "/tmp/videos"])
    assert args.command == "audit"
    assert args.roots == ["/tmp/photos", "/tmp/videos"]


def test_parser_tree_plan_args():
    parser = build_parser()
    args = parser.parse_args(
        [
            "tree-plan",
            "--roots",
            "/tmp/src",
            "--target-root",
            "/tmp/dst",
            "--mode",
            "time",
            "--granularity",
            "month",
        ]
    )
    assert args.command == "tree-plan"
    assert args.mode == "time"
    assert args.granularity == "month"

    # Verify mode choices are enforced
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "tree-plan",
                "--roots",
                "/tmp/src",
                "--target-root",
                "/tmp/dst",
                "--mode",
                "invalid_mode",
            ]
        )


def test_main_no_args_exits(monkeypatch):
    monkeypatch.setattr("sys.argv", ["gml"])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code != 0 or exc_info.value.code == 2


def test_parser_config_subcommand():
    parser = build_parser()
    args = parser.parse_args(["config"])
    assert args.command == "config"
    assert hasattr(args, "func")


def test_parser_verbose():
    parser = build_parser()
    args = parser.parse_args(["-v", "config"])
    assert args.verbose == 1

    args2 = parser.parse_args(["-vv", "config"])
    assert args2.verbose == 2


def test_cmd_config_show(capsys):
    import argparse

    args = argparse.Namespace()
    ret = cmd_config_show(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "min_size_kb" in captured.out
    assert "exiftool_bin" in captured.out


def test_cmd_apply_dry_run(tmp_path: Path, capsys):
    """Exercise cmd_apply via a plan with no valid entries."""
    plan_path = tmp_path / "plan.tsv"
    write_tsv(
        plan_path,
        ["hash", "size", "keep_path", "move_path", "reason", "keep_score", "move_score"],
        [("abc", "100", str(tmp_path / "keep.jpg"), str(tmp_path / "move.jpg"), "test", "100", "50")],
    )

    import argparse

    args = argparse.Namespace(
        plan=str(plan_path),
        quarantine_root=str(tmp_path / "quarantine"),
        dry_run=True,
    )
    ret = cmd_apply(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "skipped=" in captured.out


def test_cmd_restore_dry_run(tmp_path: Path, capsys):
    """Exercise cmd_restore with empty log."""
    log_path = tmp_path / "executed_log.tsv"
    write_tsv(
        log_path,
        ["hash", "size", "keep_path", "move_path", "quarantine_path", "reason", "verified_hash"],
        [],
    )

    import argparse

    args = argparse.Namespace(log=str(log_path), dry_run=True)
    ret = cmd_restore(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "restored=0" in captured.out


def test_parser_apply_subcommand():
    parser = build_parser()
    args = parser.parse_args(["apply", "--plan", "/tmp/plan.tsv", "--dry-run"])
    assert args.command == "apply"
    assert args.dry_run is True


def test_parser_restore_subcommand():
    parser = build_parser()
    args = parser.parse_args(["restore", "--log", "/tmp/log.tsv"])
    assert args.command == "restore"
    assert args.dry_run is False


def test_parser_promote_subcommand():
    parser = build_parser()
    args = parser.parse_args(["promote", "--manifest", "/tmp/manifest.tsv", "--dry-run"])
    assert args.command == "promote"
    assert args.dry_run is True


def test_parser_tree_apply_subcommand():
    parser = build_parser()
    args = parser.parse_args(
        [
            "tree-apply",
            "--plan",
            "/tmp/tree_plan.tsv",
            "--operation",
            "copy",
            "--collision-policy",
            "skip",
        ]
    )
    assert args.command == "tree-apply"
    assert args.operation == "copy"
    assert args.collision_policy == "skip"


def test_parser_delete_plan_subcommand():
    parser = build_parser()
    args = parser.parse_args(
        [
            "delete-plan",
            "--roots",
            "/tmp/root",
            "--out",
            "/tmp/plan.tsv",
            "--select-paths",
            "/tmp/select.txt",
        ]
    )
    assert args.command == "delete-plan"


def test_parser_delete_apply_subcommand():
    parser = build_parser()
    args = parser.parse_args(
        [
            "delete-apply",
            "--plan",
            "/tmp/plan.tsv",
            "--quarantine-root",
            "/tmp/quarantine",
            "--dry-run",
        ]
    )
    assert args.command == "delete-apply"
    assert args.dry_run is True


def test_parser_prune_recommend_subcommand():
    parser = build_parser()
    args = parser.parse_args(
        [
            "prune-recommend",
            "--roots",
            "/tmp/root1",
            "/tmp/root2",
            "--out-dir",
            "/tmp/out",
        ]
    )
    assert args.command == "prune-recommend"
    assert len(args.roots) == 2


def test_cmd_promote_dry_run(tmp_path: Path, capsys):
    """Exercise cmd_promote with empty manifest."""
    manifest_path = tmp_path / "manifest.tsv"
    write_tsv(
        manifest_path,
        ["size", "moved_from", "quarantine_path", "primary_path"],
        [],
    )

    import argparse

    args = argparse.Namespace(
        manifest=str(manifest_path),
        backup_root=str(tmp_path / "backup"),
        dry_run=True,
    )
    ret = cmd_promote(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "swapped=0" in captured.out


def test_cmd_tree_apply_dry_run(tmp_path: Path, capsys):
    """Exercise cmd_tree_apply with empty plan."""
    plan_path = tmp_path / "tree_plan.tsv"
    write_tsv(
        plan_path,
        ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
        [],
    )

    import argparse

    args = argparse.Namespace(
        plan=str(plan_path),
        operation="move",
        dry_run=True,
        collision_policy="rename",
        log=None,
    )
    ret = cmd_tree_apply(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "applied=0" in captured.out


def test_main_with_config(monkeypatch, capsys):
    """Exercise main() with the config subcommand."""
    monkeypatch.setattr("sys.argv", ["gml", "config"])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "min_size_kb" in captured.out


def test_main_with_verbose_config(monkeypatch, capsys):
    """Exercise main() with -v flag."""
    monkeypatch.setattr("sys.argv", ["gml", "-v", "config"])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 0


def test_cmd_audit(tmp_media_tree, tmp_path, capsys):
    import argparse

    args = argparse.Namespace(
        roots=[str(tmp_media_tree)],
        out_dir=str(tmp_path / "audit_out"),
        run_name="test_run",
        min_size_kb=0,
        large_file_threshold_mb=500,
        allow_asset_component_dedupe=False,
        no_prefer_earliest_origin=False,
        no_prefer_richer_metadata=False,
        prefer_root=[],
    )
    ret = cmd_audit(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "run_dir=" in captured.out
    assert "plan_rows=" in captured.out


def test_cmd_plan(tmp_media_tree, tmp_path, capsys):
    import argparse

    # First run audit to get run_dir
    args_audit = argparse.Namespace(
        roots=[str(tmp_media_tree)],
        out_dir=str(tmp_path / "plan_out"),
        run_name="plan_run",
        min_size_kb=0,
        large_file_threshold_mb=500,
        allow_asset_component_dedupe=False,
        no_prefer_earliest_origin=False,
        no_prefer_richer_metadata=False,
        prefer_root=[],
    )
    cmd_audit(args_audit)

    args = argparse.Namespace(
        run_dir=str(tmp_path / "plan_out" / "plan_run"),
        allow_asset_component_dedupe=False,
        no_prefer_earliest_origin=False,
        no_prefer_richer_metadata=False,
        prefer_root=[],
    )
    ret = cmd_plan(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "plan_rows=" in captured.out


def test_cmd_prune_recommend(tmp_media_tree, tmp_path, capsys):
    import argparse

    args = argparse.Namespace(
        roots=[str(tmp_media_tree)],
        out_dir=str(tmp_path / "prune_out"),
        run_name="prune_test",
        min_size_kb=0,
        allow_asset_component_dedupe=False,
        no_prefer_earliest_origin=False,
        no_prefer_richer_metadata=False,
        prefer_root=[],
        no_system_noise=False,
    )
    ret = cmd_prune_recommend(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "total_recommendations=" in captured.out


def test_cmd_tree_plan(tmp_media_tree, tmp_path, capsys):
    import argparse

    args = argparse.Namespace(
        roots=[str(tmp_media_tree)],
        target_root=str(tmp_path / "tree_target"),
        mode="time",
        granularity="day",
        labels_tsv=None,
        unknown_label="Unknown",
        allow_asset_set_split=False,
        out_dir=str(tmp_path / "tree_out"),
        run_name="tree_test",
    )
    ret = cmd_tree_plan(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "rows=" in captured.out


def test_cmd_labels_template(tmp_media_tree, tmp_path, capsys):
    import argparse

    args = argparse.Namespace(
        roots=[str(tmp_media_tree)],
        out=str(tmp_path / "labels.tsv"),
        include_all=True,
    )
    ret = cmd_labels_template(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "rows=" in captured.out


def test_cmd_delete_apply_dry_run(tmp_path, capsys):
    import argparse

    plan_path = tmp_path / "del_plan.tsv"
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
        [],
    )
    args = argparse.Namespace(
        plan=str(plan_path),
        quarantine_root=str(tmp_path / "quarantine"),
        log=None,
        dry_run=True,
        yes=True,
    )
    ret = cmd_delete_apply(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "moved_primary=" in captured.out


# ═══════════════════════════════════════════════════════════════════════
# New tests for untested CLI commands (lines 299-1377)
# ═══════════════════════════════════════════════════════════════════════

import argparse
from dataclasses import dataclass, field
from unittest.mock import MagicMock, patch

# ── Helpers ──────────────────────────────────────────────────────────


def _make_catalog_file_row(**overrides):
    """Create a minimal CatalogFileRow-like object for mocking."""
    defaults = dict(
        id=1,
        path="/tmp/test.jpg",
        size=1024,
        mtime=1.0,
        ctime=1.0,
        birthtime=None,
        ext="jpg",
        sha256="abc123",
        inode=100,
        device=1,
        nlink=1,
        asset_key=None,
        asset_component=False,
        xattr_count=0,
        first_seen="2024-01-01",
        last_scanned="2024-01-01",
        duration_seconds=None,
        width=None,
        height=None,
        video_codec=None,
        audio_codec=None,
        bitrate=None,
        phash=None,
        date_original=None,
        camera_make=None,
        camera_model=None,
        gps_latitude=None,
        gps_longitude=None,
        metadata_richness=None,
    )
    defaults.update(overrides)
    return type("CatalogFileRow", (), defaults)()


def _mock_catalog(**method_returns):
    """Create a mock Catalog that works as a context manager."""
    cat = MagicMock()
    cat.__enter__ = MagicMock(return_value=cat)
    cat.__exit__ = MagicMock(return_value=False)
    cat.db_path = Path("/tmp/test_catalog.db")
    for method, retval in method_returns.items():
        getattr(cat, method).return_value = retval
    return cat


# ── --version flag ───────────────────────────────────────────────────


def test_version_flag(monkeypatch):
    monkeypatch.setattr("sys.argv", ["gml", "--version"])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 0


# ── Parameter validation in main() ──────────────────────────────────


def test_main_negative_min_size_kb(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["gml", "scan", "--roots", "/tmp", "--min-size-kb", "-1"])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 2


def test_main_invalid_port(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["gml", "serve", "--port", "99999"])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 2


def test_main_negative_workers(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["gml", "scan", "--roots", "/tmp", "--workers", "0"])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 2


def test_main_negative_logfile_max_mb(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["gml", "--logfile-max-mb", "0", "config"])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 2


# ── cmd_scan ─────────────────────────────────────────────────────────


def test_cmd_scan(capsys):
    from godmode_media_library.catalog import ScanStats
    from godmode_media_library.cli import cmd_scan

    mock_stats = ScanStats(root="/tmp", files_scanned=10, files_new=5, files_changed=2, files_removed=1, bytes_hashed=4096)
    mock_cat = _mock_catalog()

    with (
        patch("godmode_media_library.cli._get_catalog", return_value=mock_cat),
        patch("godmode_media_library.cli.incremental_scan", return_value=mock_stats),
    ):
        args = argparse.Namespace(
            roots=["/tmp/photos"],
            catalog=None,
            force_rehash=False,
            min_size_kb=0,
            no_media=False,
            no_phash=False,
            exiftool=False,
            exiftool_bin="exiftool",
            workers=1,
        )
        ret = cmd_scan(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "files_scanned=10" in out
    assert "files_new=5" in out


# ── cmd_query ────────────────────────────────────────────────────────


def test_cmd_query_files(capsys):
    from godmode_media_library.cli import cmd_query

    row = _make_catalog_file_row(camera_model="Canon EOS R5", duration_seconds=12.5)
    mock_cat = _mock_catalog(query_files=[row])

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(
            catalog=None,
            duplicates=False,
            ext=None,
            date_from=None,
            date_to=None,
            min_size=None,
            max_size=None,
            path_contains=None,
            camera=None,
            duration_min=None,
            duration_max=None,
            resolution_min=None,
            no_gps=False,
            limit=10000,
        )
        ret = cmd_query(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "Canon EOS R5" in out
    assert "12.5s" in out
    assert "1 files" in out


def test_cmd_query_duplicates(capsys):
    from godmode_media_library.cli import cmd_query

    row1 = _make_catalog_file_row(path="/a.jpg", size=100)
    row2 = _make_catalog_file_row(path="/b.jpg", size=100)
    mock_cat = _mock_catalog(query_duplicates=[("group1", [row1, row2])])

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(
            catalog=None,
            duplicates=True,
            ext=None,
            date_from=None,
            date_to=None,
            min_size=None,
            max_size=None,
            path_contains=None,
            camera=None,
            duration_min=None,
            duration_max=None,
            resolution_min=None,
            no_gps=False,
            limit=10000,
        )
        ret = cmd_query(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "group1" in out
    assert "1 duplicate groups" in out


# ── cmd_stats ────────────────────────────────────────────────────────


def test_cmd_stats(capsys):
    from godmode_media_library.cli import cmd_stats

    mock_cat = _mock_catalog(stats={"total_files": 42, "total_size": 9999})

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(catalog=None)
        ret = cmd_stats(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert '"total_files": 42' in out


# ── cmd_vacuum ───────────────────────────────────────────────────────


def test_cmd_vacuum(capsys):
    from godmode_media_library.cli import cmd_vacuum

    mock_cat = _mock_catalog()

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(catalog=None)
        ret = cmd_vacuum(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "VACUUM completed" in out
    mock_cat.vacuum.assert_called_once()


# ── cmd_similar ──────────────────────────────────────────────────────


def test_cmd_similar_no_hashes(capsys):
    from godmode_media_library.cli import cmd_similar

    mock_cat = _mock_catalog(get_all_phashes={})

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(catalog=None, threshold=10, out=None)
        ret = cmd_similar(args)
    assert ret == 0
    assert "No perceptual hashes" in capsys.readouterr().out


def test_cmd_similar_with_pairs(capsys, tmp_path):
    from godmode_media_library.cli import cmd_similar
    from godmode_media_library.perceptual_hash import SimilarPair

    mock_cat = _mock_catalog(get_all_phashes={"a": "0000000000000000", "b": "0000000000000001"})
    pair = SimilarPair(path_a="/a.jpg", path_b="/b.jpg", distance=1, hash_a="0000000000000000", hash_b="0000000000000001")

    out_file = tmp_path / "similar.tsv"
    with (
        patch("godmode_media_library.cli._get_catalog", return_value=mock_cat),
        patch("godmode_media_library.cli.find_similar", return_value=[pair]),
    ):
        args = argparse.Namespace(catalog=None, threshold=10, out=str(out_file))
        ret = cmd_similar(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "dist=1" in out
    assert f"out={out_file}" in out
    assert "1 similar pairs" in out


# ── cmd_verify ───────────────────────────────────────────────────────


def test_cmd_verify_ok(capsys):
    from godmode_media_library.cli import cmd_verify
    from godmode_media_library.verify import VerifyResult

    result = VerifyResult(total_checked=100, ok=100)
    mock_cat = _mock_catalog()

    with (
        patch("godmode_media_library.cli._get_catalog", return_value=mock_cat),
        patch("godmode_media_library.cli.verify_catalog", return_value=result),
    ):
        args = argparse.Namespace(catalog=None, check_hashes=False, limit=0)
        ret = cmd_verify(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "total_checked=100" in out
    assert "ok=100" in out


def test_cmd_verify_with_issues(capsys):
    from godmode_media_library.cli import cmd_verify
    from godmode_media_library.verify import VerifyResult

    result = VerifyResult(
        total_checked=10,
        ok=7,
        missing_files=["/gone.jpg"],
        size_mismatches=[("/diff.jpg", 100, 200)],
        hash_mismatches=[("/changed.jpg", "a" * 64, "b" * 64)],
    )
    mock_cat = _mock_catalog()

    with (
        patch("godmode_media_library.cli._get_catalog", return_value=mock_cat),
        patch("godmode_media_library.cli.verify_catalog", return_value=result),
    ):
        args = argparse.Namespace(catalog=None, check_hashes=True, limit=0)
        ret = cmd_verify(args)
    assert ret == 1  # has_issues
    out = capsys.readouterr().out
    assert "missing=1" in out
    assert "size_mismatches=1" in out
    assert "hash_mismatches=1" in out
    assert "/gone.jpg" in out
    assert "catalog=100 actual=200" in out


# ── cmd_export ───────────────────────────────────────────────────────


def test_cmd_export_files(capsys, tmp_path):
    from godmode_media_library.cli import cmd_export

    row = _make_catalog_file_row()
    mock_cat = _mock_catalog(query_files=[row])
    out_path = tmp_path / "export.csv"

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(
            catalog=None,
            what="files",
            out=str(out_path),
            format="csv",
            limit=1000,
        )
        ret = cmd_export(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "exported=1 rows" in out
    assert out_path.exists()


def test_cmd_export_duplicates(capsys, tmp_path):
    from godmode_media_library.cli import cmd_export

    row1 = _make_catalog_file_row(path="/a.jpg", size=100)
    row2 = _make_catalog_file_row(path="/b.jpg", size=100)
    mock_cat = _mock_catalog(query_duplicates=[("g1", [row1, row2])])
    out_path = tmp_path / "dupes.tsv"

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(
            catalog=None,
            what="duplicates",
            out=str(out_path),
            format="tsv",
            limit=1000,
        )
        ret = cmd_export(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "exported=2 rows" in out


def test_cmd_export_unknown_what(capsys, tmp_path):
    from godmode_media_library.cli import cmd_export

    mock_cat = _mock_catalog()
    out_path = tmp_path / "bad.csv"

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(
            catalog=None,
            what="unknown_thing",
            out=str(out_path),
            format="csv",
            limit=1000,
        )
        ret = cmd_export(args)
    assert ret == 1
    assert "Unknown export type" in capsys.readouterr().out


# ── cmd_cloud ────────────────────────────────────────────────────────


def test_cmd_cloud_no_rclone(capsys):
    from godmode_media_library.cli import cmd_cloud

    with (
        patch("godmode_media_library.cloud.check_rclone", return_value=False),
        patch("godmode_media_library.cloud.format_cloud_guide", return_value="Install rclone"),
    ):
        args = argparse.Namespace()
        ret = cmd_cloud(args)
    assert ret == 1
    assert "not installed" in capsys.readouterr().out


def test_cmd_cloud_no_remotes(capsys):
    from godmode_media_library.cli import cmd_cloud

    with (
        patch("godmode_media_library.cloud.check_rclone", return_value=True),
        patch("godmode_media_library.cloud.list_remotes", return_value=[]),
    ):
        args = argparse.Namespace()
        ret = cmd_cloud(args)
    assert ret == 1
    assert "no remotes configured" in capsys.readouterr().out


def test_cmd_cloud_with_remotes(capsys):
    from godmode_media_library.cli import cmd_cloud
    from godmode_media_library.cloud import RcloneRemote

    remote = RcloneRemote(name="gdrive", type="drive")
    with (
        patch("godmode_media_library.cloud.check_rclone", return_value=True),
        patch("godmode_media_library.cloud.list_remotes", return_value=[remote]),
        patch("godmode_media_library.cloud.mount_command", return_value="rclone mount gdrive: ~/mnt/gdrive"),
    ):
        args = argparse.Namespace()
        ret = cmd_cloud(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "gdrive (drive)" in out
    assert "1 remote(s)" in out


# ── cmd_serve ────────────────────────────────────────────────────────


def test_cmd_serve_no_uvicorn(capsys):
    import builtins

    from godmode_media_library.cli import cmd_serve

    real_import = builtins.__import__

    def mock_import(name, *a, **kw):
        if name == "uvicorn":
            raise ImportError("no uvicorn")
        return real_import(name, *a, **kw)

    with patch("builtins.__import__", side_effect=mock_import):
        args = argparse.Namespace(catalog=None, host="127.0.0.1", port=8080, no_browser=True)
        ret = cmd_serve(args)
    assert ret == 2
    assert "pip install" in capsys.readouterr().out


# ── cmd_watch ────────────────────────────────────────────────────────


def test_cmd_watch(capsys):
    from godmode_media_library.cli import cmd_watch

    mock_cat = _mock_catalog()
    with (
        patch("godmode_media_library.cli._get_catalog", return_value=mock_cat),
        patch("godmode_media_library.watcher.watch_roots") as mock_watch,
    ):
        # simulate watch_roots returning immediately (not blocking)
        mock_watch.return_value = None
        args = argparse.Namespace(roots=["/tmp/photos"], catalog=None)
        ret = cmd_watch(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "Watching 1 roots" in out
    mock_watch.assert_called_once()


# ── cmd_auto ─────────────────────────────────────────────────────────


def test_cmd_auto_success(capsys):
    from godmode_media_library.cli import cmd_auto

    @dataclass
    class FakeResult:
        files_scanned: int = 5
        files_new: int = 3
        metadata_extracted: int = 2
        duplicate_groups: int = 1
        merge_plans_created: int = 0
        tags_merged: int = 0
        errors: list = field(default_factory=list)

    with (
        patch("godmode_media_library.pipeline.run_pipeline", return_value=FakeResult()),
        patch("godmode_media_library.pipeline.PipelineConfig") as MockConfig,
    ):
        MockConfig.return_value = MagicMock()
        args = argparse.Namespace(
            roots=["/tmp/photos"],
            catalog=None,
            exiftool_bin="exiftool",
            dry_run=False,
            no_interactive=False,
            workers=1,
            min_size_kb=0,
            skip=[],
        )
        ret = cmd_auto(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "files_scanned=5" in out


def test_cmd_auto_with_errors(capsys):
    from godmode_media_library.cli import cmd_auto

    @dataclass
    class FakeResult:
        files_scanned: int = 5
        files_new: int = 3
        metadata_extracted: int = 0
        duplicate_groups: int = 0
        merge_plans_created: int = 0
        tags_merged: int = 0
        errors: list = field(default_factory=lambda: ["something broke"])

    with (
        patch("godmode_media_library.pipeline.run_pipeline", return_value=FakeResult()),
        patch("godmode_media_library.pipeline.PipelineConfig") as MockConfig,
    ):
        MockConfig.return_value = MagicMock()
        args = argparse.Namespace(
            roots=["/tmp/photos"],
            catalog=None,
            exiftool_bin="exiftool",
            dry_run=True,
            no_interactive=True,
            workers=2,
            min_size_kb=0,
            skip=["scan"],
        )
        ret = cmd_auto(args)
    assert ret == 1
    out = capsys.readouterr().out
    assert "errors=1" in out
    assert "something broke" in out


# ── cmd_doctor ───────────────────────────────────────────────────────


def test_cmd_doctor_all_ok(capsys):
    from godmode_media_library.cli import cmd_doctor
    from godmode_media_library.deps import DependencyStatus

    statuses = [DependencyStatus(name="exiftool", available=True, version="12.0")]
    with (
        patch("godmode_media_library.deps.check_all", return_value=statuses),
        patch("godmode_media_library.deps.format_report", return_value="All OK\n"),
    ):
        args = argparse.Namespace(exiftool_bin="exiftool")
        ret = cmd_doctor(args)
    assert ret == 0


def test_cmd_doctor_missing_dep(capsys):
    from godmode_media_library.cli import cmd_doctor
    from godmode_media_library.deps import DependencyStatus

    statuses = [DependencyStatus(name="exiftool", available=False)]
    with (
        patch("godmode_media_library.deps.check_all", return_value=statuses),
        patch("godmode_media_library.deps.format_report", return_value="Missing deps\n"),
    ):
        args = argparse.Namespace(exiftool_bin="exiftool")
        ret = cmd_doctor(args)
    assert ret == 1


# ── cmd_batch_rename ─────────────────────────────────────────────────


def test_cmd_batch_rename_not_a_dir(capsys, tmp_path):
    from godmode_media_library.cli import cmd_batch_rename

    args = argparse.Namespace(
        root=str(tmp_path / "nonexistent"),
        pattern="{n:03d}",
        ext=None,
        start=1,
        dry_run=True,
        yes=False,
    )
    ret = cmd_batch_rename(args)
    assert ret == 1
    assert "Not a directory" in capsys.readouterr().out


def test_cmd_batch_rename_no_files(capsys, tmp_path):
    from godmode_media_library.cli import cmd_batch_rename

    args = argparse.Namespace(
        root=str(tmp_path),
        pattern="{n:03d}",
        ext="jpg",
        start=1,
        dry_run=True,
        yes=False,
    )
    ret = cmd_batch_rename(args)
    assert ret == 0
    assert "No files found" in capsys.readouterr().out


def test_cmd_batch_rename_dry_run(capsys, tmp_path):
    from godmode_media_library.batch_rename import RenameAction
    from godmode_media_library.cli import cmd_batch_rename

    # Create a real file
    (tmp_path / "photo.jpg").write_text("fake")

    action = RenameAction(
        original=tmp_path / "photo.jpg",
        new_name="001.jpg",
        new_path=tmp_path / "001.jpg",
    )
    with patch("godmode_media_library.batch_rename.plan_renames", return_value=[action]):
        args = argparse.Namespace(
            root=str(tmp_path),
            pattern="{n:03d}",
            ext=None,
            start=1,
            dry_run=True,
            yes=False,
        )
        ret = cmd_batch_rename(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out


def test_cmd_batch_rename_no_renames_needed(capsys, tmp_path):
    from godmode_media_library.cli import cmd_batch_rename

    (tmp_path / "photo.jpg").write_text("fake")

    with patch("godmode_media_library.batch_rename.plan_renames", return_value=[]):
        args = argparse.Namespace(
            root=str(tmp_path),
            pattern="{n:03d}",
            ext=None,
            start=1,
            dry_run=False,
            yes=True,
        )
        ret = cmd_batch_rename(args)
    assert ret == 0
    assert "No renames needed" in capsys.readouterr().out


# ── cmd_metadata_write ───────────────────────────────────────────────


def test_cmd_metadata_write_invalid_tag_format(capsys):
    from godmode_media_library.cli import cmd_metadata_write

    args = argparse.Namespace(
        files=["/tmp/a.jpg"],
        tags=["BADFORMAT"],
        exiftool_bin="exiftool",
        overwrite_original=False,
        dry_run=False,
        yes=True,
    )
    ret = cmd_metadata_write(args)
    assert ret == 1
    assert "Invalid tag format" in capsys.readouterr().out


def test_cmd_metadata_write_no_tags(capsys):
    from godmode_media_library.cli import cmd_metadata_write

    args = argparse.Namespace(
        files=["/tmp/a.jpg"],
        tags=[],
        exiftool_bin="exiftool",
        overwrite_original=False,
        dry_run=False,
        yes=True,
    )
    ret = cmd_metadata_write(args)
    assert ret == 1
    assert "No tags specified" in capsys.readouterr().out


def test_cmd_metadata_write_dry_run(capsys, tmp_path):
    from godmode_media_library.cli import cmd_metadata_write

    test_file = tmp_path / "photo.jpg"
    test_file.write_text("fake")

    args = argparse.Namespace(
        files=[str(test_file)],
        tags=["Artist=Test"],
        exiftool_bin="exiftool",
        overwrite_original=False,
        dry_run=True,
        yes=True,
    )
    ret = cmd_metadata_write(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "Artist=Test" in out


def test_cmd_metadata_write_dry_run_missing_file(capsys, tmp_path):
    from godmode_media_library.cli import cmd_metadata_write

    args = argparse.Namespace(
        files=[str(tmp_path / "nonexistent.jpg")],
        tags=["Artist=Test"],
        exiftool_bin="exiftool",
        overwrite_original=False,
        dry_run=True,
        yes=True,
    )
    ret = cmd_metadata_write(args)
    assert ret == 0
    assert "skip:" in capsys.readouterr().out


def test_cmd_metadata_write_aborted(capsys, monkeypatch):
    from godmode_media_library.cli import cmd_metadata_write

    monkeypatch.setattr("builtins.input", lambda _: "n")

    args = argparse.Namespace(
        files=["/tmp/a.jpg"],
        tags=["Artist=Test"],
        exiftool_bin="exiftool",
        overwrite_original=False,
        dry_run=False,
        yes=False,
    )
    ret = cmd_metadata_write(args)
    assert ret == 0
    assert "Aborted" in capsys.readouterr().out


# ── cmd_catalog_import / cmd_catalog_export ──────────────────────────


def test_cmd_catalog_import(capsys):
    from godmode_media_library.cli import cmd_catalog_import

    mock_cat = _mock_catalog(import_from_inventory_tsv=42)

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(inventory="/tmp/inv.tsv", catalog=None)
        ret = cmd_catalog_import(args)
    assert ret == 0
    assert "imported=42" in capsys.readouterr().out


def test_cmd_catalog_export(capsys, tmp_path):
    from godmode_media_library.cli import cmd_catalog_export

    mock_cat = _mock_catalog(export_inventory_tsv=10)

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(out=str(tmp_path / "export.tsv"), catalog=None)
        ret = cmd_catalog_export(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "exported=10" in out


# ── cmd_metadata_extract ─────────────────────────────────────────────


def test_cmd_metadata_extract_nothing_to_do(capsys):
    from godmode_media_library.cli import cmd_metadata_extract

    mock_cat = _mock_catalog(paths_without_metadata=[])

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(catalog=None, exiftool_bin="exiftool", force=False)
        ret = cmd_metadata_extract(args)
    assert ret == 0
    assert "All files already have metadata" in capsys.readouterr().out


def test_cmd_metadata_extract_with_files(capsys):
    from godmode_media_library.cli import cmd_metadata_extract

    mock_cat = _mock_catalog(paths_without_metadata=["/tmp/a.jpg"])

    fake_meta = {Path("/tmp/a.jpg"): {"EXIF:Make": "Canon"}}
    mock_richness = MagicMock()
    mock_richness.total = 5.0

    with (
        patch("godmode_media_library.cli._get_catalog", return_value=mock_cat),
        patch("godmode_media_library.cli.extract_all_metadata", return_value=fake_meta),
        patch("godmode_media_library.cli.compute_richness", return_value=mock_richness),
    ):
        args = argparse.Namespace(catalog=None, exiftool_bin="exiftool", force=False)
        ret = cmd_metadata_extract(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "extracted=1" in out


def test_cmd_metadata_extract_force(capsys):
    from godmode_media_library.cli import cmd_metadata_extract

    row = _make_catalog_file_row(path="/tmp/a.jpg")
    mock_cat = _mock_catalog(query_files=[row])

    fake_meta = {Path("/tmp/a.jpg"): {"EXIF:Make": "Canon"}}
    mock_richness = MagicMock()
    mock_richness.total = 5.0

    with (
        patch("godmode_media_library.cli._get_catalog", return_value=mock_cat),
        patch("godmode_media_library.cli.extract_all_metadata", return_value=fake_meta),
        patch("godmode_media_library.cli.compute_richness", return_value=mock_richness),
    ):
        args = argparse.Namespace(catalog=None, exiftool_bin="exiftool", force=True)
        ret = cmd_metadata_extract(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "extracted=1" in out


# ── cmd_metadata_diff ────────────────────────────────────────────────


def test_cmd_metadata_diff_no_groups(capsys):
    from godmode_media_library.cli import cmd_metadata_diff

    mock_cat = _mock_catalog(get_all_duplicate_group_ids=[])

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(catalog=None, group=None, out=None)
        ret = cmd_metadata_diff(args)
    assert ret == 0
    assert "No duplicate groups" in capsys.readouterr().out


def test_cmd_metadata_diff_with_group(capsys):
    from godmode_media_library.cli import cmd_metadata_diff

    mock_cat = _mock_catalog(
        get_all_duplicate_group_ids=["g1"],
        get_group_metadata=[("/a.jpg", {"Make": "Canon"}), ("/b.jpg", {"Make": "Canon"})],
    )

    mock_diff = MagicMock()
    mock_diff.scores = {"/a.jpg": 10.0, "/b.jpg": 5.0}
    mock_diff.unanimous = {"Make": "Canon"}
    mock_diff.partial = {}
    mock_diff.conflicts = {}

    with (
        patch("godmode_media_library.cli._get_catalog", return_value=mock_cat),
        patch("godmode_media_library.cli.compute_group_diff", return_value=mock_diff),
    ):
        args = argparse.Namespace(catalog=None, group=None, out=None)
        ret = cmd_metadata_diff(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "1 groups analyzed" in out


# ── cmd_metadata_merge ───────────────────────────────────────────────


def test_cmd_metadata_merge_no_groups(capsys):
    from godmode_media_library.cli import cmd_metadata_merge

    mock_cat = _mock_catalog(get_all_duplicate_group_ids=[])

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(
            catalog=None,
            group=None,
            out_dir=None,
            exiftool_bin="exiftool",
            apply=False,
            dry_run=False,
        )
        ret = cmd_metadata_merge(args)
    assert ret == 0
    assert "No duplicate groups" in capsys.readouterr().out


# ── _confirm helper ──────────────────────────────────────────────────


def test_confirm_yes_flag():
    from godmode_media_library.cli import _confirm

    assert _confirm("Do it?", yes=True) is True


def test_confirm_user_accepts(monkeypatch):
    from godmode_media_library.cli import _confirm

    monkeypatch.setattr("builtins.input", lambda _: "y")
    assert _confirm("Do it?") is True


def test_confirm_user_declines(monkeypatch):
    from godmode_media_library.cli import _confirm

    monkeypatch.setattr("builtins.input", lambda _: "n")
    assert _confirm("Do it?") is False


def test_confirm_eof():
    from godmode_media_library.cli import _confirm

    with patch("builtins.input", side_effect=EOFError):
        assert _confirm("Do it?") is False


def test_confirm_keyboard_interrupt():
    from godmode_media_library.cli import _confirm

    with patch("builtins.input", side_effect=KeyboardInterrupt):
        assert _confirm("Do it?") is False


# ── _parse_roots ─────────────────────────────────────────────────────


def test_parse_roots():
    from godmode_media_library.cli import _parse_roots

    result = _parse_roots(["/tmp/photos", "/tmp/videos"])
    assert len(result) == 2
    assert all(isinstance(r, Path) for r in result)


# ── _build_policy ────────────────────────────────────────────────────


def test_build_policy():
    from godmode_media_library.cli import _build_policy

    args = argparse.Namespace(
        allow_asset_component_dedupe=False,
        no_prefer_earliest_origin=False,
        no_prefer_richer_metadata=False,
        prefer_root=["/tmp/preferred"],
    )
    policy = _build_policy(args)
    assert policy.protect_asset_components is True
    assert policy.prefer_earliest_origin_time is True
    assert policy.prefer_richer_metadata is True
    assert len(policy.prefer_roots) == 1


def test_build_policy_all_disabled():
    from godmode_media_library.cli import _build_policy

    args = argparse.Namespace(
        allow_asset_component_dedupe=True,
        no_prefer_earliest_origin=True,
        no_prefer_richer_metadata=True,
        prefer_root=None,
    )
    policy = _build_policy(args)
    assert policy.protect_asset_components is False
    assert policy.prefer_earliest_origin_time is False
    assert policy.prefer_richer_metadata is False


# ── _get_catalog ─────────────────────────────────────────────────────


def test_get_catalog_default():
    from godmode_media_library.cli import _get_catalog

    with (
        patch("godmode_media_library.cli.default_catalog_path", return_value=Path("/tmp/default.db")),
        patch("godmode_media_library.cli.Catalog") as MockCatalog,
    ):
        args = argparse.Namespace(catalog=None)
        _get_catalog(args)
    MockCatalog.assert_called_once_with(Path("/tmp/default.db"), exclusive=False)


def test_get_catalog_custom_path():
    from godmode_media_library.cli import _get_catalog

    with patch("godmode_media_library.cli.Catalog") as MockCatalog:
        args = argparse.Namespace(catalog="/tmp/custom.db")
        _get_catalog(args, exclusive=True)
    MockCatalog.assert_called_once_with(Path("/tmp/custom.db"), exclusive=True)


# ── main() error handling paths ──────────────────────────────────────


def test_main_keyboard_interrupt(monkeypatch):
    monkeypatch.setattr("sys.argv", ["gml", "config"])

    with patch("godmode_media_library.cli.cmd_config_show", side_effect=KeyboardInterrupt), pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 130


def test_main_value_error(monkeypatch):
    monkeypatch.setattr("sys.argv", ["gml", "config"])

    with patch("godmode_media_library.cli.cmd_config_show", side_effect=ValueError("bad value")), pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 2


def test_main_file_not_found(monkeypatch):
    monkeypatch.setattr("sys.argv", ["gml", "config"])

    with patch("godmode_media_library.cli.cmd_config_show", side_effect=FileNotFoundError("nope")):
        with pytest.raises(SystemExit) as exc_info:
            main()
    assert exc_info.value.code == 2


def test_main_permission_error(monkeypatch):
    monkeypatch.setattr("sys.argv", ["gml", "config"])

    with patch("godmode_media_library.cli.cmd_config_show", side_effect=PermissionError("denied")):
        with pytest.raises(SystemExit) as exc_info:
            main()
    assert exc_info.value.code == 2


def test_main_unexpected_error(monkeypatch):
    monkeypatch.setattr("sys.argv", ["gml", "config"])

    with patch("godmode_media_library.cli.cmd_config_show", side_effect=RuntimeError("boom")), pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 1


# ── main() with --lang flag ─────────────────────────────────────────


def test_main_with_lang(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["gml", "--lang", "cs", "config"])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 0


# ── cmd_apply confirmation prompts ───────────────────────────────────


def test_cmd_apply_aborted(capsys, tmp_path, monkeypatch):
    from godmode_media_library.cli import cmd_apply

    plan_path = tmp_path / "plan.tsv"
    write_tsv(
        plan_path,
        ["hash", "size", "keep_path", "move_path", "reason", "keep_score", "move_score"],
        [],
    )

    monkeypatch.setattr("builtins.input", lambda _: "n")

    args = argparse.Namespace(
        plan=str(plan_path),
        quarantine_root=str(tmp_path / "quarantine"),
        dry_run=False,
        yes=False,
    )
    ret = cmd_apply(args)
    assert ret == 0
    assert "Aborted" in capsys.readouterr().out


# ── cmd_promote confirmation prompts ─────────────────────────────────


def test_cmd_promote_aborted(capsys, tmp_path, monkeypatch):
    manifest_path = tmp_path / "manifest.tsv"
    write_tsv(
        manifest_path,
        ["size", "moved_from", "quarantine_path", "primary_path"],
        [],
    )

    monkeypatch.setattr("builtins.input", lambda _: "n")

    args = argparse.Namespace(
        manifest=str(manifest_path),
        backup_root=None,
        dry_run=False,
        yes=False,
    )
    ret = cmd_promote(args)
    assert ret == 0
    assert "Aborted" in capsys.readouterr().out


# ── cmd_delete_plan ──────────────────────────────────────────────────


def test_cmd_delete_plan_no_inputs(capsys, tmp_path):
    from godmode_media_library.cli import cmd_delete_plan

    args = argparse.Namespace(
        roots=["/tmp/root"],
        out=str(tmp_path / "plan.tsv"),
        summary_out=None,
        select_paths=None,
        recommendations=None,
        prefer_root=[],
        no_asset_set_expansion=False,
        allow_external_links=False,
    )
    ret = cmd_delete_plan(args)
    assert ret == 2
    assert "Provide at least one" in capsys.readouterr().out


# ── cmd_auto_place error path ────────────────────────────────────────


def test_cmd_auto_place_runtime_error(capsys, tmp_path):
    from godmode_media_library.cli import cmd_auto_place

    with patch("godmode_media_library.cli.auto_place_labels", side_effect=RuntimeError("GDPR not acknowledged")):
        args = argparse.Namespace(
            roots=["/tmp/root"],
            labels_in=None,
            labels_out=str(tmp_path / "labels.tsv"),
            report_dir=None,
            exiftool_bin="exiftool",
            reverse_geocode=False,
            gdpr_consent=False,
            geocode_cache=None,
            geocode_min_delay_seconds=1.1,
            overwrite_place=False,
        )
        ret = cmd_auto_place(args)
    assert ret == 2
    assert "GDPR" in capsys.readouterr().out


# ── cmd_auto_people error path ───────────────────────────────────────


def test_cmd_auto_people_runtime_error(capsys, tmp_path):
    from godmode_media_library.cli import cmd_auto_people

    with patch("godmode_media_library.cli.auto_people_labels", side_effect=RuntimeError("No face_recognition")):
        args = argparse.Namespace(
            roots=["/tmp/root"],
            labels_in=None,
            labels_out=str(tmp_path / "labels.tsv"),
            report_dir=None,
            model="hog",
            max_dimension=1600,
            eps=0.5,
            min_samples=2,
            person_prefix="Person",
            overwrite_people=False,
        )
        ret = cmd_auto_people(args)
    assert ret == 2
    assert "No face_recognition" in capsys.readouterr().out


# ── Parser subcommand tests for new commands ─────────────────────────


def test_parser_scan_subcommand():
    parser = build_parser()
    args = parser.parse_args(["scan", "--roots", "/tmp/photos"])
    assert args.command == "scan"
    assert args.min_size_kb == 0
    assert args.workers == 1


def test_parser_query_subcommand():
    parser = build_parser()
    args = parser.parse_args(["query", "--duplicates"])
    assert args.command == "query"
    assert args.duplicates is True


def test_parser_stats_subcommand():
    parser = build_parser()
    args = parser.parse_args(["stats"])
    assert args.command == "stats"


def test_parser_vacuum_subcommand():
    parser = build_parser()
    args = parser.parse_args(["vacuum", "--catalog", "/tmp/cat.db"])
    assert args.command == "vacuum"


def test_parser_similar_subcommand():
    parser = build_parser()
    args = parser.parse_args(["similar", "--threshold", "5"])
    assert args.command == "similar"
    assert args.threshold == 5


def test_parser_verify_subcommand():
    parser = build_parser()
    args = parser.parse_args(["verify", "--check-hashes", "--limit", "50"])
    assert args.command == "verify"
    assert args.check_hashes is True
    assert args.limit == 50


def test_parser_export_subcommand():
    parser = build_parser()
    args = parser.parse_args(["export", "files", "--out", "/tmp/out.csv", "--format", "tsv"])
    assert args.command == "export"
    assert args.what == "files"
    assert args.format == "tsv"


def test_parser_batch_rename_subcommand():
    parser = build_parser()
    args = parser.parse_args(["batch-rename", "/tmp/dir", "--pattern", "{n:03d}", "--dry-run"])
    assert args.command == "batch-rename"
    assert args.dry_run is True


def test_parser_metadata_write_subcommand():
    parser = build_parser()
    args = parser.parse_args(["metadata-write", "/tmp/a.jpg", "--tag", "Artist=Test", "--dry-run"])
    assert args.command == "metadata-write"
    assert args.tags == ["Artist=Test"]


def test_parser_serve_subcommand():
    parser = build_parser()
    args = parser.parse_args(["serve", "--port", "9090", "--no-browser"])
    assert args.command == "serve"
    assert args.port == 9090
    assert args.no_browser is True


def test_parser_watch_subcommand():
    parser = build_parser()
    args = parser.parse_args(["watch", "--roots", "/tmp/photos"])
    assert args.command == "watch"


def test_parser_auto_subcommand():
    parser = build_parser()
    args = parser.parse_args(["auto", "--roots", "/tmp/photos", "--skip", "scan", "extract"])
    assert args.command == "auto"
    assert args.skip == ["scan", "extract"]


def test_parser_cloud_subcommand():
    parser = build_parser()
    args = parser.parse_args(["cloud"])
    assert args.command == "cloud"


def test_parser_doctor_subcommand():
    parser = build_parser()
    args = parser.parse_args(["doctor"])
    assert args.command == "doctor"


def test_parser_catalog_import_subcommand():
    parser = build_parser()
    args = parser.parse_args(["catalog-import", "--inventory", "/tmp/inv.tsv"])
    assert args.command == "catalog-import"


def test_parser_catalog_export_subcommand():
    parser = build_parser()
    args = parser.parse_args(["catalog-export", "--out", "/tmp/out.tsv"])
    assert args.command == "catalog-export"


def test_parser_metadata_extract_subcommand():
    parser = build_parser()
    args = parser.parse_args(["metadata-extract", "--force"])
    assert args.command == "metadata-extract"
    assert args.force is True


def test_parser_metadata_diff_subcommand():
    parser = build_parser()
    args = parser.parse_args(["metadata-diff", "--group", "abc123"])
    assert args.command == "metadata-diff"
    assert args.group == "abc123"


def test_parser_metadata_merge_subcommand():
    parser = build_parser()
    args = parser.parse_args(["metadata-merge", "--apply", "--dry-run"])
    assert args.command == "metadata-merge"
    assert args.apply is True
    assert args.dry_run is True


# ── cmd_tree_apply aborted (non-dry-run move) ───────────────────────


def test_cmd_tree_apply_move_aborted(capsys, tmp_path, monkeypatch):
    from godmode_media_library.cli import cmd_tree_apply

    plan_path = tmp_path / "tree_plan.tsv"
    write_tsv(
        plan_path,
        ["unit_id", "source_path", "destination_path", "mode", "bucket", "asset_key", "is_asset_component"],
        [],
    )

    monkeypatch.setattr("builtins.input", lambda _: "n")

    args = argparse.Namespace(
        plan=str(plan_path),
        operation="move",
        dry_run=False,
        collision_policy="rename",
        log=None,
        yes=False,
    )
    ret = cmd_tree_apply(args)
    assert ret == 0
    assert "Aborted" in capsys.readouterr().out


# ── cmd_metadata_write actual write with mock ────────────────────────


def test_cmd_metadata_write_actual_write(capsys, tmp_path):
    from godmode_media_library.cli import cmd_metadata_write

    test_file = tmp_path / "photo.jpg"
    test_file.write_text("fake")

    with patch("godmode_media_library.cli.write_tags", return_value=(True, "ok")):
        args = argparse.Namespace(
            files=[str(test_file)],
            tags=["Artist=Test"],
            exiftool_bin="exiftool",
            overwrite_original=False,
            dry_run=False,
            yes=True,
        )
        ret = cmd_metadata_write(args)
    assert ret == 0
    out = capsys.readouterr().out
    assert "written=1 failed=0" in out


def test_cmd_metadata_write_failed_write(capsys, tmp_path):
    from godmode_media_library.cli import cmd_metadata_write

    test_file = tmp_path / "photo.jpg"
    test_file.write_text("fake")

    with patch("godmode_media_library.cli.write_tags", return_value=(False, "exiftool error")):
        args = argparse.Namespace(
            files=[str(test_file)],
            tags=["Artist=Test"],
            exiftool_bin="exiftool",
            overwrite_original=True,
            dry_run=False,
            yes=True,
        )
        ret = cmd_metadata_write(args)
    assert ret == 1
    out = capsys.readouterr().out
    assert "written=0 failed=1" in out


# ── cmd_restore selective mode ───────────────────────────────────────


def test_cmd_restore_selective(capsys, tmp_path):
    from godmode_media_library.cli import cmd_restore

    log_path = tmp_path / "executed_log.tsv"
    write_tsv(
        log_path,
        ["hash", "size", "keep_path", "move_path", "quarantine_path", "reason", "verified_hash"],
        [],
    )

    with patch("godmode_media_library.cli.selective_restore", return_value=(0, 0)):
        args = argparse.Namespace(
            log=str(log_path),
            last=5,
            file=None,
            dry_run=True,
        )
        ret = cmd_restore(args)
    assert ret == 0
    assert "restored=0" in capsys.readouterr().out


# ── cmd_query with filters ──────────────────────────────────────────


def test_cmd_query_with_size_filters(capsys):
    from godmode_media_library.cli import cmd_query

    row = _make_catalog_file_row()
    mock_cat = _mock_catalog(query_files=[row])

    with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
        args = argparse.Namespace(
            catalog=None,
            duplicates=False,
            ext="jpg",
            date_from="2024-01-01",
            date_to="2024-12-31",
            min_size=100,
            max_size=5000,
            path_contains="photos",
            camera="Canon",
            duration_min=1.0,
            duration_max=60.0,
            resolution_min=1920,
            no_gps=True,
            limit=500,
        )
        ret = cmd_query(args)
    assert ret == 0

    # Verify query_files was called with the right filters
    mock_cat.query_files.assert_called_once_with(
        ext="jpg",
        date_from="2024-01-01",
        date_to="2024-12-31",
        min_size=100 * 1024,
        max_size=5000 * 1024,
        path_contains="photos",
        camera="Canon",
        min_duration=1.0,
        max_duration=60.0,
        min_width=1920,
        has_gps=False,
        limit=500,
    )
