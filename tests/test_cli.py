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
    args = parser.parse_args([
        "tree-plan",
        "--roots", "/tmp/src",
        "--target-root", "/tmp/dst",
        "--mode", "time",
        "--granularity", "month",
    ])
    assert args.command == "tree-plan"
    assert args.mode == "time"
    assert args.granularity == "month"

    # Verify mode choices are enforced
    with pytest.raises(SystemExit):
        parser.parse_args([
            "tree-plan",
            "--roots", "/tmp/src",
            "--target-root", "/tmp/dst",
            "--mode", "invalid_mode",
        ])


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
    args = parser.parse_args([
        "tree-apply",
        "--plan", "/tmp/tree_plan.tsv",
        "--operation", "copy",
        "--collision-policy", "skip",
    ])
    assert args.command == "tree-apply"
    assert args.operation == "copy"
    assert args.collision_policy == "skip"


def test_parser_delete_plan_subcommand():
    parser = build_parser()
    args = parser.parse_args([
        "delete-plan",
        "--roots", "/tmp/root",
        "--out", "/tmp/plan.tsv",
        "--select-paths", "/tmp/select.txt",
    ])
    assert args.command == "delete-plan"


def test_parser_delete_apply_subcommand():
    parser = build_parser()
    args = parser.parse_args([
        "delete-apply",
        "--plan", "/tmp/plan.tsv",
        "--quarantine-root", "/tmp/quarantine",
        "--dry-run",
    ])
    assert args.command == "delete-apply"
    assert args.dry_run is True


def test_parser_prune_recommend_subcommand():
    parser = build_parser()
    args = parser.parse_args([
        "prune-recommend",
        "--roots", "/tmp/root1", "/tmp/root2",
        "--out-dir", "/tmp/out",
    ])
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
    ret = main()
    assert ret == 0
    captured = capsys.readouterr()
    assert "min_size_kb" in captured.out


def test_main_with_verbose_config(monkeypatch, capsys):
    """Exercise main() with -v flag."""
    monkeypatch.setattr("sys.argv", ["gml", "-v", "config"])
    ret = main()
    assert ret == 0


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
        ["inode_id", "path", "action", "primary_path", "asset_key", "selected_seed", "unit_size",
         "nlink_expected", "nlink_scanned", "external_links", "note"],
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
