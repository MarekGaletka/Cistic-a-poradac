"""Extended CLI command tests targeting uncovered lines.

Covers: _format_output, _confirm, cmd_query output formats, cmd_stats tsv,
cmd_verify json/tsv, cmd_similar json/tsv, cmd_export tsv, cmd_batch_rename,
cmd_metadata_extract, cmd_metadata_diff, cmd_metadata_merge, cmd_metadata_write,
cmd_catalog_import, cmd_catalog_export, cmd_doctor, _kill_old_server, _get_pid_file.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library.cli import (
    _format_output,
    _confirm,
    build_parser,
)


# ── _format_output ──────────────────────────────────────────────────


class TestFormatOutput:
    def test_json_list(self):
        data = [{"a": 1, "b": "hello"}]
        result = _format_output(data, "json")
        parsed = json.loads(result)
        assert parsed == [{"a": 1, "b": "hello"}]

    def test_json_dict(self):
        data = {"key": "value"}
        result = _format_output(data, "json")
        parsed = json.loads(result)
        assert parsed == {"key": "value"}

    def test_tsv_list(self):
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        result = _format_output(data, "tsv")
        lines = result.split("\n")
        assert lines[0] == "a\tb"
        assert lines[1] == "1\t2"
        assert lines[2] == "3\t4"

    def test_tsv_dict_wraps_in_list(self):
        data = {"x": 10}
        result = _format_output(data, "tsv")
        assert "x" in result
        assert "10" in result

    def test_tsv_empty_list(self):
        assert _format_output([], "tsv") == ""

    def test_tsv_with_custom_headers(self):
        data = [{"a": 1, "b": 2, "c": 3}]
        result = _format_output(data, "tsv", headers=["c", "a"])
        lines = result.split("\n")
        assert lines[0] == "c\ta"
        assert lines[1] == "3\t1"

    def test_text_format_returns_empty(self):
        assert _format_output([{"a": 1}], "text") == ""

    def test_json_with_path_default_str(self):
        data = [{"path": Path("/tmp/test")}]
        result = _format_output(data, "json")
        assert "/tmp/test" in result


# ── _confirm ────────────────────────────────────────────────────────


class TestConfirm:
    def test_yes_flag_bypasses_prompt(self):
        assert _confirm("Do it?", yes=True) is True

    def test_user_confirms_yes(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "y")
        assert _confirm("Do it?") is True

    def test_user_confirms_full_yes(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "yes")
        assert _confirm("Do it?") is True

    def test_user_declines(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "n")
        assert _confirm("Do it?") is False

    def test_user_empty_input(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        assert _confirm("Do it?") is False

    def test_eof_error(self, monkeypatch):
        def raise_eof(_):
            raise EOFError

        monkeypatch.setattr("builtins.input", raise_eof)
        assert _confirm("Do it?") is False

    def test_keyboard_interrupt(self, monkeypatch):
        def raise_ki(_):
            raise KeyboardInterrupt

        monkeypatch.setattr("builtins.input", raise_ki)
        assert _confirm("Do it?") is False


# ── Helpers ─────────────────────────────────────────────────────────


def _make_catalog_file_row(**overrides):
    defaults = dict(
        id=1, path="/tmp/test.jpg", size=1024, mtime=1.0, ctime=1.0,
        birthtime=None, ext="jpg", sha256="abc123", inode=100, device=1,
        nlink=1, asset_key=None, asset_component=False, xattr_count=0,
        first_seen="2024-01-01", last_scanned="2024-01-01",
        duration_seconds=None, width=None, height=None, video_codec=None,
        audio_codec=None, bitrate=None, phash=None, date_original=None,
        camera_make=None, camera_model=None, gps_latitude=None,
        gps_longitude=None, metadata_richness=None,
    )
    defaults.update(overrides)
    return type("CatalogFileRow", (), defaults)()


def _mock_catalog(**method_returns):
    cat = MagicMock()
    cat.__enter__ = MagicMock(return_value=cat)
    cat.__exit__ = MagicMock(return_value=False)
    cat.db_path = Path("/tmp/test_catalog.db")
    for method, retval in method_returns.items():
        getattr(cat, method).return_value = retval
    return cat


# ── cmd_query with JSON format ──────────────────────────────────────


class TestCmdQueryFormats:
    def test_query_files_json(self, capsys):
        from godmode_media_library.cli import cmd_query

        row = _make_catalog_file_row(camera_model="Canon R5", duration_seconds=5.0)
        mock_cat = _mock_catalog(query_files=[row])

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
            args = argparse.Namespace(
                catalog=None, duplicates=False,
                ext=None, date_from=None, date_to=None,
                min_size=None, max_size=None, path_contains=None,
                camera=None, duration_min=None, duration_max=None,
                resolution_min=None, no_gps=False, limit=100,
                output_format="json",
            )
            ret = cmd_query(args)
        assert ret == 0
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert isinstance(parsed, list)
        assert parsed[0]["path"] == "/tmp/test.jpg"

    def test_query_files_tsv(self, capsys):
        from godmode_media_library.cli import cmd_query

        row = _make_catalog_file_row()
        mock_cat = _mock_catalog(query_files=[row])

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
            args = argparse.Namespace(
                catalog=None, duplicates=False,
                ext=None, date_from=None, date_to=None,
                min_size=None, max_size=None, path_contains=None,
                camera=None, duration_min=None, duration_max=None,
                resolution_min=None, no_gps=False, limit=100,
                output_format="tsv",
            )
            ret = cmd_query(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "path" in out  # TSV header

    def test_query_duplicates_json(self, capsys):
        from godmode_media_library.cli import cmd_query

        row1 = _make_catalog_file_row(path="/a.jpg", size=100)
        row2 = _make_catalog_file_row(path="/b.jpg", size=100)
        mock_cat = _mock_catalog(query_duplicates=[("g1", [row1, row2])])

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
            args = argparse.Namespace(
                catalog=None, duplicates=True,
                ext=None, date_from=None, date_to=None,
                min_size=None, max_size=None, path_contains=None,
                camera=None, duration_min=None, duration_max=None,
                resolution_min=None, no_gps=False, limit=100,
                output_format="json",
            )
            ret = cmd_query(args)
        assert ret == 0
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert len(parsed) == 2

    def test_query_with_size_filters(self, capsys):
        from godmode_media_library.cli import cmd_query

        mock_cat = _mock_catalog(query_files=[])

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
            args = argparse.Namespace(
                catalog=None, duplicates=False,
                ext="jpg", date_from="2024-01-01", date_to="2024-12-31",
                min_size=100, max_size=5000, path_contains="photos",
                camera="Canon", duration_min=1.0, duration_max=60.0,
                resolution_min=1920, no_gps=True, limit=50,
                output_format="text",
            )
            ret = cmd_query(args)
        assert ret == 0
        # Verify the filter was passed through
        mock_cat.query_files.assert_called_once()
        call_kwargs = mock_cat.query_files.call_args
        assert call_kwargs[1]["min_size"] == 100 * 1024
        assert call_kwargs[1]["has_gps"] is False


# ── cmd_stats with TSV format ───────────────────────────────────────


class TestCmdStatsFormat:
    def test_stats_tsv(self, capsys):
        from godmode_media_library.cli import cmd_stats

        mock_cat = _mock_catalog(stats={"total_files": 42, "total_size": 9999})

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
            args = argparse.Namespace(catalog=None, output_format="tsv")
            ret = cmd_stats(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "total_files" in out


# ── cmd_verify with JSON/TSV output ────────────────────────────────


class TestCmdVerifyFormats:
    def test_verify_json(self, capsys):
        from godmode_media_library.cli import cmd_verify
        from godmode_media_library.verify import VerifyResult

        result = VerifyResult(
            total_checked=10, ok=8,
            missing_files=["/gone.jpg"],
            size_mismatches=[("/diff.jpg", 100, 200)],
        )
        mock_cat = _mock_catalog()

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat), \
             patch("godmode_media_library.cli.verify_catalog", return_value=result):
            args = argparse.Namespace(catalog=None, check_hashes=False, limit=0, output_format="json")
            ret = cmd_verify(args)
        assert ret == 1
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert parsed["total_checked"] == 10
        assert parsed["has_issues"] is True

    def test_verify_tsv(self, capsys):
        from godmode_media_library.cli import cmd_verify
        from godmode_media_library.verify import VerifyResult

        result = VerifyResult(total_checked=5, ok=5)
        mock_cat = _mock_catalog()

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat), \
             patch("godmode_media_library.cli.verify_catalog", return_value=result):
            args = argparse.Namespace(catalog=None, check_hashes=False, limit=0, output_format="tsv")
            ret = cmd_verify(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "total_checked" in out


# ── cmd_similar with JSON/TSV output ───────────────────────────────


class TestCmdSimilarFormats:
    def test_similar_json(self, capsys):
        from godmode_media_library.cli import cmd_similar
        from godmode_media_library.perceptual_hash import SimilarPair

        pair = SimilarPair(path_a="/a.jpg", path_b="/b.jpg", distance=3,
                           hash_a="0" * 16, hash_b="1" * 16)
        mock_cat = _mock_catalog(get_all_phashes={"a": "0" * 16, "b": "1" * 16})

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat), \
             patch("godmode_media_library.cli.find_similar", return_value=[pair]):
            args = argparse.Namespace(catalog=None, threshold=10, out=None, output_format="json")
            ret = cmd_similar(args)
        assert ret == 0
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert len(parsed) == 1
        assert parsed[0]["distance"] == 3

    def test_similar_tsv(self, capsys):
        from godmode_media_library.cli import cmd_similar
        from godmode_media_library.perceptual_hash import SimilarPair

        pair = SimilarPair(path_a="/a.jpg", path_b="/b.jpg", distance=1,
                           hash_a="0" * 16, hash_b="1" * 16)
        mock_cat = _mock_catalog(get_all_phashes={"a": "0" * 16})

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat), \
             patch("godmode_media_library.cli.find_similar", return_value=[pair]):
            args = argparse.Namespace(catalog=None, threshold=10, out=None, output_format="tsv")
            ret = cmd_similar(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "distance" in out


# ── cmd_doctor ──────────────────────────────────────────────────────


class TestCmdDoctor:
    def test_doctor_all_ok(self, capsys):
        from godmode_media_library.cli import cmd_doctor

        @dataclass
        class FakeStatus:
            name: str
            available: bool
            version: str = ""
            details: str = ""

        statuses = [FakeStatus(name="python", available=True, version="3.11")]

        with patch("godmode_media_library.deps.check_all", return_value=statuses), \
             patch("godmode_media_library.deps.format_report", return_value="All OK\n"):
            args = argparse.Namespace(exiftool_bin="exiftool")
            ret = cmd_doctor(args)
        assert ret == 0
        assert "All OK" in capsys.readouterr().out

    def test_doctor_missing_dep(self, capsys):
        from godmode_media_library.cli import cmd_doctor

        @dataclass
        class FakeStatus:
            name: str
            available: bool
            version: str = ""
            details: str = ""

        statuses = [
            FakeStatus(name="exiftool", available=False),
        ]

        with patch("godmode_media_library.deps.check_all", return_value=statuses), \
             patch("godmode_media_library.deps.format_report", return_value="Missing: exiftool\n"):
            args = argparse.Namespace(exiftool_bin="exiftool")
            ret = cmd_doctor(args)
        assert ret == 1


# ── cmd_catalog_import / cmd_catalog_export ─────────────────────────


class TestCmdCatalogIO:
    def test_catalog_import(self, capsys, tmp_path):
        from godmode_media_library.cli import cmd_catalog_import

        inv_path = tmp_path / "inventory.tsv"
        inv_path.write_text("path\tsize\text\n/test.jpg\t100\tjpg\n")
        mock_cat = _mock_catalog(import_from_inventory_tsv=1)

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
            args = argparse.Namespace(inventory=str(inv_path), catalog=None)
            ret = cmd_catalog_import(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "imported=1" in out

    def test_catalog_export(self, capsys, tmp_path):
        from godmode_media_library.cli import cmd_catalog_export

        out_path = tmp_path / "export.tsv"
        mock_cat = _mock_catalog(export_inventory_tsv=42)

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
            args = argparse.Namespace(out=str(out_path), catalog=None)
            ret = cmd_catalog_export(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "exported=42" in out


# ── cmd_metadata_extract ────────────────────────────────────────────


class TestCmdMetadataExtract:
    def test_no_paths_to_extract(self, capsys):
        from godmode_media_library.cli import cmd_metadata_extract

        mock_cat = _mock_catalog(paths_without_metadata=[])

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
            args = argparse.Namespace(catalog=None, force=False, exiftool_bin="exiftool")
            ret = cmd_metadata_extract(args)
        assert ret == 0
        assert "already have metadata" in capsys.readouterr().out

    def test_extract_with_paths(self, capsys, tmp_path):
        from godmode_media_library.cli import cmd_metadata_extract

        mock_cat = _mock_catalog(paths_without_metadata=["/test.jpg"])
        fake_meta = {Path("/test.jpg"): {"FileName": "test.jpg"}}
        fake_richness = MagicMock(total=50.0)

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat), \
             patch("godmode_media_library.cli.extract_all_metadata", return_value=fake_meta), \
             patch("godmode_media_library.cli.compute_richness", return_value=fake_richness):
            args = argparse.Namespace(catalog=None, force=False, exiftool_bin="exiftool")
            ret = cmd_metadata_extract(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "extracted=1" in out

    def test_extract_force_mode(self, capsys):
        from godmode_media_library.cli import cmd_metadata_extract

        row = _make_catalog_file_row()
        mock_cat = _mock_catalog(query_files=[row])
        fake_meta = {Path("/tmp/test.jpg"): {"FileName": "test.jpg"}}
        fake_richness = MagicMock(total=30.0)

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat), \
             patch("godmode_media_library.cli.extract_all_metadata", return_value=fake_meta), \
             patch("godmode_media_library.cli.compute_richness", return_value=fake_richness):
            args = argparse.Namespace(catalog=None, force=True, exiftool_bin="exiftool")
            ret = cmd_metadata_extract(args)
        assert ret == 0


# ── cmd_metadata_diff ───────────────────────────────────────────────


class TestCmdMetadataDiff:
    def test_no_groups(self, capsys):
        from godmode_media_library.cli import cmd_metadata_diff

        mock_cat = _mock_catalog(get_all_duplicate_group_ids=[])

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
            args = argparse.Namespace(catalog=None, group=None, out=None)
            ret = cmd_metadata_diff(args)
        assert ret == 0
        assert "No duplicate groups" in capsys.readouterr().out

    def test_diff_with_group(self, capsys):
        from godmode_media_library.cli import cmd_metadata_diff

        mock_cat = _mock_catalog(
            get_all_duplicate_group_ids=["g1"],
            get_group_metadata=[("/a.jpg", {"Tag1": "v1"}), ("/b.jpg", {"Tag1": "v2"})],
        )

        fake_diff = MagicMock()
        fake_diff.scores = {"/a.jpg": 80.0, "/b.jpg": 60.0}
        fake_diff.unanimous = {"Tag0": "shared"}
        fake_diff.partial = {}
        fake_diff.conflicts = {"Tag1": {"/a.jpg": "v1", "/b.jpg": "v2"}}

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat), \
             patch("godmode_media_library.cli.compute_group_diff", return_value=fake_diff):
            args = argparse.Namespace(catalog=None, group=None, out=None)
            ret = cmd_metadata_diff(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "1 groups analyzed" in out

    def test_diff_with_output_file(self, capsys, tmp_path):
        from godmode_media_library.cli import cmd_metadata_diff

        mock_cat = _mock_catalog(
            get_all_duplicate_group_ids=["g1"],
            get_group_metadata=[("/a.jpg", {"Tag1": "v1"}), ("/b.jpg", {"Tag1": "v2"})],
        )

        fake_diff = MagicMock()
        fake_diff.scores = {"/a.jpg": 80.0, "/b.jpg": 60.0}
        fake_diff.unanimous = {}
        fake_diff.partial = {"Tag2": {"/a.jpg": "x"}}
        fake_diff.conflicts = {"Tag1": {"/a.jpg": "v1", "/b.jpg": "v2"}}

        out_file = tmp_path / "diff.json"
        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat), \
             patch("godmode_media_library.cli.compute_group_diff", return_value=fake_diff):
            args = argparse.Namespace(catalog=None, group=None, out=str(out_file))
            ret = cmd_metadata_diff(args)
        assert ret == 0
        assert out_file.exists()
        data = json.loads(out_file.read_text())
        assert len(data) == 1

    def test_diff_single_file_group_skipped(self, capsys):
        from godmode_media_library.cli import cmd_metadata_diff

        mock_cat = _mock_catalog(
            get_all_duplicate_group_ids=["g1"],
            get_group_metadata=[("/a.jpg", {"Tag1": "v1"})],  # only 1 file
        )

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
            args = argparse.Namespace(catalog=None, group=None, out=None)
            ret = cmd_metadata_diff(args)
        assert ret == 0
        assert "0 groups analyzed" in capsys.readouterr().out


# ── cmd_metadata_merge ──────────────────────────────────────────────


class TestCmdMetadataMerge:
    def test_no_groups(self, capsys):
        from godmode_media_library.cli import cmd_metadata_merge

        mock_cat = _mock_catalog(get_all_duplicate_group_ids=[])

        with patch("godmode_media_library.cli._get_catalog", return_value=mock_cat):
            args = argparse.Namespace(catalog=None, group=None, out_dir=None, apply=False,
                                      exiftool_bin="exiftool", dry_run=False)
            ret = cmd_metadata_merge(args)
        assert ret == 0
        assert "No duplicate groups" in capsys.readouterr().out


# ── cmd_metadata_write ──────────────────────────────────────────────


class TestCmdMetadataWrite:
    def test_no_tags(self, capsys):
        from godmode_media_library.cli import cmd_metadata_write

        args = argparse.Namespace(
            files=["/tmp/test.jpg"], tags=[], exiftool_bin="exiftool",
            dry_run=False, yes=False, overwrite_original=False,
        )
        ret = cmd_metadata_write(args)
        assert ret == 1
        assert "No tags specified" in capsys.readouterr().out

    def test_invalid_tag_format(self, capsys):
        from godmode_media_library.cli import cmd_metadata_write

        args = argparse.Namespace(
            files=["/tmp/test.jpg"], tags=["InvalidNoEquals"],
            exiftool_bin="exiftool", dry_run=False, yes=False,
            overwrite_original=False,
        )
        ret = cmd_metadata_write(args)
        assert ret == 1
        assert "Invalid tag format" in capsys.readouterr().out

    def test_dry_run(self, capsys, tmp_path):
        from godmode_media_library.cli import cmd_metadata_write

        test_file = tmp_path / "photo.jpg"
        test_file.write_bytes(b"JPEG")

        args = argparse.Namespace(
            files=[str(test_file)], tags=["Title=Test"],
            exiftool_bin="exiftool", dry_run=True, yes=True,
            overwrite_original=False,
        )
        ret = cmd_metadata_write(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "DRY RUN" in out

    def test_dry_run_missing_file(self, capsys):
        from godmode_media_library.cli import cmd_metadata_write

        args = argparse.Namespace(
            files=["/nonexistent/photo.jpg"], tags=["Title=Test"],
            exiftool_bin="exiftool", dry_run=True, yes=True,
            overwrite_original=False,
        )
        ret = cmd_metadata_write(args)
        assert ret == 0
        assert "not found" in capsys.readouterr().out


# ── cmd_batch_rename ────────────────────────────────────────────────


class TestCmdBatchRename:
    def test_not_a_directory(self, capsys):
        from godmode_media_library.cli import cmd_batch_rename

        args = argparse.Namespace(
            root="/nonexistent_dir_xzy", ext=None, pattern="{n:04d}",
            start=1, dry_run=True, yes=False,
        )
        ret = cmd_batch_rename(args)
        assert ret == 1
        assert "Not a directory" in capsys.readouterr().out

    def test_no_files(self, capsys, tmp_path):
        from godmode_media_library.cli import cmd_batch_rename

        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        args = argparse.Namespace(
            root=str(empty_dir), ext="xyz", pattern="{n:04d}",
            start=1, dry_run=True, yes=False,
        )
        ret = cmd_batch_rename(args)
        assert ret == 0
        assert "No files found" in capsys.readouterr().out

    def test_dry_run_with_files(self, capsys, tmp_path):
        from godmode_media_library.cli import cmd_batch_rename

        (tmp_path / "a.txt").write_text("hello")
        (tmp_path / "b.txt").write_text("world")

        args = argparse.Namespace(
            root=str(tmp_path), ext="txt", pattern="{n:04d}",
            start=1, dry_run=True, yes=False,
        )
        ret = cmd_batch_rename(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "DRY RUN" in out


# ── _get_pid_file and _kill_old_server ──────────────────────────────


class TestPidFile:
    def test_get_pid_file_returns_path(self):
        from godmode_media_library.cli import _get_pid_file

        pid_file = _get_pid_file()
        assert isinstance(pid_file, Path)
        assert "server.pid" in str(pid_file)

    def test_kill_old_server_no_processes(self, capsys):
        from godmode_media_library.cli import _kill_old_server

        with patch("godmode_media_library.cli._get_pid_file") as mock_pf:
            mock_pf.return_value = Path("/tmp/nonexistent_pid_file_xyz.pid")
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(stdout="")
                _kill_old_server(9999)
        # Should not print anything about killing
        out = capsys.readouterr().out
        assert "Ukončeno" not in out


# ── Parser subcommand validation ────────────────────────────────────


class TestParserSubcommands:
    def test_scan_parser(self):
        parser = build_parser()
        args = parser.parse_args([
            "scan", "--roots", "/tmp/photos", "--force-rehash",
            "--min-size-kb", "100", "--no-media", "--no-phash",
            "--exiftool", "--workers", "4",
        ])
        assert args.command == "scan"
        assert args.force_rehash is True
        assert args.no_media is True
        assert args.no_phash is True
        assert args.workers == 4

    def test_query_parser(self):
        parser = build_parser()
        args = parser.parse_args([
            "query", "--ext", "jpg", "--date-from", "2024-01-01",
            "--date-to", "2024-12-31", "--duplicates",
        ])
        assert args.command == "query"
        assert args.ext == "jpg"
        assert args.duplicates is True

    def test_verify_parser(self):
        parser = build_parser()
        args = parser.parse_args(["verify", "--check-hashes", "--limit", "500"])
        assert args.command == "verify"
        assert args.check_hashes is True
        assert args.limit == 500

    def test_export_parser(self):
        parser = build_parser()
        args = parser.parse_args([
            "export", "files", "--out", "/tmp/export.csv",
            "--format", "csv",
        ])
        assert args.command == "export"
        assert args.what == "files"

    def test_similar_parser(self):
        parser = build_parser()
        args = parser.parse_args(["similar", "--threshold", "5", "--out", "/tmp/sim.tsv"])
        assert args.command == "similar"
        assert args.threshold == 5

    def test_batch_rename_parser(self):
        parser = build_parser()
        args = parser.parse_args([
            "batch-rename", "/tmp/photos", "--pattern", "{n:04d}",
            "--ext", "jpg", "--start", "10", "--dry-run",
        ])
        assert args.command == "batch-rename"
        assert args.start == 10
        assert args.dry_run is True

    def test_metadata_extract_parser(self):
        parser = build_parser()
        args = parser.parse_args(["metadata-extract", "--force"])
        assert args.command == "metadata-extract"
        assert args.force is True

    def test_metadata_diff_parser(self):
        parser = build_parser()
        args = parser.parse_args(["metadata-diff", "--group", "abc123"])
        assert args.command == "metadata-diff"
        assert args.group == "abc123"

    def test_metadata_merge_parser(self):
        parser = build_parser()
        args = parser.parse_args(["metadata-merge", "--apply", "--dry-run"])
        assert args.command == "metadata-merge"
        assert args.apply is True

    def test_metadata_write_parser(self):
        parser = build_parser()
        args = parser.parse_args([
            "metadata-write", "/tmp/a.jpg", "/tmp/b.jpg",
            "--tag", "Title=Hello", "--dry-run",
        ])
        assert args.command == "metadata-write"
        assert len(args.files) == 2

    def test_catalog_import_parser(self):
        parser = build_parser()
        args = parser.parse_args(["catalog-import", "--inventory", "/tmp/inv.tsv"])
        assert args.command == "catalog-import"

    def test_catalog_export_parser(self):
        parser = build_parser()
        args = parser.parse_args(["catalog-export", "--out", "/tmp/exp.tsv"])
        assert args.command == "catalog-export"

    def test_output_format_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--output-format", "json", "stats"])
        assert args.output_format == "json"

    def test_lang_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--lang", "cs", "config"])
        assert args.lang == "cs"
