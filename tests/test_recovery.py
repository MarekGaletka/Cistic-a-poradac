"""Tests for the recovery module (quarantine, deep scan, integrity, PhotoRec)."""

from __future__ import annotations

import json
import struct
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library.recovery import (
    _categorize_ext,
    _check_gif,
    _check_jpeg,
    _check_mp4,
    _check_png,
    _detect_type_by_magic,
    _sanitize_subprocess_path,
    _validate_quarantine_path,
    check_integrity,
    check_photorec,
    deep_scan,
    delete_from_quarantine,
    list_quarantine,
    recover_files,
    repair_file,
    restore_from_quarantine,
    run_photorec,
)

# ---------------------------------------------------------------------------
# _validate_quarantine_path
# ---------------------------------------------------------------------------


class TestValidateQuarantinePath:
    def test_valid_path_inside_root(self, tmp_path):
        child = tmp_path / "file.jpg"
        child.touch()
        result = _validate_quarantine_path(child, tmp_path)
        assert result == child.resolve()

    def test_path_traversal_rejected(self, tmp_path):
        evil = tmp_path / ".." / "etc" / "passwd"
        with pytest.raises(ValueError, match="Path traversal blocked"):
            _validate_quarantine_path(evil, tmp_path)

    def test_symlink_traversal_rejected(self, tmp_path):
        outside = tmp_path.parent / "outside_file"
        outside.touch()
        link = tmp_path / "sneaky_link"
        link.symlink_to(outside)
        with pytest.raises(ValueError, match="Path traversal blocked"):
            _validate_quarantine_path(link, tmp_path)
        outside.unlink()


# ---------------------------------------------------------------------------
# _sanitize_subprocess_path
# ---------------------------------------------------------------------------


class TestSanitizeSubprocessPath:
    def test_clean_path_accepted(self):
        assert _sanitize_subprocess_path("/dev/disk2") == "/dev/disk2"

    def test_shell_metachar_semicolon_rejected(self):
        with pytest.raises(ValueError, match="Invalid characters"):
            _sanitize_subprocess_path("/dev/disk2; rm -rf /")

    def test_shell_metachar_pipe_rejected(self):
        with pytest.raises(ValueError, match="Invalid characters"):
            _sanitize_subprocess_path("/dev/disk2 | cat")

    def test_shell_metachar_backtick_rejected(self):
        with pytest.raises(ValueError, match="Invalid characters"):
            _sanitize_subprocess_path("`whoami`")

    def test_shell_metachar_dollar_rejected(self):
        with pytest.raises(ValueError, match="Invalid characters"):
            _sanitize_subprocess_path("$(id)")

    def test_newline_rejected(self):
        with pytest.raises(ValueError, match="Invalid characters"):
            _sanitize_subprocess_path("/dev/disk2\nrm -rf /")


# ---------------------------------------------------------------------------
# _categorize_ext
# ---------------------------------------------------------------------------


class TestCategorizeExt:
    def test_image(self):
        assert _categorize_ext(".jpg") == "image"
        assert _categorize_ext(".png") == "image"
        assert _categorize_ext(".heic") == "image"

    def test_video(self):
        assert _categorize_ext(".mp4") == "video"
        assert _categorize_ext(".mov") == "video"

    def test_audio(self):
        assert _categorize_ext(".mp3") == "audio"
        assert _categorize_ext(".flac") == "audio"

    def test_other(self):
        assert _categorize_ext(".pdf") == "other"
        assert _categorize_ext(".txt") == "other"


# ---------------------------------------------------------------------------
# list_quarantine
# ---------------------------------------------------------------------------


class TestListQuarantine:
    def test_empty_dir(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        entries = list_quarantine(quarantine_root=qdir)
        assert entries == []

    def test_nonexistent_dir_returns_empty(self, tmp_path):
        entries = list_quarantine(quarantine_root=tmp_path / "does_not_exist")
        assert entries == []

    def test_with_files(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        (qdir / "photo.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 100)
        (qdir / "video.mp4").write_bytes(b"\x00" * 200)

        entries = list_quarantine(quarantine_root=qdir)
        assert len(entries) == 2
        names = {Path(e.path).name for e in entries}
        assert names == {"photo.jpg", "video.mp4"}

    def test_with_manifest(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        file_path = qdir / "photo.jpg"
        file_path.write_bytes(b"\xff\xd8" + b"\x00" * 100)

        manifest = {
            str(file_path): {
                "original_path": "/Users/test/Photos/photo.jpg",
                "quarantine_date": "2025-01-01T00:00:00",
            }
        }
        (qdir / "manifest.json").write_text(json.dumps(manifest))

        entries = list_quarantine(quarantine_root=qdir)
        assert len(entries) == 1
        assert entries[0].original_path == "/Users/test/Photos/photo.jpg"
        assert entries[0].quarantine_date == "2025-01-01T00:00:00"

    def test_nested_dirs(self, tmp_path):
        qdir = tmp_path / "quarantine"
        sub = qdir / "subdir"
        sub.mkdir(parents=True)
        (sub / "deep.png").write_bytes(b"\x89PNG" + b"\x00" * 50)

        entries = list_quarantine(quarantine_root=qdir)
        assert len(entries) == 1
        assert entries[0].ext == ".png"
        assert entries[0].category == "image"

    def test_manifest_json_excluded_from_entries(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        (qdir / "manifest.json").write_text("{}")
        (qdir / "file.jpg").write_bytes(b"\x00" * 10)

        entries = list_quarantine(quarantine_root=qdir)
        names = [Path(e.path).name for e in entries]
        assert "manifest.json" not in names

    def test_corrupt_manifest_is_handled(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        (qdir / "manifest.json").write_text("NOT VALID JSON{{{")
        (qdir / "file.jpg").write_bytes(b"\x00" * 10)

        # Should not raise; falls back to unknown original
        entries = list_quarantine(quarantine_root=qdir)
        assert len(entries) == 1
        assert entries[0].original_path == "unknown"


# ---------------------------------------------------------------------------
# restore_from_quarantine
# ---------------------------------------------------------------------------


class TestRestoreFromQuarantine:
    def test_restore_to_custom_dir(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        f = qdir / "photo.jpg"
        f.write_bytes(b"JPEG_DATA")

        dest = tmp_path / "restored"
        result = restore_from_quarantine(
            paths=[str(f)], quarantine_root=qdir, restore_to=str(dest)
        )
        assert result["restored"] == 1
        assert result["errors"] == []
        assert (dest / "photo.jpg").exists()
        assert not f.exists()

    def test_restore_to_original_via_manifest(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        f = qdir / "photo.jpg"
        f.write_bytes(b"JPEG_DATA")

        original_dir = tmp_path / "original_location"
        original_dir.mkdir()
        manifest = {
            str(f): {"original_path": str(original_dir / "photo.jpg")}
        }
        (qdir / "manifest.json").write_text(json.dumps(manifest))

        result = restore_from_quarantine(paths=[str(f)], quarantine_root=qdir)
        assert result["restored"] == 1
        assert (original_dir / "photo.jpg").exists()

    def test_path_traversal_rejected(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        evil_path = str(qdir / ".." / ".." / "etc" / "passwd")
        result = restore_from_quarantine(
            paths=[evil_path], quarantine_root=qdir
        )
        assert result["restored"] == 0
        assert len(result["errors"]) == 1
        assert "Path traversal blocked" in result["errors"][0]

    def test_nonexistent_file_error(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        result = restore_from_quarantine(
            paths=[str(qdir / "nope.jpg")], quarantine_root=qdir
        )
        assert result["restored"] == 0
        assert any("Not found" in e for e in result["errors"])

    def test_name_collision_resolved(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        f = qdir / "photo.jpg"
        f.write_bytes(b"NEW_DATA")

        dest = tmp_path / "restored"
        dest.mkdir()
        (dest / "photo.jpg").write_bytes(b"EXISTING")

        result = restore_from_quarantine(
            paths=[str(f)], quarantine_root=qdir, restore_to=str(dest)
        )
        assert result["restored"] == 1
        # Original still exists, new one has suffix
        assert (dest / "photo.jpg").read_bytes() == b"EXISTING"
        assert (dest / "photo_1.jpg").read_bytes() == b"NEW_DATA"

    def test_no_original_path_in_manifest(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        f = qdir / "photo.jpg"
        f.write_bytes(b"DATA")
        (qdir / "manifest.json").write_text("{}")

        result = restore_from_quarantine(paths=[str(f)], quarantine_root=qdir)
        assert result["restored"] == 0
        assert any("No original path" in e for e in result["errors"])


# ---------------------------------------------------------------------------
# delete_from_quarantine
# ---------------------------------------------------------------------------


class TestRestoreManifestGuard:
    """Regression: restore_from_quarantine must NOT overwrite a valid manifest
    when the manifest read fails (e.g. corrupted JSON on disk)."""

    def test_corrupt_manifest_not_overwritten(self, tmp_path):
        """If manifest.json is corrupt, it must NOT be overwritten with '{}'."""
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        f = qdir / "photo.jpg"
        f.write_bytes(b"JPEG_DATA")

        # Write a valid manifest, then corrupt it
        manifest_path = qdir / "manifest.json"
        original_content = '{"VALID": "DATA", "SHOULD_SURVIVE": true}'
        manifest_path.write_text(original_content)
        # Now corrupt it so json.loads fails
        manifest_path.write_text("NOT VALID JSON{{{")

        dest = tmp_path / "restored"
        restore_from_quarantine(
            paths=[str(f)], quarantine_root=qdir, restore_to=str(dest)
        )

        # The corrupt manifest should NOT have been overwritten with empty dict
        after = manifest_path.read_text()
        assert after == "NOT VALID JSON{{{", (
            "Corrupt manifest was overwritten — manifest_loaded guard failed"
        )

    def test_valid_manifest_is_updated_after_restore(self, tmp_path):
        """If manifest is valid, it should be updated (entry removed) after restore."""
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        f = qdir / "photo.jpg"
        f.write_bytes(b"JPEG_DATA")

        dest = tmp_path / "restored"
        dest.mkdir()
        manifest = {str(f): {"original_path": str(dest / "photo.jpg")}}
        (qdir / "manifest.json").write_text(json.dumps(manifest))

        restore_from_quarantine(paths=[str(f)], quarantine_root=qdir)

        updated = json.loads((qdir / "manifest.json").read_text())
        assert str(f) not in updated


class TestDeleteFromQuarantine:
    def test_delete_file(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        f = qdir / "photo.jpg"
        f.write_bytes(b"DELETE_ME")

        result = delete_from_quarantine(paths=[str(f)], quarantine_root=qdir)
        assert result["deleted"] == 1
        assert result["errors"] == []
        assert not f.exists()

    def test_path_traversal_rejected(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        evil = str(qdir / ".." / ".." / "etc" / "passwd")
        result = delete_from_quarantine(paths=[evil], quarantine_root=qdir)
        assert result["deleted"] == 0
        assert "Path traversal blocked" in result["errors"][0]

    def test_nonexistent_file(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        result = delete_from_quarantine(
            paths=[str(qdir / "nope.jpg")], quarantine_root=qdir
        )
        assert result["deleted"] == 0
        assert any("Not found" in e for e in result["errors"])

    def test_manifest_updated_after_delete(self, tmp_path):
        qdir = tmp_path / "quarantine"
        qdir.mkdir()
        f = qdir / "photo.jpg"
        f.write_bytes(b"DATA")
        manifest = {str(f): {"original_path": "/somewhere/photo.jpg"}}
        (qdir / "manifest.json").write_text(json.dumps(manifest))

        delete_from_quarantine(paths=[str(f)], quarantine_root=qdir)

        updated = json.loads((qdir / "manifest.json").read_text())
        assert str(f) not in updated


# ---------------------------------------------------------------------------
# deep_scan
# ---------------------------------------------------------------------------


class TestDeepScan:
    def test_scan_custom_roots(self, tmp_path):
        media_dir = tmp_path / "media"
        media_dir.mkdir()
        (media_dir / "found.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 200)
        (media_dir / "small.jpg").write_bytes(b"\xff\xd8")  # < 100 bytes, skipped
        (media_dir / "readme.txt").write_bytes(b"hello" * 50)  # non-media, skipped

        result = deep_scan(roots=[str(media_dir)])
        assert result.locations_scanned == 1
        assert result.files_found == 1
        assert result.files[0]["name"] == "found.jpg"

    def test_scan_empty_dir(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        result = deep_scan(roots=[str(empty)])
        assert result.files_found == 0

    def test_progress_callback(self, tmp_path):
        d = tmp_path / "scan"
        d.mkdir()
        (d / "a.mp4").write_bytes(b"\x00" * 200)

        progress_events = []
        deep_scan(roots=[str(d)], progress_fn=progress_events.append)
        phases = [e["phase"] for e in progress_events]
        assert "deep_scan" in phases
        assert "complete" in phases


# ---------------------------------------------------------------------------
# recover_files
# ---------------------------------------------------------------------------


class TestRecoverFiles:
    def test_copy_recovery(self, tmp_path):
        src_dir = tmp_path / "source"
        src_dir.mkdir()
        f = src_dir / "photo.jpg"
        f.write_bytes(b"PHOTO")

        dest = tmp_path / "dest"
        result = recover_files([str(f)], str(dest), delete_source=False)
        assert result["recovered"] == 1
        assert f.exists()  # source kept
        assert (dest / "photo.jpg").exists()

    def test_move_recovery(self, tmp_path):
        src_dir = tmp_path / "source"
        src_dir.mkdir()
        f = src_dir / "photo.jpg"
        f.write_bytes(b"PHOTO")

        dest = tmp_path / "dest"
        result = recover_files([str(f)], str(dest), delete_source=True)
        assert result["recovered"] == 1
        assert not f.exists()  # source moved

    def test_collision_avoidance(self, tmp_path):
        src_dir = tmp_path / "source"
        src_dir.mkdir()
        f = src_dir / "photo.jpg"
        f.write_bytes(b"NEW")

        dest = tmp_path / "dest"
        dest.mkdir()
        (dest / "photo.jpg").write_bytes(b"EXISTING")

        result = recover_files([str(f)], str(dest))
        assert result["recovered"] == 1
        assert (dest / "photo_1.jpg").read_bytes() == b"NEW"

    def test_missing_source(self, tmp_path):
        dest = tmp_path / "dest"
        result = recover_files(["/nonexistent/file.jpg"], str(dest))
        assert result["recovered"] == 0
        assert any("Not found" in e for e in result["errors"])


# ---------------------------------------------------------------------------
# check_integrity helpers
# ---------------------------------------------------------------------------


class TestCheckJpeg:
    def test_valid_jpeg(self, tmp_path):
        f = tmp_path / "ok.jpg"
        f.write_bytes(b"\xff\xd8" + b"\x00" * 100 + b"\xff\xd9")
        assert _check_jpeg(f) is None

    def test_truncated_jpeg_missing_eoi(self, tmp_path):
        f = tmp_path / "trunc.jpg"
        f.write_bytes(b"\xff\xd8" + b"\x00" * 100)
        result = _check_jpeg(f)
        assert result["issue"] == "truncated"
        assert result["repairable"] is True

    def test_invalid_header(self, tmp_path):
        f = tmp_path / "bad.jpg"
        f.write_bytes(b"\x00\x00" + b"\x00" * 100)
        result = _check_jpeg(f)
        assert result["issue"] == "invalid_header"

    def test_too_small(self, tmp_path):
        f = tmp_path / "tiny.jpg"
        f.write_bytes(b"\xff")
        result = _check_jpeg(f)
        assert result["issue"] == "truncated"


class TestCheckPng:
    def test_valid_png(self, tmp_path):
        f = tmp_path / "ok.png"
        f.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
        assert _check_png(f) is None

    def test_invalid_header(self, tmp_path):
        f = tmp_path / "bad.png"
        f.write_bytes(b"\x00" * 100)
        result = _check_png(f)
        assert result["issue"] == "invalid_header"


class TestCheckGif:
    def test_valid_gif87a(self, tmp_path):
        f = tmp_path / "ok.gif"
        f.write_bytes(b"GIF87a" + b"\x00" * 100)
        assert _check_gif(f) is None

    def test_valid_gif89a(self, tmp_path):
        f = tmp_path / "ok.gif"
        f.write_bytes(b"GIF89a" + b"\x00" * 100)
        assert _check_gif(f) is None

    def test_invalid(self, tmp_path):
        f = tmp_path / "bad.gif"
        f.write_bytes(b"NOT_GIF" + b"\x00" * 100)
        result = _check_gif(f)
        assert result["issue"] == "invalid_header"


class TestCheckMp4:
    def test_valid_mp4_with_moov(self, tmp_path):
        """Build a minimal valid MP4: ftyp box followed by moov box."""
        f = tmp_path / "ok.mp4"
        ftyp = struct.pack(">I", 12) + b"ftyp" + b"isom"
        moov = struct.pack(">I", 8) + b"moov"
        f.write_bytes(ftyp + moov)
        assert _check_mp4(f) is None

    def test_mp4_missing_moov(self, tmp_path):
        f = tmp_path / "no_moov.mp4"
        ftyp = struct.pack(">I", 12) + b"ftyp" + b"isom"
        mdat = struct.pack(">I", 16) + b"mdat" + b"\x00" * 8
        f.write_bytes(ftyp + mdat)
        result = _check_mp4(f)
        assert result["issue"] == "missing_moov"
        assert result["repairable"] is True

    def test_mp4_too_small(self, tmp_path):
        f = tmp_path / "tiny.mp4"
        f.write_bytes(b"\x00" * 4)
        result = _check_mp4(f)
        assert result["issue"] == "truncated"


# ---------------------------------------------------------------------------
# check_integrity (integration of the helpers)
# ---------------------------------------------------------------------------


class TestCheckIntegrity:
    def test_with_paths(self, tmp_path):
        good = tmp_path / "ok.jpg"
        good.write_bytes(b"\xff\xd8" + b"\x00" * 100 + b"\xff\xd9")
        bad = tmp_path / "bad.jpg"
        bad.write_bytes(b"\x00\x00" + b"\x00" * 100)

        result = check_integrity(paths=[str(good), str(bad)])
        assert result.total_checked == 2
        assert result.healthy == 1
        assert result.corrupted == 1

    def test_missing_file(self, tmp_path):
        result = check_integrity(paths=[str(tmp_path / "gone.jpg")])
        assert result.corrupted == 1
        assert result.errors[0]["issue"] == "missing"

    def test_progress_callback(self, tmp_path):
        f = tmp_path / "a.png"
        f.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 50)

        events = []
        check_integrity(paths=[str(f)], progress_fn=events.append)
        phases = [e["phase"] for e in events]
        assert "complete" in phases


# ---------------------------------------------------------------------------
# check_photorec
# ---------------------------------------------------------------------------


class TestCheckPhotorec:
    @patch("godmode_media_library.recovery.subprocess.run")
    def test_available(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout="PhotoRec 7.2\nSome other line", returncode=0
        )
        result = check_photorec()
        assert result["available"] is True
        assert "7.2" in result["version"]

    @patch("godmode_media_library.recovery.subprocess.run", side_effect=FileNotFoundError)
    def test_not_installed(self, mock_run):
        result = check_photorec()
        assert result["available"] is False


# ---------------------------------------------------------------------------
# run_photorec
# ---------------------------------------------------------------------------


class TestRunPhotorec:
    @patch("godmode_media_library.recovery.check_photorec")
    def test_not_installed_raises(self, mock_check):
        mock_check.return_value = {"available": False}
        with pytest.raises(RuntimeError, match="PhotoRec"):
            run_photorec("/dev/disk2")

    @patch("godmode_media_library.recovery.subprocess.run")
    @patch("godmode_media_library.recovery.check_photorec")
    def test_command_injection_source_rejected(self, mock_check, mock_run):
        mock_check.return_value = {"available": True}
        with pytest.raises(ValueError, match="Invalid characters"):
            run_photorec("/dev/disk2; rm -rf /")

    @patch("godmode_media_library.recovery.subprocess.run")
    @patch("godmode_media_library.recovery.check_photorec")
    def test_command_injection_output_dir_rejected(self, mock_check, mock_run):
        mock_check.return_value = {"available": True}
        with pytest.raises(ValueError, match="Invalid characters"):
            run_photorec("/dev/disk2", output_dir="/tmp/out; rm -rf /")

    @patch("godmode_media_library.recovery.subprocess.run")
    @patch("godmode_media_library.recovery.check_photorec")
    def test_invalid_file_type_rejected(self, mock_check, mock_run):
        mock_check.return_value = {"available": True}
        with pytest.raises(ValueError, match="Invalid file type"):
            run_photorec("/dev/disk2", file_types=["jpg;evil"])

    @patch("godmode_media_library.recovery.subprocess.run")
    @patch("godmode_media_library.recovery.check_photorec")
    def test_successful_run(self, mock_check, mock_run, tmp_path):
        mock_check.return_value = {"available": True}

        source = tmp_path / "disk.img"
        source.write_bytes(b"\x00" * 1024)
        out_dir = tmp_path / "recovery_output"
        out_dir.mkdir()
        # Simulate photorec having created a recovered file
        (out_dir / "f0001.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 100)

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        result = run_photorec(
            str(source), output_dir=str(out_dir), file_types=["jpg"]
        )
        assert result.files_recovered == 1
        assert result.output_dir == str(out_dir)

    @patch("godmode_media_library.recovery.subprocess.run")
    @patch("godmode_media_library.recovery.check_photorec")
    def test_source_not_found(self, mock_check, mock_run):
        mock_check.return_value = {"available": True}
        with pytest.raises(ValueError, match="Source does not exist"):
            run_photorec("/nonexistent/path/disk.img")


# ---------------------------------------------------------------------------
# repair_file
# ---------------------------------------------------------------------------


class TestRepairFile:
    def test_missing_file(self):
        result = repair_file("/nonexistent/file.jpg")
        assert result["success"] is False

    def test_unsupported_extension(self, tmp_path):
        f = tmp_path / "file.bmp"
        f.write_bytes(b"\x00" * 100)
        result = repair_file(str(f))
        assert result["success"] is False
        assert "není podporována" in result["error"]

    def test_repair_jpeg_dispatches(self, tmp_path):
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"\xff\xd8" + b"\x00" * 100)
        with patch("godmode_media_library.recovery._repair_jpeg", return_value={"success": True}) as mock_repair:
            result = repair_file(str(f))
            mock_repair.assert_called_once()
            assert result["success"] is True

    def test_repair_video_dispatches(self, tmp_path):
        for ext in (".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm"):
            f = tmp_path / f"video{ext}"
            f.write_bytes(b"\x00" * 100)
            with patch("godmode_media_library.recovery._repair_video", return_value={"success": True}) as mock_repair:
                result = repair_file(str(f))
                mock_repair.assert_called_once()
                assert result["success"] is True


# ---------------------------------------------------------------------------
# _detect_type_by_magic
# ---------------------------------------------------------------------------


class TestDetectTypeByMagic:
    def test_jpeg(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"\xff\xd8\xff" + b"\x00" * 50)
        result = _detect_type_by_magic(str(f))
        assert result == (".jpg", "image")

    def test_png(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"\x89PNG" + b"\x00" * 50)
        result = _detect_type_by_magic(str(f))
        assert result == (".png", "image")

    def test_gif89a(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"GIF89a" + b"\x00" * 50)
        result = _detect_type_by_magic(str(f))
        assert result == (".gif", "image")

    def test_riff_webp(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * 50)
        result = _detect_type_by_magic(str(f))
        assert result == (".webp", "image")

    def test_riff_wav(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"RIFF\x00\x00\x00\x00WAVE" + b"\x00" * 50)
        result = _detect_type_by_magic(str(f))
        assert result == (".wav", "audio")

    def test_riff_avi(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"RIFF\x00\x00\x00\x00AVI " + b"\x00" * 50)
        result = _detect_type_by_magic(str(f))
        assert result == (".avi", "video")

    def test_ftyp_isom(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"\x00\x00\x00\x18ftypisom" + b"\x00" * 50)
        result = _detect_type_by_magic(str(f))
        assert result is not None
        assert result[1] == "video"

    def test_flac(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"fLaC" + b"\x00" * 50)
        result = _detect_type_by_magic(str(f))
        assert result == (".flac", "audio")

    def test_ogg(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"OggS" + b"\x00" * 50)
        result = _detect_type_by_magic(str(f))
        assert result == (".ogg", "audio")

    def test_too_small(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"\x00\x00")
        result = _detect_type_by_magic(str(f))
        assert result is None

    def test_unknown_bytes(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"ZZZZ" + b"\x00" * 50)
        result = _detect_type_by_magic(str(f))
        assert result is None

    def test_nonexistent_file(self):
        result = _detect_type_by_magic("/nonexistent/file")
        assert result is None

    def test_id3_mp3(self, tmp_path):
        f = tmp_path / "noext"
        f.write_bytes(b"ID3" + b"\x00" * 50)
        result = _detect_type_by_magic(str(f))
        assert result == (".mp3", "audio")


# ---------------------------------------------------------------------------
# mine_app_media
# ---------------------------------------------------------------------------


class TestMineAppMedia:
    def test_mine_with_custom_app_ids(self, tmp_path):
        """Mine only specific apps — with mocked paths."""
        from godmode_media_library.recovery import mine_app_media

        # Create a fake app directory with media files
        app_dir = tmp_path / "app_data"
        app_dir.mkdir()
        (app_dir / "photo.jpg").write_bytes(b"\xff\xd8\xff" + b"\x00" * 200)
        (app_dir / "tiny.jpg").write_bytes(b"\xff\xd8")  # too small, <100 bytes

        fake_source = {
            "id": "test_app",
            "name": "Test App",
            "icon": "T",
            "color": "#000",
            "category": "test",
            "paths": [app_dir],
        }

        with patch("godmode_media_library.recovery._APP_SOURCES", [fake_source]):
            results = mine_app_media(app_ids=["test_app"])
            assert len(results) == 1
            assert results[0].app_id == "test_app"
            assert results[0].files_found == 1
            assert results[0].images == 1
            assert results[0].available is True

    def test_mine_encrypted_app(self, tmp_path):
        """Encrypted apps should count files but not scan content."""
        from godmode_media_library.recovery import mine_app_media

        app_dir = tmp_path / "encrypted_data"
        app_dir.mkdir()
        (app_dir / "blob1").write_bytes(b"\x00" * 500)
        (app_dir / "blob2").write_bytes(b"\x00" * 300)
        (app_dir / "tiny").write_bytes(b"\x00" * 50)  # <100, skipped

        fake_source = {
            "id": "enc_app",
            "name": "Encrypted App",
            "icon": "E",
            "color": "#000",
            "category": "test",
            "paths": [app_dir],
            "encrypted": True,
        }

        with patch("godmode_media_library.recovery._APP_SOURCES", [fake_source]):
            results = mine_app_media()
            assert len(results) == 1
            assert results[0].encrypted is True
            assert results[0].raw_files_count == 2
            assert results[0].raw_total_size == 800
            assert results[0].files_found == 0  # Not scanned for content

    def test_mine_nonexistent_paths(self, tmp_path):
        """Apps with no existing paths should show as not available."""
        from godmode_media_library.recovery import mine_app_media

        fake_source = {
            "id": "gone_app",
            "name": "Gone App",
            "icon": "G",
            "color": "#000",
            "category": "test",
            "paths": [tmp_path / "nonexistent"],
        }

        with patch("godmode_media_library.recovery._APP_SOURCES", [fake_source]):
            results = mine_app_media()
            assert len(results) == 1
            assert results[0].available is False
            assert results[0].files_found == 0

    def test_mine_with_progress_callback(self, tmp_path):
        from godmode_media_library.recovery import mine_app_media

        fake_source = {
            "id": "prog_app",
            "name": "Progress App",
            "icon": "P",
            "color": "#000",
            "category": "test",
            "paths": [tmp_path / "nonexistent"],
        }

        events = []
        with patch("godmode_media_library.recovery._APP_SOURCES", [fake_source]):
            mine_app_media(progress_fn=events.append)
        phases = [e["phase"] for e in events]
        assert "app_mining" in phases
        assert "complete" in phases

    def test_mine_extensionless_files(self, tmp_path):
        """Extensionless mode should detect media via magic bytes."""
        from godmode_media_library.recovery import mine_app_media

        app_dir = tmp_path / "signal_data"
        app_dir.mkdir()
        (app_dir / "attachment1").write_bytes(b"\xff\xd8\xff" + b"\x00" * 200)
        (app_dir / "attachment2").write_bytes(b"\x89PNG" + b"\x00" * 200)
        (app_dir / "attachment3").write_bytes(b"UNKNOWN" + b"\x00" * 200)  # not media

        fake_source = {
            "id": "signal_test",
            "name": "Signal Test",
            "icon": "S",
            "color": "#000",
            "category": "messaging",
            "paths": [app_dir],
            "extensionless": True,
        }

        with patch("godmode_media_library.recovery._APP_SOURCES", [fake_source]):
            results = mine_app_media()
            assert results[0].files_found == 2
            assert results[0].images == 2


# ---------------------------------------------------------------------------
# get_available_apps
# ---------------------------------------------------------------------------


class TestGetAvailableApps:
    def test_returns_list(self):
        from godmode_media_library.recovery import get_available_apps

        apps = get_available_apps()
        assert isinstance(apps, list)
        assert len(apps) > 0
        for app in apps:
            assert "id" in app
            assert "name" in app
            assert "available" in app
            assert isinstance(app["available"], bool)


# ---------------------------------------------------------------------------
# check_integrity with scan_roots
# ---------------------------------------------------------------------------


class TestCheckIntegrityExtended:
    def test_check_gif_integrity(self, tmp_path):
        good = tmp_path / "ok.gif"
        good.write_bytes(b"GIF89a" + b"\x00" * 100)
        bad = tmp_path / "bad.gif"
        bad.write_bytes(b"NOT_GIF" + b"\x00" * 100)

        result = check_integrity(paths=[str(good), str(bad)])
        assert result.healthy == 1
        assert result.corrupted == 1

    def test_check_png_integrity(self, tmp_path):
        good = tmp_path / "ok.png"
        good.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
        result = check_integrity(paths=[str(good)])
        assert result.healthy == 1

    def test_check_unknown_ext_passes(self, tmp_path):
        """Files with unknown extensions should be treated as healthy (no checker)."""
        f = tmp_path / "file.bmp"
        f.write_bytes(b"\x00" * 100)
        result = check_integrity(paths=[str(f)])
        assert result.healthy == 1
        assert result.corrupted == 0
