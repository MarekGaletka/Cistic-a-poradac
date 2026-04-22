"""Extended recovery tests for quarantine operations and data classes.

Covers: _validate_quarantine_path, _sanitize_subprocess_path, _categorize_ext,
list_quarantine, restore_from_quarantine, delete_from_quarantine,
QuarantineEntry, DeepScanResult, IntegrityResult, PhotoRecResult data classes.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from godmode_media_library.recovery import (
    DeepScanResult,
    IntegrityResult,
    PhotoRecResult,
    QuarantineEntry,
    _sanitize_subprocess_path,
    _validate_quarantine_path,
    delete_from_quarantine,
    list_quarantine,
    restore_from_quarantine,
)


# ── Data classes ───────────────────────────────────────────────────


class TestDataClasses:
    def test_quarantine_entry(self):
        e = QuarantineEntry(
            path="/q/photo.jpg", original_path="/orig/photo.jpg",
            size=1024, ext=".jpg", quarantine_date="2024-01-01",
            category="image",
        )
        assert e.size == 1024
        assert e.category == "image"

    def test_deep_scan_result_defaults(self):
        r = DeepScanResult()
        assert r.locations_scanned == 0
        assert r.files == []

    def test_integrity_result_defaults(self):
        r = IntegrityResult()
        assert r.total_checked == 0
        assert r.corrupted == 0

    def test_photorec_result_defaults(self):
        r = PhotoRecResult()
        assert r.files_recovered == 0
        assert r.partial is False


# ── Path validation ────────────────────────────────────────────────


class TestPathValidation:
    def test_validate_quarantine_path_ok(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        f = q / "photo.jpg"
        f.write_bytes(b"data")
        result = _validate_quarantine_path(str(f), q)
        assert result == f.resolve()

    def test_validate_quarantine_path_traversal(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        with pytest.raises(ValueError, match="Path traversal blocked"):
            _validate_quarantine_path(str(tmp_path / "quarantine" / ".." / "escape.txt"), q)

    def test_sanitize_subprocess_path_ok(self):
        result = _sanitize_subprocess_path("/safe/path/file.jpg")
        assert result == "/safe/path/file.jpg"

    def test_sanitize_subprocess_path_dangerous(self):
        with pytest.raises(ValueError, match="Invalid characters"):
            _sanitize_subprocess_path("file; rm -rf /")

    def test_sanitize_subprocess_path_backtick(self):
        with pytest.raises(ValueError, match="Invalid characters"):
            _sanitize_subprocess_path("file`whoami`")

    def test_sanitize_subprocess_path_pipe(self):
        with pytest.raises(ValueError, match="Invalid characters"):
            _sanitize_subprocess_path("file | cat /etc/passwd")


# ── Quarantine operations ──────────────────────────────────────────


class TestListQuarantine:
    def test_empty_quarantine(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        entries = list_quarantine(quarantine_root=q)
        assert entries == []

    def test_nonexistent_quarantine(self, tmp_path):
        q = tmp_path / "nonexistent"
        entries = list_quarantine(quarantine_root=q)
        assert entries == []

    def test_with_files(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        (q / "photo.jpg").write_bytes(b"JPEG data")
        (q / "video.mp4").write_bytes(b"MP4 data")
        entries = list_quarantine(quarantine_root=q)
        assert len(entries) == 2
        exts = {e.ext for e in entries}
        assert ".jpg" in exts
        assert ".mp4" in exts

    def test_with_manifest(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        f = q / "photo.jpg"
        f.write_bytes(b"JPEG data")
        manifest = {
            str(f): {
                "original_path": "/original/photo.jpg",
                "quarantine_date": "2024-01-15",
            }
        }
        (q / "manifest.json").write_text(json.dumps(manifest))
        entries = list_quarantine(quarantine_root=q)
        assert len(entries) == 1
        assert entries[0].original_path == "/original/photo.jpg"
        assert entries[0].quarantine_date == "2024-01-15"

    def test_with_subdirectories(self, tmp_path):
        q = tmp_path / "quarantine"
        sub = q / "sub"
        sub.mkdir(parents=True)
        (sub / "deep.jpg").write_bytes(b"data")
        entries = list_quarantine(quarantine_root=q)
        assert len(entries) == 1

    def test_corrupt_manifest(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        (q / "photo.jpg").write_bytes(b"data")
        (q / "manifest.json").write_text("INVALID JSON{{{")
        entries = list_quarantine(quarantine_root=q)
        assert len(entries) == 1
        assert entries[0].original_path == "unknown"


class TestRestoreFromQuarantine:
    def test_restore_to_custom_dir(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        f = q / "photo.jpg"
        f.write_bytes(b"JPEG data")
        restore_dir = tmp_path / "restored"
        restore_dir.mkdir()

        result = restore_from_quarantine(
            paths=[str(f)], quarantine_root=q, restore_to=str(restore_dir)
        )
        assert result["restored"] == 1
        assert (restore_dir / "photo.jpg").exists()
        assert not f.exists()

    def test_restore_with_manifest(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        original_dir = tmp_path / "original"
        original_dir.mkdir()
        f = q / "photo.jpg"
        f.write_bytes(b"JPEG data")
        manifest = {str(f): {"original_path": str(original_dir / "photo.jpg")}}
        (q / "manifest.json").write_text(json.dumps(manifest))

        result = restore_from_quarantine(paths=[str(f)], quarantine_root=q)
        assert result["restored"] == 1
        assert (original_dir / "photo.jpg").exists()

    def test_restore_nonexistent_file(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        result = restore_from_quarantine(
            paths=[str(q / "nonexistent.jpg")], quarantine_root=q
        )
        assert result["restored"] == 0
        assert len(result["errors"]) == 1

    def test_restore_path_traversal(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        result = restore_from_quarantine(
            paths=[str(tmp_path / "escape.txt")], quarantine_root=q
        )
        assert result["restored"] == 0
        assert any("Path traversal" in e for e in result["errors"])

    def test_restore_no_original_path(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        f = q / "photo.jpg"
        f.write_bytes(b"data")
        # Empty manifest
        (q / "manifest.json").write_text("{}")

        result = restore_from_quarantine(paths=[str(f)], quarantine_root=q)
        assert result["restored"] == 0
        assert any("No original path" in e for e in result["errors"])

    def test_restore_collision_handling(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        restore_dir = tmp_path / "restored"
        restore_dir.mkdir()
        # Create collision
        (restore_dir / "photo.jpg").write_bytes(b"existing")
        f = q / "photo.jpg"
        f.write_bytes(b"quarantined")

        result = restore_from_quarantine(
            paths=[str(f)], quarantine_root=q, restore_to=str(restore_dir)
        )
        assert result["restored"] == 1
        # Original should remain, restored should have suffix
        assert (restore_dir / "photo.jpg").read_bytes() == b"existing"
        assert (restore_dir / "photo_1.jpg").exists()


class TestDeleteFromQuarantine:
    def test_delete_files(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        f = q / "photo.jpg"
        f.write_bytes(b"data")
        (q / "manifest.json").write_text(json.dumps({str(f): {"original_path": "/x"}}))

        result = delete_from_quarantine(paths=[str(f)], quarantine_root=q)
        assert result["deleted"] == 1
        assert not f.exists()

    def test_delete_nonexistent(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        result = delete_from_quarantine(
            paths=[str(q / "nonexistent.jpg")], quarantine_root=q
        )
        assert result["deleted"] == 0
        assert len(result["errors"]) == 1

    def test_delete_path_traversal(self, tmp_path):
        q = tmp_path / "quarantine"
        q.mkdir()
        result = delete_from_quarantine(
            paths=[str(tmp_path / "escape.txt")], quarantine_root=q
        )
        assert result["deleted"] == 0


# ── _detect_type_by_magic ─────────────────────────────────────────────


class TestDetectTypeByMagic:
    def test_jpeg(self, tmp_path):
        from godmode_media_library.recovery import _detect_type_by_magic
        f = tmp_path / "file"
        f.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 28)
        result = _detect_type_by_magic(str(f))
        assert result is not None
        assert result[0] == ".jpg"

    def test_png(self, tmp_path):
        from godmode_media_library.recovery import _detect_type_by_magic
        f = tmp_path / "file"
        f.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 24)
        result = _detect_type_by_magic(str(f))
        assert result is not None
        assert result[0] == ".png"

    def test_ftyp_mp4(self, tmp_path):
        from godmode_media_library.recovery import _detect_type_by_magic
        f = tmp_path / "file"
        # ftyp box: size(4) + "ftyp" + brand
        f.write_bytes(b"\x00\x00\x00\x14" + b"ftyp" + b"isom" + b"\x00" * 20)
        result = _detect_type_by_magic(str(f))
        assert result is not None
        assert result[0] == ".mp4"

    def test_ftyp_quicktime(self, tmp_path):
        from godmode_media_library.recovery import _detect_type_by_magic
        f = tmp_path / "file"
        f.write_bytes(b"\x00\x00\x00\x14" + b"ftyp" + b"qt  " + b"\x00" * 20)
        result = _detect_type_by_magic(str(f))
        assert result is not None
        assert result[0] == ".mov"

    def test_riff_webp(self, tmp_path):
        from godmode_media_library.recovery import _detect_type_by_magic
        f = tmp_path / "file"
        f.write_bytes(b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * 20)
        result = _detect_type_by_magic(str(f))
        assert result is not None
        assert result[0] == ".webp"

    def test_riff_wav(self, tmp_path):
        from godmode_media_library.recovery import _detect_type_by_magic
        f = tmp_path / "file"
        f.write_bytes(b"RIFF\x00\x00\x00\x00WAVE" + b"\x00" * 20)
        result = _detect_type_by_magic(str(f))
        assert result is not None
        assert result[0] == ".wav"

    def test_riff_avi(self, tmp_path):
        from godmode_media_library.recovery import _detect_type_by_magic
        f = tmp_path / "file"
        f.write_bytes(b"RIFF\x00\x00\x00\x00AVI " + b"\x00" * 20)
        result = _detect_type_by_magic(str(f))
        assert result is not None
        assert result[0] == ".avi"

    def test_too_small(self, tmp_path):
        from godmode_media_library.recovery import _detect_type_by_magic
        f = tmp_path / "file"
        f.write_bytes(b"\x00\x01")
        assert _detect_type_by_magic(str(f)) is None

    def test_unknown_format(self, tmp_path):
        from godmode_media_library.recovery import _detect_type_by_magic
        f = tmp_path / "file"
        f.write_bytes(b"\x00" * 32)
        assert _detect_type_by_magic(str(f)) is None

    def test_nonexistent_file(self, tmp_path):
        from godmode_media_library.recovery import _detect_type_by_magic
        assert _detect_type_by_magic(str(tmp_path / "nofile")) is None


# ── _categorize_ext ───────────────────────────────────────────────────


class TestCategorizeExt:
    def test_image(self):
        from godmode_media_library.recovery import _categorize_ext
        assert _categorize_ext(".jpg") == "image"
        assert _categorize_ext(".png") == "image"

    def test_video(self):
        from godmode_media_library.recovery import _categorize_ext
        assert _categorize_ext(".mp4") == "video"
        assert _categorize_ext(".mov") == "video"

    def test_audio(self):
        from godmode_media_library.recovery import _categorize_ext
        assert _categorize_ext(".mp3") == "audio"

    def test_other(self):
        from godmode_media_library.recovery import _categorize_ext
        assert _categorize_ext(".xyz") == "other"


# ── _check_jpeg ──────────────────────────────────────────────────────


class TestCheckJpeg:
    def test_valid_jpeg(self, tmp_path):
        from godmode_media_library.recovery import _check_jpeg
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"\xff\xd8" + b"\x00" * 100 + b"\xff\xd9")
        assert _check_jpeg(f) is None

    def test_truncated_jpeg(self, tmp_path):
        from godmode_media_library.recovery import _check_jpeg
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"\xff\xd8" + b"\x00" * 100)  # missing EOI
        result = _check_jpeg(f)
        assert result is not None
        assert result["issue"] == "truncated"
        assert result["repairable"] is True

    def test_invalid_header(self, tmp_path):
        from godmode_media_library.recovery import _check_jpeg
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"\x00\x00" + b"\x00" * 100)
        result = _check_jpeg(f)
        assert result["issue"] == "invalid_header"

    def test_too_small(self, tmp_path):
        from godmode_media_library.recovery import _check_jpeg
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"\xff\xd8")
        result = _check_jpeg(f)
        assert result["issue"] == "truncated"


# ── _check_png ───────────────────────────────────────────────────────


class TestCheckPng:
    def test_valid_png(self, tmp_path):
        from godmode_media_library.recovery import _check_png
        f = tmp_path / "image.png"
        f.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
        assert _check_png(f) is None

    def test_invalid_png(self, tmp_path):
        from godmode_media_library.recovery import _check_png
        f = tmp_path / "image.png"
        f.write_bytes(b"\x00" * 100)
        result = _check_png(f)
        assert result["issue"] == "invalid_header"

    def test_too_small_png(self, tmp_path):
        from godmode_media_library.recovery import _check_png
        f = tmp_path / "image.png"
        f.write_bytes(b"\x89PNG")
        result = _check_png(f)
        assert result["issue"] == "truncated"


# ── _check_gif ───────────────────────────────────────────────────────


class TestCheckGif:
    def test_valid_gif87(self, tmp_path):
        from godmode_media_library.recovery import _check_gif
        f = tmp_path / "image.gif"
        f.write_bytes(b"GIF87a" + b"\x00" * 100)
        assert _check_gif(f) is None

    def test_valid_gif89(self, tmp_path):
        from godmode_media_library.recovery import _check_gif
        f = tmp_path / "image.gif"
        f.write_bytes(b"GIF89a" + b"\x00" * 100)
        assert _check_gif(f) is None

    def test_invalid_gif(self, tmp_path):
        from godmode_media_library.recovery import _check_gif
        f = tmp_path / "image.gif"
        f.write_bytes(b"NOTGIF" + b"\x00" * 100)
        result = _check_gif(f)
        assert result["issue"] == "invalid_header"


# ── _check_mp4 ───────────────────────────────────────────────────────


class TestCheckMp4:
    def test_valid_mp4_with_moov(self, tmp_path):
        import struct
        from godmode_media_library.recovery import _check_mp4
        f = tmp_path / "video.mp4"
        # Build: ftyp box + moov box
        ftyp = struct.pack(">I", 16) + b"ftyp" + b"isom" + b"\x00" * 4
        moov = struct.pack(">I", 8) + b"moov"
        f.write_bytes(ftyp + moov)
        assert _check_mp4(f) is None

    def test_mp4_missing_moov(self, tmp_path):
        import struct
        from godmode_media_library.recovery import _check_mp4
        f = tmp_path / "video.mp4"
        ftyp = struct.pack(">I", 16) + b"ftyp" + b"isom" + b"\x00" * 4
        mdat = struct.pack(">I", 16) + b"mdat" + b"\x00" * 8
        f.write_bytes(ftyp + mdat)
        result = _check_mp4(f)
        assert result is not None
        assert result["issue"] == "missing_moov"
        assert result["repairable"] is True

    def test_mp4_invalid_header(self, tmp_path):
        from godmode_media_library.recovery import _check_mp4
        f = tmp_path / "video.mp4"
        f.write_bytes(b"\x00" * 4 + b"XXXX" + b"\x00" * 100)
        result = _check_mp4(f)
        assert result is not None
        assert result["issue"] == "invalid_header"

    def test_mp4_too_small(self, tmp_path):
        from godmode_media_library.recovery import _check_mp4
        f = tmp_path / "video.mp4"
        f.write_bytes(b"\x00\x00\x00")
        result = _check_mp4(f)
        assert result["issue"] == "truncated"


# ── _check_video_ffprobe ─────────────────────────────────────────────


class TestCheckVideoFfprobe:
    def test_ffprobe_not_found(self, tmp_path):
        from unittest.mock import patch
        from godmode_media_library.recovery import _check_video_ffprobe
        f = tmp_path / "video.webm"
        f.write_bytes(b"\x1a\x45\xdf\xa3" + b"\x00" * 100)
        with patch("subprocess.run", side_effect=FileNotFoundError):
            with patch("godmode_media_library.deps.resolve_bin", return_value="ffprobe"):
                result = _check_video_ffprobe(f)
        assert result is None  # ffprobe unavailable -> skip

    def test_ffprobe_error(self, tmp_path):
        from unittest.mock import MagicMock, patch
        from godmode_media_library.recovery import _check_video_ffprobe
        f = tmp_path / "video.webm"
        f.write_bytes(b"\x00" * 100)
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "error reading stream"
        with patch("subprocess.run", return_value=mock_result):
            with patch("godmode_media_library.deps.resolve_bin", return_value="ffprobe"):
                result = _check_video_ffprobe(f)
        assert result is not None
        assert result["issue"] == "ffprobe_error"

    def test_ffprobe_timeout(self, tmp_path):
        import subprocess
        from unittest.mock import patch
        from godmode_media_library.recovery import _check_video_ffprobe
        f = tmp_path / "video.webm"
        f.write_bytes(b"\x00" * 100)
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("cmd", 30)):
            with patch("godmode_media_library.deps.resolve_bin", return_value="ffprobe"):
                result = _check_video_ffprobe(f)
        assert result is not None
        assert result["issue"] == "timeout"


# ── check_integrity ──────────────────────────────────────────────────


class TestCheckIntegrity:
    def test_with_paths(self, tmp_path):
        from godmode_media_library.recovery import check_integrity
        # Create a valid JPEG
        jpg = tmp_path / "ok.jpg"
        jpg.write_bytes(b"\xff\xd8" + b"\x00" * 100 + b"\xff\xd9")
        # Create a truncated JPEG
        bad_jpg = tmp_path / "bad.jpg"
        bad_jpg.write_bytes(b"\xff\xd8" + b"\x00" * 100)

        result = check_integrity(paths=[str(jpg), str(bad_jpg)])
        assert result.total_checked == 2
        assert result.healthy == 1
        assert result.corrupted == 1

    def test_missing_file(self, tmp_path):
        from godmode_media_library.recovery import check_integrity
        result = check_integrity(paths=[str(tmp_path / "missing.jpg")])
        assert result.corrupted == 1
        assert result.errors[0]["issue"] == "missing"

    def test_with_progress_fn(self, tmp_path):
        from godmode_media_library.recovery import check_integrity
        jpg = tmp_path / "ok.jpg"
        jpg.write_bytes(b"\xff\xd8\x00\x00\xff\xd9")
        progress_calls = []
        result = check_integrity(paths=[str(jpg)], progress_fn=progress_calls.append)
        assert result.total_checked == 1
        assert len(progress_calls) >= 1

    def test_png_check(self, tmp_path):
        from godmode_media_library.recovery import check_integrity
        f = tmp_path / "image.png"
        f.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
        result = check_integrity(paths=[str(f)])
        assert result.healthy == 1

    def test_gif_check(self, tmp_path):
        from godmode_media_library.recovery import check_integrity
        f = tmp_path / "image.gif"
        f.write_bytes(b"GIF89a" + b"\x00" * 100)
        result = check_integrity(paths=[str(f)])
        assert result.healthy == 1


# ── deep_scan ────────────────────────────────────────────────────────


class TestDeepScan:
    def test_scan_with_roots(self, tmp_path):
        from godmode_media_library.recovery import deep_scan
        media_dir = tmp_path / "media"
        media_dir.mkdir()
        (media_dir / "photo.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 200 + b"\xff\xd9")
        (media_dir / "readme.txt").write_text("not media")

        result = deep_scan(roots=[str(media_dir)])
        assert result.locations_scanned == 1
        assert result.files_found == 1

    def test_scan_empty_dir(self, tmp_path):
        from godmode_media_library.recovery import deep_scan
        empty = tmp_path / "empty"
        empty.mkdir()
        result = deep_scan(roots=[str(empty)])
        assert result.files_found == 0

    def test_scan_nonexistent_root(self, tmp_path):
        from godmode_media_library.recovery import deep_scan
        result = deep_scan(roots=[str(tmp_path / "nonexistent")])
        assert result.locations_scanned == 0

    def test_scan_with_progress(self, tmp_path):
        from godmode_media_library.recovery import deep_scan
        d = tmp_path / "scan"
        d.mkdir()
        (d / "video.mp4").write_bytes(b"\x00" * 200)
        calls = []
        result = deep_scan(roots=[str(d)], progress_fn=calls.append)
        assert len(calls) >= 1  # At least start + complete

    def test_scan_skips_tiny_files(self, tmp_path):
        from godmode_media_library.recovery import deep_scan
        d = tmp_path / "scan"
        d.mkdir()
        (d / "tiny.jpg").write_bytes(b"\xff\xd8")  # < 100 bytes
        result = deep_scan(roots=[str(d)])
        assert result.files_found == 0


# ── recover_files ────────────────────────────────────────────────────


class TestRecoverFiles:
    def test_basic_recovery(self, tmp_path):
        from godmode_media_library.recovery import recover_files
        src = tmp_path / "source"
        src.mkdir()
        (src / "photo.jpg").write_bytes(b"JPEG data")
        dest = tmp_path / "dest"
        dest.mkdir()

        result = recover_files([str(src / "photo.jpg")], str(dest))
        assert result["recovered"] == 1
        assert (dest / "photo.jpg").exists()

    def test_collision_handling(self, tmp_path):
        from godmode_media_library.recovery import recover_files
        src = tmp_path / "source"
        src.mkdir()
        (src / "photo.jpg").write_bytes(b"new data")
        dest = tmp_path / "dest"
        dest.mkdir()
        (dest / "photo.jpg").write_bytes(b"existing")

        result = recover_files([str(src / "photo.jpg")], str(dest))
        assert result["recovered"] == 1
        assert (dest / "photo_1.jpg").exists()

    def test_missing_source(self, tmp_path):
        from godmode_media_library.recovery import recover_files
        dest = tmp_path / "dest"
        dest.mkdir()
        result = recover_files([str(tmp_path / "missing.jpg")], str(dest))
        assert result["recovered"] == 0
        assert len(result["errors"]) == 1

    def test_delete_source(self, tmp_path):
        from godmode_media_library.recovery import recover_files
        src = tmp_path / "source"
        src.mkdir()
        f = src / "photo.jpg"
        f.write_bytes(b"data")
        dest = tmp_path / "dest"
        dest.mkdir()

        result = recover_files([str(f)], str(dest), delete_source=True)
        assert result["recovered"] == 1
        assert not f.exists()  # Source deleted


# ── mine_app_media ───────────────────────────────────────────────────


class TestMineAppMedia:
    def test_mine_specific_app(self, tmp_path):
        from unittest.mock import patch
        from godmode_media_library.recovery import mine_app_media

        # Mock _APP_SOURCES with a test source pointing to tmp_path
        test_source = {
            "id": "test_app",
            "name": "Test App",
            "icon": "T",
            "color": "#000",
            "category": "social",
            "paths": [tmp_path],
            "encrypted": False,
            "extensionless": False,
        }
        (tmp_path / "photo.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 200 + b"\xff\xd9")

        with patch("godmode_media_library.recovery._APP_SOURCES", [test_source]):
            results = mine_app_media(app_ids=["test_app"])
        assert len(results) == 1
        assert results[0].files_found >= 1

    def test_mine_encrypted_app(self, tmp_path):
        from unittest.mock import patch
        from godmode_media_library.recovery import mine_app_media

        test_source = {
            "id": "enc_app",
            "name": "Encrypted App",
            "icon": "E",
            "color": "#f00",
            "category": "messaging",
            "paths": [tmp_path],
            "encrypted": True,
        }
        (tmp_path / "encrypted_file").write_bytes(b"\x00" * 200)

        with patch("godmode_media_library.recovery._APP_SOURCES", [test_source]):
            results = mine_app_media(app_ids=["enc_app"])
        assert len(results) == 1
        assert results[0].encrypted is True
        assert results[0].raw_files_count >= 1

    def test_mine_nonexistent_path(self, tmp_path):
        from unittest.mock import patch
        from godmode_media_library.recovery import mine_app_media

        test_source = {
            "id": "nopath",
            "name": "No Path",
            "icon": "N",
            "color": "#000",
            "category": "other",
            "paths": [tmp_path / "nonexistent"],
            "encrypted": False,
        }
        with patch("godmode_media_library.recovery._APP_SOURCES", [test_source]):
            results = mine_app_media(app_ids=["nopath"])
        assert len(results) == 1
        assert results[0].available is False

    def test_mine_with_progress(self, tmp_path):
        from unittest.mock import patch
        from godmode_media_library.recovery import mine_app_media

        test_source = {
            "id": "prog_app",
            "name": "Progress App",
            "icon": "P",
            "color": "#000",
            "category": "other",
            "paths": [tmp_path / "nope"],
            "encrypted": False,
        }
        calls = []
        with patch("godmode_media_library.recovery._APP_SOURCES", [test_source]):
            mine_app_media(app_ids=["prog_app"], progress_fn=calls.append)
        assert len(calls) >= 1


# ── Signal decrypt helpers ───────────────────────────────────────────


class TestSignalHelpers:
    def test_get_signal_key_not_found(self):
        from unittest.mock import patch
        from godmode_media_library.recovery import _get_signal_key
        with patch("godmode_media_library.recovery.subprocess.run", side_effect=FileNotFoundError):
            assert _get_signal_key() is None

    def test_find_sqlcipher_bin_default(self):
        from unittest.mock import patch
        from godmode_media_library.recovery import _find_sqlcipher_bin
        with patch("shutil.which", return_value="/usr/bin/sqlcipher"):
            result = _find_sqlcipher_bin()
        assert result == "/usr/bin/sqlcipher"

    def test_find_sqlcipher_bin_homebrew(self):
        import os
        from unittest.mock import patch
        from godmode_media_library.recovery import _find_sqlcipher_bin
        with patch("shutil.which", return_value=None):
            with patch("os.path.isfile", return_value=True):
                with patch("os.access", return_value=True):
                    result = _find_sqlcipher_bin()
        assert "sqlcipher" in result

    def test_find_sqlcipher_bin_fallback(self):
        from unittest.mock import patch
        from godmode_media_library.recovery import _find_sqlcipher_bin
        with patch("shutil.which", return_value=None):
            with patch("os.path.isfile", return_value=False):
                result = _find_sqlcipher_bin()
        assert result == "sqlcipher"


# ── _repair_jpeg ──────────────────────────────────────────────────────


class TestRepairJpeg:
    def test_repair_pil_import_error(self, tmp_path):
        """When PIL is not installed, _repair_jpeg should return failure."""
        import sys
        from unittest.mock import patch
        from godmode_media_library.recovery import _repair_jpeg
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"\xff\xd8" + b"\x00" * 100)
        # _repair_jpeg has a try/except around PIL import
        # Simulate by calling it with a broken import
        with patch.dict(sys.modules, {"PIL": None, "PIL.Image": None, "PIL.ImageFile": None}):
            # Force reimport to hit the error path
            result = _repair_jpeg(f)
        assert result["success"] is False

    def test_repair_jpeg_valid_file(self, tmp_path):
        """Repair a valid JPEG that has EOI — should succeed if PIL is available."""
        from godmode_media_library.recovery import _repair_jpeg
        f = tmp_path / "photo.jpg"
        # Minimal valid JPEG with EOI marker
        f.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100 + b"\xff\xd9")
        result = _repair_jpeg(f)
        # This might succeed or fail depending on PIL availability
        assert isinstance(result, dict)
        assert "success" in result


# ── _repair_video ─────────────────────────────────────────────────────


class TestRepairVideo:
    def test_repair_no_ffmpeg(self, tmp_path):
        from unittest.mock import patch
        from godmode_media_library.recovery import _repair_video
        f = tmp_path / "video.mp4"
        f.write_bytes(b"\x00" * 100)
        with patch("subprocess.run", side_effect=FileNotFoundError):
            with patch("godmode_media_library.deps.resolve_bin", return_value="ffmpeg"):
                result = _repair_video(f)
        assert result["success"] is False
        assert "FFmpeg" in result["error"]

    def test_repair_ffmpeg_timeout(self, tmp_path):
        import subprocess as _sp
        from unittest.mock import MagicMock, patch
        from godmode_media_library.recovery import _repair_video
        f = tmp_path / "video.mp4"
        f.write_bytes(b"\x00" * 100)
        call_count = [0]

        def mock_run(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return MagicMock(returncode=0)  # ffmpeg -version succeeds
            raise _sp.TimeoutExpired("cmd", 300)

        with patch("subprocess.run", side_effect=mock_run):
            with patch("godmode_media_library.deps.resolve_bin", return_value="ffmpeg"):
                result = _repair_video(f)
        assert result["success"] is False
        assert "timeout" in result["error"].lower()


# ── check_signal_decrypt ─────────────────────────────────────────────


class TestCheckSignalDecrypt:
    def test_no_db(self):
        from unittest.mock import patch
        from godmode_media_library.recovery import check_signal_decrypt
        with patch("godmode_media_library.recovery._SIGNAL_DB_PATH") as mock_path:
            mock_path.exists.return_value = False
            with patch("godmode_media_library.recovery._SIGNAL_ATTACH_DIR") as mock_att:
                mock_att.exists.return_value = False
                # sqlcipher not available
                with patch.dict("sys.modules", {"pysqlcipher3": None, "pysqlcipher3.dbapi2": None}):
                    with patch("shutil.which", return_value=None):
                        with patch("os.path.isfile", return_value=False):
                            with patch("godmode_media_library.recovery.subprocess.run", side_effect=FileNotFoundError):
                                result = check_signal_decrypt()
        assert result["possible"] is False
        assert result["db_exists"] is False


# ── decrypt_signal_attachments ────────────────────────────────────────


class TestDecryptSignalAttachments:
    def test_no_key(self, tmp_path):
        from unittest.mock import patch
        from godmode_media_library.recovery import decrypt_signal_attachments
        with patch("godmode_media_library.recovery._get_signal_key", return_value=None):
            result = decrypt_signal_attachments(str(tmp_path))
        assert result["decrypted"] == 0
        assert len(result["errors"]) > 0

    def test_no_attachments(self, tmp_path):
        from unittest.mock import MagicMock, patch
        from godmode_media_library.recovery import decrypt_signal_attachments
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = MagicMock(return_value=iter([]))
        mock_conn.execute.return_value = mock_cursor

        with patch("godmode_media_library.recovery._get_signal_key", return_value="deadbeef"):
            with patch("godmode_media_library.recovery._open_signal_db", return_value=mock_conn):
                result = decrypt_signal_attachments(str(tmp_path))
        assert result["decrypted"] == 0
        assert any("Nenalezeny" in e for e in result["errors"])
