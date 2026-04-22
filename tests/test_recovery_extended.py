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
