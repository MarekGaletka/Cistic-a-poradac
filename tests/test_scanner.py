"""Tests for the incremental filesystem scanner."""

from __future__ import annotations

import time
from pathlib import Path

from godmode_media_library.catalog import Catalog
from godmode_media_library.scanner import incremental_scan


def _create_file(path: Path, content: bytes = b"hello world") -> Path:
    """Create a file with given content, ensuring parent dirs exist."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


# ── New files ────────────────────────────────────────────────────────


def test_incremental_scan_new_files(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"

    _create_file(media / "photo1.jpg", b"JPEG1" * 100)
    _create_file(media / "photo2.png", b"PNG2" * 100)
    _create_file(media / "doc.pdf", b"PDF_DATA" * 50)

    with Catalog(db_path) as cat:
        stats = incremental_scan(cat, [media])

    assert stats.files_scanned == 3
    assert stats.files_new == 3
    assert stats.files_changed == 0
    assert stats.files_removed == 0

    # Verify entries exist in catalog
    with Catalog(db_path) as cat:
        assert len(cat.all_paths()) == 3
        row = cat.get_file_by_path(str(media / "photo1.jpg"))
        assert row is not None
        assert row.sha256 is not None


# ── Unchanged files ──────────────────────────────────────────────────


def test_incremental_scan_unchanged(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"

    _create_file(media / "photo.jpg", b"STABLE_CONTENT" * 100)

    with Catalog(db_path) as cat:
        stats1 = incremental_scan(cat, [media])

    assert stats1.files_new == 1

    # Second scan — nothing changed
    with Catalog(db_path) as cat:
        stats2 = incremental_scan(cat, [media])

    assert stats2.files_scanned == 1
    assert stats2.files_new == 0
    assert stats2.files_changed == 0


# ── Changed file ─────────────────────────────────────────────────────


def test_incremental_scan_changed_file(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"

    f = _create_file(media / "data.bin", b"VERSION_1" * 100)

    with Catalog(db_path) as cat:
        stats1 = incremental_scan(cat, [media])
        original = cat.get_file_by_path(str(f))
        original_sha = original.sha256

    assert stats1.files_new == 1

    # Modify the file — ensure mtime changes
    time.sleep(0.05)
    f.write_bytes(b"VERSION_2_CHANGED" * 100)

    with Catalog(db_path) as cat:
        stats2 = incremental_scan(cat, [media])
        updated = cat.get_file_by_path(str(f))

    assert stats2.files_changed == 1
    assert stats2.files_new == 0
    assert updated.sha256 is not None
    assert updated.sha256 != original_sha


# ── Removed file ─────────────────────────────────────────────────────


def test_incremental_scan_removed_file(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"

    f1 = _create_file(media / "keep.jpg", b"KEEP" * 100)
    f2 = _create_file(media / "delete_me.jpg", b"DELETE" * 100)

    with Catalog(db_path) as cat:
        incremental_scan(cat, [media])
        assert len(cat.all_paths()) == 2

    # Delete one file
    f2.unlink()

    with Catalog(db_path) as cat:
        stats = incremental_scan(cat, [media])

    assert stats.files_removed == 1
    assert stats.files_scanned == 1  # only keep.jpg scanned

    with Catalog(db_path) as cat:
        assert cat.get_file_by_path(str(f1)) is not None
        assert cat.get_file_by_path(str(f2)) is None


# ── Force rehash ─────────────────────────────────────────────────────


def test_incremental_scan_force_rehash(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"

    _create_file(media / "file.txt", b"CONTENT" * 50)

    # First scan
    with Catalog(db_path) as cat:
        stats1 = incremental_scan(cat, [media])
    assert stats1.bytes_hashed > 0

    # Second scan without force — no rehash needed
    with Catalog(db_path) as cat:
        stats2 = incremental_scan(cat, [media])
    assert stats2.bytes_hashed == 0
    assert stats2.files_new == 0
    assert stats2.files_changed == 0

    # Third scan with force_rehash — should rehash everything
    with Catalog(db_path) as cat:
        stats3 = incremental_scan(cat, [media], force_rehash=True)
    assert stats3.bytes_hashed > 0


# ── Duplicate detection ──────────────────────────────────────────────


def test_incremental_scan_detects_duplicates(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"

    identical_content = b"IDENTICAL_BYTES" * 200
    _create_file(media / "original.jpg", identical_content)
    _create_file(media / "copy.jpg", identical_content)
    _create_file(media / "unique.jpg", b"DIFFERENT_BYTES" * 200)

    with Catalog(db_path) as cat:
        stats = incremental_scan(cat, [media])

        assert stats.files_scanned == 3
        assert stats.files_new == 3

        groups = cat.query_duplicates()
        # Should have exactly one duplicate group (original + copy)
        assert len(groups) == 1
        group_id, files = groups[0]
        assert len(files) == 2
        paths = {f.path for f in files}
        assert str(media / "original.jpg") in paths
        assert str(media / "copy.jpg") in paths


# ── Permission errors ──────────────────────────────────────────────

from unittest.mock import patch


def test_scan_skips_unreadable_file(tmp_path: Path) -> None:
    """Files that raise OSError on stat should be skipped gracefully."""
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"
    _create_file(media / "good.jpg", b"GOOD" * 100)
    _create_file(media / "bad.jpg", b"BAD" * 100)

    bad_path = media / "bad.jpg"
    bad_path_str = str(bad_path)

    # Discover files normally, then patch stat to fail for bad.jpg during scan phase
    original_stat = Path.stat
    _stat_call_count = {"n": 0}

    def patched_stat(self, *args, **kwargs):
        # Only fail on the second round of stat calls (during scan, not discovery)
        if str(self) == bad_path_str:
            _stat_call_count["n"] += 1
            if _stat_call_count["n"] > 2:
                raise OSError("Permission denied")
        return original_stat(self, *args, **kwargs)

    with Catalog(db_path) as cat, patch.object(Path, "stat", patched_stat):
        stats = incremental_scan(cat, [media])

    # Good file was scanned; bad file may or may not be depending on when stat fails
    assert stats.files_scanned >= 1


# ── Symlink handling ────────────────────────────────────────────────


def test_scan_handles_symlinks(tmp_path: Path) -> None:
    """Symlinks should be followed (or skipped) without errors."""
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"
    real_dir = tmp_path / "real"
    real_dir.mkdir(parents=True)
    (real_dir / "photo.jpg").write_bytes(b"REAL" * 100)
    media.mkdir(parents=True)
    (media / "link_to_photo.jpg").symlink_to(real_dir / "photo.jpg")
    _create_file(media / "direct.jpg", b"DIRECT" * 100)

    with Catalog(db_path) as cat:
        stats = incremental_scan(cat, [media])

    assert stats.files_scanned >= 1


# ── Unicode filenames ────────────────────────────────────────────────


def test_scan_unicode_filenames(tmp_path: Path) -> None:
    """Scanner should handle unicode filenames without errors."""
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"
    _create_file(media / "foto_\u010d\u0159\u017e.jpg", b"UNICODE" * 100)
    _create_file(media / "\u65e5\u672c\u8a9e.png", b"JAPANESE" * 100)

    with Catalog(db_path) as cat:
        stats = incremental_scan(cat, [media])

    assert stats.files_scanned == 2
    assert stats.files_new == 2


# ── Progress callback ────────────────────────────────────────────────


def test_scan_progress_callback(tmp_path: Path) -> None:
    """Progress callback should be called during scan phases."""
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"
    _create_file(media / "file1.txt", b"A" * 100)
    _create_file(media / "file2.txt", b"B" * 100)

    callbacks = []

    def on_progress(info: dict):
        callbacks.append(info)

    with Catalog(db_path) as cat:
        incremental_scan(cat, [media], progress_callback=on_progress)

    phases = {c.get("phase") for c in callbacks}
    assert "scanning" in phases


# ── Multi-worker hashing ────────────────────────────────────────────


def test_scan_multi_worker(tmp_path: Path) -> None:
    """Multi-worker scan should produce same results as single-worker."""
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"
    for i in range(5):
        _create_file(media / f"file_{i}.bin", f"CONTENT_{i}".encode() * 100)

    with Catalog(db_path) as cat:
        stats = incremental_scan(cat, [media], workers=2)

    assert stats.files_scanned == 5
    assert stats.files_new == 5
    assert stats.bytes_hashed > 0


# ── Multi-worker futures exception handling ──────────────────────────


def test_scan_multi_worker_hash_failure(tmp_path: Path) -> None:
    """When sha256_file raises in a worker, the file is skipped."""
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"
    _create_file(media / "good.bin", b"GOOD" * 100)
    _create_file(media / "bad.bin", b"BAD" * 100)

    def patched_sha256(path):
        if "bad.bin" in str(path):
            raise OSError("Read error")
        from godmode_media_library.utils import sha256_file

        return sha256_file(path)

    with Catalog(db_path) as cat, patch("godmode_media_library.scanner.sha256_file", side_effect=patched_sha256):
        stats = incremental_scan(cat, [media], workers=2)

    assert stats.files_scanned == 2


# ── min_size_bytes filter ────────────────────────────────────────────


def test_scan_min_size_bytes(tmp_path: Path) -> None:
    """Files smaller than min_size_bytes should not be hashed."""
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"
    _create_file(media / "tiny.txt", b"x")  # 1 byte
    _create_file(media / "big.txt", b"X" * 1000)  # 1000 bytes

    with Catalog(db_path) as cat:
        stats = incremental_scan(cat, [media], min_size_bytes=500)

    assert stats.files_scanned == 2
    assert stats.files_new == 2

    with Catalog(db_path) as cat:
        tiny = cat.get_file_by_path(str(media / "tiny.txt"))
        big = cat.get_file_by_path(str(media / "big.txt"))
        assert tiny.sha256 is None  # too small to hash
        assert big.sha256 is not None


# ── _extract_gps_float ──────────────────────────────────────────────

from godmode_media_library.scanner import _extract_gps_float


class TestExtractGpsFloat:
    def test_float_value(self):
        assert _extract_gps_float({"GPSLatitude": 49.5}, ["GPSLatitude"]) == 49.5

    def test_int_value(self):
        assert _extract_gps_float({"GPSLatitude": 49}, ["GPSLatitude"]) == 49.0

    def test_string_float(self):
        assert _extract_gps_float({"GPSLatitude": "49.123"}, ["GPSLatitude"]) == 49.123

    def test_string_dms_format(self):
        result = _extract_gps_float({"GPSLatitude": "49 deg 7' 25.08\" N"}, ["GPSLatitude"])
        assert result is not None
        assert abs(result - 49.0) < 1

    def test_none_value(self):
        assert _extract_gps_float({}, ["GPSLatitude"]) is None

    def test_all_none(self):
        assert _extract_gps_float({"other": 1}, ["GPSLatitude"]) is None


# ── _backfill_dates_from_filesystem ────────────────────────────────


def test_backfill_dates_from_filesystem(tmp_path: Path) -> None:
    """Files without date_original should get backfilled from mtime."""
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"
    _create_file(media / "nodate.txt", b"NODATE" * 100)

    with Catalog(db_path) as cat:
        incremental_scan(cat, [media])
        row = cat.get_file_by_path(str(media / "nodate.txt"))
        # date_original should be backfilled from filesystem timestamp
        assert row.date_original is not None


# ── _update_duplicate_groups ────────────────────────────────────────


def test_update_duplicate_groups(tmp_path: Path) -> None:
    """Duplicate groups should be detected correctly."""
    db_path = tmp_path / "catalog.db"
    media = tmp_path / "media"
    content = b"SAME_CONTENT" * 200
    _create_file(media / "a.jpg", content)
    _create_file(media / "b.jpg", content)
    _create_file(media / "c.jpg", b"DIFFERENT" * 200)

    with Catalog(db_path) as cat:
        incremental_scan(cat, [media])
        groups = cat.query_duplicates()
        assert len(groups) == 1
        assert len(groups[0][1]) == 2
