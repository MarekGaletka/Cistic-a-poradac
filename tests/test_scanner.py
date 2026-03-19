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
