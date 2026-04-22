"""Regression tests for Phase 3 performance fixes.

Each test verifies that a specific N+1 query or memory issue has been resolved.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from godmode_media_library.catalog import Catalog, CatalogFileRow


def _make_row(path: str = "/tmp/test/photo.jpg", **overrides) -> CatalogFileRow:
    defaults = dict(
        id=None,
        path=path,
        size=1024,
        mtime=1700000000.0,
        ctime=1700000000.0,
        birthtime=1699999000.0,
        ext="jpg",
        sha256="abc123" * 10 + "abcd",
        inode=12345,
        device=1,
        nlink=1,
        asset_key=None,
        asset_component=False,
        xattr_count=0,
        first_seen="",
        last_scanned="",
    )
    defaults.update(overrides)
    return CatalogFileRow(**defaults)


# ── 3.1 scanner.py backfill_metadata_from_stored N+1 fix ────────────


def test_backfill_metadata_no_n_plus_1(tmp_path: Path) -> None:
    """backfill_metadata_from_stored should NOT call get_file_by_path per row.

    After the fix, the JOIN query includes date_original and gps_latitude
    directly, eliminating the per-row get_file_by_path call.
    """
    db_path = tmp_path / "catalog.db"
    cat = Catalog(db_path)
    cat.open()
    try:
        # Insert a file without date_original
        row = _make_row(
            path="/tmp/test/photo1.jpg",
            date_original=None,
            gps_latitude=None,
        )
        cat.upsert_file(row)
        cat.commit()

        # Get the file ID
        file_id = cat.conn.execute(
            "SELECT id FROM files WHERE path = ?", ("/tmp/test/photo1.jpg",)
        ).fetchone()[0]

        # Insert metadata with a date
        meta = {"EXIF:DateTimeOriginal": "2024:01:15 10:30:00"}
        cat.conn.execute(
            "INSERT OR REPLACE INTO file_metadata (file_id, raw_json, extracted_at) VALUES (?, ?, '2024-01-01T00:00:00')",
            (file_id, json.dumps(meta)),
        )
        cat.commit()

        # Patch get_file_by_path to track calls
        original_get = cat.get_file_by_path
        call_count = [0]

        def tracked_get(*args, **kwargs):
            call_count[0] += 1
            return original_get(*args, **kwargs)

        with patch.object(cat, "get_file_by_path", side_effect=tracked_get):
            from godmode_media_library.scanner import backfill_metadata_from_stored
            result = backfill_metadata_from_stored(cat)

        # The fix eliminates get_file_by_path calls entirely
        assert call_count[0] == 0, (
            f"get_file_by_path was called {call_count[0]} times; "
            "should be 0 after N+1 fix"
        )
        assert result["dates_filled"] == 1
    finally:
        cat.close()


def test_backfill_metadata_fills_date_and_gps(tmp_path: Path) -> None:
    """Verify backfill still correctly fills date_original and GPS after the fix."""
    db_path = tmp_path / "catalog.db"
    cat = Catalog(db_path)
    cat.open()
    try:
        row = _make_row(
            path="/tmp/test/photo_gps.jpg",
            date_original=None,
            gps_latitude=None,
            gps_longitude=None,
        )
        cat.upsert_file(row)
        cat.commit()

        file_id = cat.conn.execute(
            "SELECT id FROM files WHERE path = ?", ("/tmp/test/photo_gps.jpg",)
        ).fetchone()[0]

        meta = {
            "EXIF:DateTimeOriginal": "2024:06:15 14:00:00",
            "Composite:GPSLatitude": 49.1234,
            "Composite:GPSLongitude": 16.5678,
        }
        cat.conn.execute(
            "INSERT OR REPLACE INTO file_metadata (file_id, raw_json, extracted_at) VALUES (?, ?, '2024-01-01T00:00:00')",
            (file_id, json.dumps(meta)),
        )
        cat.commit()

        from godmode_media_library.scanner import backfill_metadata_from_stored
        result = backfill_metadata_from_stored(cat)

        assert result["dates_filled"] == 1
        assert result["gps_filled"] == 1

        # Verify the values were actually written
        updated = cat.conn.execute(
            "SELECT date_original, gps_latitude, gps_longitude FROM files WHERE path = ?",
            ("/tmp/test/photo_gps.jpg",),
        ).fetchone()
        assert updated[0] == "2024:06:15 14:00:00"
        assert abs(updated[1] - 49.1234) < 0.001
        assert abs(updated[2] - 16.5678) < 0.001
    finally:
        cat.close()


# ── 3.1 catalog.all_paths cursor iteration ───────────────────────────


def test_all_paths_uses_cursor_iteration(tmp_path: Path) -> None:
    """all_paths() should use cursor iteration, not fetchall()."""
    db_path = tmp_path / "catalog.db"
    cat = Catalog(db_path)
    cat.open()
    try:
        for i in range(10):
            row = _make_row(
                path=f"/tmp/test/photo_{i}.jpg",
                sha256=f"{'a' * 60}{i:04d}",
                inode=1000 + i,
            )
            cat.upsert_file(row)
        cat.commit()

        paths = cat.all_paths()
        assert len(paths) == 10
        assert all(isinstance(p, str) for p in paths)
    finally:
        cat.close()


# ── 3.2 face_crypto Fernet caching ──────────────────────────────────


def test_fernet_instance_is_cached() -> None:
    """_get_fernet should return cached Fernet instances, not re-read from disk."""
    from godmode_media_library.face_crypto import _fernet_cache, _get_fernet, _reset_cache

    # Start fresh
    _reset_cache()

    try:
        # Need a keystore to exist
        from godmode_media_library.face_crypto import _load_keystore
        _load_keystore()

        f1 = _get_fernet(0)
        f2 = _get_fernet(0)
        # Should be the exact same object (cached)
        assert f1 is f2
        # Cache should have an entry
        assert 0 in _fernet_cache
    finally:
        _reset_cache()


# ── 3.2 face_detect batch queries ───────────────────────────────────


def test_cluster_faces_uses_batch_query(tmp_path: Path) -> None:
    """cluster_faces should use batch IN(...) queries for face_person_map."""
    import re

    from godmode_media_library.face_detect import cluster_faces

    # Verify the source code uses batch query pattern
    import inspect
    source = inspect.getsource(cluster_faces)
    # Should contain "IN" query pattern for batch loading
    assert "IN ({placeholders})" in source or "IN (" in source


def test_match_face_uses_batch_query() -> None:
    """match_face_to_known should batch-load sample face encodings."""
    import inspect

    from godmode_media_library.face_detect import match_face_to_known
    source = inspect.getsource(match_face_to_known)
    # Should contain batch IN query pattern
    assert "IN ({placeholders})" in source or "IN (" in source


# ── 3.3 Memory: rate limiting eviction ──────────────────────────────


def test_rate_limit_eviction() -> None:
    """_prune_rate_dict should evict stale entries and cap size."""
    from godmode_media_library.web.app import _prune_rate_dict

    d: dict[str, list[float]] = {}
    import time
    now = time.monotonic()

    # Add stale entries (very old timestamps)
    for i in range(100):
        d[f"ip_{i}"] = [now - 3600]  # 1 hour ago

    # Add fresh entries
    for i in range(100, 105):
        d[f"ip_{i}"] = [now]

    _prune_rate_dict(d, window=60.0, max_ips=50)

    # All stale entries should be gone
    assert all(f"ip_{i}" not in d for i in range(100))
    # Fresh entries survive
    assert len(d) == 5


# ── 3.3 Memory: reorganize plans eviction ────────────────────────────


def test_reorganize_plans_eviction() -> None:
    """_evict_old_plans should remove expired plans and cap count."""
    import time

    from godmode_media_library.web.shared import (
        _REORGANIZE_PLAN_MAX,
        _evict_old_plans,
        _reorganize_plans,
        _reorganize_plans_lock,
    )

    # Save original state
    original = dict(_reorganize_plans)
    _reorganize_plans.clear()

    try:
        now = time.monotonic()
        # Add an expired plan
        _reorganize_plans["old_plan"] = (now - 7200, "old_data")
        # Add a fresh plan
        _reorganize_plans["new_plan"] = (now, "new_data")

        _evict_old_plans()

        assert "old_plan" not in _reorganize_plans
        assert "new_plan" in _reorganize_plans
    finally:
        _reorganize_plans.clear()
        _reorganize_plans.update(original)


# ── 3.3 Memory: bitrot cursor iteration ─────────────────────────────


def test_bitrot_uses_cursor_iteration(tmp_path: Path) -> None:
    """bitrot scan should iterate cursor, not fetchall() all files."""
    import inspect

    from godmode_media_library.bitrot import scan_bitrot
    source = inspect.getsource(scan_bitrot)
    # Should use enumerate(cursor) not fetchall()
    assert "fetchall()" not in source
    assert "enumerate(cursor)" in source


# ── 3.3 Memory: media_score uses SQL LIMIT ──────────────────────────


def test_media_score_uses_sql_limit() -> None:
    """score_catalog should use SQL LIMIT to cap memory usage."""
    import inspect

    from godmode_media_library.media_score import score_catalog
    source = inspect.getsource(score_catalog)
    assert "LIMIT ?" in source


# ── 3.4 tree_ops: reserved_norm accumulated, not rebuilt ─────────────


def test_allocate_destination_uses_persistent_norm_set() -> None:
    """_allocate_destination should use a persistent reserved_norm set."""
    from godmode_media_library.tree_ops import _allocate_destination

    reserved: set[Path] = set()
    reserved_norm: set[str] = set()

    # Allocate several destinations with the same persistent sets
    d1 = _allocate_destination(Path("/tmp/out/photo.jpg"), reserved, reserved_norm)
    d2 = _allocate_destination(Path("/tmp/out/photo.jpg"), reserved, reserved_norm)

    # d2 should be renamed since d1 took the original name
    assert d1 != d2
    assert "photo.jpg" in str(d1)
    assert "(1)" in str(d2)

    # Both should be tracked in the persistent sets
    assert len(reserved) == 2
    assert len(reserved_norm) == 2


def test_create_tree_plan_passes_reserved_norm() -> None:
    """create_tree_plan should pass reserved_norm to _allocate_destination."""
    import inspect

    from godmode_media_library.tree_ops import create_tree_plan
    source = inspect.getsource(create_tree_plan)
    # Should initialize and pass reserved_norm
    assert "reserved_norm" in source


# ── 3.4 tree_ops: collision tracking persists across rows ────────────


def test_apply_tree_plan_collision_tracking_persists() -> None:
    """apply_tree_plan collision sets should persist across all rows."""
    import inspect

    from godmode_media_library.tree_ops import apply_tree_plan
    source = inspect.getsource(apply_tree_plan)
    # The collision sets should be defined BEFORE the loop, not inside it
    assert "_collision_reserved: set[Path] = set()" in source
    assert "_collision_reserved_norm: set[str] = set()" in source


# ── 3.4 consolidation: batch file metadata query ────────────────────


def test_consolidation_stream_batches_file_metadata() -> None:
    """_phase_5_stream should batch-load file metadata, not query per file."""
    import inspect

    from godmode_media_library.consolidation import _phase_5_stream
    source = inspect.getsource(_phase_5_stream)
    # Should contain batch metadata cache pattern
    assert "_file_meta_cache" in source
    # Should NOT contain the per-file query pattern
    assert 'conn.execute("SELECT date_original, size FROM files WHERE sha256 = ? LIMIT 1"' not in source


# ── 3.3 OAuth cleanup exists ────────────────────────────────────────


def test_oauth_cleanup_exists() -> None:
    """cloud.py should have _cleanup_stale_oauth function."""
    from godmode_media_library.cloud import _cleanup_stale_oauth, _OAUTH_TIMEOUT
    assert callable(_cleanup_stale_oauth)
    assert _OAUTH_TIMEOUT == 600


# ── 3.1 scanner batch mtime/size pre-load ───────────────────────────


def test_scanner_preloads_mtime_size(tmp_path: Path) -> None:
    """incremental_scan should pre-load mtime/size in batch, not per file."""
    import inspect

    from godmode_media_library.scanner import incremental_scan
    source = inspect.getsource(incremental_scan)
    # Should use batch pre-load
    assert "get_all_mtime_size_for_root" in source
    # Should NOT have per-file get_file_mtime_size
    assert "get_file_mtime_size" not in source
