"""Tests for the SQLite-backed Catalog."""

from __future__ import annotations

from pathlib import Path

import pytest

from godmode_media_library.catalog import Catalog, CatalogFileRow, ScanStats, default_catalog_path


def _make_row(path: str = "/tmp/test/photo.jpg", **overrides) -> CatalogFileRow:
    """Helper to build a CatalogFileRow with sensible defaults."""
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
        first_seen="20240101_000000",
        last_scanned="20240101_000000",
    )
    defaults.update(overrides)
    return CatalogFileRow(**defaults)


# ── Basic lifecycle ──────────────────────────────────────────────────


def test_open_creates_db(tmp_path: Path) -> None:
    db_path = tmp_path / "sub" / "catalog.db"
    cat = Catalog(db_path)
    cat.open()
    try:
        assert db_path.exists()
    finally:
        cat.close()


def test_context_manager(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    with Catalog(db_path) as cat:
        assert cat.conn is not None
        assert db_path.exists()
    # After exiting the context, conn should be None
    assert cat._conn is None


def test_catalog_not_open_raises(tmp_path: Path) -> None:
    cat = Catalog(tmp_path / "catalog.db")
    with pytest.raises(RuntimeError, match="not open"):
        _ = cat.conn


# ── Upsert & get ─────────────────────────────────────────────────────


def test_upsert_file_new(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        row = _make_row()
        file_id = cat.upsert_file(row)
        cat.commit()
        assert file_id > 0


def test_upsert_file_update(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        row = _make_row()
        id1 = cat.upsert_file(row)
        cat.commit()

        # Read back first_seen
        original = cat.get_file_by_path(row.path)
        assert original is not None
        original_first_seen = original.first_seen

        # Upsert with changed size
        row2 = _make_row(size=2048)
        id2 = cat.upsert_file(row2)
        cat.commit()

        assert id1 == id2  # same row updated

        updated = cat.get_file_by_path(row.path)
        assert updated is not None
        assert updated.size == 2048
        assert updated.first_seen == original_first_seen  # preserved


def test_get_file_by_path(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        row = _make_row(path="/media/photo.jpg", size=4096)
        cat.upsert_file(row)
        cat.commit()

        result = cat.get_file_by_path("/media/photo.jpg")
        assert result is not None
        assert result.path == "/media/photo.jpg"
        assert result.size == 4096
        assert result.ext == "jpg"


def test_get_file_by_path_missing(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        assert cat.get_file_by_path("/nonexistent/file.jpg") is None


def test_get_file_mtime_size(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        row = _make_row(mtime=1700001000.0, size=512)
        cat.upsert_file(row)
        cat.commit()

        result = cat.get_file_mtime_size(row.path)
        assert result is not None
        assert result == (1700001000.0, 512)


def test_get_file_mtime_size_missing(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        assert cat.get_file_mtime_size("/no/such/file") is None


# ── Mark removed ─────────────────────────────────────────────────────


def test_mark_removed(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        p1, p2, p3 = "/a.jpg", "/b.jpg", "/c.jpg"
        for p in (p1, p2, p3):
            cat.upsert_file(_make_row(path=p))
        cat.commit()

        removed = cat.mark_removed([p1, p3])
        cat.commit()
        assert removed == 2
        assert cat.get_file_by_path(p1) is None
        assert cat.get_file_by_path(p2) is not None
        assert cat.get_file_by_path(p3) is None


def test_mark_removed_empty(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        assert cat.mark_removed([]) == 0


# ── Scan tracking ────────────────────────────────────────────────────


def test_start_finish_scan(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        scan_id = cat.start_scan("/media")
        assert scan_id > 0

        stats = ScanStats(root="/media", files_scanned=10, files_new=5, files_changed=2, files_removed=1)
        cat.finish_scan(scan_id, stats)

        cur = cat.conn.execute("SELECT * FROM scans WHERE id = ?", (scan_id,))
        row = cur.fetchone()
        assert row is not None
        assert row[3] is not None  # finished_at
        assert row[4] == 10  # files_scanned
        assert row[5] == 5  # files_new


# ── Duplicate tracking ───────────────────────────────────────────────


def test_upsert_duplicate_group(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        id1 = cat.upsert_file(_make_row(path="/dup1.jpg"))
        id2 = cat.upsert_file(_make_row(path="/dup2.jpg"))
        cat.commit()

        cat.upsert_duplicate_group("sha_group_1", [id1, id2], primary_id=id1)
        cat.commit()

        cur = cat.conn.execute(
            "SELECT file_id, is_primary FROM duplicates WHERE group_id = ? ORDER BY file_id",
            ("sha_group_1",),
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0] == (id1, 1)  # primary
        assert rows[1] == (id2, 0)


# ── Labels ───────────────────────────────────────────────────────────


def test_upsert_label(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        file_id = cat.upsert_file(_make_row())
        cat.commit()

        # Insert label
        cat.upsert_label(file_id, people="Alice", place="Prague")
        cat.commit()
        cur = cat.conn.execute("SELECT people, place FROM labels WHERE file_id = ?", (file_id,))
        row = cur.fetchone()
        assert row == ("Alice", "Prague")

        # Update label
        cat.upsert_label(file_id, people="Bob", place="Brno")
        cat.commit()
        cur = cat.conn.execute("SELECT people, place FROM labels WHERE file_id = ?", (file_id,))
        row = cur.fetchone()
        assert row == ("Bob", "Brno")


# ── Query operations ─────────────────────────────────────────────────


def _populate_catalog(cat: Catalog) -> None:
    """Insert several files for query testing."""
    cat.upsert_file(_make_row(path="/photos/a.jpg", ext="jpg", size=1000))
    cat.upsert_file(_make_row(path="/photos/b.png", ext="png", size=5000))
    cat.upsert_file(_make_row(path="/docs/c.pdf", ext="pdf", size=200))
    cat.upsert_file(_make_row(path="/photos/sub/d.jpg", ext="jpg", size=8000))
    cat.commit()


def test_query_files_by_ext(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        _populate_catalog(cat)
        results = cat.query_files(ext="jpg")
        assert len(results) == 2
        assert all(r.ext == "jpg" for r in results)


def test_query_files_by_size(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        _populate_catalog(cat)
        results = cat.query_files(min_size=1000, max_size=5000)
        assert all(1000 <= r.size <= 5000 for r in results)
        assert len(results) == 2  # a.jpg (1000) and b.png (5000)


def test_query_files_by_path_contains(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        _populate_catalog(cat)
        results = cat.query_files(path_contains="/photos/")
        assert len(results) == 3  # a.jpg, b.png, sub/d.jpg


def test_query_files_no_filter(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        _populate_catalog(cat)
        results = cat.query_files()
        assert len(results) == 4


def test_query_duplicates(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        id1 = cat.upsert_file(_make_row(path="/x.jpg"))
        id2 = cat.upsert_file(_make_row(path="/y.jpg"))
        cat.upsert_duplicate_group("grp1", [id1, id2], primary_id=id1)
        cat.commit()

        groups = cat.query_duplicates()
        assert len(groups) == 1
        group_id, files = groups[0]
        assert group_id == "grp1"
        assert len(files) == 2


# ── Stats ────────────────────────────────────────────────────────────


def test_stats(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        _populate_catalog(cat)
        s = cat.stats()
        assert s["total_files"] == 4
        assert s["total_size_bytes"] == 1000 + 5000 + 200 + 8000
        assert s["duplicate_groups"] == 0
        assert isinstance(s["top_extensions"], list)


# ── All paths ────────────────────────────────────────────────────────


def test_all_paths(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.db") as cat:
        _populate_catalog(cat)
        paths = cat.all_paths()
        assert isinstance(paths, set)
        assert len(paths) == 4
        assert "/photos/a.jpg" in paths


# ── Export / Import roundtrip ────────────────────────────────────────


def test_export_import_roundtrip(tmp_path: Path) -> None:
    tsv_path = tmp_path / "export.tsv"

    with Catalog(tmp_path / "cat1.db") as cat1:
        cat1.upsert_file(_make_row(path="/photo/a.jpg", size=1111, ext="jpg"))
        cat1.upsert_file(_make_row(path="/photo/b.png", size=2222, ext="png"))
        cat1.commit()

        count_exported = cat1.export_inventory_tsv(tsv_path)
        assert count_exported == 2

    with Catalog(tmp_path / "cat2.db") as cat2:
        count_imported = cat2.import_from_inventory_tsv(tsv_path)
        assert count_imported == 2

        a = cat2.get_file_by_path("/photo/a.jpg")
        b = cat2.get_file_by_path("/photo/b.png")
        assert a is not None and a.size == 1111
        assert b is not None and b.size == 2222


# ── Default path ─────────────────────────────────────────────────────


def test_default_catalog_path() -> None:
    p = default_catalog_path()
    assert p.name == "catalog.db"
    assert str(p).endswith(".config/gml/catalog.db")


# ── Exclusive locking ────────────────────────────────────────────────


def test_exclusive_lock_prevents_second_open(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    cat1 = Catalog(db_path, exclusive=True)
    cat1.open(exclusive=True)
    try:
        cat2 = Catalog(db_path, exclusive=True)
        with pytest.raises(RuntimeError, match="Another process"):
            cat2.open(exclusive=True)
    finally:
        cat1.close()


def test_exclusive_lock_released_on_close(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    cat1 = Catalog(db_path, exclusive=True)
    cat1.open(exclusive=True)
    cat1.close()

    # Should succeed now that lock is released
    cat2 = Catalog(db_path, exclusive=True)
    cat2.open(exclusive=True)
    cat2.close()


def test_non_exclusive_open_works_concurrently(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    cat1 = Catalog(db_path)
    cat1.open()
    try:
        cat2 = Catalog(db_path)
        cat2.open()
        cat2.close()
    finally:
        cat1.close()
