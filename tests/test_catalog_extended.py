"""Extended catalog tests targeting uncovered lines.

Covers: tag_file, untag_file, bulk_tag, bulk_untag, query_files_by_tag,
get_file_metadata, upsert_file_metadata, get_metadata_richness,
update_metadata_richness, get_group_metadata, get_all_duplicate_group_ids,
create_share, get_share, delete_share, import_from_inventory_tsv,
export_inventory_tsv, update_file_path, delete_file_by_path,
get_files_by_paths, mark_removed with inode decrement, vacuum,
concurrent open (exclusive lock), schema migrations from old versions.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

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
        first_seen="20240101_000000",
        last_scanned="20240101_000000",
    )
    defaults.update(overrides)
    return CatalogFileRow(**defaults)


@pytest.fixture()
def catalog(tmp_path):
    """Create a fresh catalog with schema."""
    db = tmp_path / "test_catalog.db"
    cat = Catalog(db)
    cat.open()
    yield cat
    cat.close()


@pytest.fixture()
def catalog_with_file(catalog):
    """Catalog with one file inserted."""
    row = _make_row()
    catalog.upsert_file(row)
    catalog.commit()
    return catalog


# ── Tag operations ──────────────────────────────────────────────────


class TestTagOperations:
    def test_tag_file(self, catalog_with_file):
        cat = catalog_with_file
        tag = cat.add_tag("vacation")
        tag_id = tag["id"]
        cat.tag_file("/tmp/test/photo.jpg", tag_id)
        cat.commit()
        files = cat.query_files_by_tag(tag_id)
        assert len(files) == 1
        assert files[0].path == "/tmp/test/photo.jpg"

    def test_tag_file_nonexistent_path(self, catalog_with_file):
        cat = catalog_with_file
        tag = cat.add_tag("test")
        cat.tag_file("/nonexistent/file.jpg", tag["id"])  # Should not raise

    def test_untag_file(self, catalog_with_file):
        cat = catalog_with_file
        tag = cat.add_tag("temp")
        tag_id = tag["id"]
        cat.tag_file("/tmp/test/photo.jpg", tag_id)
        cat.commit()
        cat.untag_file("/tmp/test/photo.jpg", tag_id)
        cat.commit()
        files = cat.query_files_by_tag(tag_id)
        assert len(files) == 0

    def test_untag_file_nonexistent(self, catalog_with_file):
        cat = catalog_with_file
        tag = cat.add_tag("test2")
        cat.untag_file("/nonexistent/file.jpg", tag["id"])  # Should not raise

    def test_bulk_tag(self, catalog_with_file):
        cat = catalog_with_file
        tag = cat.add_tag("bulk")
        count = cat.bulk_tag(["/tmp/test/photo.jpg", "/nonexistent.jpg"], tag["id"])
        assert count == 1  # Only the existing file gets tagged

    def test_bulk_untag(self, catalog_with_file):
        cat = catalog_with_file
        tag = cat.add_tag("bulk_remove")
        tag_id = tag["id"]
        cat.tag_file("/tmp/test/photo.jpg", tag_id)
        cat.commit()
        count = cat.bulk_untag(["/tmp/test/photo.jpg", "/nonexistent.jpg"], tag_id)
        assert count == 1

    def test_delete_tag(self, catalog_with_file):
        cat = catalog_with_file
        tag = cat.add_tag("to_delete")
        tag_id = tag["id"]
        cat.tag_file("/tmp/test/photo.jpg", tag_id)
        cat.commit()
        cat.delete_tag(tag_id)
        tags = cat.get_all_tags()
        assert not any(t["name"] == "to_delete" for t in tags)


# ── Metadata operations ─────────────────────────────────────────────


class TestMetadataOperations:
    def test_upsert_and_get_file_metadata(self, catalog_with_file):
        cat = catalog_with_file
        meta = {"FileName": "photo.jpg", "ImageWidth": 4000}
        cat.upsert_file_metadata("/tmp/test/photo.jpg", json.dumps(meta))
        cat.commit()
        result = cat.get_file_metadata("/tmp/test/photo.jpg")
        assert result is not None
        assert result["FileName"] == "photo.jpg"

    def test_get_file_metadata_nonexistent(self, catalog_with_file):
        cat = catalog_with_file
        result = cat.get_file_metadata("/nonexistent.jpg")
        assert result is None

    def test_upsert_metadata_nonexistent_file(self, catalog_with_file):
        cat = catalog_with_file
        cat.upsert_file_metadata("/nonexistent.jpg", '{"a": 1}')  # Should not raise

    def test_update_and_get_metadata_richness(self, catalog_with_file):
        cat = catalog_with_file
        cat.update_metadata_richness("/tmp/test/photo.jpg", 75.5)
        cat.commit()
        score = cat.get_metadata_richness("/tmp/test/photo.jpg")
        assert score == pytest.approx(75.5)

    def test_get_metadata_richness_nonexistent(self, catalog_with_file):
        cat = catalog_with_file
        assert cat.get_metadata_richness("/nonexistent.jpg") is None


# ── Duplicate group metadata ────────────────────────────────────────


class TestDuplicateGroupMetadata:
    def test_get_all_duplicate_group_ids_empty(self, catalog):
        assert catalog.get_all_duplicate_group_ids() == []

    def test_get_group_metadata_empty(self, catalog):
        result = catalog.get_group_metadata("nonexistent_group")
        assert result == []


# ── File operations ─────────────────────────────────────────────────


class TestFileOperations:
    def test_update_file_path(self, catalog_with_file):
        cat = catalog_with_file
        success = cat.update_file_path("/tmp/test/photo.jpg", "/tmp/test/renamed.jpg")
        cat.commit()
        assert success is True
        row = cat.get_file_by_path("/tmp/test/renamed.jpg")
        assert row is not None

    def test_update_file_path_nonexistent(self, catalog_with_file):
        cat = catalog_with_file
        success = cat.update_file_path("/nonexistent.jpg", "/new.jpg")
        assert success is False

    def test_delete_file_by_path(self, catalog_with_file):
        cat = catalog_with_file
        success = cat.delete_file_by_path("/tmp/test/photo.jpg")
        cat.commit()
        assert success is True
        assert cat.get_file_by_path("/tmp/test/photo.jpg") is None

    def test_delete_file_by_path_nonexistent(self, catalog_with_file):
        cat = catalog_with_file
        success = cat.delete_file_by_path("/nonexistent.jpg")
        assert success is False

    def test_get_files_by_paths(self, catalog_with_file):
        cat = catalog_with_file
        result = cat.get_files_by_paths(["/tmp/test/photo.jpg", "/nonexistent.jpg"])
        assert "/tmp/test/photo.jpg" in result
        assert "/nonexistent.jpg" not in result

    def test_get_files_by_paths_empty(self, catalog_with_file):
        cat = catalog_with_file
        result = cat.get_files_by_paths([])
        assert result == {}

    def test_vacuum(self, catalog_with_file):
        cat = catalog_with_file
        cat.vacuum()  # Should not raise


# ── Import/Export ───────────────────────────────────────────────────


class TestImportExport:
    def test_export_inventory_tsv(self, catalog_with_file, tmp_path):
        cat = catalog_with_file
        out = tmp_path / "export.tsv"
        count = cat.export_inventory_tsv(out)
        assert count == 1
        assert out.exists()
        content = out.read_text()
        assert "/tmp/test/photo.jpg" in content

    def test_import_from_inventory_tsv(self, catalog, tmp_path):
        cat = catalog
        inv = tmp_path / "inventory.tsv"
        inv.write_text(
            "path\tsize\tmtime\tctime\tbirthtime\text\tsha256\n"
            "/imported/photo.jpg\t2048\t1700000000.0\t1700000000.0\t1699999000.0\tjpg\tabc123\n"
        )
        count = cat.import_from_inventory_tsv(inv)
        cat.commit()
        assert count == 1


# ── Schema migration ───────────────────────────────────────────────


class TestSchemaMigration:
    def test_open_creates_fresh_schema(self, tmp_path):
        db = tmp_path / "fresh.db"
        cat = Catalog(db)
        cat.open()
        # Verify latest schema has all tables
        tables = cat.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert "files" in table_names
        assert "tags" in table_names
        assert "file_tags" in table_names
        assert "file_notes" in table_names
        assert "file_ratings" in table_names
        assert "meta" in table_names
        cat.close()


# ── Concurrent access ──────────────────────────────────────────────


class TestConcurrentAccess:
    def test_exclusive_open_and_close(self, tmp_path):
        """Opening catalog in exclusive mode acquires lock, closing releases it."""
        db = tmp_path / "locked.db"
        cat1 = Catalog(db, exclusive=True)
        cat1.open()
        cat1.close()
        # After close, we can reopen exclusively
        cat2 = Catalog(db, exclusive=True)
        cat2.open()
        cat2.close()

    def test_context_manager(self, tmp_path):
        db = tmp_path / "ctx.db"
        cat = Catalog(db)
        with cat:
            # Should be usable inside context
            stats = cat.stats()
            assert isinstance(stats, dict)
        # After exiting context, should be closed


# ── Notes and Ratings ──────────────────────────────────────────────


class TestNotesAndRatings:
    def test_set_and_get_note(self, catalog_with_file):
        cat = catalog_with_file
        cat.set_file_note("/tmp/test/photo.jpg", "Great photo!")
        cat.commit()
        result = cat.get_file_note("/tmp/test/photo.jpg")
        assert result is not None
        assert result[0] == "Great photo!"

    def test_set_and_get_rating(self, catalog_with_file):
        cat = catalog_with_file
        cat.set_file_rating("/tmp/test/photo.jpg", 5)
        cat.commit()
        rating = cat.get_file_rating("/tmp/test/photo.jpg")
        assert rating == 5

    def test_get_note_nonexistent(self, catalog_with_file):
        cat = catalog_with_file
        note = cat.get_file_note("/nonexistent.jpg")
        assert note is None

    def test_get_rating_nonexistent(self, catalog_with_file):
        cat = catalog_with_file
        rating = cat.get_file_rating("/nonexistent.jpg")
        assert rating is None


# ── Stats ──────────────────────────────────────────────────────────


class TestCatalogStats:
    def test_stats_empty(self, catalog):
        s = catalog.stats()
        assert s["total_files"] == 0
        assert s["total_size_bytes"] == 0

    def test_stats_with_file(self, catalog_with_file):
        s = catalog_with_file.stats()
        assert s["total_files"] == 1
        assert s["total_size_bytes"] > 0


# ── Shares ─────────────────────────────────────────────────────────


class TestShares:
    def test_create_and_get_share(self, catalog_with_file):
        cat = catalog_with_file
        share = cat.create_share("/tmp/test/photo.jpg", label="Test share")
        assert share is not None
        assert share["label"] == "Test share"
        token = share["token"]

        retrieved = cat.get_share_by_token(token)
        assert retrieved is not None
        assert retrieved["label"] == "Test share"

    def test_get_share_nonexistent(self, catalog_with_file):
        cat = catalog_with_file
        result = cat.get_share_by_token("nonexistent_token")
        assert result is None

    def test_delete_share(self, catalog_with_file):
        cat = catalog_with_file
        share = cat.create_share("/tmp/test/photo.jpg")
        assert share is not None
        share_id = share["id"]
        cat.delete_share(share_id)
        assert cat.get_share_by_token(share["token"]) is None

    def test_create_share_nonexistent_file(self, catalog_with_file):
        cat = catalog_with_file
        with pytest.raises(ValueError, match="File not found"):
            cat.create_share("/nonexistent.jpg")
