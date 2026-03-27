"""Unit tests for bitrot.py — hash computation and bitrot detection."""

import hashlib
import sqlite3
from unittest.mock import MagicMock

import pytest

from godmode_media_library.bitrot import (
    CHUNK_SIZE,
    BitrotResult,
    _sha256_file,
    get_verification_stats,
    scan_bitrot,
)


# ── _sha256_file ─────────────────────────────────────────────────────


class TestSha256File:
    def test_hash_small_file(self, tmp_path):
        f = tmp_path / "hello.txt"
        f.write_text("hello world")
        result = _sha256_file(str(f))
        expected = hashlib.sha256(b"hello world").hexdigest()
        assert result == expected

    def test_hash_empty_file(self, tmp_path):
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")
        result = _sha256_file(str(f))
        expected = hashlib.sha256(b"").hexdigest()
        assert result == expected

    def test_hash_binary_file(self, tmp_path):
        data = bytes(range(256)) * 100
        f = tmp_path / "binary.bin"
        f.write_bytes(data)
        result = _sha256_file(str(f))
        expected = hashlib.sha256(data).hexdigest()
        assert result == expected

    def test_hash_large_file_multi_chunk(self, tmp_path):
        """File larger than CHUNK_SIZE to test chunked reading."""
        data = b"x" * (CHUNK_SIZE * 3 + 42)
        f = tmp_path / "large.bin"
        f.write_bytes(data)
        result = _sha256_file(str(f))
        expected = hashlib.sha256(data).hexdigest()
        assert result == expected

    def test_nonexistent_file_returns_none(self):
        result = _sha256_file("/tmp/nonexistent_file_abc123.bin")
        assert result is None

    def test_unreadable_file_returns_none(self, tmp_path):
        """If file cannot be opened, returns None."""
        # Passing a directory path triggers OSError
        result = _sha256_file(str(tmp_path))
        assert result is None


# ── BitrotResult ─────────────────────────────────────────────────────


class TestBitrotResult:
    def test_defaults(self):
        r = BitrotResult()
        assert r.total_checked == 0
        assert r.healthy == 0
        assert r.corrupted == 0
        assert r.missing == 0
        assert r.corrupted_files == []
        assert r.missing_files == []


# ── scan_bitrot (with real SQLite) ───────────────────────────────────


def _make_catalog(tmp_path, files=None):
    """Create a minimal catalog mock with a real SQLite connection."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL,
            sha256 TEXT,
            size INTEGER,
            last_verified TEXT,
            verify_count INTEGER DEFAULT 0
        )
    """)
    if files:
        for f in files:
            conn.execute(
                "INSERT INTO files (id, path, sha256, size) VALUES (?, ?, ?, ?)",
                f,
            )
    conn.commit()

    catalog = MagicMock()
    catalog.conn = conn
    return catalog


class TestScanBitrot:
    def test_healthy_file(self, tmp_path):
        """File whose hash matches stored hash is reported healthy."""
        f = tmp_path / "good.txt"
        f.write_text("good content")
        expected_hash = hashlib.sha256(b"good content").hexdigest()

        catalog = _make_catalog(tmp_path, [(1, str(f), expected_hash, f.stat().st_size)])
        result = scan_bitrot(catalog, limit=10)

        assert result.total_checked == 1
        assert result.healthy == 1
        assert result.corrupted == 0

    def test_corrupted_file(self, tmp_path):
        """File whose content changed since cataloging is detected as corrupted."""
        f = tmp_path / "bad.txt"
        f.write_text("original content")
        stored_hash = hashlib.sha256(b"original content").hexdigest()

        # Now corrupt the file
        f.write_text("corrupted content!")

        catalog = _make_catalog(tmp_path, [(1, str(f), stored_hash, 100)])
        result = scan_bitrot(catalog)

        assert result.corrupted == 1
        assert len(result.corrupted_files) == 1
        assert result.corrupted_files[0]["stored_hash"] == stored_hash

    def test_missing_file(self, tmp_path):
        """File that no longer exists on disk is reported as missing."""
        catalog = _make_catalog(
            tmp_path,
            [(1, "/nonexistent/path/file.jpg", "abc123", 1000)],
        )
        result = scan_bitrot(catalog)

        assert result.missing == 1
        assert len(result.missing_files) == 1

    def test_empty_catalog(self, tmp_path):
        """No files to scan yields zero results."""
        catalog = _make_catalog(tmp_path, [])
        result = scan_bitrot(catalog)

        assert result.total_checked == 0
        assert result.healthy == 0

    def test_limit_parameter(self, tmp_path):
        """Limit restricts how many files are checked."""
        files = []
        for i in range(5):
            f = tmp_path / f"file_{i}.txt"
            content = f"content {i}"
            f.write_text(content)
            h = hashlib.sha256(content.encode()).hexdigest()
            files.append((i + 1, str(f), h, len(content)))

        catalog = _make_catalog(tmp_path, files)
        result = scan_bitrot(catalog, limit=2)

        assert result.total_checked == 2

    def test_progress_fn_called(self, tmp_path):
        """Progress callback is invoked during scan."""
        f = tmp_path / "test.txt"
        f.write_text("test")
        h = hashlib.sha256(b"test").hexdigest()

        catalog = _make_catalog(tmp_path, [(1, str(f), h, 4)])
        progress_calls = []
        result = scan_bitrot(catalog, progress_fn=lambda p: progress_calls.append(p))

        # Should get at least the initial call and the completion call
        assert len(progress_calls) >= 1
        # Last call should be "complete"
        assert progress_calls[-1]["phase"] == "complete"

    def test_bytes_verified_tracked(self, tmp_path):
        """bytes_verified accumulates file sizes."""
        f = tmp_path / "data.bin"
        data = b"x" * 1000
        f.write_bytes(data)
        h = hashlib.sha256(data).hexdigest()

        catalog = _make_catalog(tmp_path, [(1, str(f), h, 1000)])
        result = scan_bitrot(catalog)

        assert result.bytes_verified == 1000


# ── get_verification_stats ───────────────────────────────────────────


class TestGetVerificationStats:
    def test_basic_stats(self, tmp_path):
        catalog = _make_catalog(tmp_path, [
            (1, "/a.jpg", "hash1", 100),
            (2, "/b.jpg", "hash2", 200),
        ])
        # Mark one as verified
        catalog.conn.execute(
            "UPDATE files SET last_verified = '2023-01-01T00:00:00', verify_count = 1 WHERE id = 1"
        )
        catalog.conn.commit()

        stats = get_verification_stats(catalog)
        assert stats["total_files"] == 2
        assert stats["verified"] == 1
        assert stats["never_verified"] == 1
        assert stats["verification_pct"] == 50.0

    def test_empty_catalog_stats(self, tmp_path):
        catalog = _make_catalog(tmp_path, [])
        stats = get_verification_stats(catalog)
        assert stats["total_files"] == 0
        assert stats["verification_pct"] == 0.0
