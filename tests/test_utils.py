from __future__ import annotations

import re
from pathlib import Path

from godmode_media_library.utils import (
    ensure_dir,
    iter_files,
    meaningful_xattr_count,
    path_startswith,
    read_tsv_dict,
    safe_stat_birthtime,
    sha256_file,
    utc_stamp,
    write_tsv,
)


def test_utc_stamp_format():
    stamp = utc_stamp()
    assert re.match(r"\d{8}_\d{6}$", stamp), f"Unexpected format: {stamp}"


def test_ensure_dir_creates_nested(tmp_path: Path):
    nested = tmp_path / "a" / "b" / "c"
    assert not nested.exists()
    ensure_dir(nested)
    assert nested.is_dir()


def test_sha256_file_deterministic(tmp_path: Path):
    f = tmp_path / "same.bin"
    f.write_bytes(b"hello world")
    h1 = sha256_file(f)
    h2 = sha256_file(f)
    assert h1 == h2
    assert len(h1) == 64  # hex digest length


def test_sha256_file_different_content(tmp_path: Path):
    f1 = tmp_path / "a.bin"
    f2 = tmp_path / "b.bin"
    f1.write_bytes(b"content A")
    f2.write_bytes(b"content B")
    assert sha256_file(f1) != sha256_file(f2)


def test_iter_files_skips_missing_root():
    results = list(iter_files([Path("/nonexistent_path_12345")]))
    assert results == []


def test_iter_files_finds_files(tmp_path: Path):
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "file.txt").write_text("hello")
    (tmp_path / "root.txt").write_text("hi")
    found = list(iter_files([tmp_path]))
    names = {p.name for p in found}
    assert "file.txt" in names
    assert "root.txt" in names


def test_iter_files_skips_dirs(tmp_path: Path):
    sub = tmp_path / "subdir"
    sub.mkdir()
    (sub / "file.txt").write_text("x")
    found = list(iter_files([tmp_path]))
    for p in found:
        assert p.is_file()


def test_safe_stat_birthtime(tmp_path: Path):
    f = tmp_path / "test.txt"
    f.write_text("data")
    bt = safe_stat_birthtime(f)
    # On macOS st_birthtime is available, so we expect a float
    assert bt is None or isinstance(bt, float)


def test_meaningful_xattr_count(tmp_path: Path):
    f = tmp_path / "test.txt"
    f.write_text("data")
    count = meaningful_xattr_count(f)
    assert isinstance(count, int)
    assert count >= 0


def test_write_read_tsv_roundtrip(tmp_path: Path):
    tsv_path = tmp_path / "test.tsv"
    header = ["name", "value", "count"]
    rows = [
        ("alpha", "100", "1"),
        ("beta", "200", "2"),
    ]
    write_tsv(tsv_path, header, rows)

    loaded = read_tsv_dict(tsv_path)
    assert len(loaded) == 2
    assert loaded[0]["name"] == "alpha"
    assert loaded[0]["value"] == "100"
    assert loaded[1]["name"] == "beta"
    assert loaded[1]["count"] == "2"


def test_path_startswith_match():
    p = Path("/Users/test/photos/img.jpg")
    prefixes = ("/Users/other", "/Users/test/photos")
    idx = path_startswith(p, prefixes)
    assert idx == 1


def test_path_startswith_no_match():
    p = Path("/Users/test/photos/img.jpg")
    prefixes = ("/Users/other", "/var/data")
    idx = path_startswith(p, prefixes)
    assert idx is None


def test_path_startswith_first_match():
    p = Path("/Users/test/photos/img.jpg")
    prefixes = ("/Users/test", "/Users/test/photos")
    idx = path_startswith(p, prefixes)
    assert idx == 0


def test_iter_files_multiple_roots(tmp_path: Path):
    r1 = tmp_path / "root1"
    r2 = tmp_path / "root2"
    r1.mkdir()
    r2.mkdir()
    (r1 / "a.txt").write_text("a")
    (r2 / "b.txt").write_text("b")
    found = list(iter_files([r1, r2]))
    names = {p.name for p in found}
    assert names == {"a.txt", "b.txt"}


def test_ensure_dir_existing(tmp_path: Path):
    d = tmp_path / "exists"
    d.mkdir()
    ensure_dir(d)  # should not raise
    assert d.is_dir()


def test_sha256_file_large_content(tmp_path: Path):
    f = tmp_path / "large.bin"
    # Write >1MB to test chunked reading
    f.write_bytes(b"x" * (1024 * 1024 + 100))
    h = sha256_file(f)
    assert len(h) == 64
