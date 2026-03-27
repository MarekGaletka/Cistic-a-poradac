"""Performance / stress tests — run with pytest -m slow."""

from __future__ import annotations

import random
from pathlib import Path

import pytest

from godmode_media_library.catalog import Catalog
from godmode_media_library.perceptual_hash import find_similar
from godmode_media_library.scanner import incremental_scan


def _random_content(size: int = 512) -> bytes:
    return bytes(random.getrandbits(8) for _ in range(size))


def _random_hex(length: int = 16) -> str:
    return "".join(random.choices("0123456789abcdef", k=length))


# ── Scan performance ──────────────────────────────────────────────────


@pytest.mark.slow
def test_scan_1000_files(tmp_path: Path) -> None:
    """Scan 1000 fake files and verify the scanner completes without error."""
    media = tmp_path / "media"
    media.mkdir()

    extensions = ["jpg", "png", "heic", "mp4", "mov", "pdf", "doc"]

    for i in range(1000):
        ext = random.choice(extensions)
        name = f"file_{i:04d}.{ext}"
        (media / name).write_bytes(_random_content(random.randint(100, 2048)))

    db_path = tmp_path / "catalog.db"
    with Catalog(db_path) as cat:
        stats = incremental_scan(cat, [media])

    assert stats.files_scanned == 1000
    assert stats.files_new == 1000


# ── find_similar performance ──────────────────────────────────────────


@pytest.mark.slow
def test_find_similar_1000_hashes() -> None:
    """Create 1000 random hashes and run find_similar — basic performance sanity."""
    hashes: dict[str, str] = {}
    for i in range(1000):
        path = f"/fake/path/file_{i:04d}.jpg"
        hashes[path] = _random_hex(16)

    # Inject a few near-duplicate pairs to verify detection works
    base_hash = "abcdef0123456789"
    hashes["/fake/path/dup_a.jpg"] = base_hash
    # Flip one bit (distance = 1)
    val = int(base_hash, 16) ^ 1
    hashes["/fake/path/dup_b.jpg"] = f"{val:016x}"

    pairs = find_similar(hashes, threshold=10)

    # Should find at least the injected near-duplicate pair
    found_paths = set()
    for pair in pairs:
        found_paths.add(pair.path_a)
        found_paths.add(pair.path_b)

    assert "/fake/path/dup_a.jpg" in found_paths or "/fake/path/dup_b.jpg" in found_paths
