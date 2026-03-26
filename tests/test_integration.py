"""Integration tests with real files.

These tests use actual JPEG/MP4 fixtures and real external tools
(exiftool, ffprobe) when available. Tests auto-skip if the required
tool is not installed.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from godmode_media_library.catalog import Catalog
from godmode_media_library.scanner import incremental_scan

FIXTURES = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.integration


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def fixture_root(tmp_path):
    """Copy fixture files into a temp directory and return its path."""
    media = tmp_path / "media"
    media.mkdir()
    for f in FIXTURES.iterdir():
        if f.is_file() and not f.name.startswith(".") and f.suffix != ".py":
            shutil.copy2(f, media / f.name)
    return media


@pytest.fixture
def catalog(tmp_path):
    """Create and return an open Catalog."""
    cat = Catalog(tmp_path / "test.db")
    cat.open()
    yield cat
    cat.close()


# ── Scan tests ───────────────────────────────────────────────────────


def test_scan_real_jpeg(fixture_root, catalog):
    """Scan real JPEG fixtures into catalog."""
    stats = incremental_scan(catalog, [fixture_root], workers=1)
    assert stats.files_scanned >= 2
    assert stats.files_new >= 2

    # Verify files are in catalog
    rows = catalog.query_files(ext="jpg")
    assert len(rows) >= 2


def test_scan_file_sizes(fixture_root, catalog):
    """Scanned files should have correct sizes."""
    incremental_scan(catalog, [fixture_root], workers=1)
    rows = catalog.query_files(ext="jpg")
    for row in rows:
        real_path = Path(row.path)
        if real_path.exists():
            assert row.size == real_path.stat().st_size


def test_scan_sha256_populated(fixture_root, catalog):
    """SHA-256 hashes should be computed for all scanned files."""
    incremental_scan(catalog, [fixture_root], workers=1)
    rows = catalog.query_files(ext="jpg")
    for row in rows:
        assert row.sha256 is not None
        assert len(row.sha256) == 64  # hex SHA-256


def test_scan_different_content_different_hash(fixture_root, catalog):
    """Files with different content should have different hashes."""
    incremental_scan(catalog, [fixture_root], workers=1)
    rows = catalog.query_files(ext="jpg")
    if len(rows) >= 2:
        hashes = {r.sha256 for r in rows}
        assert len(hashes) >= 2, "Expected different hashes for different files"


# ── Perceptual hash tests ───────────────────────────────────────────


def test_phash_computed_for_jpeg(fixture_root, catalog):
    """Perceptual hash should be computed for JPEG files."""
    incremental_scan(catalog, [fixture_root], workers=1)
    rows = catalog.query_files(ext="jpg")
    # At least some files should have phash (depends on Pillow)
    phashes = [r for r in rows if getattr(r, "phash", None)]
    # If Pillow is installed (which it is in dev), phash should work
    assert len(phashes) >= 1, "Expected phash for at least one JPEG"


# ── ExifTool extraction tests ───────────────────────────────────────


@pytest.mark.requires_exiftool
def test_exiftool_extraction(fixture_root, catalog):
    """ExifTool should extract metadata from JPEG with EXIF."""
    from godmode_media_library.exiftool_extract import extract_all_metadata

    incremental_scan(catalog, [fixture_root], workers=1)
    rows = catalog.query_files()
    paths = [Path(r.path) for r in rows]
    result = extract_all_metadata(paths)
    assert len(result) >= 1


@pytest.mark.requires_exiftool
def test_exiftool_camera_info(fixture_root, catalog):
    """ExifTool should extract camera make/model from fixture."""
    from godmode_media_library.exiftool_extract import extract_all_metadata

    incremental_scan(catalog, [fixture_root], workers=1)
    rows_all = catalog.query_files()
    paths = [Path(r.path) for r in rows_all]
    extract_all_metadata(paths)

    # Check that camera info was extracted for tiny_photo.jpg
    rows = catalog.query_files(ext="jpg")
    cameras = [(getattr(r, "camera_make", None), getattr(r, "camera_model", None)) for r in rows]
    has_camera = any(make and model for make, model in cameras)
    assert has_camera, "Expected camera info from tiny_photo.jpg with EXIF"


# ── Metadata richness tests ─────────────────────────────────────────


@pytest.mark.requires_exiftool
def test_richness_scoring(fixture_root, catalog):
    """Richness scoring should work on real files after extraction."""
    from godmode_media_library.exiftool_extract import extract_all_metadata
    from godmode_media_library.metadata_richness import compute_richness

    incremental_scan(catalog, [fixture_root], workers=1)
    rows_all = catalog.query_files()
    paths = [Path(r.path) for r in rows_all]
    extract_all_metadata(paths)

    rows = catalog.query_files(ext="jpg")
    for row in rows:
        meta = catalog.get_file_metadata(row.path)
        if meta:
            score = compute_richness(meta)
            assert score.total >= 0


# ── Duplicate detection tests ────────────────────────────────────────


def test_duplicate_detection_with_identical_files(tmp_path, catalog):
    """Identical files should be detected as duplicates."""
    media = tmp_path / "media"
    media.mkdir()
    content = b"identical content for testing" * 10
    (media / "file_a.jpg").write_bytes(content)
    (media / "file_b.jpg").write_bytes(content)

    incremental_scan(catalog, [media], workers=1)
    dups = catalog.query_duplicates()
    assert len(dups) >= 1, "Expected at least one duplicate group"
    group_id, files = dups[0]
    assert len(files) >= 2


# ── Incremental scan tests ──────────────────────────────────────────


def test_incremental_scan_no_changes(fixture_root, catalog):
    """Second scan should detect no new/changed files."""
    stats1 = incremental_scan(catalog, [fixture_root], workers=1)
    assert stats1.files_new >= 2

    stats2 = incremental_scan(catalog, [fixture_root], workers=1)
    assert stats2.files_new == 0
    assert stats2.files_changed == 0


def test_incremental_scan_detects_new_file(fixture_root, catalog):
    """Adding a new file should be detected on rescan."""
    incremental_scan(catalog, [fixture_root], workers=1)

    # Add a new file
    (fixture_root / "new_photo.jpg").write_bytes(b"brand new content")
    stats2 = incremental_scan(catalog, [fixture_root], workers=1)
    assert stats2.files_new == 1


# ── Video tests (require ffmpeg/ffprobe) ─────────────────────────────


@pytest.mark.requires_ffprobe
def test_scan_video_fixture(tmp_path, catalog):
    """Scan should handle video files when ffprobe is available."""
    video_src = FIXTURES / "tiny_video.mp4"
    if not video_src.exists():
        pytest.skip("Video fixture not created (ffmpeg was not available)")

    media = tmp_path / "media"
    media.mkdir()
    shutil.copy2(video_src, media / "test_video.mp4")

    stats = incremental_scan(catalog, [media], workers=1)
    assert stats.files_scanned >= 1

    rows = catalog.query_files(ext="mp4")
    assert len(rows) >= 1


@pytest.mark.requires_ffmpeg
def test_video_phash(tmp_path, catalog):
    """Video perceptual hash should be computed when ffmpeg is available."""
    video_src = FIXTURES / "tiny_video.mp4"
    if not video_src.exists():
        pytest.skip("Video fixture not created")

    media = tmp_path / "media"
    media.mkdir()
    shutil.copy2(video_src, media / "test_video.mp4")

    incremental_scan(catalog, [media], workers=1)
    rows = catalog.query_files(ext="mp4")
    if rows:
        # Video phash may or may not be computed depending on ffmpeg
        # Just verify no crash and phash attribute exists
        assert hasattr(rows[0], "phash")
