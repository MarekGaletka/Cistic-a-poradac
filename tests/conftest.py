from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from godmode_media_library.models import DuplicateRow, FileRecord

# ── Auto-skip markers for external tools ──────────────────────────────


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: integration tests with real files")
    config.addinivalue_line("markers", "requires_exiftool: skip if exiftool not installed")
    config.addinivalue_line("markers", "requires_ffprobe: skip if ffprobe not installed")
    config.addinivalue_line("markers", "requires_ffmpeg: skip if ffmpeg not installed")


def pytest_collection_modifyitems(config, items):
    tool_markers = {
        "requires_exiftool": "exiftool",
        "requires_ffprobe": "ffprobe",
        "requires_ffmpeg": "ffmpeg",
    }
    for item in items:
        for marker_name, binary in tool_markers.items():
            if marker_name in item.keywords and not shutil.which(binary):
                item.add_marker(pytest.mark.skip(reason=f"{binary} not installed"))


@pytest.fixture()
def tmp_media_tree(tmp_path: Path) -> Path:
    """Create a realistic temp media directory tree for testing."""
    photos = tmp_path / "photos"
    photos.mkdir()

    # photo1.jpg — used as a base for duplicate testing
    (photos / "photo1.jpg").write_bytes(b"JPEG_CONTENT_1" * 100)

    # photo1.mov — companion video (same stem = Live Photo pair)
    (photos / "photo1.mov").write_bytes(b"MOV_CONTENT_1" * 80)

    # photo1.aae — sidecar
    (photos / "photo1.aae").write_bytes(b"AAE_SIDECAR_DATA" * 10)

    # photo2.jpg — different content
    (photos / "photo2.jpg").write_bytes(b"JPEG_CONTENT_2" * 100)

    # photo3.jpg — exact duplicate of photo1.jpg
    (photos / "photo3.jpg").write_bytes(b"JPEG_CONTENT_1" * 100)

    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "readme.pdf").write_bytes(b"PDF_CONTENT_HERE" * 50)

    noise = tmp_path / "noise"
    noise.mkdir()
    (noise / ".DS_Store").write_bytes(b"\x00\x00\x00\x01" * 4)
    (noise / "._photo1.jpg").write_bytes(b"\x00\x05\x16\x07" * 4)

    return tmp_path


@pytest.fixture()
def make_file_record():
    """Factory fixture that returns a FileRecord with sensible defaults."""

    def _factory(**kwargs) -> FileRecord:
        defaults = dict(
            path=Path("/tmp/test/photo.jpg"),
            size=1024,
            mtime=1700000000.0,
            ctime=1700000000.0,
            birthtime=1699999000.0,
            ext="jpg",
            meaningful_xattr_count=0,
            asset_key=None,
            asset_component=False,
        )
        defaults.update(kwargs)
        return FileRecord(**defaults)

    return _factory


@pytest.fixture()
def make_duplicate_row():
    """Factory fixture that returns a DuplicateRow with sensible defaults."""

    def _factory(**kwargs) -> DuplicateRow:
        defaults = dict(
            digest="abc123def456" * 5 + "ab",
            size=1024,
            path=Path("/tmp/test/photo.jpg"),
        )
        defaults.update(kwargs)
        return DuplicateRow(**defaults)

    return _factory
