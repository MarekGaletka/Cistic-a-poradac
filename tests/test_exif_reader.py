from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from godmode_media_library.exif_reader import (
    ExifMeta,
    _clean_string,
    _dms_to_decimal,
    _parse_gps,
    can_read_exif,
    read_exif,
)


def test_can_read_exif_jpeg():
    assert can_read_exif("jpg")
    assert can_read_exif("jpeg")
    assert can_read_exif("tiff")
    assert can_read_exif("png")


def test_can_read_exif_not_supported():
    assert not can_read_exif("mp4")
    assert not can_read_exif("mov")
    assert not can_read_exif("pdf")


def test_can_read_exif_heic_raw():
    assert can_read_exif("heic")
    assert can_read_exif("heif")
    assert can_read_exif("cr2")
    assert can_read_exif("dng")


def test_dms_to_decimal_north():
    result = _dms_to_decimal((50, 5, 30.0), "N")
    assert abs(result - 50.091667) < 0.001


def test_dms_to_decimal_south():
    result = _dms_to_decimal((33, 51, 54.0), "S")
    assert result < 0
    assert abs(result - (-33.865)) < 0.001


def test_dms_to_decimal_east():
    result = _dms_to_decimal((14, 25, 0.0), "E")
    assert abs(result - 14.416667) < 0.001


def test_dms_to_decimal_west():
    result = _dms_to_decimal((118, 14, 34.0), "W")
    assert result < 0


def test_clean_string_none():
    assert _clean_string(None) is None


def test_clean_string_normal():
    assert _clean_string("Canon") == "Canon"


def test_clean_string_with_nulls():
    assert _clean_string("Canon\x00\x00") == "Canon"


def test_clean_string_empty():
    assert _clean_string("") is None
    assert _clean_string("  ") is None


def test_exif_meta_defaults():
    m = ExifMeta()
    assert m.date_original is None
    assert m.camera_make is None
    assert m.gps_latitude is None


def test_read_exif_nonexistent():
    result = read_exif(Path("/nonexistent/file.jpg"))
    assert result is None


try:
    import importlib.util

    HAS_PILLOW = importlib.util.find_spec("PIL") is not None
except Exception:
    HAS_PILLOW = False


@pytest.mark.skipif(not HAS_PILLOW, reason="Pillow not installed")
def test_read_exif_basic_image(tmp_path: Path):
    """Read EXIF from a simple PNG (no EXIF data, but should get dimensions)."""
    from PIL import Image

    img = Image.new("RGB", (640, 480), color="green")
    img_path = tmp_path / "test.png"
    img.save(str(img_path))

    meta = read_exif(img_path)
    assert meta is not None
    assert meta.image_width == 640
    assert meta.image_height == 480
    assert meta.camera_make is None
    assert meta.date_original is None


@pytest.mark.skipif(not HAS_PILLOW, reason="Pillow not installed")
def test_read_exif_jpeg_with_exif(tmp_path: Path):
    """Create a JPEG with EXIF data and verify reading."""
    from PIL import Image

    img = Image.new("RGB", (320, 240), color="yellow")
    img_path = tmp_path / "photo.jpg"

    exif = img.getexif()
    exif[0x010F] = "TestMake"
    exif[0x0110] = "TestModel"
    exif[0x0132] = "2024:06:15 10:30:00"
    img.save(str(img_path), exif=exif.tobytes())

    meta = read_exif(img_path)
    assert meta is not None
    assert meta.camera_make == "TestMake"
    assert meta.camera_model == "TestModel"
    assert meta.date_original == "2024:06:15 10:30:00"
    assert meta.image_width == 320
    assert meta.image_height == 240


def test_read_exif_corrupt_file(tmp_path: Path):
    """Corrupt file should return None."""
    bad = tmp_path / "corrupt.jpg"
    bad.write_bytes(b"not a real image at all")
    result = read_exif(bad)
    assert result is None


def test_read_exif_heic_without_pillow_heif(tmp_path: Path):
    """HEIC file without pillow-heif should return None."""
    heic = tmp_path / "photo.heic"
    heic.write_bytes(b"\x00" * 100)
    with patch("builtins.__import__", side_effect=_make_heif_import_error()):
        result = read_exif(heic)
    # Should return None because pillow_heif is not available
    assert result is None


def _make_heif_import_error():
    """Helper to make import of pillow_heif fail while allowing PIL."""
    original_import = __import__

    def side_effect(name, *args, **kwargs):
        if name == "pillow_heif":
            raise ImportError("No module named 'pillow_heif'")
        return original_import(name, *args, **kwargs)

    return side_effect


def test_read_exif_no_pillow():
    """When Pillow is not installed, read_exif returns None."""
    original_import = __import__

    def fake_import(name, *args, **kwargs):
        if name == "PIL" or (isinstance(name, str) and name.startswith("PIL.")):
            raise ImportError("No module named 'PIL'")
        return original_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=fake_import):
        result = read_exif(Path("/tmp/photo.jpg"))
        assert result is None


def test_can_read_exif_strips_dot():
    """can_read_exif should handle extensions with or without leading dot."""
    assert can_read_exif(".jpg") is True
    assert can_read_exif("jpg") is True
    assert can_read_exif(".JPEG") is True


def test_dms_to_decimal_none_ref():
    """DMS with None ref should return positive value."""
    result = _dms_to_decimal((50, 0, 0.0), None)
    assert result == 50.0


@pytest.mark.skipif(not HAS_PILLOW, reason="Pillow not installed")
def test_read_exif_image_no_exif_data(tmp_path: Path):
    """Image with no EXIF data should still return dimensions."""
    from PIL import Image

    img = Image.new("L", (100, 50))
    img_path = tmp_path / "gray.png"
    img.save(str(img_path))

    meta = read_exif(img_path)
    assert meta is not None
    assert meta.image_width == 100
    assert meta.image_height == 50
    assert meta.camera_make is None


def test_parse_gps_with_altitude():
    """GPS parsing should extract altitude."""
    meta = ExifMeta()
    gps_ifd = {
        2: (50, 5, 30.0),  # latitude
        1: "N",  # lat ref
        4: (14, 25, 0.0),  # longitude
        3: "E",  # lon ref
        6: 250.5,  # altitude
        5: 0,  # alt ref (above sea level)
    }
    _parse_gps(gps_ifd, meta)
    assert meta.gps_latitude is not None
    assert meta.gps_longitude is not None
    assert meta.gps_altitude == 250.5


def test_parse_gps_below_sea_level():
    meta = ExifMeta()
    gps_ifd = {
        2: (31, 30, 0.0),
        1: "N",
        4: (35, 28, 0.0),
        3: "E",
        6: 430.0,
        5: 1,  # below sea level
    }
    _parse_gps(gps_ifd, meta)
    assert meta.gps_altitude == -430.0


def test_parse_gps_no_lat_lon():
    meta = ExifMeta()
    gps_ifd = {6: 100.0, 5: 0}
    _parse_gps(gps_ifd, meta)
    assert meta.gps_latitude is None
    assert meta.gps_longitude is None
    assert meta.gps_altitude == 100.0


def test_parse_gps_invalid_values():
    meta = ExifMeta()
    gps_ifd = {
        2: "invalid",
        1: "N",
        4: "invalid",
        3: "E",
    }
    _parse_gps(gps_ifd, meta)
    assert meta.gps_latitude is None
    assert meta.gps_longitude is None
