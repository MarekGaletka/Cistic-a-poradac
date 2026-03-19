from __future__ import annotations

from godmode_media_library.autolabel_place import (
    _coords_from_exif,
    _format_geocode_label,
    _normalize_lat_lon,
    _parse_coord_string,
    _to_float,
)


def test_to_float_int():
    assert _to_float(42) == 42.0


def test_to_float_string():
    assert _to_float("12.5") == 12.5


def test_to_float_empty():
    assert _to_float("") is None
    assert _to_float("  ") is None


def test_parse_coord_string():
    result = _parse_coord_string("48.8566, 2.3522")
    assert result is not None
    lat, lon = result
    assert abs(lat - 48.8566) < 0.0001
    assert abs(lon - 2.3522) < 0.0001


def test_parse_coord_string_with_direction():
    result = _parse_coord_string("48.8566 N, 2.3522 W")
    assert result is not None
    lat, lon = result
    assert abs(lat - 48.8566) < 0.0001
    assert abs(lon - (-2.3522)) < 0.0001  # W = negative longitude


def test_normalize_lat_lon_valid():
    result = _normalize_lat_lon(48.8566, 2.3522)
    assert result is not None
    assert result == (48.8566, 2.3522)


def test_normalize_lat_lon_invalid():
    # lat > 90 is invalid
    assert _normalize_lat_lon(91.0, 2.0) is None
    # lon > 180 is invalid
    assert _normalize_lat_lon(48.0, 181.0) is None
    # lat < -90
    assert _normalize_lat_lon(-91.0, 2.0) is None


def test_coords_from_exif():
    entry = {
        "GPSLatitude": 48.8566,
        "GPSLongitude": 2.3522,
    }
    result = _coords_from_exif(entry)
    assert result is not None
    lat, lon = result
    assert abs(lat - 48.8566) < 0.0001
    assert abs(lon - 2.3522) < 0.0001


def test_coords_from_exif_string_values():
    entry = {
        "GPSLatitude": "48.8566",
        "GPSLongitude": "2.3522",
    }
    result = _coords_from_exif(entry)
    assert result is not None


def test_coords_from_exif_missing():
    entry = {"SomeOtherField": "value"}
    result = _coords_from_exif(entry)
    assert result is None


def test_format_geocode_label():
    address = {"city": "Prague", "country": "Czech Republic", "state": "Central Bohemia"}
    label = _format_geocode_label(address)
    assert label == "Prague, Czech Republic"


def test_format_geocode_label_fallback():
    address = {"country": "Czech Republic"}
    label = _format_geocode_label(address)
    assert label == "Czech Republic"


def test_format_geocode_label_empty():
    address = {}
    label = _format_geocode_label(address)
    assert label == ""


def test_format_geocode_label_town():
    address = {"town": "Brno", "country": "Czech Republic"}
    label = _format_geocode_label(address)
    assert label == "Brno, Czech Republic"
