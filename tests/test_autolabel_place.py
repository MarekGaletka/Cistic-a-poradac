from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library.autolabel_place import (
    _chunks,
    _coord_key,
    _coord_label,
    _coords_from_exif,
    _format_geocode_label,
    _load_cache,
    _normalize_lat_lon,
    _parse_coord_string,
    _save_cache,
    _to_float,
    extract_gps_with_exiftool,
    reverse_geocode_coords,
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


# ---------------------------------------------------------------------------
# _to_float — edge cases
# ---------------------------------------------------------------------------


def test_to_float_none():
    assert _to_float(None) is None


def test_to_float_non_numeric_string():
    assert _to_float("abc") is None


def test_to_float_float_input():
    assert _to_float(3.14) == 3.14


# ---------------------------------------------------------------------------
# _parse_coord_string — edge cases
# ---------------------------------------------------------------------------


def test_parse_coord_string_single_number():
    """Only one number — not enough for lat/lon."""
    assert _parse_coord_string("48.8566") is None


def test_parse_coord_string_south_direction():
    result = _parse_coord_string("33.8688 S, 151.2093 E")
    assert result is not None
    lat, lon = result
    assert lat < 0  # S = negative lat
    assert lon > 0


def test_parse_coord_string_invalid_range():
    """Out of range coordinates should return None."""
    assert _parse_coord_string("95.0, 200.0") is None


# ---------------------------------------------------------------------------
# _normalize_lat_lon — boundary cases
# ---------------------------------------------------------------------------


def test_normalize_lat_lon_boundary():
    assert _normalize_lat_lon(90.0, 180.0) == (90.0, 180.0)
    assert _normalize_lat_lon(-90.0, -180.0) == (-90.0, -180.0)


def test_normalize_lat_lon_negative_out_of_range():
    assert _normalize_lat_lon(0.0, -181.0) is None


# ---------------------------------------------------------------------------
# _coords_from_exif — composite/EXIF key fallbacks
# ---------------------------------------------------------------------------


def test_coords_from_exif_composite_keys():
    entry = {
        "Composite:GPSLatitude": 50.0,
        "Composite:GPSLongitude": 14.0,
    }
    result = _coords_from_exif(entry)
    assert result == (50.0, 14.0)


def test_coords_from_exif_xmp_keys():
    entry = {
        "XMP:GPSLatitude": "48.0",
        "XMP:GPSLongitude": "16.0",
    }
    result = _coords_from_exif(entry)
    assert result is not None
    assert result == (48.0, 16.0)


def test_coords_from_exif_coord_string_fallback():
    """When lat/lon keys are missing, fall back to GPSCoordinates string."""
    entry = {
        "QuickTime:GPSCoordinates": "50.0833, 14.4167",
    }
    result = _coords_from_exif(entry)
    assert result is not None
    assert abs(result[0] - 50.0833) < 0.001


def test_coords_from_exif_gps_position():
    entry = {
        "Composite:GPSPosition": "48.2082 16.3738",
    }
    result = _coords_from_exif(entry)
    assert result is not None


def test_coords_from_exif_non_string_coord_key():
    """Non-string values for coordinate keys should be skipped."""
    entry = {
        "QuickTime:GPSCoordinates": 12345,  # not a string
    }
    result = _coords_from_exif(entry)
    assert result is None


def test_coords_from_exif_lat_only():
    """Only latitude available, no longitude — should return None."""
    entry = {"GPSLatitude": 50.0}
    result = _coords_from_exif(entry)
    assert result is None


# ---------------------------------------------------------------------------
# _chunks
# ---------------------------------------------------------------------------


def test_chunks_basic():
    items = [Path(f"/tmp/{i}.jpg") for i in range(5)]
    result = _chunks(items, 2)
    assert len(result) == 3
    assert len(result[0]) == 2
    assert len(result[2]) == 1


def test_chunks_empty():
    assert _chunks([], 10) == []


def test_chunks_exact_fit():
    items = [Path(f"/tmp/{i}.jpg") for i in range(4)]
    result = _chunks(items, 2)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# _coord_key / _coord_label
# ---------------------------------------------------------------------------


def test_coord_key():
    key = _coord_key(48.85660, 2.35220)
    assert key == "48.85660,2.35220"


def test_coord_label():
    label = _coord_label(48.85660, 2.35220)
    assert label == "GPS 48.85660,2.35220"


# ---------------------------------------------------------------------------
# _load_cache / _save_cache
# ---------------------------------------------------------------------------


def test_load_cache_nonexistent(tmp_path):
    cache = _load_cache(tmp_path / "nonexistent.json")
    assert cache == {}


def test_load_cache_valid(tmp_path):
    cache_file = tmp_path / "cache.json"
    cache_file.write_text(json.dumps({"48.0,16.0": "Prague, CZ"}))
    cache = _load_cache(cache_file)
    assert cache == {"48.0,16.0": "Prague, CZ"}


def test_load_cache_invalid_json(tmp_path):
    cache_file = tmp_path / "cache.json"
    cache_file.write_text("not json")
    cache = _load_cache(cache_file)
    assert cache == {}


def test_load_cache_non_dict(tmp_path):
    cache_file = tmp_path / "cache.json"
    cache_file.write_text(json.dumps([1, 2, 3]))
    cache = _load_cache(cache_file)
    assert cache == {}


def test_load_cache_filters_non_string_values(tmp_path):
    cache_file = tmp_path / "cache.json"
    cache_file.write_text(json.dumps({"key1": "val1", "key2": 123}))
    cache = _load_cache(cache_file)
    assert cache == {"key1": "val1"}


def test_save_cache(tmp_path):
    cache_file = tmp_path / "subdir" / "cache.json"
    _save_cache(cache_file, {"48.0,16.0": "Prague"})
    assert cache_file.exists()
    data = json.loads(cache_file.read_text())
    assert data["48.0,16.0"] == "Prague"


# ---------------------------------------------------------------------------
# _format_geocode_label — more fallback paths
# ---------------------------------------------------------------------------


def test_format_geocode_label_village():
    address = {"village": "Sedlec", "country": "Czech Republic"}
    assert _format_geocode_label(address) == "Sedlec, Czech Republic"


def test_format_geocode_label_municipality():
    address = {"municipality": "Obec", "country": "Czech Republic"}
    assert _format_geocode_label(address) == "Obec, Czech Republic"


def test_format_geocode_label_hamlet():
    address = {"hamlet": "Vinicka", "country": "Czech Republic"}
    assert _format_geocode_label(address) == "Vinicka, Czech Republic"


def test_format_geocode_label_county_fallback():
    address = {"county": "Praha-zapad", "country": "Czech Republic"}
    assert _format_geocode_label(address) == "Praha-zapad, Czech Republic"


def test_format_geocode_label_state_fallback():
    address = {"state": "California", "country": "USA"}
    assert _format_geocode_label(address) == "California, USA"


# ---------------------------------------------------------------------------
# extract_gps_with_exiftool — mocked subprocess
# ---------------------------------------------------------------------------


def test_extract_gps_no_exiftool():
    with patch("shutil.which", return_value=None):
        with pytest.raises(RuntimeError, match="ExifTool is not available"):
            extract_gps_with_exiftool([Path("/tmp/a.jpg")])


def test_extract_gps_success(tmp_path):
    photo = tmp_path / "photo.jpg"
    photo.write_bytes(b"fake")

    exif_output = json.dumps([
        {
            "SourceFile": str(photo),
            "GPSLatitude": 50.0,
            "GPSLongitude": 14.0,
        }
    ])

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = exif_output
    mock_proc.stderr = ""

    with (
        patch("shutil.which", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        mapping, binary = extract_gps_with_exiftool([photo])

    assert len(mapping) == 1
    assert binary == "/usr/bin/exiftool"
    coords = list(mapping.values())[0]
    assert coords == (50.0, 14.0)


def test_extract_gps_exiftool_error():
    mock_proc = MagicMock()
    mock_proc.returncode = 2  # Error
    mock_proc.stderr = "some error"
    mock_proc.stdout = ""

    with (
        patch("shutil.which", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        with pytest.raises(RuntimeError, match="ExifTool failed"):
            extract_gps_with_exiftool([Path("/tmp/a.jpg")])


def test_extract_gps_empty_output():
    """ExifTool returns empty stdout — should return empty mapping."""
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = ""
    mock_proc.stderr = ""

    with (
        patch("shutil.which", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        mapping, binary = extract_gps_with_exiftool([Path("/tmp/a.jpg")])
    assert mapping == {}


def test_extract_gps_invalid_json():
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = "not json"
    mock_proc.stderr = ""

    with (
        patch("shutil.which", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        with pytest.raises(RuntimeError, match="Failed to parse"):
            extract_gps_with_exiftool([Path("/tmp/a.jpg")])


def test_extract_gps_non_dict_rows():
    """Non-dict rows in JSON output should be skipped."""
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = json.dumps(["not_a_dict", None, 42])
    mock_proc.stderr = ""

    with (
        patch("shutil.which", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        mapping, _ = extract_gps_with_exiftool([Path("/tmp/a.jpg")])
    assert mapping == {}


def test_extract_gps_missing_source_file():
    """Row without SourceFile should be skipped."""
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = json.dumps([{"GPSLatitude": 50.0, "GPSLongitude": 14.0}])
    mock_proc.stderr = ""

    with (
        patch("shutil.which", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        mapping, _ = extract_gps_with_exiftool([Path("/tmp/a.jpg")])
    assert mapping == {}


def test_extract_gps_returncode_1():
    """ExifTool returncode=1 (warnings) should still process output."""
    exif_output = json.dumps([
        {
            "SourceFile": "/tmp/a.jpg",
            "GPSLatitude": 50.0,
            "GPSLongitude": 14.0,
        }
    ])
    mock_proc = MagicMock()
    mock_proc.returncode = 1
    mock_proc.stdout = exif_output
    mock_proc.stderr = ""

    with (
        patch("shutil.which", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        mapping, _ = extract_gps_with_exiftool([Path("/tmp/a.jpg")])
    assert len(mapping) == 1


# ---------------------------------------------------------------------------
# reverse_geocode_coords — mocked geopy
# ---------------------------------------------------------------------------


def test_reverse_geocode_cached(tmp_path):
    """Cached coords should not trigger API calls."""
    cache_file = tmp_path / "cache.json"
    cache_file.write_text(json.dumps({"50.00000,14.00000": "Prague, CZ"}))

    coords = {(50.0, 14.0)}

    mock_nominatim = MagicMock()
    mock_rate_limiter = MagicMock()
    mock_geocoder_error = type("GeocoderServiceError", (Exception,), {})

    with patch.dict("sys.modules", {
        "geopy": MagicMock(),
        "geopy.exc": MagicMock(GeocoderServiceError=mock_geocoder_error),
        "geopy.extra": MagicMock(),
        "geopy.extra.rate_limiter": MagicMock(RateLimiter=mock_rate_limiter),
        "geopy.geocoders": MagicMock(Nominatim=mock_nominatim),
    }):
        results, api_calls = reverse_geocode_coords(
            coords, cache_path=cache_file, min_delay_seconds=0,
        )

    assert results[(50.0, 14.0)] == "Prague, CZ"
    assert api_calls == 0


def test_reverse_geocode_api_call(tmp_path):
    """Uncached coords should trigger API and update cache."""
    cache_file = tmp_path / "cache.json"
    cache_file.write_text(json.dumps({}))

    coords = {(50.0, 14.0)}

    mock_location = MagicMock()
    mock_location.raw = {"address": {"city": "Prague", "country": "Czech Republic"}}

    mock_reverse_fn = MagicMock(return_value=mock_location)
    mock_rate_limiter = MagicMock(return_value=mock_reverse_fn)
    mock_nominatim = MagicMock()
    mock_geocoder_error = type("GeocoderServiceError", (Exception,), {})

    with patch.dict("sys.modules", {
        "geopy": MagicMock(),
        "geopy.exc": MagicMock(GeocoderServiceError=mock_geocoder_error),
        "geopy.extra": MagicMock(),
        "geopy.extra.rate_limiter": MagicMock(RateLimiter=mock_rate_limiter),
        "geopy.geocoders": MagicMock(Nominatim=mock_nominatim),
    }):
        results, api_calls = reverse_geocode_coords(
            coords, cache_path=cache_file, min_delay_seconds=0,
        )

    assert api_calls == 1
    assert "Prague" in results[(50.0, 14.0)]

    # Verify cache was updated
    cache_data = json.loads(cache_file.read_text())
    assert "50.00000,14.00000" in cache_data


def test_reverse_geocode_api_failure(tmp_path):
    """API failure should fall back to GPS coordinate label."""
    cache_file = tmp_path / "cache.json"
    cache_file.write_text(json.dumps({}))

    coords = {(50.0, 14.0)}

    mock_geocoder_error = type("GeocoderServiceError", (Exception,), {})
    mock_reverse_fn = MagicMock(side_effect=OSError("network error"))
    mock_rate_limiter = MagicMock(return_value=mock_reverse_fn)
    mock_nominatim = MagicMock()

    with patch.dict("sys.modules", {
        "geopy": MagicMock(),
        "geopy.exc": MagicMock(GeocoderServiceError=mock_geocoder_error),
        "geopy.extra": MagicMock(),
        "geopy.extra.rate_limiter": MagicMock(RateLimiter=mock_rate_limiter),
        "geopy.geocoders": MagicMock(Nominatim=mock_nominatim),
    }):
        results, api_calls = reverse_geocode_coords(
            coords, cache_path=cache_file, min_delay_seconds=0,
        )

    assert api_calls == 1
    # Should fall back to GPS label
    assert "GPS" in results[(50.0, 14.0)]
