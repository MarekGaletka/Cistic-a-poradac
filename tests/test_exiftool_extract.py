from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from godmode_media_library.exiftool_extract import (
    exiftool_available,
    extract_all_metadata,
    extract_single,
)


def test_exiftool_available_found():
    with patch("godmode_media_library.exiftool_extract.shutil.which", return_value="/usr/bin/exiftool"):
        mock_result = MagicMock()
        mock_result.returncode = 0
        with patch("subprocess.run", return_value=mock_result):
            result = exiftool_available()
            assert result == "/usr/bin/exiftool"


def test_exiftool_available_not_found():
    with patch("godmode_media_library.exiftool_extract.shutil.which", return_value=None):
        result = exiftool_available()
        assert result is None


def test_extract_all_metadata_no_exiftool():
    with patch("godmode_media_library.exiftool_extract.exiftool_available", return_value=None):
        result = extract_all_metadata([Path("/tmp/test.jpg")])
        assert result == {}


def test_extract_all_metadata_empty_list():
    result = extract_all_metadata([])
    assert result == {}


def test_extract_all_metadata_success():
    exiftool_output = json.dumps([
        {
            "SourceFile": "/tmp/photo.jpg",
            "EXIF:Make": "Canon",
            "EXIF:Model": "EOS R5",
            "EXIF:DateTimeOriginal": "2024:06:15 10:30:00",
            "EXIF:GPSLatitude": 50.0875,
            "EXIF:GPSLongitude": 14.4214,
            "XMP:Subject": ["landscape", "prague"],
            "IPTC:Keywords": ["travel"],
        }
    ])
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = exiftool_output

    with patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"), \
         patch("subprocess.run", return_value=mock_proc):
        result = extract_all_metadata([Path("/tmp/photo.jpg")])
        resolved = Path("/tmp/photo.jpg").expanduser().resolve()
        assert resolved in result
        meta = result[resolved]
        assert meta["EXIF:Make"] == "Canon"
        assert meta["EXIF:Model"] == "EOS R5"
        assert meta["EXIF:GPSLatitude"] == 50.0875
        assert meta["XMP:Subject"] == ["landscape", "prague"]


def test_extract_all_metadata_filters_source_file():
    exiftool_output = json.dumps([
        {"SourceFile": "/tmp/a.jpg", "EXIF:Make": "Nikon"}
    ])
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = exiftool_output

    with patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"), \
         patch("subprocess.run", return_value=mock_proc):
        result = extract_all_metadata([Path("/tmp/a.jpg")])
        resolved = Path("/tmp/a.jpg").expanduser().resolve()
        meta = result[resolved]
        assert "SourceFile" not in meta


def test_extract_all_metadata_excludes_binary_tags():
    exiftool_output = json.dumps([
        {"SourceFile": "/tmp/a.jpg", "EXIF:Make": "Canon", "EXIF:ThumbnailImage": "(binary data)"}
    ])
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = exiftool_output

    with patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"), \
         patch("subprocess.run", return_value=mock_proc):
        result = extract_all_metadata([Path("/tmp/a.jpg")])
        resolved = Path("/tmp/a.jpg").expanduser().resolve()
        meta = result[resolved]
        assert "EXIF:ThumbnailImage" not in meta


def test_extract_all_metadata_timeout():
    import subprocess

    with patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"), \
         patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="exiftool", timeout=120)):
        result = extract_all_metadata([Path("/tmp/a.jpg")])
        assert result == {}


def test_extract_all_metadata_invalid_json():
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = "not valid json"

    with patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"), \
         patch("subprocess.run", return_value=mock_proc):
        result = extract_all_metadata([Path("/tmp/a.jpg")])
        assert result == {}


def test_extract_single():
    exiftool_output = json.dumps([
        {"SourceFile": "/tmp/single.jpg", "EXIF:ISO": 400}
    ])
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = exiftool_output

    with patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"), \
         patch("subprocess.run", return_value=mock_proc):
        meta = extract_single(Path("/tmp/single.jpg"))
        assert meta.get("EXIF:ISO") == 400


def test_extract_single_failure():
    with patch("godmode_media_library.exiftool_extract.exiftool_available", return_value=None):
        meta = extract_single(Path("/tmp/missing.jpg"))
        assert meta == {}
