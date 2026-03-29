from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from godmode_media_library.exiftool_extract import (
    batch_write_tags,
    exiftool_available,
    extract_all_metadata,
    extract_single,
    write_tags,
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
    exiftool_output = json.dumps(
        [
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
        ]
    )
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = exiftool_output

    with (
        patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        result = extract_all_metadata([Path("/tmp/photo.jpg")])
        resolved = Path("/tmp/photo.jpg").expanduser().resolve()
        assert resolved in result
        meta = result[resolved]
        assert meta["EXIF:Make"] == "Canon"
        assert meta["EXIF:Model"] == "EOS R5"
        assert meta["EXIF:GPSLatitude"] == 50.0875
        assert meta["XMP:Subject"] == ["landscape", "prague"]


def test_extract_all_metadata_filters_source_file():
    exiftool_output = json.dumps([{"SourceFile": "/tmp/a.jpg", "EXIF:Make": "Nikon"}])
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = exiftool_output

    with (
        patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        result = extract_all_metadata([Path("/tmp/a.jpg")])
        resolved = Path("/tmp/a.jpg").expanduser().resolve()
        meta = result[resolved]
        assert "SourceFile" not in meta


def test_extract_all_metadata_excludes_binary_tags():
    exiftool_output = json.dumps([{"SourceFile": "/tmp/a.jpg", "EXIF:Make": "Canon", "EXIF:ThumbnailImage": "(binary data)"}])
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = exiftool_output

    with (
        patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        result = extract_all_metadata([Path("/tmp/a.jpg")])
        resolved = Path("/tmp/a.jpg").expanduser().resolve()
        meta = result[resolved]
        assert "EXIF:ThumbnailImage" not in meta


def test_extract_all_metadata_timeout():
    import subprocess

    with (
        patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="exiftool", timeout=120)),
    ):
        result = extract_all_metadata([Path("/tmp/a.jpg")])
        assert result == {}


def test_extract_all_metadata_invalid_json():
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = "not valid json"

    with (
        patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        result = extract_all_metadata([Path("/tmp/a.jpg")])
        assert result == {}


def test_extract_single():
    exiftool_output = json.dumps([{"SourceFile": "/tmp/single.jpg", "EXIF:ISO": 400}])
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = exiftool_output

    with (
        patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc),
    ):
        meta = extract_single(Path("/tmp/single.jpg"))
        assert meta.get("EXIF:ISO") == 400


def test_extract_single_failure():
    with patch("godmode_media_library.exiftool_extract.exiftool_available", return_value=None):
        meta = extract_single(Path("/tmp/missing.jpg"))
        assert meta == {}


# ---------------------------------------------------------------------------
# write_tags
# ---------------------------------------------------------------------------


class TestWriteTags:
    def test_exiftool_not_available(self):
        with patch("godmode_media_library.exiftool_extract.exiftool_available", return_value=None):
            ok, msg = write_tags(Path("/tmp/a.jpg"), {"EXIF:Make": "Canon"})
            assert ok is False
            assert "not available" in msg

    def test_successful_write(self):
        mock_proc = MagicMock(returncode=0, stderr="")
        with (
            patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
            patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=mock_proc),
        ):
            ok, msg = write_tags(Path("/tmp/a.jpg"), {"EXIF:Make": "Canon", "EXIF:Model": "R5"})
            assert ok is True
            assert "Updated 2 tags" in msg

    def test_write_with_overwrite_original(self):
        mock_proc = MagicMock(returncode=0, stderr="")
        with (
            patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
            patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=mock_proc) as mock_run,
        ):
            write_tags(Path("/tmp/a.jpg"), {"EXIF:Make": "Nikon"}, overwrite_original=True)
            cmd = mock_run.call_args[0][0]
            assert "-overwrite_original" in cmd

    def test_write_list_values(self):
        mock_proc = MagicMock(returncode=0, stderr="")
        with (
            patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
            patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=mock_proc) as mock_run,
        ):
            write_tags(Path("/tmp/a.jpg"), {"XMP:Subject": ["landscape", "prague"]})
            cmd = mock_run.call_args[0][0]
            assert "-XMP:Subject=landscape" in cmd
            assert "-XMP:Subject=prague" in cmd

    def test_write_timeout(self):
        import subprocess as sp
        with (
            patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
            patch("godmode_media_library.exiftool_extract.subprocess.run", side_effect=sp.TimeoutExpired("exiftool", 30)),
        ):
            ok, msg = write_tags(Path("/tmp/a.jpg"), {"EXIF:Make": "Canon"})
            assert ok is False
            assert "timeout" in msg.lower()

    def test_write_file_not_found(self):
        with (
            patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
            patch("godmode_media_library.exiftool_extract.subprocess.run", side_effect=FileNotFoundError),
        ):
            ok, msg = write_tags(Path("/tmp/a.jpg"), {"EXIF:Make": "Canon"})
            assert ok is False
            assert "not found" in msg.lower()

    def test_write_error_returncode(self):
        mock_proc = MagicMock(returncode=2, stderr="Some error occurred")
        with (
            patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
            patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=mock_proc),
        ):
            ok, msg = write_tags(Path("/tmp/a.jpg"), {"EXIF:Make": "Canon"})
            assert ok is False
            assert "error" in msg.lower()

    def test_write_returncode_1_is_ok(self):
        """returncode 1 (minor warning) should still be success."""
        mock_proc = MagicMock(returncode=1, stderr="")
        with (
            patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
            patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=mock_proc),
        ):
            ok, msg = write_tags(Path("/tmp/a.jpg"), {"EXIF:Make": "Canon"})
            assert ok is True


class TestBatchWriteTags:
    def test_batch_write(self):
        mock_proc = MagicMock(returncode=0, stderr="")
        with (
            patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
            patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=mock_proc),
        ):
            results = batch_write_tags({
                Path("/tmp/a.jpg"): {"EXIF:Make": "Canon"},
                Path("/tmp/b.jpg"): {"EXIF:Make": "Nikon"},
            })
            assert len(results) == 2
            assert all(ok for ok, _ in results.values())

    def test_batch_write_partial_failure(self):
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MagicMock(returncode=0, stderr="")
            return MagicMock(returncode=2, stderr="Error")

        with (
            patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
            patch("godmode_media_library.exiftool_extract.subprocess.run", side_effect=side_effect),
        ):
            results = batch_write_tags({
                Path("/tmp/a.jpg"): {"EXIF:Make": "Canon"},
                Path("/tmp/b.jpg"): {"EXIF:Make": "Nikon"},
            })
            successes = sum(1 for ok, _ in results.values() if ok)
            failures = sum(1 for ok, _ in results.values() if not ok)
            assert successes == 1
            assert failures == 1


# ---------------------------------------------------------------------------
# extract_all_metadata edge cases
# ---------------------------------------------------------------------------


def test_extract_all_metadata_filenotfound():
    with (
        patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
        patch("godmode_media_library.exiftool_extract.subprocess.run", side_effect=FileNotFoundError),
    ):
        result = extract_all_metadata([Path("/tmp/a.jpg")])
        assert result == {}


def test_extract_all_metadata_error_returncode():
    mock_proc = MagicMock(returncode=2, stderr="Severe error", stdout="")
    with (
        patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
        patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=mock_proc),
    ):
        result = extract_all_metadata([Path("/tmp/a.jpg")])
        assert result == {}


def test_extract_all_metadata_empty_stdout():
    mock_proc = MagicMock(returncode=0, stdout="   ")
    with (
        patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
        patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=mock_proc),
    ):
        result = extract_all_metadata([Path("/tmp/a.jpg")])
        assert result == {}


def test_extract_all_metadata_dash_path():
    """Paths starting with '-' should be prefixed with './' to avoid exiftool flag confusion."""
    exiftool_output = json.dumps([{"SourceFile": "./-photo.jpg", "EXIF:Make": "Canon"}])
    mock_proc = MagicMock(returncode=0, stdout=exiftool_output)
    with (
        patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
        patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=mock_proc) as mock_run,
    ):
        extract_all_metadata([Path("-photo.jpg")])
        cmd = mock_run.call_args[0][0]
        assert "./-photo.jpg" in cmd


def test_exiftool_available_with_slash_path():
    """When bin_path contains '/', it should be used directly without shutil.which."""
    mock_result = MagicMock(returncode=0)
    with patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=mock_result):
        result = exiftool_available("/custom/path/exiftool")
        assert result == "/custom/path/exiftool"


def test_exiftool_available_nonzero_returncode():
    with (
        patch("godmode_media_library.exiftool_extract.shutil.which", return_value="/usr/bin/exiftool"),
        patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=MagicMock(returncode=1)),
    ):
        result = exiftool_available()
        assert result is None
