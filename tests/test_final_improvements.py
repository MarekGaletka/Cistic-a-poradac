"""Tests for recent improvements across cli, asset_sets, perceptual_hash, web/shared, backup_monitor, and Range requests."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# 1. _format_output helper in cli.py
# ---------------------------------------------------------------------------

from godmode_media_library.cli import _format_output


class TestFormatOutput:
    def test_json_output_list_of_dicts(self):
        data = [{"name": "a", "size": 1}, {"name": "b", "size": 2}]
        result = _format_output(data, "json")
        parsed = json.loads(result)
        assert parsed == data

    def test_tsv_output_list_of_dicts(self):
        data = [{"name": "a", "size": 1}, {"name": "b", "size": 2}]
        result = _format_output(data, "tsv")
        lines = result.split("\n")
        assert lines[0] == "name\tsize"
        assert lines[1] == "a\t1"
        assert lines[2] == "b\t2"

    def test_tsv_output_custom_headers(self):
        data = [{"name": "a", "size": 1, "extra": "x"}]
        result = _format_output(data, "tsv", headers=["size", "name"])
        lines = result.split("\n")
        assert lines[0] == "size\tname"
        assert lines[1] == "1\ta"

    def test_tsv_output_single_dict(self):
        data = {"name": "a", "size": 1}
        result = _format_output(data, "tsv")
        lines = result.split("\n")
        assert lines[0] == "name\tsize"
        assert lines[1] == "a\t1"

    def test_text_format_returns_empty(self):
        data = [{"name": "a"}]
        result = _format_output(data, "text")
        assert result == ""


# ---------------------------------------------------------------------------
# 2. PILLOW_IMAGE_EXTS in asset_sets.py
# ---------------------------------------------------------------------------

from godmode_media_library.asset_sets import IMAGE_EXTS, PILLOW_IMAGE_EXTS


class TestPillowImageExts:
    def test_pillow_subset_of_image_exts(self):
        assert PILLOW_IMAGE_EXTS.issubset(IMAGE_EXTS)

    def test_pillow_no_raw_formats(self):
        raw_formats = {"dng", "cr2", "cr3", "nef", "arw", "raw"}
        assert PILLOW_IMAGE_EXTS.isdisjoint(raw_formats)

    def test_image_exts_contains_expected_formats(self):
        expected = {"bmp", "gif", "webp", "heif"}
        assert expected.issubset(IMAGE_EXTS)


# ---------------------------------------------------------------------------
# 3. perceptual_hash.py imports from asset_sets
# ---------------------------------------------------------------------------

from godmode_media_library.perceptual_hash import is_image_ext


class TestIsImageExt:
    @pytest.mark.parametrize("ext", ["jpg", "jpeg", "png", "bmp", "gif", "webp", "heic", "heif", "tif", "tiff"])
    def test_pillow_supported_returns_true(self, ext):
        assert is_image_ext(ext) is True

    @pytest.mark.parametrize("ext", ["dng", "cr2", "cr3", "nef", "arw", "raw"])
    def test_raw_formats_return_false(self, ext):
        assert is_image_ext(ext) is False


# ---------------------------------------------------------------------------
# 4. _sanitize_path path traversal rejection
# ---------------------------------------------------------------------------

from godmode_media_library.web.shared import _sanitize_path


class TestSanitizePath:
    def test_path_traversal_dot_dot_slash_rejected(self):
        with pytest.raises(Exception) as exc_info:
            _sanitize_path("/some/../etc/passwd")
        assert exc_info.value.status_code == 400

    def test_path_starting_with_dot_dot_rejected(self):
        with pytest.raises(Exception) as exc_info:
            _sanitize_path("../etc/passwd")
        assert exc_info.value.status_code == 400

    def test_normal_path_passes(self):
        result = _sanitize_path("/Users/me/photos/vacation")
        assert result == "/Users/me/photos/vacation"

    def test_null_bytes_rejected(self):
        with pytest.raises(Exception) as exc_info:
            _sanitize_path("/some/path\x00evil")
        assert exc_info.value.status_code == 400

    def test_overly_long_path_rejected(self):
        long_path = "/a" * 5000
        with pytest.raises(Exception) as exc_info:
            _sanitize_path(long_path)
        assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# 5. backup_monitor JXA notification (mock subprocess)
# ---------------------------------------------------------------------------

from godmode_media_library.backup_monitor import _send_notification


class TestSendNotification:
    @patch("platform.system", return_value="Darwin")
    @patch("godmode_media_library.backup_monitor.subprocess.run")
    @patch("godmode_media_library.backup_monitor._is_duplicate_notification", return_value=False)
    def test_osascript_called_with_javascript_flag(self, _mock_dup, mock_run, _mock_sys):
        _send_notification("Test Title", "Test message", "info")
        mock_run.assert_called_once()
        args = mock_run.call_args
        cmd = args[0][0] if args[0] else args[1].get("args", [])
        assert cmd == ["osascript", "-l", "JavaScript"]

    @patch("platform.system", return_value="Darwin")
    @patch("godmode_media_library.backup_monitor.subprocess.run")
    @patch("godmode_media_library.backup_monitor._is_duplicate_notification", return_value=False)
    def test_env_vars_set(self, _mock_dup, mock_run, _mock_sys):
        _send_notification("Backup Alert", "Disk full", "warning")
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        env = call_kwargs["env"]
        assert env["GML_NOTIFY_TITLE"] == "Backup Alert"
        assert env["GML_NOTIFY_MSG"] == "Disk full"
        assert env["GML_NOTIFY_SOUND"] == "Purr"  # "warning" -> "Purr"

    @patch("platform.system", return_value="Darwin")
    @patch("godmode_media_library.backup_monitor.subprocess.run")
    @patch("godmode_media_library.backup_monitor._is_duplicate_notification", return_value=False)
    def test_critical_sound_is_basso(self, _mock_dup, mock_run, _mock_sys):
        _send_notification("Alert", "Error!", "critical")
        env = mock_run.call_args[1]["env"]
        assert env["GML_NOTIFY_SOUND"] == "Basso"


# ---------------------------------------------------------------------------
# 6. Range request support for /stream/ endpoint
# ---------------------------------------------------------------------------

try:
    from starlette.testclient import TestClient
    from godmode_media_library.web.app import create_app
    from godmode_media_library.catalog import Catalog
    from godmode_media_library.scanner import incremental_scan
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False


@pytest.fixture
def stream_client(tmp_path):
    """Create a test client with a video file in the catalog."""
    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()

    media = tmp_path / "media"
    media.mkdir()
    video = media / "test_video.mp4"
    # 10KB fake video content
    video.write_bytes(b"V" * 10240)

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
    ):
        incremental_scan(cat, [media])
    cat.close()

    env = {"GML_API_TOKEN": "", "GML_RATE_LIMIT": "0"}
    with patch.dict(os.environ, env, clear=False):
        app = create_app(catalog_path=db_path)
    return TestClient(app, raise_server_exceptions=False), video


@pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi not installed")
class TestRangeRequests:
    def test_full_request_returns_accept_ranges(self, stream_client):
        client, video = stream_client
        resp = client.get(f"/api/stream{video}")
        assert resp.status_code == 200
        assert resp.headers.get("accept-ranges") == "bytes"

    def test_range_request_returns_206(self, stream_client):
        client, video = stream_client
        resp = client.get(f"/api/stream{video}", headers={"Range": "bytes=0-99"})
        assert resp.status_code == 206
        assert len(resp.content) == 100
        assert resp.headers["content-range"] == f"bytes 0-99/10240"
        assert resp.content == b"V" * 100

    def test_range_request_middle_segment(self, stream_client):
        client, video = stream_client
        resp = client.get(f"/api/stream{video}", headers={"Range": "bytes=100-199"})
        assert resp.status_code == 206
        assert len(resp.content) == 100
        assert resp.headers["content-range"] == "bytes 100-199/10240"

    def test_range_request_to_end(self, stream_client):
        client, video = stream_client
        resp = client.get(f"/api/stream{video}", headers={"Range": "bytes=10200-"})
        assert resp.status_code == 206
        assert len(resp.content) == 40  # 10240 - 10200
        assert resp.headers["content-range"] == "bytes 10200-10239/10240"

    def test_invalid_range_returns_416(self, stream_client):
        client, video = stream_client
        resp = client.get(f"/api/stream{video}", headers={"Range": "bytes=99999-100000"})
        assert resp.status_code == 416
