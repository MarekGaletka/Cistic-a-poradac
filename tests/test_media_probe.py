from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from godmode_media_library.media_probe import (
    MediaMeta,
    _parse_ffprobe,
    ffprobe_available,
    is_media_ext,
    probe_file,
)


def test_is_media_ext_video():
    assert is_media_ext("mov")
    assert is_media_ext("mp4")
    assert is_media_ext("mkv")
    assert is_media_ext("MOV")


def test_is_media_ext_audio():
    assert is_media_ext("mp3")
    assert is_media_ext("flac")
    assert is_media_ext("ogg")


def test_is_media_ext_not_media():
    assert not is_media_ext("jpg")
    assert not is_media_ext("pdf")
    assert not is_media_ext("txt")


def test_parse_ffprobe_video():
    data = {
        "format": {
            "duration": "120.5",
            "bit_rate": "5000000",
        },
        "streams": [
            {
                "codec_type": "video",
                "codec_name": "h264",
                "width": 1920,
                "height": 1080,
                "r_frame_rate": "30000/1001",
            },
            {
                "codec_type": "audio",
                "codec_name": "aac",
                "channels": 2,
                "sample_rate": "44100",
            },
        ],
    }
    meta = _parse_ffprobe(data)
    assert meta.duration_seconds == 120.5
    assert meta.bitrate == 5000000
    assert meta.width == 1920
    assert meta.height == 1080
    assert meta.video_codec == "h264"
    assert meta.audio_codec == "aac"
    assert meta.audio_channels == 2
    assert meta.audio_sample_rate == 44100
    assert meta.frame_rate is not None
    assert abs(meta.frame_rate - 29.97) < 0.01


def test_parse_ffprobe_audio_only():
    data = {
        "format": {"duration": "240.0"},
        "streams": [
            {"codec_type": "audio", "codec_name": "mp3", "channels": 2, "sample_rate": "48000"},
        ],
    }
    meta = _parse_ffprobe(data)
    assert meta.duration_seconds == 240.0
    assert meta.audio_codec == "mp3"
    assert meta.video_codec is None
    assert meta.width is None


def test_parse_ffprobe_empty():
    meta = _parse_ffprobe({"format": {}, "streams": []})
    assert meta.duration_seconds is None
    assert meta.video_codec is None


def test_parse_ffprobe_invalid_values():
    data = {
        "format": {"duration": "invalid", "bit_rate": "notanumber"},
        "streams": [
            {"codec_type": "video", "codec_name": "h264", "width": "bad", "height": "bad"},
        ],
    }
    meta = _parse_ffprobe(data)
    assert meta.duration_seconds is None
    assert meta.bitrate is None
    assert meta.width is None


def test_probe_file_no_ffprobe():
    with patch("godmode_media_library.media_probe._find_ffprobe", return_value=None):
        result = probe_file(Path("/tmp/test.mp4"))
        assert result is None


def test_probe_file_ffprobe_failure():
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "error"
    with patch("godmode_media_library.media_probe._find_ffprobe", return_value="/usr/bin/ffprobe"), \
         patch("subprocess.run", return_value=mock_result):
        result = probe_file(Path("/tmp/test.mp4"))
        assert result is None


def test_probe_file_success():
    import json

    ffprobe_output = json.dumps({
        "format": {"duration": "10.0", "bit_rate": "1000000"},
        "streams": [{"codec_type": "video", "codec_name": "h264", "width": 640, "height": 480, "r_frame_rate": "25/1"}],
    })
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = ffprobe_output
    with patch("godmode_media_library.media_probe._find_ffprobe", return_value="/usr/bin/ffprobe"), \
         patch("subprocess.run", return_value=mock_result):
        result = probe_file(Path("/tmp/test.mp4"))
        assert result is not None
        assert result.duration_seconds == 10.0
        assert result.video_codec == "h264"


def test_ffprobe_available_found():
    with patch("godmode_media_library.media_probe._find_ffprobe", return_value="/usr/bin/ffprobe"):
        assert ffprobe_available() is True


def test_ffprobe_available_not_found():
    with patch("godmode_media_library.media_probe._find_ffprobe", return_value=None):
        assert ffprobe_available() is False


def test_media_meta_defaults():
    m = MediaMeta()
    assert m.duration_seconds is None
    assert m.width is None
    assert m.frame_rate is None
