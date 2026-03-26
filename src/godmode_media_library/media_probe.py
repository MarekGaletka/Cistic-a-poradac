"""Video/audio metadata extraction via ffprobe."""

from __future__ import annotations

import contextlib
import json
import logging
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_MEDIA_EXTS = {
    "mov",
    "mp4",
    "m4v",
    "avi",
    "mkv",
    "wmv",
    "flv",
    "webm",
    "3gp",
    "mp3",
    "m4a",
    "wav",
    "flac",
    "ogg",
    "aac",
    "wma",
    "opus",
}

_FFPROBE_BIN: str | None = None


def _find_ffprobe() -> str | None:
    global _FFPROBE_BIN  # noqa: PLW0603
    if _FFPROBE_BIN is not None:
        return _FFPROBE_BIN
    from .deps import resolve_bin

    _FFPROBE_BIN = resolve_bin("ffprobe")
    return _FFPROBE_BIN


def is_media_ext(ext: str) -> bool:
    """Return True if ext (without dot) is a probed media type."""
    return ext.lower().lstrip(".") in _MEDIA_EXTS


@dataclass
class MediaMeta:
    """Extracted media metadata from ffprobe."""

    duration_seconds: float | None = None
    width: int | None = None
    height: int | None = None
    video_codec: str | None = None
    audio_codec: str | None = None
    bitrate: int | None = None
    audio_channels: int | None = None
    audio_sample_rate: int | None = None
    frame_rate: float | None = None


def probe_file(path: Path, *, ffprobe_bin: str | None = None, timeout: float = 30.0) -> MediaMeta | None:
    """Run ffprobe on a file and return parsed MediaMeta, or None on failure.

    Args:
        path: File to probe.
        ffprobe_bin: Override ffprobe binary path. Auto-detected if None.
        timeout: Maximum seconds to wait for ffprobe.
    """
    bin_path = ffprobe_bin or _find_ffprobe()
    if bin_path is None:
        logger.debug("ffprobe not found on PATH")
        return None

    cmd = [
        bin_path,
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(path),
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)  # noqa: S603
    except subprocess.TimeoutExpired:
        logger.warning("ffprobe timeout for %s", path)
        return None
    except FileNotFoundError:
        logger.debug("ffprobe binary not found: %s", bin_path)
        return None

    if result.returncode != 0:
        logger.debug("ffprobe failed for %s: %s", path, result.stderr[:200])
        return None

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        logger.warning("ffprobe produced invalid JSON for %s", path)
        return None

    return _parse_ffprobe(data)


def _parse_ffprobe(data: dict) -> MediaMeta:
    """Parse ffprobe JSON output into MediaMeta."""
    meta = MediaMeta()

    fmt = data.get("format", {})
    if "duration" in fmt:
        with contextlib.suppress(ValueError, TypeError):
            meta.duration_seconds = float(fmt["duration"])
    if "bit_rate" in fmt:
        with contextlib.suppress(ValueError, TypeError):
            meta.bitrate = int(fmt["bit_rate"])

    streams = data.get("streams", [])
    for stream in streams:
        codec_type = stream.get("codec_type", "")
        if codec_type == "video" and meta.video_codec is None:
            meta.video_codec = stream.get("codec_name")
            if "width" in stream:
                with contextlib.suppress(ValueError, TypeError):
                    meta.width = int(stream["width"])
            if "height" in stream:
                with contextlib.suppress(ValueError, TypeError):
                    meta.height = int(stream["height"])
            # Parse frame rate from r_frame_rate or avg_frame_rate
            fr_str = stream.get("r_frame_rate") or stream.get("avg_frame_rate")
            if fr_str and "/" in fr_str:
                parts = fr_str.split("/")
                with contextlib.suppress(ValueError, IndexError):
                    num, den = int(parts[0]), int(parts[1])
                    if den > 0:
                        meta.frame_rate = round(num / den, 3)

        elif codec_type == "audio" and meta.audio_codec is None:
            meta.audio_codec = stream.get("codec_name")
            if "channels" in stream:
                with contextlib.suppress(ValueError, TypeError):
                    meta.audio_channels = int(stream["channels"])
            if "sample_rate" in stream:
                with contextlib.suppress(ValueError, TypeError):
                    meta.audio_sample_rate = int(stream["sample_rate"])

    return meta


def ffprobe_available(ffprobe_bin: str | None = None) -> bool:
    """Check if ffprobe is available."""
    bin_path = ffprobe_bin or _find_ffprobe()
    return bin_path is not None
