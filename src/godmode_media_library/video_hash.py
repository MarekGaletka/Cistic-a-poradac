"""Video perceptual hashing via keyframe extraction.

Extracts N evenly-spaced frames from a video using ffmpeg,
computes dHash for each frame, and concatenates them into
a composite fingerprint for near-duplicate video detection.
"""

from __future__ import annotations

import contextlib
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

_VIDEO_EXTS = {"mov", "mp4", "m4v", "avi", "mkv", "webm", "wmv", "3gp", "mts", "flv"}


def is_video_ext(ext: str) -> bool:
    """Return True if ext (without dot) is a hashable video type."""
    return ext.lower().lstrip(".") in _VIDEO_EXTS


def _find_ffmpeg() -> str | None:
    """Return ffmpeg binary path or None."""
    from .deps import resolve_bin
    return resolve_bin("ffmpeg")


def extract_keyframes(
    path: Path,
    n_frames: int = 8,
    *,
    ffmpeg_bin: str | None = None,
    timeout: float = 60.0,
) -> list[Path]:
    """Extract N evenly-spaced frames from a video file.

    Returns list of temporary PNG frame paths. Caller is responsible
    for cleanup (frames are in a temp directory).
    """
    binary = ffmpeg_bin or _find_ffmpeg()
    if binary is None:
        logger.debug("ffmpeg not found, cannot extract keyframes")
        return []

    tmp_dir = tempfile.mkdtemp(prefix="gml_vhash_")

    # Use select filter to extract evenly-spaced frames
    # First get duration via ffprobe-like approach
    cmd = [
        binary,
        "-i", str(path),
        "-vf", f"select=not(mod(n\\,max(1\\,floor(N/{n_frames}))))",
        "-vsync", "vfr",
        "-frames:v", str(n_frames),
        "-f", "image2",
        "-q:v", "2",
        str(Path(tmp_dir) / "frame_%03d.png"),
        "-y",
        "-loglevel", "error",
    ]

    try:
        subprocess.run(cmd, capture_output=True, timeout=timeout)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        logger.debug("ffmpeg failed for keyframe extraction: %s", path)
        return []

    frames = sorted(Path(tmp_dir).glob("frame_*.png"))
    return frames


def video_dhash(
    path: Path,
    n_frames: int = 8,
    *,
    hash_size: int = 8,
    ffmpeg_bin: str | None = None,
) -> str | None:
    """Compute composite video hash from keyframe dHash values.

    Returns concatenated hex hash string (n_frames * 16 hex chars),
    or None on failure.
    """
    from .perceptual_hash import dhash as image_dhash

    frames = extract_keyframes(path, n_frames=n_frames, ffmpeg_bin=ffmpeg_bin)
    if not frames:
        return None

    try:
        hashes = []
        for frame in frames:
            h = image_dhash(frame, hash_size=hash_size)
            if h is not None:
                hashes.append(h)

        if not hashes:
            return None

        return "".join(hashes)
    finally:
        # Cleanup temp frames
        for frame in frames:
            with contextlib.suppress(OSError):
                frame.unlink()
        with contextlib.suppress(OSError):
            frames[0].parent.rmdir()


def video_hamming_distance(hash_a: str, hash_b: str, hash_size: int = 8) -> float:
    """Compute average Hamming distance across corresponding frame pairs.

    Each frame hash is hash_size^2 / 4 hex chars (16 chars for hash_size=8).
    Compares min(frames_a, frames_b) corresponding frames.

    Returns average Hamming distance (0.0 = identical).
    """
    from .perceptual_hash import hamming_distance

    chars_per_frame = hash_size * hash_size // 4  # 16 for hash_size=8
    if not hash_a or not hash_b:
        return float("inf")

    frames_a = [hash_a[i:i + chars_per_frame] for i in range(0, len(hash_a), chars_per_frame)]
    frames_b = [hash_b[i:i + chars_per_frame] for i in range(0, len(hash_b), chars_per_frame)]

    n_compare = min(len(frames_a), len(frames_b))
    if n_compare == 0:
        return float("inf")

    total_dist = sum(hamming_distance(frames_a[i], frames_b[i]) for i in range(n_compare))
    return total_dist / n_compare
