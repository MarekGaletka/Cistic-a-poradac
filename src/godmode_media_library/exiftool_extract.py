"""Comprehensive metadata extraction via ExifTool.

ExifTool is the gold standard for reading metadata — 500+ tag types
across ALL image/video/audio formats including HEIC, CR2, NEF, ARW, MOV.
This module provides batch extraction with graceful degradation.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 200

# Tags to exclude from extraction (binary blobs, too large, internal)
_EXCLUDE_TAGS = {
    "ThumbnailImage",
    "PreviewImage",
    "JpgFromRaw",
    "OtherImage",
    "ThumbnailTIFF",
    "PreviewTIFF",
}


def exiftool_available(bin_path: str = "exiftool") -> str | None:
    """Return resolved ExifTool binary path, or None if not installed."""
    resolved = shutil.which(bin_path) if "/" not in bin_path else bin_path
    if resolved is None:
        return None
    try:
        result = subprocess.run(
            [resolved, "-ver"], capture_output=True, text=True, timeout=10,
        )  # noqa: S603
        if result.returncode == 0:
            return resolved
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def extract_all_metadata(
    paths: list[Path],
    *,
    bin_path: str = "exiftool",
    timeout: float = 120.0,
) -> dict[Path, dict[str, Any]]:
    """Batch-extract ALL metadata from files using ExifTool.

    Returns dict mapping each path to its full metadata dict.
    Tag keys are prefixed with group name (e.g. 'EXIF:Make', 'XMP:Creator').
    If ExifTool is not available, returns empty dict with a warning.

    Args:
        paths: Files to extract metadata from.
        bin_path: ExifTool binary path or name.
        timeout: Timeout per batch in seconds.
    """
    if not paths:
        return {}

    binary = exiftool_available(bin_path)
    if binary is None:
        logger.warning("ExifTool not available — skipping deep metadata extraction")
        return {}

    result: dict[Path, dict[str, Any]] = {}

    for chunk in _chunks(paths, _CHUNK_SIZE):
        cmd = [
            binary,
            "-j",               # JSON output
            "-n",               # Numeric values (GPS as float, not DMS)
            "-G1",              # Group 1 prefixes (EXIF:, XMP:, IPTC:, etc.)
            "-q", "-q",         # Suppress all warnings
            "-api", "LargeFileSupport=1",
            "-b",               # Include binary tag indicators
            "--ThumbnailImage",  # Exclude large binary blobs
            "--PreviewImage",
            "--JpgFromRaw",
            "--OtherImage",
        ]
        cmd.extend(str(p) for p in chunk)

        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout,
            )  # noqa: S603
        except subprocess.TimeoutExpired:
            logger.warning("ExifTool timeout for batch of %d files", len(chunk))
            continue
        except FileNotFoundError:
            logger.warning("ExifTool binary not found: %s", binary)
            return result

        if proc.returncode not in (0, 1):
            # returncode 1 = minor warnings, still valid output
            logger.warning("ExifTool error (rc=%d): %s", proc.returncode, proc.stderr[:200])
            continue

        if not proc.stdout.strip():
            continue

        try:
            rows = json.loads(proc.stdout)
        except json.JSONDecodeError:
            logger.warning("Failed to parse ExifTool JSON output for batch")
            continue

        for row in rows:
            if not isinstance(row, dict):
                continue
            src = row.get("SourceFile")
            if not isinstance(src, str):
                continue
            path = Path(src).expanduser().resolve()
            # Remove SourceFile from metadata dict
            meta = {k: v for k, v in row.items() if k != "SourceFile"}
            # Filter out excluded binary tags and None values
            meta = {k: v for k, v in meta.items() if v is not None and k.split(":")[-1] not in _EXCLUDE_TAGS}
            result[path] = meta

    logger.info("Extracted metadata for %d / %d files via ExifTool", len(result), len(paths))
    return result


def extract_single(
    path: Path,
    *,
    bin_path: str = "exiftool",
) -> dict[str, Any]:
    """Extract all metadata from a single file. Returns empty dict on failure."""
    result = extract_all_metadata([path], bin_path=bin_path)
    return result.get(path.expanduser().resolve(), {})
