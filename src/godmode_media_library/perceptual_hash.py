"""Perceptual hashing for near-duplicate image detection.

Uses difference hash (dHash) by default — fast, orientation-sensitive,
good for photos. Requires Pillow (optional dependency).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_IMAGE_EXTS = {
    "jpg", "jpeg", "png", "bmp", "tiff", "tif", "gif", "webp",
    "heic", "heif",
}

# Hash size: 8 means 8x(8+1) = 72 pixel grid, producing 64-bit hash
HASH_SIZE = 8


def is_image_ext(ext: str) -> bool:
    """Return True if ext (without dot) is a hashable image type."""
    return ext.lower().lstrip(".") in _IMAGE_EXTS


def _check_pillow() -> bool:
    try:
        import PIL  # noqa: F401
        return True
    except ImportError:
        return False


def _check_heif() -> bool:
    try:
        import pillow_heif  # noqa: F401
        return True
    except ImportError:
        return False


def dhash(path: Path, *, hash_size: int = HASH_SIZE) -> str | None:
    """Compute difference hash (dHash) for an image file.

    Returns hex string of the hash, or None on failure.
    dHash: resize to (hash_size+1, hash_size), compare adjacent pixel brightness.
    """
    if not _check_pillow():
        logger.debug("Pillow not installed, cannot compute phash")
        return None

    from PIL import Image

    # Register HEIF opener if available
    if path.suffix.lower() in (".heic", ".heif") and _check_heif():
        import pillow_heif
        pillow_heif.register_heif_opener()

    try:
        with Image.open(path) as img:
            img = img.convert("L")  # grayscale
            img = img.resize((hash_size + 1, hash_size), Image.LANCZOS)
            get_pixels = getattr(img, "get_flattened_data", None) or img.getdata
            pixels = list(get_pixels())
    except Exception:
        logger.debug("Cannot open image for hashing: %s", path)
        return None

    # Build hash: for each row, compare pixel[x] < pixel[x+1]
    bits = []
    for y in range(hash_size):
        row_offset = y * (hash_size + 1)
        for x in range(hash_size):
            bits.append(1 if pixels[row_offset + x] < pixels[row_offset + x + 1] else 0)

    # Convert bit array to hex
    hash_int = 0
    for bit in bits:
        hash_int = (hash_int << 1) | bit
    return f"{hash_int:0{hash_size * hash_size // 4}x}"


def hamming_distance(hash_a: str, hash_b: str) -> int:
    """Compute Hamming distance between two hex hash strings."""
    if len(hash_a) != len(hash_b):
        raise ValueError(f"Hash length mismatch: {len(hash_a)} vs {len(hash_b)}")
    int_a = int(hash_a, 16)
    int_b = int(hash_b, 16)
    xor = int_a ^ int_b
    return bin(xor).count("1")


@dataclass
class SimilarPair:
    """A pair of visually similar images."""

    path_a: str
    path_b: str
    distance: int
    hash_a: str
    hash_b: str


def _is_same_hash_type(hash_a: str, hash_b: str) -> bool:
    """Check if two hashes are comparable (same length = same type)."""
    return len(hash_a) == len(hash_b)


def find_similar(
    hashes: dict[str, str],
    *,
    threshold: int = 10,
) -> list[SimilarPair]:
    """Find all pairs of files within Hamming distance threshold.

    Supports both image dHash (16 hex chars) and video composite dHash
    (N*16 hex chars). Only compares hashes of the same type/length.

    For video hashes, uses average per-frame Hamming distance.

    Args:
        hashes: Dict mapping file path → hex hash string.
        threshold: Maximum Hamming distance to consider similar.

    Returns:
        List of SimilarPair sorted by distance ascending.
    """
    items = list(hashes.items())
    pairs: list[SimilarPair] = []

    # Standard image hash length (hash_size=8 → 16 hex chars)
    image_hash_len = HASH_SIZE * HASH_SIZE // 4

    for i in range(len(items)):
        path_a, hash_a = items[i]
        for j in range(i + 1, len(items)):
            path_b, hash_b = items[j]

            # Only compare hashes of same length/type
            if not _is_same_hash_type(hash_a, hash_b):
                continue

            if len(hash_a) == image_hash_len:
                # Standard image comparison
                dist = hamming_distance(hash_a, hash_b)
            elif len(hash_a) > image_hash_len:
                # Video composite hash — average frame distance
                from .video_hash import video_hamming_distance
                dist_float = video_hamming_distance(hash_a, hash_b)
                dist = int(round(dist_float))
            else:
                continue

            if dist <= threshold:
                pairs.append(SimilarPair(
                    path_a=path_a,
                    path_b=path_b,
                    distance=dist,
                    hash_a=hash_a,
                    hash_b=hash_b,
                ))

    pairs.sort(key=lambda p: (p.distance, p.path_a))
    return pairs


def pillow_available() -> bool:
    """Check if Pillow is installed."""
    return _check_pillow()
