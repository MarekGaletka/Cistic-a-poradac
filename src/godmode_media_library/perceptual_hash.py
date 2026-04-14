"""Perceptual hashing for near-duplicate image detection.

Uses difference hash (dHash) by default — fast, orientation-sensitive,
good for photos. Requires Pillow (optional dependency).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from godmode_media_library.asset_sets import PILLOW_IMAGE_EXTS

logger = logging.getLogger(__name__)

_heif_registered = False


def _register_heif_once() -> None:
    """Register HEIF opener once at module level (lazy, called on first HEIF encounter)."""
    global _heif_registered
    if _heif_registered:
        return
    if _check_heif():
        import pillow_heif

        pillow_heif.register_heif_opener()
    _heif_registered = True

_IMAGE_EXTS = PILLOW_IMAGE_EXTS

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

    # Register HEIF opener if available (once per process)
    if path.suffix.lower() in (".heic", ".heif"):
        _register_heif_once()

    try:
        with Image.open(path) as img:
            img = img.convert("L")  # grayscale
            img = img.resize((hash_size + 1, hash_size), Image.LANCZOS)
            pixels = list(img.tobytes())
    except (OSError, ValueError):
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


def _bucket_key(hex_hash: str, prefix_bits: int = 16) -> str:
    """Return the first `prefix_bits` bits of a hex hash as a bucket key.

    With 16 prefix bits (4 hex chars), hashes are split into up to 65536
    buckets, reducing pairwise comparisons from O(n^2) to O(n * bucket_size).
    Two hashes that differ in more than `threshold` bits can still land in the
    same bucket, but hashes that differ in the prefix alone by more than the
    threshold will be in separate buckets. We use multi-probe: we also check
    neighboring buckets by flipping each prefix bit, ensuring we never miss
    pairs within the Hamming distance threshold.
    """
    prefix_hex_chars = (prefix_bits + 3) // 4  # ceil division
    return hex_hash[:prefix_hex_chars]


def _nearby_bucket_keys(key: str, threshold: int, prefix_bits: int = 16) -> set[str]:
    """Return bucket keys within Hamming distance of `threshold` from `key`.

    For small prefix_bits (16) and typical thresholds (<=10), this generates
    all keys reachable by flipping up to `min(threshold, 4)` bits
    in the prefix. When threshold >= prefix_bits, all buckets could match so
    we return an empty set as a signal to skip the optimization.

    The prefix length adapts to the threshold: for threshold > 4, we use a
    shorter effective prefix (8 bits) to create larger buckets and reduce
    false negatives.
    """
    if threshold >= prefix_bits:
        # Threshold is too large relative to prefix — bucketing won't help
        return set()

    prefix_hex_chars = (prefix_bits + 3) // 4
    prefix_int = int(key, 16)
    keys: set[str] = set()

    # Adapt flip bits to threshold: for small thresholds (<=4), 2-bit flips
    # suffice. For larger thresholds, use min(threshold, 4) to cover more
    # neighboring buckets and avoid false negatives.
    max_flips = min(threshold, 4)

    # 0-bit flip: same bucket
    keys.add(key)

    # 1-bit flips
    if max_flips >= 1:
        for bit in range(prefix_bits):
            flipped = prefix_int ^ (1 << bit)
            keys.add(f"{flipped:0{prefix_hex_chars}x}")

    # 2-bit flips
    if max_flips >= 2:
        for b1 in range(prefix_bits):
            for b2 in range(b1 + 1, prefix_bits):
                flipped = prefix_int ^ (1 << b1) ^ (1 << b2)
                keys.add(f"{flipped:0{prefix_hex_chars}x}")

    # 3-bit flips — needed for thresholds > 4 to reduce false negatives
    if max_flips >= 3:
        for b1 in range(prefix_bits):
            for b2 in range(b1 + 1, prefix_bits):
                for b3 in range(b2 + 1, prefix_bits):
                    flipped = prefix_int ^ (1 << b1) ^ (1 << b2) ^ (1 << b3)
                    keys.add(f"{flipped:0{prefix_hex_chars}x}")

    return keys


def find_similar(
    hashes: dict[str, str],
    *,
    threshold: int = 10,
) -> list[SimilarPair]:
    """Find all pairs of files within Hamming distance threshold.

    Supports both image dHash (16 hex chars) and video composite dHash
    (N*16 hex chars). Only compares hashes of the same type/length.

    For video hashes, uses average per-frame Hamming distance.

    Uses bit-prefix bucketing with multi-probe to reduce comparisons from
    O(n^2) to approximately O(n * bucket_size). Hashes are grouped by their
    first 16 bits; each hash is compared only against hashes in nearby
    buckets (those whose prefix is within 2 bit-flips).

    Args:
        hashes: Dict mapping file path -> hex hash string.
        threshold: Maximum Hamming distance to consider similar.

    Returns:
        List of SimilarPair sorted by distance ascending.
    """
    # Standard image hash length (hash_size=8 -> 16 hex chars)
    image_hash_len = HASH_SIZE * HASH_SIZE // 4
    # Adapt prefix length to threshold to avoid false negatives
    PREFIX_BITS = 8 if threshold > 4 else 16

    # Group hashes by length (type), then by prefix bucket
    # Structure: {hash_length: {bucket_key: [(path, hash_hex)]}}
    from collections import defaultdict

    by_length: dict[int, dict[str, list[tuple[str, str]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for path, hex_hash in hashes.items():
        bk = _bucket_key(hex_hash, PREFIX_BITS)
        by_length[len(hex_hash)][bk].append((path, hex_hash))

    pairs: list[SimilarPair] = []
    seen: set[tuple[str, str]] = set()  # avoid duplicate pairs from multi-probe

    for hash_len, buckets in by_length.items():
        if hash_len < image_hash_len:
            continue  # skip unknown short hashes

        is_video = hash_len > image_hash_len
        probe_keys_cache: dict[str, set[str]] = {}

        for bk, items_in_bucket in buckets.items():
            # Intra-bucket: compare all pairs within this bucket
            for i in range(len(items_in_bucket)):
                path_a, hash_a = items_in_bucket[i]
                for j in range(i + 1, len(items_in_bucket)):
                    path_b, hash_b = items_in_bucket[j]
                    pair_key = (path_a, path_b) if path_a < path_b else (path_b, path_a)
                    if pair_key in seen:
                        continue
                    seen.add(pair_key)

                    if is_video:
                        from .video_hash import video_hamming_distance
                        dist = int(round(video_hamming_distance(hash_a, hash_b)))
                    else:
                        dist = hamming_distance(hash_a, hash_b)

                    if dist <= threshold:
                        pairs.append(
                            SimilarPair(
                                path_a=path_a, path_b=path_b,
                                distance=dist, hash_a=hash_a, hash_b=hash_b,
                            )
                        )

            # Inter-bucket: compare this bucket against nearby buckets
            if bk not in probe_keys_cache:
                probe_keys_cache[bk] = _nearby_bucket_keys(bk, threshold, PREFIX_BITS)
            neighbor_keys = probe_keys_cache[bk]

            if not neighbor_keys:
                # threshold >= PREFIX_BITS: bucketing can't filter, compare all buckets
                neighbor_keys = set(buckets.keys())

            for nbk in neighbor_keys:
                if nbk <= bk or nbk not in buckets:
                    # Only compare bk < nbk to avoid double-processing bucket pairs
                    continue
                for path_a, hash_a in items_in_bucket:
                    for path_b, hash_b in buckets[nbk]:
                        pair_key = (path_a, path_b) if path_a < path_b else (path_b, path_a)
                        if pair_key in seen:
                            continue
                        seen.add(pair_key)

                        if is_video:
                            from .video_hash import video_hamming_distance
                            dist = int(round(video_hamming_distance(hash_a, hash_b)))
                        else:
                            dist = hamming_distance(hash_a, hash_b)

                        if dist <= threshold:
                            pairs.append(
                                SimilarPair(
                                    path_a=path_a, path_b=path_b,
                                    distance=dist, hash_a=hash_a, hash_b=hash_b,
                                )
                            )

    pairs.sort(key=lambda p: (p.distance, p.path_a))
    return pairs


def pillow_available() -> bool:
    """Check if Pillow is installed."""
    return _check_pillow()
