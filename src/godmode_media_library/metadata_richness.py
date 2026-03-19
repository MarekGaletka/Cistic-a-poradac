"""Metadata completeness scoring and diff for duplicate evaluation.

Computes a weighted richness score based on which metadata fields are present,
and produces diffs between duplicate copies to identify merge candidates.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# ── Richness scoring categories ────────────────────────────────────────
# Each category defines: (max_points, list_of_tag_suffixes_to_check)
# Tag suffixes match the part after the group prefix (e.g. "EXIF:Make" → "Make")

RICHNESS_CATEGORIES: dict[str, tuple[float, list[str]]] = {
    "datetime": (15.0, [
        "DateTimeOriginal", "CreateDate", "ModifyDate",
        "MediaCreateDate", "TrackCreateDate",
    ]),
    "camera": (10.0, [
        "Make", "Model", "LensModel", "LensInfo", "LensID",
    ]),
    "exposure": (10.0, [
        "ExposureTime", "FNumber", "ISO", "ExposureProgram",
        "MeteringMode", "Flash", "WhiteBalance",
    ]),
    "gps": (12.0, [
        "GPSLatitude", "GPSLongitude", "GPSAltitude",
        "GPSTimeStamp", "GPSDateStamp",
    ]),
    "dimensions": (5.0, [
        "ImageWidth", "ImageHeight", "XResolution", "YResolution",
    ]),
    "color": (8.0, [
        "ColorSpace", "ICCProfileName", "ProfileDescription",
        "ColorTemperature", "Gamma",
    ]),
    "xmp": (10.0, [
        "Creator", "Description", "Subject", "Rating",
        "Title", "Label",
    ]),
    "iptc": (8.0, [
        "Caption-Abstract", "Keywords", "City", "Country-PrimaryLocationName",
        "Province-State", "By-line", "CopyrightNotice",
    ]),
    "makernotes": (5.0, []),  # Special: any MakerNotes:* tag scores
    "video_audio": (10.0, [
        "Duration", "VideoCodec", "AudioCodec", "AvgBitrate",
        "VideoFrameRate", "AudioChannels", "AudioSampleRate",
        "CompressorName", "BitDepth",
    ]),
    "thumbnail": (3.0, [
        "ThumbnailLength", "ThumbnailOffset",
    ]),
    "rights": (4.0, [
        "Copyright", "CopyrightNotice", "Rights", "UsageTerms",
        "WebStatement", "Artist",
    ]),
}

MAX_RICHNESS = sum(cat[0] for cat in RICHNESS_CATEGORIES.values())


@dataclass
class MetadataRichnessScore:
    """Richness score breakdown for a file's metadata."""

    total: float = 0.0
    per_category: dict[str, float] = field(default_factory=dict)
    tag_count: int = 0


@dataclass
class MetadataDiff:
    """Diff of metadata across a duplicate group."""

    # Tags with identical values across ALL copies
    unanimous: dict[str, Any] = field(default_factory=dict)
    # Tags present in some but not all copies: tag → {path: value}
    partial: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Tags with conflicting values: tag → {path: value}
    conflicts: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Per-file richness scores
    scores: dict[str, float] = field(default_factory=dict)


def _tag_suffix(key: str) -> str:
    """Extract tag suffix from group-prefixed key (e.g. 'EXIF:Make' → 'Make')."""
    return key.split(":")[-1] if ":" in key else key


def _has_tag(meta: dict[str, Any], suffix: str) -> bool:
    """Check if any tag in meta dict ends with the given suffix."""
    return any(_tag_suffix(key) == suffix for key in meta)


def _has_makernotes(meta: dict[str, Any]) -> bool:
    """Check if any MakerNotes group tag exists."""
    for key in meta:
        if key.startswith("MakerNotes:") or key.startswith("Canon:") or key.startswith("Nikon:") or key.startswith("Sony:"):
            return True
        if "MakerNote" in key:
            return True
    return False


def compute_richness(meta: dict[str, Any]) -> MetadataRichnessScore:
    """Compute weighted richness score for a metadata dict.

    Args:
        meta: ExifTool metadata dict with group-prefixed keys.

    Returns:
        MetadataRichnessScore with total score and per-category breakdown.
    """
    result = MetadataRichnessScore(tag_count=len(meta))
    total = 0.0

    for category, (max_points, tags) in RICHNESS_CATEGORIES.items():
        if category == "makernotes":
            # Special: binary presence check
            cat_score = max_points if _has_makernotes(meta) else 0.0
        elif not tags:
            cat_score = 0.0
        else:
            present = sum(1 for tag in tags if _has_tag(meta, tag))
            cat_score = max_points * (present / len(tags))

        result.per_category[category] = round(cat_score, 2)
        total += cat_score

    result.total = round(total, 2)
    return result


def compute_group_diff(
    group: list[tuple[str, dict[str, Any]]],
) -> MetadataDiff:
    """Compute metadata diff across a group of duplicate files.

    Args:
        group: List of (path_str, metadata_dict) tuples.

    Returns:
        MetadataDiff with unanimous, partial, and conflict breakdowns.
    """
    if not group:
        return MetadataDiff()

    diff = MetadataDiff()

    # Compute per-file richness
    for path_str, meta in group:
        diff.scores[path_str] = compute_richness(meta).total

    # Collect all unique tag keys across all copies
    all_tags: set[str] = set()
    for _, meta in group:
        all_tags.update(meta.keys())

    # Classify each tag
    for tag in sorted(all_tags):
        values: dict[str, Any] = {}
        for path_str, meta in group:
            if tag in meta:
                values[path_str] = meta[tag]

        if len(values) == len(group):
            # Tag present in all copies
            unique_vals = set()
            for v in values.values():
                if isinstance(v, list):
                    unique_vals.add(tuple(sorted(str(x) for x in v)))
                else:
                    unique_vals.add(str(v))

            if len(unique_vals) == 1:
                # All copies have identical value
                diff.unanimous[tag] = next(iter(values.values()))
            else:
                # All copies have the tag but values differ
                diff.conflicts[tag] = values
        else:
            # Tag present in some copies only → merge candidate
            diff.partial[tag] = values

    return diff


def richest_file(group: list[tuple[str, dict[str, Any]]]) -> str | None:
    """Return the path of the file with highest metadata richness in a group."""
    if not group:
        return None
    scored = [(path, compute_richness(meta).total) for path, meta in group]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[0][0]


def merge_candidates(diff: MetadataDiff, survivor: str) -> dict[str, tuple[str, Any]]:
    """Identify tags that should be merged from donors into the survivor.

    Returns dict of tag → (source_path, value) for tags present in donors
    but missing from the survivor.
    """
    candidates: dict[str, tuple[str, Any]] = {}
    for tag, path_values in diff.partial.items():
        if survivor not in path_values:
            # Survivor doesn't have this tag — pick from first donor that has it
            for donor_path, value in path_values.items():
                if donor_path != survivor:
                    candidates[tag] = (donor_path, value)
                    break
    return candidates
