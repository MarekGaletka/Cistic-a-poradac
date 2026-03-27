"""Smart media quality scoring engine.

Evaluates each file across multiple quality dimensions to produce
an overall "appeal score" (0–100).  Higher = more visually/technically
impressive → better candidate for gallery highlights and slideshow.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Score dimensions & weights ───────────────────────────────────────

# Each dimension returns 0.0–1.0; final score = weighted sum × 100.
_WEIGHTS = {
    "resolution": 0.25,  # megapixels / technical quality
    "metadata_richness": 0.15,  # how well-documented
    "file_quality": 0.10,  # size-to-resolution ratio (compression)
    "camera_tier": 0.10,  # device quality
    "geo": 0.05,  # has GPS = more interesting
    "user_signal": 0.20,  # rating + favorite + tags
    "uniqueness": 0.10,  # not a duplicate / primary in group
    "recency": 0.05,  # newer files slightly preferred
}


# ── Camera tiers (higher = better optics) ────────────────────────────

_CAMERA_TIERS: dict[str, float] = {
    # Pro cameras
    "canon eos r": 1.0,
    "canon eos 5d": 1.0,
    "canon eos-1d": 1.0,
    "nikon z": 0.95,
    "nikon d8": 1.0,
    "nikon d7": 0.9,
    "nikon d5": 0.95,
    "sony ilce-7": 1.0,
    "sony ilce-9": 1.0,
    "sony a7": 1.0,
    "sony a9": 1.0,
    "fujifilm x-t": 0.9,
    "fujifilm x-pro": 0.9,
    "fujifilm gfx": 1.0,
    "hasselblad": 1.0,
    "leica": 0.95,
    "phase one": 1.0,
    "panasonic dc-s": 0.9,
    "panasonic dc-gh": 0.85,
    "olympus e-m1": 0.85,
    "om system": 0.85,
    # Mid-range
    "canon eos r10": 0.75,
    "canon eos m": 0.7,
    "nikon d3": 0.7,
    "nikon z 30": 0.65,
    "nikon z fc": 0.7,
    "sony ilce-6": 0.75,
    # Flagship phones
    "iphone 15 pro": 0.7,
    "iphone 14 pro": 0.65,
    "iphone 13 pro": 0.6,
    "iphone 16 pro": 0.72,
    "iphone 16": 0.6,
    "iphone 15": 0.55,
    "iphone 14": 0.5,
    "iphone 13": 0.45,
    "iphone 12 pro": 0.55,
    "iphone 12": 0.4,
    "pixel 9 pro": 0.65,
    "pixel 8 pro": 0.6,
    "pixel 7 pro": 0.55,
    "samsung sm-s92": 0.65,
    "samsung sm-s91": 0.6,  # S24/S23 Ultra
    "samsung sm-s90": 0.55,
    # DJI drones
    "dji": 0.8,
    "mavic": 0.8,
    "phantom": 0.75,
    # GoPro
    "gopro": 0.55,
    # Basic phones
    "iphone se": 0.3,
    "iphone 8": 0.25,
    "iphone 7": 0.2,
}

# ── Image category thresholds ────────────────────────────────────────

_IMAGE_EXTS = {
    "jpg",
    "jpeg",
    "png",
    "tiff",
    "tif",
    "bmp",
    "webp",
    "heic",
    "heif",
    "raw",
    "cr2",
    "cr3",
    "nef",
    "arw",
    "orf",
    "rw2",
    "dng",
    "raf",
}
_VIDEO_EXTS = {
    "mp4",
    "mov",
    "avi",
    "mkv",
    "webm",
    "m4v",
    "mts",
    "3gp",
    "wmv",
    "flv",
}


@dataclass
class MediaScore:
    """Quality score breakdown for a single file."""

    path: str
    total: float = 0.0  # 0–100
    dimensions: dict[str, float] = field(default_factory=dict)
    tier: str = ""  # "masterpiece", "excellent", "good", "average", "poor"

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "total": round(self.total, 1),
            "dimensions": {k: round(v, 3) for k, v in self.dimensions.items()},
            "tier": self.tier,
        }


def _tier_label(score: float) -> str:
    if score >= 85:
        return "masterpiece"
    if score >= 70:
        return "excellent"
    if score >= 50:
        return "good"
    if score >= 30:
        return "average"
    return "poor"


# ── Dimension scorers ────────────────────────────────────────────────


def _score_resolution(width: int | None, height: int | None, ext: str) -> float:
    """Score based on megapixels. 20+ MP = 1.0, <0.3 MP = 0.0."""
    if not width or not height:
        # Videos without resolution info get a neutral score
        return 0.3 if ext.lower() in _VIDEO_EXTS else 0.0
    mp = (width * height) / 1_000_000
    if mp >= 20:
        return 1.0
    if mp >= 12:
        return 0.85
    if mp >= 8:
        return 0.7
    if mp >= 4:
        return 0.55
    if mp >= 2:
        return 0.4
    if mp >= 0.5:
        return 0.25
    return 0.1


def _score_file_quality(
    size: int,
    width: int | None,
    height: int | None,
    ext: str,
) -> float:
    """Score compression quality — bits per pixel for images, bitrate for video."""
    if ext.lower() in _VIDEO_EXTS:
        # For video, larger file per second = better quality
        # We don't have duration here, so just reward larger files
        mb = size / (1024 * 1024)
        if mb >= 500:
            return 1.0
        if mb >= 100:
            return 0.8
        if mb >= 20:
            return 0.6
        if mb >= 5:
            return 0.4
        return 0.2

    if not width or not height or width * height == 0:
        return 0.3

    # Bits per pixel — higher = less compression = better quality
    bpp = (size * 8) / (width * height)
    if bpp >= 24:  # RAW/TIFF territory
        return 1.0
    if bpp >= 8:
        return 0.85
    if bpp >= 4:
        return 0.7
    if bpp >= 2:
        return 0.55
    if bpp >= 1:
        return 0.4
    return 0.2


def _score_camera(make: str | None, model: str | None) -> float:
    """Score based on camera device tier."""
    if not make and not model:
        return 0.3  # unknown = neutral

    identifier = f"{make or ''} {model or ''}".lower().strip()

    # Check exact and prefix matches with word boundary
    import re
    for key, score in _CAMERA_TIERS.items():
        if re.search(r'(?<!\S)' + re.escape(key) + r'(?:\s|$)', identifier):
            return score

    # Known brand but unknown model
    pro_brands = {"canon", "nikon", "sony", "fujifilm", "panasonic", "olympus", "leica"}
    for brand in pro_brands:
        if brand in identifier:
            return 0.6

    return 0.35


def _score_geo(lat: float | None, lng: float | None) -> float:
    """Having GPS data makes a photo more interesting/documented."""
    if lat is not None and lng is not None:
        return 1.0
    return 0.0


def _score_richness(richness: float | None) -> float:
    """Normalize metadata_richness (0-100+) to 0-1."""
    if richness is None:
        return 0.0
    return min(richness / 80.0, 1.0)


def _score_user_signal(
    rating: int | None,
    is_favorite: bool,
    tag_count: int,
    has_note: bool,
) -> float:
    """User-provided quality signals are the strongest indicator."""
    score = 0.0

    # Rating is the strongest signal (0-0.6)
    if rating is not None:
        score += (rating / 5.0) * 0.6

    # Favorite (0.25)
    if is_favorite:
        score += 0.25

    # Tags show curation effort (0.1)
    if tag_count > 0:
        score += min(tag_count * 0.03, 0.1)

    # Notes (0.05)
    if has_note:
        score += 0.05

    return min(score, 1.0)


def _score_uniqueness(duplicate_group_id: int | None, is_primary: bool | None) -> float:
    """Unique files or primary copies score higher."""
    if duplicate_group_id is None:
        return 1.0  # not a duplicate at all
    if is_primary:
        return 0.8  # primary in a duplicate group
    return 0.2  # secondary duplicate


def _score_recency(date_original: str | None, mtime: float | None) -> float:
    """Slightly prefer recent content (last 2 years = 1.0, >10 years = 0.3)."""
    import time

    ts = None
    if date_original:
        try:
            from datetime import datetime

            dt = datetime.fromisoformat(date_original.replace(":", "-", 2).replace("Z", "+00:00"))
            ts = dt.timestamp()
        except (ValueError, TypeError):
            pass
    if ts is None and mtime:
        ts = mtime

    if ts is None:
        return 0.3

    age_years = (time.time() - ts) / (365.25 * 86400)
    if age_years <= 0.5:
        return 1.0
    if age_years <= 2:
        return 0.85
    if age_years <= 5:
        return 0.65
    if age_years <= 10:
        return 0.45
    return 0.3


# ── Main scoring function ────────────────────────────────────────────


def score_file(row: dict) -> MediaScore:
    """Compute appeal score for a single file row from the catalog."""
    ext = row.get("ext") or Path(row.get("path", "")).suffix.lstrip(".") or ""
    ext = ext.lstrip(".").lower()

    dims = {
        "resolution": _score_resolution(
            row.get("width"),
            row.get("height"),
            ext,
        ),
        "metadata_richness": _score_richness(row.get("metadata_richness")),
        "file_quality": _score_file_quality(
            row.get("size") or 0,
            row.get("width"),
            row.get("height"),
            ext,
        ),
        "camera_tier": _score_camera(
            row.get("camera_make"),
            row.get("camera_model"),
        ),
        "geo": _score_geo(
            row.get("gps_latitude"),
            row.get("gps_longitude"),
        ),
        "user_signal": _score_user_signal(
            row.get("rating"),
            bool(row.get("is_favorite")),
            row.get("tag_count") or 0,
            bool(row.get("has_note")),
        ),
        "uniqueness": _score_uniqueness(
            row.get("duplicate_group_id"),
            row.get("is_primary"),
        ),
        "recency": _score_recency(
            row.get("date_original"),
            row.get("mtime"),
        ),
    }

    total = sum(dims[k] * _WEIGHTS[k] for k in _WEIGHTS) * 100
    tier = _tier_label(total)

    return MediaScore(path=row["path"], total=total, dimensions=dims, tier=tier)


# ── Batch scoring from catalog ───────────────────────────────────────


def score_catalog(
    db_path: str | Path,
    *,
    media_only: bool = True,
    min_score: float = 0.0,
    limit: int = 200,
) -> list[MediaScore]:
    """Score all (media) files in the catalog, return top N by score.

    Parameters
    ----------
    db_path : path to catalog.db
    media_only : if True, only score image + video files
    min_score : filter out files below this score
    limit : max results to return
    """
    with sqlite3.connect(str(db_path)) as db:
        db.row_factory = sqlite3.Row

        # Build query with all needed columns
        query = """
            SELECT
                f.path, f.ext, f.size, f.mtime,
                f.width, f.height, f.bitrate,
                f.date_original, f.camera_make, f.camera_model,
                f.gps_latitude, f.gps_longitude,
                f.metadata_richness,
                f.sha256, f.phash,
                d.group_id AS duplicate_group_id,
                d.is_primary,
                fr.rating,
                fn.note IS NOT NULL AS has_note,
                (SELECT COUNT(*) FROM file_tags ft WHERE ft.file_id = f.id) AS tag_count
            FROM files f
            LEFT JOIN duplicates d ON d.file_id = f.id
            LEFT JOIN file_ratings fr ON fr.file_id = f.id
            LEFT JOIN file_notes fn ON fn.file_id = f.id
        """

        if media_only:
            all_exts = _IMAGE_EXTS | _VIDEO_EXTS
            placeholders = ",".join("?" for _ in all_exts)
            query += f" WHERE LOWER(f.ext) IN ({placeholders})"
            params: list = list(all_exts)
        else:
            params = []

        rows = db.execute(query, params).fetchall()

    # Score all files
    scores: list[MediaScore] = []
    for row in rows:
        row_dict = dict(row)
        ms = score_file(row_dict)
        if ms.total >= min_score:
            scores.append(ms)

    # Sort by score descending
    scores.sort(key=lambda s: s.total, reverse=True)
    return scores[:limit]


def get_smart_collections(
    db_path: str | Path,
) -> dict[str, list[dict]]:
    """Generate auto-curated collections.

    Returns dict with collection names as keys, each value is a list of
    {path, score, tier} dicts.
    """
    all_scores = score_catalog(db_path, limit=5000, min_score=0)

    collections: dict[str, list[dict]] = {}

    # Best of — top 50 overall
    collections["best_of"] = [s.to_dict() for s in all_scores[:50]]

    # Masterpieces — score >= 85
    collections["masterpieces"] = [s.to_dict() for s in all_scores if s.tier == "masterpiece"][:30]

    # Top rated — files with user rating 4+
    collections["top_rated"] = [s.to_dict() for s in all_scores if s.dimensions.get("user_signal", 0) >= 0.5][:30]

    # Travel — files with GPS data, sorted by score
    collections["travel"] = [s.to_dict() for s in all_scores if s.dimensions.get("geo", 0) > 0][:40]

    # Pro shots — high camera tier + high resolution
    collections["pro_shots"] = [
        s.to_dict() for s in all_scores if s.dimensions.get("camera_tier", 0) >= 0.55 and s.dimensions.get("resolution", 0) >= 0.7
    ][:30]

    # Recent highlights — last 6 months, score >= 40
    collections["recent"] = [s.to_dict() for s in all_scores if s.dimensions.get("recency", 0) >= 0.85 and s.total >= 40][:30]

    # Hidden gems — decent quality but no user interaction
    collections["hidden_gems"] = [s.to_dict() for s in all_scores if s.dimensions.get("user_signal", 0) == 0 and s.total >= 45][:30]

    return collections
