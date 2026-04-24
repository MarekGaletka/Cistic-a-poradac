"""Image quality scoring and smart classification.

Analyzes images for blur, brightness, and classifies them into categories
(photo, screenshot, meme, document, video) using Pillow-based heuristics.
"""

from __future__ import annotations

import contextlib
import logging
import os
from dataclasses import dataclass

from .asset_sets import PILLOW_IMAGE_EXTS

logger = logging.getLogger(__name__)

# ── Known screen resolutions (width, height) ────────────────────────

_SCREEN_RESOLUTIONS: set[tuple[int, int]] = {
    # iPhones
    (1170, 2532),
    (2532, 1170),
    (1284, 2778),
    (2778, 1284),
    (1179, 2556),
    (2556, 1179),
    (1290, 2796),
    (2796, 1290),
    (1125, 2436),
    (2436, 1125),
    (1242, 2688),
    (2688, 1242),
    (750, 1334),
    (1334, 750),
    (1080, 2340),
    (2340, 1080),
    (1440, 3200),
    (3200, 1440),
    (1440, 3120),
    (3120, 1440),
    (1440, 3168),
    (3168, 1440),
    # Common desktop
    (1920, 1080),
    (1080, 1920),
    (2560, 1440),
    (1440, 2560),
    (3840, 2160),
    (2160, 3840),
    (1366, 768),
    (768, 1366),
    (1536, 2048),
    (2048, 1536),
    (2048, 2732),
    (2732, 2048),
    (2560, 1600),
    (1600, 2560),
    (2880, 1800),
    (1800, 2880),
    (3024, 1964),
    (1964, 3024),
    (3456, 2234),
    (2234, 3456),
    # Retina MacBook
    (2560, 1664),
    (1664, 2560),
    (3024, 1890),
    (1890, 3024),
}

# Common social media aspect ratios (w/h) for meme detection
_MEME_RATIOS = {1.0, 4 / 5, 5 / 4, 16 / 9, 9 / 16, 1.91}
_MEME_RATIO_TOLERANCE = 0.05

# Laplacian-like 3x3 kernel for edge/blur detection
_LAPLACIAN_KERNEL = (0, 1, 0, 1, -4, 1, 0, 1, 0)

# Size threshold for meme detection (100KB)
_MEME_SIZE_THRESHOLD = 100 * 1024

# Video extensions (classified without analysis)
VIDEO_EXTS = {"mp4", "mov", "avi", "mkv", "wmv", "flv", "webm", "m4v", "3gp", "mts"}

# Document extensions
DOC_EXTS = {"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp"}


@dataclass
class QualityInfo:
    blur_score: float = 0.0
    brightness: float = 128.0
    is_blurry: bool = False
    is_dark: bool = False
    is_overexposed: bool = False
    is_screenshot: bool = False
    is_meme: bool = False
    category: str = "photo"  # photo, screenshot, meme, document, video


def _compute_blur_score(img) -> float:
    """Compute Laplacian variance as a sharpness metric using Pillow.

    High values indicate sharp images, low values indicate blur.
    """
    from PIL import ImageFilter

    gray = img.convert("L")
    # Apply Laplacian-like kernel
    kernel = ImageFilter.Kernel(
        size=(3, 3),
        kernel=_LAPLACIAN_KERNEL,
        scale=1,
        offset=128,  # shift to avoid negative clipping
    )
    filtered = gray.filter(kernel)

    # Compute variance of filtered pixels
    _ = filtered.getextrema()  # (min, max) — cheap sanity check
    pixels = list(filtered.tobytes())
    n = len(pixels)
    if n == 0:
        return 0.0
    mean = sum(pixels) / n
    variance = sum((p - mean) ** 2 for p in pixels) / n
    return variance


def _compute_brightness(img) -> float:
    """Compute average pixel brightness (0-255)."""
    gray = img.convert("L")
    pixels = list(gray.tobytes())
    if not pixels:
        return 128.0
    return sum(pixels) / len(pixels)


def _is_meme_ratio(width: int, height: int) -> bool:
    """Check if dimensions match common social media aspect ratios."""
    if height == 0:
        return False
    ratio = width / height
    return any(abs(ratio - target) < _MEME_RATIO_TOLERANCE for target in _MEME_RATIOS)


def analyze_image_quality(
    path: str,
    width: int = 0,
    height: int = 0,
    size: int = 0,
    camera_make: str | None = None,
) -> QualityInfo:
    """Analyze a single image for quality metrics.

    Parameters
    ----------
    path : str
        File path to the image.
    width, height : int
        Pre-known dimensions (from catalog). If 0, read from image.
    size : int
        File size in bytes. If 0, read from filesystem.
    camera_make : str or None
        EXIF camera make (from catalog).

    Returns
    -------
    QualityInfo
        Quality metrics and classification.
    """
    info = QualityInfo()

    # Get file size if not provided
    if size == 0:
        with contextlib.suppress(OSError):
            size = os.path.getsize(path)

    # Check extension for non-image types
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    if ext in VIDEO_EXTS:
        info.category = "video"
        return info
    if ext in DOC_EXTS:
        info.category = "document"
        return info

    try:
        from PIL import Image

        with Image.open(path) as img:
            # Get dimensions
            if width == 0 or height == 0:
                width, height = img.size

            # Blur detection — downsample large images for speed
            analyze_img = img
            if width > 1000 or height > 1000:
                ratio = min(1000 / max(width, 1), 1000 / max(height, 1))
                new_w = max(int(width * ratio), 1)
                new_h = max(int(height * ratio), 1)
                analyze_img = img.resize((new_w, new_h))

            info.blur_score = _compute_blur_score(analyze_img)
            info.brightness = _compute_brightness(analyze_img)

    except Exception as exc:
        logger.debug("Cannot analyze image %s: %s", path, exc)
        return info

    # Thresholds
    info.is_blurry = info.blur_score < 50
    info.is_dark = info.brightness < 40
    info.is_overexposed = info.brightness > 220

    # Screenshot detection: exact screen resolution AND no camera EXIF
    has_camera = bool(camera_make and camera_make.strip())
    if (width, height) in _SCREEN_RESOLUTIONS and not has_camera:
        info.is_screenshot = True

    # Meme detection: small file, social media ratio, no EXIF
    if size > 0 and size < _MEME_SIZE_THRESHOLD and _is_meme_ratio(width, height) and not has_camera:
        info.is_meme = True

    # Category assignment (priority order)
    if info.is_screenshot:
        info.category = "screenshot"
    elif info.is_meme:
        info.category = "meme"
    else:
        info.category = "photo"

    return info


def batch_analyze(catalog, *, limit: int = 0, progress_fn=None) -> dict:
    """Analyze all unanalyzed image files in catalog.

    Parameters
    ----------
    catalog : Catalog
        Open catalog instance.
    limit : int
        Maximum files to process (0 = unlimited).
    progress_fn : callable or None
        Called with (done, total) during processing.

    Returns
    -------
    dict
        Summary stats: {analyzed, photo, screenshot, meme, blurry, dark, overexposed, errors}.
    """
    files = catalog.files_without_quality(PILLOW_IMAGE_EXTS)
    if limit > 0:
        files = files[:limit]

    total = len(files)
    stats = {
        "analyzed": 0,
        "photo": 0,
        "screenshot": 0,
        "meme": 0,
        "blurry": 0,
        "dark": 0,
        "overexposed": 0,
        "errors": 0,
    }

    for i, (file_id, path, width, height, size, camera_make) in enumerate(files):
        try:
            info = analyze_image_quality(
                path,
                width=width or 0,
                height=height or 0,
                size=size or 0,
                camera_make=camera_make,
            )
            catalog.update_quality(file_id, info.blur_score, info.brightness, info.category)
            stats["analyzed"] += 1
            stats[info.category] = stats.get(info.category, 0) + 1
            if info.is_blurry:
                stats["blurry"] += 1
            if info.is_dark:
                stats["dark"] += 1
            if info.is_overexposed:
                stats["overexposed"] += 1
        except Exception as exc:
            logger.debug("Error analyzing %s: %s", path, exc)
            stats["errors"] += 1

        if progress_fn and (i + 1) % 10 == 0:
            progress_fn(i + 1, total)

    # Commit all changes
    catalog.commit()

    # Final progress callback
    if progress_fn:
        progress_fn(total, total)

    return stats
