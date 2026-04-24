from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

IMAGE_EXTS = {"heic", "heif", "jpg", "jpeg", "png", "tif", "tiff", "bmp", "gif", "webp", "dng", "cr2", "cr3", "nef", "arw", "raw", "orf", "rw2", "raf", "svg"}
# Dot-prefixed variant for suffix matching (e.g. Path.suffix == ".jpg")
IMAGE_EXTS_DOTTED = {"." + e for e in IMAGE_EXTS}
# Subset of IMAGE_EXTS that Pillow can decode natively (or via pillow-heif).
# RAW camera formats (dng, cr2, cr3, nef, arw, raw, orf, rw2, raf) need a third-party raw processor.
PILLOW_IMAGE_EXTS = {"jpg", "jpeg", "png", "bmp", "tiff", "tif", "gif", "webp", "heic", "heif"}
VIDEO_EXTS = {"mov", "mp4", "m4v", "avi", "mkv"}
SIDECAR_EXTS = {"aae", "xmp", "lrv", "thm", "srt", "json"}
ASSET_EXTS = IMAGE_EXTS | VIDEO_EXTS | SIDECAR_EXTS


def asset_key_for_path(path: Path) -> str:
    return f"{path.parent}\t{path.stem}"


def build_asset_membership(paths: Iterable[Path]) -> tuple[dict[Path, str], dict[Path, bool], dict[str, set[str]]]:
    key_to_exts: dict[str, set[str]] = defaultdict(set)
    path_to_key: dict[Path, str] = {}

    for path in paths:
        ext = path.suffix.lower().lstrip(".")
        if ext not in ASSET_EXTS:
            continue
        key = asset_key_for_path(path)
        path_to_key[path] = key
        key_to_exts[key].add(ext)

    path_is_component: dict[Path, bool] = {}
    for path, key in path_to_key.items():
        exts = key_to_exts[key]
        has_image = any(e in IMAGE_EXTS for e in exts)
        has_video = any(e in VIDEO_EXTS for e in exts)
        has_sidecar = any(e in SIDECAR_EXTS for e in exts)
        # We treat anything with multiple related members as an atomic set.
        path_is_component[path] = len(exts) > 1 and (has_image or has_video or has_sidecar)

    return path_to_key, path_is_component, key_to_exts


def summarize_asset_sets(key_to_exts: dict[str, set[str]]) -> dict[str, int]:
    total = len(key_to_exts)
    live_pairs = 0
    live_pairs_with_aae = 0
    with_sidecar = 0
    only_img = 0
    only_video = 0

    for exts in key_to_exts.values():
        has_img = any(e in IMAGE_EXTS for e in exts)
        has_video = any(e in VIDEO_EXTS for e in exts)
        has_aae = "aae" in exts
        has_sc = any(e in SIDECAR_EXTS for e in exts)

        if has_sc:
            with_sidecar += 1
        if has_img and has_video:
            live_pairs += 1
            if has_aae:
                live_pairs_with_aae += 1
        elif has_img and not has_video:
            only_img += 1
        elif has_video and not has_img:
            only_video += 1

    return {
        "asset_sets_total": total,
        "live_pairs_img_plus_video": live_pairs,
        "live_pairs_with_aae": live_pairs_with_aae,
        "sets_with_any_sidecar": with_sidecar,
        "only_image_sets": only_img,
        "only_video_sets": only_video,
    }
