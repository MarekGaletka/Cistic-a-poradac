from __future__ import annotations

from pathlib import Path

from godmode_media_library.asset_sets import (
    IMAGE_EXTS,
    IMAGE_EXTS_DOTTED,
    PILLOW_IMAGE_EXTS,
    VIDEO_EXTS,
    asset_key_for_path,
    build_asset_membership,
    summarize_asset_sets,
)


def test_asset_key_for_path():
    p = Path("/Users/test/photos/vacation.jpg")
    key = asset_key_for_path(p)
    assert key == f"{p.parent}\t{p.stem}"
    assert "/Users/test/photos" in key
    assert "vacation" in key


def test_build_asset_membership_live_photo():
    photo = Path("/photos/trip/IMG_001.jpg")
    video = Path("/photos/trip/IMG_001.mov")
    paths = [photo, video]
    path_to_key, path_is_component, key_to_exts = build_asset_membership(paths)

    assert photo in path_to_key
    assert video in path_to_key
    assert path_to_key[photo] == path_to_key[video]

    # Both are components because the set has >1 ext with image+video
    assert path_is_component[photo] is True
    assert path_is_component[video] is True


def test_build_asset_membership_single_file():
    photo = Path("/photos/single.jpg")
    path_to_key, path_is_component, _ = build_asset_membership([photo])

    assert photo in path_to_key
    # Single file — not a component (only 1 ext in set)
    assert path_is_component[photo] is False


def test_build_asset_membership_raw_xmp():
    raw = Path("/photos/shoot/IMG_100.dng")
    xmp = Path("/photos/shoot/IMG_100.xmp")
    path_to_key, path_is_component, _ = build_asset_membership([raw, xmp])

    assert path_to_key[raw] == path_to_key[xmp]
    assert path_is_component[raw] is True
    assert path_is_component[xmp] is True


def test_summarize_asset_sets_live_pair():
    key_to_exts = {
        "/photos\tIMG_001": {"jpg", "mov"},
        "/photos\tIMG_002": {"jpg", "mov", "aae"},
    }
    summary = summarize_asset_sets(key_to_exts)
    assert summary["live_pairs_img_plus_video"] == 2
    assert summary["live_pairs_with_aae"] == 1
    assert summary["asset_sets_total"] == 2


def test_summarize_asset_sets_only_image():
    key_to_exts = {
        "/photos\tIMG_001": {"jpg"},
        "/photos\tIMG_002": {"png"},
        "/photos\tVID_001": {"mov"},
    }
    summary = summarize_asset_sets(key_to_exts)
    assert summary["only_image_sets"] == 2
    assert summary["only_video_sets"] == 1
    assert summary["live_pairs_img_plus_video"] == 0


# ── IMAGE_EXTS unification tests ──────────────────────────────────


def test_pillow_image_exts_subset_of_image_exts():
    assert PILLOW_IMAGE_EXTS <= IMAGE_EXTS


def test_image_exts_dotted_consistency():
    assert len(IMAGE_EXTS_DOTTED) == len(IMAGE_EXTS)
    for ext in IMAGE_EXTS:
        assert f".{ext}" in IMAGE_EXTS_DOTTED


def test_no_overlap_image_video():
    assert IMAGE_EXTS.isdisjoint(VIDEO_EXTS)
