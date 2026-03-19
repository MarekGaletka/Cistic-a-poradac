from __future__ import annotations

from godmode_media_library.metadata_richness import (
    MetadataDiff,
    MetadataRichnessScore,
    compute_group_diff,
    compute_richness,
    merge_candidates,
    richest_file,
)


def _photo_meta_full() -> dict:
    return {
        "EXIF:DateTimeOriginal": "2024:06:15 10:30:00",
        "EXIF:CreateDate": "2024:06:15 10:30:00",
        "EXIF:Make": "Canon",
        "EXIF:Model": "EOS R5",
        "EXIF:LensModel": "RF 24-70mm F2.8L",
        "EXIF:ExposureTime": 0.004,
        "EXIF:FNumber": 2.8,
        "EXIF:ISO": 400,
        "EXIF:GPSLatitude": 50.0875,
        "EXIF:GPSLongitude": 14.4214,
        "EXIF:ImageWidth": 8192,
        "EXIF:ImageHeight": 5464,
        "ICC_Profile:ProfileDescription": "sRGB",
        "XMP:Creator": "Photographer Name",
        "XMP:Subject": ["landscape", "prague"],
        "IPTC:Keywords": ["travel", "europe"],
        "IPTC:City": "Prague",
        "MakerNotes:SerialNumber": "1234",
        "EXIF:Copyright": "2024 Photographer",
    }


def _photo_meta_minimal() -> dict:
    return {
        "EXIF:DateTimeOriginal": "2024:06:15 10:30:00",
        "EXIF:Make": "Canon",
        "EXIF:Model": "EOS R5",
        "EXIF:ImageWidth": 8192,
        "EXIF:ImageHeight": 5464,
    }


def test_compute_richness_full_metadata():
    meta = _photo_meta_full()
    score = compute_richness(meta)
    assert isinstance(score, MetadataRichnessScore)
    assert score.total > 30  # Should be high with all those tags
    assert score.tag_count == len(meta)
    assert "datetime" in score.per_category
    assert "camera" in score.per_category
    assert score.per_category["gps"] > 0
    assert score.per_category["makernotes"] > 0
    assert score.per_category["rights"] > 0


def test_compute_richness_minimal_metadata():
    meta = _photo_meta_minimal()
    score = compute_richness(meta)
    assert score.total > 0
    assert score.total < 40  # Should be relatively low
    assert score.per_category["gps"] == 0  # No GPS
    assert score.per_category["makernotes"] == 0  # No MakerNotes


def test_compute_richness_empty():
    score = compute_richness({})
    assert score.total == 0.0
    assert score.tag_count == 0


def test_compute_richness_video():
    meta = {
        "QuickTime:Duration": 120.5,
        "QuickTime:VideoCodec": "h264",
        "QuickTime:AudioCodec": "aac",
        "QuickTime:AvgBitrate": 5000000,
        "QuickTime:VideoFrameRate": 29.97,
    }
    score = compute_richness(meta)
    assert score.per_category["video_audio"] > 0


def test_compute_group_diff_two_files():
    meta_a = {
        "EXIF:Make": "Canon",
        "EXIF:Model": "EOS R5",
        "EXIF:GPSLatitude": 50.0875,
        "EXIF:GPSLongitude": 14.4214,
    }
    meta_b = {
        "EXIF:Make": "Canon",
        "EXIF:Model": "EOS R5",
        "EXIF:DateTimeOriginal": "2024:06:15",
    }
    group = [("/a.jpg", meta_a), ("/b.jpg", meta_b)]
    diff = compute_group_diff(group)

    # Make and Model are in both → unanimous
    assert "EXIF:Make" in diff.unanimous
    assert "EXIF:Model" in diff.unanimous
    assert diff.unanimous["EXIF:Make"] == "Canon"

    # GPS only in A → partial
    assert "EXIF:GPSLatitude" in diff.partial
    assert "/a.jpg" in diff.partial["EXIF:GPSLatitude"]

    # DateTimeOriginal only in B → partial
    assert "EXIF:DateTimeOriginal" in diff.partial
    assert "/b.jpg" in diff.partial["EXIF:DateTimeOriginal"]

    # Scores should be computed
    assert "/a.jpg" in diff.scores
    assert "/b.jpg" in diff.scores


def test_compute_group_diff_conflicts():
    meta_a = {"EXIF:Make": "Canon", "EXIF:ISO": 400}
    meta_b = {"EXIF:Make": "Nikon", "EXIF:ISO": 800}
    group = [("/a.jpg", meta_a), ("/b.jpg", meta_b)]
    diff = compute_group_diff(group)

    # Make differs → conflict
    assert "EXIF:Make" in diff.conflicts
    # ISO differs → conflict
    assert "EXIF:ISO" in diff.conflicts
    # Nothing should be unanimous or partial
    assert len(diff.unanimous) == 0
    assert len(diff.partial) == 0


def test_compute_group_diff_empty():
    diff = compute_group_diff([])
    assert diff.unanimous == {}
    assert diff.partial == {}
    assert diff.conflicts == {}


def test_compute_group_diff_list_values():
    meta_a = {"XMP:Subject": ["landscape", "prague"]}
    meta_b = {"XMP:Subject": ["landscape", "travel"]}
    group = [("/a.jpg", meta_a), ("/b.jpg", meta_b)]
    diff = compute_group_diff(group)
    # Lists differ → conflict
    assert "XMP:Subject" in diff.conflicts


def test_richest_file():
    meta_full = _photo_meta_full()
    meta_min = _photo_meta_minimal()
    group = [("/full.jpg", meta_full), ("/min.jpg", meta_min)]
    result = richest_file(group)
    assert result == "/full.jpg"


def test_richest_file_empty():
    assert richest_file([]) is None


def test_merge_candidates():
    diff = MetadataDiff(
        unanimous={"EXIF:Make": "Canon"},
        partial={
            "EXIF:GPSLatitude": {"/a.jpg": 50.0875},
            "EXIF:DateTimeOriginal": {"/b.jpg": "2024:06:15"},
        },
        conflicts={},
        scores={"/a.jpg": 60.0, "/b.jpg": 40.0},
    )
    # If /a.jpg is survivor, it already has GPS but needs DateTimeOriginal from /b.jpg
    candidates = merge_candidates(diff, "/a.jpg")
    assert "EXIF:DateTimeOriginal" in candidates
    assert candidates["EXIF:DateTimeOriginal"] == ("/b.jpg", "2024:06:15")
    # GPS not in candidates (survivor already has it)
    assert "EXIF:GPSLatitude" not in candidates


def test_merge_candidates_survivor_has_all():
    diff = MetadataDiff(
        partial={
            "EXIF:GPSLatitude": {"/a.jpg": 50.0875, "/b.jpg": 51.0},
        },
    )
    # /a.jpg has the tag → nothing to merge
    candidates = merge_candidates(diff, "/a.jpg")
    assert len(candidates) == 0
