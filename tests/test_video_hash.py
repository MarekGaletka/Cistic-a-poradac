from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from godmode_media_library.video_hash import (
    extract_keyframes,
    is_video_ext,
    video_dhash,
    video_hamming_distance,
)


def test_is_video_ext():
    assert is_video_ext("mp4") is True
    assert is_video_ext("MOV") is True
    assert is_video_ext(".mkv") is True
    assert is_video_ext("jpg") is False
    assert is_video_ext("mp3") is False


def test_extract_keyframes_no_ffmpeg():
    with patch("godmode_media_library.video_hash._find_ffmpeg", return_value=None):
        frames = extract_keyframes(Path("/tmp/video.mp4"))
    assert frames == []


def test_extract_keyframes_with_ffmpeg(tmp_path):
    def fake_run(cmd, **kwargs):
        # Parse output dir from command
        out_pattern = None
        for arg in cmd:
            if "frame_" in str(arg):
                out_pattern = Path(arg)
                break
        if out_pattern:
            out_pattern.parent.mkdir(parents=True, exist_ok=True)
            for i in range(1, 4):
                (out_pattern.parent / f"frame_{i:03d}.png").write_bytes(b"fake")
        return MagicMock(returncode=0)

    with patch("godmode_media_library.video_hash._find_ffmpeg", return_value="/usr/bin/ffmpeg"), \
         patch("subprocess.run", side_effect=fake_run):
        frames = extract_keyframes(Path("/tmp/video.mp4"), n_frames=3)
    assert len(frames) == 3


def test_video_dhash_no_ffmpeg():
    with patch("godmode_media_library.video_hash._find_ffmpeg", return_value=None):
        result = video_dhash(Path("/tmp/video.mp4"))
    assert result is None


def test_video_dhash_success(tmp_path):
    # Create fake frame files
    frame_dir = tmp_path / "gml_vhash_test"
    frame_dir.mkdir()
    for i in range(1, 4):
        (frame_dir / f"frame_{i:03d}.png").write_bytes(b"fake")

    fake_hash = "0123456789abcdef"

    with patch("godmode_media_library.video_hash.extract_keyframes") as mock_extract, \
         patch("godmode_media_library.perceptual_hash.dhash", return_value=fake_hash):
        mock_extract.return_value = list(frame_dir.glob("frame_*.png"))
        result = video_dhash(Path("/tmp/video.mp4"), n_frames=3)

    assert result is not None
    # 3 frames × 16 hex chars = 48 chars
    assert result == fake_hash * 3


def test_video_hamming_distance_identical():
    hash_a = "0123456789abcdef" * 4  # 4 frames
    hash_b = "0123456789abcdef" * 4
    dist = video_hamming_distance(hash_a, hash_b)
    assert dist == 0.0


def test_video_hamming_distance_different():
    hash_a = "0000000000000000" * 2  # 2 frames of all zeros
    hash_b = "ffffffffffffffff" * 2  # 2 frames of all ones
    dist = video_hamming_distance(hash_a, hash_b)
    assert dist == 64.0  # Max distance per frame


def test_video_hamming_distance_mismatched_lengths():
    hash_a = "0123456789abcdef" * 3  # 3 frames
    hash_b = "0123456789abcdef" * 2  # 2 frames
    # Should compare only 2 corresponding frames
    dist = video_hamming_distance(hash_a, hash_b)
    assert dist == 0.0


def test_video_hamming_distance_empty():
    dist = video_hamming_distance("", "abc")
    assert dist == float("inf")
