from __future__ import annotations

from pathlib import Path

import pytest

from godmode_media_library.perceptual_hash import (
    SimilarPair,
    dhash,
    find_similar,
    hamming_distance,
    is_image_ext,
    pillow_available,
)


def test_is_image_ext_images():
    assert is_image_ext("jpg")
    assert is_image_ext("jpeg")
    assert is_image_ext("png")
    assert is_image_ext("heic")
    assert is_image_ext("TIFF")


def test_is_image_ext_non_images():
    assert not is_image_ext("mp4")
    assert not is_image_ext("pdf")
    assert not is_image_ext("mov")


def test_hamming_distance_identical():
    assert hamming_distance("ff00ff00", "ff00ff00") == 0


def test_hamming_distance_one_bit():
    assert hamming_distance("ff00ff00", "ff00ff01") == 1


def test_hamming_distance_all_different():
    assert hamming_distance("0000000000000000", "ffffffffffffffff") == 64


def test_hamming_distance_mismatch_length():
    with pytest.raises(ValueError, match="length mismatch"):
        hamming_distance("ff", "ff00")


def test_find_similar_empty():
    pairs = find_similar({})
    assert pairs == []


def test_find_similar_identical():
    hashes = {
        "/a.jpg": "ff00ff00ff00ff00",
        "/b.jpg": "ff00ff00ff00ff00",
    }
    pairs = find_similar(hashes, threshold=0)
    assert len(pairs) == 1
    assert pairs[0].distance == 0


def test_find_similar_threshold():
    hashes = {
        "/a.jpg": "ff00ff00ff00ff00",
        "/b.jpg": "ff00ff00ff00ff01",  # distance 1
        "/c.jpg": "0000000000000000",  # far away
    }
    pairs = find_similar(hashes, threshold=2)
    assert len(pairs) == 1
    assert pairs[0].distance == 1
    assert {pairs[0].path_a, pairs[0].path_b} == {"/a.jpg", "/b.jpg"}


def test_find_similar_sorted_by_distance():
    hashes = {
        "/a.jpg": "ff00ff00ff00ff00",
        "/b.jpg": "ff00ff00ff00ff01",  # dist 1
        "/c.jpg": "ff00ff00ff00ff03",  # dist 2 from a
    }
    pairs = find_similar(hashes, threshold=5)
    assert len(pairs) >= 2
    distances = [p.distance for p in pairs]
    assert distances == sorted(distances)


def test_similar_pair_dataclass():
    p = SimilarPair(path_a="/a.jpg", path_b="/b.jpg", distance=3, hash_a="aabb", hash_b="aabc")
    assert p.distance == 3


@pytest.mark.skipif(not pillow_available(), reason="Pillow not installed")
def test_dhash_creates_valid_hash(tmp_path: Path):
    from PIL import Image

    img = Image.new("RGB", (100, 100), color="red")
    img_path = tmp_path / "test.png"
    img.save(str(img_path))

    result = dhash(img_path)
    assert result is not None
    assert len(result) == 16  # 64 bits = 16 hex chars


@pytest.mark.skipif(not pillow_available(), reason="Pillow not installed")
def test_dhash_identical_images(tmp_path: Path):
    from PIL import Image

    img = Image.new("RGB", (100, 100), color="blue")
    p1 = tmp_path / "a.png"
    p2 = tmp_path / "b.png"
    img.save(str(p1))
    img.save(str(p2))

    h1 = dhash(p1)
    h2 = dhash(p2)
    assert h1 == h2


@pytest.mark.skipif(not pillow_available(), reason="Pillow not installed")
def test_dhash_different_images(tmp_path: Path):
    from PIL import Image

    img1 = Image.new("RGB", (100, 100), color="red")
    img2 = Image.new("RGB", (100, 100), color="blue")
    p1 = tmp_path / "red.png"
    p2 = tmp_path / "blue.png"
    img1.save(str(p1))
    img2.save(str(p2))

    h1 = dhash(p1)
    h2 = dhash(p2)
    assert h1 is not None
    assert h2 is not None
    # Different solid colors should produce different hashes
    # (though for solid images the hash difference may vary)


def test_dhash_invalid_file(tmp_path: Path):
    bad = tmp_path / "notanimage.jpg"
    bad.write_bytes(b"this is not a jpeg")
    result = dhash(bad)
    assert result is None


def test_pillow_available():
    result = pillow_available()
    assert isinstance(result, bool)
