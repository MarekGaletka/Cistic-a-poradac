"""Tests for quality.py — image quality scoring and classification."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from godmode_media_library.asset_sets import PILLOW_IMAGE_EXTS
from godmode_media_library.quality import (
    _SCREEN_RESOLUTIONS,
    DOC_EXTS,
    VIDEO_EXTS,
    QualityInfo,
    _compute_blur_score,
    _compute_brightness,
    _is_meme_ratio,
    analyze_image_quality,
    batch_analyze,
)

# ── _is_meme_ratio ──────────────────────────────────────────────────


class TestIsMemeRatio:
    def test_square_is_meme(self):
        assert _is_meme_ratio(1080, 1080) is True

    def test_16_9_is_meme(self):
        assert _is_meme_ratio(1920, 1080) is True

    def test_weird_ratio_not_meme(self):
        assert _is_meme_ratio(1000, 700) is False

    def test_zero_height(self):
        assert _is_meme_ratio(100, 0) is False


# ── _compute_blur_score ──────────────────────────────────────────────


class TestComputeBlurScore:
    def test_uniform_image_low_score(self):
        """A solid-color image should have very low blur score (no edges)."""
        from PIL import Image

        img = Image.new("L", (100, 100), color=128)
        score = _compute_blur_score(img)
        assert score < 10  # nearly zero variance

    def test_noisy_image_high_score(self):
        """An image with random noise should have high blur score."""
        import random

        from PIL import Image

        random.seed(42)
        pixels = [random.randint(0, 255) for _ in range(100 * 100)]
        img = Image.new("L", (100, 100))
        img.putdata(pixels)
        score = _compute_blur_score(img)
        assert score > 50


# ── _compute_brightness ─────────────────────────────────────────────


class TestComputeBrightness:
    def test_black_image(self):
        from PIL import Image

        img = Image.new("L", (10, 10), color=0)
        assert _compute_brightness(img) == pytest.approx(0.0)

    def test_white_image(self):
        from PIL import Image

        img = Image.new("L", (10, 10), color=255)
        assert _compute_brightness(img) == pytest.approx(255.0)

    def test_mid_gray(self):
        from PIL import Image

        img = Image.new("L", (10, 10), color=128)
        assert _compute_brightness(img) == pytest.approx(128.0)


# ── analyze_image_quality ────────────────────────────────────────────


class TestAnalyzeImageQuality:
    def test_video_file_returns_video_category(self, tmp_path):
        f = tmp_path / "clip.mp4"
        f.write_bytes(b"\x00" * 100)
        info = analyze_image_quality(str(f))
        assert info.category == "video"

    def test_document_file(self, tmp_path):
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"\x00" * 100)
        info = analyze_image_quality(str(f))
        assert info.category == "document"

    def test_screenshot_detection(self, tmp_path):
        """A PNG at screen resolution with no camera EXIF = screenshot."""
        from PIL import Image

        f = tmp_path / "screen.png"
        img = Image.new("RGB", (1920, 1080), color=(100, 100, 100))
        img.save(str(f))

        info = analyze_image_quality(str(f), width=1920, height=1080, camera_make=None)
        assert info.is_screenshot is True
        assert info.category == "screenshot"

    def test_photo_with_camera_not_screenshot(self, tmp_path):
        """Even at screen resolution, having camera EXIF means it's a photo."""
        from PIL import Image

        f = tmp_path / "photo.jpg"
        img = Image.new("RGB", (1920, 1080), color=(100, 100, 100))
        img.save(str(f))

        info = analyze_image_quality(str(f), width=1920, height=1080, camera_make="Canon")
        assert info.is_screenshot is False
        assert info.category == "photo"

    def test_meme_detection(self, tmp_path):
        """Small file, square ratio, no camera = meme."""
        from PIL import Image

        f = tmp_path / "meme.jpg"
        img = Image.new("RGB", (500, 500), color=(200, 200, 200))
        img.save(str(f), quality=10)  # small file

        size = f.stat().st_size
        assert size < 100 * 1024  # confirm it's small
        info = analyze_image_quality(str(f), width=500, height=500, size=size, camera_make=None)
        assert info.is_meme is True
        assert info.category == "meme"

    def test_dark_image(self, tmp_path):
        from PIL import Image

        f = tmp_path / "dark.jpg"
        img = Image.new("RGB", (200, 200), color=(10, 10, 10))
        img.save(str(f))
        info = analyze_image_quality(str(f))
        assert info.is_dark is True

    def test_overexposed_image(self, tmp_path):
        from PIL import Image

        f = tmp_path / "bright.jpg"
        img = Image.new("RGB", (200, 200), color=(250, 250, 250))
        img.save(str(f))
        info = analyze_image_quality(str(f))
        assert info.is_overexposed is True

    def test_blurry_detection(self, tmp_path):
        """A solid-color image has no edges, so it should be classified as blurry."""
        from PIL import Image

        f = tmp_path / "blur.jpg"
        img = Image.new("RGB", (200, 200), color=(128, 128, 128))
        img.save(str(f))
        info = analyze_image_quality(str(f))
        assert info.is_blurry is True

    def test_corrupt_file_returns_default(self, tmp_path):
        f = tmp_path / "corrupt.jpg"
        f.write_bytes(b"\x00\x01\x02\x03")
        info = analyze_image_quality(str(f))
        assert isinstance(info, QualityInfo)
        assert info.category == "photo"  # default

    def test_nonexistent_file(self, tmp_path):
        info = analyze_image_quality(str(tmp_path / "nope.jpg"))
        assert isinstance(info, QualityInfo)

    def test_size_read_from_filesystem(self, tmp_path):
        """When size=0, function reads from filesystem."""
        from PIL import Image

        f = tmp_path / "auto.jpg"
        img = Image.new("RGB", (100, 100), color=(128, 128, 128))
        img.save(str(f))
        # Should not raise, size will be read internally
        info = analyze_image_quality(str(f), size=0)
        assert isinstance(info, QualityInfo)


# ── batch_analyze ────────────────────────────────────────────────────


class TestBatchAnalyze:
    def test_batch_with_mock_catalog(self, tmp_path):
        from PIL import Image

        f = tmp_path / "photo.jpg"
        img = Image.new("RGB", (200, 200), color=(100, 100, 100))
        img.save(str(f))

        mock_catalog = MagicMock()
        mock_catalog.files_without_quality.return_value = [
            (1, str(f), 200, 200, f.stat().st_size, None),
        ]

        stats = batch_analyze(mock_catalog, limit=10)
        assert stats["analyzed"] == 1
        mock_catalog.update_quality.assert_called_once()
        mock_catalog.commit.assert_called_once()

    def test_batch_empty(self):
        mock_catalog = MagicMock()
        mock_catalog.files_without_quality.return_value = []
        stats = batch_analyze(mock_catalog)
        assert stats["analyzed"] == 0
        mock_catalog.commit.assert_called_once()

    def test_batch_progress_callback(self, tmp_path):
        from PIL import Image

        # Create 10 images so progress callback fires (every 10)
        files = []
        for i in range(10):
            f = tmp_path / f"img_{i}.jpg"
            img = Image.new("RGB", (50, 50), color=(i * 20, i * 20, i * 20))
            img.save(str(f))
            files.append((i + 1, str(f), 50, 50, f.stat().st_size, None))

        mock_catalog = MagicMock()
        mock_catalog.files_without_quality.return_value = files

        progress_calls = []
        batch_analyze(mock_catalog, progress_fn=lambda done, total: progress_calls.append((done, total)))
        # progress is called at i+1 % 10 == 0 (i.e., i=9 -> done=10) plus final call
        assert len(progress_calls) >= 1

    def test_batch_handles_errors(self, tmp_path):
        mock_catalog = MagicMock()
        mock_catalog.files_without_quality.return_value = [
            (1, str(tmp_path / "nonexistent.jpg"), 0, 0, 0, None),
        ]
        # analyze_image_quality will fail to open file, batch should catch it
        stats = batch_analyze(mock_catalog)
        # Either analyzed (with default) or error count incremented
        assert stats["analyzed"] + stats["errors"] == 1


# ── Constants / extension sets ───────────────────────────────────────


class TestExtensionSets:
    def test_image_exts_has_common(self):
        assert "jpg" in PILLOW_IMAGE_EXTS
        assert "png" in PILLOW_IMAGE_EXTS
        assert "heic" in PILLOW_IMAGE_EXTS

    def test_video_exts(self):
        assert "mp4" in VIDEO_EXTS
        assert "mov" in VIDEO_EXTS

    def test_doc_exts(self):
        assert "pdf" in DOC_EXTS

    def test_screen_resolutions_has_common(self):
        assert (1920, 1080) in _SCREEN_RESOLUTIONS
        assert (2560, 1440) in _SCREEN_RESOLUTIONS
