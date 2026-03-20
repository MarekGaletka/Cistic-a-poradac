#!/usr/bin/env python3
"""Regenerate test fixture files.

Run from repo root: python tests/fixtures/create_fixtures.py
"""

from pathlib import Path

HERE = Path(__file__).parent


def create_jpeg_with_exif():
    """Create a 4x4 red JPEG with EXIF metadata (camera, date)."""
    from PIL import Image

    img = Image.new("RGB", (4, 4), color=(255, 0, 0))
    exif = img.getexif()
    exif[0x010F] = "TestCamera"       # Make
    exif[0x0110] = "TestModel X1"     # Model
    exif_ifd = exif.get_ifd(0x8769)
    exif_ifd[0x9003] = "2024:06:15 14:30:00"  # DateTimeOriginal
    img.save(HERE / "tiny_photo.jpg", "JPEG", exif=exif.tobytes())
    print(f"Created {HERE / 'tiny_photo.jpg'}")


def create_jpeg_no_exif():
    """Create a 4x4 blue JPEG without EXIF."""
    from PIL import Image

    img = Image.new("RGB", (4, 4), color=(0, 0, 255))
    img.save(HERE / "tiny_photo_no_exif.jpg", "JPEG")
    print(f"Created {HERE / 'tiny_photo_no_exif.jpg'}")


def create_tiny_video():
    """Create a minimal 1-second 16x16 black MP4 via ffmpeg (if available)."""
    import shutil
    import subprocess

    if not shutil.which("ffmpeg"):
        print("ffmpeg not found — skipping video fixture")
        return

    out = HERE / "tiny_video.mp4"
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", "color=c=black:s=16x16:d=1:r=1",
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            "-t", "1",
            str(out),
        ],
        capture_output=True,
    )
    if out.exists():
        print(f"Created {out}")
    else:
        print("Failed to create video fixture")


if __name__ == "__main__":
    create_jpeg_with_exif()
    create_jpeg_no_exif()
    create_tiny_video()
