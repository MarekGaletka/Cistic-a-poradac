#!/usr/bin/env python3
"""Generate GOD MODE Media Library app icon as .icns.

Creates a dark gradient background with stylized "GOD" text and an infinity
symbol, rendered at all required macOS icon sizes.  Requires Pillow.
"""
from __future__ import annotations

import math
import subprocess
import sys
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    print("Pillow required: pip install Pillow")
    sys.exit(1)

SCRIPT_DIR = Path(__file__).parent
ICONSET_DIR = SCRIPT_DIR / "icon.iconset"
ICNS_PATH = SCRIPT_DIR / "AppIcon.icns"

# macOS required icon sizes (filename → pixel size)
SIZES = {
    "icon_16x16.png": 16,
    "icon_16x16@2x.png": 32,
    "icon_32x32.png": 32,
    "icon_32x32@2x.png": 64,
    "icon_128x128.png": 128,
    "icon_128x128@2x.png": 256,
    "icon_256x256.png": 256,
    "icon_256x256@2x.png": 512,
    "icon_512x512.png": 512,
    "icon_512x512@2x.png": 1024,
}

# Colors
BG_TOP = (18, 18, 28)       # deep navy
BG_BOTTOM = (45, 25, 70)    # dark purple
ACCENT = (130, 80, 255)     # electric violet
ACCENT2 = (0, 200, 180)     # teal
TEXT_COLOR = (255, 255, 255) # white


def _draw_gradient(draw: ImageDraw.ImageDraw, size: int) -> None:
    """Draw vertical gradient background."""
    for y in range(size):
        t = y / max(size - 1, 1)
        r = int(BG_TOP[0] + (BG_BOTTOM[0] - BG_TOP[0]) * t)
        g = int(BG_TOP[1] + (BG_BOTTOM[1] - BG_TOP[1]) * t)
        b = int(BG_TOP[2] + (BG_BOTTOM[2] - BG_TOP[2]) * t)
        draw.line([(0, y), (size, y)], fill=(r, g, b))


def _draw_infinity(draw: ImageDraw.ImageDraw, cx: float, cy: float,
                   rx: float, ry: float, width: int, color: tuple) -> None:
    """Draw an infinity symbol (lemniscate) at given center."""
    points: list[tuple[float, float]] = []
    for i in range(200):
        t = (i / 200) * 2 * math.pi
        denom = 1 + math.sin(t) ** 2
        x = cx + rx * math.cos(t) / denom
        y = cy + ry * math.sin(t) * math.cos(t) / denom
        points.append((x, y))
    # Draw as thick line segments
    for i in range(len(points)):
        p1 = points[i]
        p2 = points[(i + 1) % len(points)]
        draw.line([p1, p2], fill=color, width=max(width, 1))


def _draw_glow_circle(img: Image.Image, cx: int, cy: int,
                      radius: int, color: tuple, alpha: int = 40) -> None:
    """Draw a soft glow circle."""
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    odraw = ImageDraw.Draw(overlay)
    for r in range(radius, 0, -1):
        a = int(alpha * (r / radius) ** 0.5)
        odraw.ellipse(
            [cx - r, cy - r, cx + r, cy + r],
            fill=(*color, a),
        )
    img.paste(Image.alpha_composite(img.convert("RGBA"), overlay))


def render_icon(size: int) -> Image.Image:
    """Render the icon at a given pixel size."""
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Rounded rectangle background
    _draw_gradient(draw, size)

    # Corner radius mask
    margin = max(size // 8, 2)
    mask = Image.new("L", (size, size), 0)
    mdraw = ImageDraw.Draw(mask)
    radius = max(size // 5, 4)
    mdraw.rounded_rectangle([0, 0, size - 1, size - 1], radius=radius, fill=255)
    img.putalpha(mask)

    draw = ImageDraw.Draw(img)

    # Glow effects
    _draw_glow_circle(img, int(size * 0.3), int(size * 0.35),
                      int(size * 0.4), ACCENT, alpha=25)
    _draw_glow_circle(img, int(size * 0.7), int(size * 0.65),
                      int(size * 0.35), ACCENT2, alpha=20)

    draw = ImageDraw.Draw(img)

    if size >= 64:
        # Large enough for text
        # "GOD" text
        font_size = int(size * 0.28)
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
        except (OSError, IOError):
            font = ImageFont.load_default()

        text = "GOD"
        bbox = draw.textbbox((0, 0), text, font=font)
        tw = bbox[2] - bbox[0]
        tx = (size - tw) // 2
        ty = int(size * 0.12)
        draw.text((tx, ty), text, fill=TEXT_COLOR, font=font)

        # "MODE" smaller text below
        font_small = int(size * 0.13)
        try:
            font2 = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_small)
        except (OSError, IOError):
            font2 = ImageFont.load_default()
        bbox2 = draw.textbbox((0, 0), "MODE", font=font2)
        tw2 = bbox2[2] - bbox2[0]
        tx2 = (size - tw2) // 2
        ty2 = ty + font_size + int(size * 0.02)
        draw.text((tx2, ty2), "MODE", fill=ACCENT, font=font2)

        # Infinity symbol (represents media library completeness)
        inf_cx = size * 0.5
        inf_cy = size * 0.72
        inf_rx = size * 0.22
        inf_ry = size * 0.10
        inf_width = max(int(size * 0.025), 2)
        _draw_infinity(draw, inf_cx, inf_cy, inf_rx, inf_ry, inf_width, ACCENT2)

    elif size >= 32:
        # Medium: just "G∞" or simplified
        font_size = int(size * 0.45)
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
        except (OSError, IOError):
            font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), "G", font=font)
        tw = bbox[2] - bbox[0]
        tx = (size - tw) // 2
        ty = int(size * 0.12)
        draw.text((tx, ty), "G", fill=TEXT_COLOR, font=font)

        inf_cx = size * 0.5
        inf_cy = size * 0.75
        inf_rx = size * 0.2
        inf_ry = size * 0.08
        inf_width = max(int(size * 0.04), 1)
        _draw_infinity(draw, inf_cx, inf_cy, inf_rx, inf_ry, inf_width, ACCENT2)

    else:
        # Tiny: just a colored "G"
        font_size = int(size * 0.6)
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
        except (OSError, IOError):
            font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), "G", font=font)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
        tx = (size - tw) // 2
        ty = (size - th) // 2 - bbox[1]
        draw.text((tx, ty), "G", fill=ACCENT2, font=font)

    return img


def main() -> None:
    ICONSET_DIR.mkdir(parents=True, exist_ok=True)

    for filename, px in SIZES.items():
        icon = render_icon(px)
        icon.save(ICONSET_DIR / filename, "PNG")
        print(f"  {filename} ({px}×{px})")

    # Convert iconset → .icns
    subprocess.run(
        ["iconutil", "-c", "icns", str(ICONSET_DIR), "-o", str(ICNS_PATH)],
        check=True,
    )
    print(f"\nicns={ICNS_PATH}")


if __name__ == "__main__":
    main()
