"""Unified dependency checker for external tools and optional Python packages.

Provides actionable install instructions per platform and a ``gml doctor``
report for quick troubleshooting.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import platform
import shutil
import subprocess
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DependencyStatus:
    """Result of checking a single dependency."""

    name: str
    available: bool
    version: str | None = None
    install_hint: str | None = None
    category: str = "optional"  # "required" | "optional"


# ── Platform detection ────────────────────────────────────────────────

def _platform() -> str:
    """Return simplified platform: 'macos', 'linux', or 'windows'."""
    system = platform.system().lower()
    if system == "darwin":
        return "macos"
    if system == "windows":
        return "windows"
    return "linux"


# ── Path resolution ──────────────────────────────────────────────────

_EXTRA_BIN_DIRS = ("/opt/homebrew/bin", "/usr/local/bin")


def _which(name: str) -> str | None:
    """Resolve a binary, falling back to common Homebrew/system paths."""
    resolved = shutil.which(name)
    if resolved:
        return resolved
    for prefix in _EXTRA_BIN_DIRS:
        candidate = os.path.join(prefix, name)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


# ── Individual checkers ───────────────────────────────────────────────

def check_exiftool(bin_path: str = "exiftool") -> DependencyStatus:
    """Check ExifTool availability and version."""
    resolved = _which(bin_path) if "/" not in bin_path else bin_path
    if resolved is None:
        plat = _platform()
        hints = {
            "macos": "brew install exiftool",
            "linux": "sudo apt install libimage-exiftool-perl",
            "windows": "winget install OliverBetz.ExifTool  (or https://exiftool.org)",
        }
        return DependencyStatus(
            name="ExifTool",
            available=False,
            install_hint=hints.get(plat, "https://exiftool.org"),
            category="optional",
        )
    version = None
    try:
        result = subprocess.run(
            [resolved, "-ver"], capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            version = result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return DependencyStatus(name="ExifTool", available=True, version=version)


def check_ffprobe() -> DependencyStatus:
    """Check ffprobe availability and version."""
    resolved = _which("ffprobe")
    if resolved is None:
        plat = _platform()
        hints = {
            "macos": "brew install ffmpeg",
            "linux": "sudo apt install ffmpeg",
            "windows": "winget install Gyan.FFmpeg  (or https://ffmpeg.org/download.html)",
        }
        return DependencyStatus(
            name="ffprobe (FFmpeg)",
            available=False,
            install_hint=hints.get(plat, "https://ffmpeg.org"),
            category="optional",
        )
    version = None
    try:
        result = subprocess.run(
            [resolved, "-version"], capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            first_line = result.stdout.split("\n")[0]
            version = first_line.replace("ffprobe version ", "").split(" ")[0]
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return DependencyStatus(name="ffprobe (FFmpeg)", available=True, version=version)


def check_ffmpeg() -> DependencyStatus:
    """Check ffmpeg availability (needed for video perceptual hashing)."""
    resolved = _which("ffmpeg")
    if resolved is None:
        plat = _platform()
        hints = {
            "macos": "brew install ffmpeg",
            "linux": "sudo apt install ffmpeg",
            "windows": "winget install Gyan.FFmpeg  (or https://ffmpeg.org/download.html)",
        }
        return DependencyStatus(
            name="ffmpeg",
            available=False,
            install_hint=hints.get(plat, "https://ffmpeg.org"),
            category="optional",
        )
    return DependencyStatus(name="ffmpeg", available=True)


def check_rclone() -> DependencyStatus:
    """Check rclone availability (needed for cloud storage access)."""
    resolved = _which("rclone")
    if resolved is None:
        return DependencyStatus(
            name="rclone",
            available=False,
            install_hint="https://rclone.org/install/",
            category="optional",
        )
    version = None
    try:
        result = subprocess.run(
            [resolved, "version", "--check"], capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            for line in result.stdout.split("\n"):
                if line.startswith("rclone v"):
                    version = line.strip()
                    break
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return DependencyStatus(name="rclone", available=True, version=version)


def _check_python_package(
    name: str,
    import_name: str,
    pip_install: str,
    category: str = "optional",
) -> DependencyStatus:
    """Check if a Python package is importable."""
    spec = importlib.util.find_spec(import_name)
    if spec is None:
        return DependencyStatus(
            name=name,
            available=False,
            install_hint=f"pip install {pip_install}",
            category=category,
        )
    # Try to get version
    version = None
    try:
        mod = __import__(import_name)
        version = getattr(mod, "__version__", None) or getattr(mod, "VERSION", None)
        if isinstance(version, tuple):
            version = ".".join(str(v) for v in version)
    except (ImportError, AttributeError, TypeError) as exc:
        logger.debug("Could not determine version for %s: %s", name, exc)
    return DependencyStatus(name=name, available=True, version=str(version) if version else None)


def check_pillow() -> DependencyStatus:
    """Check Pillow availability."""
    return _check_python_package("Pillow", "PIL", "Pillow")


def check_pillow_heif() -> DependencyStatus:
    """Check pillow-heif availability."""
    return _check_python_package("pillow-heif", "pillow_heif", "pillow-heif")


def check_face_recognition() -> DependencyStatus:
    """Check face_recognition + dlib availability."""
    status = _check_python_package("face_recognition", "face_recognition", "face-recognition")
    if not status.available:
        status.install_hint = "pip install godmode-media-library[people]  (requires dlib + CMake)"
    return status


def check_geopy() -> DependencyStatus:
    """Check geopy availability (for reverse geocoding)."""
    status = _check_python_package("geopy", "geopy", "geopy")
    if not status.available:
        status.install_hint = "pip install godmode-media-library[geo]"
    return status


# ── Aggregate checker ─────────────────────────────────────────────────

def check_all(exiftool_bin: str = "exiftool") -> list[DependencyStatus]:
    """Check all dependencies and return list of statuses."""
    return [
        check_exiftool(exiftool_bin),
        check_ffprobe(),
        check_ffmpeg(),
        check_pillow(),
        check_pillow_heif(),
        check_face_recognition(),
        check_geopy(),
        check_rclone(),
    ]


# ── Formatting ────────────────────────────────────────────────────────

def format_report(statuses: list[DependencyStatus]) -> str:
    """Format dependency statuses as a human-readable report."""
    lines = ["GOD MODE Media Library — Dependency Check", ""]

    available = [s for s in statuses if s.available]
    missing = [s for s in statuses if not s.available]

    if available:
        lines.append("Available:")
        for s in available:
            ver = f" ({s.version})" if s.version else ""
            lines.append(f"  + {s.name}{ver}")

    if missing:
        lines.append("")
        lines.append("Missing (install for full functionality):")
        for s in missing:
            hint = f"  Install: {s.install_hint}" if s.install_hint else ""
            lines.append(f"  - {s.name}")
            if hint:
                lines.append(f"    {hint}")

    if not missing:
        lines.append("")
        lines.append("All dependencies available!")

    lines.append("")
    return "\n".join(lines)
