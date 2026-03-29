"""Data recovery module for GOD MODE Media Library.

Provides four recovery capabilities:
1. Quarantine browser — browse, preview and restore quarantined files
2. Deep scan — find media in hidden/lost locations (Trash, caches, temp dirs)
3. Integrity check — detect corrupted media files (truncated JPEG, broken MP4)
4. PhotoRec integration — raw disk recovery for deleted files via testdisk/photorec
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import struct
import subprocess
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_QUARANTINE = (Path.home() / ".config" / "gml" / "quarantine").resolve()

# Shell metacharacters that must never appear in paths passed to subprocess
_SHELL_METACHARACTERS = set(";|&$`\"'\\!#(){}[]<>*?~\n\r")


def _validate_quarantine_path(path: str | Path, quarantine_root: Path) -> Path:
    """Resolve *path* and verify it lives inside *quarantine_root*.

    Raises ``ValueError`` if the resolved path escapes the quarantine directory
    (e.g. via ``../../etc/passwd``).
    """
    resolved = Path(path).resolve()
    if not resolved.is_relative_to(quarantine_root.resolve()):
        raise ValueError(
            f"Path traversal blocked: {path!r} resolves to {resolved} "
            f"which is outside quarantine root {quarantine_root}"
        )
    return resolved


def _sanitize_subprocess_path(path: str, label: str = "path") -> str:
    """Validate that *path* contains no shell metacharacters.

    Raises ``ValueError`` if dangerous characters are found.
    """
    bad = _SHELL_METACHARACTERS.intersection(path)
    if bad:
        raise ValueError(
            f"Invalid characters in {label}: {bad!r} — "
            f"refusing to pass to subprocess"
        )
    return path

# Media extensions we look for during deep scan / recovery
_IMAGE_EXTS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".bmp",
    ".tiff",
    ".tif",
    ".webp",
    ".heic",
    ".heif",
    ".svg",
    ".raw",
    ".cr2",
    ".nef",
    ".arw",
    ".dng",
}
_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".wmv", ".flv", ".webm", ".m4v", ".3gp"}
_AUDIO_EXTS = {".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a", ".opus"}
_MEDIA_EXTS = _IMAGE_EXTS | _VIDEO_EXTS | _AUDIO_EXTS


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class QuarantineEntry:
    """A single file in the quarantine."""

    path: str
    original_path: str
    size: int
    ext: str
    quarantine_date: str
    category: str  # image / video / audio / other


@dataclass
class DeepScanResult:
    """Result of a deep scan for hidden/lost media."""

    locations_scanned: int = 0
    files_found: int = 0
    total_size: int = 0
    files: list[dict] = field(default_factory=list)
    locations: list[dict] = field(default_factory=list)


@dataclass
class IntegrityResult:
    """Result of a file integrity check."""

    total_checked: int = 0
    healthy: int = 0
    corrupted: int = 0
    repaired: int = 0
    errors: list[dict] = field(default_factory=list)


@dataclass
class PhotoRecResult:
    """Result of a PhotoRec recovery run."""

    files_recovered: int = 0
    total_size: int = 0
    output_dir: str = ""
    files: list[dict] = field(default_factory=list)
    partial: bool = False


# ---------------------------------------------------------------------------
# 1. Quarantine browser
# ---------------------------------------------------------------------------


def list_quarantine(quarantine_root: Path | None = None) -> list[QuarantineEntry]:
    """List all files in the quarantine directory."""
    root = (quarantine_root.resolve() if quarantine_root else None) or _DEFAULT_QUARANTINE
    if not root.exists():
        return []

    entries: list[QuarantineEntry] = []
    manifest_path = root / "manifest.json"
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Cannot read quarantine manifest %s: %s", manifest_path, exc)

    for fpath in sorted(root.rglob("*")):
        if fpath.is_file() and fpath.name != "manifest.json":
            # Ensure the file is actually inside the quarantine root
            try:
                _validate_quarantine_path(fpath, root)
            except ValueError:
                logger.warning("Skipping path outside quarantine root: %s", fpath)
                continue
            ext = fpath.suffix.lower()
            stat = fpath.stat()
            original = manifest.get(str(fpath), {}).get("original_path", "unknown")
            qdate = manifest.get(str(fpath), {}).get("quarantine_date", "")
            category = _categorize_ext(ext)
            entries.append(
                QuarantineEntry(
                    path=str(fpath),
                    original_path=original,
                    size=stat.st_size,
                    ext=ext,
                    quarantine_date=qdate,
                    category=category,
                )
            )
    return entries


def restore_from_quarantine(
    paths: list[str],
    quarantine_root: Path | None = None,
    restore_to: str | None = None,
) -> dict:
    """Restore files from quarantine to their original location or a custom directory."""
    root = (quarantine_root.resolve() if quarantine_root else None) or _DEFAULT_QUARANTINE
    manifest_path = root / "manifest.json"
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Cannot read quarantine manifest %s: %s", manifest_path, exc)

    restored = 0
    errors: list[str] = []

    for p in paths:
        try:
            src = _validate_quarantine_path(p, root)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if not src.exists():
            errors.append(f"Not found: {p}")
            continue

        if restore_to:
            dest = Path(restore_to) / src.name
        else:
            original = manifest.get(str(src), {}).get("original_path")
            if not original:
                errors.append(f"No original path for: {p}")
                continue
            dest = Path(original)

        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            # Handle name collision
            if dest.exists():
                stem = dest.stem
                ext = dest.suffix
                counter = 1
                while dest.exists():
                    dest = dest.parent / f"{stem}_{counter}{ext}"
                    counter += 1
            shutil.move(str(src), str(dest))
            # Remove from manifest
            if str(src) in manifest:
                del manifest[str(src)]
            restored += 1
        except Exception as e:
            errors.append(f"Failed to restore {p}: {e}")

    # Update manifest
    if manifest_path.exists() or restored > 0:
        try:
            manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
        except OSError as exc:
            logger.warning("Cannot write quarantine manifest %s: %s", manifest_path, exc)

    return {"restored": restored, "errors": errors}


def delete_from_quarantine(paths: list[str], quarantine_root: Path | None = None) -> dict:
    """Permanently delete files from quarantine."""
    root = (quarantine_root.resolve() if quarantine_root else None) or _DEFAULT_QUARANTINE
    manifest_path = root / "manifest.json"
    manifest: dict[str, Any] = {}
    manifest_loaded = False
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
            manifest_loaded = True
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Cannot read quarantine manifest %s: %s", manifest_path, exc)
    else:
        manifest_loaded = True  # No manifest file yet is a valid initial state

    deleted = 0
    errors: list[str] = []

    for p in paths:
        try:
            src = _validate_quarantine_path(p, root)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if not src.exists():
            errors.append(f"Not found: {p}")
            continue
        try:
            src.unlink()
            if str(src) in manifest:
                del manifest[str(src)]
            deleted += 1
        except Exception as e:
            errors.append(f"Failed to delete {p}: {e}")

    # Only write manifest back if it was successfully loaded to avoid
    # overwriting a valid manifest with an empty dict on read failure
    if manifest_loaded:
        try:
            manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
        except OSError as exc:
            logger.warning("Cannot write quarantine manifest %s: %s", manifest_path, exc)
    else:
        logger.warning("Skipping manifest write — manifest was not successfully loaded from %s", manifest_path)

    return {"deleted": deleted, "errors": errors}


# ---------------------------------------------------------------------------
# 2. Deep scan — find hidden / lost media
# ---------------------------------------------------------------------------

_DEEP_SCAN_LOCATIONS = [
    # macOS Trash
    ("Koš (Trash)", Path.home() / ".Trash"),
    # Volume Trashes
    ("Koš na discích", None),  # Special: scan /Volumes/*/.Trashes
    # Recently Deleted (iCloud / Photos)
    ("Nedávno smazané (iCloud)", Path.home() / "Library" / "Mobile Documents" / "com~apple~CloudDocs" / ".Trash"),
    # Downloads
    ("Stažené soubory", Path.home() / "Downloads"),
    # Desktop
    ("Plocha", Path.home() / "Desktop"),
    # Temp directories
    ("Dočasné soubory", Path("/tmp")),
    ("Uživatelský temp", Path(tempfile.gettempdir())),
    # iPhoto / Photos library
    ("Fotky (Photos Library)", Path.home() / "Pictures" / "Photos Library.photoslibrary"),
]


# ---------------------------------------------------------------------------
# 2b. App media mining — known app data locations on macOS
# ---------------------------------------------------------------------------

# Magic bytes for detecting media files without extensions (e.g. Signal)
_MAGIC_BYTES: dict[bytes, tuple[str, str]] = {
    b"\xff\xd8\xff": (".jpg", "image"),
    b"\x89PNG": (".png", "image"),
    b"GIF87a": (".gif", "image"),
    b"GIF89a": (".gif", "image"),
    b"RIFF": (".webp", "image"),  # could also be .wav/.avi, check further
    b"\x00\x00\x00\x18ftypmp4": (".mp4", "video"),
    b"\x00\x00\x00\x1cftypmp4": (".mp4", "video"),
    b"\x00\x00\x00\x20ftypmp4": (".mp4", "video"),
    b"\x00\x00\x00\x18ftypisom": (".mp4", "video"),
    b"\x00\x00\x00\x1cftypisom": (".mp4", "video"),
    b"\x00\x00\x00\x14ftypqt": (".mov", "video"),
    b"\x00\x00\x00\x18ftypqt": (".mov", "video"),
    b"\x1aE\xdf\xa3": (".webm", "video"),
    b"OggS": (".ogg", "audio"),
    b"fLaC": (".flac", "audio"),
    b"ID3": (".mp3", "audio"),
    b"\xff\xfb": (".mp3", "audio"),
    b"\xff\xf3": (".mp3", "audio"),
    b"\xff\xf2": (".mp3", "audio"),
}

# Comprehensive list of apps and their media storage locations on macOS
_APP_SOURCES: list[dict[str, Any]] = [
    # ── Messaging apps ──
    {
        "id": "whatsapp",
        "name": "WhatsApp",
        "icon": "\U0001f4ac",
        "color": "#25D366",
        "category": "messaging",
        "paths": [
            Path.home() / "Library" / "Containers" / "net.whatsapp.WhatsApp" / "Data" / "Downloads",
            Path.home() / "Library" / "Containers" / "net.whatsapp.WhatsApp" / "Data" / "Library" / "Caches" / "ChatMedia",
            Path.home() / "Library" / "Containers" / "net.whatsapp.WhatsApp" / "Data" / "Library" / "Caches" / "GalleryMedia",
            Path.home() / "Library" / "Containers" / "net.whatsapp.WhatsApp" / "Data" / "tmp" / "MediaCache",
            Path.home() / "Library" / "Containers" / "desktop.WhatsApp" / "Data" / "Downloads",
            Path.home() / "Library" / "Containers" / "desktop.WhatsApp" / "Data" / "Library" / "Caches" / "ChatMedia",
            Path.home() / "Library" / "Group Containers" / "group.net.whatsapp.WhatsApp.shared" / "Message" / "Media",
        ],
    },
    {
        "id": "signal",
        "name": "Signal",
        "icon": "\U0001f512",
        "color": "#3A76F0",
        "category": "messaging",
        "paths": [
            Path.home() / "Library" / "Application Support" / "Signal" / "attachments.noindex",
        ],
        "extensionless": True,
        "encrypted": True,
        "decryptable": True,  # We can decrypt via SQLCipher + Keychain
        "note": "Signal šifruje přílohy. Klikněte na 'Dešifrovat' pro extrakci médií.",
    },
    {
        "id": "telegram",
        "name": "Telegram",
        "icon": "\u2708\ufe0f",
        "color": "#0088cc",
        "category": "messaging",
        "paths": [
            Path.home() / "Library" / "Containers" / "ru.keepcoder.Telegram" / "Data" / "Documents",
            Path.home() / "Library" / "Containers" / "ru.keepcoder.Telegram" / "Data" / "Library" / "Caches",
            Path.home() / "Library" / "Group Containers" / "6N38VWS5BX.ru.keepcoder.Telegram" / "appstore",
        ],
    },
    {
        "id": "imessage",
        "name": "iMessage",
        "icon": "\U0001f4e8",
        "color": "#34C759",
        "category": "messaging",
        "paths": [
            Path.home() / "Library" / "Messages" / "Attachments",
        ],
    },
    {
        "id": "messenger",
        "name": "Messenger (Facebook)",
        "icon": "\U0001f4ad",
        "color": "#0084FF",
        "category": "messaging",
        "paths": [
            Path.home() / "Library" / "Containers" / "com.facebook.archon" / "Data",
            Path.home() / "Library" / "Group Containers" / "group.com.facebook.Messenger",
        ],
    },
    {
        "id": "viber",
        "name": "Viber",
        "icon": "\U0001f4de",
        "color": "#7360F2",
        "category": "messaging",
        "paths": [
            Path.home() / "Library" / "Application Support" / "Viber" / "Media",
            Path.home() / "Library" / "Application Support" / "ViberPC" / "Media",
        ],
    },
    {
        "id": "skype",
        "name": "Skype",
        "icon": "\U0001f4f9",
        "color": "#00AFF0",
        "category": "messaging",
        "paths": [
            Path.home() / "Library" / "Application Support" / "Skype",
            Path.home() / "Library" / "Containers" / "com.skype.skype" / "Data" / "Library" / "Application Support" / "Skype",
        ],
    },
    {
        "id": "line",
        "name": "LINE",
        "icon": "\U0001f49a",
        "color": "#00C300",
        "category": "messaging",
        "paths": [
            Path.home() / "Library" / "Containers" / "jp.naver.line.mac" / "Data" / "Library" / "Application Support",
        ],
    },
    # ── Social / Community ──
    {
        "id": "discord",
        "name": "Discord",
        "icon": "\U0001f3ae",
        "color": "#5865F2",
        "category": "social",
        "paths": [
            Path.home() / "Library" / "Application Support" / "discord" / "Cache",
            Path.home() / "Library" / "Application Support" / "discord" / "Cached",
        ],
    },
    {
        "id": "slack",
        "name": "Slack",
        "icon": "\U0001f4bc",
        "color": "#4A154B",
        "category": "work",
        "paths": [
            Path.home()
            / "Library"
            / "Containers"
            / "com.tinyspeck.slackmacgap"
            / "Data"
            / "Library"
            / "Application Support"
            / "Slack"
            / "Cache",
            Path.home() / "Library" / "Application Support" / "Slack" / "Cache",
            Path.home() / "Library" / "Application Support" / "Slack" / "Service Worker" / "CacheStorage",
        ],
    },
    {
        "id": "teams",
        "name": "Microsoft Teams",
        "icon": "\U0001f465",
        "color": "#6264A7",
        "category": "work",
        "paths": [
            Path.home() / "Library" / "Application Support" / "Microsoft" / "Teams" / "Cache",
            Path.home() / "Library" / "Containers" / "com.microsoft.teams2" / "Data" / "Library" / "Caches",
        ],
    },
    # ── Browsers (cached media) ──
    {
        "id": "safari",
        "name": "Safari",
        "icon": "\U0001f310",
        "color": "#006CFF",
        "category": "browser",
        "paths": [
            Path.home() / "Library" / "Containers" / "com.apple.Safari" / "Data" / "Library" / "Caches",
        ],
    },
    {
        "id": "chrome",
        "name": "Google Chrome",
        "icon": "\U0001f30d",
        "color": "#4285F4",
        "category": "browser",
        "paths": [
            Path.home() / "Library" / "Caches" / "Google" / "Chrome" / "Default" / "Cache",
            Path.home() / "Library" / "Application Support" / "Google" / "Chrome" / "Default" / "Cache",
        ],
    },
    {
        "id": "firefox",
        "name": "Firefox",
        "icon": "\U0001f525",
        "color": "#FF7139",
        "category": "browser",
        "paths": [
            Path.home() / "Library" / "Caches" / "Firefox" / "Profiles",
        ],
    },
    # ── Apple ecosystem ──
    {
        "id": "photos",
        "name": "Fotky (Apple Photos)",
        "icon": "\U0001f338",
        "color": "#FF2D55",
        "category": "apple",
        "paths": [
            Path.home() / "Pictures" / "Photos Library.photoslibrary" / "originals",
            Path.home() / "Pictures" / "Photos Library.photoslibrary" / "resources" / "media",
        ],
    },
    {
        "id": "icloud_drive",
        "name": "iCloud Drive",
        "icon": "\u2601\ufe0f",
        "color": "#3693F3",
        "category": "apple",
        "paths": [
            Path.home() / "Library" / "Mobile Documents" / "com~apple~CloudDocs",
        ],
    },
    {
        "id": "airdrop",
        "name": "AirDrop",
        "icon": "\U0001f4e1",
        "color": "#007AFF",
        "category": "apple",
        "paths": [
            Path.home() / "Library" / "Sharing",
        ],
    },
    # ── Creative / Edit ──
    {
        "id": "preview",
        "name": "Náhled (Preview)",
        "icon": "\U0001f5bc\ufe0f",
        "color": "#30B0C7",
        "category": "creative",
        "paths": [
            Path.home() / "Library" / "Containers" / "com.apple.Preview" / "Data" / "Library",
        ],
    },
]


@dataclass
class AppMineResult:
    """Result of mining media from a single app."""

    app_id: str
    app_name: str
    icon: str
    color: str
    category: str
    available: bool = False
    encrypted: bool = False
    note: str = ""
    files_found: int = 0
    total_size: int = 0
    raw_files_count: int = 0  # Total files including encrypted/undetectable
    raw_total_size: int = 0
    images: int = 0
    videos: int = 0
    audio: int = 0
    other: int = 0
    files: list[dict] = field(default_factory=list)
    paths_checked: list[str] = field(default_factory=list)


def _detect_type_by_magic(fpath: str) -> tuple[str, str] | None:
    """Detect file type by reading magic bytes (for extensionless files like Signal)."""
    try:
        with open(fpath, "rb") as f:
            header = f.read(32)
        if len(header) < 4:
            return None

        # Check ftyp-based formats (MP4/MOV) — variable offset
        if b"ftyp" in header[:16]:
            ftyp_pos = header.find(b"ftyp")
            brand = header[ftyp_pos + 4 : ftyp_pos + 8]
            if brand in (b"mp41", b"mp42", b"isom", b"M4A ", b"M4V ", b"avc1", b"dash"):
                return (".mp4", "video")
            if brand in (b"qt  ", b"MSNV"):
                return (".mov", "video")
            if brand in (b"M4A ",):
                return (".m4a", "audio")
            return (".mp4", "video")  # generic ftyp

        # Check RIFF-based (WAV, WebP, AVI)
        if header[:4] == b"RIFF" and len(header) >= 12:
            fmt = header[8:12]
            if fmt == b"WEBP":
                return (".webp", "image")
            if fmt == b"WAVE":
                return (".wav", "audio")
            if fmt == b"AVI ":
                return (".avi", "video")

        # Simple prefix matching
        for magic, result in _MAGIC_BYTES.items():
            if header[: len(magic)] == magic:
                return result

        return None
    except (OSError, PermissionError):
        return None


def mine_app_media(
    app_ids: list[str] | None = None,
    progress_fn: Callable | None = None,
) -> list[AppMineResult]:
    """Mine media files from application data directories.

    Args:
        app_ids: If given, only mine these apps. Otherwise mine all known apps.
        progress_fn: Progress callback.

    Returns:
        List of per-app results.
    """
    sources = _APP_SOURCES
    if app_ids:
        sources = [s for s in sources if s["id"] in app_ids]

    results: list[AppMineResult] = []
    total = len(sources)

    for idx, source in enumerate(sources):
        if progress_fn:
            progress_fn(
                {
                    "phase": "app_mining",
                    "app": source["name"],
                    "app_icon": source["icon"],
                    "progress_pct": int((idx / max(total, 1)) * 100),
                    "apps_scanned": idx,
                }
            )

        app_result = AppMineResult(
            app_id=source["id"],
            app_name=source["name"],
            icon=source["icon"],
            color=source["color"],
            category=source["category"],
            encrypted=source.get("encrypted", False),
            note=source.get("note", ""),
        )
        extensionless = source.get("extensionless", False)
        is_encrypted = source.get("encrypted", False)

        for base_path in source["paths"]:
            if not base_path.exists():
                continue
            app_result.available = True

            # For encrypted apps, just count files and total size
            if is_encrypted:
                try:
                    for dirpath, _dirnames, filenames in os.walk(base_path, followlinks=False):
                        for fname in filenames:
                            fpath = os.path.join(dirpath, fname)
                            try:
                                fsize = os.path.getsize(fpath)
                                if fsize < 100:
                                    continue
                                app_result.raw_files_count += 1
                                app_result.raw_total_size += fsize
                            except OSError:
                                continue
                except (OSError, PermissionError):
                    pass
                continue  # Skip normal scanning for encrypted apps
            app_result.paths_checked.append(str(base_path))

            try:
                for dirpath, _dirnames, filenames in os.walk(base_path, followlinks=False):
                    for fname in filenames:
                        fpath = os.path.join(dirpath, fname)
                        ext = os.path.splitext(fname)[1].lower()

                        # For extensionless files (Signal), detect via magic bytes
                        if not ext and extensionless:
                            detected = _detect_type_by_magic(fpath)
                            if detected:
                                ext, cat = detected
                            else:
                                continue  # Skip non-media
                        elif ext and ext in _MEDIA_EXTS:
                            cat = _categorize_ext(ext)
                        elif not ext:
                            continue  # No extension and not extensionless mode
                        else:
                            continue  # Extension but not media

                        try:
                            fsize = os.path.getsize(fpath)
                            if fsize < 100:
                                continue
                        except OSError:
                            continue

                        file_info = {
                            "path": fpath,
                            "name": fname,
                            "size": fsize,
                            "ext": ext,
                            "category": cat,
                            "app": source["id"],
                            "app_name": source["name"],
                        }
                        app_result.files.append(file_info)
                        app_result.files_found += 1
                        app_result.total_size += fsize

                        if cat == "image":
                            app_result.images += 1
                        elif cat == "video":
                            app_result.videos += 1
                        elif cat == "audio":
                            app_result.audio += 1
                        else:
                            app_result.other += 1

            except (OSError, PermissionError):
                continue

        results.append(app_result)

    if progress_fn:
        progress_fn(
            {
                "phase": "complete",
                "progress_pct": 100,
                "apps_scanned": total,
            }
        )

    return results


def get_available_apps() -> list[dict]:
    """Quick check which apps have data directories present (no file scanning)."""
    apps: list[dict] = []
    for source in _APP_SOURCES:
        available = any(p.exists() for p in source["paths"])
        apps.append(
            {
                "id": source["id"],
                "name": source["name"],
                "icon": source["icon"],
                "color": source["color"],
                "category": source["category"],
                "available": available,
                "encrypted": source.get("encrypted", False),
                "decryptable": source.get("decryptable", False),
                "note": source.get("note", ""),
            }
        )
    return apps


def deep_scan(
    roots: list[str] | None = None,
    progress_fn: Callable | None = None,
) -> DeepScanResult:
    """Scan hidden/lost locations for media files."""
    result = DeepScanResult()
    locations_to_scan: list[tuple[str, Path]] = []

    if roots:
        for r in roots:
            p = Path(r)
            if p.exists():
                locations_to_scan.append((str(p), p))
    else:
        for name, path in _DEEP_SCAN_LOCATIONS:
            if path is None:
                # Scan /Volumes/*/.Trashes
                volumes = Path("/Volumes")
                if volumes.exists():
                    for vol in volumes.iterdir():
                        trash_dir = vol / ".Trashes"
                        if trash_dir.exists():
                            locations_to_scan.append((f"Koš na {vol.name}", trash_dir))
                continue
            if path.exists():
                locations_to_scan.append((name, path))

    total_locs = len(locations_to_scan)

    for idx, (name, scan_path) in enumerate(locations_to_scan):
        if progress_fn:
            progress_fn(
                {
                    "phase": "deep_scan",
                    "location": name,
                    "progress_pct": int((idx / max(total_locs, 1)) * 100),
                    "files_found": result.files_found,
                }
            )

        loc_files: list[dict] = []
        loc_size = 0

        try:
            for dirpath, _dirnames, filenames in os.walk(scan_path, followlinks=False):
                for fname in filenames:
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in _MEDIA_EXTS:
                        continue
                    fpath = os.path.join(dirpath, fname)
                    try:
                        stat = os.stat(fpath)
                        fsize = stat.st_size
                        if fsize < 100:  # Skip tiny/empty files
                            continue
                        loc_files.append(
                            {
                                "path": fpath,
                                "name": fname,
                                "size": fsize,
                                "ext": ext,
                                "category": _categorize_ext(ext),
                                "location": name,
                            }
                        )
                        loc_size += fsize
                        result.files_found += 1
                        result.total_size += fsize
                    except (OSError, PermissionError):
                        continue
        except (OSError, PermissionError):
            pass

        if loc_files:
            result.locations.append(
                {
                    "name": name,
                    "path": str(scan_path),
                    "files_count": len(loc_files),
                    "total_size": loc_size,
                }
            )
            result.files.extend(loc_files)

    result.locations_scanned = total_locs

    if progress_fn:
        progress_fn(
            {
                "phase": "complete",
                "progress_pct": 100,
                "files_found": result.files_found,
                "total_size": result.total_size,
            }
        )

    return result


def recover_files(
    file_paths: list[str],
    destination: str,
    delete_source: bool = False,
) -> dict:
    """Copy or move found files to a recovery destination."""
    dest = Path(destination)
    dest.mkdir(parents=True, exist_ok=True, mode=0o700)

    recovered = 0
    errors: list[str] = []
    total_size = 0

    for fpath in file_paths:
        src = Path(fpath)
        if not src.exists():
            errors.append(f"Not found: {fpath}")
            continue

        target = dest / src.name
        # Avoid collisions
        if target.exists():
            stem = target.stem
            ext = target.suffix
            counter = 1
            while target.exists():
                target = dest / f"{stem}_{counter}{ext}"
                counter += 1

        try:
            if delete_source:
                shutil.move(str(src), str(target))
            else:
                shutil.copy2(str(src), str(target))
            recovered += 1
            total_size += target.stat().st_size
        except Exception as e:
            errors.append(f"Failed: {fpath}: {e}")

    return {"recovered": recovered, "total_size": total_size, "errors": errors}


# ---------------------------------------------------------------------------
# 3. Integrity check — detect corrupted media
# ---------------------------------------------------------------------------


def check_integrity(
    paths: list[str] | None = None,
    catalog_path: str | None = None,
    progress_fn: Callable | None = None,
) -> IntegrityResult:
    """Check media file integrity. Uses catalog files if no paths given."""
    result = IntegrityResult()
    files_to_check: list[str] = []

    if paths:
        files_to_check = paths
    elif catalog_path:
        from .catalog import Catalog

        cat = Catalog(catalog_path)
        cat.open()
        try:
            rows = cat.query_files(limit=100000)
            files_to_check = [r.path for r in rows]
        finally:
            cat.close()

    total = len(files_to_check)

    for idx, fpath in enumerate(files_to_check):
        if progress_fn and idx % 50 == 0:
            progress_fn(
                {
                    "phase": "integrity_check",
                    "progress_pct": int((idx / max(total, 1)) * 100),
                    "checked": result.total_checked,
                    "corrupted": result.corrupted,
                }
            )

        p = Path(fpath)
        if not p.exists():
            result.errors.append({"path": fpath, "issue": "missing", "description": "Soubor neexistuje", "repairable": False})
            result.corrupted += 1
            result.total_checked += 1
            continue

        ext = p.suffix.lower()
        issue = None

        try:
            if ext in {".jpg", ".jpeg"}:
                issue = _check_jpeg(p)
            elif ext in {".png"}:
                issue = _check_png(p)
            elif ext in {".mp4", ".mov", ".m4v"}:
                issue = _check_mp4(p)
            elif ext in {".gif"}:
                issue = _check_gif(p)
            elif ext in _VIDEO_EXTS:
                issue = _check_video_ffprobe(p)
        except Exception as e:
            issue = {"issue": "read_error", "description": f"Nelze přečíst: {e}", "repairable": False}

        if issue:
            issue["path"] = fpath
            result.errors.append(issue)
            result.corrupted += 1
        else:
            result.healthy += 1

        result.total_checked += 1

    if progress_fn:
        progress_fn(
            {
                "phase": "complete",
                "progress_pct": 100,
                "checked": result.total_checked,
                "corrupted": result.corrupted,
            }
        )

    return result


def repair_file(fpath: str) -> dict:
    """Attempt to repair a corrupted media file."""
    p = Path(fpath)
    if not p.exists():
        return {"success": False, "error": "Soubor neexistuje"}

    ext = p.suffix.lower()

    # JPEG repair via Pillow re-save
    if ext in {".jpg", ".jpeg"}:
        return _repair_jpeg(p)

    # MP4/MOV repair via ffmpeg
    if ext in {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm"}:
        return _repair_video(p)

    return {"success": False, "error": f"Oprava není podporována pro {ext}"}


# ---------------------------------------------------------------------------
# 4. PhotoRec integration
# ---------------------------------------------------------------------------


def check_photorec() -> dict:
    """Check if PhotoRec (testdisk) is installed."""
    try:
        result = subprocess.run(
            ["photorec", "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        version = result.stdout.strip().split("\n")[0] if result.stdout else "unknown"
        return {"available": True, "version": version}
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {"available": False, "version": None, "install_hint": "brew install testdisk"}


def list_disks() -> list[dict]:
    """List available disks/partitions for PhotoRec scanning."""
    disks: list[dict] = []

    # macOS: use diskutil list
    try:
        result = subprocess.run(
            ["diskutil", "list", "-plist"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            # Parse plist output — simplified approach via diskutil info
            result2 = subprocess.run(
                ["diskutil", "list"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            current_disk = None
            for line in result2.stdout.split("\n"):
                line = line.strip()
                if line.startswith("/dev/"):
                    current_disk = line.split()[0].rstrip(":")
                    desc = line.split("(", 1)[-1].rstrip("):") if "(" in line else ""
                    disks.append(
                        {
                            "device": current_disk,
                            "description": desc,
                            "partitions": [],
                        }
                    )
                elif current_disk and ":" in line and ("Apple" in line or "Microsoft" in line or "EFI" in line):
                    parts = line.split()
                    if len(parts) >= 3 and disks:
                        disks[-1]["partitions"].append(
                            {
                                "name": parts[1] if len(parts) > 1 else "",
                                "size": parts[-2] + " " + parts[-1] if len(parts) > 2 else "",
                            }
                        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Also list mounted volumes
    volumes_dir = Path("/Volumes")
    if volumes_dir.exists():
        for vol in sorted(volumes_dir.iterdir()):
            if vol.is_dir() and not vol.name.startswith("."):
                try:
                    usage = shutil.disk_usage(vol)
                    disks.append(
                        {
                            "device": str(vol),
                            "description": f"Volume: {vol.name}",
                            "total_size": usage.total,
                            "free_size": usage.free,
                            "partitions": [],
                        }
                    )
                except OSError:
                    pass

    return disks


def run_photorec(
    source: str,
    output_dir: str | None = None,
    file_types: list[str] | None = None,
    progress_fn: Callable | None = None,
) -> PhotoRecResult:
    """Run PhotoRec on a source disk/partition/image.

    This is a wrapper around the photorec CLI tool.
    For safety, we run in non-destructive read-only mode.
    """
    check = check_photorec()
    if not check["available"]:
        raise RuntimeError("PhotoRec není nainstalován. Spusťte: brew install testdisk")

    # Validate source path — block shell metacharacters
    _sanitize_subprocess_path(source, label="source")

    if not output_dir:
        output_dir = str(Path.home() / "Desktop" / "GML_Recovery")
    else:
        _sanitize_subprocess_path(output_dir, label="output_dir")

    # Validate source exists as a file or block device
    source_path = Path(source)
    if not (source_path.exists() or source_path.is_block_device()):
        raise ValueError(f"Source does not exist: {source!r}")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True, mode=0o700)

    result = PhotoRecResult(output_dir=output_dir)

    # Build photorec command — always use a list, never shell=True
    cmd = [
        "photorec",
        "/log",
        "/d",
        str(out),
    ]

    # File type filter — validate each type is alphanumeric to prevent injection
    if file_types:
        for ft in file_types:
            if not ft.isalnum():
                raise ValueError(f"Invalid file type filter: {ft!r}")
        cmd.extend(["/fileopt", "everything,disable", *[f"{ft},enable" for ft in file_types]])

    cmd.append(str(source_path))

    if progress_fn:
        progress_fn(
            {
                "phase": "photorec",
                "status": "starting",
                "source": source,
                "output_dir": output_dir,
            }
        )

    try:
        # PhotoRec is interactive by default; we use /cmd for non-interactive
        # For now, run with a timeout and capture output
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour max
            cwd=str(out),
        )

        if proc.returncode != 0:
            logger.error(
                "PhotoRec exited with code %d: %s",
                proc.returncode,
                (proc.stderr or proc.stdout or "")[:500],
            )
            result.partial = True

        # Count recovered files
        for dirpath, _, filenames in os.walk(out):
            for fname in filenames:
                fpath = os.path.join(dirpath, fname)
                try:
                    fsize = os.path.getsize(fpath)
                    result.files.append(
                        {
                            "path": fpath,
                            "name": fname,
                            "size": fsize,
                            "ext": os.path.splitext(fname)[1].lower(),
                        }
                    )
                    result.files_recovered += 1
                    result.total_size += fsize
                except OSError:
                    pass

    except subprocess.TimeoutExpired:
        logger.warning("PhotoRec timed out after 1 hour")
    except Exception as e:
        logger.error("PhotoRec error: %s", e)

    if progress_fn:
        progress_fn(
            {
                "phase": "complete",
                "files_recovered": result.files_recovered,
                "total_size": result.total_size,
            }
        )

    return result


# ---------------------------------------------------------------------------
# 5. Signal decryption
# ---------------------------------------------------------------------------

_SIGNAL_DB_PATH = Path.home() / "Library" / "Application Support" / "Signal" / "sql" / "db.sqlite"
_SIGNAL_ATTACH_DIR = Path.home() / "Library" / "Application Support" / "Signal" / "attachments.noindex"
_SIGNAL_KEYCHAIN_SERVICE = "Signal Safe Storage"
_SIGNAL_KEYCHAIN_ACCOUNT = "Signal Key"


def check_signal_decrypt() -> dict:
    """Check if Signal decryption is possible.

    Returns status dict with:
      - possible: bool — all prerequisites met
      - db_exists: bool
      - attachments_exist: bool
      - sqlcipher_available: bool
      - keychain_accessible: bool
      - attachment_count: int
      - error: str | None
    """
    result = {
        "possible": False,
        "db_exists": _SIGNAL_DB_PATH.exists(),
        "attachments_exist": _SIGNAL_ATTACH_DIR.exists(),
        "sqlcipher_available": False,
        "keychain_accessible": False,
        "attachment_count": 0,
        "error": None,
    }

    # Count attachments
    if result["attachments_exist"]:
        try:
            count = 0
            for _, _, files in os.walk(_SIGNAL_ATTACH_DIR):
                count += len(files)
            result["attachment_count"] = count
        except OSError:
            pass

    # Check sqlcipher
    try:
        import pysqlcipher3.dbapi2  # noqa: F401

        result["sqlcipher_available"] = True
    except ImportError:
        # Try system sqlcipher via subprocess (check Homebrew paths too)
        sqlcipher_bin = shutil.which("sqlcipher")
        if not sqlcipher_bin:
            for prefix in ("/opt/homebrew/bin", "/usr/local/bin"):
                candidate = os.path.join(prefix, "sqlcipher")
                if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                    sqlcipher_bin = candidate
                    break
        try:
            subprocess.run(
                [sqlcipher_bin or "sqlcipher", "--version"],
                capture_output=True,
                timeout=5,
            )
            result["sqlcipher_available"] = True
        except (FileNotFoundError, subprocess.TimeoutExpired, TypeError):
            result["error"] = "SQLCipher není nainstalován. Spusťte: pip install pysqlcipher3  nebo  brew install sqlcipher"
            return result

    # Check keychain entry exists (without reading value — that requires user auth)
    try:
        proc = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-s",
                _SIGNAL_KEYCHAIN_SERVICE,
                "-a",
                _SIGNAL_KEYCHAIN_ACCOUNT,
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            result["keychain_accessible"] = True
        else:
            result["error"] = "Signal klíč nenalezen v Keychain. Je Signal Desktop nainstalován?"
            return result
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        result["error"] = f"Chyba při přístupu ke Keychain: {e}"
        return result

    result["possible"] = (
        result["db_exists"] and result["attachments_exist"] and result["sqlcipher_available"] and result["keychain_accessible"]
    )
    return result


def _get_signal_key() -> str | None:
    """Read Signal's SQLCipher encryption key from macOS Keychain."""
    try:
        proc = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-s",
                _SIGNAL_KEYCHAIN_SERVICE,
                "-a",
                _SIGNAL_KEYCHAIN_ACCOUNT,
                "-w",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if proc.returncode == 0:
            return proc.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def _open_signal_db(key: str):
    """Open Signal's SQLCipher database and return connection.

    Tries pysqlcipher3 first, falls back to sqlcipher CLI.
    """
    try:
        import pysqlcipher3.dbapi2 as sqlcipher

        conn = sqlcipher.connect(str(_SIGNAL_DB_PATH))
        conn.execute(f"PRAGMA key = \"x'{key}'\"")
        conn.execute("PRAGMA cipher_compatibility = 4")
        # Verify access
        conn.execute("SELECT count(*) FROM sqlite_master")
        return conn
    except ImportError:
        pass
    except Exception as e:
        logger.warning("pysqlcipher3 failed: %s, trying CLI", e)

    # Fallback: use sqlcipher CLI
    return None


def _find_sqlcipher_bin() -> str:
    """Find sqlcipher binary, checking Homebrew paths."""
    resolved = shutil.which("sqlcipher")
    if resolved:
        return resolved
    for prefix in ("/opt/homebrew/bin", "/usr/local/bin"):
        candidate = os.path.join(prefix, "sqlcipher")
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return "sqlcipher"


def _query_signal_attachments_cli(key: str) -> list[dict]:
    """Query Signal attachments via sqlcipher CLI."""
    query = """
        SELECT
            json_extract(json, '$.path') as path,
            json_extract(json, '$.contentType') as content_type,
            json_extract(json, '$.size') as size,
            json_extract(json, '$.fileName') as file_name,
            json_extract(json, '$.localKey') as local_key,
            json_extract(json, '$.width') as width,
            json_extract(json, '$.height') as height
        FROM messages
        WHERE json_extract(json, '$.hasAttachments') = 1
        LIMIT 10000;
    """
    # Pass commands via stdin instead of CLI arguments to avoid leaking
    # the SQLCipher key in the process table (visible via ps).
    try:
        sqlcipher_input = (
            f"PRAGMA key = \"x'{key}'\";\n"
            "PRAGMA cipher_compatibility = 4;\n"
            ".mode json\n"
            f"{query}\n"
        )
        proc = subprocess.run(
            [
                _find_sqlcipher_bin(),
                str(_SIGNAL_DB_PATH),
            ],
            input=sqlcipher_input,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return json.loads(proc.stdout)
    except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError) as e:
        logger.error("sqlcipher CLI failed: %s", e)
    return []


def decrypt_signal_attachments(
    destination: str,
    progress_fn: Callable | None = None,
) -> dict:
    """Decrypt and export Signal media attachments.

    Reads encryption keys from Signal's SQLCipher database (via macOS Keychain),
    then decrypts each attachment and saves it with the correct file extension.

    Args:
        destination: Directory to save decrypted files
        progress_fn: Progress callback

    Returns:
        Dict with: decrypted, skipped, errors, total_size, files
    """
    import base64

    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    result = {
        "decrypted": 0,
        "skipped": 0,
        "errors": [],
        "total_size": 0,
        "files": [],
    }

    dest = Path(destination)
    dest.mkdir(parents=True, exist_ok=True, mode=0o700)

    if progress_fn:
        progress_fn({"phase": "signal_decrypt", "status": "reading_key", "progress_pct": 0})

    # Get encryption key from Keychain
    key = _get_signal_key()
    if not key:
        result["errors"].append("Nelze přečíst šifrovací klíč z Keychain")
        return result

    if progress_fn:
        progress_fn({"phase": "signal_decrypt", "status": "reading_db", "progress_pct": 5})

    # Read attachment metadata from database
    attachments = []
    conn = _open_signal_db(key)
    if conn:
        try:
            cursor = conn.execute("""
                SELECT
                    json_extract(json, '$.attachments') as attachments_json
                FROM messages
                WHERE json_extract(json, '$.hasAttachments') = 1
            """)
            for row in cursor:
                if row[0]:
                    try:
                        atts = json.loads(row[0])
                        if isinstance(atts, list):
                            attachments.extend(atts)
                    except (json.JSONDecodeError, TypeError):
                        pass
        except Exception as e:
            logger.error("Signal DB query failed: %s", e)
            result["errors"].append(f"Chyba při čtení databáze: {e}")
        finally:
            conn.close()
    else:
        # Try CLI fallback
        rows = _query_signal_attachments_cli(key)
        attachments = rows

    if not attachments:
        result["errors"].append("Nenalezeny žádné přílohy v databázi Signal")
        return result

    total = len(attachments)
    logger.info("Found %d Signal attachments to process", total)

    if progress_fn:
        progress_fn(
            {
                "phase": "signal_decrypt",
                "status": "decrypting",
                "progress_pct": 10,
                "total": total,
            }
        )

    # Process each attachment
    media_types = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/heic": ".heic",
        "image/heif": ".heif",
        "image/bmp": ".bmp",
        "image/tiff": ".tiff",
        "video/mp4": ".mp4",
        "video/quicktime": ".mov",
        "video/webm": ".webm",
        "video/3gpp": ".3gp",
        "audio/aac": ".aac",
        "audio/mpeg": ".mp3",
        "audio/ogg": ".ogg",
        "audio/mp4": ".m4a",
        "audio/x-m4a": ".m4a",
    }

    for idx, att in enumerate(attachments):
        if progress_fn and idx % 20 == 0:
            progress_fn(
                {
                    "phase": "signal_decrypt",
                    "status": "decrypting",
                    "progress_pct": 10 + int((idx / max(total, 1)) * 85),
                    "processed": idx,
                    "total": total,
                    "decrypted": result["decrypted"],
                }
            )

        att_path = att.get("path")
        content_type = att.get("contentType", "")
        local_key = att.get("localKey")
        file_name = att.get("fileName")

        # Skip non-media
        if not content_type or not content_type.startswith(("image/", "video/", "audio/")):
            result["skipped"] += 1
            continue

        if not att_path:
            result["skipped"] += 1
            continue

        # Resolve attachment file on disk
        full_path = _SIGNAL_ATTACH_DIR / att_path
        if not full_path.exists():
            # Try without leading directories
            for candidate in _SIGNAL_ATTACH_DIR.rglob(Path(att_path).name):
                full_path = candidate
                break
            else:
                result["skipped"] += 1
                continue

        # Determine output filename
        ext = media_types.get(content_type, "")
        if not ext and "/" in content_type:
            ext = "." + content_type.split("/")[-1]
        if file_name:
            out_name = file_name
            if not os.path.splitext(out_name)[1]:
                out_name += ext
        else:
            out_name = f"signal_{idx:05d}{ext}"

        out_path = dest / out_name
        # Avoid collisions
        if out_path.exists():
            stem = out_path.stem
            suffix = out_path.suffix
            counter = 1
            while out_path.exists():
                out_path = dest / f"{stem}_{counter}{suffix}"
                counter += 1

        try:
            encrypted_data = full_path.read_bytes()

            if local_key and len(encrypted_data) > 80:
                # Signal uses AES-256-CBC with HMAC-SHA256
                # localKey is base64-encoded: first 32 bytes = AES key, next 32 = HMAC key
                try:
                    key_material = base64.b64decode(local_key)
                    aes_key = key_material[:32]
                    hmac_key = key_material[32:64] if len(key_material) >= 64 else None
                    # First 16 bytes of encrypted data = IV
                    iv = encrypted_data[:16]
                    # Last 32 bytes = HMAC
                    stored_hmac = encrypted_data[-32:]
                    ciphertext = encrypted_data[16:-32]

                    # Verify HMAC before decryption
                    if hmac_key:
                        import hashlib as _hashlib
                        import hmac as _hmac_mod
                        expected_hmac = _hmac_mod.new(hmac_key, iv + ciphertext, _hashlib.sha256).digest()
                        if not _hmac_mod.compare_digest(stored_hmac, expected_hmac):
                            raise ValueError("HMAC verification failed — data may be corrupted or tampered")

                    cipher = Cipher(
                        algorithms.AES(aes_key),
                        modes.CBC(iv),
                        backend=default_backend(),
                    )
                    decryptor = cipher.decryptor()
                    plaintext = decryptor.update(ciphertext) + decryptor.finalize()

                    # Remove PKCS7 padding
                    pad_len = plaintext[-1] if plaintext else 0
                    if 0 < pad_len <= 16:
                        plaintext = plaintext[:-pad_len]

                    out_path.write_bytes(plaintext)
                    result["decrypted"] += 1
                    result["total_size"] += len(plaintext)
                    result["files"].append(
                        {
                            "path": str(out_path),
                            "name": out_name,
                            "size": len(plaintext),
                            "ext": ext,
                            "category": _categorize_ext(ext),
                            "content_type": content_type,
                        }
                    )
                    continue
                except Exception as e:
                    logger.debug("AES decrypt failed for %s: %s, trying raw copy", att_path, e)

            # Fallback: try raw copy (some attachments may not be encrypted)
            # Check if it's actually a valid media file via magic bytes
            detected = _detect_type_by_magic(str(full_path))
            if detected:
                ext_detected, cat = detected
                if not ext:
                    ext = ext_detected
                    out_path = out_path.with_suffix(ext)

                shutil.copy2(str(full_path), str(out_path))
                result["decrypted"] += 1
                fsize = out_path.stat().st_size
                result["total_size"] += fsize
                result["files"].append(
                    {
                        "path": str(out_path),
                        "name": out_path.name,
                        "size": fsize,
                        "ext": ext,
                        "category": cat,
                        "content_type": content_type,
                    }
                )
            else:
                result["skipped"] += 1

        except Exception as e:
            result["errors"].append(f"{att_path}: {e}")

    if progress_fn:
        progress_fn(
            {
                "phase": "complete",
                "progress_pct": 100,
                "decrypted": result["decrypted"],
                "total_size": result["total_size"],
            }
        )

    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _categorize_ext(ext: str) -> str:
    if ext in _IMAGE_EXTS:
        return "image"
    if ext in _VIDEO_EXTS:
        return "video"
    if ext in _AUDIO_EXTS:
        return "audio"
    return "other"


def _check_jpeg(path: Path) -> dict | None:
    """Check JPEG file integrity — looks for SOI marker at start and EOI at end."""
    try:
        data = path.read_bytes()
        if len(data) < 4:
            return {"issue": "truncated", "description": "Soubor je příliš malý", "repairable": False}

        # Check SOI marker (FF D8)
        if data[:2] != b"\xff\xd8":
            return {"issue": "invalid_header", "description": "Chybí JPEG SOI marker (FF D8)", "repairable": False}

        # Check EOI marker (FF D9) at end
        if data[-2:] != b"\xff\xd9":
            return {"issue": "truncated", "description": "Chybí JPEG EOI marker — soubor je pravděpodobně oříznutý", "repairable": True}

        return None
    except Exception as e:
        return {"issue": "read_error", "description": str(e), "repairable": False}


def _check_png(path: Path) -> dict | None:
    """Check PNG file — verify header signature."""
    try:
        with open(path, "rb") as f:
            header = f.read(8)
        if len(header) < 8:
            return {"issue": "truncated", "description": "Soubor je příliš malý", "repairable": False}
        # PNG signature: 89 50 4E 47 0D 0A 1A 0A
        if header != b"\x89PNG\r\n\x1a\n":
            return {"issue": "invalid_header", "description": "Chybí PNG signatura", "repairable": False}
        return None
    except Exception as e:
        return {"issue": "read_error", "description": str(e), "repairable": False}


def _check_gif(path: Path) -> dict | None:
    """Check GIF file — verify header."""
    try:
        with open(path, "rb") as f:
            header = f.read(6)
        if header not in (b"GIF87a", b"GIF89a"):
            return {"issue": "invalid_header", "description": "Chybí GIF signatura", "repairable": False}
        return None
    except Exception as e:
        return {"issue": "read_error", "description": str(e), "repairable": False}


def _check_mp4(path: Path) -> dict | None:
    """Check MP4/MOV — verify ftyp box and look for moov atom."""
    try:
        with open(path, "rb") as f:
            # Read first 12 bytes for ftyp box
            header = f.read(12)
            if len(header) < 8:
                return {"issue": "truncated", "description": "Soubor je příliš malý", "repairable": False}

            # Check for ftyp box (bytes 4-7 should be 'ftyp')
            has_ftyp = header[4:8] == b"ftyp"
            if not has_ftyp and header[4:8] not in (b"wide", b"mdat", b"moov", b"free", b"skip"):
                    return {"issue": "invalid_header", "description": "Chybí MP4 ftyp box", "repairable": False}

            # Scan for moov atom (needed for playback)
            f.seek(0)
            file_size = path.stat().st_size
            pos = 0
            found_moov = False
            max_scan = min(file_size, 100 * 1024 * 1024)  # Scan max 100MB

            while pos < max_scan:
                f.seek(pos)
                box_header = f.read(8)
                if len(box_header) < 8:
                    break
                box_size = struct.unpack(">I", box_header[:4])[0]
                box_type = box_header[4:8]

                if box_type == b"moov":
                    found_moov = True
                    break

                if box_size == 0:
                    break  # Box extends to EOF
                if box_size == 1:
                    # 64-bit extended size
                    ext_size = f.read(8)
                    if len(ext_size) < 8:
                        break
                    box_size = struct.unpack(">Q", ext_size)[0]
                if box_size < 8:
                    break  # Invalid box

                pos += box_size

            if not found_moov:
                return {
                    "issue": "missing_moov",
                    "description": "Chybí moov atom — video nelze přehrát",
                    "repairable": True,
                }

        return None
    except Exception as e:
        return {"issue": "read_error", "description": str(e), "repairable": False}


def _check_video_ffprobe(path: Path) -> dict | None:
    """Check video integrity using ffprobe."""
    try:
        from .deps import resolve_bin

        _ffprobe = resolve_bin("ffprobe") or "ffprobe"
        result = subprocess.run(
            [_ffprobe, "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=codec_type", "-of", "json", str(path)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            return {
                "issue": "ffprobe_error",
                "description": f"FFprobe error: {stderr[:200]}",
                "repairable": True,
            }
        return None
    except FileNotFoundError:
        return None  # ffprobe not available, skip
    except subprocess.TimeoutExpired:
        return {"issue": "timeout", "description": "FFprobe timeout — soubor může být poškozený", "repairable": False}


def _repair_jpeg(path: Path) -> dict:
    """Try to repair a truncated JPEG using Pillow."""
    try:
        from PIL import Image, ImageFile

        old_load_truncated = ImageFile.LOAD_TRUNCATED_IMAGES
        ImageFile.LOAD_TRUNCATED_IMAGES = True

        backup = path.with_suffix(path.suffix + ".bak")
        shutil.copy2(str(path), str(backup))

        try:
            img = Image.open(str(path))
            img.load()  # Force load
            img.save(str(path), "JPEG", quality=95)

            # Verify the repair
            issue = _check_jpeg(path)
            if issue:
                # Restore backup
                shutil.move(str(backup), str(path))
                return {"success": False, "error": "Oprava se nezdařila", "backup": str(backup)}

            return {"success": True, "path": str(path)}
        except Exception as e:
            # Restore from backup on any failure
            if backup.exists():
                shutil.move(str(backup), str(path))
            return {"success": False, "error": f"Chyba při opravě: {e}"}
        finally:
            ImageFile.LOAD_TRUNCATED_IMAGES = old_load_truncated
            # Clean up backup if it still exists (success case)
            backup.unlink(missing_ok=True)

    except Exception as e:
        return {"success": False, "error": f"Chyba při opravě: {e}"}


def _repair_video(path: Path) -> dict:
    """Try to repair a video using ffmpeg re-mux."""
    try:
        # Check ffmpeg availability
        from .deps import resolve_bin

        _ffmpeg = resolve_bin("ffmpeg") or "ffmpeg"
        subprocess.run([_ffmpeg, "-version"], capture_output=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {"success": False, "error": "FFmpeg není nainstalován"}

    backup = path.with_suffix(path.suffix + ".bak")
    repaired = path.with_stem(path.stem + "_repaired")

    try:
        shutil.copy2(str(path), str(backup))

        # Re-mux with ffmpeg — fixes missing moov, broken index
        result = subprocess.run(
            [
                _ffmpeg,
                "-y",
                "-err_detect",
                "ignore_err",
                "-i",
                str(path),
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                str(repaired),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode == 0 and repaired.exists() and repaired.stat().st_size > 0:
            # Replace original with repaired
            shutil.move(str(repaired), str(path))
            backup.unlink(missing_ok=True)
            return {"success": True, "path": str(path)}
        else:
            # Restore backup
            if backup.exists():
                shutil.move(str(backup), str(path))
            repaired.unlink(missing_ok=True)
            return {"success": False, "error": f"FFmpeg error: {result.stderr[:200]}"}

    except subprocess.TimeoutExpired:
        if backup.exists():
            shutil.move(str(backup), str(path))
        repaired.unlink(missing_ok=True)
        return {"success": False, "error": "FFmpeg timeout"}
    except Exception as e:
        if backup.exists():
            shutil.move(str(backup), str(path))
        repaired.unlink(missing_ok=True)
        return {"success": False, "error": str(e)}
