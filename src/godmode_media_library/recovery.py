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
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

_DEFAULT_QUARANTINE = Path.home() / ".config" / "gml" / "quarantine"

# Media extensions we look for during deep scan / recovery
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif", ".webp", ".heic", ".heif", ".svg", ".raw", ".cr2", ".nef", ".arw", ".dng"}
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


# ---------------------------------------------------------------------------
# 1. Quarantine browser
# ---------------------------------------------------------------------------

def list_quarantine(quarantine_root: Path | None = None) -> list[QuarantineEntry]:
    """List all files in the quarantine directory."""
    root = quarantine_root or _DEFAULT_QUARANTINE
    if not root.exists():
        return []

    entries: list[QuarantineEntry] = []
    manifest_path = root / "manifest.json"
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception:
            pass

    for fpath in sorted(root.rglob("*")):
        if fpath.is_file() and fpath.name != "manifest.json":
            ext = fpath.suffix.lower()
            stat = fpath.stat()
            original = manifest.get(str(fpath), {}).get("original_path", "unknown")
            qdate = manifest.get(str(fpath), {}).get("quarantine_date", "")
            category = _categorize_ext(ext)
            entries.append(QuarantineEntry(
                path=str(fpath),
                original_path=original,
                size=stat.st_size,
                ext=ext,
                quarantine_date=qdate,
                category=category,
            ))
    return entries


def restore_from_quarantine(
    paths: list[str],
    quarantine_root: Path | None = None,
    restore_to: str | None = None,
) -> dict:
    """Restore files from quarantine to their original location or a custom directory."""
    root = quarantine_root or _DEFAULT_QUARANTINE
    manifest_path = root / "manifest.json"
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception:
            pass

    restored = 0
    errors: list[str] = []

    for p in paths:
        src = Path(p)
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
        except Exception:
            pass

    return {"restored": restored, "errors": errors}


def delete_from_quarantine(paths: list[str], quarantine_root: Path | None = None) -> dict:
    """Permanently delete files from quarantine."""
    root = quarantine_root or _DEFAULT_QUARANTINE
    manifest_path = root / "manifest.json"
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception:
            pass

    deleted = 0
    errors: list[str] = []

    for p in paths:
        src = Path(p)
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

    try:
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    except Exception:
        pass

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
    # Application caches
    ("App cache", Path.home() / "Library" / "Caches"),
    # Screenshots
    ("Snímky obrazovky", Path.home() / "Desktop"),
    # iPhoto / Photos library
    ("Fotky (Photos Library)", Path.home() / "Pictures" / "Photos Library.photoslibrary"),
    # WhatsApp / Telegram media
    ("WhatsApp", Path.home() / "Library" / "Group Containers" / "group.net.whatsapp.WhatsApp.shared" / "Message" / "Media"),
    ("Telegram", Path.home() / "Library" / "Group Containers" / "6N38VWS5BX.ru.keepcoder.Telegram" / "appstore" / "account-*"),
]


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
            progress_fn({
                "phase": "deep_scan",
                "location": name,
                "progress_pct": int((idx / max(total_locs, 1)) * 100),
                "files_found": result.files_found,
            })

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
                        loc_files.append({
                            "path": fpath,
                            "name": fname,
                            "size": fsize,
                            "ext": ext,
                            "category": _categorize_ext(ext),
                            "location": name,
                        })
                        loc_size += fsize
                        result.files_found += 1
                        result.total_size += fsize
                    except (OSError, PermissionError):
                        continue
        except (OSError, PermissionError):
            pass

        if loc_files:
            result.locations.append({
                "name": name,
                "path": str(scan_path),
                "files_count": len(loc_files),
                "total_size": loc_size,
            })
            result.files.extend(loc_files)

    result.locations_scanned = total_locs

    if progress_fn:
        progress_fn({
            "phase": "complete",
            "progress_pct": 100,
            "files_found": result.files_found,
            "total_size": result.total_size,
        })

    return result


def recover_files(
    file_paths: list[str],
    destination: str,
    delete_source: bool = False,
) -> dict:
    """Copy or move found files to a recovery destination."""
    dest = Path(destination)
    dest.mkdir(parents=True, exist_ok=True)

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
            progress_fn({
                "phase": "integrity_check",
                "progress_pct": int((idx / max(total, 1)) * 100),
                "checked": result.total_checked,
                "corrupted": result.corrupted,
            })

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
        progress_fn({
            "phase": "complete",
            "progress_pct": 100,
            "checked": result.total_checked,
            "corrupted": result.corrupted,
        })

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
            capture_output=True, text=True, timeout=10,
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
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            # Parse plist output — simplified approach via diskutil info
            result2 = subprocess.run(
                ["diskutil", "list"],
                capture_output=True, text=True, timeout=10,
            )
            current_disk = None
            for line in result2.stdout.split("\n"):
                line = line.strip()
                if line.startswith("/dev/"):
                    current_disk = line.split()[0].rstrip(":")
                    desc = line.split("(", 1)[-1].rstrip("):") if "(" in line else ""
                    disks.append({
                        "device": current_disk,
                        "description": desc,
                        "partitions": [],
                    })
                elif current_disk and ":" in line and "Apple" in line or "Microsoft" in line or "EFI" in line:
                    parts = line.split()
                    if len(parts) >= 3:
                        if disks:
                            disks[-1]["partitions"].append({
                                "name": parts[1] if len(parts) > 1 else "",
                                "size": parts[-2] + " " + parts[-1] if len(parts) > 2 else "",
                            })
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Also list mounted volumes
    volumes_dir = Path("/Volumes")
    if volumes_dir.exists():
        for vol in sorted(volumes_dir.iterdir()):
            if vol.is_dir() and not vol.name.startswith("."):
                try:
                    usage = shutil.disk_usage(vol)
                    disks.append({
                        "device": str(vol),
                        "description": f"Volume: {vol.name}",
                        "total_size": usage.total,
                        "free_size": usage.free,
                        "partitions": [],
                    })
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

    if not output_dir:
        output_dir = str(Path.home() / "Desktop" / "GML_Recovery")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    result = PhotoRecResult(output_dir=output_dir)

    # Build photorec command
    cmd = [
        "photorec",
        "/log",
        "/d", str(out),
    ]

    # File type filter
    if file_types:
        type_str = ",".join(file_types)
        cmd.extend(["/fileopt", f"everything,disable", *[f"{ft},enable" for ft in file_types]])

    cmd.append(source)

    if progress_fn:
        progress_fn({
            "phase": "photorec",
            "status": "starting",
            "source": source,
            "output_dir": output_dir,
        })

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

        # Count recovered files
        for dirpath, _, filenames in os.walk(out):
            for fname in filenames:
                fpath = os.path.join(dirpath, fname)
                try:
                    fsize = os.path.getsize(fpath)
                    result.files.append({
                        "path": fpath,
                        "name": fname,
                        "size": fsize,
                        "ext": os.path.splitext(fname)[1].lower(),
                    })
                    result.files_recovered += 1
                    result.total_size += fsize
                except OSError:
                    pass

    except subprocess.TimeoutExpired:
        logger.warning("PhotoRec timed out after 1 hour")
    except Exception as e:
        logger.error("PhotoRec error: %s", e)

    if progress_fn:
        progress_fn({
            "phase": "complete",
            "files_recovered": result.files_recovered,
            "total_size": result.total_size,
        })

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
            if not has_ftyp:
                # Some MOV files start with 'wide' or 'mdat'
                if header[4:8] not in (b"wide", b"mdat", b"moov", b"free", b"skip"):
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
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_type", "-of", "json", str(path)],
            capture_output=True, text=True, timeout=30,
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
        ImageFile.LOAD_TRUNCATED_IMAGES = True

        backup = path.with_suffix(path.suffix + ".bak")
        shutil.copy2(str(path), str(backup))

        img = Image.open(str(path))
        img.load()  # Force load
        img.save(str(path), "JPEG", quality=95)

        # Verify the repair
        issue = _check_jpeg(path)
        if issue:
            # Restore backup
            shutil.move(str(backup), str(path))
            return {"success": False, "error": "Oprava se nezdařila", "backup": str(backup)}

        # Remove backup
        backup.unlink(missing_ok=True)
        return {"success": True, "path": str(path)}

    except Exception as e:
        return {"success": False, "error": f"Chyba při opravě: {e}"}


def _repair_video(path: Path) -> dict:
    """Try to repair a video using ffmpeg re-mux."""
    try:
        # Check ffmpeg availability
        subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {"success": False, "error": "FFmpeg není nainstalován"}

    backup = path.with_suffix(path.suffix + ".bak")
    repaired = path.with_stem(path.stem + "_repaired")

    try:
        shutil.copy2(str(path), str(backup))

        # Re-mux with ffmpeg — fixes missing moov, broken index
        result = subprocess.run(
            [
                "ffmpeg", "-y",
                "-err_detect", "ignore_err",
                "-i", str(path),
                "-c", "copy",
                "-movflags", "+faststart",
                str(repaired),
            ],
            capture_output=True, text=True, timeout=300,
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
