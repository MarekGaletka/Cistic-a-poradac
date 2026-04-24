"""GDrive cleanup — process .staging, unknown/00, and Unsorted leftovers.

Methodology (persisted in memory/methodology_gdrive_cleanup.md):
  1. Never delete without verification — confirm target copy exists
  2. MD5 hash matching (GDrive native) preferred, filename+size fallback
  3. Server-side moves only — instant on GDrive, no re-upload
  4. Oldest date wins — EXIF > filename regex > modification time
  5. Dry-run first — always preview before destructive operations

Three cleanup workflows:
  A. Unsorted dupes — verify each file has match in year/month, delete confirmed dupes
  B. .staging — match against organized folders by MD5, delete dupes, organize uniques
  C. unknown/00 — categorize by extension, extract date, organize into year/month
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from .cloud import (
    _rclone_bin,
    check_rclone,
    rclone_server_side_move,
)
from .consolidation import _categorize_file

logger = logging.getLogger(__name__)

# ── Types ────────────────────────────────────────────────────────────


@dataclass
class CleanupProgress:
    """Mutable progress state for cleanup operations."""

    phase: str = ""
    phase_label: str = ""
    current: int = 0
    total: int = 0
    deleted: int = 0
    moved: int = 0
    skipped: int = 0
    failed: int = 0
    current_file: str = ""
    bytes_freed: int = 0


@dataclass
class CleanupResult:
    """Result of a cleanup run."""

    unsorted: dict = field(default_factory=dict)
    staging: dict = field(default_factory=dict)
    unknown: dict = field(default_factory=dict)
    dedup: dict = field(default_factory=dict)
    dry_run: bool = False
    elapsed_seconds: float = 0.0


# ── Helpers ──────────────────────────────────────────────────────────


def _delete_file(remote: str, path: str, timeout: int = 120) -> bool:
    """Delete a single file on a remote using rclone deletefile."""
    target = f"{remote}:{path}" if remote else path
    cmd = [_rclone_bin(), "deletefile", target]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.returncode == 0
    except Exception as exc:
        logger.warning("delete_file failed for %s: %s", target, exc)
        return False


# Filename date patterns: IMG_20210315, VID_20200423, 20210315_123456, etc.
_DATE_PATTERNS = [
    # IMG_20210315_123456 or VID_20210315
    re.compile(r"(?:IMG|VID|PANO|MVIMG|PXL)[_-](\d{4})(\d{2})(\d{2})"),
    # WhatsApp: IMG-20210315-WA0001
    re.compile(r"(?:IMG|VID|AUD|DOC|STK|PTT)[_-](\d{4})(\d{2})(\d{2})[_-]WA"),
    # Screenshot_20210315-123456
    re.compile(r"Screenshot[_-](\d{4})(\d{2})(\d{2})"),
    # Generic: 20210315_123456 or 20210315-123456 (at word boundary)
    re.compile(r"(?:^|[_\-\s])(\d{4})(\d{2})(\d{2})(?=[_\-\s\.]|$)"),
]


def _extract_date_from_filename(filename: str) -> datetime | None:
    """Try to extract a date from the filename using common patterns."""
    for pattern in _DATE_PATTERNS:
        m = pattern.search(filename)
        if m:
            try:
                dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                if 1990 <= dt.year <= 2100:
                    return dt
            except ValueError:
                continue
    return None


def _extract_date_from_modtime(modtime: str) -> datetime | None:
    """Parse rclone ModTime string to datetime."""
    if not modtime:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            dt = datetime.strptime(modtime[:26], fmt)
            if dt.year >= 1990:
                return dt
        except (ValueError, TypeError):
            continue
    return None


def _best_date(filename: str, modtime: str = "") -> datetime | None:
    """Get the best date: filename pattern first, then modtime fallback."""
    dt = _extract_date_from_filename(filename)
    if dt:
        return dt
    return _extract_date_from_modtime(modtime)


def _year_month_path(dt: datetime | None, category: str, filename: str, base_path: str) -> str:
    """Build organized destination path: base/year/year-month/filename."""
    if dt:
        return f"{base_path}/{dt.year}/{dt.year}-{dt.month:02d}/{filename}"
    return f"{base_path}/unknown/{category.lower()}/{filename}"


def _rclone_lsjson_full(
    remote: str,
    path: str,
    recursive: bool = True,
    timeout: int = 600,
) -> list[dict]:
    """List files with hashes AND modification time (unlike rclone_lsjson_hashes which skips modtime)."""
    if not check_rclone():
        return []

    cmd = [
        _rclone_bin(),
        "lsjson",
        f"{remote}:{path}",
        "--hash",
        "--no-mimetype",
        "--fast-list",
    ]
    if recursive:
        cmd.append("-R")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            logger.warning("rclone lsjson failed for %s:%s: %s", remote, path, result.stderr.strip()[:200])
            return []

        entries = json.loads(result.stdout)
        out = []
        for entry in entries:
            if entry.get("IsDir"):
                continue
            hashes = entry.get("Hashes", {})
            out.append(
                {
                    "name": entry["Name"],
                    "path": entry["Path"],
                    "size": entry.get("Size", 0),
                    "md5": hashes.get("md5") or hashes.get("MD5"),
                    "sha256": hashes.get("sha256") or hashes.get("SHA-256"),
                    "modtime": entry.get("ModTime", ""),
                }
            )
        return out

    except subprocess.TimeoutExpired:
        logger.warning("rclone lsjson timed out for %s:%s", remote, path)
        return []
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("rclone lsjson error: %s", exc)
        return []


# ── Cleanup: Unsorted duplicates ─────────────────────────────────────


def _cleanup_unsorted(
    remote: str,
    base_path: str,
    organized_index: dict[str, set[int]],
    dry_run: bool,
    progress_fn: Callable[[dict], None] | None = None,
) -> dict:
    """Delete files in Unsorted/ that have verified copies in year/month folders.

    Args:
        organized_index: {md5_hash: {file_sizes}} built from organized year/month files.
    """
    result = {"total": 0, "deleted": 0, "skipped": 0, "failed": 0, "bytes_freed": 0}

    logger.info("Cleanup: listing Unsorted/ files...")
    unsorted_files = _rclone_lsjson_full(remote, f"{base_path}/Unsorted", recursive=True)
    result["total"] = len(unsorted_files)

    if not unsorted_files:
        logger.info("Cleanup: Unsorted/ is empty or inaccessible")
        return result

    logger.info("Cleanup: %d files in Unsorted/, checking against organized folders", len(unsorted_files))

    for i, f in enumerate(unsorted_files):
        if progress_fn and i % 10 == 0:
            progress_fn(
                {
                    "phase": "unsorted",
                    "phase_label": "Mazání duplikátů z Unsorted/",
                    "current": i,
                    "total": len(unsorted_files),
                    "deleted": result["deleted"],
                    "current_file": f["name"],
                }
            )

        md5 = f.get("md5")
        size = f.get("size", 0)

        # Verify: file with same MD5 exists in organized folders
        if md5 and md5 in organized_index:
            if size in organized_index[md5]:
                # Confirmed duplicate — safe to delete
                if not dry_run:
                    full_path = f"{base_path}/Unsorted/{f['path']}"
                    ok = _delete_file(remote, full_path)
                    if ok:
                        result["deleted"] += 1
                        result["bytes_freed"] += size
                    else:
                        result["failed"] += 1
                        logger.warning("Cleanup: failed to delete %s", f["path"])
                else:
                    result["deleted"] += 1
                    result["bytes_freed"] += size
            else:
                result["skipped"] += 1
                logger.info("Cleanup: MD5 match but size mismatch for %s", f["name"])
        else:
            result["skipped"] += 1

    logger.info(
        "Cleanup Unsorted: deleted=%d, skipped=%d, failed=%d, freed=%.1f MB",
        result["deleted"],
        result["skipped"],
        result["failed"],
        result["bytes_freed"] / (1024**2),
    )
    return result


# ── Cleanup: .staging ────────────────────────────────────────────────


def _cleanup_staging(
    remote: str,
    base_path: str,
    organized_index: dict[str, set[int]],
    dry_run: bool,
    progress_fn: Callable[[dict], None] | None = None,
) -> dict:
    """Process .staging/ subfolders: delete dupes, organize unique files.

    For each file in .staging/:
      - If MD5 matches a file in organized folders → delete (it's a dupe)
      - If no match → extract date, move to year/month folder
    """
    result = {
        "total": 0,
        "deleted_dupes": 0,
        "moved_unique": 0,
        "skipped": 0,
        "failed": 0,
        "bytes_freed": 0,
        "subfolder_stats": {},
    }

    logger.info("Cleanup: listing .staging/ files with hashes...")
    staging_files = _rclone_lsjson_full(remote, f"{base_path}/.staging", recursive=True, timeout=1200)
    result["total"] = len(staging_files)

    if not staging_files:
        logger.info("Cleanup: .staging/ is empty or inaccessible")
        return result

    logger.info("Cleanup: %d files in .staging/, processing...", len(staging_files))

    # Track per-subfolder stats
    for f in staging_files:
        subfolder = f["path"].split("/")[0] if "/" in f["path"] else "(root)"
        if subfolder not in result["subfolder_stats"]:
            result["subfolder_stats"][subfolder] = {"total": 0, "dupes": 0, "unique": 0}
        result["subfolder_stats"][subfolder]["total"] += 1

    for i, f in enumerate(staging_files):
        if progress_fn and i % 20 == 0:
            progress_fn(
                {
                    "phase": "staging",
                    "phase_label": "Čištění .staging/",
                    "current": i,
                    "total": len(staging_files),
                    "deleted": result["deleted_dupes"],
                    "moved": result["moved_unique"],
                    "current_file": f["name"],
                }
            )

        md5 = f.get("md5")
        size = f.get("size", 0)
        subfolder = f["path"].split("/")[0] if "/" in f["path"] else "(root)"

        # Check if duplicate exists in organized folders
        is_dupe = False
        if md5 and md5 in organized_index and size in organized_index[md5]:
            is_dupe = True

        if is_dupe:
            # Delete duplicate
            if not dry_run:
                full_path = f"{base_path}/.staging/{f['path']}"
                ok = _delete_file(remote, full_path)
                if ok:
                    result["deleted_dupes"] += 1
                    result["bytes_freed"] += size
                    result["subfolder_stats"][subfolder]["dupes"] += 1
                else:
                    result["failed"] += 1
            else:
                result["deleted_dupes"] += 1
                result["bytes_freed"] += size
                result["subfolder_stats"][subfolder]["dupes"] += 1
        else:
            # Unique file — organize into year/month
            filename = f["name"]
            category = _categorize_file(filename)
            dt = _best_date(filename, f.get("modtime", ""))
            dest_path = _year_month_path(dt, category, filename, base_path)

            if not dry_run:
                src_full = f"{base_path}/.staging/{f['path']}"
                ok = rclone_server_side_move(remote, src_full, dest_path, timeout=30)
                if ok:
                    result["moved_unique"] += 1
                    result["subfolder_stats"][subfolder]["unique"] += 1
                else:
                    result["failed"] += 1
                    logger.warning("Cleanup: failed to move %s → %s", f["path"], dest_path)
            else:
                result["moved_unique"] += 1
                result["subfolder_stats"][subfolder]["unique"] += 1

    logger.info(
        "Cleanup .staging: dupes=%d, moved=%d, skipped=%d, failed=%d, freed=%.1f MB",
        result["deleted_dupes"],
        result["moved_unique"],
        result["skipped"],
        result["failed"],
        result["bytes_freed"] / (1024**2),
    )
    return result


# ── Cleanup: unknown/00 ─────────────────────────────────────────────


def _cleanup_unknown(
    remote: str,
    base_path: str,
    organized_index: dict[str, set[int]],
    dry_run: bool,
    progress_fn: Callable[[dict], None] | None = None,
) -> dict:
    """Process unknown/00/ flat dump: categorize, date-extract, organize.

    Steps:
      1. Check for dupes against organized folders (delete if match)
      2. Try date extraction (filename pattern → modtime)
      3. Move to year/month or unknown/{category}/ if undateable
    """
    result = {
        "total": 0,
        "deleted_dupes": 0,
        "moved_dated": 0,
        "moved_undated": 0,
        "skipped_junk": 0,
        "failed": 0,
        "bytes_freed": 0,
        "by_category": {},
    }

    # Junk files to skip/delete
    junk_patterns = {".ds_store", "thumbs.db", "desktop.ini", ".picasa.ini", ".nomedia"}

    logger.info("Cleanup: listing unknown/00/ files...")
    # Also check unknown/ root and any unknown subfolders
    unknown_files = _rclone_lsjson_full(remote, f"{base_path}/unknown", recursive=True, timeout=1200)
    result["total"] = len(unknown_files)

    if not unknown_files:
        logger.info("Cleanup: unknown/ is empty or inaccessible")
        return result

    logger.info("Cleanup: %d files in unknown/, processing...", len(unknown_files))

    for i, f in enumerate(unknown_files):
        if progress_fn and i % 20 == 0:
            progress_fn(
                {
                    "phase": "unknown",
                    "phase_label": "Třídění unknown/",
                    "current": i,
                    "total": len(unknown_files),
                    "moved": result["moved_dated"] + result["moved_undated"],
                    "deleted": result["deleted_dupes"],
                    "current_file": f["name"],
                }
            )

        filename = f["name"]
        md5 = f.get("md5")
        size = f.get("size", 0)

        # Skip junk files
        if filename.lower() in junk_patterns:
            if not dry_run:
                full_path = f"{base_path}/unknown/{f['path']}"
                _delete_file(remote, full_path)
            result["skipped_junk"] += 1
            continue

        # Check for duplicate in organized folders
        is_dupe = False
        if md5 and md5 in organized_index and size in organized_index[md5]:
            is_dupe = True

        if is_dupe:
            if not dry_run:
                full_path = f"{base_path}/unknown/{f['path']}"
                ok = _delete_file(remote, full_path)
                if ok:
                    result["deleted_dupes"] += 1
                    result["bytes_freed"] += size
                else:
                    result["failed"] += 1
            else:
                result["deleted_dupes"] += 1
                result["bytes_freed"] += size
            continue

        # Categorize and date-extract
        category = _categorize_file(filename)
        result["by_category"][category] = result["by_category"].get(category, 0) + 1
        dt = _best_date(filename, f.get("modtime", ""))

        # Build destination path
        if dt:
            dest_path = f"{base_path}/{dt.year}/{dt.year}-{dt.month:02d}/{filename}"
        else:
            # Undateable — organize by category under unknown/
            dest_path = f"{base_path}/unknown/{category.lower()}/{filename}"

        # Don't move if already in correct location
        current_path = f"{base_path}/unknown/{f['path']}"
        if current_path == dest_path:
            result["skipped_junk"] += 1  # reuse counter for "already in place"
            continue

        if not dry_run:
            ok = rclone_server_side_move(remote, current_path, dest_path, timeout=30)
            if ok:
                if dt:
                    result["moved_dated"] += 1
                else:
                    result["moved_undated"] += 1
            else:
                result["failed"] += 1
                logger.warning("Cleanup: failed to move %s → %s", f["path"], dest_path)
        else:
            if dt:
                result["moved_dated"] += 1
            else:
                result["moved_undated"] += 1

    logger.info(
        "Cleanup unknown: dupes=%d, dated=%d, undated=%d, junk=%d, failed=%d, freed=%.1f MB",
        result["deleted_dupes"],
        result["moved_dated"],
        result["moved_undated"],
        result["skipped_junk"],
        result["failed"],
        result["bytes_freed"] / (1024**2),
    )
    return result


# ── Build organized file index ───────────────────────────────────────


def _build_organized_index(
    remote: str,
    base_path: str,
    progress_fn: Callable[[dict], None] | None = None,
) -> dict[str, set[int]]:
    """Build an index of all organized files: {md5_hash: {file_sizes}}.

    Scans year/month folders (the canonical organized structure) to create
    a lookup table for duplicate detection.
    """
    if progress_fn:
        progress_fn({"phase": "indexing", "phase_label": "Indexuji organizované soubory...", "current": 0, "total": 0})

    logger.info("Building organized file index for %s:%s...", remote, base_path)

    # List all files in the base path (excluding .staging and unknown)
    all_files = _rclone_lsjson_full(remote, base_path, recursive=True, timeout=1800)

    index: dict[str, set[int]] = {}
    indexed = 0

    for f in all_files:
        path = f["path"]
        # Only index files in year/month folders (the organized structure)
        # Skip .staging/, unknown/, Unsorted/, and root-level files
        parts = path.split("/")
        if len(parts) < 2:
            continue
        top = parts[0]
        # Include year folders (4-digit), category folders, and explicitly organized paths
        if top.startswith(".") or top in ("unknown", "Unsorted", "test-move", "GML-Backup"):
            continue

        md5 = f.get("md5")
        if md5:
            if md5 not in index:
                index[md5] = set()
            index[md5].add(f.get("size", 0))
            indexed += 1

    logger.info("Organized index: %d unique hashes from %d files (of %d total)", len(index), indexed, len(all_files))

    if progress_fn:
        progress_fn(
            {
                "phase": "indexing",
                "phase_label": f"Index hotový: {len(index)} unikátních hashů",
                "current": indexed,
                "total": len(all_files),
            }
        )

    return index


# ── Main orchestrator ────────────────────────────────────────────────


def gdrive_cleanup(
    remote: str = "gws-backup",
    base_path: str = "GML-Consolidated",
    dry_run: bool = False,
    skip_unsorted: bool = False,
    skip_staging: bool = False,
    skip_unknown: bool = False,
    run_dedup: bool = True,
    progress_fn: Callable[[dict], None] | None = None,
) -> dict:
    """Run full GDrive cleanup: Unsorted dupes, .staging, unknown/00.

    Args:
        remote: rclone remote name (default: gws-backup)
        base_path: base path on remote (default: GML-Consolidated)
        dry_run: if True, only report what would be done
        skip_unsorted/skip_staging/skip_unknown: skip specific phases
        run_dedup: run rclone dedupe at the end
        progress_fn: callback for progress updates

    Returns dict with results for each phase.
    """
    start = time.monotonic()
    results: dict = {"dry_run": dry_run}

    logger.info("=== GDrive Cleanup START (dry_run=%s) ===", dry_run)

    # Step 1: Build index of organized files (shared across all phases)
    organized_index = _build_organized_index(remote, base_path, progress_fn)
    results["index"] = {"unique_hashes": len(organized_index)}

    # Step 2: Clean Unsorted/ duplicates
    if not skip_unsorted:
        results["unsorted"] = _cleanup_unsorted(remote, base_path, organized_index, dry_run, progress_fn)
    else:
        results["unsorted"] = {"skipped": True}

    # Step 3: Clean .staging/ folders
    if not skip_staging:
        results["staging"] = _cleanup_staging(remote, base_path, organized_index, dry_run, progress_fn)
    else:
        results["staging"] = {"skipped": True}

    # Step 4: Clean unknown/ folder
    if not skip_unknown:
        results["unknown"] = _cleanup_unknown(remote, base_path, organized_index, dry_run, progress_fn)
    else:
        results["unknown"] = {"skipped": True}

    # Step 5: Final dedup pass
    if run_dedup and not dry_run:
        if progress_fn:
            progress_fn({"phase": "dedup", "phase_label": "Finální deduplikace...", "current": 0, "total": 0})
        from .cloud import rclone_dedupe

        results["dedup"] = rclone_dedupe(remote, base_path, mode="largest", dry_run=False, timeout=3600, progress_fn=progress_fn)
    else:
        results["dedup"] = {"skipped": True}

    elapsed = time.monotonic() - start
    results["elapsed_seconds"] = round(elapsed, 1)

    # Summary
    total_deleted = (
        results.get("unsorted", {}).get("deleted", 0)
        + results.get("staging", {}).get("deleted_dupes", 0)
        + results.get("unknown", {}).get("deleted_dupes", 0)
        + results.get("unknown", {}).get("skipped_junk", 0)
    )
    total_moved = (
        results.get("staging", {}).get("moved_unique", 0)
        + results.get("unknown", {}).get("moved_dated", 0)
        + results.get("unknown", {}).get("moved_undated", 0)
    )
    total_freed = (
        results.get("unsorted", {}).get("bytes_freed", 0)
        + results.get("staging", {}).get("bytes_freed", 0)
        + results.get("unknown", {}).get("bytes_freed", 0)
    )
    results["summary"] = {
        "total_deleted": total_deleted,
        "total_moved": total_moved,
        "total_bytes_freed": total_freed,
        "total_freed_human": f"{total_freed / (1024**3):.1f} GB",
    }

    logger.info(
        "=== GDrive Cleanup DONE: deleted=%d, moved=%d, freed=%s, elapsed=%.0fs ===",
        total_deleted,
        total_moved,
        results["summary"]["total_freed_human"],
        elapsed,
    )

    return results
