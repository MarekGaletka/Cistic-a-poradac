"""Core reorganization module for media library consolidation.

Provides source detection, plan generation, and execution for reorganizing
media files from multiple sources into a unified, deduplicated directory
structure.
"""

from __future__ import annotations

import contextlib
import logging
import shutil
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from .tree_ops import _date_bucket, _file_category
from .utils import ensure_dir, iter_files, safe_stat_birthtime, sha256_file

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

StatusType = Literal["planned", "copied", "moved", "skipped", "error"]
StructurePattern = Literal["year_month", "year_type", "year_month_day", "type_year", "flat"]


@dataclass
class ReorganizeConfig:
    """Configuration for a reorganization run."""

    sources: list[Path]
    destination: Path
    structure_pattern: StructurePattern = "year_month"
    deduplicate: bool = True
    merge_metadata: bool = True
    delete_originals: bool = False
    dry_run: bool = True
    workers: int = 4
    min_size_bytes: int = 0
    exclude_patterns: list[str] = field(default_factory=list)


@dataclass
class ReorganizeFileEntry:
    """A single file tracked through the reorganization pipeline."""

    source_path: Path
    destination_path: Path | None = None
    file_size: int = 0
    file_ext: str = ""
    file_category: str = ""
    sha256: str | None = None
    is_duplicate: bool = False
    duplicate_of: Path | None = None
    date_bucket: str = ""
    status: StatusType = "planned"


@dataclass
class ReorganizePlan:
    """Full reorganization plan with statistics."""

    config: ReorganizeConfig
    entries: list[ReorganizeFileEntry] = field(default_factory=list)
    total_files: int = 0
    unique_files: int = 0
    duplicate_files: int = 0
    total_size: int = 0
    unique_size: int = 0
    duplicate_size: int = 0
    categories: dict[str, int] = field(default_factory=dict)
    source_stats: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


@dataclass
class ReorganizeResult:
    """Outcome of executing a reorganization plan."""

    files_processed: int = 0
    files_copied: int = 0
    files_skipped: int = 0
    originals_deleted: int = 0
    space_saved: int = 0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Source detection
# ---------------------------------------------------------------------------

_HOME = Path.home()

_WELL_KNOWN_DIRS = [
    ("Pictures", _HOME / "Pictures", "mac"),
    ("Desktop", _HOME / "Desktop", "mac"),
    ("Downloads", _HOME / "Downloads", "mac"),
    ("Movies", _HOME / "Movies", "mac"),
    ("Music", _HOME / "Music", "mac"),
    ("Documents", _HOME / "Documents", "mac"),
]

_ICLOUD_DIR = _HOME / "Library" / "Mobile Documents"
_VOLUMES = Path("/Volumes")


def _quick_file_count(path: Path, limit: int = 5000) -> int:
    """Return a quick estimate of file count (stop after *limit*)."""
    count = 0
    try:
        for _ in path.rglob("*"):
            count += 1
            if count >= limit:
                return count
    except (PermissionError, OSError):
        pass
    return count


def detect_sources() -> list[dict]:
    """Discover available media sources on this machine.

    Returns a list of dicts with keys:
        name, path, icon, type (mac|external|iphone|icloud), available, file_count
    """
    sources: list[dict] = []

    # Well-known user directories
    for name, path, src_type in _WELL_KNOWN_DIRS:
        available = path.is_dir()
        sources.append(
            {
                "name": name,
                "path": path,
                "icon": _icon_for(src_type),
                "type": src_type,
                "available": available,
                "file_count": _quick_file_count(path) if available else 0,
            }
        )

    # iCloud Mobile Documents
    if _ICLOUD_DIR.is_dir():
        sources.append(
            {
                "name": "iCloud Drive",
                "path": _ICLOUD_DIR,
                "icon": _icon_for("icloud"),
                "type": "icloud",
                "available": True,
                "file_count": _quick_file_count(_ICLOUD_DIR),
            }
        )

    # External / mounted volumes
    if _VOLUMES.is_dir():
        try:
            for vol in sorted(_VOLUMES.iterdir()):
                if vol.name == "Macintosh HD":
                    continue
                if not vol.is_dir():
                    continue
                vol_type = "iphone" if _looks_like_iphone(vol) else "external"
                sources.append(
                    {
                        "name": vol.name,
                        "path": vol,
                        "icon": _icon_for(vol_type),
                        "type": vol_type,
                        "available": True,
                        "file_count": _quick_file_count(vol),
                    }
                )
        except PermissionError:
            logger.warning("Cannot list /Volumes — permission denied")

    return sources


def _icon_for(src_type: str) -> str:
    return {
        "mac": "folder",
        "external": "hard-drive",
        "iphone": "smartphone",
        "icloud": "cloud",
    }.get(src_type, "folder")


def _looks_like_iphone(vol: Path) -> bool:
    """Heuristic: iPhones expose DCIM at root level."""
    return (vol / "DCIM").is_dir()


# ---------------------------------------------------------------------------
# Plan generation
# ---------------------------------------------------------------------------


def _should_exclude(path: Path, patterns: list[str]) -> bool:
    """Return True if *path* matches any exclude pattern."""
    path_str = str(path)
    name = path.name
    for pat in patterns:
        if pat.startswith(".") and name.startswith(pat):
            return True
        if pat in path_str:
            return True
    return bool(name.startswith(".") and ".*" in patterns)


def _origin_timestamp(path: Path) -> float:
    """Best-effort origin timestamp (birthtime > mtime)."""
    try:
        birth = safe_stat_birthtime(path)
        if birth is not None and birth > 0:
            return birth
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _compute_destination(
    entry: ReorganizeFileEntry,
    dest_root: Path,
    pattern: StructurePattern,
) -> Path:
    """Compute the destination path for a file based on the structure pattern."""
    import datetime as dt

    ts = _origin_timestamp(entry.source_path)
    d = dt.datetime.fromtimestamp(ts) if ts > 0 else dt.datetime(2000, 1, 1)
    cat = entry.file_category
    name = entry.source_path.name

    if pattern == "year_month":
        return dest_root / f"{d:%Y}" / f"{d:%m}" / name
    elif pattern == "year_type":
        return dest_root / f"{d:%Y}" / cat / name
    elif pattern == "year_month_day":
        return dest_root / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}" / name
    elif pattern == "type_year":
        return dest_root / cat / f"{d:%Y}" / name
    elif pattern == "flat":
        return dest_root / name
    else:
        return dest_root / f"{d:%Y}" / f"{d:%m}" / name


def _resolve_collision(desired: Path, reserved: set[Path]) -> Path:
    """If *desired* is taken (on disk or reserved), append _1, _2, etc."""
    if desired not in reserved and not desired.exists():
        reserved.add(desired)
        return desired

    stem = desired.stem
    suffix = desired.suffix
    parent = desired.parent
    n = 1
    while True:
        candidate = parent / f"{stem}_{n}{suffix}"
        if candidate not in reserved and not candidate.exists():
            reserved.add(candidate)
            return candidate
        n += 1


def plan_reorganization(
    config: ReorganizeConfig,
    catalog_path: Path | None = None,
    progress_fn: Callable[[dict], None] | None = None,
) -> ReorganizePlan:
    """Build a full reorganization plan.

    Phases:
        1. Discovery — collect files from all sources
        2. Hashing — parallel SHA-256
        3. Dedup — identify duplicates
        4. Path planning — compute destinations with collision handling
    """
    plan = ReorganizePlan(config=config)
    effective_workers = max(1, config.workers)

    # ------------------------------------------------------------------
    # Phase 1: Discovery
    # ------------------------------------------------------------------
    if progress_fn:
        progress_fn({"phase": "discovery", "current": 0, "total": 0, "current_file": ""})

    raw_paths: list[Path] = []
    for path in iter_files(config.sources):
        if path.stat().st_size < config.min_size_bytes:
            continue
        if _should_exclude(path, config.exclude_patterns):
            continue
        raw_paths.append(path)

    logger.info("Discovery found %d files across %d sources", len(raw_paths), len(config.sources))

    entries: list[ReorganizeFileEntry] = []
    for _idx, path in enumerate(raw_paths):
        try:
            st = path.stat()
        except OSError as exc:
            plan.errors.append(f"Cannot stat {path}: {exc}")
            continue

        ext = path.suffix.lower().lstrip(".")
        cat = _file_category(ext)
        ts = _origin_timestamp(path)

        entry = ReorganizeFileEntry(
            source_path=path,
            file_size=int(st.st_size),
            file_ext=ext,
            file_category=cat,
            date_bucket=_date_bucket(ts, "month") if ts > 0 else "Unknown",
        )
        entries.append(entry)

        # Per-source stats
        for src in config.sources:
            try:
                path.relative_to(src)
                key = str(src)
                plan.source_stats[key] = plan.source_stats.get(key, 0) + 1
                break
            except ValueError:
                continue

        # Per-category stats
        plan.categories[cat] = plan.categories.get(cat, 0) + 1

    plan.entries = entries
    plan.total_files = len(entries)
    plan.total_size = sum(e.file_size for e in entries)

    if progress_fn:
        progress_fn({"phase": "discovery", "current": len(entries), "total": len(entries), "current_file": ""})

    # ------------------------------------------------------------------
    # Phase 2: Hashing
    # ------------------------------------------------------------------
    if config.deduplicate:
        hashable = [(i, e) for i, e in enumerate(entries) if e.file_size >= config.min_size_bytes]
        total_hash = len(hashable)
        hashed_count = 0

        if progress_fn:
            progress_fn({"phase": "hashing", "current": 0, "total": total_hash, "current_file": ""})

        def _hash_one(idx: int, entry: ReorganizeFileEntry) -> tuple[int, str | None]:
            try:
                return idx, sha256_file(entry.source_path)
            except OSError as exc:
                logger.warning("Cannot hash %s: %s", entry.source_path, exc)
                return idx, None

        if effective_workers > 1 and total_hash > 0:
            with ThreadPoolExecutor(max_workers=effective_workers) as pool:
                futures = {pool.submit(_hash_one, idx, entry): idx for idx, entry in hashable}
                for future in as_completed(futures):
                    idx, digest = future.result()
                    entries[idx].sha256 = digest
                    hashed_count += 1
                    if progress_fn and hashed_count % 50 == 0:
                        progress_fn(
                            {
                                "phase": "hashing",
                                "current": hashed_count,
                                "total": total_hash,
                                "current_file": str(entries[idx].source_path),
                            }
                        )
        else:
            for idx, entry in hashable:
                _, digest = _hash_one(idx, entry)
                entries[idx].sha256 = digest
                hashed_count += 1
                if progress_fn and hashed_count % 50 == 0:
                    progress_fn(
                        {
                            "phase": "hashing",
                            "current": hashed_count,
                            "total": total_hash,
                            "current_file": str(entry.source_path),
                        }
                    )

        if progress_fn:
            progress_fn({"phase": "hashing", "current": total_hash, "total": total_hash, "current_file": ""})

    # ------------------------------------------------------------------
    # Phase 3: Dedup
    # ------------------------------------------------------------------
    if config.deduplicate:
        hash_groups: dict[str, list[int]] = defaultdict(list)
        for idx, entry in enumerate(entries):
            if entry.sha256:
                hash_groups[entry.sha256].append(idx)

        for _sha, indices in hash_groups.items():
            if len(indices) < 2:
                continue
            # Pick the "richest" file: largest size as proxy for richest metadata
            indices.sort(key=lambda i: entries[i].file_size, reverse=True)
            primary_idx = indices[0]
            for dup_idx in indices[1:]:
                entries[dup_idx].is_duplicate = True
                entries[dup_idx].duplicate_of = entries[primary_idx].source_path
                entries[dup_idx].status = "skipped"

    unique = [e for e in entries if not e.is_duplicate]
    duplicates = [e for e in entries if e.is_duplicate]
    plan.unique_files = len(unique)
    plan.duplicate_files = len(duplicates)
    plan.unique_size = sum(e.file_size for e in unique)
    plan.duplicate_size = sum(e.file_size for e in duplicates)

    # ------------------------------------------------------------------
    # Phase 4: Path planning
    # ------------------------------------------------------------------
    if progress_fn:
        progress_fn({"phase": "planning", "current": 0, "total": plan.unique_files, "current_file": ""})

    reserved: set[Path] = set()
    planned = 0
    for entry in entries:
        if entry.is_duplicate:
            continue

        desired = _compute_destination(entry, config.destination, config.structure_pattern)
        final = _resolve_collision(desired, reserved)
        entry.destination_path = final
        planned += 1

        if progress_fn and planned % 200 == 0:
            progress_fn(
                {
                    "phase": "planning",
                    "current": planned,
                    "total": plan.unique_files,
                    "current_file": str(entry.source_path),
                }
            )

    if progress_fn:
        progress_fn({"phase": "planning", "current": plan.unique_files, "total": plan.unique_files, "current_file": ""})

    logger.info(
        "Plan ready: %d total, %d unique, %d duplicates, %s total size",
        plan.total_files,
        plan.unique_files,
        plan.duplicate_files,
        _human_size(plan.total_size),
    )
    return plan


# ---------------------------------------------------------------------------
# Plan execution
# ---------------------------------------------------------------------------


def execute_reorganization(
    plan: ReorganizePlan,
    progress_fn: Callable[[dict], None] | None = None,
) -> ReorganizeResult:
    """Execute a reorganization plan by copying files to their destinations.

    Respects ``plan.config.dry_run`` — if True, no files are written.
    """
    result = ReorganizeResult()
    config = plan.config
    actionable = [e for e in plan.entries if not e.is_duplicate and e.destination_path]
    total = len(actionable)

    if progress_fn:
        progress_fn({"phase": "executing", "current": 0, "total": total, "current_file": ""})

    successfully_copied: list[ReorganizeFileEntry] = []

    for idx, entry in enumerate(actionable):
        result.files_processed += 1

        if config.dry_run:
            entry.status = "planned"
            result.files_skipped += 1
            if progress_fn and (idx + 1) % 50 == 0:
                progress_fn(
                    {
                        "phase": "executing",
                        "current": idx + 1,
                        "total": total,
                        "current_file": str(entry.source_path),
                    }
                )
            continue

        src = entry.source_path
        dst = entry.destination_path
        assert dst is not None  # guarded above

        try:
            if not src.exists():
                entry.status = "error"
                result.errors.append(f"Source missing: {src}")
                result.files_skipped += 1
                continue

            ensure_dir(dst.parent)
            shutil.copy2(str(src), str(dst))

            # Verify copy
            try:
                src_size = src.stat().st_size
                dst_size = dst.stat().st_size
                if src_size != dst_size:
                    entry.status = "error"
                    result.errors.append(f"Size mismatch after copy: {src} ({src_size}) -> {dst} ({dst_size})")
                    # Remove bad copy
                    with contextlib.suppress(OSError):
                        dst.unlink()
                    result.files_skipped += 1
                    continue
            except OSError as exc:
                entry.status = "error"
                result.errors.append(f"Cannot verify copy {dst}: {exc}")
                result.files_skipped += 1
                continue

            entry.status = "copied"
            result.files_copied += 1
            successfully_copied.append(entry)

        except PermissionError as exc:
            entry.status = "error"
            result.errors.append(f"Permission denied: {src} -> {dst}: {exc}")
            result.files_skipped += 1
        except OSError as exc:
            entry.status = "error"
            result.errors.append(f"OS error copying {src} -> {dst}: {exc}")
            result.files_skipped += 1

        if progress_fn and (idx + 1) % 50 == 0:
            progress_fn(
                {
                    "phase": "executing",
                    "current": idx + 1,
                    "total": total,
                    "current_file": str(entry.source_path),
                }
            )

    if progress_fn:
        progress_fn({"phase": "executing", "current": total, "total": total, "current_file": ""})

    # ------------------------------------------------------------------
    # Delete originals (only if ALL copies succeeded and flag is set)
    # ------------------------------------------------------------------
    if config.delete_originals and not config.dry_run:
        if progress_fn:
            progress_fn({"phase": "cleanup", "current": 0, "total": len(successfully_copied), "current_file": ""})

        for idx, entry in enumerate(successfully_copied):
            try:
                entry.source_path.unlink()
                entry.status = "moved"
                result.originals_deleted += 1
                result.space_saved += entry.file_size
            except PermissionError as exc:
                result.errors.append(f"Cannot delete original {entry.source_path}: {exc}")
            except OSError as exc:
                result.errors.append(f"Cannot delete original {entry.source_path}: {exc}")

            if progress_fn and (idx + 1) % 50 == 0:
                progress_fn(
                    {
                        "phase": "cleanup",
                        "current": idx + 1,
                        "total": len(successfully_copied),
                        "current_file": str(entry.source_path),
                    }
                )

        if progress_fn:
            progress_fn(
                {
                    "phase": "cleanup",
                    "current": len(successfully_copied),
                    "total": len(successfully_copied),
                    "current_file": "",
                }
            )

    # Count space saved from dedup (skipped duplicates)
    if config.deduplicate:
        result.space_saved += plan.duplicate_size

    logger.info(
        "Reorganization complete: %d processed, %d copied, %d skipped, %d deleted, %s saved",
        result.files_processed,
        result.files_copied,
        result.files_skipped,
        result.originals_deleted,
        _human_size(result.space_saved),
    )
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _human_size(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024  # type: ignore[assignment]
    return f"{size_bytes:.1f} PB"
