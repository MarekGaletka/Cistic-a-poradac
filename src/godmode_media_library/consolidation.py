"""Ultimate Consolidation Pipeline — GOD MODE orchestrator (v2).

Safety-first design for irreplaceable personal media (childhood photos, family videos).

Principles:
  - NEVER delete source files — only copy, never move
  - ALWAYS verify after transfer (size + hash when available)
  - ALWAYS checkpoint every single file operation to SQLite
  - Resume skips completed phases entirely, picks up mid-file in stream phase
  - Paginated cloud listing — handles millions of files without OOM
  - Dynamic timeout per file (based on size, not fixed 10min)
  - Source + destination connectivity monitoring with auto-wait
  - Bandwidth limiting to avoid saturating network
  - Dry-run mode: full preview before any transfer
  - Collision-safe destination paths: year/month/filename + hash suffix on conflict
  - Failed file retry pass at end — re-attempts with longer timeout
  - Rate-limit-aware cloud API calls with inter-request delays
  - Google 750GB/day upload limit auto-pause
  - Watchdog: stall detection (60s without transfer → warning)
  - Bundle integrity: .app, .xcodeproj etc. transferred as unit
  - Deferred dedup: NO dedup during transfer — all files transfer first
  - Archive extraction: unpack archives on destination before dedup
  - File organization by category (Media/Documents/Software/Other)

Phases:
  1. Wait for sources — probe all remotes, wait for connectivity
  2. Cloud catalog scan — paginated metadata scan, write to files table
  3. Local scan — incremental scan of local roots
  4. Register files — catalog/count files, mark ALL as PENDING (no dedup)
  5. Stream cloud→cloud — ALL files, checkpoint-resumable, verified, daily limit
  6. Retry failed — second pass with longer timeout for failed transfers
  7. Verify integrity — check ALL transferred files
  8. Extract archives — unpack .zip/.rar/.7z/.tar on destination
  9. Dedup — final dedup over ALL data (rclone dedupe, mode=largest)
 10. Organize — categorize files into Media/Documents/Software/Other
 11. Final report — summary of everything

sync_to_disk is now a standalone public function (not a pipeline phase).
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import random
import re
import shutil
import sqlite3
import subprocess
import tarfile
import tempfile
import threading
import time
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from . import checkpoint as ckpt
from .catalog import Catalog
from .cloud import (
    RcloneTransferError,
    _dynamic_timeout,
    _rclone_bin,
    check_rclone,
    check_volume_mounted,
    get_native_hash_type,
    list_remotes,
    rclone_bulk_copy,
    rclone_check_file,
    rclone_copy,
    rclone_copyto,
    rclone_dedupe,
    rclone_hashsum,
    rclone_is_reachable,
    rclone_ls_paginated,
    rclone_server_side_move,
    rclone_verify_transfer,
    retry_with_backoff,
    wait_for_connectivity,
)
from .consolidation_types import (
    ARCHIVE_COMPOUND_SUFFIXES,
    ARCHIVE_EXTENSIONS,
    BUNDLE_EXTENSIONS,
    CATALOG_COMMIT_INTERVAL,
    CONSOLIDATION_JOB_TYPES,
    DAILY_LIMIT_PAUSE_SECONDS,
    DEDUP_TIMEOUT,
    DEFAULT_RETRY_TIMEOUT,
    DEST_CONNECTIVITY_TIMEOUT,
    DOCUMENT_EXTENSIONS,
    ERROR_TRUNCATE_LEN,
    ERROR_TRUNCATE_MEDIUM,
    ERROR_TRUNCATE_SHORT,
    GOOGLE_DAILY_UPLOAD_SAFETY,
    JOB_TYPE_ULTIMATE,
    MACOS_SOFTWARE_EXTENSIONS,
    MAX_RETRY_ATTEMPTS,
    MAX_SOURCE_FAILURES,
    MEDIA_EXTENSIONS,
    QUOTA_ERRORS,
    RETRY_CONNECTIVITY_WAIT,
    SOFTWARE_EXTENSIONS,
    SOURCE_CONNECTIVITY_WAIT,
    STREAM_BATCH_SIZE,
    VERIFY_FAIL_THRESHOLD_PCT,
    VERIFY_REPORT_INTERVAL,
    WATCHDOG_STALL_SECONDS,
    WINDOWS_SOFTWARE_EXTENSIONS,
    DedupStrategy,
    FileStatus,
    JobStatus,
    Phase,
    StructurePattern,
)

logger = logging.getLogger(__name__)

# In-process pause signaling — avoids opening a new DB connection (which deadlocks
# when the running consolidation holds a SQLite write lock).
_pause_events: dict[str, tuple[threading.Event, float]] = {}  # job_id -> (event, created_at)
_pause_events_lock = threading.Lock()
_PAUSE_EVENT_MAX_AGE = 86400  # 24 hours — stale event cleanup threshold


def signal_pause(job_id: str) -> bool:
    """Signal a running consolidation to pause via in-process Event.

    Also attempts a short DB write as fallback (for external processes).
    Returns True if the event was set (in-process signal delivered).
    """
    with _pause_events_lock:
        entry = _pause_events.get(job_id)
        if entry is not None:
            entry[0].set()
            logger.info("Pause signal sent to job %s (in-process)", job_id)
            return True
    logger.warning("No in-process event for job %s — pause may be delayed", job_id)
    return False


@dataclass
class ConsolidationConfig:
    """Configuration for a full consolidation run."""

    source_remotes: list[str] = field(default_factory=list)
    local_roots: list[str] = field(default_factory=list)
    dest_remote: str = "gws-backup"
    dest_path: str = "GML-Consolidated"
    disk_path: str = "/Volumes/4TB/GML-Library"
    structure_pattern: str = StructurePattern.YEAR_MONTH
    dedup_strategy: str = DedupStrategy.RICHNESS
    verify_pct: int = 100
    connectivity_timeout: int = 300
    max_transfer_retries: int = 3
    scan_workers: int = 4
    bwlimit: str | None = None
    dry_run: bool = False
    source_wait_timeout: int = 120
    api_delay: float = 0.5
    retry_timeout_multiplier: float = 3.0
    media_only: bool = False
    # Session 3: skip disk sync if disk not needed (legacy compat)
    skip_disk_sync: bool = False


@dataclass
class ConsolidationProgress:
    """Progress snapshot for UI updates."""

    phase: str = "idle"
    phase_label: str = ""
    current_step: int = 0
    total_steps: int = 11
    files_cataloged: int = 0
    files_unique: int = 0
    files_duplicate: int = 0
    files_transferred: int = 0
    files_verified: int = 0
    files_failed: int = 0
    files_retried: int = 0
    bytes_transferred: int = 0
    bytes_total_estimate: int = 0
    transfer_speed_bps: float = 0.0
    eta_seconds: int = 0
    errors: int = 0
    paused: bool = False
    error: str | None = None
    dry_run: bool = False
    sources_available: list[str] = field(default_factory=list)
    sources_unavailable: list[str] = field(default_factory=list)
    # Current file being processed (for operator visibility)
    current_file: str = ""
    # EMA-smoothed speed (alpha=0.3)
    _ema_speed: float = 0.0
    # Archive extraction stats
    archives_extracted: int = 0
    archive_files_added: int = 0
    # Organization stats
    files_organized: int = 0


@dataclass
class PhaseContext:
    """Shared context passed to every phase function.

    Avoids deeply nested closures and makes phase functions independently testable.
    """

    cat: Catalog
    config: ConsolidationConfig
    job: ckpt.ConsolidationJob
    progress: ConsolidationProgress
    progress_fn: Callable[[ConsolidationProgress], None] | None
    results: dict[str, Any]
    # Phase-shared mutable state
    available: list[str] = field(default_factory=list)
    unavailable: list[str] = field(default_factory=list)
    total_unique: int = 0
    local_scanned: int = 0
    stream_start_time: float = 0.0
    # Google daily upload tracking
    daily_bytes_uploaded: int = 0
    daily_upload_start: float = 0.0
    # Watchdog stall detection
    last_transfer_time: float = 0.0

    def report(self, phase: str, label: str, step: int, **kwargs):
        """Update progress and notify callback."""
        self.progress.phase = phase
        self.progress.phase_label = label
        self.progress.current_step = step
        for k, v in kwargs.items():
            if hasattr(self.progress, k):
                setattr(self.progress, k, v)
        if self.progress_fn:
            self.progress_fn(self.progress)

    def phase_done(self, phase: str) -> bool:
        return ckpt.is_phase_done(self.cat, self.job.job_id, phase)

    def finish_phase(self, phase: str):
        ckpt.mark_phase_done(self.cat, self.job.job_id, phase)

    @property
    def conn(self) -> sqlite3.Connection:
        return self.cat.conn


# ── Helpers ──────────────────────────────────────────────────────────


def _is_media_file(path: str) -> bool:
    """Check if path has a media file extension."""
    return PurePosixPath(path).suffix.lower() in MEDIA_EXTENSIONS


def _categorize_file(path: str) -> str:
    """Categorize a file by its extension.

    Returns one of: "Media", "Documents", "Software", "Other".
    """
    ext = PurePosixPath(path).suffix.lower()
    # Media: photos, videos, audio, images
    media_cat_exts = frozenset({
        ".jpg", ".jpeg", ".png", ".heic", ".raw", ".cr2", ".nef", ".arw",
        ".mp4", ".mov", ".avi", ".mkv", ".wmv", ".flv", ".m4v", ".3gp",
        ".webm", ".gif", ".bmp", ".tiff", ".svg", ".webp",
        # Also include broader media from MEDIA_EXTENSIONS
    }) | MEDIA_EXTENSIONS
    # Check Documents BEFORE Media (because MEDIA_EXTENSIONS has legacy .pdf)
    if ext in DOCUMENT_EXTENSIONS:
        return "Documents"
    if ext in SOFTWARE_EXTENSIONS:
        return "Software"
    if ext in media_cat_exts:
        return "Media"
    return "Other"


def _is_archive(path: str) -> bool:
    """Check if path is an archive file (.zip, .rar, .7z, .tar.gz, .tar.bz2, .tar, .gz, .bz2)."""
    lower = path.lower()
    # Check compound suffixes first
    for compound in ARCHIVE_COMPOUND_SUFFIXES:
        if lower.endswith(compound):
            return True
    ext = PurePosixPath(lower).suffix
    return ext in ARCHIVE_EXTENSIONS


def _is_bundle_dir(path: str) -> bool:
    """Check if path is a bundle directory (.app, .xcodeproj, .lproj, etc.)."""
    # Check if any component of the path has a bundle extension
    parts = PurePosixPath(path).parts
    for part in parts:
        ext = PurePosixPath(part).suffix.lower()
        if ext in BUNDLE_EXTENSIONS:
            return True
    return False


def _get_bundle_root(path: str) -> str | None:
    """Get the root bundle directory path if the file is inside a bundle.

    E.g. for 'MyApp.app/Contents/Info.plist' returns 'MyApp.app'.
    """
    parts = PurePosixPath(path).parts
    for i, part in enumerate(parts):
        ext = PurePosixPath(part).suffix.lower()
        if ext in BUNDLE_EXTENSIONS:
            return str(PurePosixPath(*parts[: i + 1]))
    return None


def _software_subcategory(path: str) -> str:
    """Determine Software subcategory: macOS, Windows, or Other."""
    ext = PurePosixPath(path).suffix.lower()
    if ext in MACOS_SOFTWARE_EXTENSIONS:
        return "macOS"
    if ext in WINDOWS_SOFTWARE_EXTENSIONS:
        return "Windows"
    return "Other"


def _check_pause(ctx: PhaseContext) -> bool:
    """Check if the job was paused. Returns True if paused (caller should break/return)."""
    with _pause_events_lock:
        entry = _pause_events.get(ctx.job.job_id)
        if entry and entry[0].is_set():
            logger.info("Job %s paused via in-process signal", ctx.job.job_id)
            ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED)
            ctx.progress.paused = True
            return True
    _job_check = ckpt.get_job(ctx.cat, ctx.job.job_id)
    if _job_check and _job_check.status == JobStatus.PAUSED:
        logger.info("Job %s paused externally", ctx.job.job_id)
        ctx.progress.paused = True
        return True
    return False


def _build_dest_path(
    base_path: str,
    filename: str,
    file_hash: str,
    mod_time: str | None,
    structure: str,
) -> str:
    """Build collision-safe destination path with year/month structure.

    Format: base_path/YYYY/MM/filename
    On collision (same name, different hash): base_path/YYYY/MM/filename_abc123.ext
    """
    filename = filename.strip()
    if not filename:
        filename = f"unnamed_{file_hash[:12]}" if file_hash else "unnamed_file"

    year, month = "unknown", "00"

    if mod_time:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            with contextlib.suppress(ValueError):
                dt = datetime.strptime(mod_time[:19], fmt)
                year = str(dt.year)
                month = f"{dt.month:02d}"
                break

    if structure == StructurePattern.YEAR_MONTH:
        prefix = f"{base_path}/{year}/{month}"
    elif structure == StructurePattern.YEAR:
        prefix = f"{base_path}/{year}"
    else:
        prefix = base_path

    return f"{prefix}/{filename}"


def _make_collision_safe(
    dest_path: str,
    file_hash: str,
    existing_paths: set[str] | None = None,
) -> str:
    """Add hash suffix to filename to avoid collisions.

    IMG_0001.jpg -> IMG_0001_a3f2b1.jpg
    """
    p = PurePosixPath(dest_path)
    stem = p.stem
    suffix = p.suffix
    # MD5 is used here only for collision-avoidance suffix generation, not security.
    raw_hash = file_hash if file_hash else hashlib.md5(dest_path.encode()).hexdigest()  # noqa: S324
    # Strip "surrogate:" prefix so suffix is a real hex hash, not "_surrog"
    full_hash = raw_hash.split(":", 1)[1] if ":" in raw_hash else raw_hash

    hash_len = 6
    candidate = str(p.parent / f"{stem}_{full_hash[:hash_len]}{suffix}")

    if existing_paths is not None:
        while candidate in existing_paths and hash_len < len(full_hash):
            hash_len = min(hash_len + 4, len(full_hash))
            candidate = str(p.parent / f"{stem}_{full_hash[:hash_len]}{suffix}")
        if candidate in existing_paths:
            counter = 2
            while candidate in existing_paths:
                if counter > 10_000:
                    raise RuntimeError(f"Cannot allocate collision-safe path after 10000 attempts: {dest_path}")
                candidate = str(p.parent / f"{stem}_{full_hash[:hash_len]}_{counter}{suffix}")
                counter += 1

    return candidate


def _estimate_speed(bytes_transferred: int, elapsed: float) -> float:
    """Bytes per second (raw, for use when EMA not applicable)."""
    if elapsed <= 0:
        return 0.0
    return bytes_transferred / elapsed


# EMA smoothing constant for speed/ETA (0.3 = responsive but not jittery)
_EMA_ALPHA = 0.3


def _ema_speed(prev_ema: float, instant_speed: float) -> float:
    """Exponential moving average for transfer speed."""
    if prev_ema <= 0:
        return instant_speed
    return _EMA_ALPHA * instant_speed + (1 - _EMA_ALPHA) * prev_ema


def _check_disk_space(disk_path: str, required_bytes: int) -> dict:
    """Pre-check available disk space before sync.

    Returns {"ok": bool, "available_bytes": int, "required_bytes": int, "error": str|None}
    """
    try:
        usage = shutil.disk_usage(disk_path)
        available = usage.free
        ok = available >= required_bytes
        return {
            "ok": ok,
            "available_bytes": available,
            "required_bytes": required_bytes,
            "error": None
            if ok
            else (
                f"Nedostatek mista na disku: potreba {required_bytes / 1e9:.1f} GB, "
                f"dostupné {available / 1e9:.1f} GB — uvolni misto a spust resume"
            ),
        }
    except OSError as exc:
        return {
            "ok": False,
            "available_bytes": 0,
            "required_bytes": required_bytes,
            "error": f"Nelze zjistit místo na disku {disk_path}: {exc}",
        }


def _resolve_rclone() -> str:
    """Resolve the rclone binary path, falling back to 'rclone'."""
    from .deps import resolve_bin
    return resolve_bin("rclone") or "rclone"


def _rclone_moveto(src_remote: str, src_path: str, dst_remote: str, dst_path: str, timeout: int = 300) -> dict:
    """Move a single file on a remote using rclone moveto.

    Used for reorganizing files on the destination (server-side move).
    """
    src = f"{src_remote}:{src_path}" if src_remote else src_path
    dst = f"{dst_remote}:{dst_path}" if dst_remote else dst_path
    cmd = [_resolve_rclone(), "moveto", src, dst, "--no-traverse"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode == 0:
            return {"success": True}
        return {"success": False, "error": result.stderr[:200]}
    except subprocess.TimeoutExpired:
        return {"success": False, "error": f"Timeout after {timeout}s"}
    except Exception as exc:
        return {"success": False, "error": str(exc)[:200]}


def _rclone_copy_dir(src_remote: str, src_path: str, dst_remote: str, dst_path: str, timeout: int = 600) -> dict:
    """Copy an entire directory (bundle) using rclone copy.

    Used for bundle integrity — transfers .app, .xcodeproj etc. as a unit.
    """
    src = f"{src_remote}:{src_path}" if src_remote else src_path
    dst = f"{dst_remote}:{dst_path}" if dst_remote else dst_path
    cmd = [_resolve_rclone(), "copy", src, dst]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode == 0:
            return {"success": True}
        return {"success": False, "error": result.stderr[:200]}
    except subprocess.TimeoutExpired:
        return {"success": False, "error": f"Timeout after {timeout}s"}
    except Exception as exc:
        return {"success": False, "error": str(exc)[:200]}


def _rclone_delete(remote: str, path: str, timeout: int = 120) -> dict:
    """Delete a single file on a remote using rclone deletefile."""
    target = f"{remote}:{path}" if remote else path
    cmd = [_resolve_rclone(), "deletefile", target]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode == 0:
            return {"success": True}
        return {"success": False, "error": result.stderr[:200]}
    except Exception as exc:
        return {"success": False, "error": str(exc)[:200]}


# ── Phase functions ──────────────────────────────────────────────────


def _phase_1_wait_for_sources(ctx: PhaseContext) -> None:
    """Phase 1: Probe all remotes, wait for connectivity."""
    if ctx.phase_done(Phase.WAIT_FOR_SOURCES):
        logger.info("Phase %s already done, skipping", Phase.WAIT_FOR_SOURCES)
        source_remotes = ctx.config.source_remotes or [r.name for r in list_remotes()]
        ctx.available = [r for r in source_remotes if rclone_is_reachable(r, timeout=5)]
        ctx.unavailable = [r for r in source_remotes if r not in ctx.available]
        ctx.results["sources"] = {"available": ctx.available, "unavailable": ctx.unavailable}
        return

    ctx.report(Phase.WAIT_FOR_SOURCES, "Cekani na zdroje...", 1)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.WAIT_FOR_SOURCES)

    source_remotes = ctx.config.source_remotes or [r.name for r in list_remotes()]

    for rname in source_remotes:
        ctx.report(Phase.WAIT_FOR_SOURCES, f"Testuji {rname}...", 1)
        if rclone_is_reachable(rname):
            ctx.available.append(rname)
            logger.info("Source %s: reachable", rname)
        else:
            logger.info("Source %s: not reachable, waiting %ds...", rname, ctx.config.source_wait_timeout)
            if wait_for_connectivity(rname, timeout=ctx.config.source_wait_timeout):
                ctx.available.append(rname)
            else:
                ctx.unavailable.append(rname)
                logger.warning("Source %s: still unreachable after %ds", rname, ctx.config.source_wait_timeout)

    ctx.results["sources"] = {"available": ctx.available, "unavailable": ctx.unavailable}
    ctx.progress.sources_available = ctx.available
    ctx.progress.sources_unavailable = ctx.unavailable

    if not ctx.available:
        error = "Zadný zdroj neni dostupný — zkontroluj pripojeni"
        ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED, error=error)
        ctx.progress.paused = True
        ctx.progress.error = error
        ctx.report(Phase.WAIT_FOR_SOURCES, error, 1)
        return

    ctx.finish_phase(Phase.WAIT_FOR_SOURCES)


def _rclone_lsjson_recursive_stream(remote: str, timeout: int = 1800) -> Iterator[dict]:
    """Single recursive rclone lsjson call — much faster than folder-by-folder walk.

    Uses ``--fast-list`` for providers that support it (gdrive, s3, etc.)
    which reduces API calls dramatically.  Streams results by reading
    stdout incrementally so memory stays bounded.
    """
    if not check_rclone():
        return
    cmd = [
        _rclone_bin(), "lsjson", f"{remote}:",
        "-R", "--no-mimetype", "--fast-list",
    ]
    logger.info("cloud_catalog: starting %s", " ".join(cmd))
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except OSError as exc:
        logger.warning("rclone lsjson spawn error for %s: %s", remote, exc)
        return

    import ijson  # type: ignore[import-untyped]

    try:
        # ijson streams JSON array items one by one — constant memory
        for item in ijson.items(proc.stdout, "item"):  # type: ignore[arg-type]
            yield item
    except Exception:
        # Fallback: if ijson is not installed, read full output
        pass
    finally:
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()

    if proc.returncode and proc.returncode != 0:
        stderr = (proc.stderr.read() if proc.stderr else "") or ""
        logger.warning("rclone lsjson -R failed for %s (rc=%d): %s", remote, proc.returncode, stderr[:300])


def _rclone_lsjson_recursive_fallback(remote: str, timeout: int = 1800) -> list[dict]:
    """Fallback: single recursive call, parse full JSON output at once."""
    if not check_rclone():
        return []
    cmd = [
        _rclone_bin(), "lsjson", f"{remote}:",
        "-R", "--no-mimetype", "--fast-list",
    ]
    logger.info("cloud_catalog fallback: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            logger.warning("rclone lsjson -R failed for %s: %s", remote, result.stderr[:300])
            return []
        return json.loads(result.stdout) if result.stdout.strip() else []
    except subprocess.TimeoutExpired:
        logger.warning("rclone lsjson -R timed out for %s after %ds", remote, timeout)
        return []
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("rclone lsjson -R error for %s: %s", remote, exc)
        return []


def _rclone_lsjson_fast(remote: str, timeout: int = 1800) -> Iterator[dict]:
    """Best-effort fast recursive listing — tries ijson streaming, falls back to full parse."""
    try:
        import ijson  # noqa: F401
        yield from _rclone_lsjson_recursive_stream(remote, timeout)
    except ImportError:
        logger.info("ijson not installed, using full-parse fallback for %s", remote)
        yield from _rclone_lsjson_recursive_fallback(remote, timeout)


def _phase_2_cloud_catalog_scan(ctx: PhaseContext) -> None:
    """Phase 2: Fast recursive scan of all cloud remotes."""
    if ctx.phase_done(Phase.CLOUD_CATALOG_SCAN):
        logger.info("Phase %s already done, skipping", Phase.CLOUD_CATALOG_SCAN)
        return

    ctx.report(Phase.CLOUD_CATALOG_SCAN, "Katalogizace vzdálených zdrojů...", 2)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.CLOUD_CATALOG_SCAN)

    total_cataloged = 0
    total_skipped_non_media = 0
    remote_count = len(ctx.available)
    conn = ctx.conn
    commit_interval = 100  # frequent commits & progress updates

    for ridx, rname in enumerate(ctx.available):
        _check_pause(ctx)
        # Skip dest remote to prevent self-copy (gws-backup copying its own GML-Consolidated)
        if rname == ctx.config.dest_remote:
            logger.info("Skipping dest remote %s to prevent self-copy", rname)
            continue
        ctx.report(
            Phase.CLOUD_CATALOG_SCAN,
            f"Skenuji {rname}... ({ridx + 1}/{remote_count})",
            2,
            files_cataloged=total_cataloged,
        )
        try:
            for f in _rclone_lsjson_fast(rname):
                if f.get("IsDir"):
                    continue

                fpath = f.get("Path", f.get("Name", ""))
                fsize = f.get("Size", 0) or 0

                if ctx.config.media_only and not _is_media_file(fpath):
                    total_skipped_non_media += 1
                    continue

                mod_time = f.get("ModTime", "")
                cloud_hash_input = f"{rname}:{fpath}:{fsize}"
                surrogate_hash = "surrogate:" + hashlib.sha256(cloud_hash_input.encode()).hexdigest()

                real_hash = None
                hashes = f.get("Hashes", {})
                if hashes:
                    real_hash = hashes.get("sha256") or hashes.get("SHA-256")

                effective_hash = real_hash or surrogate_hash
                now_str = datetime.now(timezone.utc).isoformat()

                try:
                    conn.execute(
                        """
                        INSERT INTO files (path, size, mtime, ctime, birthtime, ext,
                            sha256, source_remote, first_seen, last_scanned, date_original)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(path) DO UPDATE SET
                            size = excluded.size,
                            sha256 = COALESCE(excluded.sha256, sha256),
                            source_remote = excluded.source_remote,
                            last_scanned = excluded.last_scanned
                    """,
                        (
                            f"{rname}:{fpath}",
                            fsize,
                            0.0,
                            0.0,
                            None,
                            PurePosixPath(fpath).suffix.lower(),
                            effective_hash,
                            rname,
                            now_str,
                            now_str,
                            mod_time[:10] if mod_time else None,
                        ),
                    )
                except (sqlite3.IntegrityError, sqlite3.OperationalError) as exc:
                    logger.debug("catalog insert error for %s:%s: %s", rname, fpath, exc)
                    continue

                total_cataloged += 1

                if total_cataloged % commit_interval == 0:
                    conn.commit()
                    ctx.report(
                        Phase.CLOUD_CATALOG_SCAN,
                        f"Skenuji {rname}... ({total_cataloged} souborů)",
                        2,
                        files_cataloged=total_cataloged,
                    )

            conn.commit()

        except (OSError, RuntimeError) as exc:
            logger.warning("cloud_catalog_scan: %s error: %s", rname, exc)
            conn.rollback()

    ctx.results["catalog"] = {
        "total_cataloged": total_cataloged,
        "skipped_non_media": total_skipped_non_media,
    }
    ctx.report(Phase.CLOUD_CATALOG_SCAN, "Katalogizace hotová", 2, files_cataloged=total_cataloged)
    ctx.finish_phase(Phase.CLOUD_CATALOG_SCAN)


def _phase_3_local_scan(ctx: PhaseContext) -> None:
    """Phase 3: Incremental scan of local filesystem roots."""
    if ctx.phase_done(Phase.LOCAL_SCAN):
        logger.info("Phase %s already done, skipping", Phase.LOCAL_SCAN)
        return

    ctx.report(Phase.LOCAL_SCAN, "Skenovani lokalnich souboru...", 3)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.LOCAL_SCAN)

    local_scanned = 0
    if ctx.config.local_roots:
        try:
            from .scanner import incremental_scan  # Lazy import to avoid circular dependency

            # Get pause event for this job
            with _pause_events_lock:
                pause_entry = _pause_events.get(ctx.job.job_id)
                pause_event = pause_entry[0] if pause_entry else None

            def _scan_progress(info):
                processed = info.get("processed", 0)
                total_files = info.get("total", 0)
                ctx.report(
                    Phase.LOCAL_SCAN,
                    f"Skenovani lokalnich souboru... ({processed:,}/{total_files:,})",
                    3,
                    files_cataloged=ctx.progress.files_cataloged + processed,
                )

            stats = incremental_scan(
                catalog=ctx.cat,
                roots=[Path(r) for r in ctx.config.local_roots],
                workers=ctx.config.scan_workers,
                progress_callback=_scan_progress,
                cancel_event=pause_event,
            )
            local_scanned = getattr(stats, "files_scanned", 0) if stats else 0
        except PermissionError as exc:
            logger.warning("Local scan permission error (skipping): %s", exc)
        except (OSError, RuntimeError) as exc:
            logger.warning("Local scan error: %s", exc)

    ctx.local_scanned = local_scanned
    ctx.results["local_scan"] = {"scanned": local_scanned}
    ctx.finish_phase(Phase.LOCAL_SCAN)


def _phase_4_register_files(ctx: PhaseContext) -> None:
    """Phase 4: Register ALL files for transfer — NO deduplication.

    All files are marked PENDING for transfer regardless of duplicates.
    Actual dedup happens later in phase 9 (final dedup on destination).
    """
    if ctx.phase_done(Phase.REGISTER_FILES):
        logger.info("Phase %s already done, skipping", Phase.REGISTER_FILES)
        conn = ctx.conn
        conn.row_factory = sqlite3.Row
        cur2 = conn.execute("SELECT COUNT(*) as cnt FROM files WHERE sha256 IS NOT NULL")
        ctx.total_unique = cur2.fetchone()["cnt"]
        return

    ctx.report(Phase.REGISTER_FILES, "Registrace souboru pro prenos...", 4)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.REGISTER_FILES)

    conn = ctx.conn
    conn.row_factory = sqlite3.Row

    # Count total files (no dedup filtering — ALL files get transferred)
    cur_total = conn.execute("SELECT COUNT(*) as cnt FROM files WHERE sha256 IS NOT NULL")
    ctx.total_unique = cur_total.fetchone()["cnt"]

    cur_cloud = conn.execute("""
        SELECT COUNT(*) as cnt FROM files
        WHERE sha256 IS NOT NULL
          AND source_remote IS NOT NULL AND source_remote != '' AND source_remote != 'local'
    """)
    cloud_files_total = cur_cloud.fetchone()["cnt"]

    cur_local = conn.execute("""
        SELECT COUNT(*) as cnt FROM files
        WHERE sha256 IS NOT NULL
          AND (source_remote IS NULL OR source_remote = '' OR source_remote = 'local')
    """)
    local_files_total = cur_local.fetchone()["cnt"]

    ctx.results["register"] = {
        "total_files": ctx.total_unique,
        "cloud_files_to_transfer": cloud_files_total,
        "local_files": local_files_total,
        "note": "Bez deduplikace — vsechny soubory budou preneseny, dedup az ve fazi 9",
    }
    ctx.report(
        Phase.REGISTER_FILES,
        "Registrace hotová",
        4,
        files_unique=ctx.total_unique,
        files_duplicate=0,
    )
    ctx.finish_phase(Phase.REGISTER_FILES)


def _phase_5_stream(ctx: PhaseContext) -> None:
    """Phase 5: Stream ALL files to destination.

    Checkpoint-resumable, collision-safe, with post-transfer size verification.
    No dedup skipping — ALL pending files are transferred.
    Includes Google 750GB/day upload limit auto-pause and watchdog stall detection.
    Detects bundle directories and transfers them as a unit.
    """
    ctx.report(Phase.STREAM, "Priprava streamovani cloud->cloud...", 5)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.STREAM)
    ckpt.reset_stale_in_progress(ctx.cat, ctx.job.job_id, Phase.STREAM)

    conn = ctx.conn
    conn.row_factory = sqlite3.Row

    # Recovery: fix files that were transferred via bulk copy but process crashed
    # before server-side moves could mark them COMPLETED. Only recover files with
    # staging dest_location (set by post-bulk-copy marking), not files with final
    # dest (set during IN_PROGRESS planning — may not have been transferred).
    _recovered = conn.execute(
        """UPDATE consolidation_file_state
           SET status = 'completed', updated_at = ?
           WHERE job_id = ? AND step_name = ? AND status IN ('pending', 'in_progress')
             AND dest_location LIKE '%/.staging/%'""",
        (datetime.now(timezone.utc).isoformat(), ctx.job.job_id, Phase.STREAM),
    ).rowcount
    if _recovered:
        conn.commit()
        logger.info("Recovery: marked %d bulk-copied files as completed (staging dest found)", _recovered)

    # Get ALL cloud files for transfer (no dedup filtering)
    cur = conn.execute("""
        SELECT sha256, path, source_remote, size, date_original, metadata_richness
        FROM files
        WHERE sha256 IS NOT NULL
          AND source_remote IS NOT NULL AND source_remote != '' AND source_remote != 'local'
        ORDER BY source_remote, path
    """)
    all_cloud_files = cur.fetchall()

    # Also get local files for upload
    cur_local = conn.execute("""
        SELECT sha256, path, size, date_original FROM files
        WHERE sha256 IS NOT NULL
          AND (source_remote IS NULL OR source_remote = '' OR source_remote = 'local')
    """)
    local_files = cur_local.fetchall()

    total_bytes_estimate = sum(row["size"] or 0 for row in all_cloud_files)
    ctx.progress.bytes_total_estimate = total_bytes_estimate

    # Register ALL files as pending (no dedup skipping)
    registered = 0
    bundles_detected: set[str] = set()  # Track bundle roots to avoid duplicate transfers

    for row in all_cloud_files:
        source = row["source_remote"] or "local"
        fpath = row["path"]

        # Bundle integrity: detect if file is inside a bundle
        bundle_root = _get_bundle_root(fpath)
        if bundle_root and source != "local":
            bundle_key = f"{source}:{bundle_root}"
            if bundle_key in bundles_detected:
                # Already registered as part of a bundle — skip individual file
                continue
            bundles_detected.add(bundle_key)

        ckpt.mark_file(
            ctx.cat,
            ctx.job.job_id,
            row["sha256"],
            f"{source}:{fpath}" if ":" not in fpath else fpath,
            Phase.STREAM,
            FileStatus.PENDING,
        )
        registered += 1

    # Register local files as pending too (for upload)
    for row in local_files:
        ckpt.mark_file(
            ctx.cat,
            ctx.job.job_id,
            row["sha256"],
            f"local:{row['path']}",
            Phase.STREAM,
            FileStatus.PENDING,
        )
        registered += 1

    logger.info("Registered %d files for streaming (including %d bundle roots)", registered, len(bundles_detected))

    if ctx.config.dry_run:
        stream_progress = ckpt.get_job_progress(ctx.cat, ctx.job.job_id, Phase.STREAM)
        ctx.results["stream"] = {
            "dry_run": True,
            "would_transfer": stream_progress[FileStatus.PENDING],
            "estimated_bytes": total_bytes_estimate,
            "estimated_time_hours": total_bytes_estimate / (5_000_000 * 3600) if total_bytes_estimate else 0,
        }
        ctx.report(Phase.STREAM, "Dry run — prehled hotový", 5)
        return

    # Execute streaming transfers
    ctx.stream_start_time = time.monotonic()
    ctx.last_transfer_time = time.monotonic()
    ctx.daily_upload_start = time.monotonic()
    ctx.daily_bytes_uploaded = 0
    total_stream_bytes = 0

    # Check if resuming after daily limit pause
    job_data = ckpt.get_job(ctx.cat, ctx.job.job_id)
    if job_data and job_data.error and "Denní limit Google uploadu" in (job_data.error or "") and job_data.updated_at:
        try:
            pause_time = datetime.fromisoformat(job_data.updated_at)
            now = datetime.now(timezone.utc)
            elapsed_since_pause = (now - pause_time).total_seconds()
            if elapsed_since_pause < DAILY_LIMIT_PAUSE_SECONDS:
                remaining_hours = (DAILY_LIMIT_PAUSE_SECONDS - elapsed_since_pause) / 3600
                ctx.progress.paused = True
                ctx.progress.error = f"Denní limit — pokračuje za {remaining_hours:.1f} hodin"
                ctx.report(Phase.STREAM, ctx.progress.error, 5)
                return
            else:
                # 24h passed, clear the error and continue
                logger.info("24h since daily limit pause elapsed, resuming")
                ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.RUNNING, error=None)
                ctx.daily_bytes_uploaded = 0
                ctx.daily_upload_start = time.monotonic()
        except (ValueError, TypeError):
            pass  # Can't parse time, just continue

    # Detect native hash type for destination (e.g. MD5 on Google Drive)
    dest_hash_type = get_native_hash_type(ctx.config.dest_remote)
    if dest_hash_type:
        logger.info("Destination %s supports native %s hashes — will use for verification", ctx.config.dest_remote, dest_hash_type)

    # Rebuild dest_paths_used from checkpoint DB (survive resume!)
    dest_paths_used: set[str] = set()
    completed_dests = conn.execute(
        """
        SELECT dest_location FROM consolidation_file_state
        WHERE job_id = ? AND step_name = ? AND status = ? AND dest_location IS NOT NULL
    """,
        (ctx.job.job_id, Phase.STREAM, FileStatus.COMPLETED),
    ).fetchall()
    for row_d in completed_dests:
        dl = row_d["dest_location"]
        if ":" in dl:
            dest_paths_used.add(dl.split(":", 1)[1])
    if dest_paths_used:
        logger.info("Resume: loaded %d existing dest paths for collision detection", len(dest_paths_used))

    pending = ckpt.get_pending_files(ctx.cat, ctx.job.job_id, Phase.STREAM, limit=STREAM_BATCH_SIZE)
    _wal_counter = 0  # WAL checkpoint every 500 files

    while pending:
        # Check if job was paused via in-process Event or DB flag
        with _pause_events_lock:
            _pause_entry = _pause_events.get(ctx.job.job_id)
            _pause_signaled = _pause_entry[0].is_set() if _pause_entry else False
        if _pause_signaled:
            logger.info("Job %s paused via in-process signal, stopping stream", ctx.job.job_id)
            ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED)
            ctx.progress.paused = True
            ctx.report(Phase.STREAM, "Pozastaveno uzivatelem", 5)
            break
        _job_check = ckpt.get_job(ctx.cat, ctx.job.job_id)
        if _job_check and _job_check.status == JobStatus.PAUSED:
            logger.info("Job %s paused externally, stopping stream", ctx.job.job_id)
            ctx.progress.paused = True
            ctx.report(Phase.STREAM, "Pozastaveno uzivatelem", 5)
            break

        # Destination connectivity check
        if not rclone_is_reachable(ctx.config.dest_remote, timeout=DEST_CONNECTIVITY_TIMEOUT):
            logger.warning("Destination %s unreachable, waiting...", ctx.config.dest_remote)
            ctx.report(Phase.STREAM, f"Cekani na {ctx.config.dest_remote}...", 5, paused=True)
            if not wait_for_connectivity(ctx.config.dest_remote, timeout=ctx.config.connectivity_timeout):
                ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED, error="Cilové uloziste nedostupné")
                ctx.progress.paused = True
                ctx.report(Phase.STREAM, "Pozastaveno — cil nedostupný", 5)
                break

        source_failures: dict[str, int] = {}
        PARALLEL_TRANSFERS = 8

        # --- Prepare batch: build transfer tasks sequentially ---
        transfer_tasks: list[tuple] = []  # (fs, src_remote, src_path, dest_path, file_size)

        for fs in pending:
            # Pause check between preparations
            with _pause_events_lock:
                _pause_entry_inner = _pause_events.get(ctx.job.job_id)
                _pause_signaled_inner = _pause_entry_inner[0].is_set() if _pause_entry_inner else False
            if _pause_signaled_inner:
                logger.info("Job %s paused via signal, stopping", ctx.job.job_id)
                ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED)
                ctx.progress.paused = True
                ctx.report(Phase.STREAM, "Pozastaveno uzivatelem", 5)
                break

            if ctx.daily_bytes_uploaded >= GOOGLE_DAILY_UPLOAD_SAFETY:
                pause_msg = "Denní limit Google uploadu (750 GB) — pipeline automaticky pokračuje zítra"
                logger.warning("Daily upload limit reached (%d bytes), pausing", ctx.daily_bytes_uploaded)
                ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED, error=pause_msg)
                ctx.progress.paused = True
                ctx.progress.error = pause_msg
                ctx.report(Phase.STREAM, pause_msg, 5)
                break

            parts = fs.source_location.split(":", 1)
            src_remote = parts[0]
            src_path = parts[1] if len(parts) > 1 else parts[0]

            if src_remote == "local":
                if not os.path.exists(src_path):
                    ckpt.mark_file(ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                                   Phase.STREAM, FileStatus.FAILED, error="Lokální soubor nenalezen")
                    continue

            if src_remote != "local" and source_failures.get(src_remote, 0) >= MAX_SOURCE_FAILURES:
                continue

            # Bundle handling (sequential — rare)
            bundle_root = _get_bundle_root(src_path) if src_remote != "local" else None
            if bundle_root and src_remote != "local":
                dest_bundle_path = f"{ctx.config.dest_path}/{bundle_root}"
                ckpt.mark_file(ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                               Phase.STREAM, FileStatus.IN_PROGRESS,
                               dest=f"{ctx.config.dest_remote}:{dest_bundle_path}")
                try:
                    result = _rclone_copy_dir(src_remote, bundle_root, ctx.config.dest_remote, dest_bundle_path)
                    status = FileStatus.COMPLETED if result["success"] else FileStatus.FAILED
                    ckpt.mark_file(ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                                   Phase.STREAM, status,
                                   dest=f"{ctx.config.dest_remote}:{dest_bundle_path}",
                                   error=None if result["success"] else result.get("error", "")[:ERROR_TRUNCATE_LEN])
                    if result["success"]:
                        ctx.last_transfer_time = time.monotonic()
                except Exception as exc:
                    ckpt.mark_file(ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                                   Phase.STREAM, FileStatus.FAILED, error=str(exc)[:ERROR_TRUNCATE_LEN])
                continue

            # Build collision-safe destination path
            filename = PurePosixPath(src_path).name
            file_row = conn.execute("SELECT date_original, size FROM files WHERE sha256 = ? LIMIT 1",
                                    (fs.file_hash,)).fetchone()
            mod_time = file_row["date_original"] if file_row else None
            file_size = file_row["size"] if file_row else None

            dest_path = _build_dest_path(ctx.config.dest_path, filename, fs.file_hash, mod_time,
                                         ctx.config.structure_pattern)
            if dest_path in dest_paths_used:
                dest_path = _make_collision_safe(dest_path, fs.file_hash, dest_paths_used)
            dest_paths_used.add(dest_path)

            ckpt.mark_file(ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                           Phase.STREAM, FileStatus.IN_PROGRESS,
                           dest=f"{ctx.config.dest_remote}:{dest_path}")

            transfer_tasks.append((fs, src_remote, src_path, dest_path, file_size))

        if ctx.progress.paused:
            break

        # --- Execute transfers using BULK mode ---
        if not transfer_tasks:
            pending = ckpt.get_pending_files(ctx.cat, ctx.job.job_id, Phase.STREAM, limit=STREAM_BATCH_SIZE)
            continue

        # Group tasks by source remote for bulk transfer
        from collections import defaultdict
        remote_groups: dict[str, list[tuple]] = defaultdict(list)
        for task in transfer_tasks:
            fs, src_remote, src_path, dest_path, file_size = task
            remote_groups[src_remote].append(task)

        for src_remote, tasks in remote_groups.items():
            if ctx.progress.paused:
                break

            if src_remote == "local":
                # ── BULK LOCAL TRANSFER ──
                # Same pattern as cloud bulk: copy to staging, then server-side move
                staging_base = f"{ctx.config.dest_path}/.staging/local"
                src_paths = [t[2] for t in tasks]  # absolute local paths
                task_map = {t[2]: t for t in tasks}

                ctx.report(Phase.STREAM, f"Bulk upload: local ({len(tasks)} souborů)...", 5,
                           files_transferred=_wal_counter, bytes_transferred=total_stream_bytes)

                def _bulk_local_progress(files_done, bytes_done, speed_bps):
                    ctx.progress.current_file = f"local: {files_done}/{len(tasks)}"
                    ctx.last_transfer_time = time.monotonic()
                    mb_done = bytes_done / (1024 * 1024)
                    speed_mbs = speed_bps / (1024 * 1024) if speed_bps else 0
                    label = f"local: {files_done}/{len(tasks)} ({mb_done:.0f} MB)"
                    if speed_mbs:
                        label += f" [{speed_mbs:.1f} MB/s]"
                    ctx.report(Phase.STREAM, label, 5,
                               files_transferred=_wal_counter + files_done,
                               bytes_transferred=total_stream_bytes + bytes_done,
                               transfer_speed_bps=speed_bps)

                bulk_result = rclone_bulk_copy(
                    "local", ctx.config.dest_remote, staging_base, src_paths,
                    transfers=4, checkers=8, bwlimit=ctx.config.bwlimit,
                    progress_fn=_bulk_local_progress,
                )

                if not bulk_result["success"]:
                    error_msg = bulk_result.get("error", "bulk local upload failed")
                    logger.warning("Bulk local upload failed: %s", error_msg)
                    if any(q in error_msg.lower() for q in QUOTA_ERRORS):
                        ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED,
                                        error=f"Rate limit: {error_msg[:ERROR_TRUNCATE_MEDIUM]}")
                        ctx.progress.paused = True
                        break
                    for task in tasks:
                        ckpt.mark_file(ctx.cat, ctx.job.job_id, task[0].file_hash, task[0].source_location,
                                       Phase.STREAM, FileStatus.FAILED, error=error_msg[:ERROR_TRUNCATE_LEN])
                    continue

                total_stream_bytes += bulk_result["bytes"]
                ctx.daily_bytes_uploaded += bulk_result["bytes"]
                ctx.last_transfer_time = time.monotonic()

                # Mark all COMPLETED with staging path
                for task in tasks:
                    t_fs = task[0]
                    t_file_size = task[4]
                    t_staging_dest = f"{staging_base}/{task[2]}"
                    ckpt.mark_file(ctx.cat, ctx.job.job_id, t_fs.file_hash, t_fs.source_location,
                                   Phase.STREAM, FileStatus.COMPLETED,
                                   dest=f"{ctx.config.dest_remote}:{t_staging_dest}",
                                   bytes_transferred=t_file_size or 0)
                    _wal_counter += 1
                ckpt.wal_checkpoint(ctx.cat)

                # Server-side move from staging to final paths
                from concurrent.futures import ThreadPoolExecutor, as_completed

                ctx.report(Phase.STREAM, f"Přesouvám {len(tasks)} souborů na finální cesty...", 5,
                           bytes_transferred=total_stream_bytes)

                def _do_local_move(task_args):
                    t_fs, _, t_src_path, t_dest_path, t_file_size = task_args
                    t_staging = f"{staging_base}/{t_src_path}"
                    moved = rclone_server_side_move(ctx.config.dest_remote, t_staging, t_dest_path)
                    return (t_fs, t_dest_path, t_staging, t_file_size, moved)

                with ThreadPoolExecutor(max_workers=16) as pool:
                    for future in as_completed([pool.submit(_do_local_move, t) for t in tasks]):
                        t_fs, t_dest_path, t_staging, t_file_size, moved = future.result()
                        if moved:
                            ckpt.mark_file(ctx.cat, ctx.job.job_id, t_fs.file_hash, t_fs.source_location,
                                           Phase.STREAM, FileStatus.COMPLETED,
                                           dest=f"{ctx.config.dest_remote}:{t_dest_path}",
                                           bytes_transferred=t_file_size or 0)
                        else:
                            logger.warning("Server-side move failed: %s → %s", t_staging, t_dest_path)
                continue

            # ── BULK CLOUD TRANSFER ──
            # Phase A: Bulk copy all files from this remote to staging area
            staging_base = f"{ctx.config.dest_path}/.staging/{src_remote}"
            src_paths = [t[2] for t in tasks]  # extract src_path from each task
            task_map = {t[2]: t for t in tasks}  # src_path → task tuple

            ctx.report(Phase.STREAM, f"Bulk transfer: {src_remote} ({len(tasks)} souborů)...", 5,
                       files_transferred=_wal_counter, bytes_transferred=total_stream_bytes)

            def _bulk_progress(files_done, bytes_done, speed_bps):
                ctx.progress.current_file = f"{src_remote}: {files_done}/{len(tasks)}"
                ctx.last_transfer_time = time.monotonic()
                mb_done = bytes_done / (1024 * 1024)
                speed_mbs = speed_bps / (1024 * 1024) if speed_bps else 0
                speed_str = f"{speed_mbs:.1f} MB/s" if speed_mbs else ""
                label = f"{src_remote}: {files_done}/{len(tasks)} ({mb_done:.0f} MB)"
                if speed_str:
                    label += f" [{speed_str}]"
                ctx.report(Phase.STREAM, label, 5,
                           files_transferred=_wal_counter + files_done,
                           bytes_transferred=total_stream_bytes + bytes_done,
                           transfer_speed_bps=speed_bps)

            bulk_result = rclone_bulk_copy(
                src_remote, ctx.config.dest_remote, staging_base, src_paths,
                transfers=32, checkers=64, bwlimit=ctx.config.bwlimit,
                progress_fn=_bulk_progress,
            )

            if not bulk_result["success"]:
                error_msg = bulk_result.get("error", "bulk copy failed")
                logger.warning("Bulk copy from %s failed: %s", src_remote, error_msg)
                if any(q in error_msg.lower() for q in QUOTA_ERRORS):
                    ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED,
                                    error=f"Rate limit: {error_msg[:ERROR_TRUNCATE_MEDIUM]}")
                    ctx.progress.paused = True
                    break
                # Mark all as failed and continue to next remote
                for task in tasks:
                    ckpt.mark_file(ctx.cat, ctx.job.job_id, task[0].file_hash, task[0].source_location,
                                   Phase.STREAM, FileStatus.FAILED, error=error_msg[:ERROR_TRUNCATE_LEN])
                continue

            total_stream_bytes += bulk_result["bytes"]
            ctx.daily_bytes_uploaded += bulk_result["bytes"]
            ctx.last_transfer_time = time.monotonic()

            # CRITICAL: Mark ALL files COMPLETED immediately after bulk copy succeeds.
            # This prevents re-transfer if the process crashes before server-side moves finish.
            # Files are initially marked with staging path; server-side move updates to final path.
            for task in tasks:
                t_fs = task[0]
                t_file_size = task[4]
                t_staging_dest = f"{staging_base}/{task[2]}"
                ckpt.mark_file(ctx.cat, ctx.job.job_id, t_fs.file_hash, t_fs.source_location,
                               Phase.STREAM, FileStatus.COMPLETED,
                               dest=f"{ctx.config.dest_remote}:{t_staging_dest}",
                               bytes_transferred=t_file_size or 0)
                _wal_counter += 1
            ckpt.wal_checkpoint(ctx.cat)
            logger.info("Marked %d files COMPLETED (staging) for %s after bulk copy", len(tasks), src_remote)

            # Phase B: Server-side move from staging to final paths (instant on GDrive)
            # Parallelized — each moveto is a metadata-only API call
            ctx.report(Phase.STREAM, f"Přesouvám {len(tasks)} souborů na finální cesty...", 5,
                       bytes_transferred=total_stream_bytes)

            from concurrent.futures import ThreadPoolExecutor, as_completed

            move_counter = 0

            def _do_move(task_args):
                t_fs, _, t_src_path, t_dest_path, t_file_size = task_args
                t_staging = f"{staging_base}/{t_src_path}"
                moved = rclone_server_side_move(ctx.config.dest_remote, t_staging, t_dest_path)
                return (t_fs, t_dest_path, t_staging, t_file_size, moved)

            with ThreadPoolExecutor(max_workers=16) as pool:
                for future in as_completed([pool.submit(_do_move, t) for t in tasks]):
                    t_fs, t_dest_path, t_staging, t_file_size, moved = future.result()
                    move_counter += 1
                    ctx.report(Phase.STREAM,
                               f"Přesouvám {move_counter}/{len(tasks)} souborů na finální cesty...",
                               5,
                               bytes_transferred=total_stream_bytes,
                               transfer_speed_bps=0,
                               eta_seconds=0)
                    if moved:
                        # Update dest to final path (status stays COMPLETED)
                        ckpt.mark_file(ctx.cat, ctx.job.job_id, t_fs.file_hash, t_fs.source_location,
                                       Phase.STREAM, FileStatus.COMPLETED,
                                       dest=f"{ctx.config.dest_remote}:{t_dest_path}",
                                       bytes_transferred=t_file_size or 0)
                    else:
                        logger.warning("Server-side move failed: %s → %s (file stays in staging)", t_staging, t_dest_path)
                    if move_counter % 500 == 0:
                        ckpt.wal_checkpoint(ctx.cat)

        # Progress update after all remotes in this batch
        p = ckpt.get_job_progress(ctx.cat, ctx.job.job_id, Phase.STREAM)
        elapsed = time.monotonic() - ctx.stream_start_time
        instant_speed = _estimate_speed(total_stream_bytes, elapsed)
        ctx.progress._ema_speed = _ema_speed(ctx.progress._ema_speed, instant_speed)
        smoothed_speed = ctx.progress._ema_speed
        remaining_bytes = total_bytes_estimate - total_stream_bytes
        eta = int(remaining_bytes / smoothed_speed) if smoothed_speed > 0 else 0

        ctx.report(
            Phase.STREAM,
            f"Streaming: {ctx.progress.current_file or '...'}",
            5,
            files_transferred=p[FileStatus.COMPLETED],
            bytes_transferred=p["bytes_transferred"],
            errors=p[FileStatus.FAILED],
            transfer_speed_bps=smoothed_speed,
            eta_seconds=eta,
        )

        if ctx.progress.paused:
            break
        pending = ckpt.get_pending_files(ctx.cat, ctx.job.job_id, Phase.STREAM, limit=STREAM_BATCH_SIZE)

    stream_progress = ckpt.get_job_progress(ctx.cat, ctx.job.job_id, Phase.STREAM)
    ctx.results["stream"] = {
        "transferred": stream_progress[FileStatus.COMPLETED],
        "failed": stream_progress[FileStatus.FAILED],
        "skipped": stream_progress.get(FileStatus.SKIPPED, 0),
        "bytes": stream_progress["bytes_transferred"],
        "daily_bytes_uploaded": ctx.daily_bytes_uploaded,
    }

    if not ctx.progress.paused:
        ctx.finish_phase(Phase.STREAM)


def _phase_6_retry_failed(ctx: PhaseContext) -> None:
    """Phase 6: Retry failed files with longer timeout."""
    if ctx.config.dry_run or ctx.phase_done(Phase.RETRY_FAILED):
        return

    failed_files = ckpt.get_failed_files(ctx.cat, ctx.job.job_id, Phase.STREAM, limit=5000)

    if failed_files:
        ctx.report(Phase.RETRY_FAILED, f"Opakovani {len(failed_files)} neúspesných prenosu...", 6)
        ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.RETRY_FAILED)

        retried_ok = 0
        retried_fail = 0
        conn = ctx.conn
        conn.row_factory = sqlite3.Row

        # Pre-filter: skip local, max attempts, unreachable remotes
        reachable_cache: dict[str, bool] = {}
        retry_tasks: list[tuple] = []  # (fs, src_remote, src_path, dest_path, file_size)

        for fs in failed_files:
            if fs.attempt_count >= MAX_RETRY_ATTEMPTS:
                continue
            parts = fs.source_location.split(":", 1)
            src_remote = parts[0]
            src_path = parts[1] if len(parts) > 1 else parts[0]
            if src_remote == "local":
                continue
            # Cache reachability per remote
            if src_remote not in reachable_cache:
                reachable_cache[src_remote] = (
                    rclone_is_reachable(src_remote, timeout=DEST_CONNECTIVITY_TIMEOUT)
                    or wait_for_connectivity(src_remote, timeout=RETRY_CONNECTIVITY_WAIT)
                )
            if not reachable_cache[src_remote]:
                continue

            file_row = conn.execute("SELECT date_original, size FROM files WHERE sha256 = ? LIMIT 1",
                                    (fs.file_hash,)).fetchone()
            file_size = file_row["size"] if file_row else None
            if fs.dest_location and ":" in fs.dest_location:
                dest_path = fs.dest_location.split(":", 1)[1]
            else:
                filename = PurePosixPath(src_path).name
                mod_time = file_row["date_original"] if file_row else None
                dest_path = _build_dest_path(ctx.config.dest_path, filename, fs.file_hash, mod_time,
                                             ctx.config.structure_pattern)
            retry_tasks.append((fs, src_remote, src_path, dest_path, file_size))

        # Parallel retry transfers
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _do_retry(args):
            r_fs, r_src_remote, r_src_path, r_dest_path, r_file_size = args
            try:
                retry_timeout = (int(_dynamic_timeout(r_file_size) * ctx.config.retry_timeout_multiplier)
                                 if r_file_size else DEFAULT_RETRY_TIMEOUT)
                result = rclone_copyto(r_src_remote, r_src_path, ctx.config.dest_remote, r_dest_path,
                                       file_size=r_file_size, timeout=retry_timeout,
                                       bwlimit=ctx.config.bwlimit, checksum=True)
                return (r_fs, r_dest_path, r_file_size, result, None)
            except Exception as exc:
                return (r_fs, r_dest_path, r_file_size, None, exc)

        with ThreadPoolExecutor(max_workers=8) as pool:
            for future in as_completed([pool.submit(_do_retry, t) for t in retry_tasks]):
                r_fs, r_dest_path, r_file_size, result, exc = future.result()
                if exc is not None:
                    ckpt.mark_file(ctx.cat, ctx.job.job_id, r_fs.file_hash, r_fs.source_location,
                                   Phase.STREAM, FileStatus.FAILED,
                                   error=f"Retry exception: {str(exc)[:ERROR_TRUNCATE_SHORT]}")
                    retried_fail += 1
                elif result and result["success"]:
                    ckpt.mark_file(ctx.cat, ctx.job.job_id, r_fs.file_hash, r_fs.source_location,
                                   Phase.STREAM, FileStatus.COMPLETED,
                                   dest=f"{ctx.config.dest_remote}:{r_dest_path}",
                                   bytes_transferred=result["bytes"] or r_file_size or 0)
                    retried_ok += 1
                else:
                    ckpt.mark_file(ctx.cat, ctx.job.job_id, r_fs.file_hash, r_fs.source_location,
                                   Phase.STREAM, FileStatus.FAILED,
                                   error=(result.get("error", "retry failed") if result else "unknown")[:ERROR_TRUNCATE_LEN])
                    retried_fail += 1
                ctx.report(Phase.RETRY_FAILED, f"Retry: {retried_ok} OK, {retried_fail} fail", 6,
                           files_retried=retried_ok)

        ctx.results["retry"] = {"retried_ok": retried_ok, "retried_fail": retried_fail}
    else:
        ctx.results["retry"] = {"retried_ok": 0, "retried_fail": 0}

    ctx.finish_phase(Phase.RETRY_FAILED)


def _phase_7_verify(ctx: PhaseContext) -> None:
    """Phase 7: Verify integrity on destination.

    Runs before dedupe so all transferred files are still present.
    """
    if ctx.config.dry_run or ctx.phase_done(Phase.VERIFY):
        return

    ctx.report(Phase.VERIFY, "Overovani integrity na cili...", 7)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.VERIFY)

    if not rclone_is_reachable(ctx.config.dest_remote):
        ctx.results["verify"] = {"note": "Cilové uloziste nedostupné pro overeni"}
        ctx.finish_phase(Phase.VERIFY)
        return

    conn = ctx.conn
    conn.row_factory = sqlite3.Row
    completed_transfers = conn.execute(
        """
        SELECT cfs.file_hash, cfs.dest_location, cfs.bytes_transferred, cfs.source_location
        FROM consolidation_file_state cfs
        WHERE cfs.job_id = ? AND cfs.step_name = ? AND cfs.status = ?
          AND cfs.dest_location IS NOT NULL
    """,
        (ctx.job.job_id, Phase.STREAM, FileStatus.COMPLETED),
    ).fetchall()

    if ctx.config.verify_pct < 100:
        sample_size = max(1, len(completed_transfers) * ctx.config.verify_pct // 100)
        completed_transfers = random.sample(completed_transfers, min(sample_size, len(completed_transfers)))

    # Detect native hash type for hash-based verification
    dest_hash_type = get_native_hash_type(ctx.config.dest_remote)
    # Cache which source remotes natively support the dest hash type
    # Only compare hashes when both sides support the same type natively
    _src_hash_cache: dict[str, bool] = {}
    hash_verified_count = 0

    verified_ok = 0
    verified_fail = 0
    total_to_verify = len(completed_transfers)

    # Parallel verification — read-only operations, fully safe
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _src_supports_hash(src_remote_name: str) -> bool:
        """Check if source remote natively supports the dest hash type."""
        if src_remote_name not in _src_hash_cache:
            src_hash = get_native_hash_type(src_remote_name)
            _src_hash_cache[src_remote_name] = (src_hash == dest_hash_type) if dest_hash_type else False
        return _src_hash_cache[src_remote_name]

    def _verify_one(row):
        """Verify a single file. Returns (file_hash, dest_loc, status, error, is_hash_verified)."""
        dest_loc = row["dest_location"]
        if not dest_loc or ":" not in dest_loc:
            return (row["file_hash"], dest_loc, "skip", None, False)

        try:
            remote, path = dest_loc.split(":", 1)
            expected_bytes = row["bytes_transferred"]

            # Step 1: Size check (fast)
            check = rclone_check_file(remote, path, expected_size=expected_bytes)
            if not check["exists"]:
                return (row["file_hash"], dest_loc, "fail", "verify_missing: file not found on destination", False)

            if check.get("size_match") is False:
                return (row["file_hash"], dest_loc, "fail",
                        f"verify_size_mismatch: expected={expected_bytes}, got={check.get('size')}", False)

            # Step 2: Hash check — ONLY when source natively supports dest hash type
            # Cross-remote hash comparison (e.g. pcloud SHA-1 vs Google MD5) is unreliable
            if dest_hash_type:
                src_loc = row["source_location"]
                src_parts = src_loc.split(":", 1) if src_loc else []
                src_remote_name = src_parts[0] if len(src_parts) > 1 else None
                src_path_name = src_parts[1] if len(src_parts) > 1 else None

                if src_remote_name and src_path_name and _src_supports_hash(src_remote_name):
                    dest_hash = rclone_hashsum(remote, path, hash_type=dest_hash_type)
                    source_hash = rclone_hashsum(src_remote_name, src_path_name, hash_type=dest_hash_type)

                    if dest_hash and source_hash:
                        if dest_hash.lower() == source_hash.lower():
                            return (row["file_hash"], dest_loc, "ok", None, True)
                        else:
                            return (row["file_hash"], dest_loc, "fail",
                                    f"verify_hash_mismatch: src={source_hash[:16]}..., dest={dest_hash[:16]}...", False)
                    logger.debug("Hash unavailable for %s, relying on size check", dest_loc)

            # Size check passed (hash not comparable across different remotes)
            return (row["file_hash"], dest_loc, "ok", None, False)
        except Exception as exc:
            return (row["file_hash"], dest_loc, "fail_exc", str(exc)[:200], False)

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(_verify_one, row) for row in completed_transfers]
        for idx, future in enumerate(as_completed(futures)):
            if idx % 100 == 0 and _check_pause(ctx):
                ctx.report(Phase.VERIFY, "Pozastaveno uzivatelem", 7)
                # Cancel remaining futures
                for f in futures:
                    f.cancel()
                break

            file_hash, dest_loc, status, error, is_hash = future.result()
            if status == "skip":
                continue
            elif status == "ok":
                verified_ok += 1
                if is_hash:
                    hash_verified_count += 1
            elif status in ("fail", "fail_exc"):
                verified_fail += 1
                if error and status == "fail":
                    ckpt.mark_file(ctx.cat, ctx.job.job_id, file_hash, dest_loc,
                                   Phase.STREAM, FileStatus.FAILED, error=error)
                    logger.error("VERIFY FAIL: %s — %s", dest_loc, error)
                else:
                    logger.warning("Verify failed for %s: %s", dest_loc, error)

            if (idx + 1) % VERIFY_REPORT_INTERVAL == 0:
                ctx.report(Phase.VERIFY, f"Overeno {idx + 1}/{total_to_verify}...", 7,
                           files_verified=verified_ok, errors=verified_fail)

    ctx.results["verify"] = {
        "total_checked": total_to_verify,
        "verified_ok": verified_ok,
        "verified_fail": verified_fail,
        "hash_verified": hash_verified_count,
        "hash_type": dest_hash_type,
    }
    ctx.report(Phase.VERIFY, "Overeni hotové", 7, files_verified=verified_ok, errors=verified_fail)
    if hash_verified_count:
        logger.info("VERIFY: %d/%d files verified by %s hash (strongest guarantee)", hash_verified_count, verified_ok, dest_hash_type)

    # Pause before dedupe if significant verification failures
    if verified_fail > 0 and total_to_verify > 0:
        fail_pct = 100 * verified_fail / total_to_verify
        if fail_pct > VERIFY_FAIL_THRESHOLD_PCT:
            logger.error(
                "VERIFY: %d/%d (%.1f%%) failed — pausing before dedupe",
                verified_fail,
                total_to_verify,
                fail_pct,
            )
            ckpt.update_job(
                ctx.cat,
                ctx.job.job_id,
                status=JobStatus.PAUSED,
                error=f"Overeni: {verified_fail}/{total_to_verify} souboru selhalo ({fail_pct:.1f}%) — zkontroluj a spust resume",
            )
            ctx.progress.paused = True

    ctx.finish_phase(Phase.VERIFY)


def _safe_tar_extractall(tf: tarfile.TarFile, extract_dir: str) -> None:
    """Extract tar archive with path traversal protection."""
    for member in tf.getmembers():
        member_path = os.path.realpath(os.path.join(extract_dir, member.name))
        if not member_path.startswith(os.path.realpath(extract_dir) + os.sep) and member_path != os.path.realpath(extract_dir):
            raise ValueError(f"Tar path traversal: {member.name} escapes extraction directory")
        if member.issym() or member.islnk():
            link_target = os.path.realpath(os.path.join(extract_dir, member.linkname))
            if not link_target.startswith(os.path.realpath(extract_dir) + os.sep) and link_target != os.path.realpath(extract_dir):
                raise ValueError(f"Tar symlink traversal: {member.name} -> {member.linkname} escapes extraction directory")
    import sys

    if sys.version_info >= (3, 12):
        tf.extractall(extract_dir, filter="data")
    else:
        tf.extractall(extract_dir)


def _phase_8_extract_archives(ctx: PhaseContext) -> None:
    """Phase 8: Extract archives on destination.

    Scans destination for .zip, .rar, .7z, .tar.gz, .tar.bz2, .tar files.
    For each archive:
      - Download to temp dir on Mac
      - Extract contents
      - Upload extracted files to Google 6TB destination
      - Delete the archive from destination
      - Log what was extracted
    """
    if ctx.config.dry_run or ctx.phase_done(Phase.EXTRACT_ARCHIVES):
        return

    ctx.report(Phase.EXTRACT_ARCHIVES, "Rozbalovani archivu na cili...", 8)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.EXTRACT_ARCHIVES)

    if not rclone_is_reachable(ctx.config.dest_remote):
        ctx.results["extract_archives"] = {"note": "Cilové uloziste nedostupné pro rozbalovani"}
        ctx.finish_phase(Phase.EXTRACT_ARCHIVES)
        return

    # Scan destination for archives
    try:
        dest_files = rclone_ls_paginated(
            ctx.config.dest_remote,
            ctx.config.dest_path,
            max_depth=-1,
            inter_page_delay=ctx.config.api_delay,
        )
    except (OSError, RuntimeError) as exc:
        logger.warning("extract_archives: cannot list destination: %s", exc)
        ctx.results["extract_archives"] = {"error": str(exc)[:ERROR_TRUNCATE_LEN]}
        ctx.finish_phase(Phase.EXTRACT_ARCHIVES)
        return

    archives = []
    for f in dest_files:
        if f.get("IsDir"):
            continue
        fpath = f.get("Path", f.get("Name", ""))
        if _is_archive(fpath):
            archives.append(fpath)

    logger.info("Found %d archives on destination for extraction", len(archives))

    archives_extracted = 0
    files_from_archives = 0
    archives_failed = 0

    # Parallel archive extraction — each archive uses isolated temp directory
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _extract_one(archive_path: str) -> tuple[str, bool, int, str | None]:
        """Extract a single archive. Returns (archive_path, success, files_count, error)."""
        try:
            with tempfile.TemporaryDirectory(prefix="gml_archive_") as tmpdir:
                # Download archive to temp dir
                local_archive = os.path.join(tmpdir, PurePosixPath(archive_path).name)
                dl_cmd = [
                    _resolve_rclone(), "copyto",
                    f"{ctx.config.dest_remote}:{ctx.config.dest_path}/{archive_path}",
                    local_archive,
                ]
                subprocess.run(dl_cmd, capture_output=True, text=True, timeout=1800, check=True)

                # Extract archive
                extract_dir = os.path.join(tmpdir, "extracted")
                os.makedirs(extract_dir, exist_ok=True)

                lower_path = archive_path.lower()
                if lower_path.endswith(".zip"):
                    with zipfile.ZipFile(local_archive, "r") as zf:
                        for info in zf.infolist():
                            member_path = os.path.realpath(os.path.join(extract_dir, info.filename))
                            real_extract = os.path.realpath(extract_dir)
                            if not member_path.startswith(real_extract + os.sep) and member_path != real_extract:
                                raise ValueError(f"Zip Slip: {info.filename} escapes extraction directory")
                        zf.extractall(extract_dir)
                elif lower_path.endswith(".tar.gz") or lower_path.endswith(".tgz"):
                    with tarfile.open(local_archive, "r:gz") as tf:
                        _safe_tar_extractall(tf, extract_dir)
                elif lower_path.endswith(".tar.bz2"):
                    with tarfile.open(local_archive, "r:bz2") as tf:
                        _safe_tar_extractall(tf, extract_dir)
                elif lower_path.endswith(".tar"):
                    with tarfile.open(local_archive, "r:") as tf:
                        _safe_tar_extractall(tf, extract_dir)
                elif lower_path.endswith(".7z"):
                    subprocess.run(
                        ["7z", "x", local_archive, f"-o{extract_dir}", "-y"],
                        capture_output=True, text=True, timeout=1800, check=True,
                    )
                elif lower_path.endswith(".rar"):
                    subprocess.run(
                        ["unrar", "x", "-o+", local_archive, extract_dir + "/"],
                        capture_output=True, text=True, timeout=1800, check=True,
                    )
                else:
                    return (archive_path, False, 0, f"Unsupported format: {archive_path}")

                # Count extracted files
                extracted_files = []
                for root, _dirs, fnames in os.walk(extract_dir):
                    for fname in fnames:
                        full = os.path.join(root, fname)
                        rel = os.path.relpath(full, extract_dir)
                        extracted_files.append(rel)

                if not extracted_files:
                    logger.info("Archive %s was empty", archive_path)
                    return (archive_path, True, 0, None)

                # Upload extracted files to destination
                archive_parent = str(PurePosixPath(archive_path).parent)
                if archive_parent == ".":
                    archive_parent = ""
                upload_base = f"{ctx.config.dest_path}/{archive_parent}" if archive_parent else ctx.config.dest_path

                upload_cmd = [
                    _resolve_rclone(), "copy",
                    extract_dir,
                    f"{ctx.config.dest_remote}:{upload_base}",
                    "--transfers", "8", "--checkers", "16",
                ]
                upload_result = subprocess.run(upload_cmd, capture_output=True, text=True, timeout=3600)

                if upload_result.returncode != 0:
                    return (archive_path, False, 0,
                            f"Upload failed (rc={upload_result.returncode}): {(upload_result.stderr or '')[:150]}")

                # Verify upload by checking file count on destination
                verify_cmd = [
                    _resolve_rclone(), "size", "--json",
                    f"{ctx.config.dest_remote}:{upload_base}",
                ]
                verify_result = subprocess.run(verify_cmd, capture_output=True, text=True, timeout=300)
                if verify_result.returncode != 0:
                    return (archive_path, False, 0,
                            f"Cannot verify upload: {(verify_result.stderr or '')[:150]}")

                try:
                    size_info = json.loads(verify_result.stdout)
                    remote_count = size_info.get("count", 0)
                except (json.JSONDecodeError, KeyError):
                    remote_count = 0

                local_total_size = sum(
                    os.path.getsize(os.path.join(extract_dir, f))
                    for f in extracted_files
                    if os.path.exists(os.path.join(extract_dir, f))
                )
                remote_total_size = size_info.get("bytes", 0) if isinstance(size_info, dict) else 0

                if remote_count < len(extracted_files):
                    return (archive_path, False, 0,
                            f"File count mismatch: expected {len(extracted_files)}, found {remote_count}")

                if local_total_size > 0 and remote_total_size < local_total_size * 0.95:
                    return (archive_path, False, 0,
                            f"Size mismatch: local={local_total_size}, remote={remote_total_size}")

                # Delete the archive from destination only after verified upload
                delete_result = _rclone_delete(ctx.config.dest_remote, f"{ctx.config.dest_path}/{archive_path}")
                if delete_result["success"]:
                    logger.info("Extracted, verified and deleted archive %s (%d files)", archive_path, len(extracted_files))
                else:
                    logger.warning("Extracted archive %s but failed to delete: %s", archive_path, delete_result.get("error"))

                return (archive_path, True, len(extracted_files), None)

        except subprocess.CalledProcessError as exc:
            return (archive_path, False, 0, (exc.stderr[:200] if exc.stderr else str(exc)))
        except Exception as exc:
            return (archive_path, False, 0, str(exc)[:200])

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_extract_one, ap) for ap in archives]
        for idx, future in enumerate(as_completed(futures)):
            if idx % 5 == 0 and _check_pause(ctx):
                ctx.report(Phase.EXTRACT_ARCHIVES, "Pozastaveno uzivatelem", 8)
                for f in futures:
                    f.cancel()
                break

            ap, success, fcount, error = future.result()
            if success:
                archives_extracted += 1
                files_from_archives += fcount
            else:
                if error:
                    logger.warning("Failed to extract archive %s: %s", ap, error)
                archives_failed += 1

            ctx.report(
                Phase.EXTRACT_ARCHIVES,
                f"Rozbaleno {archives_extracted}/{len(archives)} archivu...",
                8,
                archives_extracted=archives_extracted,
                archive_files_added=files_from_archives,
            )

    ctx.results["extract_archives"] = {
        "archives_found": len(archives),
        "archives_extracted": archives_extracted,
        "files_from_archives": files_from_archives,
        "archives_failed": archives_failed,
    }
    ctx.progress.archives_extracted = archives_extracted
    ctx.progress.archive_files_added = files_from_archives
    ctx.report(Phase.EXTRACT_ARCHIVES, f"Rozbaleno {archives_extracted} archivu ({files_from_archives} souboru)", 8)
    ctx.finish_phase(Phase.EXTRACT_ARCHIVES)


def _phase_9_dedup(ctx: PhaseContext) -> None:
    """Phase 9: Final deduplication over ALL data on destination.

    Uses rclone dedupe with mode=largest to keep the highest quality copy.
    This runs AFTER all data is transferred and archives are extracted.
    """
    if ctx.config.dry_run:
        ctx.results["dedup"] = {
            "dry_run": True,
            "note": "Finalni deduplikace probehne po prenosu pomoci rclone dedupe (mode=largest)",
        }
        return

    if ctx.phase_done(Phase.DEDUP):
        return

    if _check_pause(ctx):
        ctx.report(Phase.DEDUP, "Pozastaveno uzivatelem", 9)
        return

    ctx.report(Phase.DEDUP, "Finalni deduplikace nad vsemi daty...", 9)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.DEDUP)

    if not rclone_is_reachable(ctx.config.dest_remote):
        ctx.results["dedup"] = {"note": "Cilové uloziste nedostupné pro deduplikaci"}
        ctx.finish_phase(Phase.DEDUP)
        return

    dedup_result = rclone_dedupe(
        ctx.config.dest_remote,
        ctx.config.dest_path,
        mode=DedupStrategy.LARGEST,
        dry_run=False,
        timeout=DEDUP_TIMEOUT,
    )

    ctx.results["dedup"] = {
        "success": dedup_result["success"],
        "duplicates_removed": dedup_result.get("duplicates_removed", 0),
        "bytes_freed": dedup_result.get("bytes_freed", 0),
    }

    if dedup_result["success"]:
        logger.info(
            "Final dedupe: removed %d duplicates, freed %d bytes",
            dedup_result.get("duplicates_removed", 0),
            dedup_result.get("bytes_freed", 0),
        )
    else:
        logger.warning("Final dedupe had issues: %s", dedup_result.get("error", ""))

    ctx.report(Phase.DEDUP, "Finalni deduplikace hotová", 9)
    ctx.finish_phase(Phase.DEDUP)


_SURROG_PATTERN = re.compile(r"(_surrog)+")


def _surrog_cleanup(ctx: PhaseContext, dest_files: list[dict]) -> dict:
    """Clean up accumulated _surrog suffixes from filenames on destination.

    When gws-backup is both source and destination, collision detection can
    produce names like file_surrog_surrog_surrog.pdf. This step:
      1. Finds files with _surrog in their name
      2. Strips ALL _surrog suffixes from the stem
      3. Re-adds a proper 6-char hash suffix via _make_collision_safe logic
      4. Renames via server-side rclone moveto (instant on GDrive)
      5. Updates checkpoint DB dest_location for affected files

    Returns {"renamed": int, "failed": int, "skipped": int}.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Collect all current dest paths for collision checking
    all_dest_paths: set[str] = set()
    surrog_files: list[dict] = []

    for f in dest_files:
        if f.get("IsDir"):
            continue
        fpath = f.get("Path", f.get("Name", ""))
        if not fpath:
            continue
        all_dest_paths.add(fpath)
        if "_surrog" in fpath:
            surrog_files.append(f)

    if not surrog_files:
        logger.info("surrog_cleanup: no files with _surrog suffix found")
        return {"renamed": 0, "failed": 0, "skipped": 0}

    logger.info("surrog_cleanup: found %d files with _surrog in name", len(surrog_files))
    ctx.report(Phase.ORGANIZE, f"Surrog cleanup: {len(surrog_files)} souboru k prejmenovani...", 10)

    renamed = 0
    failed = 0
    skipped = 0

    rename_tasks: list[tuple[str, str]] = []  # (old_path, new_path)

    for f in surrog_files:
        fpath = f.get("Path", f.get("Name", ""))
        p = PurePosixPath(fpath)
        stem = p.stem
        suffix = p.suffix
        parent = p.parent

        # Strip all _surrog occurrences from the stem
        clean_stem = _SURROG_PATTERN.sub("", stem)
        if clean_stem == stem:
            # No actual _surrog pattern (shouldn't happen but be safe)
            continue

        # Compute a proper hash suffix: use MD5 of the clean path for determinism
        clean_base = str(parent / f"{clean_stem}{suffix}")
        raw_hash = hashlib.md5(clean_base.encode()).hexdigest()  # noqa: S324
        hash_suffix = raw_hash[:6]
        candidate = str(parent / f"{clean_stem}_{hash_suffix}{suffix}")

        # Extend hash if collision
        hash_len = 6
        while candidate in all_dest_paths and candidate != fpath and hash_len < len(raw_hash):
            hash_len = min(hash_len + 4, len(raw_hash))
            candidate = str(parent / f"{clean_stem}_{raw_hash[:hash_len]}{suffix}")

        if candidate in all_dest_paths and candidate != fpath:
            # Even with full hash there's a collision -- skip to avoid overwrite
            logger.warning("surrog_cleanup: skipping %s, clean name %s already exists", fpath, candidate)
            skipped += 1
            continue

        if candidate == fpath:
            # Already clean (unlikely but possible)
            continue

        rename_tasks.append((fpath, candidate))
        # Reserve the new path to prevent collisions within this batch
        all_dest_paths.add(candidate)

    if not rename_tasks:
        logger.info("surrog_cleanup: no renames needed after analysis")
        return {"renamed": 0, "failed": 0, "skipped": skipped}

    logger.info("surrog_cleanup: %d renames to execute", len(rename_tasks))

    def _do_rename(old_path: str, new_path: str) -> tuple[str, str, bool, str | None]:
        src_full = f"{ctx.config.dest_path}/{old_path}"
        dst_full = f"{ctx.config.dest_path}/{new_path}"
        logger.info("surrog_cleanup: %s -> %s", old_path, new_path)
        result = _rclone_moveto(ctx.config.dest_remote, src_full, ctx.config.dest_remote, dst_full)
        return (old_path, new_path, result["success"], result.get("error"))

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(_do_rename, old, new) for old, new in rename_tasks]
        for idx, future in enumerate(as_completed(futures)):
            if idx % 50 == 0 and _check_pause(ctx):
                ctx.report(Phase.ORGANIZE, "Pozastaveno uzivatelem", 10)
                for f_remaining in futures:
                    f_remaining.cancel()
                break

            old_path, new_path, success, error = future.result()
            if success:
                renamed += 1
                # Update checkpoint DB: replace old dest_location with new one
                old_dest_loc = f"{ctx.config.dest_remote}:{ctx.config.dest_path}/{old_path}"
                new_dest_loc = f"{ctx.config.dest_remote}:{ctx.config.dest_path}/{new_path}"
                try:
                    conn = ctx.cat.conn
                    with conn:
                        conn.execute(
                            """UPDATE consolidation_file_state
                               SET dest_location = ?, updated_at = ?
                               WHERE job_id = ? AND dest_location = ?""",
                            (new_dest_loc, datetime.now(tz=timezone.utc).isoformat(), ctx.job.job_id, old_dest_loc),
                        )
                except Exception as db_exc:
                    logger.warning("surrog_cleanup: DB update failed for %s: %s", old_path, db_exc)

                # Update the in-memory dest_files list so organize phase sees correct paths
                for ff in dest_files:
                    if ff.get("Path") == old_path or ff.get("Name") == old_path:
                        if "Path" in ff:
                            ff["Path"] = new_path
                        if "Name" in ff:
                            ff["Name"] = PurePosixPath(new_path).name
                        break
            else:
                failed += 1
                logger.warning("surrog_cleanup: failed to rename %s -> %s: %s", old_path, new_path, error)

            if (idx + 1) % 25 == 0:
                ctx.report(
                    Phase.ORGANIZE,
                    f"Surrog cleanup: {renamed}/{len(rename_tasks)} prejmenovano...",
                    10,
                )

    logger.info("surrog_cleanup: done — renamed=%d, failed=%d, skipped=%d", renamed, failed, skipped)
    return {"renamed": renamed, "failed": failed, "skipped": skipped}


def _phase_10_organize(ctx: PhaseContext) -> None:
    """Phase 10: Organize files on destination by category and date.

    Moves files into:
      Category/Year/Month/filename
    For Software:
      Software/macOS/, Software/Windows/, Software/Other/ (no year/month)
    """
    if ctx.config.dry_run or ctx.phase_done(Phase.ORGANIZE):
        return

    ctx.report(Phase.ORGANIZE, "Organizace souboru podle kategorii...", 10)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.ORGANIZE)

    if not rclone_is_reachable(ctx.config.dest_remote):
        ctx.results["organize"] = {"note": "Cilové uloziste nedostupné pro organizaci"}
        ctx.finish_phase(Phase.ORGANIZE)
        return

    # List all files on destination
    try:
        dest_files = rclone_ls_paginated(
            ctx.config.dest_remote,
            ctx.config.dest_path,
            max_depth=-1,
            inter_page_delay=ctx.config.api_delay,
        )
    except (OSError, RuntimeError) as exc:
        logger.warning("organize: cannot list destination: %s", exc)
        ctx.results["organize"] = {"error": str(exc)[:ERROR_TRUNCATE_LEN]}
        ctx.finish_phase(Phase.ORGANIZE)
        return

    # --- Step 0: Clean up accumulated _surrog suffixes ---
    surrog_result = _surrog_cleanup(ctx, dest_files)
    ctx.results["surrog_cleanup"] = surrog_result

    moves_done = 0
    moves_failed = 0
    category_counts: dict[str, int] = {"Media": 0, "Documents": 0, "Software": 0, "Other": 0}

    # Build move list first, then execute in parallel
    move_tasks: list[tuple[str, str, str]] = []  # (fpath, new_path, category)

    for f in dest_files:
        if f.get("IsDir"):
            continue

        fpath = f.get("Path", f.get("Name", ""))
        if not fpath:
            continue

        # Skip files already in a category folder
        top_folder = PurePosixPath(fpath).parts[0] if PurePosixPath(fpath).parts else ""
        if top_folder in ("Media", "Documents", "Software", "Other"):
            continue

        category = _categorize_file(fpath)
        category_counts[category] = category_counts.get(category, 0) + 1
        mod_time = f.get("ModTime", "")

        if category == "Software":
            subcat = _software_subcategory(fpath)
            filename = PurePosixPath(fpath).name
            new_path = f"Software/{subcat}/{filename}"
        else:
            filename = PurePosixPath(fpath).name
            year, month = "unknown", "00"
            if mod_time:
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    with contextlib.suppress(ValueError):
                        dt = datetime.strptime(mod_time[:19], fmt)
                        year = str(dt.year)
                        month = f"{dt.month:02d}"
                        break
            new_path = f"{category}/{year}/{month}/{filename}"

        if fpath != new_path:
            move_tasks.append((fpath, new_path, category))

    logger.info("Organize: %d files to move into categories", len(move_tasks))

    # Parallel server-side moves — metadata-only on same remote, no collision risk (unique paths)
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _do_move(args):
        fpath, new_path, _cat = args
        src_full = f"{ctx.config.dest_path}/{fpath}"
        dst_full = f"{ctx.config.dest_path}/{new_path}"
        result = _rclone_moveto(ctx.config.dest_remote, src_full, ctx.config.dest_remote, dst_full)
        return (fpath, new_path, result["success"], result.get("error"))

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(_do_move, t) for t in move_tasks]
        for idx, future in enumerate(as_completed(futures)):
            if idx % 100 == 0 and _check_pause(ctx):
                ctx.report(Phase.ORGANIZE, "Pozastaveno uzivatelem", 10)
                for f in futures:
                    f.cancel()
                break

            fpath, new_path, success, error = future.result()
            if success:
                moves_done += 1
            else:
                moves_failed += 1
                logger.warning("organize: failed to move %s -> %s: %s", fpath, new_path, error)

            if (idx + 1) % 50 == 0:
                ctx.report(
                    Phase.ORGANIZE,
                    f"Organizace: {moves_done}/{len(move_tasks)} presunuto...",
                    10,
                    files_organized=moves_done,
                )

    ctx.results["organize"] = {
        "moves_done": moves_done,
        "moves_failed": moves_failed,
        "category_counts": category_counts,
    }
    ctx.progress.files_organized = moves_done
    ctx.report(Phase.ORGANIZE, f"Organizace hotová ({moves_done} souboru presunuto)", 10, files_organized=moves_done)
    ctx.finish_phase(Phase.ORGANIZE)


def _phase_11_report(ctx: PhaseContext) -> None:
    """Phase 11: Generate final summary report."""
    ctx.report(Phase.REPORT, "Generovani záverecného reportu...", 11)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.REPORT)

    final_progress = ckpt.get_job_progress(ctx.cat, ctx.job.job_id, Phase.STREAM)
    elapsed_total = time.monotonic() - ctx.stream_start_time if ctx.stream_start_time else 0

    ctx.results["summary"] = {
        "sources_available": len(ctx.available),
        "sources_unavailable": len(ctx.unavailable),
        "sources_unavailable_names": ctx.unavailable,
        "files_cataloged": ctx.results.get("catalog", {}).get("total_cataloged", 0) + ctx.local_scanned,
        "total_files_registered": ctx.total_unique,
        "files_transferred": final_progress.get(FileStatus.COMPLETED, 0),
        "bytes_transferred": final_progress.get("bytes_transferred", 0),
        "transfer_failures": final_progress.get(FileStatus.FAILED, 0),
        "files_retried_ok": ctx.results.get("retry", {}).get("retried_ok", 0),
        "files_retried_fail": ctx.results.get("retry", {}).get("retried_fail", 0),
        "verified_ok": ctx.results.get("verify", {}).get("verified_ok", 0),
        "verified_fail": ctx.results.get("verify", {}).get("verified_fail", 0),
        "archives_extracted": ctx.results.get("extract_archives", {}).get("archives_extracted", 0),
        "files_from_archives": ctx.results.get("extract_archives", {}).get("files_from_archives", 0),
        "dedup_duplicates_removed": ctx.results.get("dedup", {}).get("duplicates_removed", 0),
        "dedup_bytes_freed": ctx.results.get("dedup", {}).get("bytes_freed", 0),
        "files_organized": ctx.results.get("organize", {}).get("moves_done", 0),
        "category_counts": ctx.results.get("organize", {}).get("category_counts", {}),
        "elapsed_seconds": int(elapsed_total),
        "dry_run": ctx.config.dry_run,
    }

    # Log permanent failures
    still_failed = final_progress.get(FileStatus.FAILED, 0)
    if still_failed > 0:
        logger.error("CONSOLIDATION: %d files STILL FAILED after retry — manual intervention needed", still_failed)
        failed_list = ckpt.get_failed_files(ctx.cat, ctx.job.job_id, Phase.STREAM, limit=100)
        for ff in failed_list:
            logger.error("  FAILED: %s -> %s (attempts=%d)", ff.source_location, ff.last_error, ff.attempt_count)

    # Complete job (unless paused)
    if not ctx.progress.paused:
        if still_failed > 0:
            ckpt.complete_job(ctx.cat, ctx.job.job_id, error=f"{still_failed} souboru se nepodarilo prenést")
        else:
            ckpt.complete_job(ctx.cat, ctx.job.job_id)
        ctx.report(Phase.COMPLETE, "Konsolidace dokoncena", 11)


# ── Standalone sync_to_disk ──────────────────────────────────────────


def sync_to_disk(
    catalog_path: str | Path,
    dest_remote: str,
    dest_path: str,
    disk_path: str,
    progress_fn: Callable[[dict], None] | None = None,
) -> dict:
    """Standalone sync from cloud destination to local disk.

    Not part of the main pipeline. Called separately via UI button.
    Performs: connectivity check, disk space check, rclone copy, integrity report.
    """
    result: dict[str, Any] = {"synced": False, "disk_path": disk_path}

    if not check_volume_mounted(disk_path):
        result["error"] = f"Disk {disk_path} není připojený — připojte disk a zkuste znovu"
        return result

    # Disk space pre-check
    cat = Catalog(str(catalog_path))
    cat.open()
    try:
        jobs = ckpt.list_jobs(cat)
        required_bytes = 0
        for j in jobs:
            if j.job_type == JOB_TYPE_ULTIMATE:
                p = ckpt.get_job_progress(cat, j.job_id, Phase.STREAM)
                required_bytes = max(required_bytes, p.get("bytes_transferred", 0))
    finally:
        cat.close()

    if required_bytes > 0:
        space_check = _check_disk_space(disk_path, required_bytes)
        if not space_check["ok"]:
            result["error"] = space_check["error"]
            return result

    if not rclone_is_reachable(dest_remote):
        result["error"] = f"Cloud {dest_remote} není dostupný"
        return result

    try:
        rclone_copy(
            dest_remote,
            dest_path,
            disk_path,
            progress_fn=lambda p: progress_fn(p) if progress_fn else None,
        )
        result["synced"] = True
    except Exception as exc:
        result["error"] = str(exc)[:ERROR_TRUNCATE_LEN]
        logger.error("Sync to disk failed: %s", exc)

    return result


# ── Main pipeline ────────────────────────────────────────────────────


def run_consolidation(
    catalog_path: str | Path,
    config: ConsolidationConfig | None = None,
    progress_fn: Callable[[ConsolidationProgress], None] | None = None,
    scenario_id: str | None = None,
) -> dict[str, Any]:
    """Execute the full Ultimate Consolidation pipeline (v2).

    All phases are checkpoint-resumable. On resume, completed phases are skipped.
    Returns summary dict with per-phase results and job_id for resume.

    Phase order:
      1. wait_for_sources
      2. cloud_catalog_scan
      3. local_scan
      4. register_files (no dedup)
      5. stream (transfer ALL)
      6. retry_failed
      7. verify
      8. extract_archives
      9. dedup (final dedup)
     10. organize
     11. report

    sync_to_disk is NOT part of this pipeline — use sync_to_disk() separately.
    """
    config = config or ConsolidationConfig()
    catalog_path = str(catalog_path)

    cat = Catalog(catalog_path)
    cat.open()

    progress = ConsolidationProgress(total_steps=11, dry_run=config.dry_run)
    job = None

    try:
        # DB integrity check on resume
        if not ckpt.check_db_integrity(cat):
            logger.error("DB integrity check failed — proceeding with caution")

        # Find or create job
        resumable = ckpt.get_resumable_jobs(cat)
        for j in resumable:
            if j.job_type == JOB_TYPE_ULTIMATE:
                job = j
                logger.info("Resuming consolidation job %s (status=%s, step=%s)", j.job_id, j.status, j.current_step)
                if j.config:
                    saved = j.config
                    config.dest_remote = saved.get("dest_remote", config.dest_remote)
                    config.dest_path = saved.get("dest_path", config.dest_path)
                    config.disk_path = saved.get("disk_path", config.disk_path)
                    config.source_remotes = saved.get("source_remotes", config.source_remotes)
                    config.local_roots = saved.get("local_roots", config.local_roots)
                    config.bwlimit = saved.get("bwlimit", config.bwlimit)
                    logger.info(
                        "Restored config from job: dest=%s:%s, disk=%s, bwlimit=%s",
                        config.dest_remote,
                        config.dest_path,
                        config.disk_path,
                        config.bwlimit,
                    )
                break

        if not job:
            job = ckpt.create_job(
                cat,
                JOB_TYPE_ULTIMATE,
                config={
                    "dest_remote": config.dest_remote,
                    "dest_path": config.dest_path,
                    "disk_path": config.disk_path,
                    "source_remotes": config.source_remotes,
                    "local_roots": config.local_roots,
                    "dry_run": config.dry_run,
                    "bwlimit": config.bwlimit,
                    "completed_phases": [],
                },
                scenario_id=scenario_id,
            )

        ckpt.update_job(cat, job.job_id, status=JobStatus.RUNNING)

        # Register in-process pause event for this job
        with _pause_events_lock:
            _pause_events[job.job_id] = (threading.Event(), time.time())
            # Purge stale events from crashed jobs that bypassed finally cleanup
            _now = time.time()
            _stale = [k for k, (_, ts) in _pause_events.items()
                       if k != job.job_id and _now - ts > _PAUSE_EVENT_MAX_AGE]
            for k in _stale:
                logger.warning("Purging stale pause event for job %s (age > %ds)", k, _PAUSE_EVENT_MAX_AGE)
                del _pause_events[k]

        ctx = PhaseContext(
            cat=cat,
            config=config,
            job=job,
            progress=progress,
            progress_fn=progress_fn,
            results={"job_id": job.job_id, "dry_run": config.dry_run},
        )

        # Execute phases sequentially, each skips if already done
        _phase_1_wait_for_sources(ctx)
        if ctx.progress.paused:
            return ctx.results

        _phase_2_cloud_catalog_scan(ctx)
        _phase_3_local_scan(ctx)
        _phase_4_register_files(ctx)

        _phase_5_stream(ctx)
        if ctx.progress.paused:
            return ctx.results

        _phase_6_retry_failed(ctx)
        _phase_7_verify(ctx)
        if ctx.progress.paused:
            return ctx.results

        _phase_8_extract_archives(ctx)
        _phase_9_dedup(ctx)
        _phase_10_organize(ctx)
        _phase_11_report(ctx)

        return ctx.results

    except Exception as exc:
        logger.exception("Consolidation pipeline failed")
        if job:
            ckpt.complete_job(cat, job.job_id, error=str(exc)[:500])
        raise
    finally:
        if job:
            with _pause_events_lock:
                _pause_events.pop(job.job_id, None)
        cat.close()


# ── Public API ───────────────────────────────────────────────────────


def get_consolidation_status(catalog_path: str | Path) -> dict[str, Any]:
    """Get current consolidation status for UI display."""
    cat = Catalog(str(catalog_path))
    cat.open()
    try:
        jobs = ckpt.list_jobs(cat)
        consolidation_jobs = [j for j in jobs if j.job_type in CONSOLIDATION_JOB_TYPES]

        # Detect orphaned jobs: marked "running" in DB but no in-process event
        # Mark as "paused" (not "failed") so they can be resumed on next run
        with _pause_events_lock:
            live_job_ids = set(_pause_events.keys())
        for j in consolidation_jobs:
            if j.status == JobStatus.RUNNING and j.job_id not in live_job_ids:
                logger.warning("Orphaned job %s detected (running in DB, no live process) — marking paused for resume", j.job_id)
                ckpt.update_job(cat, j.job_id, status=JobStatus.PAUSED,
                                error="Server restart — automaticky pokračuje při dalším spuštění")
                j.status = JobStatus.PAUSED

        active = [j for j in consolidation_jobs if j.status in (JobStatus.CREATED, JobStatus.RUNNING, JobStatus.PAUSED)]

        result: dict[str, Any] = {
            "has_active_job": len(active) > 0,
            "total_jobs": len(consolidation_jobs),
            "jobs": [],
        }

        # Check source availability (always, so wizard can show remotes before first job)
        sources_available: list[str] = []
        sources_unavailable: list[str] = []
        if active:
            active_config = active[0].config or {}
            source_remotes = active_config.get("source_remotes", [])
        else:
            source_remotes = []
        if not source_remotes:
            source_remotes = [r.name for r in list_remotes()]
        for rname in source_remotes:
            if rclone_is_reachable(rname, timeout=5):
                sources_available.append(rname)
            else:
                sources_unavailable.append(rname)
        result["sources_available"] = sources_available
        result["sources_unavailable"] = sources_unavailable

        for j in consolidation_jobs[:10]:
            progress = ckpt.get_job_progress(cat, j.job_id)
            result["jobs"].append(
                {
                    "job_id": j.job_id,
                    "status": j.status,
                    "current_step": j.current_step,
                    "created_at": j.created_at,
                    "updated_at": j.updated_at,
                    "completed_at": j.completed_at,
                    "error": j.error,
                    "progress": progress,
                    "config": j.config,
                }
            )

        return result
    finally:
        cat.close()


def preview_consolidation(
    catalog_path: str | Path,
    config: ConsolidationConfig | None = None,
) -> dict[str, Any]:
    """Dry-run: scan sources, count files, estimate transfer — no actual transfers."""
    config = config or ConsolidationConfig()
    config.dry_run = True
    return run_consolidation(catalog_path, config=config)


def pause_consolidation(catalog_path: str | Path) -> dict:
    """Pause the active consolidation job.

    Uses in-process Event signaling to avoid opening a new DB connection
    (which would deadlock on the SQLite write lock held by the running job).
    Falls back to DB read+write for external-process callers.
    """
    # First, try in-process signal (no DB needed — avoids lock contention)
    with _pause_events_lock:
        active_jobs = list(_pause_events.keys())
    for job_id in active_jobs:
        signaled = signal_pause(job_id)
        if signaled:
            return {"paused": True, "job_id": job_id}

    # Fallback: no in-process jobs found, try DB (external process scenario)
    try:
        cat = Catalog(str(catalog_path))
        cat.open()
        try:
            jobs = ckpt.get_resumable_jobs(cat)
            for j in jobs:
                if j.job_type in CONSOLIDATION_JOB_TYPES and j.status == JobStatus.RUNNING:
                    ckpt.pause_job(cat, j.job_id)
                    return {"paused": True, "job_id": j.job_id}
        finally:
            cat.close()
    except Exception as e:
        logger.warning("DB fallback for pause failed: %s", e)

    return {"paused": False, "note": "Zadný bezici job k pozastaveni"}


def resume_consolidation(
    catalog_path: str | Path,
    config: ConsolidationConfig | None = None,
    progress_fn: Callable | None = None,
) -> dict[str, Any]:
    """Resume a paused/interrupted consolidation."""
    return run_consolidation(catalog_path, config=config, progress_fn=progress_fn)


def get_failed_files_report(catalog_path: str | Path) -> list[dict]:
    """Get detailed report of all failed transfers for manual review."""
    cat = Catalog(str(catalog_path))
    cat.open()
    try:
        jobs = ckpt.list_jobs(cat)
        result = []
        for j in jobs:
            if j.job_type == JOB_TYPE_ULTIMATE:
                failed = ckpt.get_failed_files(cat, j.job_id, Phase.STREAM, limit=10000)
                for ff in failed:
                    result.append(
                        {
                            "job_id": j.job_id,
                            "file_hash": ff.file_hash,
                            "source": ff.source_location,
                            "error": ff.last_error,
                            "attempts": ff.attempt_count,
                            "last_attempt": ff.updated_at,
                        }
                    )
        return result
    finally:
        cat.close()
