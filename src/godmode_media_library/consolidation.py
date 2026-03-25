"""Ultimate Consolidation Pipeline — GOD MODE orchestrator.

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

Phases:
  1. Wait for sources — probe all remotes, wait for connectivity
  2. Cloud catalog scan — paginated metadata scan, write to files table
  3. Local scan — incremental scan of local roots
  4. Pre-transfer dedup — SHA256-based for files with real hashes (local)
  5. Stream cloud→cloud — ALL cloud files, checkpoint-resumable, verified
  6. Retry failed — second pass with longer timeout for failed transfers
  7. Verify integrity — check ALL transferred files (BEFORE dedupe!)
  8. Post-transfer dedup — rclone dedupe on destination (100% accurate, mode=largest)
  9. Sync to disk — rclone sync from cloud to external drive
 10. Final report — summary of everything
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from . import checkpoint as ckpt
from .catalog import Catalog
from .cloud import (
    _dynamic_timeout,
    _rclone_bin,
    check_rclone,
    check_volume_mounted,
    list_remotes,
    rclone_check_file,
    rclone_copyto,
    rclone_dedupe,
    rclone_is_reachable,
    rclone_ls,
    rclone_ls_paginated,
    rclone_verify_transfer,
    retry_with_backoff,
    wait_for_connectivity,
)

logger = logging.getLogger(__name__)

# Media file extensions for filtering
_MEDIA_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif", ".webp",
    ".heic", ".heif", ".avif", ".raw", ".cr2", ".cr3", ".nef", ".arw",
    ".dng", ".orf", ".rw2", ".pef", ".srw",
    ".mp4", ".mov", ".avi", ".mkv", ".wmv", ".flv", ".webm", ".m4v",
    ".3gp", ".mts", ".m2ts", ".mpg", ".mpeg", ".vob",
    ".mp3", ".aac", ".flac", ".wav", ".ogg", ".m4a", ".wma", ".aiff",
    ".pdf",
})


@dataclass
class ConsolidationConfig:
    """Configuration for a full consolidation run."""
    # Source remotes to include (empty = all configured)
    source_remotes: list[str] = field(default_factory=list)
    # Local roots to scan (empty = use GML config defaults)
    local_roots: list[str] = field(default_factory=list)
    # Destination cloud for streaming
    dest_remote: str = "gws-backup"
    dest_path: str = "GML-Consolidated"
    # Final local disk path
    disk_path: str = "/Volumes/4TB/GML-Library"
    # Structure pattern: "year_month" | "year" | "flat"
    structure_pattern: str = "year_month"
    # Deduplication strategy
    dedup_strategy: str = "richness"
    # Verification: 0 = skip, 100 = verify all transferred files
    verify_pct: int = 100
    # Connectivity retry settings
    connectivity_timeout: int = 300  # seconds to wait for network
    max_transfer_retries: int = 3
    # Workers for local scan
    scan_workers: int = 4
    # Bandwidth limit (e.g. "10M" for 10 MB/s, None = unlimited)
    bwlimit: str | None = None
    # Dry run: scan and plan, but don't transfer
    dry_run: bool = False
    # Source wait timeout per remote (seconds)
    source_wait_timeout: int = 120
    # Inter-API-call delay (seconds) — rate limit protection
    api_delay: float = 0.5
    # Retry failed files with longer timeout multiplier
    retry_timeout_multiplier: float = 3.0
    # Media files only (skip non-media)
    media_only: bool = True


@dataclass
class ConsolidationProgress:
    """Progress snapshot for UI updates."""
    phase: str = "idle"
    phase_label: str = ""
    current_step: int = 0
    total_steps: int = 10
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


# ── Helpers ──────────────────────────────────────────────────────────


def _is_media_file(path: str) -> bool:
    """Check if path has a media file extension."""
    return PurePosixPath(path).suffix.lower() in _MEDIA_EXTENSIONS


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
    year, month = "unknown", "00"

    if mod_time:
        # Try to parse ISO date or common formats
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            with contextlib.suppress(ValueError):
                dt = datetime.strptime(mod_time[:19], fmt)
                year = str(dt.year)
                month = f"{dt.month:02d}"
                break

    if structure == "year_month":
        prefix = f"{base_path}/{year}/{month}"
    elif structure == "year":
        prefix = f"{base_path}/{year}"
    else:
        prefix = base_path

    return f"{prefix}/{filename}"


def _make_collision_safe(dest_path: str, file_hash: str) -> str:
    """Add hash suffix to filename to avoid collisions.

    IMG_0001.jpg → IMG_0001_a3f2b1.jpg
    """
    p = PurePosixPath(dest_path)
    stem = p.stem
    suffix = p.suffix
    hash_suffix = file_hash[:6] if file_hash else hashlib.md5(dest_path.encode()).hexdigest()[:6]
    return str(p.parent / f"{stem}_{hash_suffix}{suffix}")


def _estimate_speed(bytes_transferred: int, elapsed: float) -> float:
    """Bytes per second, smoothed."""
    if elapsed <= 0:
        return 0.0
    return bytes_transferred / elapsed


# ── Main pipeline ────────────────────────────────────────────────────


def run_consolidation(
    catalog_path: str | Path,
    config: ConsolidationConfig | None = None,
    progress_fn: Callable[[ConsolidationProgress], None] | None = None,
    scenario_id: str | None = None,
) -> dict[str, Any]:
    """Execute the full Ultimate Consolidation pipeline.

    All phases are checkpoint-resumable. On resume, completed phases are skipped.
    Returns summary dict with per-phase results and job_id for resume.
    """
    config = config or ConsolidationConfig()
    catalog_path = str(catalog_path)

    cat = Catalog(catalog_path)
    cat.open()

    progress = ConsolidationProgress(total_steps=10, dry_run=config.dry_run)
    stream_start_time = 0.0
    job = None  # declare early for exception handler

    def _report(phase: str, label: str, step: int, **kwargs):
        progress.phase = phase
        progress.phase_label = label
        progress.current_step = step
        for k, v in kwargs.items():
            if hasattr(progress, k):
                setattr(progress, k, v)
        if progress_fn:
            progress_fn(progress)

    try:
        # ── Find or create job ───────────────────────────────────
        resumable = ckpt.get_resumable_jobs(cat)
        for j in resumable:
            if j.job_type == "ultimate_consolidation":
                job = j
                logger.info("Resuming consolidation job %s (status=%s, step=%s)",
                            j.job_id, j.status, j.current_step)
                # Restore original config from job if caller didn't provide one
                if j.config:
                    saved = j.config
                    config.dest_remote = saved.get("dest_remote", config.dest_remote)
                    config.dest_path = saved.get("dest_path", config.dest_path)
                    config.disk_path = saved.get("disk_path", config.disk_path)
                    config.source_remotes = saved.get("source_remotes", config.source_remotes)
                    config.bwlimit = saved.get("bwlimit", config.bwlimit)
                    logger.info("Restored config from job: dest=%s:%s, disk=%s, bwlimit=%s",
                               config.dest_remote, config.dest_path, config.disk_path, config.bwlimit)
                break

        if not job:
            job = ckpt.create_job(
                cat,
                "ultimate_consolidation",
                config={
                    "dest_remote": config.dest_remote,
                    "dest_path": config.dest_path,
                    "disk_path": config.disk_path,
                    "source_remotes": config.source_remotes,
                    "dry_run": config.dry_run,
                    "bwlimit": config.bwlimit,
                    "completed_phases": [],
                },
                scenario_id=scenario_id,
            )

        ckpt.update_job(cat, job.job_id, status="running")
        results: dict[str, Any] = {"job_id": job.job_id, "dry_run": config.dry_run}

        # Helper: check if phase already done (for resume)
        def _phase_done(phase: str) -> bool:
            return ckpt.is_phase_done(cat, job.job_id, phase)

        def _finish_phase(phase: str):
            ckpt.mark_phase_done(cat, job.job_id, phase)

        # ══════════════════════════════════════════════════════════
        # Phase 1: Wait for sources
        # ══════════════════════════════════════════════════════════
        if not _phase_done("wait_for_sources"):
            _report("wait_for_sources", "Čekání na zdroje…", 1)
            ckpt.update_job(cat, job.job_id, current_step="wait_for_sources")

            source_remotes = config.source_remotes or [r.name for r in list_remotes()]
            available: list[str] = []
            unavailable: list[str] = []

            for rname in source_remotes:
                _report("wait_for_sources", f"Testuji {rname}…", 1)
                if rclone_is_reachable(rname):
                    available.append(rname)
                    logger.info("Source %s: reachable", rname)
                else:
                    logger.info("Source %s: not reachable, waiting %ds…", rname, config.source_wait_timeout)
                    if wait_for_connectivity(rname, timeout=config.source_wait_timeout):
                        available.append(rname)
                    else:
                        unavailable.append(rname)
                        logger.warning("Source %s: still unreachable after %ds", rname, config.source_wait_timeout)

            results["sources"] = {"available": available, "unavailable": unavailable}
            progress.sources_available = available
            progress.sources_unavailable = unavailable

            if not available:
                error = "Žádný zdroj není dostupný — zkontroluj připojení"
                ckpt.update_job(cat, job.job_id, status="paused", error=error)
                progress.paused = True
                progress.error = error
                _report("wait_for_sources", error, 1)
                return results

            _finish_phase("wait_for_sources")
        else:
            logger.info("Phase wait_for_sources already done, skipping")
            # Reconstruct available list from config
            source_remotes = config.source_remotes or [r.name for r in list_remotes()]
            available = [r for r in source_remotes if rclone_is_reachable(r, timeout=5)]
            unavailable = [r for r in source_remotes if r not in available]
            results["sources"] = {"available": available, "unavailable": unavailable}

        # ══════════════════════════════════════════════════════════
        # Phase 2: Cloud catalog scan (paginated, writes to files table)
        # ══════════════════════════════════════════════════════════
        if not _phase_done("cloud_catalog_scan"):
            _report("cloud_catalog_scan", "Katalogizace vzdálených zdrojů…", 2)
            ckpt.update_job(cat, job.job_id, current_step="cloud_catalog_scan")

            total_cataloged = 0
            total_skipped_non_media = 0
            conn = cat.conn

            for rname in available:
                _report("cloud_catalog_scan", f"Skenuji {rname}…", 2,
                        files_cataloged=total_cataloged)
                try:
                    # Paginated listing — safe for millions of files
                    files = rclone_ls_paginated(
                        rname, "",
                        max_depth=-1,  # full recursive
                        inter_page_delay=config.api_delay,
                    )

                    for f in files:
                        if f.get("IsDir"):
                            continue

                        fpath = f.get("Path", f.get("Name", ""))
                        fsize = f.get("Size", 0) or 0

                        # Filter non-media files
                        if config.media_only and not _is_media_file(fpath):
                            total_skipped_non_media += 1
                            continue

                        # Extract date from rclone metadata
                        mod_time = f.get("ModTime", "")

                        # Compute a surrogate hash for cloud files:
                        # SHA256 of "remote:path:size" — unique enough for dedup
                        cloud_hash_input = f"{rname}:{fpath}:{fsize}"
                        surrogate_hash = hashlib.sha256(cloud_hash_input.encode()).hexdigest()

                        # Check if real hash is available (some remotes provide it)
                        real_hash = None
                        hashes = f.get("Hashes", {})
                        if hashes:
                            real_hash = hashes.get("sha256") or hashes.get("SHA-256")

                        effective_hash = real_hash or surrogate_hash

                        # INSERT into files table (or update if exists)
                        now_str = datetime.now(timezone.utc).isoformat()
                        try:
                            conn.execute("""
                                INSERT INTO files (path, size, mtime, ctime, birthtime, ext,
                                    sha256, source_remote, first_seen, last_scanned, date_original)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(path) DO UPDATE SET
                                    size = excluded.size,
                                    sha256 = COALESCE(excluded.sha256, sha256),
                                    source_remote = excluded.source_remote,
                                    last_scanned = excluded.last_scanned
                            """, (
                                f"{rname}:{fpath}",  # full path with remote prefix
                                fsize,
                                0.0,  # mtime placeholder
                                0.0,  # ctime placeholder
                                None,
                                PurePosixPath(fpath).suffix.lower(),
                                effective_hash,
                                rname,
                                now_str,
                                now_str,
                                mod_time[:10] if mod_time else None,  # date part only
                            ))
                        except Exception as exc:
                            logger.debug("catalog insert error for %s:%s: %s", rname, fpath, exc)
                            continue

                        total_cataloged += 1

                        # Checkpoint every 1000 files
                        if total_cataloged % 1000 == 0:
                            conn.commit()
                            _report("cloud_catalog_scan", f"Skenuji {rname}… ({total_cataloged})", 2,
                                    files_cataloged=total_cataloged)

                    conn.commit()

                except Exception as exc:
                    logger.warning("cloud_catalog_scan: %s error: %s", rname, exc)
                    conn.rollback()

            results["catalog"] = {
                "total_cataloged": total_cataloged,
                "skipped_non_media": total_skipped_non_media,
            }
            _report("cloud_catalog_scan", "Katalogizace hotová", 2,
                    files_cataloged=total_cataloged)
            _finish_phase("cloud_catalog_scan")
        else:
            logger.info("Phase cloud_catalog_scan already done, skipping")
            total_cataloged = 0  # will be counted from DB

        # ══════════════════════════════════════════════════════════
        # Phase 3: Local scan
        # ══════════════════════════════════════════════════════════
        if not _phase_done("local_scan"):
            _report("local_scan", "Skenování lokálních souborů…", 3)
            ckpt.update_job(cat, job.job_id, current_step="local_scan")

            local_scanned = 0
            if config.local_roots:
                try:
                    from .scanner import incremental_scan
                    stats = incremental_scan(
                        roots=[Path(r) for r in config.local_roots],
                        catalog_path=catalog_path,
                        workers=config.scan_workers,
                    )
                    local_scanned = getattr(stats, "total_files", 0) if stats else 0
                except Exception as exc:
                    logger.warning("Local scan error: %s", exc)

            results["local_scan"] = {"scanned": local_scanned}
            _finish_phase("local_scan")
        else:
            logger.info("Phase local_scan already done, skipping")
            local_scanned = 0

        # ══════════════════════════════════════════════════════════
        # Phase 4: Pre-transfer dedup (real hashes only)
        # ══════════════════════════════════════════════════════════
        # Only deduplicate files where we have REAL SHA256 hashes (local files,
        # or remotes that provide content hashes). Cloud files with surrogate
        # hashes are NOT deduplicated here — they ALL get transferred to the
        # destination. Cross-source dedup happens in Phase 6 via `rclone dedupe`
        # on the destination, where Google Drive provides real MD5 hashes.
        # This is the safest approach: better to transfer 20% extra than to
        # lose a unique file due to heuristic misclassification.
        if not _phase_done("dedup"):
            _report("dedup", "Pre-transfer deduplikace (reálné hashe)…", 4)
            ckpt.update_job(cat, job.job_id, current_step="dedup")

            conn = cat.conn
            conn.row_factory = sqlite3.Row

            # Count duplicate groups with real SHA256 (local files)
            cur = conn.execute("""
                SELECT sha256, COUNT(*) as cnt
                FROM files
                WHERE sha256 IS NOT NULL
                  AND (source_remote IS NULL OR source_remote = '' OR source_remote = 'local')
                GROUP BY sha256
                HAVING cnt > 1
            """)
            local_dedup_groups = cur.fetchall()
            local_duplicates = sum(row["cnt"] - 1 for row in local_dedup_groups)

            # Total unique per source (for info)
            cur2 = conn.execute("""
                SELECT COUNT(DISTINCT sha256) as uniq
                FROM files WHERE sha256 IS NOT NULL
            """)
            total_unique = cur2.fetchone()["uniq"]

            # Count cloud files that will ALL be transferred (deduped later on dest)
            cur3 = conn.execute("""
                SELECT COUNT(*) as cnt
                FROM files
                WHERE sha256 IS NOT NULL
                  AND source_remote IS NOT NULL AND source_remote != '' AND source_remote != 'local'
            """)
            cloud_files_total = cur3.fetchone()["cnt"]

            results["dedup"] = {
                "unique_hashes": total_unique,
                "local_duplicate_groups": len(local_dedup_groups),
                "local_duplicates_skipped": local_duplicates,
                "cloud_files_to_transfer": cloud_files_total,
                "note": "Cross-source dedup proběhne po přenosu na cíl (Phase 6, rclone dedupe)",
            }
            _report("dedup", "Pre-transfer deduplikace hotová", 4,
                    files_unique=total_unique, files_duplicate=local_duplicates)
            _finish_phase("dedup")
        else:
            logger.info("Phase dedup already done, skipping")
            conn = cat.conn
            conn.row_factory = sqlite3.Row
            cur2 = conn.execute("SELECT COUNT(DISTINCT sha256) as uniq FROM files WHERE sha256 IS NOT NULL")
            total_unique = cur2.fetchone()["uniq"]

        # ══════════════════════════════════════════════════════════
        # Phase 5: Stream cloud→cloud (the big one)
        # ══════════════════════════════════════════════════════════
        _report("stream", "Příprava streamování cloud→cloud…", 5)
        ckpt.update_job(cat, job.job_id, current_step="stream")
        ckpt.reset_stale_in_progress(cat, job.job_id, "stream")

        conn = cat.conn
        conn.row_factory = sqlite3.Row

        # Get ALL cloud files for transfer (no pre-grouping by sha256!)
        # Cross-source dedup happens AFTER transfer via rclone dedupe (Phase 6).
        # This ensures we never lose a unique file due to surrogate hash collision.
        cur = conn.execute("""
            SELECT sha256, path, source_remote, size, date_original,
                   metadata_richness
            FROM files
            WHERE sha256 IS NOT NULL
              AND source_remote IS NOT NULL
              AND source_remote != ''
              AND source_remote != 'local'
            ORDER BY source_remote, path
        """)
        unique_cloud_files = cur.fetchall()

        # Also get local-only unique files (for tracking, they skip streaming)
        cur_local = conn.execute("""
            SELECT sha256, path, size, date_original
            FROM files
            WHERE sha256 IS NOT NULL
              AND (source_remote IS NULL OR source_remote = '' OR source_remote = 'local')
            GROUP BY sha256
        """)
        local_only_files = cur_local.fetchall()

        # Estimate total bytes
        total_bytes_estimate = sum(row["size"] or 0 for row in unique_cloud_files)
        progress.bytes_total_estimate = total_bytes_estimate

        # Register pending transfers (skip already registered from previous run)
        registered = 0
        for row in unique_cloud_files:
            source = row["source_remote"] or "local"
            fpath = row["path"]
            # Don't re-register completed files
            ckpt.mark_file(
                cat, job.job_id, row["sha256"],
                f"{source}:{fpath}" if ":" not in fpath else fpath,
                "stream", "pending",
            )
            registered += 1

        # Mark local files as skipped (they'll be handled by sync_to_disk)
        for row in local_only_files:
            ckpt.mark_file(
                cat, job.job_id, row["sha256"],
                f"local:{row['path']}", "stream", "skipped",
            )

        logger.info("Registered %d cloud files for streaming, %d local files skipped",
                     registered, len(local_only_files))

        if config.dry_run:
            # Dry run — collect stats without transferring
            stream_progress = ckpt.get_job_progress(cat, job.job_id, "stream")
            results["stream"] = {
                "dry_run": True,
                "would_transfer": stream_progress["pending"],
                "would_skip": stream_progress["skipped"],
                "estimated_bytes": total_bytes_estimate,
                "estimated_time_hours": total_bytes_estimate / (5_000_000 * 3600) if total_bytes_estimate else 0,
            }
            _report("stream", "Dry run — přehled hotový", 5)
        else:
            # Execute streaming transfers
            stream_start_time = time.monotonic()
            total_stream_bytes = 0

            # Rebuild dest_paths_used from checkpoint DB (survive resume!)
            dest_paths_used: set[str] = set()
            completed_dests = conn.execute("""
                SELECT dest_location FROM consolidation_file_state
                WHERE job_id = ? AND step_name = 'stream'
                  AND status = 'completed' AND dest_location IS NOT NULL
            """, (job.job_id,)).fetchall()
            for row_d in completed_dests:
                # dest_location is "remote:path" — extract path part
                dl = row_d["dest_location"]
                if ":" in dl:
                    dest_paths_used.add(dl.split(":", 1)[1])
            if dest_paths_used:
                logger.info("Resume: loaded %d existing dest paths for collision detection",
                           len(dest_paths_used))

            pending = ckpt.get_pending_files(cat, job.job_id, "stream", limit=200)

            _QUOTA_ERRORS = ("quota", "insufficient storage", "no space", "storage limit",
                             "rate limit exceeded", "user rate limit")

            while pending:
                # ── Check if job was paused externally (via API) ──
                _job_check = ckpt.get_job(cat, job.job_id)
                if _job_check and _job_check.status == "paused":
                    logger.info("Job %s paused externally, stopping stream", job.job_id)
                    progress.paused = True
                    _report("stream", "Pozastaveno uživatelem", 5)
                    break

                # ── Destination connectivity check ──
                if not rclone_is_reachable(config.dest_remote, timeout=15):
                    logger.warning("Destination %s unreachable, waiting…", config.dest_remote)
                    _report("stream", f"Čekání na {config.dest_remote}…", 5, paused=True)
                    if not wait_for_connectivity(config.dest_remote, timeout=config.connectivity_timeout):
                        ckpt.update_job(cat, job.job_id, status="paused",
                                        error="Cílové úložiště nedostupné")
                        progress.paused = True
                        _report("stream", "Pozastaveno — cíl nedostupný", 5)
                        break

                # Group pending by source remote to detect source failures
                source_failures: dict[str, int] = {}

                for fs in pending:
                    parts = fs.source_location.split(":", 1)
                    src_remote = parts[0]
                    src_path = parts[1] if len(parts) > 1 else parts[0]

                    if src_remote == "local":
                        ckpt.mark_file(cat, job.job_id, fs.file_hash,
                                       fs.source_location, "stream", "skipped")
                        continue

                    # ── Source remote connectivity check ──
                    if source_failures.get(src_remote, 0) >= 5:
                        # This source remote has failed 5+ times in this batch — skip for now
                        logger.warning("Source %s has %d consecutive failures, deferring remaining files",
                                       src_remote, source_failures[src_remote])
                        continue

                    if not rclone_is_reachable(src_remote, timeout=10):
                        logger.warning("Source %s unreachable, waiting…", src_remote)
                        if not wait_for_connectivity(src_remote, timeout=60):
                            source_failures[src_remote] = source_failures.get(src_remote, 0) + 5
                            logger.warning("Source %s still unreachable, skipping batch", src_remote)
                            continue

                    # ── Build collision-safe destination path ──
                    filename = PurePosixPath(src_path).name

                    # Look up date_original for this file
                    file_row = conn.execute(
                        "SELECT date_original FROM files WHERE sha256 = ? LIMIT 1",
                        (fs.file_hash,),
                    ).fetchone()
                    mod_time = file_row["date_original"] if file_row else None

                    dest_path = _build_dest_path(
                        config.dest_path, filename, fs.file_hash,
                        mod_time, config.structure_pattern,
                    )

                    # Collision detection
                    if dest_path in dest_paths_used:
                        dest_path = _make_collision_safe(dest_path, fs.file_hash)
                    dest_paths_used.add(dest_path)

                    # ── Get file size for dynamic timeout ──
                    file_size_row = conn.execute(
                        "SELECT size FROM files WHERE sha256 = ? LIMIT 1",
                        (fs.file_hash,),
                    ).fetchone()
                    file_size = file_size_row["size"] if file_size_row else None

                    # ── Transfer ──
                    ckpt.mark_file(cat, job.job_id, fs.file_hash,
                                   fs.source_location, "stream", "in_progress")

                    try:
                        result = retry_with_backoff(
                            rclone_copyto,
                            src_remote, src_path, config.dest_remote, dest_path,
                            max_retries=config.max_transfer_retries,
                            retryable_exceptions=(RuntimeError, OSError),
                            file_size=file_size,
                            bwlimit=config.bwlimit,
                            checksum=True,
                        )

                        if result["success"]:
                            # ── Post-transfer verification ──
                            verify = rclone_verify_transfer(
                                config.dest_remote, dest_path,
                                expected_size=file_size,
                            )

                            if verify["verified"]:
                                ckpt.mark_file(
                                    cat, job.job_id, fs.file_hash, fs.source_location,
                                    "stream", "completed",
                                    dest=f"{config.dest_remote}:{dest_path}",
                                    bytes_transferred=result["bytes"] or file_size or 0,
                                )
                                total_stream_bytes += result["bytes"] or file_size or 0
                                source_failures[src_remote] = 0  # reset on success
                            else:
                                # Transfer "succeeded" but verification failed!
                                error_msg = f"Verification failed: {verify.get('error', 'unknown')}"
                                logger.error("VERIFY FAIL for %s: %s", fs.source_location, error_msg)
                                ckpt.mark_file(
                                    cat, job.job_id, fs.file_hash, fs.source_location,
                                    "stream", "failed", error=error_msg,
                                )
                        else:
                            error_msg = result.get("error", "unknown")[:200]
                            ckpt.mark_file(
                                cat, job.job_id, fs.file_hash, fs.source_location,
                                "stream", "failed", error=error_msg,
                            )
                            source_failures[src_remote] = source_failures.get(src_remote, 0) + 1

                            # ── Quota/disk-full detection → auto-pause ──
                            if any(q in error_msg.lower() for q in _QUOTA_ERRORS):
                                logger.error("QUOTA/RATE LIMIT detected: %s — pausing job", error_msg)
                                ckpt.update_job(cat, job.job_id, status="paused",
                                                error=f"Cílové úložiště plné nebo rate limit: {error_msg[:100]}")
                                progress.paused = True
                                _report("stream", "Pozastaveno — úložiště plné", 5)
                                break

                    except Exception as exc:
                        ckpt.mark_file(
                            cat, job.job_id, fs.file_hash, fs.source_location,
                            "stream", "failed", error=str(exc)[:200],
                        )
                        source_failures[src_remote] = source_failures.get(src_remote, 0) + 1

                    # ── Progress update ──
                    p = ckpt.get_job_progress(cat, job.job_id, "stream")
                    elapsed = time.monotonic() - stream_start_time
                    speed = _estimate_speed(total_stream_bytes, elapsed)
                    remaining_bytes = total_bytes_estimate - total_stream_bytes
                    eta = int(remaining_bytes / speed) if speed > 0 else 0

                    _report("stream", "Streaming cloud→cloud…", 5,
                            files_transferred=p["completed"],
                            bytes_transferred=p["bytes_transferred"],
                            errors=p["failed"],
                            transfer_speed_bps=speed,
                            eta_seconds=eta)

                # Exit if paused (by user, quota, or connectivity)
                if progress.paused:
                    break
                # Next batch
                pending = ckpt.get_pending_files(cat, job.job_id, "stream", limit=200)

            stream_progress = ckpt.get_job_progress(cat, job.job_id, "stream")
            results["stream"] = {
                "transferred": stream_progress["completed"],
                "failed": stream_progress["failed"],
                "skipped": stream_progress["skipped"],
                "bytes": stream_progress["bytes_transferred"],
            }

        # ══════════════════════════════════════════════════════════
        # Phase 6: Retry failed files
        # ══════════════════════════════════════════════════════════
        if not config.dry_run and not _phase_done("retry_failed"):
            failed_files = ckpt.get_failed_files(cat, job.job_id, "stream", limit=5000)

            if failed_files:
                _report("retry_failed", f"Opakování {len(failed_files)} neúspěšných přenosů…", 6)
                ckpt.update_job(cat, job.job_id, current_step="retry_failed")

                retried_ok = 0
                retried_fail = 0

                for fs in failed_files:
                    # Only retry files with < 5 attempts
                    if fs.attempt_count >= 5:
                        logger.warning("Skipping %s: too many attempts (%d)", fs.source_location, fs.attempt_count)
                        continue

                    parts = fs.source_location.split(":", 1)
                    src_remote = parts[0]
                    src_path = parts[1] if len(parts) > 1 else parts[0]

                    if src_remote == "local":
                        continue

                    # Check source reachability
                    if not rclone_is_reachable(src_remote, timeout=15):
                        if not wait_for_connectivity(src_remote, timeout=120):
                            logger.warning("Source %s unreachable for retry, skipping", src_remote)
                            continue

                    # Rebuild dest path
                    filename = PurePosixPath(src_path).name
                    file_row = conn.execute(
                        "SELECT date_original, size FROM files WHERE sha256 = ? LIMIT 1",
                        (fs.file_hash,),
                    ).fetchone()
                    mod_time = file_row["date_original"] if file_row else None
                    file_size = file_row["size"] if file_row else None

                    dest_path = _build_dest_path(
                        config.dest_path, filename, fs.file_hash,
                        mod_time, config.structure_pattern,
                    )

                    # Use longer timeout for retry (explicit timeout, not fake file_size)
                    try:
                        retry_timeout = int(_dynamic_timeout(file_size) * config.retry_timeout_multiplier) if file_size else 600
                        result = rclone_copyto(
                            src_remote, src_path, config.dest_remote, dest_path,
                            file_size=file_size,
                            timeout=retry_timeout,
                            bwlimit=config.bwlimit,
                            checksum=True,
                        )

                        if result["success"]:
                            verify = rclone_verify_transfer(
                                config.dest_remote, dest_path, expected_size=file_size,
                            )
                            if verify["verified"]:
                                ckpt.mark_file(
                                    cat, job.job_id, fs.file_hash, fs.source_location,
                                    "stream", "completed",
                                    dest=f"{config.dest_remote}:{dest_path}",
                                    bytes_transferred=result["bytes"] or file_size or 0,
                                )
                                retried_ok += 1
                            else:
                                ckpt.mark_file(
                                    cat, job.job_id, fs.file_hash, fs.source_location,
                                    "stream", "failed",
                                    error=f"Retry verify failed: {verify.get('error', '')}",
                                )
                                retried_fail += 1
                        else:
                            ckpt.mark_file(
                                cat, job.job_id, fs.file_hash, fs.source_location,
                                "stream", "failed", error=result.get("error", "retry failed")[:200],
                            )
                            retried_fail += 1
                    except Exception as exc:
                        ckpt.mark_file(
                            cat, job.job_id, fs.file_hash, fs.source_location,
                            "stream", "failed", error=f"Retry exception: {str(exc)[:150]}",
                        )
                        retried_fail += 1

                    _report("retry_failed", f"Retry: {retried_ok} OK, {retried_fail} fail", 6,
                            files_retried=retried_ok)

                results["retry"] = {"retried_ok": retried_ok, "retried_fail": retried_fail}
            else:
                results["retry"] = {"retried_ok": 0, "retried_fail": 0}

            _finish_phase("retry_failed")

        # ══════════════════════════════════════════════════════════
        # Phase 7: Verify integrity on destination (BEFORE dedupe!)
        # ══════════════════════════════════════════════════════════
        # Verify runs BEFORE dedupe so that all transferred files are
        # still present and can be checked. After dedupe, some would be
        # removed and verification would count them as false failures.
        if not config.dry_run and not _phase_done("verify"):
            _report("verify", "Ověřování integrity na cíli…", 7)
            ckpt.update_job(cat, job.job_id, current_step="verify")

            if rclone_is_reachable(config.dest_remote):
                # Get ALL completed transfers for verification
                conn.row_factory = sqlite3.Row
                completed_transfers = conn.execute("""
                    SELECT file_hash, dest_location, bytes_transferred
                    FROM consolidation_file_state
                    WHERE job_id = ? AND step_name = 'stream' AND status = 'completed'
                      AND dest_location IS NOT NULL
                """, (job.job_id,)).fetchall()

                # Apply verify_pct
                if config.verify_pct < 100:
                    import random
                    sample_size = max(1, len(completed_transfers) * config.verify_pct // 100)
                    completed_transfers = random.sample(completed_transfers, min(sample_size, len(completed_transfers)))

                verified_ok = 0
                verified_fail = 0
                total_to_verify = len(completed_transfers)

                for idx, row in enumerate(completed_transfers):
                    dest_loc = row["dest_location"]
                    if not dest_loc or ":" not in dest_loc:
                        continue

                    remote, path = dest_loc.split(":", 1)
                    expected_bytes = row["bytes_transferred"]

                    check = rclone_check_file(remote, path, expected_size=expected_bytes)
                    if check["exists"] and (check.get("size_match") is not False):
                        verified_ok += 1
                    else:
                        verified_fail += 1
                        logger.error("VERIFY FAIL: %s (exists=%s, size_match=%s)",
                                     dest_loc, check["exists"], check.get("size_match"))

                    if (idx + 1) % 50 == 0:
                        _report("verify", f"Ověřeno {idx + 1}/{total_to_verify}…", 7,
                                files_verified=verified_ok, errors=verified_fail)

                results["verify"] = {
                    "total_checked": total_to_verify,
                    "verified_ok": verified_ok,
                    "verified_fail": verified_fail,
                }
                _report("verify", "Ověření hotové", 7,
                        files_verified=verified_ok, errors=verified_fail)
            else:
                results["verify"] = {"note": "Cílové úložiště nedostupné pro ověření"}

            _finish_phase("verify")

        # ══════════════════════════════════════════════════════════
        # Phase 8: Post-transfer deduplication (rclone dedupe)
        # ══════════════════════════════════════════════════════════
        # Runs AFTER verify so that verification counts are not inflated
        # by dedupe-removed files. Uses "largest" mode to keep highest
        # quality copy (not "newest" which could keep a re-encoded version).
        if not config.dry_run and not _phase_done("post_transfer_dedup"):
            _report("post_transfer_dedup", "Post-transfer deduplikace na cíli…", 8)
            ckpt.update_job(cat, job.job_id, current_step="post_transfer_dedup")

            if rclone_is_reachable(config.dest_remote):
                dedup_result = rclone_dedupe(
                    config.dest_remote,
                    config.dest_path,
                    mode="largest",  # keep largest copy (highest quality) — safest for media
                    dry_run=False,
                    timeout=7200,  # 2h for large datasets
                )

                results["post_transfer_dedup"] = {
                    "success": dedup_result["success"],
                    "duplicates_removed": dedup_result.get("duplicates_removed", 0),
                    "bytes_freed": dedup_result.get("bytes_freed", 0),
                }

                if dedup_result["success"]:
                    logger.info("Post-transfer dedupe: removed %d duplicates, freed %d bytes",
                               dedup_result.get("duplicates_removed", 0),
                               dedup_result.get("bytes_freed", 0))
                else:
                    logger.warning("Post-transfer dedupe had issues: %s", dedup_result.get("error", ""))

                _report("post_transfer_dedup", "Post-transfer deduplikace hotová", 8)
            else:
                results["post_transfer_dedup"] = {
                    "note": "Cílové úložiště nedostupné pro deduplikaci"
                }
            _finish_phase("post_transfer_dedup")
        elif config.dry_run:
            results["post_transfer_dedup"] = {
                "dry_run": True,
                "note": "Deduplikace proběhne po přenosu pomocí rclone dedupe (mode=largest)",
            }

        # ══════════════════════════════════════════════════════════
        # Phase 9: Sync to disk
        # ══════════════════════════════════════════════════════════
        if not config.dry_run and not _phase_done("sync_to_disk"):
            _report("sync_to_disk", "Synchronizace na disk…", 9)
            ckpt.update_job(cat, job.job_id, current_step="sync_to_disk")

            if check_volume_mounted(config.disk_path):
                from .cloud import rclone_copy
                try:
                    # rclone copy is incremental — already existing files are skipped
                    rclone_copy(
                        config.dest_remote, config.dest_path, config.disk_path,
                        progress_fn=lambda p: _report("sync_to_disk",
                            f"Synchronizace na disk… {p.get('progress_pct', 0)}%", 9),
                    )
                    results["sync"] = {"synced": True, "disk_path": config.disk_path}
                    _finish_phase("sync_to_disk")
                except Exception as exc:
                    results["sync"] = {"synced": False, "error": str(exc)[:200]}
                    logger.error("Sync to disk failed: %s", exc)
                    # DON'T mark phase done — resume will retry
                    ckpt.update_job(cat, job.job_id, status="paused",
                                    error=f"Sync selhal: {str(exc)[:150]}")
                    progress.paused = True
            else:
                results["sync"] = {
                    "synced": False,
                    "note": f"Disk {config.disk_path} není připojený — připoj disk a spusť resume",
                }
                # DON'T mark phase done — resume will retry
                ckpt.update_job(cat, job.job_id, status="paused",
                                error=f"Disk {config.disk_path} není připojený")
                progress.paused = True
        elif config.dry_run:
            results["sync"] = {"dry_run": True, "disk_path": config.disk_path}

        # ══════════════════════════════════════════════════════════
        # Phase 10: Final report
        # ══════════════════════════════════════════════════════════
        _report("report", "Generování závěrečného reportu…", 10)
        ckpt.update_job(cat, job.job_id, current_step="report")

        final_progress = ckpt.get_job_progress(cat, job.job_id, "stream")
        elapsed_total = time.monotonic() - stream_start_time if stream_start_time else 0

        results["summary"] = {
            "sources_available": len(available),
            "sources_unavailable": len(unavailable),
            "sources_unavailable_names": unavailable,
            "files_cataloged": results.get("catalog", {}).get("total_cataloged", 0) + local_scanned,
            "unique_files": total_unique,
            "local_duplicate_groups": results.get("dedup", {}).get("local_duplicate_groups", 0),
            "post_transfer_dedup_removed": results.get("post_transfer_dedup", {}).get("duplicates_removed", 0),
            "post_transfer_dedup_bytes_freed": results.get("post_transfer_dedup", {}).get("bytes_freed", 0),
            "files_transferred": final_progress.get("completed", 0),
            "bytes_transferred": final_progress.get("bytes_transferred", 0),
            "transfer_failures": final_progress.get("failed", 0),
            "files_retried_ok": results.get("retry", {}).get("retried_ok", 0),
            "files_retried_fail": results.get("retry", {}).get("retried_fail", 0),
            "verified_ok": results.get("verify", {}).get("verified_ok", 0),
            "verified_fail": results.get("verify", {}).get("verified_fail", 0),
            "synced_to_disk": results.get("sync", {}).get("synced", False),
            "elapsed_seconds": int(elapsed_total),
            "dry_run": config.dry_run,
        }

        # Log permanent failures
        still_failed = final_progress.get("failed", 0)
        if still_failed > 0:
            logger.error("CONSOLIDATION: %d files STILL FAILED after retry — manual intervention needed", still_failed)
            failed_list = ckpt.get_failed_files(cat, job.job_id, "stream", limit=100)
            for ff in failed_list:
                logger.error("  FAILED: %s → %s (attempts=%d)", ff.source_location, ff.last_error, ff.attempt_count)

        # Complete job (unless paused)
        if not progress.paused:
            if still_failed > 0:
                ckpt.complete_job(cat, job.job_id,
                                  error=f"{still_failed} souborů se nepodařilo přenést")
            else:
                ckpt.complete_job(cat, job.job_id)
            _report("complete", "Konsolidace dokončena", 10)

        return results

    except Exception as exc:
        logger.exception("Consolidation pipeline failed")
        if job:
            ckpt.complete_job(cat, job.job_id, error=str(exc)[:500])
        raise
    finally:
        cat.close()


# ── Public API ───────────────────────────────────────────────────────


def get_consolidation_status(catalog_path: str | Path) -> dict[str, Any]:
    """Get current consolidation status for UI display."""
    cat = Catalog(str(catalog_path))
    cat.open()
    try:
        jobs = ckpt.list_jobs(cat)
        consolidation_jobs = [j for j in jobs if j.job_type in ("ultimate_consolidation", "cloud_stream_reorganize")]
        active = [j for j in consolidation_jobs if j.status in ("created", "running", "paused")]

        result: dict[str, Any] = {
            "has_active_job": len(active) > 0,
            "total_jobs": len(consolidation_jobs),
            "jobs": [],
        }

        for j in consolidation_jobs[:10]:
            progress = ckpt.get_job_progress(cat, j.job_id)
            result["jobs"].append({
                "job_id": j.job_id,
                "status": j.status,
                "current_step": j.current_step,
                "created_at": j.created_at,
                "updated_at": j.updated_at,
                "completed_at": j.completed_at,
                "error": j.error,
                "progress": progress,
                "config": j.config,
            })

        return result
    finally:
        cat.close()


def preview_consolidation(
    catalog_path: str | Path,
    config: ConsolidationConfig | None = None,
) -> dict[str, Any]:
    """Dry-run: scan sources, count files, estimate transfer — no actual transfers.

    ALWAYS run this before the real thing.
    """
    config = config or ConsolidationConfig()
    config.dry_run = True
    return run_consolidation(catalog_path, config=config)


def pause_consolidation(catalog_path: str | Path) -> dict:
    """Pause the active consolidation job."""
    cat = Catalog(str(catalog_path))
    cat.open()
    try:
        jobs = ckpt.get_resumable_jobs(cat)
        for j in jobs:
            if j.job_type in ("ultimate_consolidation", "cloud_stream_reorganize") and j.status == "running":
                ckpt.pause_job(cat, j.job_id)
                return {"paused": True, "job_id": j.job_id}
        return {"paused": False, "note": "Žádný běžící job k pozastavení"}
    finally:
        cat.close()


def resume_consolidation(
    catalog_path: str | Path,
    config: ConsolidationConfig | None = None,
    progress_fn: Callable | None = None,
) -> dict[str, Any]:
    """Resume a paused/interrupted consolidation.

    Completed phases are automatically skipped.
    """
    return run_consolidation(catalog_path, config=config, progress_fn=progress_fn)


def get_failed_files_report(catalog_path: str | Path) -> list[dict]:
    """Get detailed report of all failed transfers for manual review."""
    cat = Catalog(str(catalog_path))
    cat.open()
    try:
        jobs = ckpt.list_jobs(cat)
        result = []
        for j in jobs:
            if j.job_type == "ultimate_consolidation":
                failed = ckpt.get_failed_files(cat, j.job_id, "stream", limit=10000)
                for ff in failed:
                    result.append({
                        "job_id": j.job_id,
                        "file_hash": ff.file_hash,
                        "source": ff.source_location,
                        "error": ff.last_error,
                        "attempts": ff.attempt_count,
                        "last_attempt": ff.updated_at,
                    })
        return result
    finally:
        cat.close()
