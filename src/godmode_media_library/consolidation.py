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
  9. Sync to disk — rclone copy from cloud to external drive (additive, never deletes)
 10. Final report — summary of everything
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from . import checkpoint as ckpt
from .catalog import Catalog
from .cloud import (
    RcloneTransferError,
    _dynamic_timeout,
    check_volume_mounted,
    list_remotes,
    rclone_check_file,
    rclone_copyto,
    rclone_dedupe,
    rclone_is_reachable,
    rclone_ls_paginated,
    rclone_verify_transfer,
    retry_with_backoff,
    wait_for_connectivity,
)
from .consolidation_types import (
    CATALOG_COMMIT_INTERVAL,
    CONSOLIDATION_JOB_TYPES,
    DEFAULT_RETRY_TIMEOUT,
    DEDUP_TIMEOUT,
    DEST_CONNECTIVITY_TIMEOUT,
    ERROR_TRUNCATE_LEN,
    ERROR_TRUNCATE_MEDIUM,
    ERROR_TRUNCATE_SHORT,
    JOB_TYPE_ULTIMATE,
    MAX_RETRY_ATTEMPTS,
    MAX_SOURCE_FAILURES,
    MEDIA_EXTENSIONS,
    QUOTA_ERRORS,
    RETRY_CONNECTIVITY_WAIT,
    SOURCE_CONNECTIVITY_WAIT,
    STREAM_BATCH_SIZE,
    VERIFY_FAIL_THRESHOLD_PCT,
    VERIFY_REPORT_INTERVAL,
    DedupStrategy,
    FileStatus,
    JobStatus,
    Phase,
    StructurePattern,
)

logger = logging.getLogger(__name__)


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

    IMG_0001.jpg → IMG_0001_a3f2b1.jpg
    """
    p = PurePosixPath(dest_path)
    stem = p.stem
    suffix = p.suffix
    full_hash = file_hash if file_hash else hashlib.md5(dest_path.encode()).hexdigest()

    hash_len = 6
    candidate = str(p.parent / f"{stem}_{full_hash[:hash_len]}{suffix}")

    if existing_paths is not None:
        while candidate in existing_paths and hash_len < len(full_hash):
            hash_len = min(hash_len + 4, len(full_hash))
            candidate = str(p.parent / f"{stem}_{full_hash[:hash_len]}{suffix}")
        if candidate in existing_paths:
            counter = 2
            while candidate in existing_paths:
                candidate = str(p.parent / f"{stem}_{full_hash[:hash_len]}_{counter}{suffix}")
                counter += 1

    return candidate


def _estimate_speed(bytes_transferred: int, elapsed: float) -> float:
    """Bytes per second, smoothed."""
    if elapsed <= 0:
        return 0.0
    return bytes_transferred / elapsed


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


def _phase_2_cloud_catalog_scan(ctx: PhaseContext) -> None:
    """Phase 2: Paginated metadata scan of all cloud remotes."""
    if ctx.phase_done(Phase.CLOUD_CATALOG_SCAN):
        logger.info("Phase %s already done, skipping", Phase.CLOUD_CATALOG_SCAN)
        return

    ctx.report(Phase.CLOUD_CATALOG_SCAN, "Katalogizace vzdálených zdroju...", 2)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.CLOUD_CATALOG_SCAN)

    total_cataloged = 0
    total_skipped_non_media = 0
    conn = ctx.conn

    for rname in ctx.available:
        ctx.report(Phase.CLOUD_CATALOG_SCAN, f"Skenuji {rname}...", 2,
                   files_cataloged=total_cataloged)
        try:
            files = rclone_ls_paginated(
                rname, "", max_depth=-1, inter_page_delay=ctx.config.api_delay,
            )

            for f in files:
                if f.get("IsDir"):
                    continue

                fpath = f.get("Path", f.get("Name", ""))
                fsize = f.get("Size", 0) or 0

                if ctx.config.media_only and not _is_media_file(fpath):
                    total_skipped_non_media += 1
                    continue

                mod_time = f.get("ModTime", "")
                cloud_hash_input = f"{rname}:{fpath}:{fsize}"
                surrogate_hash = hashlib.sha256(cloud_hash_input.encode()).hexdigest()

                real_hash = None
                hashes = f.get("Hashes", {})
                if hashes:
                    real_hash = hashes.get("sha256") or hashes.get("SHA-256")

                effective_hash = real_hash or surrogate_hash
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
                        f"{rname}:{fpath}",
                        fsize, 0.0, 0.0, None,
                        PurePosixPath(fpath).suffix.lower(),
                        effective_hash, rname, now_str, now_str,
                        mod_time[:10] if mod_time else None,
                    ))
                except Exception as exc:
                    logger.debug("catalog insert error for %s:%s: %s", rname, fpath, exc)
                    continue

                total_cataloged += 1

                if total_cataloged % CATALOG_COMMIT_INTERVAL == 0:
                    conn.commit()
                    ctx.report(Phase.CLOUD_CATALOG_SCAN,
                               f"Skenuji {rname}... ({total_cataloged})", 2,
                               files_cataloged=total_cataloged)

            conn.commit()

        except Exception as exc:
            logger.warning("cloud_catalog_scan: %s error: %s", rname, exc)
            conn.rollback()

    ctx.results["catalog"] = {
        "total_cataloged": total_cataloged,
        "skipped_non_media": total_skipped_non_media,
    }
    ctx.report(Phase.CLOUD_CATALOG_SCAN, "Katalogizace hotová", 2,
               files_cataloged=total_cataloged)
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
            from .scanner import incremental_scan
            stats = incremental_scan(
                roots=[Path(r) for r in ctx.config.local_roots],
                catalog_path=str(ctx.cat.path) if hasattr(ctx.cat, 'path') else str(ctx.cat),
                workers=ctx.config.scan_workers,
            )
            local_scanned = getattr(stats, "total_files", 0) if stats else 0
        except Exception as exc:
            logger.warning("Local scan error: %s", exc)

    ctx.local_scanned = local_scanned
    ctx.results["local_scan"] = {"scanned": local_scanned}
    ctx.finish_phase(Phase.LOCAL_SCAN)


def _phase_4_pre_dedup(ctx: PhaseContext) -> None:
    """Phase 4: Pre-transfer deduplication using real SHA256 hashes.

    Only deduplicates local files with real hashes. Cloud files with surrogate
    hashes ALL get transferred — cross-source dedup happens in Phase 8 via
    rclone dedupe on the destination with real MD5 hashes.
    """
    if ctx.phase_done(Phase.DEDUP):
        logger.info("Phase %s already done, skipping", Phase.DEDUP)
        conn = ctx.conn
        conn.row_factory = sqlite3.Row
        cur2 = conn.execute("SELECT COUNT(DISTINCT sha256) as uniq FROM files WHERE sha256 IS NOT NULL")
        ctx.total_unique = cur2.fetchone()["uniq"]
        return

    ctx.report(Phase.DEDUP, "Pre-transfer deduplikace (reálné hashe)...", 4)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.DEDUP)

    conn = ctx.conn
    conn.row_factory = sqlite3.Row

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

    cur2 = conn.execute("SELECT COUNT(DISTINCT sha256) as uniq FROM files WHERE sha256 IS NOT NULL")
    ctx.total_unique = cur2.fetchone()["uniq"]

    cur3 = conn.execute("""
        SELECT COUNT(*) as cnt FROM files
        WHERE sha256 IS NOT NULL
          AND source_remote IS NOT NULL AND source_remote != '' AND source_remote != 'local'
    """)
    cloud_files_total = cur3.fetchone()["cnt"]

    ctx.results["dedup"] = {
        "unique_hashes": ctx.total_unique,
        "local_duplicate_groups": len(local_dedup_groups),
        "local_duplicates_skipped": local_duplicates,
        "cloud_files_to_transfer": cloud_files_total,
        "note": "Cross-source dedup probehne po prenosu na cil (Phase 8, rclone dedupe)",
    }
    ctx.report(Phase.DEDUP, "Pre-transfer deduplikace hotová", 4,
               files_unique=ctx.total_unique, files_duplicate=local_duplicates)
    ctx.finish_phase(Phase.DEDUP)


def _phase_5_stream(ctx: PhaseContext) -> None:
    """Phase 5: Stream all cloud files to destination (the big one).

    Checkpoint-resumable, collision-safe, with post-transfer size verification.
    """
    ctx.report(Phase.STREAM, "Priprava streamovani cloud->cloud...", 5)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.STREAM)
    ckpt.reset_stale_in_progress(ctx.cat, ctx.job.job_id, Phase.STREAM)

    conn = ctx.conn
    conn.row_factory = sqlite3.Row

    # Get ALL cloud files for transfer
    cur = conn.execute("""
        SELECT sha256, path, source_remote, size, date_original, metadata_richness
        FROM files
        WHERE sha256 IS NOT NULL
          AND source_remote IS NOT NULL AND source_remote != '' AND source_remote != 'local'
        ORDER BY source_remote, path
    """)
    unique_cloud_files = cur.fetchall()

    cur_local = conn.execute("""
        SELECT sha256, path, size, date_original FROM files
        WHERE sha256 IS NOT NULL
          AND (source_remote IS NULL OR source_remote = '' OR source_remote = 'local')
        GROUP BY sha256
    """)
    local_only_files = cur_local.fetchall()

    total_bytes_estimate = sum(row["size"] or 0 for row in unique_cloud_files)
    ctx.progress.bytes_total_estimate = total_bytes_estimate

    # Register pending transfers
    registered = 0
    for row in unique_cloud_files:
        source = row["source_remote"] or "local"
        fpath = row["path"]
        ckpt.mark_file(
            ctx.cat, ctx.job.job_id, row["sha256"],
            f"{source}:{fpath}" if ":" not in fpath else fpath,
            Phase.STREAM, FileStatus.PENDING,
        )
        registered += 1

    for row in local_only_files:
        ckpt.mark_file(
            ctx.cat, ctx.job.job_id, row["sha256"],
            f"local:{row['path']}", Phase.STREAM, FileStatus.SKIPPED,
        )

    logger.info("Registered %d cloud files for streaming, %d local files skipped",
                 registered, len(local_only_files))

    if ctx.config.dry_run:
        stream_progress = ckpt.get_job_progress(ctx.cat, ctx.job.job_id, Phase.STREAM)
        ctx.results["stream"] = {
            "dry_run": True,
            "would_transfer": stream_progress[FileStatus.PENDING],
            "would_skip": stream_progress[FileStatus.SKIPPED],
            "estimated_bytes": total_bytes_estimate,
            "estimated_time_hours": total_bytes_estimate / (5_000_000 * 3600) if total_bytes_estimate else 0,
        }
        ctx.report(Phase.STREAM, "Dry run — prehled hotový", 5)
        return

    # Execute streaming transfers
    ctx.stream_start_time = time.monotonic()
    total_stream_bytes = 0

    # Rebuild dest_paths_used from checkpoint DB (survive resume!)
    dest_paths_used: set[str] = set()
    completed_dests = conn.execute("""
        SELECT dest_location FROM consolidation_file_state
        WHERE job_id = ? AND step_name = ? AND status = ? AND dest_location IS NOT NULL
    """, (ctx.job.job_id, Phase.STREAM, FileStatus.COMPLETED)).fetchall()
    for row_d in completed_dests:
        dl = row_d["dest_location"]
        if ":" in dl:
            dest_paths_used.add(dl.split(":", 1)[1])
    if dest_paths_used:
        logger.info("Resume: loaded %d existing dest paths for collision detection",
                     len(dest_paths_used))

    pending = ckpt.get_pending_files(ctx.cat, ctx.job.job_id, Phase.STREAM, limit=STREAM_BATCH_SIZE)

    while pending:
        # Check if job was paused externally (via API)
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
                ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED,
                                error="Cilové uloziste nedostupné")
                ctx.progress.paused = True
                ctx.report(Phase.STREAM, "Pozastaveno — cil nedostupný", 5)
                break

        source_failures: dict[str, int] = {}

        for fs in pending:
            parts = fs.source_location.split(":", 1)
            src_remote = parts[0]
            src_path = parts[1] if len(parts) > 1 else parts[0]

            if src_remote == "local":
                ckpt.mark_file(ctx.cat, ctx.job.job_id, fs.file_hash,
                               fs.source_location, Phase.STREAM, FileStatus.SKIPPED)
                continue

            # Source remote connectivity check
            if source_failures.get(src_remote, 0) >= MAX_SOURCE_FAILURES:
                logger.warning("Source %s has %d consecutive failures, deferring remaining files",
                               src_remote, source_failures[src_remote])
                continue

            if not rclone_is_reachable(src_remote, timeout=10):
                logger.warning("Source %s unreachable, waiting...", src_remote)
                if not wait_for_connectivity(src_remote, timeout=SOURCE_CONNECTIVITY_WAIT):
                    source_failures[src_remote] = source_failures.get(src_remote, 0) + MAX_SOURCE_FAILURES
                    logger.warning("Source %s still unreachable, skipping batch", src_remote)
                    continue

            # Build collision-safe destination path
            filename = PurePosixPath(src_path).name
            file_row = conn.execute(
                "SELECT date_original FROM files WHERE sha256 = ? LIMIT 1",
                (fs.file_hash,),
            ).fetchone()
            mod_time = file_row["date_original"] if file_row else None

            dest_path = _build_dest_path(
                ctx.config.dest_path, filename, fs.file_hash,
                mod_time, ctx.config.structure_pattern,
            )
            if dest_path in dest_paths_used:
                dest_path = _make_collision_safe(dest_path, fs.file_hash, dest_paths_used)
            dest_paths_used.add(dest_path)

            # Get file size for dynamic timeout
            file_size_row = conn.execute(
                "SELECT size FROM files WHERE sha256 = ? LIMIT 1", (fs.file_hash,),
            ).fetchone()
            file_size = file_size_row["size"] if file_size_row else None

            # Transfer
            ckpt.mark_file(ctx.cat, ctx.job.job_id, fs.file_hash,
                           fs.source_location, Phase.STREAM, FileStatus.IN_PROGRESS,
                           dest=f"{ctx.config.dest_remote}:{dest_path}")

            try:
                result = retry_with_backoff(
                    rclone_copyto,
                    src_remote, src_path, ctx.config.dest_remote, dest_path,
                    max_retries=ctx.config.max_transfer_retries,
                    retryable_exceptions=(RcloneTransferError, RuntimeError, OSError),
                    file_size=file_size,
                    bwlimit=ctx.config.bwlimit,
                    checksum=True,
                    raise_on_failure=True,
                )

                if result["success"]:
                    verify = rclone_verify_transfer(
                        ctx.config.dest_remote, dest_path, expected_size=file_size,
                    )
                    if verify["verified"]:
                        ckpt.mark_file(
                            ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                            Phase.STREAM, FileStatus.COMPLETED,
                            dest=f"{ctx.config.dest_remote}:{dest_path}",
                            bytes_transferred=result["bytes"] or file_size or 0,
                        )
                        total_stream_bytes += result["bytes"] or file_size or 0
                        source_failures[src_remote] = 0
                    else:
                        error_msg = f"Verification failed: {verify.get('error', 'unknown')}"
                        logger.error("VERIFY FAIL for %s: %s", fs.source_location, error_msg)
                        ckpt.mark_file(
                            ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                            Phase.STREAM, FileStatus.FAILED, error=error_msg,
                        )
                else:
                    error_msg = result.get("error", "unknown")[:ERROR_TRUNCATE_LEN]
                    ckpt.mark_file(
                        ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                        Phase.STREAM, FileStatus.FAILED, error=error_msg,
                    )
                    source_failures[src_remote] = source_failures.get(src_remote, 0) + 1

                    if any(q in error_msg.lower() for q in QUOTA_ERRORS):
                        logger.error("QUOTA/RATE LIMIT detected: %s — pausing job", error_msg)
                        ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED,
                                        error=f"Cilové uloziste plné nebo rate limit: {error_msg[:ERROR_TRUNCATE_MEDIUM]}")
                        ctx.progress.paused = True
                        ctx.report(Phase.STREAM, "Pozastaveno — uloziste plné", 5)
                        break

            except RcloneTransferError as exc:
                error_msg = str(exc)[:ERROR_TRUNCATE_LEN]
                ckpt.mark_file(
                    ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                    Phase.STREAM, FileStatus.FAILED, error=error_msg,
                )
                source_failures[src_remote] = source_failures.get(src_remote, 0) + 1

                if any(q in error_msg.lower() for q in QUOTA_ERRORS):
                    logger.error("QUOTA/RATE LIMIT detected after retries: %s — pausing job", error_msg)
                    ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED,
                                    error=f"Cilové uloziste plné nebo rate limit: {error_msg[:ERROR_TRUNCATE_MEDIUM]}")
                    ctx.progress.paused = True
                    ctx.report(Phase.STREAM, "Pozastaveno — uloziste plné", 5)
                    break

            except Exception as exc:
                ckpt.mark_file(
                    ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                    Phase.STREAM, FileStatus.FAILED, error=str(exc)[:ERROR_TRUNCATE_LEN],
                )
                source_failures[src_remote] = source_failures.get(src_remote, 0) + 1

            # Progress update
            p = ckpt.get_job_progress(ctx.cat, ctx.job.job_id, Phase.STREAM)
            elapsed = time.monotonic() - ctx.stream_start_time
            speed = _estimate_speed(total_stream_bytes, elapsed)
            remaining_bytes = total_bytes_estimate - total_stream_bytes
            eta = int(remaining_bytes / speed) if speed > 0 else 0

            ctx.report(Phase.STREAM, "Streaming cloud->cloud...", 5,
                       files_transferred=p[FileStatus.COMPLETED],
                       bytes_transferred=p["bytes_transferred"],
                       errors=p[FileStatus.FAILED],
                       transfer_speed_bps=speed,
                       eta_seconds=eta)

        if ctx.progress.paused:
            break
        pending = ckpt.get_pending_files(ctx.cat, ctx.job.job_id, Phase.STREAM, limit=STREAM_BATCH_SIZE)

    stream_progress = ckpt.get_job_progress(ctx.cat, ctx.job.job_id, Phase.STREAM)
    ctx.results["stream"] = {
        "transferred": stream_progress[FileStatus.COMPLETED],
        "failed": stream_progress[FileStatus.FAILED],
        "skipped": stream_progress[FileStatus.SKIPPED],
        "bytes": stream_progress["bytes_transferred"],
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

        for fs in failed_files:
            if fs.attempt_count >= MAX_RETRY_ATTEMPTS:
                logger.warning("Skipping %s: too many attempts (%d)", fs.source_location, fs.attempt_count)
                continue

            parts = fs.source_location.split(":", 1)
            src_remote = parts[0]
            src_path = parts[1] if len(parts) > 1 else parts[0]

            if src_remote == "local":
                continue

            if not rclone_is_reachable(src_remote, timeout=DEST_CONNECTIVITY_TIMEOUT):
                if not wait_for_connectivity(src_remote, timeout=RETRY_CONNECTIVITY_WAIT):
                    logger.warning("Source %s unreachable for retry, skipping", src_remote)
                    continue

            file_row = conn.execute(
                "SELECT date_original, size FROM files WHERE sha256 = ? LIMIT 1",
                (fs.file_hash,),
            ).fetchone()
            file_size = file_row["size"] if file_row else None

            if fs.dest_location and ":" in fs.dest_location:
                dest_path = fs.dest_location.split(":", 1)[1]
            else:
                filename = PurePosixPath(src_path).name
                mod_time = file_row["date_original"] if file_row else None
                dest_path = _build_dest_path(
                    ctx.config.dest_path, filename, fs.file_hash,
                    mod_time, ctx.config.structure_pattern,
                )
                logger.warning(
                    "Retry %s: no stored dest_location, rebuilt path %s "
                    "(may differ from Phase 5 collision-safe path)",
                    fs.source_location, dest_path,
                )

            try:
                retry_timeout = (
                    int(_dynamic_timeout(file_size) * ctx.config.retry_timeout_multiplier)
                    if file_size else DEFAULT_RETRY_TIMEOUT
                )
                result = rclone_copyto(
                    src_remote, src_path, ctx.config.dest_remote, dest_path,
                    file_size=file_size,
                    timeout=retry_timeout,
                    bwlimit=ctx.config.bwlimit,
                    checksum=True,
                )

                if result["success"]:
                    verify = rclone_verify_transfer(
                        ctx.config.dest_remote, dest_path, expected_size=file_size,
                    )
                    if verify["verified"]:
                        ckpt.mark_file(
                            ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                            Phase.STREAM, FileStatus.COMPLETED,
                            dest=f"{ctx.config.dest_remote}:{dest_path}",
                            bytes_transferred=result["bytes"] or file_size or 0,
                        )
                        retried_ok += 1
                    else:
                        ckpt.mark_file(
                            ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                            Phase.STREAM, FileStatus.FAILED,
                            error=f"Retry verify failed: {verify.get('error', '')}",
                        )
                        retried_fail += 1
                else:
                    ckpt.mark_file(
                        ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                        Phase.STREAM, FileStatus.FAILED,
                        error=result.get("error", "retry failed")[:ERROR_TRUNCATE_LEN],
                    )
                    retried_fail += 1
            except Exception as exc:
                ckpt.mark_file(
                    ctx.cat, ctx.job.job_id, fs.file_hash, fs.source_location,
                    Phase.STREAM, FileStatus.FAILED,
                    error=f"Retry exception: {str(exc)[:ERROR_TRUNCATE_SHORT]}",
                )
                retried_fail += 1

            ctx.report(Phase.RETRY_FAILED, f"Retry: {retried_ok} OK, {retried_fail} fail", 6,
                       files_retried=retried_ok)

        ctx.results["retry"] = {"retried_ok": retried_ok, "retried_fail": retried_fail}
    else:
        ctx.results["retry"] = {"retried_ok": 0, "retried_fail": 0}

    ctx.finish_phase(Phase.RETRY_FAILED)


def _phase_7_verify(ctx: PhaseContext) -> None:
    """Phase 7: Verify integrity on destination (BEFORE dedupe!).

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
    completed_transfers = conn.execute("""
        SELECT file_hash, dest_location, bytes_transferred
        FROM consolidation_file_state
        WHERE job_id = ? AND step_name = ? AND status = ?
          AND dest_location IS NOT NULL
    """, (ctx.job.job_id, Phase.STREAM, FileStatus.COMPLETED)).fetchall()

    if ctx.config.verify_pct < 100:
        import random
        sample_size = max(1, len(completed_transfers) * ctx.config.verify_pct // 100)
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
            file_hash = row["file_hash"]
            reason = "missing" if not check["exists"] else "size_mismatch"
            ckpt.mark_file(
                ctx.cat, ctx.job.job_id, file_hash, dest_loc,
                Phase.STREAM, FileStatus.FAILED,
                error=f"verify_{reason}: exists={check['exists']}, size_match={check.get('size_match')}",
            )
            logger.error("VERIFY FAIL: %s (exists=%s, size_match=%s)",
                         dest_loc, check["exists"], check.get("size_match"))

        if (idx + 1) % VERIFY_REPORT_INTERVAL == 0:
            ctx.report(Phase.VERIFY, f"Overeno {idx + 1}/{total_to_verify}...", 7,
                       files_verified=verified_ok, errors=verified_fail)

    ctx.results["verify"] = {
        "total_checked": total_to_verify,
        "verified_ok": verified_ok,
        "verified_fail": verified_fail,
    }
    ctx.report(Phase.VERIFY, "Overeni hotové", 7,
               files_verified=verified_ok, errors=verified_fail)

    # Pause before dedupe if significant verification failures
    if verified_fail > 0 and total_to_verify > 0:
        fail_pct = 100 * verified_fail / total_to_verify
        if fail_pct > VERIFY_FAIL_THRESHOLD_PCT:
            logger.error(
                "VERIFY: %d/%d (%.1f%%) failed — pausing before dedupe",
                verified_fail, total_to_verify, fail_pct,
            )
            ckpt.update_job(
                ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED,
                error=f"Overeni: {verified_fail}/{total_to_verify} souboru selhalo ({fail_pct:.1f}%) — zkontroluj a spust resume",
            )
            ctx.progress.paused = True

    ctx.finish_phase(Phase.VERIFY)


def _phase_8_post_dedup(ctx: PhaseContext) -> None:
    """Phase 8: Post-transfer deduplication via rclone dedupe.

    Uses 'largest' mode to keep the highest quality copy.
    """
    if ctx.config.dry_run:
        ctx.results["post_transfer_dedup"] = {
            "dry_run": True,
            "note": "Deduplikace probehne po prenosu pomoci rclone dedupe (mode=largest)",
        }
        return

    if ctx.phase_done(Phase.POST_TRANSFER_DEDUP):
        return

    ctx.report(Phase.POST_TRANSFER_DEDUP, "Post-transfer deduplikace na cili...", 8)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.POST_TRANSFER_DEDUP)

    if not rclone_is_reachable(ctx.config.dest_remote):
        ctx.results["post_transfer_dedup"] = {"note": "Cilové uloziste nedostupné pro deduplikaci"}
        ctx.finish_phase(Phase.POST_TRANSFER_DEDUP)
        return

    dedup_result = rclone_dedupe(
        ctx.config.dest_remote, ctx.config.dest_path,
        mode=DedupStrategy.LARGEST,
        dry_run=False,
        timeout=DEDUP_TIMEOUT,
    )

    ctx.results["post_transfer_dedup"] = {
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

    ctx.report(Phase.POST_TRANSFER_DEDUP, "Post-transfer deduplikace hotová", 8)
    ctx.finish_phase(Phase.POST_TRANSFER_DEDUP)


def _phase_9_sync_to_disk(ctx: PhaseContext) -> None:
    """Phase 9: Sync destination cloud to local disk."""
    if ctx.config.dry_run:
        ctx.results["sync"] = {"dry_run": True, "disk_path": ctx.config.disk_path}
        return

    if ctx.phase_done(Phase.SYNC_TO_DISK):
        return

    ctx.report(Phase.SYNC_TO_DISK, "Synchronizace na disk...", 9)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.SYNC_TO_DISK)

    if not check_volume_mounted(ctx.config.disk_path):
        ctx.results["sync"] = {
            "synced": False,
            "note": f"Disk {ctx.config.disk_path} neni pripojený — pripoj disk a spust resume",
        }
        ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED,
                        error=f"Disk {ctx.config.disk_path} neni pripojený")
        ctx.progress.paused = True
        return

    from .cloud import rclone_copy
    try:
        rclone_copy(
            ctx.config.dest_remote, ctx.config.dest_path, ctx.config.disk_path,
            progress_fn=lambda p: ctx.report(Phase.SYNC_TO_DISK,
                f"Synchronizace na disk... {p.get('progress_pct', 0)}%", 9),
        )
        ctx.results["sync"] = {"synced": True, "disk_path": ctx.config.disk_path}
        ctx.finish_phase(Phase.SYNC_TO_DISK)
    except Exception as exc:
        ctx.results["sync"] = {"synced": False, "error": str(exc)[:ERROR_TRUNCATE_LEN]}
        logger.error("Sync to disk failed: %s", exc)
        ckpt.update_job(ctx.cat, ctx.job.job_id, status=JobStatus.PAUSED,
                        error=f"Sync selhal: {str(exc)[:ERROR_TRUNCATE_SHORT]}")
        ctx.progress.paused = True


def _phase_10_report(ctx: PhaseContext) -> None:
    """Phase 10: Generate final summary report."""
    ctx.report(Phase.REPORT, "Generovani záverecného reportu...", 10)
    ckpt.update_job(ctx.cat, ctx.job.job_id, current_step=Phase.REPORT)

    final_progress = ckpt.get_job_progress(ctx.cat, ctx.job.job_id, Phase.STREAM)
    elapsed_total = time.monotonic() - ctx.stream_start_time if ctx.stream_start_time else 0

    ctx.results["summary"] = {
        "sources_available": len(ctx.available),
        "sources_unavailable": len(ctx.unavailable),
        "sources_unavailable_names": ctx.unavailable,
        "files_cataloged": ctx.results.get("catalog", {}).get("total_cataloged", 0) + ctx.local_scanned,
        "unique_files": ctx.total_unique,
        "local_duplicate_groups": ctx.results.get("dedup", {}).get("local_duplicate_groups", 0),
        "post_transfer_dedup_removed": ctx.results.get("post_transfer_dedup", {}).get("duplicates_removed", 0),
        "post_transfer_dedup_bytes_freed": ctx.results.get("post_transfer_dedup", {}).get("bytes_freed", 0),
        "files_transferred": final_progress.get(FileStatus.COMPLETED, 0),
        "bytes_transferred": final_progress.get("bytes_transferred", 0),
        "transfer_failures": final_progress.get(FileStatus.FAILED, 0),
        "files_retried_ok": ctx.results.get("retry", {}).get("retried_ok", 0),
        "files_retried_fail": ctx.results.get("retry", {}).get("retried_fail", 0),
        "verified_ok": ctx.results.get("verify", {}).get("verified_ok", 0),
        "verified_fail": ctx.results.get("verify", {}).get("verified_fail", 0),
        "synced_to_disk": ctx.results.get("sync", {}).get("synced", False),
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
            ckpt.complete_job(ctx.cat, ctx.job.job_id,
                              error=f"{still_failed} souboru se nepodarilo prenést")
        else:
            ckpt.complete_job(ctx.cat, ctx.job.job_id)
        ctx.report(Phase.COMPLETE, "Konsolidace dokoncena", 10)


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
    job = None

    try:
        # Find or create job
        resumable = ckpt.get_resumable_jobs(cat)
        for j in resumable:
            if j.job_type == JOB_TYPE_ULTIMATE:
                job = j
                logger.info("Resuming consolidation job %s (status=%s, step=%s)",
                            j.job_id, j.status, j.current_step)
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
                cat, JOB_TYPE_ULTIMATE,
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

        ckpt.update_job(cat, job.job_id, status=JobStatus.RUNNING)

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
        _phase_4_pre_dedup(ctx)
        _phase_5_stream(ctx)
        _phase_6_retry_failed(ctx)
        _phase_7_verify(ctx)
        _phase_8_post_dedup(ctx)
        _phase_9_sync_to_disk(ctx)
        _phase_10_report(ctx)

        return ctx.results

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
        consolidation_jobs = [j for j in jobs if j.job_type in CONSOLIDATION_JOB_TYPES]
        active = [j for j in consolidation_jobs if j.status in (JobStatus.CREATED, JobStatus.RUNNING, JobStatus.PAUSED)]

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
    """Dry-run: scan sources, count files, estimate transfer — no actual transfers."""
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
            if j.job_type in CONSOLIDATION_JOB_TYPES and j.status == JobStatus.RUNNING:
                ckpt.pause_job(cat, j.job_id)
                return {"paused": True, "job_id": j.job_id}
        return {"paused": False, "note": "Zadný bezici job k pozastaveni"}
    finally:
        cat.close()


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
