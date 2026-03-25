"""Ultimate Consolidation Pipeline — orchestrator.

High-level API for running the full consolidation workflow:
  1. Wait for sources (remotes, volumes)
  2. Catalog all sources (metadata only, no download)
  3. Cross-source deduplication
  4. Stream unique files cloud-to-cloud → destination remote
  5. Verify integrity on destination
  6. Sync cleaned result to local disk

All operations are checkpoint-resumable. If interrupted (internet drop, sleep,
disk disconnect), the pipeline resumes from the last completed file.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from . import checkpoint as ckpt
from .catalog import Catalog
from .cloud import (
    _rclone_bin,
    check_rclone,
    check_volume_mounted,
    list_remotes,
    rclone_check_file,
    rclone_copyto,
    rclone_is_reachable,
    rclone_ls,
    retry_with_backoff,
    wait_for_connectivity,
)

logger = logging.getLogger(__name__)


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
    # Structure pattern for reorganized files
    structure_pattern: str = "year_month"
    # Deduplication strategy
    dedup_strategy: str = "richness"
    # Verification sample percentage
    verify_sample_pct: int = 10
    # Connectivity retry settings
    connectivity_timeout: int = 300  # seconds to wait for network
    max_transfer_retries: int = 3
    # Workers for local scan
    scan_workers: int = 4


@dataclass
class ConsolidationProgress:
    """Progress snapshot for UI updates."""
    phase: str = "idle"
    phase_label: str = ""
    current_step: int = 0
    total_steps: int = 8
    files_cataloged: int = 0
    files_deduplicated: int = 0
    files_transferred: int = 0
    files_verified: int = 0
    files_synced: int = 0
    bytes_transferred: int = 0
    errors: int = 0
    paused: bool = False
    error: str | None = None


def run_consolidation(
    catalog_path: str | Path,
    config: ConsolidationConfig | None = None,
    progress_fn: Callable[[ConsolidationProgress], None] | None = None,
    scenario_id: str | None = None,
) -> dict[str, Any]:
    """Execute the full Ultimate Consolidation pipeline.

    Returns summary dict with per-phase results and the job_id for resume.
    """
    config = config or ConsolidationConfig()
    catalog_path = str(catalog_path)

    cat = Catalog(catalog_path)
    cat.open()

    progress = ConsolidationProgress(total_steps=8)

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
        # Find or create job
        resumable = ckpt.get_resumable_jobs(cat)
        job = None
        for j in resumable:
            if j.job_type == "ultimate_consolidation":
                job = j
                logger.info("Resuming consolidation job %s (status=%s, step=%s)",
                            j.job_id, j.status, j.current_step)
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
                },
                scenario_id=scenario_id,
            )

        ckpt.update_job(cat, job.job_id, status="running")
        results: dict[str, Any] = {"job_id": job.job_id}

        # ── Phase 1: Wait for sources ──────────────────────────────
        _report("wait_for_sources", "Čekání na zdroje", 1)
        ckpt.update_job(cat, job.job_id, current_step="wait_for_sources")

        source_remotes = config.source_remotes or [r.name for r in list_remotes()]
        available = []
        unavailable = []
        for rname in source_remotes:
            if rclone_is_reachable(rname):
                available.append(rname)
            else:
                # Wait briefly
                if wait_for_connectivity(rname, timeout=60):
                    available.append(rname)
                else:
                    unavailable.append(rname)
        results["sources"] = {"available": available, "unavailable": unavailable}

        # ── Phase 2: Cloud catalog scan ────────────────────────────
        _report("cloud_catalog_scan", "Katalogizace vzdálených zdrojů", 2)
        ckpt.update_job(cat, job.job_id, current_step="cloud_catalog_scan")

        total_cataloged = 0
        for rname in available:
            try:
                files = rclone_ls(rname, "", recursive=True)
                for f in files:
                    if f.get("IsDir"):
                        continue
                    total_cataloged += 1
                    # Register as pending for later transfer
                    file_hash = f.get("Hashes", {}).get("sha256") or f.get("Name", "")
                    ckpt.mark_file(
                        cat, job.job_id, file_hash,
                        f"{rname}:{f.get('Path', f.get('Name', ''))}",
                        "catalog", "completed",
                    )
            except Exception as exc:
                logger.warning("cloud_catalog_scan: %s error: %s", rname, exc)
        results["catalog"] = {"total_cataloged": total_cataloged}
        _report("cloud_catalog_scan", "Katalogizace vzdálených zdrojů", 2,
                files_cataloged=total_cataloged)

        # ── Phase 3: Local scan ────────────────────────────────────
        _report("local_scan", "Skenování lokálních souborů", 3)
        ckpt.update_job(cat, job.job_id, current_step="local_scan")
        # Delegate to scanner if roots available
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

        # ── Phase 4: Deduplication ─────────────────────────────────
        _report("dedup", "Cross-source deduplikace", 4)
        ckpt.update_job(cat, job.job_id, current_step="dedup")

        groups = cat.query_duplicates(limit=50000)
        dedup_groups = len(groups.get("groups", []))
        results["dedup"] = {"groups": dedup_groups}
        _report("dedup", "Cross-source deduplikace", 4, files_deduplicated=dedup_groups)

        # ── Phase 5: Stream to cloud ───────────────────────────────
        _report("stream", "Streaming cloud\u2192cloud", 5)
        ckpt.update_job(cat, job.job_id, current_step="stream")
        ckpt.reset_stale_in_progress(cat, job.job_id, "stream")

        # Get unique files (one per sha256 group)
        import sqlite3
        conn = cat.conn
        conn.row_factory = sqlite3.Row
        cur = conn.execute("""
            SELECT sha256, path, source_remote, size
            FROM files
            WHERE sha256 IS NOT NULL
            GROUP BY sha256
            ORDER BY date_original DESC NULLS LAST
        """)
        unique_files = cur.fetchall()

        # Register pending transfers
        for row in unique_files:
            source = row["source_remote"] or "local"
            ckpt.mark_file(
                cat, job.job_id, row["sha256"],
                f"{source}:{row['path']}", "stream", "pending",
            )

        # Execute streaming transfers
        transferred = 0
        stream_failed = 0
        pending = ckpt.get_pending_files(cat, job.job_id, "stream", limit=500)
        while pending:
            # Connectivity check
            if not rclone_is_reachable(config.dest_remote, timeout=10):
                logger.warning("Destination %s unreachable, waiting...", config.dest_remote)
                if not wait_for_connectivity(config.dest_remote, timeout=config.connectivity_timeout):
                    ckpt.update_job(cat, job.job_id, status="paused",
                                    error="Cílové úložiště nedostupné")
                    progress.paused = True
                    _report("stream", "Pozastaveno — čekání na připojení", 5)
                    break

            for fs in pending:
                parts = fs.source_location.split(":", 1)
                src_remote = parts[0]
                src_path = parts[1] if len(parts) > 1 else parts[0]

                if src_remote == "local":
                    ckpt.mark_file(cat, job.job_id, fs.file_hash,
                                   fs.source_location, "stream", "skipped")
                    continue

                # Build dest path with year/month structure
                from pathlib import PurePosixPath
                fname = PurePosixPath(src_path).name
                dest_file = f"{config.dest_path}/{fname}"

                ckpt.mark_file(cat, job.job_id, fs.file_hash,
                               fs.source_location, "stream", "in_progress")
                try:
                    result = retry_with_backoff(
                        rclone_copyto,
                        src_remote, src_path, config.dest_remote, dest_file,
                        max_retries=config.max_transfer_retries,
                    )
                    if result["success"]:
                        ckpt.mark_file(
                            cat, job.job_id, fs.file_hash, fs.source_location,
                            "stream", "completed",
                            dest=f"{config.dest_remote}:{dest_file}",
                            bytes_transferred=result["bytes"],
                        )
                        transferred += 1
                    else:
                        ckpt.mark_file(
                            cat, job.job_id, fs.file_hash, fs.source_location,
                            "stream", "failed", error=result.get("error", "unknown"),
                        )
                        stream_failed += 1
                except Exception as exc:
                    ckpt.mark_file(
                        cat, job.job_id, fs.file_hash, fs.source_location,
                        "stream", "failed", error=str(exc)[:200],
                    )
                    stream_failed += 1

                if progress_fn:
                    p = ckpt.get_job_progress(cat, job.job_id, "stream")
                    _report("stream", "Streaming cloud\u2192cloud", 5,
                            files_transferred=p["completed"],
                            bytes_transferred=p["bytes_transferred"],
                            errors=p["failed"])

            pending = ckpt.get_pending_files(cat, job.job_id, "stream", limit=500)

        stream_progress = ckpt.get_job_progress(cat, job.job_id, "stream")
        results["stream"] = {
            "transferred": stream_progress["completed"],
            "failed": stream_progress["failed"],
            "skipped": stream_progress["skipped"],
            "bytes": stream_progress["bytes_transferred"],
        }

        # ── Phase 6: Verify integrity ──────────────────────────────
        _report("verify", "Ověření integrity na cíli", 6)
        ckpt.update_job(cat, job.job_id, current_step="verify")

        if rclone_is_reachable(config.dest_remote):
            sample_count = max(1, len(unique_files) * config.verify_sample_pct // 100)
            cur = conn.execute(
                "SELECT sha256, size, path FROM files WHERE sha256 IS NOT NULL ORDER BY RANDOM() LIMIT ?",
                (sample_count,),
            )
            sample_rows = cur.fetchall()
            verified = 0
            verify_missing = 0
            for row in sample_rows:
                check = rclone_check_file(config.dest_remote, f"{config.dest_path}/{Path(row['path']).name}",
                                          expected_size=row["size"])
                if check["exists"]:
                    verified += 1
                else:
                    verify_missing += 1
            results["verify"] = {"verified": verified, "missing": verify_missing, "sample": len(sample_rows)}
            _report("verify", "Ověření integrity na cíli", 6, files_verified=verified)
        else:
            results["verify"] = {"note": "Cílové úložiště nedostupné pro ověření"}

        # ── Phase 7: Sync to disk ──────────────────────────────────
        _report("sync_to_disk", "Synchronizace na disk", 7)
        ckpt.update_job(cat, job.job_id, current_step="sync_to_disk")

        if check_volume_mounted(config.disk_path):
            from .cloud import rclone_copy
            try:
                rclone_copy(config.dest_remote, config.dest_path, config.disk_path,
                            progress_fn=progress_fn)
                results["sync"] = {"synced": True, "disk_path": config.disk_path}
            except Exception as exc:
                results["sync"] = {"synced": False, "error": str(exc)[:200]}
        else:
            results["sync"] = {"synced": False, "note": f"Disk {config.disk_path} není připojený"}

        # ── Phase 8: Report ────────────────────────────────────────
        _report("report", "Generování reportu", 8)
        ckpt.update_job(cat, job.job_id, current_step="report")

        results["summary"] = {
            "sources_available": len(available),
            "sources_unavailable": len(unavailable),
            "files_cataloged": total_cataloged + local_scanned,
            "dedup_groups": dedup_groups,
            "files_transferred": stream_progress["completed"],
            "bytes_transferred": stream_progress["bytes_transferred"],
            "transfer_failures": stream_progress["failed"],
            "verified": results.get("verify", {}).get("verified", 0),
            "synced_to_disk": results.get("sync", {}).get("synced", False),
        }

        # Complete job
        if not progress.paused:
            ckpt.complete_job(cat, job.job_id)
            _report("complete", "Konsolidace dokončena", 8)

        return results

    except Exception as exc:
        logger.exception("Consolidation failed")
        if job:
            ckpt.complete_job(cat, job.job_id, error=str(exc)[:500])
        raise
    finally:
        cat.close()


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
            })

        return result
    finally:
        cat.close()


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
    progress_fn: Callable | None = None,
) -> dict[str, Any]:
    """Resume a paused/interrupted consolidation."""
    return run_consolidation(catalog_path, progress_fn=progress_fn)
