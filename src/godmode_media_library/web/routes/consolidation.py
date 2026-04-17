"""Consolidation endpoints."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

from ..shared import (
    _create_task,
    _finish_task,
    _open_catalog,
    _sanitize_path,
    _update_progress,
    logger,
)

router = APIRouter()


# ── Request models ────────────────────────────────────────────────────

class ConsolidationStartRequest(BaseModel):
    source_remotes: list[str] = []
    local_roots: list[str] = []
    dest_remote: str = Field(default="gws-backup", min_length=1, max_length=100)
    dest_path: str = Field(default="GML-Consolidated", min_length=1, max_length=500)
    disk_path: str = "/Volumes/4TB/GML-Library"
    structure_pattern: str = "year_month"
    dedup_strategy: str = "richness"
    verify_pct: int = Field(default=100, ge=0, le=100)
    bwlimit: str | None = None
    dry_run: bool = False
    media_only: bool = False

    @field_validator("dest_remote", "dest_path")
    @classmethod
    def no_dangerous_chars(cls, v: str) -> str:
        if "\x00" in v or "\n" in v or "\r" in v:
            raise ValueError("Contains invalid characters (null/newline)")
        if ".." in v:
            raise ValueError("Path traversal (..) not allowed")
        return v

    @field_validator("bwlimit")
    @classmethod
    def validate_bwlimit(cls, v: str | None) -> str | None:
        if v is not None and not re.match(r"^\d+[KMGkmg]?$", v):
            raise ValueError("bwlimit must be like '10M', '1G', '512K' or a number")
        return v


class SyncToDiskRequest(BaseModel):
    dest_remote: str = "gws-backup"
    dest_path: str = "GML-Consolidated"
    disk_path: str = Field(min_length=1, max_length=500)

    @field_validator("dest_remote", "dest_path", "disk_path")
    @classmethod
    def no_dangerous_chars(cls, v: str) -> str:
        if "\x00" in v or "\n" in v or "\r" in v:
            raise ValueError("Contains invalid characters (null/newline)")
        if ".." in v:
            raise ValueError("Path traversal (..) not allowed")
        return v


class SyncDiskRequest(BaseModel):
    dest_remote: str = "gws-backup"
    dest_path: str = "GML-Consolidated"
    disk_path: str = Field(min_length=1, max_length=500)
    delete_extra: bool = True

    @field_validator("dest_remote", "dest_path", "disk_path")
    @classmethod
    def no_dangerous_chars(cls, v: str) -> str:
        if "\x00" in v or "\n" in v or "\r" in v:
            raise ValueError("Contains invalid characters (null/newline)")
        if ".." in v:
            raise ValueError("Path traversal (..) not allowed")
        return v


# ── Local helpers ─────────────────────────────────────────────────────

def _consolidation_progress_dict(p) -> dict:
    """Convert ConsolidationProgress to dict for task updates."""
    return {
        "phase": p.phase,
        "phase_label": p.phase_label,
        "current_step": max(p.current_step, 1),
        "total_steps": p.total_steps,
        "files_cataloged": p.files_cataloged,
        "files_unique": p.files_unique,
        "files_transferred": p.files_transferred,
        "files_verified": p.files_verified,
        "files_failed": p.files_failed,
        "files_retried": p.files_retried,
        "bytes_transferred": p.bytes_transferred,
        "bytes_total_estimate": p.bytes_total_estimate,
        "transfer_speed_bps": p.transfer_speed_bps,
        "eta_seconds": p.eta_seconds,
        "errors": p.errors,
        "paused": p.paused,
        "dry_run": p.dry_run,
        "current_file": getattr(p, "current_file", ""),
    }


# System volumes to exclude from available-disks listing
_EXCLUDED_VOLUMES = {"Macintosh HD", "Macintosh HD - Data", "Recovery", "Preboot", "VM", "Update", "com.apple.TimeMachine.localsnapshots"}


def _validate_disk_path(disk_path: str) -> Path:
    """Validate that disk_path is a real, mounted directory. Returns resolved Path."""
    _sanitize_path(disk_path, param_name="disk_path")
    p = Path(disk_path).resolve()
    if not p.exists():
        raise HTTPException(status_code=400, detail=f"Cesta k disku neexistuje: {disk_path}")
    if not p.is_dir():
        raise HTTPException(status_code=400, detail=f"Cesta k disku není složka: {disk_path}")
    return p


def _run_rclone_sync(task_id: str, source: str, dest: str, *, sync_mode: bool = False):
    """Run rclone copy or sync in a subprocess, updating task progress."""
    cmd = ["rclone", "sync" if sync_mode else "copy", source, dest, "--progress", "--stats-one-line", "-v"]
    logger.info("Running rclone: %s", " ".join(cmd))
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        lines_buf = []
        for line in iter(proc.stdout.readline, ""):
            line = line.strip()
            if not line:
                continue
            lines_buf.append(line)
            # Keep last 20 log lines for progress reporting
            if len(lines_buf) > 20:
                lines_buf = lines_buf[-20:]
            _update_progress(task_id, {"rclone_log": lines_buf, "status": "transferring"})
        proc.wait(timeout=7200)  # 2 hour max for rclone sync
        if proc.returncode != 0:
            _finish_task(task_id, error=f"rclone exited with code {proc.returncode}. Last output: {' | '.join(lines_buf[-5:])}")
        else:
            _finish_task(task_id, result={"status": "completed", "last_output": lines_buf[-5:] if lines_buf else []})
    except FileNotFoundError:
        _finish_task(task_id, error="rclone is not installed or not on PATH")
    except Exception as e:
        logger.exception("rclone sync failed")
        _finish_task(task_id, error=str(e))


# ── Endpoints ─────────────────────────────────────────────────────────

@router.get("/consolidation/status")
def consolidation_status(request: Request):
    """Get current consolidation status."""
    from ...consolidation import get_consolidation_status

    catalog_path = str(request.app.state.catalog_path)
    status = get_consolidation_status(catalog_path)

    # Enrich with disk space info if a disk_path is configured
    disk_path = status.get("disk_path")
    if disk_path:
        try:
            usage = shutil.disk_usage(disk_path)
            status["disk_free_gb"] = round(usage.free / (1024**3), 2)
        except OSError:
            pass

    # Enrich active job with live task progress (files_cataloged, phase_label, etc.)
    if status.get("has_active_job") and status.get("jobs"):
        active = status["jobs"][0]
        try:
            from ..shared import _tasks as tasks_dict
            from ..shared import _tasks_lock
            with _tasks_lock:
                snapshot = list(tasks_dict.values())
            for t in snapshot:
                if getattr(t, "command", "").startswith("consolidation:") and getattr(t, "status", None) == "running":
                    prog = getattr(t, "progress", None)
                    if prog:
                        active["task_progress"] = prog
                    active["task_id"] = t.id
                    break
        except Exception:
            pass  # best-effort enrichment

    return status


@router.get("/consolidation/health")
def consolidation_health(request: Request):
    """Lightweight health check — no DB queries, polled every 10s by FE."""
    import time

    health: dict = {"ok": True, "timestamp": time.time()}

    # Disk connectivity for configured local roots
    try:
        catalog_path = str(request.app.state.catalog_path)
        # Avoid full status — just check if we have an active job config
        from ...consolidation import Catalog, ckpt
        cat = Catalog(catalog_path)
        cat.open()
        try:
            jobs = ckpt.list_jobs(cat)
            active = [j for j in jobs if j.status in ("created", "running", "paused")]
            if active:
                cfg = active[0].config or {}
                local_roots = cfg.get("local_roots", [])
                disks = {}
                for lr in local_roots:
                    p = Path(lr)
                    disks[lr] = {
                        "connected": p.exists(),
                        "free_gb": round(shutil.disk_usage(lr).free / (1024**3), 1) if p.exists() else None,
                    }
                health["disks"] = disks
                health["disk_connected"] = all(d["connected"] for d in disks.values()) if disks else None
        finally:
            cat.close()
    except Exception:
        pass

    # Check rclone process
    try:
        import subprocess as sp
        result = sp.run(["pgrep", "-f", "rclone"], capture_output=True, text=True, timeout=3)
        health["rclone_running"] = result.returncode == 0
        health["rclone_pids"] = [int(p) for p in result.stdout.strip().split("\n") if p.strip()] if result.returncode == 0 else []
    except Exception:
        health["rclone_running"] = None

    return health


@router.post("/consolidation/preview")
def consolidation_preview(body: ConsolidationStartRequest, request: Request, background_tasks: BackgroundTasks):
    """Dry-run: scan all sources, count files, estimate transfer — NO actual transfers.

    ALWAYS run this before starting a real consolidation.
    """
    from ...consolidation import ConsolidationConfig, preview_consolidation

    task = _create_task("consolidation:preview")
    catalog_path = str(request.app.state.catalog_path)
    cfg = ConsolidationConfig(
        source_remotes=body.source_remotes,
        local_roots=body.local_roots,
        dest_remote=body.dest_remote,
        dest_path=body.dest_path,
        disk_path=body.disk_path,
        structure_pattern=body.structure_pattern,
        dedup_strategy=body.dedup_strategy,
        verify_pct=0,
        media_only=body.media_only,
    )

    def run():
        try:
            result = preview_consolidation(catalog_path=catalog_path, config=cfg)
            _finish_task(task.id, result=result)
        except Exception as e:
            logger.exception("Consolidation preview failed")
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "previewing"}


@router.post("/consolidation/start")
def consolidation_start(body: ConsolidationStartRequest, request: Request, background_tasks: BackgroundTasks):
    """Start the Ultimate Consolidation pipeline.

    Recommended: run /consolidation/preview first to see what will happen.
    """
    client_ip = request.client.host if request.client else "unknown"
    logger.warning("AUDIT: %s started consolidation (dry_run=%s)", client_ip, body.dry_run)
    from ...consolidation import ConsolidationConfig, get_consolidation_status, run_consolidation

    # Guard: prevent starting a second consolidation while one is running
    catalog_path_str = str(request.app.state.catalog_path)
    try:
        status = get_consolidation_status(catalog_path_str)
        if status.get("has_active_job"):

            raise HTTPException(
                status_code=409,
                detail="Konsolidace již běží. Nejdřív ji pozastavte nebo počkejte na dokončení.",
            )
    except Exception as exc:
        if hasattr(exc, "status_code"):  # Re-raise HTTPException
            raise
        logger.warning("Could not check active consolidation: %s", exc)

    # Pre-flight: check disk space on local destination
    dest_disk = body.disk_path or body.dest_path
    if dest_disk:
        try:
            usage = shutil.disk_usage(dest_disk)
            free_gb = usage.free / (1024**3)
            if free_gb < 1.0:
                raise HTTPException(
                    status_code=400,
                    detail=f"Nedostatek místa na disku: pouze {free_gb:.1f} GB volných na {dest_disk}",
                )
        except OSError:
            pass  # Path might not exist yet, skip check

    task = _create_task("consolidation:ultimate")
    catalog_path = str(request.app.state.catalog_path)
    cfg = ConsolidationConfig(
        source_remotes=body.source_remotes,
        local_roots=body.local_roots,
        dest_remote=body.dest_remote,
        dest_path=body.dest_path,
        disk_path=body.disk_path,
        structure_pattern=body.structure_pattern,
        dedup_strategy=body.dedup_strategy,
        verify_pct=body.verify_pct,
        bwlimit=body.bwlimit,
        dry_run=body.dry_run,
        media_only=body.media_only,
    )

    def run():
        try:
            result = run_consolidation(
                catalog_path=catalog_path,
                config=cfg,
                progress_fn=lambda p: _update_progress(task.id, _consolidation_progress_dict(p)),
            )
            _finish_task(task.id, result=result)
        except Exception as e:
            logger.exception("Consolidation failed")
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started", "dry_run": body.dry_run}


@router.post("/consolidation/pause")
def consolidation_pause(request: Request):
    """Pause an active consolidation job.

    Uses in-process Event signaling to avoid SQLite write lock contention.
    """
    client_ip = request.client.host if request.client else "unknown"
    logger.warning("AUDIT: %s paused consolidation", client_ip)
    from ...consolidation import pause_consolidation

    catalog_path = str(request.app.state.catalog_path)
    return pause_consolidation(catalog_path)


@router.post("/consolidation/resume")
def consolidation_resume(request: Request, background_tasks: BackgroundTasks):
    """Resume a paused consolidation job. Completed phases are skipped automatically."""
    from ...consolidation import resume_consolidation

    task = _create_task("consolidation:resume")
    catalog_path = str(request.app.state.catalog_path)

    def run():
        try:
            result = resume_consolidation(
                catalog_path=catalog_path,
                progress_fn=lambda p: _update_progress(task.id, _consolidation_progress_dict(p)),
            )
            _finish_task(task.id, result=result)
        except Exception as e:
            logger.exception("Consolidation resume failed")
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "resuming"}


@router.get("/consolidation/failed")
def consolidation_failed(request: Request):
    """Get detailed report of all failed transfers for manual review."""
    from ...consolidation import get_failed_files_report

    catalog_path = str(request.app.state.catalog_path)
    return {"failed_files": get_failed_files_report(catalog_path)}


# ── Consolidation: Disk Sync & Catalog Stats ─────────────────────────

@router.post("/consolidation/sync-to-disk")
def consolidation_sync_to_disk(body: SyncToDiskRequest, request: Request, background_tasks: BackgroundTasks):
    """Download consolidated data from cloud to local disk (rclone copy, non-destructive)."""
    disk = _validate_disk_path(body.disk_path)

    # Check free space
    try:
        usage = shutil.disk_usage(str(disk))
        free_gb = usage.free / (1024**3)
        if free_gb < 1.0:
            raise HTTPException(status_code=400, detail=f"Nedostatek místa na disku: pouze {free_gb:.1f} GB volných na {body.disk_path}")
    except OSError:
        pass

    source = f"{body.dest_remote}:{body.dest_path}"
    task = _create_task("consolidation:sync-to-disk")

    def run():
        _run_rclone_sync(task.id, source, str(disk), sync_mode=False)

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "syncing", "source": source, "dest": str(disk)}


@router.get("/consolidation/catalog-stats")
def consolidation_catalog_stats(request: Request):
    """Get catalog statistics for the consolidation report dashboard."""
    cat = _open_catalog(request)
    try:
        conn = cat._conn

        # Total files and size by category (derived from extension)
        # Group extensions into categories: Media (image/video/audio), Documents, Software, Other
        category_stats = []
        rows = conn.execute(
            "SELECT CASE "
            "  WHEN LOWER(ext) IN ("
            "'.jpg','.jpeg','.png','.gif','.bmp','.tiff','.tif',"
            "'.heic','.heif','.webp','.svg','.raw','.cr2','.nef','.arw','.dng',"
            "'.mp4','.mov','.avi','.mkv','.wmv','.flv','.m4v','.3gp','.mpg','.mpeg','.webm',"
            "'.mp3','.wav','.aac','.flac','.ogg','.wma','.m4a','.aiff','.alac'"
            ") THEN 'Media' "
            "  WHEN LOWER(ext) IN ("
            "'.pdf','.doc','.docx','.xls','.xlsx','.ppt','.pptx',"
            "'.txt','.rtf','.csv','.pages','.numbers','.keynote','.odt','.ods'"
            ") THEN 'Documents' "
            "  WHEN LOWER(ext) IN ("
            "'.app','.dmg','.pkg','.exe','.msi','.deb','.rpm',"
            "'.zip','.tar','.gz','.rar','.7z','.iso'"
            ") THEN 'Software' "
            "  ELSE 'Other' "
            "END AS category, COUNT(*) AS count, "
            "COALESCE(SUM(size), 0) AS total_size FROM files GROUP BY category ORDER BY total_size DESC"
        ).fetchall()
        for row in rows:
            category_stats.append({"category": row[0], "count": row[1], "total_size": row[2]})

        # Duplicate groups count
        dup_row = conn.execute(
            "SELECT COUNT(*) FROM (SELECT sha256 FROM files WHERE sha256 IS NOT NULL AND sha256 != '' GROUP BY sha256 HAVING COUNT(*) > 1)"
        ).fetchone()
        duplicate_groups = dup_row[0] if dup_row else 0

        # Files by extension (top 20)
        ext_stats = []
        ext_rows = conn.execute(
            "SELECT LOWER(REPLACE(COALESCE(ext, ''), '.', '')) AS extension, "
            "COUNT(*) AS count, COALESCE(SUM(size), 0) AS total_size "
            "FROM files GROUP BY extension ORDER BY count DESC LIMIT 20"
        ).fetchall()
        for row in ext_rows:
            ext_stats.append({"extension": row[0] or "(none)", "count": row[1], "total_size": row[2]})

        # Files by year (use date_original if available, otherwise derive year from mtime)
        year_stats = []
        year_rows = conn.execute(
            "SELECT COALESCE(SUBSTR(date_original, 1, 4), "
            "  CAST(STRFTIME('%Y', mtime, 'unixepoch') AS TEXT), 'Unknown') AS year, "
            "COUNT(*) AS count, COALESCE(SUM(size), 0) AS total_size "
            "FROM files GROUP BY year ORDER BY year"
        ).fetchall()
        for row in year_rows:
            year_stats.append({"year": row[0], "count": row[1], "total_size": row[2]})

        # Grand totals
        totals_row = conn.execute("SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files").fetchone()

        return {
            "total_files": totals_row[0] if totals_row else 0,
            "total_size": totals_row[1] if totals_row else 0,
            "categories": category_stats,
            "duplicate_groups": duplicate_groups,
            "by_extension": ext_stats,
            "by_year": year_stats,
        }
    finally:
        cat.close()


@router.post("/consolidation/sync-disk")
def consolidation_sync_disk(body: SyncDiskRequest, request: Request, background_tasks: BackgroundTasks):
    """Synchronize disk with cloud destination (one-way: cloud -> disk).

    Uses rclone sync to make the disk identical to the cloud copy.
    When delete_extra=True (default), files on disk that don't exist in cloud are removed.
    """
    disk = _validate_disk_path(body.disk_path)

    # Check free space
    try:
        usage = shutil.disk_usage(str(disk))
        free_gb = usage.free / (1024**3)
        if free_gb < 1.0:
            raise HTTPException(status_code=400, detail=f"Nedostatek místa na disku: pouze {free_gb:.1f} GB volných na {body.disk_path}")
    except OSError:
        pass

    source = f"{body.dest_remote}:{body.dest_path}"
    task = _create_task("consolidation:sync-disk")

    def run():
        # rclone sync is inherently destructive (makes dest match source).
        # When delete_extra is False, fall back to rclone copy (non-destructive).
        _run_rclone_sync(task.id, source, str(disk), sync_mode=body.delete_extra)

    background_tasks.add_task(run)
    return {
        "task_id": task.id,
        "status": "syncing",
        "source": source,
        "dest": str(disk),
        "delete_extra": body.delete_extra,
    }


@router.post("/consolidation/enrich-hashes")
async def consolidation_enrich_hashes(request: Request, bg: BackgroundTasks):
    """Enrich catalog with MD5+SHA-256 hashes from GDrive (no download)."""
    from ...cloud import enrich_catalog_hashes

    cat = _open_catalog(request)
    task = _create_task("enrich_hashes")

    def _run():
        try:
            result = enrich_catalog_hashes(cat.db_path, progress_fn=lambda p: None)
            task.status = "completed"
            task.result = result
        except Exception as exc:
            task.status = "failed"
            task.error = str(exc)

    bg.add_task(_run)
    return {"task_id": task.id, "status": "started"}


@router.get("/consolidation/available-disks")
def consolidation_available_disks():
    """List mounted external volumes for disk sync target selection."""
    volumes_dir = Path("/Volumes")
    disks = []

    if not volumes_dir.is_dir():
        return {"disks": disks}

    for entry in sorted(volumes_dir.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name in _EXCLUDED_VOLUMES:
            continue
        # Skip hidden volumes
        if entry.name.startswith("."):
            continue
        try:
            usage = shutil.disk_usage(str(entry))
            disks.append({
                "name": entry.name,
                "path": str(entry),
                "total_size": usage.total,
                "free_space": usage.free,
                "used_space": usage.used,
            })
        except OSError:
            # Volume not accessible, skip
            continue

    return {"disks": disks}
