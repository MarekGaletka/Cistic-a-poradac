from __future__ import annotations

import os
import sqlite3

from fastapi import APIRouter, BackgroundTasks, Request
from pydantic import BaseModel

from ..shared import (
    _create_task,
    _finish_task,
    _open_catalog,
    _return_catalog,
    _update_progress,
    logger,
)

router = APIRouter()


# ── Pydantic models ──────────────────────────────────────────────────


class BackupTargetUpdate(BaseModel):
    enabled: bool | None = None
    priority: int | None = None
    total_bytes: int | None = None
    free_bytes: int | None = None


class BackupExecuteRequest(BaseModel):
    dry_run: bool = False


# ── Backup endpoints ─────────────────────────────────────────────────


@router.get("/backup/stats")
def backup_stats(request: Request):
    """Get overall backup health statistics."""
    from ...distributed_backup import ensure_backup_tables, get_backup_stats

    cat = _open_catalog(request)
    try:
        ensure_backup_tables(cat)
        stats = get_backup_stats(cat)
        return {
            "total_files": stats.total_files_in_catalog,
            "backed_up": stats.backed_up_files,
            "not_backed_up": stats.not_backed_up,
            "coverage_pct": stats.backup_coverage_pct,
            "total_size": stats.total_backup_size,
            "remotes_used": stats.remotes_used,
            "remotes_healthy": stats.remotes_healthy,
            "last_backup_at": stats.last_backup_at,
            "files_by_remote": stats.files_by_remote,
        }
    finally:
        _return_catalog(cat)


@router.get("/backup/targets")
def backup_targets(request: Request):
    """List all backup targets with capacity info."""
    from ...distributed_backup import ensure_backup_tables, get_targets

    cat = _open_catalog(request)
    try:
        ensure_backup_tables(cat)
        targets = get_targets(cat)
        return {
            "targets": [
                {
                    "remote_name": t.remote_name,
                    "remote_path": t.remote_path,
                    "enabled": t.enabled,
                    "priority": t.priority,
                    "total_bytes": t.total_bytes,
                    "used_bytes": t.used_bytes,
                    "free_bytes": t.free_bytes,
                    "available_bytes": t.available_bytes,
                    "encrypted": t.encrypted,
                    "crypt_remote": t.crypt_remote,
                }
                for t in targets
            ]
        }
    finally:
        _return_catalog(cat)


@router.post("/backup/probe")
def backup_probe(request: Request):
    """Probe all remotes for storage capacity."""
    from ...distributed_backup import probe_targets

    cat = _open_catalog(request)
    try:
        targets = probe_targets(cat)
        return {
            "probed": len(targets),
            "targets": [
                {
                    "remote_name": t.remote_name,
                    "total_bytes": t.total_bytes,
                    "used_bytes": t.used_bytes,
                    "free_bytes": t.free_bytes,
                    "available_bytes": t.available_bytes,
                }
                for t in targets
            ],
        }
    finally:
        _return_catalog(cat)


@router.put("/backup/targets/{remote_name}")
def update_backup_target(remote_name: str, body: BackupTargetUpdate, request: Request):
    """Enable/disable a target or change its priority."""
    from ...distributed_backup import ensure_backup_tables, set_target_enabled, set_target_priority

    cat = _open_catalog(request)
    try:
        ensure_backup_tables(cat)
        if body.enabled is not None:
            set_target_enabled(cat, remote_name, body.enabled)
        if body.priority is not None:
            set_target_priority(cat, remote_name, body.priority)
        if body.total_bytes is not None or body.free_bytes is not None:
            updates = []
            params: list = []
            if body.total_bytes is not None:
                updates.append("total_bytes = ?")
                params.append(body.total_bytes)
            if body.free_bytes is not None:
                updates.append("free_bytes = ?")
                params.append(body.free_bytes)
            if updates:
                params.append(remote_name)
                cat.conn.execute(
                    f"UPDATE backup_targets SET {', '.join(updates)} WHERE remote_name = ?",  # noqa: S608
                    params,
                )
                cat.conn.commit()
        return {"status": "ok"}
    finally:
        _return_catalog(cat)


@router.post("/backup/plan")
def backup_plan(request: Request):
    """Create a distribution plan for backing up files."""
    from ...distributed_backup import create_backup_plan

    cat = _open_catalog(request)
    try:
        plan = create_backup_plan(cat)
        # Summarize by remote
        by_remote: dict[str, dict] = {}
        for e in plan.entries:
            r = e["target_remote"]
            if r not in by_remote:
                by_remote[r] = {"files": 0, "bytes": 0}
            by_remote[r]["files"] += 1
            by_remote[r]["bytes"] += e["size"]
        return {
            "total_files": plan.total_files,
            "total_bytes": plan.total_bytes,
            "targets_used": plan.targets_used,
            "overflow_files": plan.overflow_files,
            "overflow_bytes": plan.overflow_bytes,
            "by_remote": by_remote,
            "entries": plan.entries[:200],  # First 200 for preview
        }
    finally:
        _return_catalog(cat)


@router.post("/backup/execute")
def backup_execute(request: Request, background_tasks: BackgroundTasks, body: BackupExecuteRequest):
    """Execute the backup plan (background task)."""
    from ...distributed_backup import create_backup_plan, execute_backup_plan

    task = _create_task("backup:distribute")
    catalog_path = str(request.app.state.catalog_path)

    def run():
        from ...catalog import Catalog  # Lazy import to avoid circular dependency

        cat = Catalog(catalog_path)
        cat.open()
        try:
            plan = create_backup_plan(cat)
            result = execute_backup_plan(
                cat,
                plan,
                dry_run=body.dry_run,
                progress_fn=lambda p: _update_progress(task.id, p),
            )
            _finish_task(task.id, result=result)
        except Exception as e:
            logger.exception("Backup execution failed")
            _finish_task(task.id, error=str(e))
        finally:
            _return_catalog(cat)

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started", "dry_run": body.dry_run}


@router.post("/backup/verify")
def backup_verify(request: Request, background_tasks: BackgroundTasks):
    """Verify backed up files exist on remotes (background task)."""
    from ...distributed_backup import verify_backups

    task = _create_task("backup:verify")
    catalog_path = str(request.app.state.catalog_path)

    def run():
        from ...catalog import Catalog  # Lazy import to avoid circular dependency

        cat = Catalog(catalog_path)
        cat.open()
        try:
            result = verify_backups(
                cat,
                progress_fn=lambda p: _update_progress(task.id, p),
            )
            _finish_task(task.id, result=result)
        except Exception as e:
            logger.exception("Backup verification failed")
            _finish_task(task.id, error=str(e))
        finally:
            _return_catalog(cat)

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started"}


@router.get("/backup/manifest")
def backup_manifest(request: Request, page: int = 1, limit: int = 50, search: str = ""):
    """Get paginated backup manifest."""
    from ...distributed_backup import ensure_backup_tables

    cat = _open_catalog(request)
    try:
        ensure_backup_tables(cat)
        offset = (page - 1) * limit

        where = ""
        params: list = []
        if search:
            where = "WHERE bm.path LIKE ?"
            params.append(f"%{search}%")

        total = cat.conn.execute(
            f"SELECT COUNT(*) FROM backup_manifest bm {where}",
            params,
        ).fetchone()[0]

        rows = cat.conn.execute(
            f"""
            SELECT bm.path, bm.size, bm.remote_name, bm.remote_path,
                   bm.backed_up_at, bm.verified, bm.verified_at
            FROM backup_manifest bm
            {where}
            ORDER BY bm.backed_up_at DESC
            LIMIT ? OFFSET ?
        """,
            [*params, limit, offset],
        ).fetchall()

        entries = [
            {
                "path": r[0],
                "filename": os.path.basename(r[0]),
                "size": r[1],
                "remote_name": r[2],
                "remote_path": r[3],
                "backed_up_at": r[4],
                "verified": bool(r[5]),
                "verified_at": r[6],
            }
            for r in rows
        ]

        return {
            "entries": entries,
            "total": total,
            "page": page,
            "pages": max(1, (total + limit - 1) // limit),
        }
    finally:
        _return_catalog(cat)


# ── Backup Monitoring ────────────────────────────────────────────────


@router.get("/backup/monitor")
def backup_monitor_status():
    """Get backup monitoring status and alerts."""
    from ...backup_monitor import get_monitor_status

    return get_monitor_status()


@router.post("/backup/monitor/check")
def backup_monitor_run(background_tasks: BackgroundTasks):
    """Run health checks on all backup targets."""
    from ...backup_monitor import run_health_checks

    task = _create_task("backup:health-check")

    def run():
        try:
            checks = run_health_checks()
            results = [
                {
                    "remote": c.remote_name,
                    "accessible": c.accessible,
                    "write_ok": c.write_ok,
                    "read_ok": c.read_ok,
                    "latency_ms": c.latency_ms,
                    "error": c.error,
                }
                for c in checks
            ]
            ok = sum(1 for c in checks if c.accessible)
            _finish_task(
                task.id,
                result={
                    "checked": len(checks),
                    "healthy": ok,
                    "unhealthy": len(checks) - ok,
                    "details": results,
                },
            )
        except Exception as e:
            logger.exception("Health check failed")
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started"}


@router.post("/backup/monitor/acknowledge")
def backup_monitor_ack():
    """Acknowledge all active alerts."""
    from ...backup_monitor import acknowledge_all_alerts

    count = acknowledge_all_alerts()
    return {"acknowledged": count}


@router.post("/backup/monitor/test-notification")
def backup_test_notification():
    """Send a test notification."""
    from ...backup_monitor import send_test_notification

    return send_test_notification()


# ── Bit Rot Detection ────────────────────────────────────────────────


@router.get("/bitrot/stats")
def bitrot_stats(request: Request):
    """Get bit rot verification statistics."""
    from ...bitrot import get_verification_stats

    cat = _open_catalog(request)
    try:
        return get_verification_stats(cat)
    finally:
        _return_catalog(cat)


@router.post("/bitrot/scan")
def bitrot_scan(request: Request, background_tasks: BackgroundTasks, limit: int = 500):
    """Run bit rot scan (background task)."""
    from ...bitrot import scan_bitrot

    task = _create_task("bitrot:scan")
    catalog_path = str(request.app.state.catalog_path)

    def run():
        from ...catalog import Catalog  # Lazy import to avoid circular dependency

        cat = Catalog(catalog_path)
        cat.open()
        try:
            result = scan_bitrot(
                cat,
                limit=limit,
                progress_fn=lambda p: _update_progress(task.id, p),
            )
            _finish_task(
                task.id,
                result={
                    "total_checked": result.total_checked,
                    "healthy": result.healthy,
                    "corrupted": result.corrupted,
                    "missing": result.missing,
                    "bytes_verified": result.bytes_verified,
                    "elapsed_seconds": result.elapsed_seconds,
                    "corrupted_files": result.corrupted_files[:50],
                    "missing_files": result.missing_files[:50],
                },
            )
        except Exception as e:
            logger.exception("Bit rot scan failed")
            _finish_task(task.id, error=str(e))
        finally:
            _return_catalog(cat)

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started", "limit": limit}


# ── Library integrity score ────────────────────────────────────────────


@router.get("/integrity-score")
def integrity_score(request: Request):
    """Compute overall library integrity score (0-100)."""
    cat = _open_catalog(request)
    try:
        total_files = cat.conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        if total_files == 0:
            return {"score": 0, "grade": "N/A", "factors": {}}

        # Factor 1: Hash coverage (files with SHA256)
        hashed = cat.conn.execute("SELECT COUNT(*) FROM files WHERE sha256 IS NOT NULL").fetchone()[0]
        hash_pct = hashed / max(total_files, 1)

        # Factor 2: Metadata richness (files with date_original)
        with_date = cat.conn.execute("SELECT COUNT(*) FROM files WHERE date_original IS NOT NULL AND date_original > '0000'").fetchone()[0]
        date_pct = with_date / max(total_files, 1)

        # Factor 3: Duplicate resolution (unresolved duplicate groups)
        dup_groups = cat.conn.execute("SELECT COUNT(DISTINCT group_id) FROM duplicates").fetchone()[0]
        dup_penalty = min(dup_groups * 0.5, 15)  # Max 15% penalty

        # Factor 4: Backup coverage
        try:
            backed_up = cat.conn.execute("SELECT COUNT(DISTINCT file_id) FROM backup_manifest").fetchone()[0]
            backup_pct = backed_up / max(total_files, 1)
        except (sqlite3.OperationalError, sqlite3.DatabaseError):
            backup_pct = 0

        # Factor 5: Verification freshness (files verified in last 30 days)
        try:
            recently_verified = cat.conn.execute(
                "SELECT COUNT(*) FROM files WHERE last_verified IS NOT NULL AND last_verified > datetime('now', '-30 days')"
            ).fetchone()[0]
            verify_pct = recently_verified / max(total_files, 1)
        except (sqlite3.OperationalError, sqlite3.DatabaseError):
            verify_pct = 0

        # Factor 6: Quality analysis coverage
        try:
            quality_analyzed = cat.conn.execute("SELECT COUNT(*) FROM files WHERE quality_category IS NOT NULL").fetchone()[0]
            quality_pct = quality_analyzed / max(total_files, 1)
        except (sqlite3.OperationalError, sqlite3.DatabaseError):
            quality_pct = 0

        # Compute weighted score
        score = (
            hash_pct * 25  # 25% weight: hashing
            + date_pct * 15  # 15% weight: metadata
            + backup_pct * 25  # 25% weight: backup
            + verify_pct * 15  # 15% weight: verification
            + quality_pct * 5  # 5% weight: quality
            + 15
            - dup_penalty  # 15% base minus duplicate penalty
        )
        score = max(0, min(100, round(score, 1)))

        # Grade
        if score >= 90:
            grade = "A+"
        elif score >= 80:
            grade = "A"
        elif score >= 70:
            grade = "B"
        elif score >= 60:
            grade = "C"
        elif score >= 50:
            grade = "D"
        else:
            grade = "F"

        return {
            "score": score,
            "grade": grade,
            "factors": {
                "hash_coverage": {"value": round(hash_pct * 100, 1), "weight": 25, "label": "Hashov\u00e1n\u00ed"},
                "metadata": {"value": round(date_pct * 100, 1), "weight": 15, "label": "Metadata"},
                "backup": {"value": round(backup_pct * 100, 1), "weight": 25, "label": "Z\u00e1loha"},
                "verification": {"value": round(verify_pct * 100, 1), "weight": 15, "label": "Verifikace"},
                "quality": {"value": round(quality_pct * 100, 1), "weight": 5, "label": "Kvalita"},
                "duplicates": {"value": round(max(0, 15 - dup_penalty), 1), "weight": 15, "label": "Duplicity", "groups": dup_groups},
            },
            "total_files": total_files,
        }
    finally:
        _return_catalog(cat)


# ── Backup auto-heal ──────────────────────────────────────────────────


@router.post("/backup/auto-heal")
def backup_auto_heal(request: Request, background_tasks: BackgroundTasks):
    """Auto-heal: redistribute files from unhealthy remotes."""
    from ...distributed_backup import auto_heal

    task = _create_task("backup:auto-heal")
    catalog_path = str(request.app.state.catalog_path)

    def run():
        from ...catalog import Catalog  # Lazy import to avoid circular dependency

        cat = Catalog(catalog_path)
        cat.open()
        try:
            result = auto_heal(cat, progress_fn=lambda p: _update_progress(task.id, p))
            _finish_task(task.id, result=result)
        except Exception as e:
            logger.exception("Auto-heal failed")
            _finish_task(task.id, error=str(e))
        finally:
            _return_catalog(cat)

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started"}
