from __future__ import annotations

import asyncio
import os
import platform
import shutil
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import quote as _url_quote

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from ...deps import check_all
from ..shared import (
    ScanConfig,
    _create_task,
    _finish_task,
    _open_catalog,
    _task_to_msg,
    _tasks,
    _tasks_lock,
    _update_progress,
    _ws_connections,
    _ws_lock,
    logger,
)

router = APIRouter()


# ── Stats & categories ────────────────────────────────────────────────


@router.get("/stats")
def get_stats(request: Request) -> dict:
    """Library statistics overview."""
    cat = _open_catalog(request)
    try:
        return cat.stats()
    finally:
        cat.close()


@router.get("/categories")
def get_categories(request: Request) -> dict:
    """Get file counts grouped by media type category."""
    cat = _open_catalog(request)
    try:
        categories = {}
        category_defs = [
            ("images", "jpg,jpeg,png,gif,bmp,tiff,tif,webp,heic,heif,svg,raw,cr2,nef,arw,dng"),
            ("videos", "mp4,mov,avi,mkv,wmv,flv,webm,m4v,3gp"),
            ("audio", "mp3,wav,flac,aac,ogg,wma,m4a,opus"),
            ("documents", "pdf,doc,docx,xls,xlsx,ppt,pptx,odt,ods,odp,rtf,epub"),
            ("text", "txt,md,csv,json,xml,yaml,yml,toml,ini,cfg,py,js,ts,html,css,sql,sh,go,rs,java,c,cpp,h,rb,php,swift,kt,log"),
            ("archives", "zip,tar,gz,bz2,xz,7z,rar,dmg,iso"),
        ]
        for cat_name, exts in category_defs:
            ext_list = exts.split(",")
            placeholders = ",".join("?" * len(ext_list))
            cur = cat.conn.execute(
                f"SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files WHERE LOWER(ext) IN ({placeholders})",  # noqa: S608
                ext_list,
            )
            count, total_size = cur.fetchone()
            categories[cat_name] = {"count": count, "size": total_size}

        total_cur = cat.conn.execute("SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files")
        total_count, total_size = total_cur.fetchone()
        known_count = sum(c["count"] for c in categories.values())
        known_size = sum(c["size"] for c in categories.values())
        categories["other"] = {"count": total_count - known_count, "size": total_size - known_size}

        return {"categories": categories}
    finally:
        cat.close()


# ── Memories (On This Day) ────────────────────────────────────────────


@router.get("/memories")
def get_memories(request: Request) -> dict:
    """Get photos from this day in previous years (On This Day)."""
    today = date.today()
    cat = _open_catalog(request)
    try:
        memories: list[dict] = []
        cur = cat.conn.execute(
            "SELECT path, date_original, camera_model, size "
            "FROM files WHERE date_original IS NOT NULL "
            "AND strftime('%%m-%%d', date_original) = ? "
            "ORDER BY date_original DESC",
            (today.strftime("%m-%d"),),
        )
        by_year: dict[str, list[dict]] = {}
        for row in cur.fetchall():
            year = row[1][:4] if row[1] else None
            if year and year != str(today.year):
                by_year.setdefault(year, []).append(
                    {
                        "path": row[0],
                        "date": row[1],
                        "camera": row[2],
                        "size": row[3],
                    }
                )
        for year in sorted(by_year.keys(), reverse=True):
            years_ago = today.year - int(year)
            memories.append(
                {
                    "year": year,
                    "years_ago": years_ago,
                    "files": by_year[year][:10],
                }
            )
        return {"date": today.isoformat(), "memories": memories}
    finally:
        cat.close()


# ── System info & dependencies ────────────────────────────────────────


@router.get("/system-info")
def get_system_info(request: Request) -> dict:
    """System information for the Doctor page."""
    cat = _open_catalog(request)
    try:
        stats = cat.stats()
        cat_path = request.app.state.catalog_path
        cat_size = cat_path.stat().st_size if cat_path.exists() else 0
        quarantine_path = Path.home() / ".config" / "gml" / "quarantine"
        quarantine_size = sum(f.stat().st_size for f in quarantine_path.rglob("*") if f.is_file()) if quarantine_path.exists() else 0
        # Disk free space for the catalog volume
        try:
            disk = shutil.disk_usage(cat_path.parent)
            disk_free = disk.free
            disk_total = disk.total
        except OSError:
            disk_free = 0
            disk_total = 0

        return {
            "python_version": sys.version,
            "platform": platform.platform(),
            "catalog_path": str(cat_path),
            "catalog_size": cat_size,
            "total_files": stats.get("total_files", 0),
            "total_size": stats.get("total_size_bytes", 0),
            "quarantine_size": quarantine_size,
            "last_scan_root": stats.get("last_scan_root", ""),
            "disk_free": disk_free,
            "disk_total": disk_total,
        }
    finally:
        cat.close()


@router.get("/deps")
def get_deps() -> dict:
    """Check dependency status."""
    statuses = check_all()
    return {
        "dependencies": [
            {
                "name": s.name,
                "available": s.available,
                "version": s.version,
                "install_hint": s.install_hint,
            }
            for s in statuses
        ]
    }


# ── Background tasks: scan, pipeline, verify ─────────────────────────


@router.post("/scan")
def start_scan(
    request: Request,
    background_tasks: BackgroundTasks,
    config: ScanConfig | None = None,
) -> dict:
    """Trigger an incremental scan as a background task."""
    cfg = config or ScanConfig()
    task = _create_task("scan")

    def _run_scan():
        try:
            from ...catalog import Catalog  # Lazy import to avoid circular dependency
            from ...scanner import incremental_scan  # Lazy import to avoid circular dependency

            cat = Catalog(request.app.state.catalog_path)
            scan_roots = [Path(r) for r in cfg.roots] if cfg.roots else []
            if not scan_roots:
                with cat:
                    last_scan = cat.stats().get("last_scan_root", "")
                    scan_roots = [Path(r) for r in last_scan.split(";") if r] if last_scan else []
            if not scan_roots:
                _finish_task(task.id, error="No roots configured. Provide roots or run gml scan --roots first.")
                return
            with cat:
                stats = incremental_scan(
                    cat,
                    scan_roots,
                    extract_exiftool=cfg.extract_exiftool,
                    workers=cfg.workers,
                    progress_callback=lambda p: _update_progress(task.id, p),
                )
            _finish_task(
                task.id,
                result={
                    "files_scanned": stats.files_scanned,
                    "files_new": stats.files_new,
                    "files_changed": stats.files_changed,
                },
            )
        except Exception as e:
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(_run_scan)
    return {"task_id": task.id, "status": "started"}


@router.post("/backfill-metadata")
def backfill_metadata(request: Request):
    """Backfill date_original and GPS from already-stored ExifTool metadata + filesystem dates."""
    from ...scanner import _backfill_dates_from_filesystem, backfill_metadata_from_stored

    cat = _open_catalog(request)
    try:
        result = backfill_metadata_from_stored(cat)
        fs_dates = _backfill_dates_from_filesystem(cat)
        result["fs_dates_filled"] = fs_dates
        return result
    finally:
        cat.close()


@router.post("/pipeline")
def start_pipeline(
    request: Request,
    background_tasks: BackgroundTasks,
    config: ScanConfig | None = None,
) -> dict:
    """Trigger the full pipeline as a background task."""
    cfg = config or ScanConfig()
    task = _create_task("pipeline")

    def _run_pipeline():
        try:
            from ...catalog import Catalog  # Lazy import to avoid circular dependency
            from ...pipeline import PipelineConfig, run_pipeline  # Lazy import to avoid circular dependency

            pipeline_roots = [Path(r) for r in cfg.roots] if cfg.roots else []
            if not pipeline_roots:
                cat = Catalog(request.app.state.catalog_path)
                with cat:
                    last_scan = cat.stats().get("last_scan_root", "")
                    pipeline_roots = [Path(r) for r in last_scan.split(";") if r] if last_scan else []
            if not pipeline_roots:
                _finish_task(task.id, error="No roots configured. Provide roots or run gml scan --roots first.")
                return

            pipeline_config = PipelineConfig(
                roots=pipeline_roots,
                catalog_path=request.app.state.catalog_path,
                interactive=False,
                auto_merge=True,
                workers=cfg.workers,
            )
            result = run_pipeline(pipeline_config)
            _finish_task(
                task.id,
                result={
                    "files_scanned": result.files_scanned,
                    "metadata_extracted": result.metadata_extracted,
                    "duplicate_groups": result.duplicate_groups,
                    "merge_plans": result.merge_plans_created,
                    "tags_merged": result.tags_merged,
                    "errors": result.errors[:10],
                },
            )
        except Exception as e:
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(_run_pipeline)
    return {"task_id": task.id, "status": "started"}


@router.post("/verify")
def start_verify(
    request: Request,
    background_tasks: BackgroundTasks,
    check_hashes: bool = False,
) -> dict:
    """Trigger catalog verification as a background task."""
    task = _create_task("verify")

    def _run_verify():
        try:
            from ...catalog import Catalog  # Lazy import to avoid circular dependency
            from ...verify import verify_catalog  # Lazy import to avoid circular dependency

            cat = Catalog(request.app.state.catalog_path)
            with cat:
                result = verify_catalog(
                    cat,
                    check_hashes=check_hashes,
                    progress_callback=lambda p: _update_progress(task.id, p),
                )
            _finish_task(
                task.id,
                result={
                    "total_checked": result.total_checked,
                    "ok": result.ok,
                    "missing": len(result.missing_files),
                    "size_mismatches": len(result.size_mismatches),
                    "hash_mismatches": len(result.hash_mismatches),
                    "has_issues": result.has_issues,
                },
            )
        except Exception as e:
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(_run_verify)
    return {"task_id": task.id, "status": "started"}


# ── Task status endpoints ─────────────────────────────────────────────


@router.get("/tasks/{task_id}")
def get_task(task_id: str) -> dict:
    """Check status of a background task."""
    with _tasks_lock:
        task = _tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return {
        "id": task.id,
        "command": task.command,
        "status": task.status,
        "result": task.result,
        "error": task.error,
        "progress": task.progress,
        "started_at": task.started_at,
        "finished_at": task.finished_at,
    }


@router.websocket("/ws/tasks/{task_id}")
async def ws_task(websocket: WebSocket, task_id: str):
    # Explicit token authentication for WebSocket connections.
    # WebSocket upgrade requests may bypass HTTP middleware, so we check here.
    # Localhost is trusted (same as HTTP auth middleware).
    api_token = os.environ.get("GML_API_TOKEN", "")
    client_ip = websocket.client.host if websocket.client else "unknown"
    if api_token and client_ip not in ("127.0.0.1", "::1", "localhost"):
        import hmac as _hmac

        token_param = websocket.query_params.get("token", "")
        if not token_param or not _hmac.compare_digest(token_param, api_token):
            await websocket.close(code=4003, reason="Invalid or missing API token")
            return

    await websocket.accept()
    with _ws_lock:
        _ws_connections.setdefault(task_id, []).append(websocket)
    try:
        while True:
            with _tasks_lock:
                task = _tasks.get(task_id)
            if task is None:
                await websocket.send_json({"error": "Task not found"})
                break
            msg = _task_to_msg(task)
            await websocket.send_json(msg)
            if task.status in ("completed", "failed"):
                break
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass
    finally:
        with _ws_lock:
            conns = _ws_connections.get(task_id, [])
            if websocket in conns:
                conns.remove(websocket)
            if not conns and task_id in _ws_connections:
                del _ws_connections[task_id]


@router.get("/tasks")
def list_tasks() -> dict:
    """List all tasks."""
    with _tasks_lock:
        tasks = [
            {
                "id": t.id,
                "command": t.command,
                "status": t.status,
                "started_at": t.started_at,
                "finished_at": t.finished_at,
                "error": t.error,
            }
            for t in _tasks.values()
        ]
    return {"tasks": tasks}


# ── Timeline / gaps ──────────────────────────────────────────────────


def _safe_disk_count(root: str, max_seconds: float = 3.0) -> int:
    """Count files on disk, skipping hidden dirs and respecting timeout."""
    count = 0
    deadline = time.monotonic() + max_seconds
    try:
        for _dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if not d.startswith(".")]
            count += len(filenames)
            if time.monotonic() > deadline:
                break
    except OSError:
        pass
    return count


@router.get("/timeline/gaps")
def get_timeline_gaps(request: Request) -> dict:
    """Analyse timeline coverage — monthly file counts, gaps, and coverage stats."""
    cat = _open_catalog(request)
    try:
        # Fetch all year-month combos with counts
        # date_original uses EXIF format "YYYY:MM:DD HH:MM:SS" — use substr
        cur = cat.conn.execute(
            "SELECT SUBSTR(date_original, 1, 4) AS y, SUBSTR(date_original, 6, 2) AS m, COUNT(*) AS cnt "
            "FROM files WHERE date_original IS NOT NULL "
            "AND LENGTH(date_original) >= 10 "
            "AND SUBSTR(date_original, 1, 4) > '0000' "
            "GROUP BY y, m ORDER BY y, m"
        )
        rows = cur.fetchall()
        if not rows:
            return {
                "months": [],
                "gaps": [],
                "coverage": {"first_date": None, "last_date": None, "total_months": 0, "covered_months": 0, "coverage_pct": 0},
            }

        # Build month list — skip rows with NULL year/month (bad date format)
        month_counts: dict[tuple[int, int], int] = {}
        for r in rows:
            if r[0] is None or r[1] is None:
                continue
            year, month = int(r[0]), int(r[1])
            month_counts[(year, month)] = r[2]

        if not month_counts:
            return {
                "months": [],
                "gaps": [],
                "coverage": {"first_date": None, "last_date": None, "total_months": 0, "covered_months": 0, "coverage_pct": 0},
            }

        first_ym = min(month_counts.keys())
        last_ym = max(month_counts.keys())

        # Generate all months in range
        all_months: list[dict] = []
        y, m = first_ym
        while (y, m) <= last_ym:
            cnt = month_counts.get((y, m), 0)
            all_months.append({"year": y, "month": m, "count": cnt})
            m += 1
            if m > 12:
                m = 1
                y += 1

        # Detect consecutive gaps (months with 0 files)
        gaps: list[dict] = []
        gap_start = None
        for entry in all_months:
            if entry["count"] == 0:
                if gap_start is None:
                    gap_start = entry
            else:
                if gap_start is not None:
                    # End of gap — previous entry was the last zero month
                    prev = all_months[all_months.index(entry) - 1]
                    from_str = f"{gap_start['year']}-{gap_start['month']:02d}"
                    to_str = f"{prev['year']}-{prev['month']:02d}"
                    gap_len = 0
                    gy, gm = gap_start["year"], gap_start["month"]
                    while (gy, gm) <= (prev["year"], prev["month"]):
                        gap_len += 1
                        gm += 1
                        if gm > 12:
                            gm = 1
                            gy += 1
                    gaps.append({"from": from_str, "to": to_str, "months": gap_len})
                    gap_start = None
        # Handle trailing gap
        if gap_start is not None:
            last = all_months[-1]
            from_str = f"{gap_start['year']}-{gap_start['month']:02d}"
            to_str = f"{last['year']}-{last['month']:02d}"
            gap_len = 0
            gy, gm = gap_start["year"], gap_start["month"]
            while (gy, gm) <= (last["year"], last["month"]):
                gap_len += 1
                gm += 1
                if gm > 12:
                    gm = 1
                    gy += 1
            gaps.append({"from": from_str, "to": to_str, "months": gap_len})

        total_months = len(all_months)
        covered_months = sum(1 for e in all_months if e["count"] > 0)
        coverage_pct = round(covered_months / total_months * 100, 1) if total_months else 0

        # Fetch sample thumbnails per month (up to 4 image paths per month)
        _IMG_EXTS = (
            "jpg", "jpeg", "heic", "png", "webp", "tiff", "tif", "bmp", "gif",
        )
        # ext column may or may not have a leading dot
        ext_vals = []
        for e in _IMG_EXTS:
            ext_vals.append(e)
            ext_vals.append(f".{e}")
        ext_placeholders = ",".join(f"'{v}'" for v in ext_vals)
        thumb_cur = cat.conn.execute(
            f"SELECT SUBSTR(date_original, 1, 4) AS y, "
            f"       SUBSTR(date_original, 6, 2) AS m, "
            f"       path "
            f"FROM files "
            f"WHERE date_original IS NOT NULL "
            f"  AND LENGTH(date_original) >= 10 "
            f"  AND SUBSTR(date_original, 1, 4) > '0000' "
            f"  AND LOWER(ext) IN ({ext_placeholders}) "
            f"ORDER BY date_original DESC"
        )
        month_thumbs: dict[tuple[str, str], list[str]] = {}
        for row in thumb_cur:
            key = (row[0], row[1])
            if key not in month_thumbs:
                month_thumbs[key] = []
            if len(month_thumbs[key]) < 4:
                month_thumbs[key].append(row[2])

        # Add thumbs to month entries
        for entry in all_months:
            key = (str(entry["year"]).zfill(4), str(entry["month"]).zfill(2))
            entry["thumbs"] = month_thumbs.get(key, [])

        return {
            "months": all_months,
            "gaps": gaps,
            "coverage": {
                "first_date": f"{first_ym[0]}-{first_ym[1]:02d}",
                "last_date": f"{last_ym[0]}-{last_ym[1]:02d}",
                "total_months": total_months,
                "covered_months": covered_months,
                "coverage_pct": coverage_pct,
            },
        }
    finally:
        cat.close()


# ── Quality scoring endpoints ─────────────────────────────────────────


@router.post("/quality/analyze")
def trigger_quality_analysis(request: Request, background: BackgroundTasks):
    """Start background quality analysis for all unanalyzed images."""
    task = _create_task("quality_analyze")

    def _run():
        try:
            from godmode_media_library.quality import batch_analyze

            cat = _open_catalog(request)

            def on_progress(done, total):
                _update_progress(task.id, {"done": done, "total": total})

            try:
                stats = batch_analyze(cat, progress_fn=on_progress)
                _finish_task(task.id, result=stats)
            finally:
                cat.close()
        except Exception as exc:
            _finish_task(task.id, error=str(exc))
            logger.exception("Quality analysis task failed")

    background.add_task(_run)
    return {"task_id": task.id, "status": "started"}


@router.get("/quality/stats")
def get_quality_stats(request: Request) -> dict:
    """Return quality category breakdown."""
    cat = _open_catalog(request)
    try:
        cur = cat.conn.execute("SELECT quality_category, COUNT(*) FROM files WHERE quality_category IS NOT NULL GROUP BY quality_category")
        categories = {row[0]: row[1] for row in cur.fetchall()}

        blurry = cat.conn.execute("SELECT COUNT(*) FROM files WHERE quality_blur IS NOT NULL AND quality_blur < 50").fetchone()[0]

        dark = cat.conn.execute("SELECT COUNT(*) FROM files WHERE quality_brightness IS NOT NULL AND quality_brightness < 40").fetchone()[0]

        overexposed = cat.conn.execute(
            "SELECT COUNT(*) FROM files WHERE quality_brightness IS NOT NULL AND quality_brightness > 220"
        ).fetchone()[0]

        analyzed = cat.conn.execute("SELECT COUNT(*) FROM files WHERE quality_category IS NOT NULL").fetchone()[0]

        return {
            **categories,
            "blurry": blurry,
            "dark": dark,
            "overexposed": overexposed,
            "analyzed": analyzed,
        }
    finally:
        cat.close()


# ── Report endpoints ──────────────────────────────────────────────────


@router.get("/report/generate")
def report_generate(request: Request) -> HTMLResponse:
    """Generate a comprehensive HTML report and return it inline."""
    from ...report import generate_report_html

    catalog_path = request.app.state.catalog_path
    html = generate_report_html(catalog_path)
    return HTMLResponse(content=html)


@router.get("/report/download")
def report_download(request: Request) -> HTMLResponse:
    """Generate a comprehensive HTML report and return it as a downloadable file."""
    from ...report import generate_report_html

    catalog_path = request.app.state.catalog_path
    html = generate_report_html(catalog_path)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"godmode_report_{ts}.html"
    return HTMLResponse(
        content=html,
        headers={"Content-Disposition": "attachment; filename*=UTF-8''" + _url_quote(filename, safe="")},
    )
