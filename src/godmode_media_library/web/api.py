"""REST API endpoints for GOD MODE Media Library."""

from __future__ import annotations

import io
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

router = APIRouter()


# ── Background task tracking ──────────────────────────────────────────

@dataclass
class TaskStatus:
    id: str
    command: str
    status: str = "running"  # running | completed | failed
    progress: dict | None = None
    result: dict | None = None
    started_at: str = ""
    finished_at: str | None = None
    error: str | None = None


_tasks: dict[str, TaskStatus] = {}
_tasks_lock = threading.Lock()


def _create_task(command: str) -> TaskStatus:
    task = TaskStatus(
        id=str(uuid.uuid4())[:8],
        command=command,
        started_at=datetime.now(timezone.utc).isoformat(),
    )
    with _tasks_lock:
        _tasks[task.id] = task
    return task


def _finish_task(task_id: str, result: dict | None = None, error: str | None = None) -> None:
    with _tasks_lock:
        if task_id in _tasks:
            _tasks[task_id].status = "failed" if error else "completed"
            _tasks[task_id].result = result
            _tasks[task_id].error = error
            _tasks[task_id].finished_at = datetime.now(timezone.utc).isoformat()


# ── Catalog helper ────────────────────────────────────────────────────

def _open_catalog(request: Request):
    from ..catalog import Catalog
    cat = Catalog(request.app.state.catalog_path)
    cat.open()
    return cat


# ── Endpoints ─────────────────────────────────────────────────────────

@router.get("/stats")
def get_stats(request: Request) -> dict:
    """Library statistics overview."""
    cat = _open_catalog(request)
    try:
        return cat.stats()
    finally:
        cat.close()


@router.get("/files")
def get_files(
    request: Request,
    ext: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
    path_contains: str | None = None,
    camera: str | None = None,
    has_gps: bool | None = None,
    has_phash: bool | None = None,
    limit: int = Query(default=500, le=10000),
) -> dict:
    """Query files with filters."""
    cat = _open_catalog(request)
    try:
        rows = cat.query_files(
            ext=ext,
            date_from=date_from,
            date_to=date_to,
            min_size=min_size * 1024 if min_size else None,
            max_size=max_size * 1024 if max_size else None,
            path_contains=path_contains,
            camera=camera,
            has_gps=has_gps,
            has_phash=has_phash,
            limit=limit + 1,
        )
        has_more = len(rows) > limit
        items = rows[:limit]
        return {
            "files": [_row_to_dict(r) for r in items],
            "count": len(items),
            "has_more": has_more,
        }
    finally:
        cat.close()


@router.get("/files/{file_path:path}")
def get_file_detail(request: Request, file_path: str) -> dict:
    """Get file details including deep metadata."""
    cat = _open_catalog(request)
    try:
        row = cat.get_file_by_path(f"/{file_path}")
        if row is None:
            raise HTTPException(status_code=404, detail="File not found in catalog")
        meta = cat.get_file_metadata(f"/{file_path}")
        richness = cat.get_metadata_richness(f"/{file_path}")
        return {
            "file": _row_to_dict(row),
            "metadata": meta,
            "richness": richness,
        }
    finally:
        cat.close()


@router.get("/duplicates")
def get_duplicates(
    request: Request,
    limit: int = Query(default=100, le=1000),
) -> dict:
    """List duplicate groups."""
    cat = _open_catalog(request)
    try:
        all_groups = cat.query_duplicates()  # list of (group_id, rows) tuples
        groups = []
        for gid, rows in all_groups[:limit]:
            groups.append({
                "group_id": gid,
                "file_count": len(rows),
                "total_size": sum(r.size for r in rows),
                "files": [{"path": str(r.path), "size": r.size} for r in rows],
            })
        return {"groups": groups, "total_groups": len(all_groups)}
    finally:
        cat.close()


@router.get("/duplicates/{group_id}")
def get_duplicate_group(request: Request, group_id: str) -> dict:
    """Get detailed metadata for a duplicate group."""
    cat = _open_catalog(request)
    try:
        group_meta = cat.get_group_metadata(group_id)
        if not group_meta:
            raise HTTPException(status_code=404, detail="Group not found")
        return {
            "group_id": group_id,
            "files": [
                {"path": path, "metadata": meta}
                for path, meta in group_meta
            ],
        }
    finally:
        cat.close()


@router.get("/duplicates/{group_id}/diff")
def get_duplicate_diff(request: Request, group_id: str) -> dict:
    """Compute metadata diff for a duplicate group."""
    from ..metadata_richness import compute_group_diff

    cat = _open_catalog(request)
    try:
        group_meta = cat.get_group_metadata(group_id)
        if len(group_meta) < 2:
            raise HTTPException(status_code=404, detail="Group not found or has < 2 files")
        diff = compute_group_diff(group_meta)
        return {
            "group_id": group_id,
            "unanimous": diff.unanimous,
            "partial": diff.partial,
            "conflicts": diff.conflicts,
            "scores": diff.scores,
        }
    finally:
        cat.close()


@router.get("/similar")
def get_similar(
    request: Request,
    threshold: int = Query(default=10, ge=0, le=64),
    limit: int = Query(default=100, le=1000),
) -> dict:
    """Find visually similar files via perceptual hash."""
    from ..perceptual_hash import find_similar

    cat = _open_catalog(request)
    try:
        hashes = cat.get_all_phashes()
        pairs = find_similar(hashes, threshold=threshold)
        return {
            "pairs": [
                {
                    "path_a": p.path_a,
                    "path_b": p.path_b,
                    "distance": p.distance,
                }
                for p in pairs[:limit]
            ],
            "total_pairs": len(pairs),
        }
    finally:
        cat.close()


@router.get("/deps")
def get_deps() -> dict:
    """Check dependency status."""
    from ..deps import check_all

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


@router.get("/thumbnail/{file_path:path}")
def get_thumbnail(file_path: str, size: int = Query(default=200, le=800)) -> StreamingResponse:
    """Generate and serve a thumbnail for an image file."""
    full_path = Path(f"/{file_path}")
    if not full_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    ext = full_path.suffix.lower()
    image_exts = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".gif", ".webp", ".heic", ".heif"}
    if ext not in image_exts:
        raise HTTPException(status_code=400, detail="Not an image file")

    try:
        from PIL import Image

        if ext in (".heic", ".heif"):
            try:
                import pillow_heif
                pillow_heif.register_heif_opener()
            except ImportError:
                raise HTTPException(status_code=400, detail="pillow-heif required for HEIC") from None

        with Image.open(full_path) as img:
            img.thumbnail((size, size), Image.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=80)
            buf.seek(0)
            return StreamingResponse(buf, media_type="image/jpeg")
    except ImportError:
        raise HTTPException(status_code=500, detail="Pillow not installed") from None
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/scan")
def start_scan(request: Request, background_tasks: BackgroundTasks) -> dict:
    """Trigger an incremental scan as a background task."""
    task = _create_task("scan")

    def _run_scan():
        try:
            from ..catalog import Catalog
            from ..scanner import incremental_scan

            cat = Catalog(request.app.state.catalog_path)
            # Get roots from last scan
            with cat:
                last_scan = cat.stats().get("last_scan_root", "")
                roots = [Path(r) for r in last_scan.split(";") if r] if last_scan else []
                if not roots:
                    _finish_task(task.id, error="No roots configured. Run gml scan --roots first.")
                    return
                stats = incremental_scan(cat, roots, extract_exiftool=True)
            _finish_task(task.id, result={
                "files_scanned": stats.files_scanned,
                "files_new": stats.files_new,
                "files_changed": stats.files_changed,
            })
        except Exception as e:
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(_run_scan)
    return {"task_id": task.id, "status": "started"}


@router.post("/pipeline")
def start_pipeline(request: Request, background_tasks: BackgroundTasks) -> dict:
    """Trigger the full pipeline as a background task."""
    task = _create_task("pipeline")

    def _run_pipeline():
        try:
            from ..catalog import Catalog
            from ..pipeline import PipelineConfig, run_pipeline

            cat = Catalog(request.app.state.catalog_path)
            with cat:
                last_scan = cat.stats().get("last_scan_root", "")
                roots = [Path(r) for r in last_scan.split(";") if r] if last_scan else []
            if not roots:
                _finish_task(task.id, error="No roots configured. Run gml scan --roots first.")
                return

            config = PipelineConfig(
                roots=roots,
                catalog_path=request.app.state.catalog_path,
                interactive=False,
                auto_merge=True,
            )
            result = run_pipeline(config)
            _finish_task(task.id, result={
                "files_scanned": result.files_scanned,
                "metadata_extracted": result.metadata_extracted,
                "duplicate_groups": result.duplicate_groups,
                "merge_plans": result.merge_plans_created,
                "tags_merged": result.tags_merged,
                "errors": result.errors[:10],
            })
        except Exception as e:
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(_run_pipeline)
    return {"task_id": task.id, "status": "started"}


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
        "started_at": task.started_at,
        "finished_at": task.finished_at,
    }


# ── Helpers ───────────────────────────────────────────────────────────

def _row_to_dict(row: Any) -> dict:
    """Convert a CatalogFileRow to a serializable dict."""
    return {
        "path": row.path,
        "size": row.size,
        "ext": row.ext,
        "sha256": row.sha256,
        "mtime": row.mtime,
        "birthtime": row.birthtime,
        "width": getattr(row, "width", None),
        "height": getattr(row, "height", None),
        "duration_seconds": getattr(row, "duration_seconds", None),
        "video_codec": getattr(row, "video_codec", None),
        "audio_codec": getattr(row, "audio_codec", None),
        "bitrate": getattr(row, "bitrate", None),
        "phash": getattr(row, "phash", None),
        "date_original": getattr(row, "date_original", None),
        "camera_make": getattr(row, "camera_make", None),
        "camera_model": getattr(row, "camera_model", None),
        "gps_latitude": getattr(row, "gps_latitude", None),
        "gps_longitude": getattr(row, "gps_longitude", None),
        "asset_key": getattr(row, "asset_key", None),
        "asset_component": getattr(row, "asset_component", None),
    }
