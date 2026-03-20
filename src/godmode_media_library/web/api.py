"""REST API endpoints for GOD MODE Media Library."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import logging
import shutil
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

# Maximum completed tasks to keep in memory before eviction
_MAX_COMPLETED_TASKS = 50
_TASK_TTL_SECONDS = 3600  # 1 hour


class ScanConfig(BaseModel):
    roots: list[str] | None = None
    workers: int = 1
    extract_exiftool: bool = True


class QuarantineRequest(BaseModel):
    paths: list[str]
    quarantine_root: str | None = None


class DeleteRequest(BaseModel):
    paths: list[str]


class RenameItem(BaseModel):
    path: str
    new_name: str


class RenameRequest(BaseModel):
    renames: list[RenameItem]


class MoveRequest(BaseModel):
    paths: list[str]
    destination: str


class DuplicateKeepRequest(BaseModel):
    keep_path: str


class RestoreRequest(BaseModel):
    paths: list[str]
    quarantine_root: str | None = None


class RootsRequest(BaseModel):
    roots: list[str]


class RemoveRootRequest(BaseModel):
    path: str


_DEFAULT_QUARANTINE_ROOT = Path.home() / ".config" / "gml" / "quarantine"

# Directories that should not be browsable for security
_BLOCKED_PREFIXES = ("/etc", "/var", "/private", "/sbin", "/usr", "/bin", "/tmp", "/dev", "/proc", "/sys")


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
    _created_ts: float = field(default_factory=time.monotonic, repr=False)


_tasks: dict[str, TaskStatus] = {}
_tasks_lock = threading.Lock()

_ws_connections: dict[str, list[WebSocket]] = {}
_ws_lock = threading.Lock()


def _evict_old_tasks() -> None:
    """Remove completed/failed tasks older than TTL. Must be called under _tasks_lock."""
    now = time.monotonic()
    to_remove = [
        tid
        for tid, t in _tasks.items()
        if t.status in ("completed", "failed") and (now - t._created_ts) > _TASK_TTL_SECONDS
    ]
    for tid in to_remove:
        del _tasks[tid]
    # Hard cap: if still too many completed, remove oldest
    completed = [(tid, t._created_ts) for tid, t in _tasks.items() if t.status in ("completed", "failed")]
    if len(completed) > _MAX_COMPLETED_TASKS:
        completed.sort(key=lambda x: x[1])
        for tid, _ in completed[: len(completed) - _MAX_COMPLETED_TASKS]:
            del _tasks[tid]


def _create_task(command: str) -> TaskStatus:
    task = TaskStatus(
        id=str(uuid.uuid4())[:8],
        command=command,
        started_at=datetime.now(timezone.utc).isoformat(),
    )
    with _tasks_lock:
        _evict_old_tasks()
        _tasks[task.id] = task
    return task


def _task_to_msg(task: TaskStatus) -> dict:
    """Serialize a TaskStatus to a JSON-safe dict for WebSocket broadcast."""
    return {
        "id": task.id,
        "command": task.command,
        "status": task.status,
        "progress": task.progress,
        "result": task.result,
        "error": task.error,
        "started_at": task.started_at,
        "finished_at": task.finished_at,
    }


def _notify_ws(task_id: str, msg: dict) -> None:
    """Best-effort broadcast to all WebSocket connections for a task."""
    with _ws_lock:
        conns = _ws_connections.get(task_id, [])
        if not conns:
            return
        stale: list[WebSocket] = []
        for ws in conns:
            try:
                # send_json is a coroutine; schedule it on the running loop
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.run_coroutine_threadsafe(ws.send_json(msg), loop)
                else:
                    loop.run_until_complete(ws.send_json(msg))
            except Exception:
                stale.append(ws)
        for ws in stale:
            conns.remove(ws)


def _update_progress(task_id: str, progress: dict) -> None:
    with _tasks_lock:
        if task_id in _tasks:
            _tasks[task_id].progress = progress
            msg = _task_to_msg(_tasks[task_id])
    _notify_ws(task_id, msg)


def _finish_task(task_id: str, result: dict | None = None, error: str | None = None) -> None:
    with _tasks_lock:
        if task_id in _tasks:
            _tasks[task_id].status = "failed" if error else "completed"
            _tasks[task_id].result = result
            _tasks[task_id].error = error
            _tasks[task_id].finished_at = datetime.now(timezone.utc).isoformat()
            msg = _task_to_msg(_tasks[task_id])
    _notify_ws(task_id, msg)


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
    offset: int = Query(default=0, ge=0),
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
            offset=offset,
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
def get_thumbnail(request: Request, file_path: str, size: int = Query(default=200, le=800)) -> StreamingResponse:
    """Generate and serve a thumbnail for an image file."""
    full_path = Path(f"/{file_path}").resolve()

    # Security: verify the file is within the catalog (exists in DB)
    cat = _open_catalog(request)
    try:
        row = cat.get_file_by_path(str(full_path))
        if row is None:
            raise HTTPException(status_code=404, detail="File not found in catalog")
    finally:
        cat.close()

    if not full_path.exists():
        raise HTTPException(status_code=404, detail="File not found on disk")

    ext = full_path.suffix.lower()
    image_exts = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".gif", ".webp", ".heic", ".heif"}
    if ext not in image_exts:
        raise HTTPException(status_code=400, detail="Not an image file")

    try:
        from PIL import Image
    except ImportError:
        raise HTTPException(status_code=500, detail="Pillow not installed") from None

    if ext in (".heic", ".heif"):
        try:
            import pillow_heif
            pillow_heif.register_heif_opener()
        except ImportError:
            raise HTTPException(status_code=400, detail="pillow-heif required for HEIC") from None

    try:
        with Image.open(full_path) as img:
            img.thumbnail((size, size), Image.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=80)
            buf.seek(0)
            return StreamingResponse(
                buf,
                media_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=86400"},
            )
    except (OSError, ValueError) as e:
        logger.warning("Thumbnail generation failed for %s: %s", full_path, e)
        raise HTTPException(status_code=500, detail="Failed to generate thumbnail") from e


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
            from ..catalog import Catalog
            from ..scanner import incremental_scan

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
                    cat, scan_roots,
                    extract_exiftool=cfg.extract_exiftool,
                    workers=cfg.workers,
                    progress_callback=lambda p: _update_progress(task.id, p),
                )
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
            from ..catalog import Catalog
            from ..pipeline import PipelineConfig, run_pipeline

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
            from ..catalog import Catalog
            from ..verify import verify_catalog

            cat = Catalog(request.app.state.catalog_path)
            with cat:
                result = verify_catalog(
                    cat,
                    check_hashes=check_hashes,
                    progress_callback=lambda p: _update_progress(task.id, p),
                )
            _finish_task(task.id, result={
                "total_checked": result.total_checked,
                "ok": result.ok,
                "missing": len(result.missing_files),
                "size_mismatches": len(result.size_mismatches),
                "hash_mismatches": len(result.hash_mismatches),
                "has_issues": result.has_issues,
            })
        except Exception as e:
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(_run_verify)
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
        "progress": task.progress,
        "started_at": task.started_at,
        "finished_at": task.finished_at,
    }


@router.websocket("/ws/tasks/{task_id}")
async def ws_task(websocket: WebSocket, task_id: str):
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


# ── Action endpoints ──────────────────────────────────────────────────


def _quarantine_dest(quarantine_root: Path, original_path: Path) -> Path:
    """Compute quarantine destination preserving absolute path structure."""
    rest = str(original_path).lstrip("/")
    return quarantine_root / rest


@router.post("/files/quarantine")
def quarantine_files(request: Request, body: QuarantineRequest) -> dict:
    """Move files to quarantine directory."""
    quarantine_root = Path(body.quarantine_root) if body.quarantine_root else _DEFAULT_QUARANTINE_ROOT
    cat = _open_catalog(request)
    moved = 0
    skipped = 0
    errors: list[str] = []
    try:
        for path_str in body.paths:
            p = Path(path_str)
            if not p.exists():
                skipped += 1
                errors.append(f"File not found on disk: {path_str}")
                continue
            row = cat.get_file_by_path(path_str)
            if row is None:
                skipped += 1
                errors.append(f"File not in catalog: {path_str}")
                continue
            dest = _quarantine_dest(quarantine_root, p)
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                if dest.exists():
                    suffix = 1
                    candidate = Path(f"{dest}.dup{suffix}")
                    while candidate.exists():
                        suffix += 1
                        candidate = Path(f"{dest}.dup{suffix}")
                    dest = candidate
                shutil.move(str(p), str(dest))
                cat.delete_file_by_path(path_str)
                moved += 1
            except OSError as e:
                errors.append(f"Failed to move {path_str}: {e}")
                skipped += 1
        cat.commit()
    finally:
        cat.close()
    return {"moved": moved, "skipped": skipped, "errors": errors}


@router.post("/files/delete")
def delete_files(request: Request, body: DeleteRequest) -> dict:
    """Permanently delete files."""
    cat = _open_catalog(request)
    deleted = 0
    skipped = 0
    errors: list[str] = []
    try:
        for path_str in body.paths:
            p = Path(path_str)
            if not p.exists():
                skipped += 1
                errors.append(f"File not found on disk: {path_str}")
                continue
            try:
                p.unlink()
                cat.delete_file_by_path(path_str)
                deleted += 1
            except OSError as e:
                errors.append(f"Failed to delete {path_str}: {e}")
                skipped += 1
        cat.commit()
    finally:
        cat.close()
    return {"deleted": deleted, "skipped": skipped, "errors": errors}


@router.post("/files/rename")
def rename_files(request: Request, body: RenameRequest) -> dict:
    """Rename files."""
    cat = _open_catalog(request)
    renamed = 0
    skipped = 0
    errors: list[str] = []
    try:
        for item in body.renames:
            p = Path(item.path)
            if not p.exists():
                skipped += 1
                errors.append(f"File not found: {item.path}")
                continue
            new_path = p.parent / item.new_name
            if new_path.exists():
                skipped += 1
                errors.append(f"Target already exists: {new_path}")
                continue
            try:
                p.rename(new_path)
                cat.update_file_path(item.path, str(new_path))
                renamed += 1
            except OSError as e:
                errors.append(f"Failed to rename {item.path}: {e}")
                skipped += 1
        cat.commit()
    finally:
        cat.close()
    return {"renamed": renamed, "skipped": skipped, "errors": errors}


@router.post("/files/move")
def move_files(request: Request, body: MoveRequest) -> dict:
    """Move files to a destination directory."""
    dest_dir = Path(body.destination)
    if not dest_dir.is_dir():
        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise HTTPException(status_code=400, detail=f"Cannot create destination: {e}") from e

    cat = _open_catalog(request)
    moved = 0
    skipped = 0
    errors: list[str] = []
    try:
        for path_str in body.paths:
            p = Path(path_str)
            if not p.exists():
                skipped += 1
                errors.append(f"File not found: {path_str}")
                continue
            new_path = dest_dir / p.name
            if new_path.exists():
                skipped += 1
                errors.append(f"Target already exists: {new_path}")
                continue
            try:
                shutil.move(str(p), str(new_path))
                cat.update_file_path(path_str, str(new_path))
                moved += 1
            except OSError as e:
                errors.append(f"Failed to move {path_str}: {e}")
                skipped += 1
        cat.commit()
    finally:
        cat.close()
    return {"moved": moved, "skipped": skipped, "errors": errors}


@router.post("/duplicates/{group_id}/quarantine")
def quarantine_duplicate_group(request: Request, group_id: str, body: DuplicateKeepRequest) -> dict:
    """Quarantine all files in a duplicate group except the keeper."""
    quarantine_root = _DEFAULT_QUARANTINE_ROOT
    cat = _open_catalog(request)
    quarantined = 0
    errors: list[str] = []
    try:
        group_data = cat.query_duplicates()
        group_rows = None
        for gid, rows in group_data:
            if gid == group_id:
                group_rows = rows
                break
        if group_rows is None:
            raise HTTPException(status_code=404, detail="Duplicate group not found")

        all_paths = [r.path for r in group_rows]
        if body.keep_path not in all_paths:
            raise HTTPException(status_code=400, detail="keep_path not in this duplicate group")

        for row in group_rows:
            if row.path == body.keep_path:
                continue
            p = Path(row.path)
            if not p.exists():
                errors.append(f"File not found on disk: {row.path}")
                continue
            dest = _quarantine_dest(quarantine_root, p)
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                if dest.exists():
                    suffix = 1
                    candidate = Path(f"{dest}.dup{suffix}")
                    while candidate.exists():
                        suffix += 1
                        candidate = Path(f"{dest}.dup{suffix}")
                    dest = candidate
                shutil.move(str(p), str(dest))
                cat.delete_file_by_path(row.path)
                quarantined += 1
            except OSError as e:
                errors.append(f"Failed to quarantine {row.path}: {e}")
        cat.commit()
    finally:
        cat.close()
    result: dict[str, Any] = {"quarantined": quarantined, "kept": body.keep_path}
    if errors:
        result["errors"] = errors
    return result


@router.post("/duplicates/{group_id}/merge")
def merge_duplicate_group(request: Request, group_id: str, body: DuplicateKeepRequest) -> dict:
    """Merge metadata from all copies to keeper, then quarantine the rest."""
    from ..metadata_merge import create_merge_plan, execute_merge
    from ..metadata_richness import compute_group_diff

    quarantine_root = _DEFAULT_QUARANTINE_ROOT
    cat = _open_catalog(request)
    quarantined = 0
    merge_applied = False
    errors: list[str] = []
    try:
        group_meta = cat.get_group_metadata(group_id)
        if not group_meta:
            raise HTTPException(status_code=404, detail="Duplicate group not found")

        all_paths_in_group = [path for path, _ in group_meta]
        if body.keep_path not in all_paths_in_group:
            raise HTTPException(status_code=400, detail="keep_path not in this duplicate group")

        # Attempt metadata merge if we have metadata for at least 2 files
        if len(group_meta) >= 2:
            try:
                survivor_meta: dict[str, Any] = {}
                for path, meta in group_meta:
                    if path == body.keep_path:
                        survivor_meta = meta
                        break

                diff = compute_group_diff(group_meta)
                plan = create_merge_plan(body.keep_path, survivor_meta, diff)
                if plan.actions:
                    merge_result = execute_merge(plan)
                    if merge_result.error:
                        errors.append(f"Merge error: {merge_result.error}")
                    else:
                        merge_applied = True
            except Exception as e:
                errors.append(f"Metadata merge failed: {e}")

        # Quarantine all but keeper
        group_data = cat.query_duplicates()
        group_rows = None
        for gid, rows in group_data:
            if gid == group_id:
                group_rows = rows
                break
        if group_rows is None:
            raise HTTPException(status_code=404, detail="Duplicate group not found")

        for row in group_rows:
            if row.path == body.keep_path:
                continue
            p = Path(row.path)
            if not p.exists():
                errors.append(f"File not found on disk: {row.path}")
                continue
            dest = _quarantine_dest(quarantine_root, p)
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                if dest.exists():
                    suffix = 1
                    candidate = Path(f"{dest}.dup{suffix}")
                    while candidate.exists():
                        suffix += 1
                        candidate = Path(f"{dest}.dup{suffix}")
                    dest = candidate
                shutil.move(str(p), str(dest))
                cat.delete_file_by_path(row.path)
                quarantined += 1
            except OSError as e:
                errors.append(f"Failed to quarantine {row.path}: {e}")
        cat.commit()
    finally:
        cat.close()
    result: dict[str, Any] = {
        "merged": merge_applied,
        "quarantined": quarantined,
        "kept": body.keep_path,
    }
    if errors:
        result["errors"] = errors
    return result


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


@router.post("/files/restore")
def restore_files(request: Request, body: RestoreRequest) -> dict:
    """Restore files from quarantine."""
    quarantine_root = Path(body.quarantine_root) if body.quarantine_root else _DEFAULT_QUARANTINE_ROOT
    cat = _open_catalog(request)
    restored = 0
    errors: list[str] = []
    try:
        for path_str in body.paths:
            original_path = Path(path_str)
            quarantined_path = _quarantine_dest(quarantine_root, original_path)
            if not quarantined_path.exists():
                errors.append(f"Not found in quarantine: {path_str}")
                continue
            if original_path.exists():
                errors.append(f"Original path already occupied: {path_str}")
                continue
            try:
                original_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(quarantined_path), str(original_path))
                restored += 1
            except OSError as e:
                errors.append(f"Failed to restore {path_str}: {e}")
    finally:
        cat.close()
    return {"restored": restored, "errors": errors}


# ── Filesystem browsing & roots ───────────────────────────────────────


def _is_path_allowed(p: Path) -> bool:
    """Check if a path is allowed to browse (security guard)."""
    resolved = str(p.resolve())
    return all(
        resolved != prefix and not resolved.startswith(prefix + "/")
        for prefix in _BLOCKED_PREFIXES
    )


def _get_bookmarks() -> list[dict]:
    """Return quick-access bookmark locations."""
    home = Path.home()
    bookmarks = [
        {"name": "Plocha", "path": str(home / "Desktop"), "icon": "\U0001f5a5"},
        {"name": "Obr\u00e1zky", "path": str(home / "Pictures"), "icon": "\U0001f5bc"},
        {"name": "Dokumenty", "path": str(home / "Documents"), "icon": "\U0001f4c1"},
        {"name": "Sta\u017een\u00e9", "path": str(home / "Downloads"), "icon": "\U0001f4e5"},
        {"name": "Domovsk\u00e1 slo\u017eka", "path": str(home), "icon": "\U0001f3e0"},
    ]
    # Detect mounted volumes
    volumes_path = Path("/Volumes")
    if volumes_path.exists():
        try:
            for entry in sorted(volumes_path.iterdir()):
                if entry.is_dir() and entry.name != "Macintosh HD":
                    bookmarks.append({
                        "name": entry.name,
                        "path": str(entry),
                        "icon": "\U0001f4be",
                    })
        except PermissionError:
            pass
    return bookmarks


@router.get("/browse")
def browse_filesystem(
    path: str | None = Query(default=None),
) -> dict:
    """Browse filesystem directories for folder picker."""
    browse_path = Path(path).resolve() if path else Path.home()

    if not _is_path_allowed(browse_path):
        raise HTTPException(status_code=403, detail="Access to this path is not allowed")

    if not browse_path.exists():
        raise HTTPException(status_code=404, detail="Path does not exist")

    if not browse_path.is_dir():
        raise HTTPException(status_code=400, detail="Path is not a directory")

    entries: list[dict] = []
    try:
        for entry in sorted(browse_path.iterdir(), key=lambda e: e.name.lower()):
            if entry.name.startswith("."):
                continue
            if not entry.is_dir():
                continue
            if not _is_path_allowed(entry):
                continue
            item_count = 0
            with contextlib.suppress(PermissionError):
                item_count = sum(1 for _ in entry.iterdir())
            entries.append({
                "name": entry.name,
                "path": str(entry),
                "is_dir": True,
                "item_count": item_count,
            })
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied") from None

    parent = str(browse_path.parent) if browse_path != browse_path.parent else None

    return {
        "current": str(browse_path),
        "parent": parent,
        "entries": entries,
        "bookmarks": _get_bookmarks(),
    }


def _get_configured_roots(request: Request) -> list[str]:
    """Read configured_roots from catalog meta table."""
    cat = _open_catalog(request)
    try:
        cur = cat.conn.execute("SELECT value FROM meta WHERE key = 'configured_roots'")
        row = cur.fetchone()
        if row is None:
            return []
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return []
    finally:
        cat.close()


def _set_configured_roots(request: Request, roots: list[str]) -> None:
    """Write configured_roots to catalog meta table."""
    cat = _open_catalog(request)
    try:
        cat.conn.execute(
            "INSERT INTO meta (key, value) VALUES ('configured_roots', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (json.dumps(roots),),
        )
        cat.conn.commit()
    finally:
        cat.close()


@router.get("/roots")
def get_roots(request: Request) -> dict:
    """Get saved media root folders."""
    roots = _get_configured_roots(request)
    return {"roots": roots}


@router.post("/roots")
def save_roots(request: Request, body: RootsRequest) -> dict:
    """Save media root folders."""
    # Validate paths exist
    valid_roots = []
    for root in body.roots:
        p = Path(root)
        if p.exists() and p.is_dir():
            valid_roots.append(str(p.resolve()))
    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_roots: list[str] = []
    for r in valid_roots:
        if r not in seen:
            seen.add(r)
            unique_roots.append(r)
    _set_configured_roots(request, unique_roots)
    return {"saved": True, "roots": unique_roots}


@router.delete("/roots")
def remove_root(request: Request, body: RemoveRootRequest) -> dict:
    """Remove a specific root folder."""
    roots = _get_configured_roots(request)
    path_to_remove = str(Path(body.path).resolve())
    roots = [r for r in roots if r != path_to_remove]
    _set_configured_roots(request, roots)
    return {"removed": True, "roots": roots}


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
