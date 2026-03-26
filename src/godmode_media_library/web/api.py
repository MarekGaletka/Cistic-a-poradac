"""REST API endpoints for GOD MODE Media Library."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import io
import json
import logging
import shutil
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..catalog import Catalog

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator

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


class FavoriteRequest(BaseModel):
    path: str


class NoteRequest(BaseModel):
    note: str


class RatingRequest(BaseModel):
    rating: int


class CreateTagRequest(BaseModel):
    name: str
    color: str = "#58a6ff"


class DedupRulesRequest(BaseModel):
    strategy: str = "richness"
    similarity_threshold: int = 10
    auto_resolve: bool = False
    merge_metadata: bool = True
    quarantine_path: str = ""
    exclude_extensions: list[str] = []
    exclude_paths: list[str] = []
    min_file_size_kb: int = 0


class TagFilesRequest(BaseModel):
    paths: list[str]
    tag_id: int


class CreateShareRequest(BaseModel):
    path: str
    label: str = ""
    password: str | None = None
    expires_hours: float | None = None
    max_downloads: int | None = None


class ReorganizeConfigRequest(BaseModel):
    sources: list[str]
    destination: str
    structure_pattern: str = "year_month"
    deduplicate: bool = True
    merge_metadata: bool = True
    delete_originals: bool = False
    dry_run: bool = True
    workers: int = 4
    exclude_patterns: list[str] = []


class ReorganizeExecuteRequest(BaseModel):
    plan_id: str
    delete_originals: bool = False


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
_reorganize_plans: dict[str, Any] = {}

_ws_connections: dict[str, list[WebSocket]] = {}
_ws_lock = threading.Lock()


def _evict_old_tasks() -> None:
    """Remove completed/failed tasks older than TTL. Must be called under _tasks_lock."""
    now = time.monotonic()
    to_remove = [tid for tid, t in _tasks.items() if t.status in ("completed", "failed") and (now - t._created_ts) > _TASK_TTL_SECONDS]
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


_event_loop: asyncio.AbstractEventLoop | None = None


def _capture_event_loop() -> None:
    """Capture the running event loop at startup for cross-thread WS dispatch."""
    global _event_loop
    _event_loop = asyncio.get_running_loop()


def _notify_ws(task_id: str, msg: dict) -> None:
    """Best-effort broadcast to all WebSocket connections for a task."""
    loop = _event_loop
    if loop is None:
        return
    with _ws_lock:
        conns = _ws_connections.get(task_id, [])
        if not conns:
            return
        conns_snapshot = list(conns)
    stale: list[WebSocket] = []
    for ws in conns_snapshot:
        try:
            asyncio.run_coroutine_threadsafe(ws.send_json(msg), loop)
        except (RuntimeError, OSError):
            stale.append(ws)
    if stale:
        with _ws_lock:
            conns = _ws_connections.get(task_id, [])
            for ws in stale:
                if ws in conns:
                    conns.remove(ws)


def _update_progress(task_id: str, progress: dict) -> None:
    msg = None
    with _tasks_lock:
        if task_id in _tasks:
            _tasks[task_id].progress = progress
            msg = _task_to_msg(_tasks[task_id])
    if msg is not None:
        _notify_ws(task_id, msg)


def _finish_task(task_id: str, result: dict | None = None, error: str | None = None) -> None:
    msg = None
    with _tasks_lock:
        if task_id in _tasks:
            _tasks[task_id].status = "failed" if error else "completed"
            _tasks[task_id].result = result
            _tasks[task_id].error = error
            _tasks[task_id].finished_at = datetime.now(timezone.utc).isoformat()
            msg = _task_to_msg(_tasks[task_id])
    if msg is not None:
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
    favorites_only: bool | None = None,
    tag_id: int | None = None,
    min_rating: int | None = None,
    has_notes: bool | None = None,
    quality_category: str | None = None,
    sort: str | None = None,
    order: str | None = None,
    limit: int = Query(default=500, le=10000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Query files with filters."""
    cat = _open_catalog(request)
    try:
        favs = _get_favorites_set(request)

        def _enrich_items(items_list):
            """Enrich file rows with duplicates, favorites, tags, ratings, notes."""
            paths = [r.path for r in items_list]
            dup_map = cat.get_duplicate_group_ids_for_paths(paths)
            tags_map = cat.get_files_tags_bulk(paths)
            ratings_map = cat.get_files_ratings_bulk(paths)
            notes_set = cat.get_files_notes_bulk(paths)
            result = []
            for r in items_list:
                d = _row_to_dict(r)
                d["duplicate_group_id"] = dup_map.get(r.path)
                d["is_favorite"] = r.path in favs
                d["tags"] = tags_map.get(r.path, [])
                d["rating"] = ratings_map.get(r.path)
                d["has_note"] = r.path in notes_set
                result.append(d)
            return result

        # Determine if we need full-scan mode (rating/notes filters or tag/favorites)
        needs_full_scan = tag_id is not None or favorites_only or min_rating is not None or has_notes

        if needs_full_scan:
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
                quality_category=quality_category,
                sort=sort,
                order=order,
                limit=100000,
                offset=0,
            )
            # Apply tag filter
            if tag_id is not None:
                tag_rows = cat.query_files_by_tag(tag_id, limit=100000, offset=0)
                tag_paths = {r.path for r in tag_rows}
                rows = [r for r in rows if r.path in tag_paths]

            # Apply favorites filter
            if favorites_only:
                rows = [r for r in rows if r.path in favs]

            # Apply rating filter
            if min_rating is not None:
                all_paths = [r.path for r in rows]
                ratings_map = cat.get_files_ratings_bulk(all_paths)
                rows = [r for r in rows if ratings_map.get(r.path, 0) >= min_rating]

            # Apply notes filter
            if has_notes:
                all_paths = [r.path for r in rows]
                notes_set = cat.get_files_notes_bulk(all_paths)
                rows = [r for r in rows if r.path in notes_set]

            total = len(rows)
            items = rows[offset : offset + limit]
            has_more = (offset + limit) < total
        else:
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
                quality_category=quality_category,
                sort=sort,
                order=order,
                limit=limit + 1,
                offset=offset,
            )
            has_more = len(rows) > limit
            items = rows[:limit]

        file_dicts = _enrich_items(items)
        return {
            "files": file_dicts,
            "count": len(items),
            "has_more": has_more,
        }
    finally:
        cat.close()


@router.post("/files/favorite")
def toggle_favorite(request: Request, body: FavoriteRequest) -> dict:
    """Toggle favorite status for a file."""
    favorites = _get_favorites_list(request)
    path = body.path
    if path in favorites:
        favorites.remove(path)
        is_favorite = False
    else:
        favorites.append(path)
        is_favorite = True
    _set_favorites(request, favorites)
    return {"path": path, "is_favorite": is_favorite}


@router.get("/files/favorites")
def list_favorites(request: Request) -> dict:
    """List all favorited file paths."""
    favorites = _get_favorites_list(request)
    return {"favorites": favorites, "count": len(favorites)}


# ── Notes ────────────────────────────────────────────────────────────
# These must be registered before the catch-all /files/{file_path:path}


@router.get("/files/{file_path:path}/note")
def get_file_note(request: Request, file_path: str) -> dict:
    """Get note for a file."""
    cat = _open_catalog(request)
    try:
        result = cat.get_file_note(f"/{file_path}")
        if result is None:
            return {"note": None, "updated_at": None}
        return {"note": result[0], "updated_at": result[1]}
    finally:
        cat.close()


@router.put("/files/{file_path:path}/note")
def set_file_note(request: Request, file_path: str, body: NoteRequest) -> dict:
    """Set or update a note for a file."""
    cat = _open_catalog(request)
    try:
        cat.set_file_note(f"/{file_path}", body.note)
        return {"saved": True}
    finally:
        cat.close()


@router.delete("/files/{file_path:path}/note")
def delete_file_note(request: Request, file_path: str) -> dict:
    """Remove a note from a file."""
    cat = _open_catalog(request)
    try:
        deleted = cat.delete_file_note(f"/{file_path}")
        return {"deleted": deleted}
    finally:
        cat.close()


# ── Ratings ──────────────────────────────────────────────────────────


@router.put("/files/{file_path:path}/rating")
def set_file_rating(request: Request, file_path: str, body: RatingRequest) -> dict:
    """Set a rating (1-5) for a file."""
    if body.rating < 1 or body.rating > 5:
        raise HTTPException(status_code=400, detail="Rating must be between 1 and 5")
    cat = _open_catalog(request)
    try:
        cat.set_file_rating(f"/{file_path}", body.rating)
        return {"saved": True, "rating": body.rating}
    finally:
        cat.close()


@router.delete("/files/{file_path:path}/rating")
def delete_file_rating(request: Request, file_path: str) -> dict:
    """Clear a rating from a file."""
    cat = _open_catalog(request)
    try:
        deleted = cat.delete_file_rating(f"/{file_path}")
        return {"deleted": deleted}
    finally:
        cat.close()


# ── File detail (catch-all, must be after /note and /rating) ─────────


@router.get("/files/{file_path:path}")
def get_file_detail(request: Request, file_path: str) -> dict:
    """Get file details including deep metadata."""
    cat = _open_catalog(request)
    try:
        path = f"/{file_path}"
        row = cat.get_file_by_path(path)
        if row is None:
            raise HTTPException(status_code=404, detail="File not found in catalog")
        meta = cat.get_file_metadata(path)
        richness = cat.get_metadata_richness(path)
        tags = cat.get_file_tags(path)
        note_data = cat.get_file_note(path)
        rating = cat.get_file_rating(path)
        return {
            "file": _row_to_dict(row),
            "metadata": meta,
            "richness": richness,
            "tags": tags,
            "note": note_data[0] if note_data else None,
            "note_updated_at": note_data[1] if note_data else None,
            "rating": rating,
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
            groups.append(
                {
                    "group_id": gid,
                    "file_count": len(rows),
                    "total_size": sum(r.size for r in rows),
                    "files": [{"path": str(r.path), "size": r.size} for r in rows],
                }
            )
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
            "files": [{"path": path, "metadata": meta} for path, meta in group_meta],
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


@router.get("/memories")
def get_memories(request: Request) -> dict:
    """Get photos from this day in previous years (On This Day)."""
    from datetime import date

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


@router.get("/system-info")
def get_system_info(request: Request) -> dict:
    """System information for the Doctor page."""
    import platform
    import sys

    cat = _open_catalog(request)
    try:
        stats = cat.stats()
        cat_path = request.app.state.catalog_path
        cat_size = cat_path.stat().st_size if cat_path.exists() else 0
        quarantine_path = Path.home() / ".config" / "gml" / "quarantine"
        quarantine_size = sum(f.stat().st_size for f in quarantine_path.rglob("*") if f.is_file()) if quarantine_path.exists() else 0
        return {
            "python_version": sys.version,
            "platform": platform.platform(),
            "catalog_path": str(cat_path),
            "catalog_size": cat_size,
            "total_files": stats.get("total_files", 0),
            "total_size": stats.get("total_size_bytes", 0),
            "quarantine_size": quarantine_size,
            "last_scan_root": stats.get("last_scan_root", ""),
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


def _thumb_cache_dir() -> Path:
    """Return the persistent thumbnail cache directory."""
    return Path.home() / ".config" / "gml" / "cache" / "thumbnails"


def _thumb_cache_key(path: str, size: int) -> str:
    """Deterministic cache key from file path + size."""
    return hashlib.sha256(f"{path}:{size}".encode()).hexdigest()


def _thumb_cache_get(path: str, size: int) -> bytes | None:
    """Read cached thumbnail bytes or None."""
    cache_dir = _thumb_cache_dir()
    cache_file = cache_dir / f"{_thumb_cache_key(path, size)}.jpg"
    if cache_file.exists():
        return cache_file.read_bytes()
    return None


def _thumb_cache_put(path: str, size: int, data: bytes) -> None:
    """Write thumbnail bytes to cache."""
    cache_dir = _thumb_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{_thumb_cache_key(path, size)}.jpg"
    cache_file.write_bytes(data)


@router.get("/thumbnail/{file_path:path}")
def get_thumbnail(request: Request, file_path: str, size: int = Query(default=200, le=800)) -> StreamingResponse:
    """Generate and serve a thumbnail for an image file. Uses persistent disk cache."""
    full_path = Path(f"/{file_path}").resolve()

    # Security: verify the file is within the catalog (exists in DB)
    cat = _open_catalog(request)
    try:
        row = cat.get_file_by_path(str(full_path))
        if row is None:
            raise HTTPException(status_code=404, detail="File not found in catalog")
    finally:
        cat.close()

    # Try cached thumbnail first (works even when source disk is offline)
    cached = _thumb_cache_get(str(full_path), size)
    if cached is not None:
        return StreamingResponse(
            io.BytesIO(cached),
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    if not full_path.exists():
        raise HTTPException(status_code=404, detail="File not found on disk")

    ext = full_path.suffix.lower()
    image_exts = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".gif", ".webp", ".heic", ".heif"}
    video_exts = {".mp4", ".mov", ".avi", ".mkv", ".wmv", ".flv", ".webm", ".m4v", ".3gp"}

    if ext not in image_exts and ext not in video_exts:
        raise HTTPException(status_code=400, detail="Not a supported media file")

    try:
        from PIL import Image
    except ImportError:
        raise HTTPException(status_code=500, detail="Pillow not installed") from None

    # Video thumbnail: extract a frame with ffmpeg
    if ext in video_exts:
        try:
            import subprocess
            import tempfile

            from ..deps import resolve_bin

            _ffmpeg = resolve_bin("ffmpeg") or "ffmpeg"

            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                tmp_path = tmp.name

            try:
                result = subprocess.run(
                    [
                        _ffmpeg,
                        "-y",
                        "-i",
                        str(full_path),
                        "-ss",
                        "00:00:01",
                        "-frames:v",
                        "1",
                        "-vf",
                        f"scale={size}:{size}:force_original_aspect_ratio=decrease",
                        tmp_path,
                    ],
                    capture_output=True,
                    timeout=10,
                )
                if result.returncode != 0 or not Path(tmp_path).exists():
                    raise HTTPException(status_code=500, detail="Failed to extract video frame")

                with Image.open(tmp_path) as img:
                    buf = io.BytesIO()
                    img.save(buf, format="JPEG", quality=80)
                    thumb_bytes = buf.getvalue()
            finally:
                Path(tmp_path).unlink(missing_ok=True)
            _thumb_cache_put(str(full_path), size, thumb_bytes)
            return StreamingResponse(
                io.BytesIO(thumb_bytes),
                media_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=86400"},
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.warning("Video thumbnail failed for %s: %s", full_path, e)
            raise HTTPException(status_code=500, detail="Failed to generate video thumbnail") from e

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
            thumb_bytes = buf.getvalue()
            _thumb_cache_put(str(full_path), size, thumb_bytes)
            return StreamingResponse(
                io.BytesIO(thumb_bytes),
                media_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=86400"},
            )
    except (OSError, ValueError) as e:
        logger.warning("Thumbnail generation failed for %s: %s", full_path, e)
        raise HTTPException(status_code=500, detail="Failed to generate thumbnail") from e


@router.get("/preview/{file_path:path}")
def get_preview(request: Request, file_path: str, size: int = Query(default=120, le=400)) -> StreamingResponse:
    """Generate a thumbnail preview for any file on disk (no catalog check).

    Used by recovery/app-mine to preview files not yet in the catalog.
    Only serves image thumbnails — no video frame extraction for speed.
    """
    full_path = Path(f"/{file_path}").resolve()

    # Security: block access to sensitive system directories
    full_str = str(full_path)
    if any(full_str == prefix or full_str.startswith(prefix + "/") for prefix in _BLOCKED_PREFIXES):
        raise HTTPException(status_code=403, detail="Access denied")

    # Also verify file is within one of the configured scan roots
    scan_roots = getattr(request.app.state, "scan_roots", None)
    if scan_roots:
        resolved_roots = [Path(r).resolve() for r in scan_roots]
        if not any(full_path == root or str(full_path).startswith(str(root) + "/") for root in resolved_roots):
            raise HTTPException(status_code=403, detail="File outside allowed roots")

    if not full_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    ext = full_path.suffix.lower()
    image_exts = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".gif", ".webp", ".heic", ".heif"}

    if ext not in image_exts:
        raise HTTPException(status_code=400, detail="Not a supported image")

    try:
        from PIL import Image
    except ImportError:
        raise HTTPException(status_code=500, detail="Pillow not installed") from None

    if ext in (".heic", ".heif"):
        try:
            import pillow_heif

            pillow_heif.register_heif_opener()
        except ImportError:
            raise HTTPException(status_code=400, detail="pillow-heif required") from None

    try:
        with Image.open(full_path) as img:
            img.thumbnail((size, size), Image.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=75)
            buf.seek(0)
            return StreamingResponse(
                buf,
                media_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=3600"},
            )
    except (OSError, ValueError):
        raise HTTPException(status_code=500, detail="Preview failed") from None


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
    from ..scanner import backfill_metadata_from_stored, _backfill_dates_from_filesystem

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
            from ..catalog import Catalog
            from ..verify import verify_catalog

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
            if not conns and task_id in _ws_connections:
                del _ws_connections[task_id]


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
    return all(resolved != prefix and not resolved.startswith(prefix + "/") for prefix in _BLOCKED_PREFIXES)


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
                    bookmarks.append(
                        {
                            "name": entry.name,
                            "path": str(entry),
                            "icon": "\U0001f4be",
                        }
                    )
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
            entries.append(
                {
                    "name": entry.name,
                    "path": str(entry),
                    "is_dir": True,
                    "item_count": item_count,
                }
            )
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
            "INSERT INTO meta (key, value) VALUES ('configured_roots', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
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


@router.get("/sources")
def get_sources(request: Request) -> dict:
    """Check availability of all configured roots and scanned root prefixes.

    Returns each source with online/offline status and file counts.
    Works without requiring the source to be currently mounted.
    """
    cat = _open_catalog(request)
    try:
        # Gather roots from config + distinct path prefixes from catalog
        configured = _get_configured_roots(request)

        # Also discover roots from scan history
        scan_roots: list[str] = []
        try:
            for row in cat.conn.execute("SELECT DISTINCT root FROM scans"):
                for r in (row[0] or "").split(";"):
                    r = r.strip()
                    if r:
                        scan_roots.append(r)
        except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
            logger.debug("Failed to read scan roots from catalog: %s", exc)

        all_roots = list(dict.fromkeys(configured + scan_roots))  # dedupe, preserve order

        sources = []
        for root in all_roots:
            root_path = Path(root)
            online = root_path.exists() and root_path.is_dir()

            # Count files in catalog under this root
            count_row = cat.conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files WHERE path LIKE ? || '%'",
                (root.rstrip("/"),),
            ).fetchone()
            file_count = count_row[0] if count_row else 0
            total_size = count_row[1] if count_row else 0

            # Last scan time for this root
            last_scan_row = cat.conn.execute(
                "SELECT MAX(finished_at) FROM scans WHERE root LIKE '%' || ? || '%'",
                (root,),
            ).fetchone()
            last_scan = last_scan_row[0] if last_scan_row else None

            sources.append(
                {
                    "path": root,
                    "name": root_path.name or root,
                    "online": online,
                    "file_count": file_count,
                    "total_size": total_size,
                    "last_scan": last_scan,
                    "configured": root in configured,
                }
            )

        # Thumbnail cache stats
        cache_dir = _thumb_cache_dir()
        cache_count = 0
        cache_size = 0
        if cache_dir.exists():
            for f in cache_dir.iterdir():
                if f.suffix == ".jpg":
                    cache_count += 1
                    cache_size += f.stat().st_size

        return {
            "sources": sources,
            "thumbnail_cache": {
                "path": str(cache_dir),
                "count": cache_count,
                "size": cache_size,
            },
        }
    finally:
        cat.close()


# ── Video streaming ───────────────────────────────────────────────────


@router.get("/stream/{file_path:path}")
def stream_file(request: Request, file_path: str) -> StreamingResponse:
    """Stream a media file for preview."""
    full_path = Path(f"/{file_path}").resolve()
    cat = _open_catalog(request)
    try:
        row = cat.get_file_by_path(str(full_path))
        if row is None:
            raise HTTPException(status_code=404, detail="Not in catalog")
    finally:
        cat.close()
    if not full_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    media_types = {
        ".mp4": "video/mp4",
        ".webm": "video/webm",
        ".mov": "video/quicktime",
        ".avi": "video/x-msvideo",
        ".mkv": "video/x-matroska",
        ".mp3": "audio/mpeg",
        ".wav": "audio/wav",
        ".flac": "audio/flac",
        ".ogg": "audio/ogg",
        ".m4a": "audio/mp4",
        ".aac": "audio/aac",
        ".wma": "audio/x-ms-wma",
        ".pdf": "application/pdf",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".bmp": "image/bmp",
        ".tiff": "image/tiff",
        ".tif": "image/tiff",
        ".heic": "image/heic",
        ".heif": "image/heif",
        ".svg": "image/svg+xml",
    }
    ext = full_path.suffix.lower()
    media_type = media_types.get(ext, "application/octet-stream")

    from fastapi.responses import FileResponse

    return FileResponse(
        str(full_path),
        media_type=media_type,
        headers={"Accept-Ranges": "bytes", "Cache-Control": "public, max-age=86400"},
    )


# ── Favorites ─────────────────────────────────────────────────────────


def _get_favorites_list(request: Request) -> list[str]:
    """Read favorites from catalog meta table."""
    cat = _open_catalog(request)
    try:
        cur = cat.conn.execute("SELECT value FROM meta WHERE key = 'favorites'")
        row = cur.fetchone()
        if row is None:
            return []
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return []
    finally:
        cat.close()


def _get_favorites_set(request: Request) -> set[str]:
    """Read favorites as a set for fast lookup."""
    return set(_get_favorites_list(request))


def _set_favorites(request: Request, favorites: list[str]) -> None:
    """Write favorites to catalog meta table."""
    cat = _open_catalog(request)
    try:
        cat.conn.execute(
            "INSERT INTO meta (key, value) VALUES ('favorites', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (json.dumps(favorites),),
        )
        cat.conn.commit()
    finally:
        cat.close()


# ── Tags ──────────────────────────────────────────────────────────────


@router.get("/tags")
def list_tags(request: Request) -> dict:
    """List all tags with file counts."""
    cat = _open_catalog(request)
    try:
        tags = cat.get_all_tags()
        return {"tags": tags}
    finally:
        cat.close()


@router.post("/tags")
def create_tag(request: Request, body: CreateTagRequest) -> dict:
    """Create a new tag."""
    cat = _open_catalog(request)
    try:
        tag = cat.add_tag(body.name, body.color)
        return tag
    except Exception as e:
        if "UNIQUE" in str(e):
            raise HTTPException(status_code=409, detail="Tag name already exists") from e
        raise
    finally:
        cat.close()


@router.delete("/tags/{tag_id}")
def delete_tag(request: Request, tag_id: int) -> dict:
    """Delete a tag."""
    cat = _open_catalog(request)
    try:
        cat.delete_tag(tag_id)
        return {"deleted": True}
    finally:
        cat.close()


@router.post("/files/tag")
def tag_files(request: Request, body: TagFilesRequest) -> dict:
    """Add a tag to files."""
    cat = _open_catalog(request)
    try:
        count = cat.bulk_tag(body.paths, body.tag_id)
        return {"tagged": count}
    finally:
        cat.close()


@router.delete("/files/tag")
def untag_files(request: Request, body: TagFilesRequest) -> dict:
    """Remove a tag from files."""
    cat = _open_catalog(request)
    try:
        count = cat.bulk_untag(body.paths, body.tag_id)
        return {"untagged": count}
    finally:
        cat.close()


# ── Shares ─────────────────────────────────────────────────────────


@router.post("/shares")
def create_share(request: Request, body: CreateShareRequest) -> dict:
    """Create a share link for a file."""
    cat = _open_catalog(request)
    try:
        share = cat.create_share(
            path=body.path,
            label=body.label,
            password=body.password,
            expires_hours=body.expires_hours,
            max_downloads=body.max_downloads,
        )
        return share
    except ValueError as e:
        logger.warning("Share creation failed: %s", e)
        raise HTTPException(status_code=404, detail="File not found") from e
    finally:
        cat.close()


@router.get("/shares")
def list_shares(request: Request, limit: int = 100, offset: int = 0) -> dict:
    """List all shares."""
    cat = _open_catalog(request)
    try:
        shares = cat.get_all_shares(limit=limit, offset=offset)
        return {"shares": shares}
    finally:
        cat.close()


@router.get("/shares/file")
def shares_for_file(request: Request, path: str = Query(...)) -> dict:
    """List shares for a specific file."""
    cat = _open_catalog(request)
    try:
        shares = cat.get_shares_for_file(path)
        return {"shares": shares}
    finally:
        cat.close()


@router.delete("/shares/{share_id}")
def revoke_share(request: Request, share_id: int) -> dict:
    """Revoke/delete a share link."""
    cat = _open_catalog(request)
    try:
        cat.delete_share(share_id)
        return {"deleted": True}
    finally:
        cat.close()


# ── Tag suggestions ────────────────────────────────────────────────


@router.get("/tags/suggest")
def suggest_tags(request: Request, path: str = Query(...)) -> dict:
    """Suggest tags based on file metadata."""
    cat = _open_catalog(request)
    try:
        file_row = cat.get_file_by_path(path)
        if file_row is None:
            raise HTTPException(status_code=404, detail="File not found")

        suggestions: list[dict] = []
        ext = (file_row.ext or "").lower()

        # Camera model
        if file_row.camera_model:
            cam = file_row.camera_model.strip()
            if file_row.camera_make:
                cam = f"{file_row.camera_make.strip()} {cam}"
            suggestions.append({"name": cam, "color": "#58a6ff", "reason": "Fotoaparat"})

        # Year from date_original
        if file_row.date_original:
            year = file_row.date_original[:4]
            if year.isdigit() and 1900 <= int(year) <= 2100:
                suggestions.append({"name": year, "color": "#d29922", "reason": "Rok porizeni"})

        # File type category
        image_exts = {"jpg", "jpeg", "png", "gif", "bmp", "tiff", "tif", "webp", "heic", "heif", "svg", "raw", "cr2", "nef", "arw", "dng"}
        video_exts = {"mp4", "mov", "avi", "mkv", "wmv", "flv", "webm", "m4v", "3gp"}
        doc_exts = {"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp", "rtf", "epub"}
        if ext in image_exts:
            suggestions.append({"name": "Fotky", "color": "#3fb950", "reason": "Typ souboru"})
        elif ext in video_exts:
            suggestions.append({"name": "Videa", "color": "#f0883e", "reason": "Typ souboru"})
        elif ext in doc_exts:
            suggestions.append({"name": "Dokumenty", "color": "#bc8cff", "reason": "Typ souboru"})

        # GPS
        if file_row.gps_latitude and file_row.gps_longitude:
            suggestions.append({"name": "S GPS", "color": "#58a6ff", "reason": "GPS souradnice"})

        # Check for faces
        faces = cat.get_faces_for_file_by_path(path) if hasattr(cat, "get_faces_for_file_by_path") else []
        if not faces:
            # Try alternative method
            fr = cat.get_file_by_path(path)
            if fr:
                faces_cur = cat.conn.execute("SELECT COUNT(*) FROM faces WHERE file_id = ?", (fr.id,))
                face_count = faces_cur.fetchone()[0]
                if face_count > 0:
                    suggestions.append({"name": "Portrety", "color": "#f85149", "reason": "Detekce obliceju"})

        # Rating
        rating_row = cat.conn.execute(
            "SELECT fr.rating FROM file_ratings fr JOIN files f ON fr.file_id = f.id WHERE f.path = ?",
            (path,),
        ).fetchone()
        if rating_row and rating_row[0] >= 4:
            suggestions.append({"name": "Oblibene", "color": "#d29922", "reason": "Vysoke hodnoceni"})

        # Filter out tags that already exist on the file
        existing_tags = cat.get_file_tags(path)
        existing_names = {t["name"].lower() for t in existing_tags}
        suggestions = [s for s in suggestions if s["name"].lower() not in existing_names]

        return {"suggestions": suggestions}
    finally:
        cat.close()


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


# ── File preview ─────────────────────────────────────────────────────────


_TEXT_EXTS = {
    ".txt",
    ".md",
    ".csv",
    ".json",
    ".xml",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".log",
    ".sh",
    ".bash",
    ".py",
    ".js",
    ".ts",
    ".html",
    ".css",
    ".sql",
    ".r",
    ".swift",
    ".go",
    ".rs",
    ".java",
    ".kt",
    ".c",
    ".cpp",
    ".h",
    ".hpp",
    ".rb",
    ".php",
    ".pl",
    ".lua",
    ".vim",
    ".env",
    ".gitignore",
    ".dockerfile",
    ".makefile",
}

_TEXT_NAMES = {"makefile", "dockerfile", "readme", "license", "changelog"}

_AUDIO_EXTS = {".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a"}

_ARCHIVE_EXTS = {".zip", ".tar", ".gz", ".bz2", ".xz", ".7z", ".rar"}


def _detect_language(ext: str) -> str:
    lang_map = {
        ".py": "python",
        ".js": "javascript",
        ".ts": "typescript",
        ".html": "html",
        ".css": "css",
        ".json": "json",
        ".xml": "xml",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".sql": "sql",
        ".sh": "bash",
        ".bash": "bash",
        ".md": "markdown",
        ".csv": "csv",
        ".go": "go",
        ".rs": "rust",
        ".java": "java",
        ".c": "c",
        ".cpp": "cpp",
        ".rb": "ruby",
        ".php": "php",
        ".swift": "swift",
    }
    return lang_map.get(ext, "text")


@router.get("/config/dedup-rules")
async def get_dedup_rules(request: Request):
    """Get current deduplication rules."""
    from ..config import load_config

    config = load_config()
    return {
        "strategy": config.dedup_strategy,
        "similarity_threshold": config.dedup_similarity_threshold,
        "auto_resolve": config.dedup_auto_resolve,
        "merge_metadata": config.dedup_merge_metadata,
        "quarantine_path": config.dedup_quarantine_path,
        "exclude_extensions": config.dedup_exclude_extensions,
        "exclude_paths": config.dedup_exclude_paths,
        "min_file_size_kb": config.dedup_min_file_size_kb,
    }


@router.put("/config/dedup-rules")
async def put_dedup_rules(request: Request, body: DedupRulesRequest):
    """Update deduplication rules. Saves to global config.toml."""
    import tomllib

    from ..config import _global_config_path

    config_path = _global_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)

    # Read existing config
    existing = {}
    if config_path.is_file():
        with config_path.open("rb") as f:
            existing = tomllib.load(f)

    # Update dedup fields
    existing["dedup_strategy"] = body.strategy
    existing["dedup_similarity_threshold"] = body.similarity_threshold
    existing["dedup_auto_resolve"] = body.auto_resolve
    existing["dedup_merge_metadata"] = body.merge_metadata
    existing["dedup_quarantine_path"] = body.quarantine_path
    existing["dedup_exclude_extensions"] = body.exclude_extensions
    existing["dedup_exclude_paths"] = body.exclude_paths
    existing["dedup_min_file_size_kb"] = body.min_file_size_kb

    # Write back as TOML
    lines = []
    for key, value in existing.items():
        if isinstance(value, bool):
            lines.append(f"{key} = {'true' if value else 'false'}")
        elif isinstance(value, str):
            lines.append(f'{key} = "{value}"')
        elif isinstance(value, list):
            items = ", ".join(f'"{v}"' for v in value)
            lines.append(f"{key} = [{items}]")
        else:
            lines.append(f"{key} = {value}")

    config_path.write_text("\n".join(lines) + "\n")

    return {"status": "ok"}


@router.get("/reorganize/sources")
def get_reorganize_sources():
    """Detect available media sources (mounted volumes, common folders)."""
    from ..reorganize import detect_sources

    return {"sources": detect_sources()}


@router.post("/reorganize/plan")
def start_reorganize_plan(request: Request, background_tasks: BackgroundTasks, config: ReorganizeConfigRequest):
    """Start planning reorganization (background task)."""
    from ..reorganize import ReorganizeConfig, plan_reorganization

    task = _create_task("reorganize-plan")

    def run_plan():
        try:
            rc = ReorganizeConfig(
                sources=[Path(s) for s in config.sources],
                destination=Path(config.destination),
                structure_pattern=config.structure_pattern,
                deduplicate=config.deduplicate,
                merge_metadata=config.merge_metadata,
                delete_originals=config.delete_originals,
                dry_run=True,  # planning is always dry
                workers=config.workers,
                exclude_patterns=config.exclude_patterns,
            )
            cat_path = request.app.state.catalog_path

            def on_progress(info):
                _update_progress(task.id, info)

            plan = plan_reorganization(rc, catalog_path=cat_path, progress_fn=on_progress)

            # Store plan for later execution
            _reorganize_plans[task.id] = plan

            # Build summary for the client
            summary = {
                "total_files": plan.total_files,
                "unique_files": plan.unique_files,
                "duplicate_files": plan.duplicate_files,
                "total_size": plan.total_size,
                "unique_size": plan.unique_size,
                "duplicate_size": plan.duplicate_size,
                "categories": plan.categories,
                "source_stats": {str(k): v for k, v in plan.source_stats.items()},
                "errors": plan.errors[:50],
                "plan_id": task.id,
            }
            _finish_task(task.id, result=summary)
        except Exception as e:
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run_plan)
    return {"task_id": task.id, "status": "started"}


@router.post("/reorganize/execute")
def start_reorganize_execute(request: Request, background_tasks: BackgroundTasks, body: ReorganizeExecuteRequest):
    """Execute a previously planned reorganization."""
    plan = _reorganize_plans.get(body.plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found. Please re-scan.")

    from ..reorganize import execute_reorganization

    task = _create_task("reorganize-execute")

    # Override delete_originals from execution request
    plan.config.delete_originals = body.delete_originals
    plan.config.dry_run = False

    def run_execute():
        try:

            def on_progress(info):
                _update_progress(task.id, info)

            result = execute_reorganization(plan, progress_fn=on_progress)

            _finish_task(
                task.id,
                result={
                    "files_processed": result.files_processed,
                    "files_copied": result.files_copied,
                    "files_skipped": result.files_skipped,
                    "originals_deleted": result.originals_deleted,
                    "space_saved": result.space_saved,
                    "errors": result.errors[:50],
                },
            )

            # Clean up the plan
            _reorganize_plans.pop(body.plan_id, None)
        except Exception as e:
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run_execute)
    return {"task_id": task.id, "status": "started"}


@router.get("/preview/{file_path:path}")
def get_file_preview(request: Request, file_path: str) -> dict:
    """Generate preview data for a file."""
    full_path = Path(f"/{file_path}").resolve()

    # Security check
    cat = _open_catalog(request)
    try:
        row = cat.get_file_by_path(str(full_path))
        if row is None:
            raise HTTPException(status_code=404, detail="Not in catalog")
    finally:
        cat.close()

    if not full_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    ext = full_path.suffix.lower()
    result: dict[str, Any] = {
        "path": str(full_path),
        "type": "unknown",
        "name": full_path.name,
        "size": full_path.stat().st_size,
    }

    # Text files
    if ext in _TEXT_EXTS or full_path.name.lower() in _TEXT_NAMES:
        try:
            content = full_path.read_text(encoding="utf-8", errors="replace")[:50000]
            lang = _detect_language(ext)
            result.update(
                {
                    "type": "text",
                    "content": content,
                    "language": lang,
                    "lines": content.count("\n") + 1,
                }
            )
        except OSError:
            result["type"] = "unknown"
        return result

    # PDF
    if ext == ".pdf":
        result.update({"type": "pdf", "url": f"/api/stream/{file_path}"})
        return result

    # Archives
    if ext in _ARCHIVE_EXTS:
        import tarfile
        import zipfile

        try:
            entries: list[dict[str, Any]] = []
            if ext == ".zip":
                with zipfile.ZipFile(full_path) as zf:
                    for info in zf.infolist()[:100]:
                        entries.append(
                            {
                                "name": info.filename,
                                "size": info.file_size,
                                "is_dir": info.is_dir(),
                            }
                        )
            elif ext in {".tar", ".gz", ".bz2", ".xz"}:
                with tarfile.open(full_path) as tf:
                    for member in tf.getmembers()[:100]:
                        entries.append(
                            {
                                "name": member.name,
                                "size": member.size,
                                "is_dir": member.isdir(),
                            }
                        )
            result.update(
                {
                    "type": "archive",
                    "entries": entries,
                    "total_entries": len(entries),
                }
            )
        except (OSError, zipfile.BadZipFile, tarfile.TarError):
            result["type"] = "unknown"
        return result

    # Audio
    if ext in _AUDIO_EXTS:
        audio_media_types = {
            ".mp3": "audio/mpeg",
            ".wav": "audio/wav",
            ".flac": "audio/flac",
            ".ogg": "audio/ogg",
            ".m4a": "audio/mp4",
            ".aac": "audio/aac",
        }
        result.update(
            {
                "type": "audio",
                "url": f"/api/stream/{file_path}",
                "media_type": audio_media_types.get(ext, "audio/mpeg"),
            }
        )
        return result

    return result


# ── Recovery endpoints ────────────────────────────────────────────────


class RecoverFilesRequest(BaseModel):
    paths: list[str]
    destination: str
    delete_source: bool = False


class RepairRequest(BaseModel):
    path: str


class PhotoRecRequest(BaseModel):
    source: str
    output_dir: str | None = None
    file_types: list[str] | None = None


class QuarantineDeleteRequest(BaseModel):
    paths: list[str]


class QuarantineRestoreRequest(BaseModel):
    paths: list[str]
    restore_to: str | None = None


class AppMineRequest(BaseModel):
    app_ids: list[str] | None = None


@router.get("/recovery/apps")
def get_available_apps_endpoint():
    """List all known apps and whether they have data present."""
    from ..recovery import get_available_apps

    return {"apps": get_available_apps()}


@router.post("/recovery/app-mine")
def start_app_mine(background_tasks: BackgroundTasks, body: AppMineRequest):
    """Mine media from selected app data directories (background task)."""
    from ..recovery import mine_app_media

    task = _create_task("app-mine")

    def run():
        try:
            results = mine_app_media(
                app_ids=body.app_ids,
                progress_fn=lambda p: _update_progress(task.id, p),
            )
            # Serialize results — cap file lists for response size
            serialized = []
            for r in results:
                serialized.append(
                    {
                        "app_id": r.app_id,
                        "app_name": r.app_name,
                        "icon": r.icon,
                        "color": r.color,
                        "category": r.category,
                        "available": r.available,
                        "encrypted": r.encrypted,
                        "note": r.note,
                        "files_found": r.files_found,
                        "total_size": r.total_size,
                        "raw_files_count": r.raw_files_count,
                        "raw_total_size": r.raw_total_size,
                        "images": r.images,
                        "videos": r.videos,
                        "audio": r.audio,
                        "other": r.other,
                        "files": r.files[:200],  # Cap per app
                        "paths_checked": r.paths_checked,
                    }
                )
            total_files = sum(r.files_found for r in results)
            total_size = sum(r.total_size for r in results)
            _finish_task(
                task.id,
                result={
                    "apps": serialized,
                    "total_files": total_files,
                    "total_size": total_size,
                },
            )
        except Exception as e:
            logger.exception("App mining failed")
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started"}


@router.get("/recovery/quarantine")
def get_quarantine(request: Request):
    """List all files in the quarantine."""
    from ..recovery import list_quarantine

    qroot = getattr(request.app.state, "quarantine_root", None)
    entries = list_quarantine(Path(qroot) if qroot else None)
    return {
        "entries": [
            {
                "path": e.path,
                "original_path": e.original_path,
                "size": e.size,
                "ext": e.ext,
                "quarantine_date": e.quarantine_date,
                "category": e.category,
            }
            for e in entries
        ],
        "total": len(entries),
        "total_size": sum(e.size for e in entries),
    }


@router.post("/recovery/quarantine/restore")
def restore_quarantine(request: Request, body: QuarantineRestoreRequest):
    """Restore files from quarantine."""
    from ..recovery import restore_from_quarantine

    qroot = getattr(request.app.state, "quarantine_root", None)
    return restore_from_quarantine(
        body.paths,
        quarantine_root=Path(qroot) if qroot else None,
        restore_to=body.restore_to,
    )


@router.post("/recovery/quarantine/delete")
def delete_quarantine(request: Request, body: QuarantineDeleteRequest):
    """Permanently delete files from quarantine."""
    from ..recovery import delete_from_quarantine

    qroot = getattr(request.app.state, "quarantine_root", None)
    return delete_from_quarantine(body.paths, quarantine_root=Path(qroot) if qroot else None)


@router.post("/recovery/deep-scan")
def start_deep_scan(request: Request, background_tasks: BackgroundTasks):
    """Start a deep scan for hidden/lost media files (background task)."""
    from ..recovery import deep_scan

    task = _create_task("deep-scan")

    def run_scan():
        try:
            result = deep_scan(
                progress_fn=lambda p: _update_progress(task.id, p),
            )
            _finish_task(
                task.id,
                result={
                    "locations_scanned": result.locations_scanned,
                    "files_found": result.files_found,
                    "total_size": result.total_size,
                    "files": result.files[:500],
                    "locations": result.locations,
                },
            )
        except Exception as e:
            logger.exception("Deep scan failed")
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run_scan)
    return {"task_id": task.id, "status": "started"}


@router.post("/recovery/recover-files")
def start_recover_files(body: RecoverFilesRequest):
    """Copy/move found files to a recovery destination."""
    from ..recovery import recover_files

    return recover_files(body.paths, body.destination, body.delete_source)


@router.post("/recovery/integrity-check")
def start_integrity_check(request: Request, background_tasks: BackgroundTasks):
    """Check integrity of all cataloged media files (background task)."""
    from ..recovery import check_integrity

    task = _create_task("integrity-check")
    catalog_path = str(request.app.state.catalog_path)

    def run_check():
        try:
            result = check_integrity(
                catalog_path=catalog_path,
                progress_fn=lambda p: _update_progress(task.id, p),
            )
            _finish_task(
                task.id,
                result={
                    "total_checked": result.total_checked,
                    "healthy": result.healthy,
                    "corrupted": result.corrupted,
                    "repaired": result.repaired,
                    "errors": result.errors[:200],
                },
            )
        except Exception as e:
            logger.exception("Integrity check failed")
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run_check)
    return {"task_id": task.id, "status": "started"}


@router.post("/recovery/repair")
def repair_single_file(body: RepairRequest):
    """Attempt to repair a single corrupted file."""
    from ..recovery import repair_file

    return repair_file(body.path)


@router.get("/recovery/photorec/status")
def photorec_status():
    """Check if PhotoRec is available."""
    from ..recovery import check_photorec

    return check_photorec()


@router.get("/recovery/disks")
def get_disks():
    """List available disks for recovery."""
    from ..recovery import list_disks

    return {"disks": list_disks()}


@router.post("/recovery/photorec/run")
def start_photorec(background_tasks: BackgroundTasks, body: PhotoRecRequest):
    """Start a PhotoRec recovery run (background task)."""
    from ..recovery import check_photorec, run_photorec

    check = check_photorec()
    if not check["available"]:
        raise HTTPException(status_code=400, detail="PhotoRec není nainstalován. Spusťte: brew install testdisk")

    task = _create_task("photorec")

    def run():
        try:
            result = run_photorec(
                source=body.source,
                output_dir=body.output_dir,
                file_types=body.file_types,
                progress_fn=lambda p: _update_progress(task.id, p),
            )
            _finish_task(
                task.id,
                result={
                    "files_recovered": result.files_recovered,
                    "total_size": result.total_size,
                    "output_dir": result.output_dir,
                    "files": result.files[:500],
                },
            )
        except Exception as e:
            logger.exception("PhotoRec failed")
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started"}


# ── Scenario endpoints ────────────────────────────────────────────────


class ScenarioCreateRequest(BaseModel):
    name: str
    description: str = ""
    icon: str = "\U0001f3ac"
    color: str = "#58a6ff"
    steps: list[dict] = []
    trigger: dict = {}


class ScenarioUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    icon: str | None = None
    color: str | None = None
    steps: list[dict] | None = None
    trigger: dict | None = None


@router.get("/scenarios")
def get_scenarios():
    """List all saved scenarios."""
    from ..scenarios import list_scenarios

    return {"scenarios": list_scenarios()}


@router.get("/scenarios/templates")
def get_scenario_templates():
    """Get built-in scenario templates."""
    from ..scenarios import get_templates

    return {"templates": get_templates()}


@router.get("/scenarios/step-types")
def get_step_types():
    """Get available step types for building scenarios."""
    from ..scenarios import STEP_TYPES

    return {"step_types": STEP_TYPES}


@router.get("/scenarios/triggers")
def check_triggers():
    """Check if any volume-mount triggers match currently mounted volumes."""
    from ..scenarios import check_volume_triggers

    return {"triggered": check_volume_triggers()}


@router.get("/scenarios/{scenario_id}")
def get_scenario_detail(scenario_id: str):
    """Get a single scenario by ID."""
    from ..scenarios import get_scenario

    sc = get_scenario(scenario_id)
    if not sc:
        raise HTTPException(status_code=404, detail="Scénář nenalezen")
    return sc


@router.post("/scenarios")
def create_new_scenario(body: ScenarioCreateRequest):
    """Create a new scenario."""
    from ..scenarios import create_scenario

    return create_scenario(body.model_dump())


@router.put("/scenarios/{scenario_id}")
def update_existing_scenario(scenario_id: str, body: ScenarioUpdateRequest):
    """Update a scenario."""
    from ..scenarios import update_scenario

    data = {k: v for k, v in body.model_dump().items() if v is not None}
    result = update_scenario(scenario_id, data)
    if not result:
        raise HTTPException(status_code=404, detail="Scénář nenalezen")
    return result


@router.delete("/scenarios/{scenario_id}")
def delete_existing_scenario(scenario_id: str):
    """Delete a scenario."""
    from ..scenarios import delete_scenario

    if not delete_scenario(scenario_id):
        raise HTTPException(status_code=404, detail="Scénář nenalezen")
    return {"status": "ok"}


@router.post("/scenarios/{scenario_id}/duplicate")
def duplicate_existing_scenario(scenario_id: str):
    """Duplicate a scenario."""
    from ..scenarios import duplicate_scenario

    result = duplicate_scenario(scenario_id)
    if not result:
        raise HTTPException(status_code=404, detail="Scénář nenalezen")
    return result


@router.post("/scenarios/{scenario_id}/run")
def run_scenario(scenario_id: str, request: Request, background_tasks: BackgroundTasks):
    """Execute a scenario (background task)."""
    from ..scenarios import execute_scenario, get_scenario

    sc = get_scenario(scenario_id)
    if not sc:
        raise HTTPException(status_code=404, detail="Scénář nenalezen")

    task = _create_task(f"scenario:{sc['name']}")
    catalog_path = str(request.app.state.catalog_path)

    def run():
        try:
            result = execute_scenario(
                scenario_id=scenario_id,
                catalog_path=catalog_path,
                progress_fn=lambda p: _update_progress(task.id, p),
            )
            _finish_task(task.id, result=result)
        except Exception as e:
            logger.exception("Scenario execution failed")
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started", "scenario": sc["name"]}


# ── Distributed Backup ───────────────────────────────────────────────


class BackupTargetUpdate(BaseModel):
    enabled: bool | None = None
    priority: int | None = None
    total_bytes: int | None = None
    free_bytes: int | None = None


class BackupExecuteRequest(BaseModel):
    dry_run: bool = False


@router.get("/backup/stats")
def backup_stats(request: Request):
    """Get overall backup health statistics."""
    from ..distributed_backup import get_backup_stats, ensure_backup_tables

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
        cat.close()


@router.get("/backup/targets")
def backup_targets(request: Request):
    """List all backup targets with capacity info."""
    from ..distributed_backup import get_targets, ensure_backup_tables

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
        cat.close()


@router.post("/backup/probe")
def backup_probe(request: Request):
    """Probe all remotes for storage capacity."""
    from ..distributed_backup import probe_targets

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
        cat.close()


@router.put("/backup/targets/{remote_name}")
def update_backup_target(remote_name: str, body: BackupTargetUpdate, request: Request):
    """Enable/disable a target or change its priority."""
    from ..distributed_backup import set_target_enabled, set_target_priority, ensure_backup_tables

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
        cat.close()


@router.post("/backup/plan")
def backup_plan(request: Request):
    """Create a distribution plan for backing up files."""
    from ..distributed_backup import create_backup_plan

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
        cat.close()


@router.post("/backup/execute")
def backup_execute(request: Request, background_tasks: BackgroundTasks, body: BackupExecuteRequest):
    """Execute the backup plan (background task)."""
    from ..distributed_backup import create_backup_plan, execute_backup_plan

    task = _create_task("backup:distribute")
    catalog_path = str(request.app.state.catalog_path)

    def run():
        from ..catalog import Catalog

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
            cat.close()

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started", "dry_run": body.dry_run}


@router.post("/backup/verify")
def backup_verify(request: Request, background_tasks: BackgroundTasks):
    """Verify backed up files exist on remotes (background task)."""
    from ..distributed_backup import verify_backups

    task = _create_task("backup:verify")
    catalog_path = str(request.app.state.catalog_path)

    def run():
        from ..catalog import Catalog

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
            cat.close()

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started"}


@router.get("/backup/manifest")
def backup_manifest(request: Request, page: int = 1, limit: int = 50, search: str = ""):
    """Get paginated backup manifest."""
    from ..distributed_backup import ensure_backup_tables

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
        cat.close()


# ── Backup Monitoring ────────────────────────────────────────────────


@router.get("/backup/monitor")
def backup_monitor_status():
    """Get backup monitoring status and alerts."""
    from ..backup_monitor import get_monitor_status

    return get_monitor_status()


@router.post("/backup/monitor/check")
def backup_monitor_run(background_tasks: BackgroundTasks):
    """Run health checks on all backup targets."""
    from ..backup_monitor import run_health_checks

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
    from ..backup_monitor import acknowledge_all_alerts

    count = acknowledge_all_alerts()
    return {"acknowledged": count}


@router.post("/backup/monitor/test-notification")
def backup_test_notification():
    """Send a test notification."""
    from ..backup_monitor import send_test_notification

    return send_test_notification()


# ── Bit Rot Detection ────────────────────────────────────────────────


@router.get("/bitrot/stats")
def bitrot_stats(request: Request):
    """Get bit rot verification statistics."""
    from ..bitrot import get_verification_stats

    cat = _open_catalog(request)
    try:
        return get_verification_stats(cat)
    finally:
        cat.close()


@router.post("/bitrot/scan")
def bitrot_scan(request: Request, background_tasks: BackgroundTasks, limit: int = 500):
    """Run bit rot scan (background task)."""
    from ..bitrot import scan_bitrot

    task = _create_task("bitrot:scan")
    catalog_path = str(request.app.state.catalog_path)

    def run():
        from ..catalog import Catalog

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
            cat.close()

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
        cat.close()


# ── Backup auto-heal ──────────────────────────────────────────────────


@router.post("/backup/auto-heal")
def backup_auto_heal(request: Request, background_tasks: BackgroundTasks):
    """Auto-heal: redistribute files from unhealthy remotes."""
    from ..distributed_backup import auto_heal

    task = _create_task("backup:auto-heal")
    catalog_path = str(request.app.state.catalog_path)

    def run():
        from ..catalog import Catalog

        cat = Catalog(catalog_path)
        cat.open()
        try:
            result = auto_heal(cat, progress_fn=lambda p: _update_progress(task.id, p))
            _finish_task(task.id, result=result)
        except Exception as e:
            logger.exception("Auto-heal failed")
            _finish_task(task.id, error=str(e))
        finally:
            cat.close()

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started"}


# ── Signal decryption ─────────────────────────────────────────────────


class SignalDecryptRequest(BaseModel):
    destination: str


@router.get("/recovery/signal/status")
def signal_decrypt_status():
    """Check if Signal decryption is possible."""
    from ..recovery import check_signal_decrypt

    return check_signal_decrypt()


@router.post("/recovery/signal/decrypt")
def start_signal_decrypt(
    background_tasks: BackgroundTasks,
    body: SignalDecryptRequest,
):
    """Decrypt Signal attachments and save to destination (background task)."""
    from ..recovery import decrypt_signal_attachments

    task = _create_task("signal-decrypt")

    def run():
        try:
            result = decrypt_signal_attachments(
                destination=body.destination,
                progress_fn=lambda p: _update_progress(task.id, p),
            )
            _finish_task(task.id, result=result)
        except Exception as e:
            logger.exception("Signal decryption failed")
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started"}


# ── Gallery & Smart Scoring ──────────────────────────────────────────


@router.get("/gallery/highlights")
async def gallery_highlights(
    request: Request,
    limit: int = Query(200, ge=1, le=1000),
    min_score: float = Query(0.0, ge=0, le=100),
):
    """Return top-scored media files for gallery display."""
    from godmode_media_library.media_score import score_catalog

    catalog_path = str(request.app.state.catalog_path)
    scores = score_catalog(catalog_path, limit=limit, min_score=min_score)
    return {"files": [s.to_dict() for s in scores]}


@router.get("/gallery/collections")
async def gallery_collections(request: Request):
    """Return auto-curated smart collections."""
    from godmode_media_library.media_score import get_smart_collections

    catalog_path = str(request.app.state.catalog_path)
    collections = get_smart_collections(catalog_path)
    return {"collections": collections}


@router.get("/gallery/score/{file_path:path}")
async def gallery_file_score(request: Request, file_path: str):
    """Get detailed quality score breakdown for a single file."""
    import sqlite3

    from godmode_media_library.media_score import score_file

    catalog_path = str(request.app.state.catalog_path)
    db = sqlite3.connect(catalog_path)
    db.row_factory = sqlite3.Row

    row = db.execute(
        """
        SELECT
            f.path, f.ext, f.size, f.mtime,
            f.width, f.height, f.bitrate,
            f.date_original, f.camera_make, f.camera_model,
            f.gps_latitude, f.gps_longitude,
            f.metadata_richness,
            d.group_id AS duplicate_group_id,
            d.is_primary,
            fr.rating,
            fn.note IS NOT NULL AS has_note,
            (SELECT COUNT(*) FROM file_tags ft WHERE ft.file_id = f.id) AS tag_count
        FROM files f
        LEFT JOIN duplicates d ON d.file_id = f.id
        LEFT JOIN file_ratings fr ON fr.file_id = f.id
        LEFT JOIN file_notes fn ON fn.file_id = f.id
        WHERE f.path = ?
        """,
        [f"/{file_path}"],
    ).fetchone()
    db.close()

    if not row:
        raise HTTPException(status_code=404, detail="Soubor nenalezen v katalogu")

    ms = score_file(dict(row))
    return ms.to_dict()


@router.get("/gallery/slideshow")
async def gallery_slideshow(
    request: Request,
    collection: str = Query("best_of"),
    limit: int = Query(50, ge=1, le=200),
    shuffle: bool = Query(False),
):
    """Return an ordered list of files for slideshow playback.

    Supports named collections or 'best_of' for top scored files.
    """
    import random

    from godmode_media_library.media_score import get_smart_collections, score_catalog

    catalog_path = str(request.app.state.catalog_path)

    if collection == "all_top":
        scores = score_catalog(catalog_path, limit=limit, min_score=40)
        files = [s.to_dict() for s in scores]
    else:
        collections = get_smart_collections(catalog_path)
        files = collections.get(collection, [])[:limit]

    if shuffle:
        random.shuffle(files)

    return {"collection": collection, "count": len(files), "files": files}


# ── Face / Person API ──────────────────────────────────────────────


class PersonRenameRequest(BaseModel):
    name: str


class PersonMergeRequest(BaseModel):
    merge_ids: list[int]


class FaceAssignRequest(BaseModel):
    person_id: int


class FaceDetectRequest(BaseModel):
    model: str = "hog"
    max_dimension: int = 1600


class FaceClusterRequest(BaseModel):
    eps: float = 0.5
    min_samples: int = 2


@router.get("/persons")
def list_persons(request: Request):
    """List all persons with face counts."""
    cat = _open_catalog(request)
    try:
        persons = cat.get_all_persons()
        return {"persons": persons, "total": len(persons)}
    finally:
        cat.close()


@router.get("/persons/{person_id}")
def get_person(request: Request, person_id: int):
    cat = _open_catalog(request)
    try:
        person = cat.get_person(person_id)
        if person is None:
            raise HTTPException(404, "Person not found")
        return person
    finally:
        cat.close()


@router.put("/persons/{person_id}/name")
def rename_person(request: Request, person_id: int, body: PersonRenameRequest):
    """Rename a person."""
    cat = _open_catalog(request)
    try:
        person = cat.get_person(person_id)
        if person is None:
            raise HTTPException(404, "Person not found")
        cat.update_person_name(person_id, body.name)
        # Sync labels table for files containing this person
        _sync_person_labels(cat, person_id)
        cat.commit()
        return {"status": "ok", "person_id": person_id, "name": body.name}
    finally:
        cat.close()


@router.post("/persons/{person_id}/merge")
def merge_persons(request: Request, person_id: int, body: PersonMergeRequest):
    """Merge other persons into this one."""
    cat = _open_catalog(request)
    try:
        person = cat.get_person(person_id)
        if person is None:
            raise HTTPException(404, "Person not found")
        reassigned = cat.merge_persons(person_id, body.merge_ids)
        _sync_person_labels(cat, person_id)
        cat.commit()
        return {"status": "ok", "reassigned_faces": reassigned}
    finally:
        cat.close()


@router.delete("/persons/{person_id}")
def delete_person(request: Request, person_id: int):
    cat = _open_catalog(request)
    try:
        person = cat.get_person(person_id)
        if person is None:
            raise HTTPException(404, "Person not found")
        cat.delete_person(person_id)
        cat.commit()
        return {"status": "ok"}
    finally:
        cat.close()


@router.get("/persons/{person_id}/faces")
def get_person_faces(
    request: Request,
    person_id: int,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    cat = _open_catalog(request)
    try:
        faces = cat.get_faces_for_person(person_id, limit=limit, offset=offset)
        return {"faces": faces, "count": len(faces), "person_id": person_id}
    finally:
        cat.close()


@router.get("/faces")
def list_faces(
    request: Request,
    person_id: int | None = Query(None),
    unidentified: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """List faces, optionally filtered by person or unidentified status."""
    cat = _open_catalog(request)
    try:
        if unidentified:
            faces = cat.get_unidentified_faces(limit=limit, offset=offset)
        elif person_id is not None:
            faces = cat.get_faces_for_person(person_id, limit=limit, offset=offset)
        else:
            # All faces
            cur = cat.conn.execute(
                """SELECT f.id, f.file_id, f.face_index, f.person_id,
                          f.bbox_top, f.bbox_right, f.bbox_bottom, f.bbox_left,
                          f.confidence, f.cluster_id, fi.path, p.name as person_name
                   FROM faces f
                   JOIN files fi ON f.file_id = fi.id
                   LEFT JOIN persons p ON f.person_id = p.id
                   ORDER BY f.person_id NULLS LAST, f.id LIMIT ? OFFSET ?""",
                (limit, offset),
            )
            faces = [
                {
                    "id": r[0],
                    "file_id": r[1],
                    "face_index": r[2],
                    "person_id": r[3],
                    "bbox": {"top": r[4], "right": r[5], "bottom": r[6], "left": r[7]},
                    "confidence": r[8],
                    "cluster_id": r[9],
                    "path": r[10],
                    "person_name": r[11] or "",
                }
                for r in cur.fetchall()
            ]
        return {"faces": faces, "count": len(faces)}
    finally:
        cat.close()


@router.get("/faces/stats")
def face_stats(request: Request):
    cat = _open_catalog(request)
    try:
        return cat.face_stats()
    finally:
        cat.close()


@router.get("/faces/{face_id}/thumbnail")
def get_face_thumbnail(request: Request, face_id: int, size: int = Query(150, ge=32, le=512)):
    """Return a cropped face thumbnail as JPEG."""
    from godmode_media_library.face_detect import crop_face_thumbnail

    cat = _open_catalog(request)
    try:
        face = cat.get_face_by_id(face_id)
        if face is None:
            raise HTTPException(404, "Face not found")

        # Check thumbnail cache
        cache_key = f"face_{face_id}_{size}"
        cached = _thumb_cache_get(cache_key, size)
        if cached:
            return StreamingResponse(io.BytesIO(cached), media_type="image/jpeg")

        data = crop_face_thumbnail(face["path"], face["bbox"], size=size)
        if data is None:
            raise HTTPException(404, "Cannot generate face thumbnail")

        _thumb_cache_put(cache_key, size, data)
        return StreamingResponse(io.BytesIO(data), media_type="image/jpeg")
    finally:
        cat.close()


@router.put("/faces/{face_id}/person")
def assign_face_to_person(request: Request, face_id: int, body: FaceAssignRequest):
    """Assign a face to a person."""
    cat = _open_catalog(request)
    try:
        face = cat.get_face_by_id(face_id)
        if face is None:
            raise HTTPException(404, "Face not found")
        person = cat.get_person(body.person_id)
        if person is None:
            raise HTTPException(404, "Person not found")
        cat.assign_face_to_person(face_id, body.person_id)
        _sync_person_labels(cat, body.person_id)
        cat.commit()
        return {"status": "ok", "face_id": face_id, "person_id": body.person_id}
    finally:
        cat.close()


@router.get("/faces/by-file")
def get_file_faces(request: Request, path: str = Query(...)):
    """Get all detected faces in a specific file."""
    cat = _open_catalog(request)
    try:
        file_row = cat.get_file_by_path(path)
        if file_row is None:
            raise HTTPException(404, "File not found")
        faces = cat.get_faces_for_file(file_row.id)
        return {"faces": faces, "file_path": path}
    finally:
        cat.close()


@router.post("/faces/detect")
def trigger_face_detection(request: Request, background: BackgroundTasks, body: FaceDetectRequest):
    """Start background face detection for all unscanned images."""
    task = _create_task("face_detect")

    def _run():
        try:
            from godmode_media_library.face_crypto import get_encrypt_fn
            from godmode_media_library.face_detect import scan_new_faces

            cat = _open_catalog(request)
            encrypt_fn = get_encrypt_fn(enabled=True)

            def on_progress(done, total):
                task.progress = {"done": done, "total": total}
                _notify_ws(task.id, _task_to_msg(task))

            try:
                result = scan_new_faces(
                    cat,
                    model=body.model,
                    max_dimension=body.max_dimension,
                    encrypt_fn=encrypt_fn,
                    progress_fn=on_progress,
                )
                task.result = {
                    "files_processed": result.files_processed,
                    "faces_detected": result.faces_detected,
                    "errors": result.errors,
                }
                task.status = "completed"
            finally:
                cat.close()
        except Exception as exc:
            task.status = "failed"
            task.error = str(exc)
            logger.exception("Face detection task failed")
        task.finished_at = datetime.now(timezone.utc).isoformat()
        _notify_ws(task.id, _task_to_msg(task))

    background.add_task(_run)
    return {"task_id": task.id, "status": "started"}


@router.post("/faces/cluster")
def trigger_face_clustering(request: Request, background: BackgroundTasks, body: FaceClusterRequest):
    """Start background face clustering."""
    task = _create_task("face_cluster")

    def _run():
        try:
            from godmode_media_library.face_crypto import get_decrypt_fn
            from godmode_media_library.face_detect import cluster_faces

            cat = _open_catalog(request)
            decrypt_fn = get_decrypt_fn(enabled=True)
            try:
                clusters = cluster_faces(
                    cat,
                    eps=body.eps,
                    min_samples=body.min_samples,
                    decrypt_fn=decrypt_fn,
                )
                task.result = {
                    "clusters": len(clusters),
                    "total_faces_clustered": sum(len(v) for v in clusters.values()),
                }
                task.status = "completed"
            finally:
                cat.close()
        except Exception as exc:
            task.status = "failed"
            task.error = str(exc)
            logger.exception("Face clustering task failed")
        task.finished_at = datetime.now(timezone.utc).isoformat()
        _notify_ws(task.id, _task_to_msg(task))

    background.add_task(_run)
    return {"task_id": task.id, "status": "started"}


@router.post("/faces/privacy/consent")
def record_privacy_consent(request: Request):
    """Record that the user has consented to face encoding storage."""
    cat = _open_catalog(request)
    try:
        cat.set_privacy_flag("consent_given", datetime.now(timezone.utc).isoformat())
        return {"status": "ok", "consent_given": True}
    finally:
        cat.close()


@router.get("/faces/privacy")
def get_privacy_status(request: Request):
    """Check if privacy consent was given and encryption status."""
    cat = _open_catalog(request)
    try:
        consent = cat.get_privacy_flag("consent_given")
        return {
            "consent_given": consent is not None,
            "consent_timestamp": consent,
            "encryption_enabled": True,
        }
    finally:
        cat.close()


@router.delete("/faces/privacy/encodings")
def wipe_face_encodings(request: Request):
    """Delete all stored face encodings for privacy. Face records remain but without biometric data."""
    cat = _open_catalog(request)
    try:
        count = cat.wipe_face_encodings()
        return {"status": "ok", "encodings_wiped": count}
    finally:
        cat.close()


@router.post("/persons/create")
def create_person(request: Request, body: PersonRenameRequest):
    """Create a new named person."""
    cat = _open_catalog(request)
    try:
        person_id = cat.upsert_person(body.name)
        cat.commit()
        return {"status": "ok", "person_id": person_id, "name": body.name}
    finally:
        cat.close()


# ── Cloud API ──────────────────────────────────────────────────────


class CloudSyncRequest(BaseModel):
    remote: str
    remote_path: str = ""
    local_path: str = ""
    include_pattern: str = "*.{jpg,jpeg,png,heic,heif,mp4,mov,avi,mkv,mp3,m4a}"
    dry_run: bool = False


class CloudMountRequest(BaseModel):
    remote: str
    mount_point: str = ""


@router.get("/cloud/status")
def cloud_status(request: Request):
    """Get cloud storage status — rclone remotes + native paths, enriched with scan info."""
    from godmode_media_library.cloud import get_cloud_status

    status = get_cloud_status()

    def _safe_disk_count(root: str, max_seconds: float = 3.0) -> int:
        """Count files on disk, skipping hidden dirs and respecting timeout."""
        import os
        import time

        count = 0
        deadline = time.monotonic() + max_seconds
        try:
            for dirpath, dirnames, filenames in os.walk(root):
                # Skip hidden directories (.Trash, .DS_Store dirs, etc.)
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
                count += len(filenames)
                if time.monotonic() > deadline:
                    break  # Return partial count rather than hang
        except OSError as exc:
            logger.debug("os.walk failed for %s: %s", root, exc)
        return count

    # Enrich each source with scan/file info from catalog + disk file count
    cat = _open_catalog(request)
    try:
        for src in status.get("sources", []):
            path = src.get("mount_path") or src.get("sync_path") or ""
            if not path:
                src["scanned"] = False
                src["last_scan"] = None
                src["file_count"] = 0
                src["disk_count"] = 0
                continue
            # Count indexed files under this path
            count_row = cat.conn.execute(
                "SELECT COUNT(*) FROM files WHERE path LIKE ? || '%'",
                (path.rstrip("/"),),
            ).fetchone()
            src["file_count"] = count_row[0] if count_row else 0
            # Last scan time
            scan_row = cat.conn.execute(
                "SELECT MAX(finished_at) FROM scans WHERE root LIKE '%' || ? || '%'",
                (path.rstrip("/"),),
            ).fetchone()
            src["last_scan"] = scan_row[0] if scan_row and scan_row[0] else None
            src["scanned"] = src["file_count"] > 0
            # Count actual files on disk (safe, skips hidden dirs, 3s timeout)
            src["disk_count"] = _safe_disk_count(path) if Path(path).is_dir() else 0
    finally:
        cat.close()

    return status


@router.get("/cloud/remotes")
def cloud_remotes():
    """List configured rclone remotes."""
    from godmode_media_library.cloud import check_rclone, list_remotes, rclone_version

    if not check_rclone():
        return {"installed": False, "remotes": [], "version": None}
    return {
        "installed": True,
        "version": rclone_version(),
        "remotes": [{"name": r.name, "type": r.type, "label": r.provider_label, "icon": r.icon} for r in list_remotes()],
    }


@router.get("/cloud/native")
def cloud_native_paths(request: Request):
    """Detect natively synced cloud storage paths (iCloud, MEGA, pCloud, etc.)."""
    from godmode_media_library.cloud import detect_native_cloud_paths

    paths = detect_native_cloud_paths()

    def _safe_count(root: str) -> int:
        import os, time

        count, deadline = 0, time.monotonic() + 3.0
        try:
            for dirpath, dirnames, filenames in os.walk(root):
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
                count += len(filenames)
                if time.monotonic() > deadline:
                    break
        except OSError as exc:
            logger.debug("os.walk failed for %s: %s", root, exc)
        return count

    # Enrich with scan info + disk count
    cat = _open_catalog(request)
    try:
        for p in paths:
            path = p.get("path", "")
            if not path:
                p["scanned"] = False
                p["file_count"] = 0
                p["disk_count"] = 0
                continue
            count_row = cat.conn.execute(
                "SELECT COUNT(*) FROM files WHERE path LIKE ? || '%'",
                (path.rstrip("/"),),
            ).fetchone()
            p["file_count"] = count_row[0] if count_row else 0
            p["scanned"] = p["file_count"] > 0
            p["disk_count"] = _safe_count(path) if Path(path).is_dir() else 0
    finally:
        cat.close()

    return {"paths": paths, "count": len(paths)}


@router.get("/cloud/providers")
def cloud_providers():
    """List supported cloud providers with setup instructions."""
    from godmode_media_library.cloud import PROVIDERS, provider_setup_guide

    result = {}
    for key in PROVIDERS:
        result[key] = provider_setup_guide(key)
    return {"providers": result}


@router.get("/cloud/providers/{provider_key}")
def cloud_provider_guide(provider_key: str):
    """Get setup guide for a specific cloud provider."""
    from godmode_media_library.cloud import provider_setup_guide

    guide = provider_setup_guide(provider_key)
    if "error" in guide:
        raise HTTPException(404, guide["error"])
    return guide


@router.get("/cloud/remote/{remote_name}/browse")
def cloud_browse(remote_name: str, path: str = Query("")):
    """Browse files/folders in a remote."""
    from godmode_media_library.cloud import rclone_ls

    try:
        items = rclone_ls(remote_name, path)
        return {"remote": remote_name, "path": path, "items": items}
    except RuntimeError as e:
        logger.error("Cloud listing failed for %s/%s: %s", remote_name, path, e)
        raise HTTPException(500, "Cloud listing failed") from e


@router.get("/cloud/remote/{remote_name}/about")
def cloud_remote_about(remote_name: str):
    """Get storage usage for a remote (total, used, free)."""
    from godmode_media_library.cloud import rclone_about

    return rclone_about(remote_name)


@router.post("/cloud/sync")
def cloud_sync(request: Request, background: BackgroundTasks, body: CloudSyncRequest):
    """Start background sync (download) from cloud to local."""
    task = _create_task("cloud_sync")

    def _run():
        try:
            from godmode_media_library.cloud import default_sync_dir, rclone_copy

            local = body.local_path or str(default_sync_dir() / body.remote)
            result = rclone_copy(
                body.remote,
                body.remote_path,
                local,
                include_pattern=body.include_pattern,
                dry_run=body.dry_run,
            )
            task.result = {
                "remote": result.remote,
                "local_path": result.local_path,
                "files_transferred": result.files_transferred,
                "errors": result.errors,
                "elapsed_seconds": round(result.elapsed_seconds, 1),
            }
            task.status = "completed"
        except Exception as exc:
            task.status = "failed"
            task.error = str(exc)
            logger.exception("Cloud sync task failed")
        task.finished_at = datetime.now(timezone.utc).isoformat()
        _notify_ws(task.id, _task_to_msg(task))

    background.add_task(_run)
    return {"task_id": task.id, "status": "started"}


class CloudBackupRequest(BaseModel):
    remote: str  # target rclone remote name
    remote_path: str = "GML-Backup"  # destination folder on remote
    source_paths: list[str] = []  # local paths to back up (empty = all scanned roots)
    include_pattern: str = "*.{jpg,jpeg,png,heic,heif,tiff,tif,webp,mp4,mov,avi,mkv,mp3,m4a,wav,flac}"
    dry_run: bool = False


@router.post("/cloud/backup")
def cloud_backup(request: Request, background: BackgroundTasks, body: CloudBackupRequest):
    """Start background backup (upload) from local sources to cloud remote."""
    # Resolve source paths — use all scanned roots if none specified
    source_paths = body.source_paths
    if not source_paths:
        cat = _open_catalog(request)
        try:
            rows = cat.conn.execute("SELECT DISTINCT root FROM scans WHERE finished_at IS NOT NULL").fetchall()
            for row in rows:
                for r in (row[0] or "").split(";"):
                    r = r.strip()
                    if r and Path(r).is_dir():
                        source_paths.append(r)
            source_paths = list(dict.fromkeys(source_paths))  # dedupe
        finally:
            cat.close()

    if not source_paths:
        raise HTTPException(400, "Žádné zdroje k zálohování — nejdřív naskenujte soubory")

    task = _create_task("cloud_backup")

    def _run():
        try:
            from godmode_media_library.cloud import rclone_upload

            total_files = 0
            total_errors = 0
            for i, src in enumerate(source_paths):
                src_name = Path(src).name or "root"
                dest_path = f"{body.remote_path}/{src_name}" if body.remote_path else src_name
                _update_progress(
                    task.id,
                    {
                        "step": f"Zálohuji {src_name}",
                        "source": src,
                        "current": i + 1,
                        "total": len(source_paths),
                    },
                )
                result = rclone_upload(
                    src,
                    body.remote,
                    dest_path,
                    include_pattern=body.include_pattern,
                    dry_run=body.dry_run,
                )
                total_files += result.files_transferred
                total_errors += result.errors

            _finish_task(
                task.id,
                result={
                    "remote": body.remote,
                    "remote_path": body.remote_path,
                    "sources": len(source_paths),
                    "files_uploaded": total_files,
                    "errors": total_errors,
                    "dry_run": body.dry_run,
                },
            )
        except Exception as exc:
            _finish_task(task.id, error=str(exc))
            logger.exception("Cloud backup task failed")

    background.add_task(_run)
    return {"task_id": task.id, "status": "started", "sources": source_paths}


@router.post("/cloud/mount")
def cloud_mount_remote(body: CloudMountRequest):
    """Mount a remote as a local filesystem."""
    from godmode_media_library.cloud import rclone_mount

    try:
        path, success = rclone_mount(body.remote, body.mount_point or None)
        return {"mount_path": path, "success": success}
    except RuntimeError as e:
        logger.error("Cloud mount failed: %s", e)
        return {"mount_path": "", "success": False, "message": "Mount operation failed"}


@router.post("/cloud/unmount")
def cloud_unmount_remote(body: CloudMountRequest):
    """Unmount a FUSE mount."""
    from godmode_media_library.cloud import rclone_unmount

    mount_point = body.mount_point or str(Path.home() / "mnt" / body.remote)
    success = rclone_unmount(mount_point)
    return {"mount_point": mount_point, "success": success}


class CloudConnectRequest(BaseModel):
    provider_key: str
    name: str
    credentials: dict[str, str] = {}


@router.get("/cloud/provider-fields/{provider_key}")
def cloud_provider_fields(provider_key: str):
    """Return the credential fields needed for a provider."""
    from godmode_media_library.cloud import PROVIDERS

    info = PROVIDERS.get(provider_key)
    if not info:
        raise HTTPException(404, f"Unknown provider: {provider_key}")
    return {
        "provider": info["label"],
        "icon": info["icon"],
        "auth": info.get("auth", "credentials"),
        "fields": info.get("fields", []),
    }


@router.post("/cloud/connect")
def cloud_connect(body: CloudConnectRequest):
    """Create a new rclone remote (credential or start OAuth)."""
    from godmode_media_library.cloud import create_remote

    result = create_remote(body.provider_key, body.name, body.credentials)
    if not result["success"]:
        raise HTTPException(400, result["message"])
    return result


@router.get("/cloud/oauth/status/{name}")
def cloud_oauth_status(name: str):
    """Check OAuth flow status for a remote being configured."""
    from godmode_media_library.cloud import get_oauth_status

    return get_oauth_status(name)


@router.post("/cloud/oauth/finalize")
def cloud_oauth_finalize(body: CloudConnectRequest):
    """Finalize an OAuth remote with the captured token."""
    from godmode_media_library.cloud import finalize_oauth

    token = body.credentials.get("token", "")
    if not token:
        raise HTTPException(400, "Missing OAuth token")
    result = finalize_oauth(body.provider_key, body.name, token)
    if not result["success"]:
        raise HTTPException(400, result["message"])
    return result


@router.delete("/cloud/remote/{name}")
def cloud_delete_remote(name: str):
    """Remove an rclone remote configuration."""
    from godmode_media_library.cloud import delete_remote

    result = delete_remote(name)
    if not result["success"]:
        raise HTTPException(400, result["message"])
    return result


@router.post("/cloud/test/{name}")
def cloud_test_remote(name: str):
    """Test connection to a remote."""
    from godmode_media_library.cloud import test_remote

    return test_remote(name)


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
                task.progress = {"done": done, "total": total}
                _notify_ws(task.id, _task_to_msg(task))

            try:
                stats = batch_analyze(cat, progress_fn=on_progress)
                task.result = stats
                task.status = "completed"
            finally:
                cat.close()
        except Exception as exc:
            task.status = "failed"
            task.error = str(exc)
            logger.exception("Quality analysis task failed")
        task.finished_at = datetime.now(timezone.utc).isoformat()
        _notify_ws(task.id, _task_to_msg(task))

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


def _sync_person_labels(cat: Catalog, person_id: int) -> None:
    """Sync labels table when person name changes — update people column for all files with this person's faces."""
    faces = cat.get_faces_for_person(person_id, limit=100000)
    file_ids = {f["file_id"] for f in faces}
    for fid in file_ids:
        all_faces = cat.get_faces_for_file(fid)
        names = sorted({f["person_name"] for f in all_faces if f.get("person_name")})
        people_str = ";".join(names)
        cat.upsert_label(fid, people=people_str)


# ── Report endpoints ──────────────────────────────────────────────


@router.get("/report/generate")
def report_generate(request: Request) -> HTMLResponse:
    """Generate a comprehensive HTML report and return it inline."""
    from ..report import generate_report_html

    catalog_path = request.app.state.catalog_path
    html = generate_report_html(catalog_path)
    return HTMLResponse(content=html)


@router.get("/report/download")
def report_download(request: Request) -> HTMLResponse:
    """Generate a comprehensive HTML report and return it as a downloadable file."""
    from ..report import generate_report_html

    catalog_path = request.app.state.catalog_path
    html = generate_report_html(catalog_path)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"godmode_report_{ts}.html"
    return HTMLResponse(
        content=html,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Ultimate Consolidation ────────────────────────────────────────────


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
    media_only: bool = True

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
        if v is not None:
            import re

            if not re.match(r"^\d+[KMGkmg]?$", v):
                raise ValueError("bwlimit must be like '10M', '1G', '512K' or a number")
        return v


def _consolidation_progress_dict(p) -> dict:
    """Convert ConsolidationProgress to dict for task updates."""
    return {
        "phase": p.phase,
        "phase_label": p.phase_label,
        "current_step": p.current_step,
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
    }


@router.get("/consolidation/status")
def consolidation_status(request: Request):
    """Get current consolidation status."""
    from ..consolidation import get_consolidation_status

    catalog_path = str(request.app.state.catalog_path)
    return get_consolidation_status(catalog_path)


@router.post("/consolidation/preview")
def consolidation_preview(body: ConsolidationStartRequest, request: Request, background_tasks: BackgroundTasks):
    """Dry-run: scan all sources, count files, estimate transfer — NO actual transfers.

    ALWAYS run this before starting a real consolidation.
    """
    from ..consolidation import preview_consolidation, ConsolidationConfig

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
    from ..consolidation import run_consolidation, ConsolidationConfig, get_consolidation_status

    # Guard: prevent starting a second consolidation while one is running
    catalog_path_str = str(request.app.state.catalog_path)
    try:
        status = get_consolidation_status(catalog_path_str)
        if status.get("has_active_job"):
            from fastapi import HTTPException

            raise HTTPException(
                status_code=409,
                detail="Consolidation job is already running. Pause it first or wait for completion.",
            )
    except Exception as exc:
        if hasattr(exc, "status_code"):  # Re-raise HTTPException
            raise
        logger.warning("Could not check active consolidation: %s", exc)

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
    """Pause an active consolidation job."""
    from ..consolidation import pause_consolidation

    catalog_path = str(request.app.state.catalog_path)
    return pause_consolidation(catalog_path)


@router.post("/consolidation/resume")
def consolidation_resume(request: Request, background_tasks: BackgroundTasks):
    """Resume a paused consolidation job. Completed phases are skipped automatically."""
    from ..consolidation import resume_consolidation

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
    from ..consolidation import get_failed_files_report

    catalog_path = str(request.app.state.catalog_path)
    return {"failed_files": get_failed_files_report(catalog_path)}
