"""Shared state, helpers, and Pydantic models for the web API.

All route sub-modules import from here instead of duplicating state.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

from fastapi import HTTPException, Request, WebSocket
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ── Pydantic request models ──────────────────────────────────────────


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


_DEFAULT_SHARE_EXPIRY_HOURS: float = 7 * 24  # 7 days


class CreateShareRequest(BaseModel):
    path: str
    label: str = ""
    password: str | None = None
    expires_hours: float | None = None  # Default: 7 days. Set to 0 to disable expiry.
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


# ── Constants ─────────────────────────────────────────────────────────

_DEFAULT_QUARANTINE_ROOT = (Path.home() / ".config" / "gml" / "quarantine").resolve()

# Directories that should not be browsable for security
_BLOCKED_PREFIXES = ("/etc", "/var", "/private", "/sbin", "/usr", "/bin", "/tmp", "/dev", "/proc", "/sys")

_MAX_PATH_LENGTH = 4096  # Reasonable OS limit

# Maximum completed tasks to keep in memory before eviction
_MAX_COMPLETED_TASKS = 50
_TASK_TTL_SECONDS = 3600  # 1 hour

# ── Path validation helpers ───────────────────────────────────────────


def _check_path_within_roots(request: Request, file_path: Path) -> None:
    """Verify that a file path is within managed (configured/scanned) roots.

    Raises HTTPException 403 if the path is outside all known roots.
    """
    import sqlite3

    resolved = file_path.resolve()
    resolved_str = str(resolved)

    # Get all managed roots (configured + scan history)
    roots: list[str] = []
    cat = _open_catalog(request)
    try:
        # Configured roots
        cur = cat.conn.execute("SELECT value FROM meta WHERE key = 'configured_roots'")
        row = cur.fetchone()
        if row:
            with contextlib.suppress(json.JSONDecodeError, TypeError):
                roots.extend(json.loads(row[0]))
        # Scan history roots
        try:
            for scan_row in cat.conn.execute("SELECT DISTINCT root FROM scans"):
                for r in (scan_row[0] or "").split(";"):
                    r = r.strip()
                    if r:
                        roots.append(r)
        except (sqlite3.OperationalError, sqlite3.DatabaseError):
            pass
    finally:
        cat.close()

    if not roots:
        raise HTTPException(
            status_code=403,
            detail="No managed roots configured. Add roots before deleting files.",
        )

    # Check if path is within any managed root — if so, allow it
    for root in roots:
        root_resolved = Path(root).resolve()
        root_str = str(root_resolved)
        if resolved_str == root_str or resolved_str.startswith(root_str + "/"):
            return  # Path is within a managed root

    # Block sensitive system directories (only for paths NOT within managed roots)
    if any(resolved_str == prefix or resolved_str.startswith(prefix + "/") for prefix in _BLOCKED_PREFIXES):
        raise HTTPException(status_code=403, detail="Access denied: system directory")

    raise HTTPException(status_code=403, detail="File outside managed roots — deletion denied")


def _sanitize_path(path_str: str, *, param_name: str = "path") -> str:
    """Validate and sanitize a user-supplied path string.

    Raises HTTPException for null bytes, excessive length, or dot-only paths.
    Returns the cleaned path string.
    """
    if "\x00" in path_str:
        raise HTTPException(status_code=400, detail=f"Invalid {param_name}: null bytes not allowed")
    if len(path_str) > _MAX_PATH_LENGTH:
        raise HTTPException(status_code=400, detail=f"Invalid {param_name}: path too long")
    stripped = path_str.strip().rstrip("/")
    # Reject paths that resolve to just "." or ".."
    basename = stripped.rsplit("/", 1)[-1] if "/" in stripped else stripped
    if basename in (".", ".."):
        raise HTTPException(status_code=400, detail=f"Invalid {param_name}: relative-only path not allowed")
    return stripped


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
_reorganize_plans: dict[str, tuple[float, Any]] = {}  # plan_id -> (created_monotonic, plan)
_reorganize_plans_lock = threading.Lock()
_REORGANIZE_PLAN_TTL = 3600.0  # 1 hour
_REORGANIZE_PLAN_MAX = 100

_ws_connections: dict[str, list[WebSocket]] = {}
_ws_lock = threading.Lock()


def _evict_old_plans() -> None:
    """Remove reorganize plans older than TTL or exceeding max count."""
    now = time.monotonic()
    expired = [pid for pid, (ts, _) in _reorganize_plans.items() if (now - ts) > _REORGANIZE_PLAN_TTL]
    for pid in expired:
        del _reorganize_plans[pid]
    if len(_reorganize_plans) > _REORGANIZE_PLAN_MAX:
        by_age = sorted(_reorganize_plans.items(), key=lambda kv: kv[1][0])
        for pid, _ in by_age[: len(_reorganize_plans) - _REORGANIZE_PLAN_MAX]:
            del _reorganize_plans[pid]


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
        id=uuid.uuid4().hex[:16],
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
    from ..catalog import Catalog  # Lazy import to avoid circular dependency

    cat = Catalog(request.app.state.catalog_path)
    cat.open()
    return cat


# ── Common data helpers ───────────────────────────────────────────────


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


# ── Thumbnail cache helpers ───────────────────────────────────────────


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
