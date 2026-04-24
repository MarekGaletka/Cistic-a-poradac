from __future__ import annotations

import copy
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
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


# ── Pydantic request models ──────────────────────────────────────────


class CloudSyncRequest(BaseModel):
    remote: str
    remote_path: str = ""
    local_path: str = ""
    include_pattern: str = "*.{jpg,jpeg,png,heic,heif,mp4,mov,avi,mkv,mp3,m4a}"
    dry_run: bool = False


class CloudMountRequest(BaseModel):
    remote: str
    mount_point: str = ""


class CloudBackupRequest(BaseModel):
    remote: str  # target rclone remote name
    remote_path: str = "GML-Backup"  # destination folder on remote
    source_paths: list[str] = []  # local paths to back up (empty = all scanned roots)
    include_pattern: str = "*.{jpg,jpeg,png,heic,heif,tiff,tif,webp,mp4,mov,avi,mkv,mp3,m4a,wav,flac}"
    dry_run: bool = False


class CloudConnectRequest(BaseModel):
    provider_key: str
    name: str
    credentials: dict[str, str] = {}


# ── Helpers ───────────────────────────────────────────────────────────


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


# Cache for full cloud status response — invalidated after 30s
_cloud_status_cache: dict = {"data": None, "ts": 0.0}
_CLOUD_CACHE_TTL = 30.0

_remotes_cache: dict = {"data": None, "ts": 0.0}


# ── Endpoints ─────────────────────────────────────────────────────────


@router.get("/cloud/status")
def cloud_status(request: Request):
    """Get cloud storage status — rclone remotes + native paths, enriched with scan info."""
    from godmode_media_library.cloud import get_cloud_status  # Lazy import to avoid circular dependency

    # Return cached full response if fresh
    now = time.monotonic()
    if _cloud_status_cache["data"] and (now - _cloud_status_cache["ts"]) < _CLOUD_CACHE_TTL:
        return copy.deepcopy(_cloud_status_cache["data"])

    status = get_cloud_status()
    sources = status.get("sources", [])

    # Collect paths for parallel disk counting
    path_map: dict[int, str] = {}
    for i, src in enumerate(sources):
        p = src.get("mount_path") or src.get("sync_path") or ""
        if p and Path(p).is_dir():
            path_map[i] = p

    # Run all os.walk counts in parallel (instead of sequential 3s × N)
    disk_counts: dict[int, int] = {}
    if path_map:
        with ThreadPoolExecutor(max_workers=min(8, len(path_map))) as pool:
            futures = {pool.submit(_safe_disk_count, p): idx for idx, p in path_map.items()}
            for fut in futures:
                try:
                    disk_counts[futures[fut]] = fut.result(timeout=5)
                except Exception:
                    disk_counts[futures[fut]] = 0

    # Enrich with catalog data (single DB connection, fast queries)
    cat = _open_catalog(request)
    try:
        for i, src in enumerate(sources):
            path = src.get("mount_path") or src.get("sync_path") or ""
            if not path:
                src.update({"scanned": False, "last_scan": None, "file_count": 0, "disk_count": 0})
                continue
            count_row = cat.conn.execute(
                "SELECT COUNT(*) FROM files WHERE path LIKE ? || '%'",
                (path.rstrip("/"),),
            ).fetchone()
            src["file_count"] = count_row[0] if count_row else 0
            scan_row = cat.conn.execute(
                "SELECT MAX(finished_at) FROM scans WHERE root LIKE '%' || ? || '%'",
                (path.rstrip("/"),),
            ).fetchone()
            src["last_scan"] = scan_row[0] if scan_row and scan_row[0] else None
            src["scanned"] = src["file_count"] > 0
            src["disk_count"] = disk_counts.get(i, 0)
    finally:
        _return_catalog(cat)

    # Cache the full enriched response
    _cloud_status_cache["data"] = copy.deepcopy(status)
    _cloud_status_cache["ts"] = time.monotonic()

    return status


@router.get("/cloud/remotes")
def cloud_remotes():
    """List configured rclone remotes."""
    from godmode_media_library.cloud import check_rclone, list_remotes, rclone_version

    now = time.monotonic()
    if _remotes_cache["data"] and (now - _remotes_cache["ts"]) < _CLOUD_CACHE_TTL:
        return _remotes_cache["data"]

    if not check_rclone():
        return {"installed": False, "remotes": [], "version": None}
    result = {
        "installed": True,
        "version": rclone_version(),
        "remotes": [{"name": r.name, "type": r.type, "label": r.provider_label, "icon": r.icon} for r in list_remotes()],
    }
    _remotes_cache["data"] = result
    _remotes_cache["ts"] = now
    return result


@router.get("/cloud/native")
def cloud_native_paths(request: Request):
    """Detect natively synced cloud storage paths (iCloud, MEGA, pCloud, etc.)."""
    from godmode_media_library.cloud import detect_native_cloud_paths  # Lazy import to avoid circular dependency

    paths = detect_native_cloud_paths()

    # Parallel disk counting
    path_map: dict[int, str] = {}
    for i, p in enumerate(paths):
        pp = p.get("path", "")
        if pp and Path(pp).is_dir():
            path_map[i] = pp

    disk_counts: dict[int, int] = {}
    if path_map:
        with ThreadPoolExecutor(max_workers=min(8, len(path_map))) as pool:
            futures = {pool.submit(_safe_disk_count, pp): idx for idx, pp in path_map.items()}
            for fut in futures:
                try:
                    disk_counts[futures[fut]] = fut.result(timeout=5)
                except Exception:
                    disk_counts[futures[fut]] = 0

    # Enrich with scan info
    cat = _open_catalog(request)
    try:
        for i, p in enumerate(paths):
            path = p.get("path", "")
            if not path:
                p.update({"scanned": False, "file_count": 0, "disk_count": 0})
                continue
            count_row = cat.conn.execute(
                "SELECT COUNT(*) FROM files WHERE path LIKE ? || '%'",
                (path.rstrip("/"),),
            ).fetchone()
            p["file_count"] = count_row[0] if count_row else 0
            p["scanned"] = p["file_count"] > 0
            p["disk_count"] = disk_counts.get(i, 0)
    finally:
        _return_catalog(cat)

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
        raise HTTPException(500, "Nepodařilo se načíst obsah cloudu") from e


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
            _finish_task(
                task.id,
                result={
                    "remote": result.remote,
                    "local_path": result.local_path,
                    "files_transferred": result.files_transferred,
                    "errors": result.errors,
                    "elapsed_seconds": round(result.elapsed_seconds, 1),
                },
            )
        except Exception as exc:
            _finish_task(task.id, error=str(exc))
            logger.exception("Cloud sync task failed")

    background.add_task(_run)
    return {"task_id": task.id, "status": "started"}


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
            _return_catalog(cat)

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
        return {"mount_path": "", "success": False, "message": "Připojení se nezdařilo"}


@router.post("/cloud/unmount")
def cloud_unmount_remote(body: CloudMountRequest):
    """Unmount a FUSE mount."""
    from godmode_media_library.cloud import rclone_unmount

    mount_point = body.mount_point or str(Path.home() / "mnt" / body.remote)
    success = rclone_unmount(mount_point)
    return {"mount_point": mount_point, "success": success}


@router.get("/cloud/provider-fields/{provider_key}")
def cloud_provider_fields(provider_key: str):
    """Return the credential fields needed for a provider."""
    from godmode_media_library.cloud import PROVIDERS

    info = PROVIDERS.get(provider_key)
    if not info:
        raise HTTPException(404, f"Neznámý poskytovatel: {provider_key}")
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
        raise HTTPException(400, "Chybí OAuth token")
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
