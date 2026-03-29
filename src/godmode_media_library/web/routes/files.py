"""File-related API endpoints: listing, favorites, notes, ratings, detail,
thumbnails, previews, quarantine, delete, rename, move, restore, browse,
roots, sources, and streaming.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse

from ...deps import resolve_bin
from ...disk_space import check_disk_space
from ..shared import (
    _DEFAULT_QUARANTINE_ROOT,
    DeleteRequest,
    FavoriteRequest,
    MoveRequest,
    NoteRequest,
    QuarantineRequest,
    RatingRequest,
    RemoveRootRequest,
    RenameRequest,
    RestoreRequest,
    RootsRequest,
    _check_path_within_roots,
    _get_bookmarks,
    _get_configured_roots,
    _get_favorites_list,
    _get_favorites_set,
    _is_path_allowed,
    _open_catalog,
    _row_to_dict,
    _sanitize_path,
    _set_configured_roots,
    _thumb_cache_dir,
    _thumb_cache_get,
    _thumb_cache_put,
    logger,
)

router = APIRouter()


# ── Local helpers (only used by files routes) ─────────────────────────


def _quarantine_dest(quarantine_root: Path, original_path: Path) -> Path:
    """Compute quarantine destination preserving absolute path structure."""
    rest = str(original_path).lstrip("/")
    return quarantine_root / rest


# ── Files listing ─────────────────────────────────────────────────────


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
            # Cap the SQL-side fetch; after in-memory filtering + pagination
            # we only need offset+limit rows, but must over-fetch to account
            # for rows filtered out by tag/favorites/rating/notes.  Use a
            # reasonable cap to avoid unbounded memory usage.
            _FULL_SCAN_CAP = 20000
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
                limit=_FULL_SCAN_CAP,
                offset=0,
            )
            # Apply tag filter
            if tag_id is not None:
                tag_rows = cat.query_files_by_tag(tag_id, limit=_FULL_SCAN_CAP, offset=0)
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


# ── Favorites ─────────────────────────────────────────────────────────


@router.post("/files/favorite")
def toggle_favorite(request: Request, body: FavoriteRequest) -> dict:
    """Toggle favorite status for a file (atomic read-modify-write)."""
    _sanitize_path(body.path, param_name="path")
    path = body.path
    cat = _open_catalog(request)
    try:
        cur = cat.conn.execute("SELECT value FROM meta WHERE key = 'favorites'")
        row = cur.fetchone()
        try:
            favorites = json.loads(row[0]) if row else []
        except (json.JSONDecodeError, TypeError):
            favorites = []
        if path in favorites:
            favorites.remove(path)
            is_favorite = False
        else:
            favorites.append(path)
            is_favorite = True
        cat.conn.execute(
            "INSERT INTO meta (key, value) VALUES ('favorites', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (json.dumps(favorites),),
        )
        cat.conn.commit()
    finally:
        cat.close()
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
    file_path = _sanitize_path(file_path, param_name="file_path")
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
    file_path = _sanitize_path(file_path, param_name="file_path")
    cat = _open_catalog(request)
    try:
        cat.set_file_note(f"/{file_path}", body.note)
        return {"saved": True}
    finally:
        cat.close()


@router.delete("/files/{file_path:path}/note")
def delete_file_note(request: Request, file_path: str) -> dict:
    """Remove a note from a file."""
    file_path = _sanitize_path(file_path, param_name="file_path")
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
    file_path = _sanitize_path(file_path, param_name="file_path")
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
    file_path = _sanitize_path(file_path, param_name="file_path")
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
    file_path = _sanitize_path(file_path, param_name="file_path")
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


# ── Thumbnails ────────────────────────────────────────────────────────


@router.get("/thumbnail/{file_path:path}")
def get_thumbnail(request: Request, file_path: str, size: int = Query(default=200, le=800)) -> StreamingResponse:
    """Generate and serve a thumbnail for an image file. Uses persistent disk cache."""
    file_path = _sanitize_path(file_path, param_name="file_path")
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
        from PIL import Image  # Lazy import: optional dependency, may not be installed
    except ImportError:
        raise HTTPException(status_code=500, detail="Pillow not installed") from None

    # Video thumbnail: extract a frame with ffmpeg
    if ext in video_exts:
        try:
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
            import pillow_heif  # Lazy import: optional dependency, may not be installed

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
    file_path = _sanitize_path(file_path, param_name="file_path")
    full_path = Path(f"/{file_path}").resolve()

    # Security: validate file is within managed roots and not in system dirs
    _check_path_within_roots(request, full_path)

    if not full_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    ext = full_path.suffix.lower()
    image_exts = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".gif", ".webp", ".heic", ".heif"}

    if ext not in image_exts:
        raise HTTPException(status_code=400, detail="Not a supported image")

    try:
        from PIL import Image  # Lazy import: optional dependency, may not be installed
    except ImportError:
        raise HTTPException(status_code=500, detail="Pillow not installed") from None

    if ext in (".heic", ".heif"):
        try:
            import pillow_heif  # Lazy import: optional dependency, may not be installed

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


# ── Quarantine / Delete / Rename / Move ──────────────────────────────


@router.post("/files/quarantine")
def quarantine_files(request: Request, body: QuarantineRequest) -> dict:
    """Move files to quarantine directory."""
    client_ip = request.client.host if request.client else "unknown"
    logger.warning("AUDIT: %s quarantine request for %d files", client_ip, len(body.paths))
    if body.quarantine_root:
        quarantine_root = Path(body.quarantine_root).resolve()
        # Security: quarantine root must be within managed roots and not a system directory
        try:
            _check_path_within_roots(request, quarantine_root)
        except HTTPException as exc:
            raise HTTPException(
                status_code=403,
                detail="Quarantine root must not be a system directory",
            ) from exc
    else:
        quarantine_root = _DEFAULT_QUARANTINE_ROOT
    cat = _open_catalog(request)
    moved = 0
    skipped = 0
    errors: list[str] = []
    try:
        for path_str in body.paths:
            try:
                path_str = _sanitize_path(path_str, param_name="file path")
            except HTTPException:
                skipped += 1
                errors.append(f"Invalid path: {path_str[:200]}")
                continue
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
                file_size = p.stat().st_size
            except OSError:
                file_size = 0
            if file_size and not check_disk_space(dest.parent, file_size):
                skipped += 1
                errors.append(f"Insufficient disk space to quarantine {path_str}")
                continue
            try:
                dest.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
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
    client_ip = request.client.host if request.client else "unknown"
    for path_str in body.paths:
        logger.warning("AUDIT: %s deleted file %s", client_ip, path_str)
    cat = _open_catalog(request)
    deleted = 0
    skipped = 0
    errors: list[str] = []
    try:
        for path_str in body.paths:
            try:
                path_str = _sanitize_path(path_str, param_name="file path")
            except HTTPException:
                skipped += 1
                errors.append(f"Invalid path: {path_str[:200]}")
                continue
            p = Path(path_str)
            # Verify file is within managed roots before allowing deletion
            try:
                _check_path_within_roots(request, p)
            except HTTPException:
                skipped += 1
                errors.append(f"Path outside managed roots: {path_str[:200]}")
                continue
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
            try:
                item.path = _sanitize_path(item.path, param_name="file path")
            except HTTPException:
                skipped += 1
                errors.append(f"Invalid path: {item.path[:200]}")
                continue
            # Validate new_name: no path separators or traversal sequences
            basename = os.path.basename(item.new_name)
            if (
                "/" in item.new_name
                or "\\" in item.new_name
                or ".." in item.new_name
                or not basename
                or basename != item.new_name
            ):
                skipped += 1
                errors.append(f"Invalid new name (path separators or traversal not allowed): {item.new_name[:200]}")
                continue
            p = Path(item.path)
            if not p.exists():
                skipped += 1
                errors.append(f"File not found: {item.path}")
                continue
            new_path = p.parent / basename
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
    _sanitize_path(body.destination, param_name="destination")
    dest_dir = Path(body.destination).resolve()

    # Security: validate destination is within managed roots and not a system directory
    _check_path_within_roots(request, dest_dir)

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
            try:
                path_str = _sanitize_path(path_str, param_name="file path")
            except HTTPException:
                skipped += 1
                errors.append(f"Invalid path: {path_str[:200]}")
                continue
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


# ── Restore from quarantine ──────────────────────────────────────────


@router.post("/files/restore")
def restore_files(request: Request, body: RestoreRequest) -> dict:
    """Restore files from quarantine."""
    client_ip = request.client.host if request.client else "unknown"
    logger.warning("AUDIT: %s restore request for %d files from quarantine", client_ip, len(body.paths))
    if body.quarantine_root:
        quarantine_root = Path(body.quarantine_root).resolve()
        try:
            _check_path_within_roots(request, quarantine_root)
        except HTTPException as exc:
            raise HTTPException(
                status_code=403,
                detail="Quarantine root must not be a system directory",
            ) from exc
    else:
        quarantine_root = _DEFAULT_QUARANTINE_ROOT
    cat = _open_catalog(request)
    restored = 0
    errors: list[str] = []
    try:
        for path_str in body.paths:
            try:
                path_str = _sanitize_path(path_str, param_name="file path")
            except HTTPException:
                errors.append(f"Invalid path: {path_str[:200]}")
                continue
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


# ── Browse filesystem ─────────────────────────────────────────────────


@router.get("/browse")
def browse_filesystem(
    path: str | None = Query(default=None),
) -> dict:
    """Browse filesystem directories for folder picker."""
    if path:
        path = _sanitize_path(path)
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


# ── Roots ─────────────────────────────────────────────────────────────


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
    _sanitize_path(body.path, param_name="path")
    roots = _get_configured_roots(request)
    path_to_remove = str(Path(body.path).resolve())
    roots = [r for r in roots if r != path_to_remove]
    _set_configured_roots(request, roots)
    return {"removed": True, "roots": roots}


# ── Sources ───────────────────────────────────────────────────────────


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
    file_path = _sanitize_path(file_path, param_name="file_path")
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

    return FileResponse(
        str(full_path),
        media_type=media_type,
        headers={"Cache-Control": "public, max-age=86400"},
    )
