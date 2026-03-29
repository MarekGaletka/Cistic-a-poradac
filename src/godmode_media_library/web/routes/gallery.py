from __future__ import annotations

import random
import sqlite3

from fastapi import APIRouter, HTTPException, Query, Request

from ..shared import _sanitize_path

router = APIRouter()

# ── Gallery ───────────────────────────────────────────────────────────


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
    file_path = _sanitize_path(file_path, param_name="file_path")

    from godmode_media_library.media_score import score_file  # Lazy import to avoid circular dependency

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
        raise HTTPException(status_code=404, detail="File not found in catalog")

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
    from godmode_media_library.media_score import get_smart_collections, score_catalog  # Lazy import to avoid circular dependency

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
