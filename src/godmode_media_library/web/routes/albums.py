"""Smart Albums API endpoints: CRUD and dynamic file queries."""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from ..shared import _open_catalog, _return_catalog, _row_to_dict

router = APIRouter()


# ── Request models ────────────────────────────────────────────────────


class CreateAlbumRequest(BaseModel):
    name: str
    icon: str = ""
    filters: dict = {}


class UpdateAlbumRequest(BaseModel):
    name: str | None = None
    icon: str | None = None
    filters: dict | None = None


# ── Endpoints ─────────────────────────────────────────────────────────


@router.get("/albums")
def list_albums(request: Request) -> dict:
    """List all smart albums with file counts."""
    cat = _open_catalog(request)
    try:
        albums = cat.get_smart_albums()
        # Enrich each album with a file count and cover thumbnail
        result = []
        for album in albums:
            rows, total = cat.query_smart_album_files(album["id"], limit=1, offset=0)
            cover_path = rows[0].path if rows else None
            try:
                filters = json.loads(album["filters_json"])
            except (ValueError, TypeError):
                filters = {}
            result.append(
                {
                    "id": album["id"],
                    "name": album["name"],
                    "icon": album["icon"],
                    "filters": filters,
                    "file_count": total,
                    "cover_path": cover_path,
                    "created_at": album["created_at"],
                    "updated_at": album["updated_at"],
                }
            )
        return {"albums": result}
    finally:
        _return_catalog(cat)


@router.post("/albums")
def create_album(request: Request, body: CreateAlbumRequest) -> dict:
    """Create a new smart album."""
    if not body.name or not body.name.strip():
        raise HTTPException(status_code=400, detail="Album name is required")
    cat = _open_catalog(request)
    try:
        album = cat.create_smart_album(
            name=body.name.strip(),
            icon=body.icon,
            filters_json=json.dumps(body.filters),
        )
        album["filters"] = body.filters
        return album
    finally:
        _return_catalog(cat)


@router.get("/albums/{album_id}")
def get_album(request: Request, album_id: int) -> dict:
    """Get album details + first page of files."""
    cat = _open_catalog(request)
    try:
        album = cat.get_smart_album(album_id)
        if album is None:
            raise HTTPException(status_code=404, detail="Album not found")
        try:
            filters = json.loads(album["filters_json"])
        except (ValueError, TypeError):
            filters = {}
        rows, total = cat.query_smart_album_files(album_id, limit=100, offset=0)
        files = [_row_to_dict(r) for r in rows]
        return {
            "id": album["id"],
            "name": album["name"],
            "icon": album["icon"],
            "filters": filters,
            "file_count": total,
            "files": files,
            "created_at": album["created_at"],
            "updated_at": album["updated_at"],
        }
    finally:
        _return_catalog(cat)


@router.put("/albums/{album_id}")
def update_album(request: Request, album_id: int, body: UpdateAlbumRequest) -> dict:
    """Update a smart album."""
    cat = _open_catalog(request)
    try:
        existing = cat.get_smart_album(album_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Album not found")
        filters_json = json.dumps(body.filters) if body.filters is not None else None
        updated = cat.update_smart_album(
            album_id,
            name=body.name.strip() if body.name else None,
            icon=body.icon,
            filters_json=filters_json,
        )
        if not updated:
            raise HTTPException(status_code=400, detail="No changes provided")
        album = cat.get_smart_album(album_id)
        try:
            filters = json.loads(album["filters_json"])
        except (ValueError, TypeError):
            filters = {}
        album["filters"] = filters
        return album
    finally:
        _return_catalog(cat)


@router.delete("/albums/{album_id}")
def delete_album(request: Request, album_id: int) -> dict:
    """Delete a smart album."""
    cat = _open_catalog(request)
    try:
        deleted = cat.delete_smart_album(album_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Album not found")
        return {"deleted": True}
    finally:
        _return_catalog(cat)


@router.get("/albums/{album_id}/files")
def get_album_files(
    request: Request,
    album_id: int,
    limit: int = Query(default=100, le=10000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Get paginated files matching a smart album's filters."""
    cat = _open_catalog(request)
    try:
        album = cat.get_smart_album(album_id)
        if album is None:
            raise HTTPException(status_code=404, detail="Album not found")
        rows, total = cat.query_smart_album_files(album_id, limit=limit, offset=offset)
        files = [_row_to_dict(r) for r in rows]
        return {
            "files": files,
            "count": len(files),
            "total": total,
            "has_more": (offset + limit) < total,
        }
    finally:
        _return_catalog(cat)
