"""Search API endpoint for global file search."""

from __future__ import annotations

from fastapi import APIRouter, Query, Request

from ..shared import (
    _open_catalog,
    _return_catalog,
    _row_to_dict,
)

router = APIRouter()


@router.get("/search")
def search_files(
    request: Request,
    q: str = Query(default="", description="Search query"),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Global search across filenames, camera info, and dates."""
    if not q.strip():
        return {"items": [], "total": 0, "query": q}

    cat = _open_catalog(request)
    try:
        results, total = cat.search_files(query=q, limit=limit, offset=offset)
        items = [_row_to_dict(r) for r in results]
        return {
            "items": items,
            "total": total,
            "query": q,
            "limit": limit,
            "offset": offset,
        }
    finally:
        _return_catalog(cat)
