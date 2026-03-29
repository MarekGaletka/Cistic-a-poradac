from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from ..shared import _DEFAULT_SHARE_EXPIRY_HOURS, CreateShareRequest, _open_catalog, _sanitize_path, logger

router = APIRouter()

# ── Shares ─────────────────────────────────────────────────────────


@router.post("/shares")
def create_share(request: Request, body: CreateShareRequest) -> dict:
    """Create a share link for a file."""
    _sanitize_path(body.path, param_name="path")
    cat = _open_catalog(request)
    try:
        # Apply default expiration (7 days) unless caller explicitly set 0
        effective_expires = body.expires_hours
        if effective_expires is None:
            effective_expires = _DEFAULT_SHARE_EXPIRY_HOURS
        elif effective_expires == 0:
            effective_expires = None  # Caller explicitly requested no expiry
        share = cat.create_share(
            path=body.path,
            label=body.label,
            password=body.password,
            expires_hours=effective_expires,
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
    path = _sanitize_path(path, param_name="path")
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
