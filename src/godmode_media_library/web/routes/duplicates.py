"""Duplicate and similarity endpoints."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from ...disk_space import check_disk_space
from ...metadata_richness import compute_group_diff
from ...perceptual_hash import find_similar
from ..shared import (
    _DEFAULT_QUARANTINE_ROOT,
    DuplicateKeepRequest,
    _open_catalog,
    _return_catalog,
    logger,
)

router = APIRouter()


def _quarantine_dest(quarantine_root: Path, original_path: Path) -> Path:
    """Compute quarantine destination preserving absolute path structure."""
    rest = str(original_path).lstrip("/")
    return quarantine_root / rest


@router.get("/duplicates")
def get_duplicates(
    request: Request,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """List duplicate groups."""
    cat = _open_catalog(request)
    try:
        total = cat.count_duplicate_groups()
        page_groups = cat.query_duplicates(limit=limit, offset=offset)
        groups = []
        for gid, rows in page_groups:
            groups.append(
                {
                    "group_id": gid,
                    "file_count": len(rows),
                    "total_size": sum(r.size for r in rows),
                    "files": [{"path": str(r.path), "size": r.size} for r in rows],
                }
            )
        return {"groups": groups, "total_groups": total}
    finally:
        _return_catalog(cat)


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
        _return_catalog(cat)


@router.get("/duplicates/{group_id}/diff")
def get_duplicate_diff(request: Request, group_id: str) -> dict:
    """Compute metadata diff for a duplicate group."""
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
        _return_catalog(cat)


@router.get("/similar")
def get_similar(
    request: Request,
    threshold: int = Query(default=10, ge=0, le=64),
    limit: int = Query(default=100, le=1000),
) -> dict:
    """Find visually similar files via perceptual hash."""
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
        _return_catalog(cat)


@router.post("/duplicates/{group_id}/quarantine")
def quarantine_duplicate_group(request: Request, group_id: str, body: DuplicateKeepRequest) -> dict:
    """Quarantine all files in a duplicate group except the keeper."""
    client_ip = request.client.host if request.client else "unknown"
    logger.warning("AUDIT: %s quarantine duplicate group %s (keep=%s)", client_ip, group_id, body.keep_path)
    quarantine_root = _DEFAULT_QUARANTINE_ROOT
    cat = _open_catalog(request)
    quarantined = 0
    errors: list[str] = []
    try:
        group_rows = cat.query_duplicate_group(group_id)
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
                file_size = p.stat().st_size
            except OSError:
                file_size = 0
            if file_size and not check_disk_space(dest.parent, file_size):
                errors.append(f"Insufficient disk space to quarantine {row.path}")
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
                cat.delete_file_by_path(row.path)
                quarantined += 1
            except OSError as e:
                errors.append(f"Failed to quarantine {row.path}: {e}")
        cat.commit()
    finally:
        _return_catalog(cat)
    result: dict[str, Any] = {"quarantined": quarantined, "kept": body.keep_path}
    if errors:
        result["errors"] = errors
    return result


@router.post("/duplicates/{group_id}/merge")
def merge_duplicate_group(request: Request, group_id: str, body: DuplicateKeepRequest) -> dict:
    """Merge metadata from all copies to keeper, then quarantine the rest."""
    from ...metadata_merge import create_merge_plan, execute_merge
    from ...metadata_richness import compute_group_diff

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
        group_rows = cat.query_duplicate_group(group_id)
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
                file_size = p.stat().st_size
            except OSError:
                file_size = 0
            if file_size and not check_disk_space(dest.parent, file_size):
                errors.append(f"Insufficient disk space to quarantine {row.path}")
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
                cat.delete_file_by_path(row.path)
                quarantined += 1
            except OSError as e:
                errors.append(f"Failed to quarantine {row.path}: {e}")
        cat.commit()
    finally:
        _return_catalog(cat)
    result: dict[str, Any] = {
        "merged": merge_applied,
        "quarantined": quarantined,
        "kept": body.keep_path,
    }
    if errors:
        result["errors"] = errors
    return result
