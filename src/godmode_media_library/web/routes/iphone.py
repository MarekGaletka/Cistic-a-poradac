"""iPhone import API endpoints."""

from __future__ import annotations

import asyncio
import threading

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel, Field

from ..shared import (
    _create_task,
    _finish_task,
    _notify_ws,
    _update_progress,
    logger,
)

router = APIRouter()

_import_thread: threading.Thread | None = None


class IPhoneStartRequest(BaseModel):
    dest_remote: str = Field(default="gws-backup", min_length=1)
    dest_path: str = Field(default="GML-Consolidated", min_length=1)
    structure_pattern: str = "year_month"
    bwlimit: str | None = None
    media_only: bool = True


@router.get("/iphone/status")
async def iphone_status(request: Request):
    """Get iPhone connection status and import progress."""
    from ...iphone_import import get_iphone_status

    catalog_path = str(request.app.state.catalog_path)
    return get_iphone_status(catalog_path)


@router.get("/iphone/list")
async def iphone_list():
    """List media files on connected iPhone."""
    from ...iphone_import import list_iphone_files, _check_iphone_connected, _is_media

    if not _check_iphone_connected():
        raise HTTPException(status_code=404, detail="iPhone není připojen")

    try:
        files = await list_iphone_files()
        media_files = [f for f in files if _is_media(f.filename)]
        return {
            "total_files": len(files),
            "media_files": len(media_files),
            "total_size": sum(f.size for f in files),
            "media_size": sum(f.size for f in media_files),
            "folders": sorted({f.afc_path.split("/")[2] for f in files if "/" in f.afc_path}),
            "files": [
                {"name": f.filename, "path": f.afc_path, "size": f.size}
                for f in media_files[:100]  # Preview first 100
            ],
            "has_more": len(media_files) > 100,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chyba při čtení iPhone: {e}") from e


@router.post("/iphone/start")
async def iphone_start(req: IPhoneStartRequest, request: Request, bg: BackgroundTasks):
    """Start iPhone media import pipeline."""
    global _import_thread
    from ...iphone_import import (
        IPhoneImportConfig,
        _check_iphone_connected,
        get_progress,
        run_import,
    )

    if not _check_iphone_connected():
        raise HTTPException(status_code=404, detail="iPhone není připojen")

    # Check if already running
    progress = get_progress()
    if progress["phase"] in ("listing", "transferring"):
        raise HTTPException(status_code=409, detail="Import již běží")

    catalog_path = str(request.app.state.catalog_path)
    config = IPhoneImportConfig(
        dest_remote=req.dest_remote,
        dest_path=req.dest_path,
        structure_pattern=req.structure_pattern,
        bwlimit=req.bwlimit,
        media_only=req.media_only,
    )

    task = _create_task(f"iphone_import: {req.dest_remote}:{req.dest_path}")

    def _run():
        try:
            def _on_progress(prog):
                _update_progress(task.id, prog)
                _notify_ws(task.id, {"type": "iphone_progress", **prog})

            result = run_import(catalog_path, config, progress_fn=_on_progress)
            _finish_task(task.id, result)
        except Exception as e:
            logger.exception("iPhone import failed")
            _finish_task(task.id, {"error": str(e)}, error=str(e))

    _import_thread = threading.Thread(target=_run, daemon=True, name="iphone-import")
    _import_thread.start()

    return {"task_id": task.id, "status": "started"}


@router.post("/iphone/pause")
async def iphone_pause():
    """Pause running iPhone import."""
    from ...iphone_import import get_progress, pause_import

    progress = get_progress()
    if progress["phase"] not in ("listing", "transferring"):
        raise HTTPException(status_code=409, detail="Import neběží")

    pause_import()
    return {"status": "paused"}


@router.post("/iphone/resume")
async def iphone_resume(request: Request, bg: BackgroundTasks):
    """Resume paused iPhone import."""
    global _import_thread
    from ...iphone_import import (
        IPhoneImportConfig,
        get_progress,
        resume_import,
        run_import,
    )

    progress = get_progress()

    # If just paused in memory, unblock
    if progress["phase"] == "paused" and _import_thread and _import_thread.is_alive():
        resume_import()
        return {"status": "resumed"}

    # Otherwise, restart the pipeline (it will resume from checkpoint)
    catalog_path = str(request.app.state.catalog_path)
    config = IPhoneImportConfig()  # Uses defaults, job config is in checkpoint

    task = _create_task("iphone_import: resume")

    def _run():
        try:
            def _on_progress(prog):
                _update_progress(task.id, prog)
                _notify_ws(task.id, {"type": "iphone_progress", **prog})

            result = run_import(catalog_path, config, progress_fn=_on_progress)
            _finish_task(task.id, result)
        except Exception as e:
            logger.exception("iPhone import resume failed")
            _finish_task(task.id, {"error": str(e)}, error=str(e))

    _import_thread = threading.Thread(target=_run, daemon=True, name="iphone-import")
    _import_thread.start()

    return {"task_id": task.id, "status": "resumed"}


@router.get("/iphone/progress")
async def iphone_progress():
    """Get real-time import progress."""
    from ...iphone_import import get_progress
    return get_progress()
