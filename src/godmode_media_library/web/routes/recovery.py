from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel

from ..shared import (
    _check_path_within_roots,
    _create_task,
    _finish_task,
    _sanitize_path,
    _update_progress,
    logger,
)

router = APIRouter()


# ── Pydantic models ──────────────────────────────────────────────────


class AppMineRequest(BaseModel):
    app_ids: list[str] | None = None


class QuarantineRestoreRequest(BaseModel):
    paths: list[str]
    restore_to: str | None = None


class QuarantineDeleteRequest(BaseModel):
    paths: list[str]


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


class SignalDecryptRequest(BaseModel):
    destination: str


# ── Recovery endpoints ───────────────────────────────────────────────


@router.get("/recovery/apps")
def get_available_apps_endpoint():
    """List all known apps and whether they have data present."""
    from ...recovery import get_available_apps

    return {"apps": get_available_apps()}


@router.post("/recovery/app-mine")
def start_app_mine(background_tasks: BackgroundTasks, body: AppMineRequest):
    """Mine media from selected app data directories (background task)."""
    from ...recovery import mine_app_media

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
    from ...recovery import list_quarantine

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
    from ...recovery import restore_from_quarantine

    for p in body.paths:
        _sanitize_path(p, param_name="path")
    if body.restore_to:
        _sanitize_path(body.restore_to, param_name="restore_to")
        # Only block truly dangerous system dirs for restore (user chooses destination)
        _RESTORE_BLOCKED = ("/etc", "/sbin", "/usr/bin", "/usr/sbin", "/bin", "/dev", "/proc", "/sys")
        resolved_dest = str(Path(body.restore_to).resolve())
        for bp in _RESTORE_BLOCKED:
            if resolved_dest == bp or resolved_dest.startswith(bp + "/"):
                raise HTTPException(status_code=403, detail=f"Nelze obnovit do syst\u00e9mov\u00e9ho adres\u00e1\u0159e: {bp}")
    qroot = getattr(request.app.state, "quarantine_root", None)
    return restore_from_quarantine(
        body.paths,
        quarantine_root=Path(qroot) if qroot else None,
        restore_to=body.restore_to,
    )


@router.post("/recovery/quarantine/delete")
def delete_quarantine(request: Request, body: QuarantineDeleteRequest):
    """Permanently delete files from quarantine."""
    from ...recovery import delete_from_quarantine

    for p in body.paths:
        _sanitize_path(p, param_name="path")
    qroot = getattr(request.app.state, "quarantine_root", None)
    return delete_from_quarantine(body.paths, quarantine_root=Path(qroot) if qroot else None)


@router.post("/recovery/deep-scan")
def start_deep_scan(request: Request, background_tasks: BackgroundTasks):
    """Start a deep scan for hidden/lost media files (background task)."""
    from ...recovery import deep_scan

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
def start_recover_files(request: Request, body: RecoverFilesRequest):
    """Copy/move found files to a recovery destination."""
    from ...recovery import recover_files

    _sanitize_path(body.destination, param_name="destination")
    _check_path_within_roots(request, Path(body.destination).resolve())
    for p in body.paths:
        _sanitize_path(p, param_name="path")
    return recover_files(body.paths, body.destination, body.delete_source)


@router.post("/recovery/integrity-check")
def start_integrity_check(request: Request, background_tasks: BackgroundTasks):
    """Check integrity of all cataloged media files (background task)."""
    from ...recovery import check_integrity

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
def repair_single_file(request: Request, body: RepairRequest):
    """Attempt to repair a single corrupted file."""
    from ...recovery import repair_file

    _sanitize_path(body.path, param_name="path")
    _check_path_within_roots(request, Path(body.path).resolve())
    return repair_file(body.path)


@router.get("/recovery/photorec/status")
def photorec_status():
    """Check if PhotoRec is available."""
    from ...recovery import check_photorec

    return check_photorec()


@router.get("/recovery/disks")
def get_disks():
    """List available disks for recovery."""
    from ...recovery import list_disks

    return {"disks": list_disks()}


@router.post("/recovery/photorec/run")
def start_photorec(request: Request, background_tasks: BackgroundTasks, body: PhotoRecRequest):
    """Start a PhotoRec recovery run (background task)."""
    from ...recovery import check_photorec, run_photorec

    _sanitize_path(body.source, param_name="source")
    if body.output_dir:
        _sanitize_path(body.output_dir, param_name="output_dir")
        _check_path_within_roots(request, Path(body.output_dir).resolve())

    check = check_photorec()
    if not check["available"]:
        raise HTTPException(status_code=400, detail="PhotoRec nen\u00ed nainstalov\u00e1n. Spus\u0165te: brew install testdisk")

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


# ── Signal endpoints ─────────────────────────────────────────────────


@router.get("/recovery/signal/status")
def signal_decrypt_status():
    """Check if Signal decryption is possible."""
    from ...recovery import check_signal_decrypt

    return check_signal_decrypt()


@router.post("/recovery/signal/decrypt")
def start_signal_decrypt(
    request: Request,
    background_tasks: BackgroundTasks,
    body: SignalDecryptRequest,
):
    """Decrypt Signal attachments and save to destination (background task)."""
    from ...recovery import decrypt_signal_attachments

    _sanitize_path(body.destination, param_name="destination")
    _check_path_within_roots(request, Path(body.destination).resolve())

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
