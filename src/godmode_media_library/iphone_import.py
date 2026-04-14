"""iPhone media import pipeline — transfer photos/videos from iPhone to Google Drive.

Uses pymobiledevice3 AFC protocol for USB access. Processes one file at a time
to minimize local disk usage. Full catalog integration with SHA-256, EXIF, thumbnails.
Supports resume after iPhone disconnect/reconnect.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from . import checkpoint as ckpt
from .catalog import Catalog, CatalogFileRow
from .cloud import rclone_copyto
from .exif_reader import ExifMeta, can_read_exif, read_exif
from .media_probe import probe_file
from .utils import sha256_file

logger = logging.getLogger(__name__)

DCIM_ROOT = "/DCIM"
IPHONE_JOB_TYPE = "iphone_import"
STEP_TRANSFER = "transfer"
TEMP_PREFIX = "gml-iphone-"
MIN_FREE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB minimum free space


MAX_UPLOAD_WORKERS = 3  # concurrent rclone uploads
MAX_PREFETCH = 5  # max files downloaded ahead of uploads


@dataclass
class IPhoneImportConfig:
    dest_remote: str = "gws-backup"
    dest_path: str = "GML-Consolidated"
    temp_dir: str = "/tmp/gml-iphone"
    structure_pattern: str = "year_month"  # year_month, flat, original
    bwlimit: str | None = None
    media_only: bool = True
    upload_workers: int = MAX_UPLOAD_WORKERS


@dataclass
class IPhoneFile:
    afc_path: str
    filename: str
    size: int
    mtime: float = 0.0


@dataclass
class ImportProgress:
    phase: str = "idle"  # idle, listing, transferring, paused, completed, error
    total_files: int = 0
    completed_files: int = 0
    failed_files: int = 0
    skipped_files: int = 0
    bytes_transferred: int = 0
    bytes_total: int = 0
    current_file: str = ""
    speed_bps: float = 0.0
    iphone_connected: bool = False
    error: str | None = None
    job_id: str | None = None


# ── Global state for in-process control ──────────────────────────────

_pause_event = threading.Event()
_cancel_event = threading.Event()
_progress = ImportProgress()
_progress_lock = threading.Lock()


def get_progress() -> dict:
    with _progress_lock:
        return {
            "phase": _progress.phase,
            "total_files": _progress.total_files,
            "completed_files": _progress.completed_files,
            "failed_files": _progress.failed_files,
            "skipped_files": _progress.skipped_files,
            "bytes_transferred": _progress.bytes_transferred,
            "bytes_total": _progress.bytes_total,
            "current_file": _progress.current_file,
            "speed_bps": _progress.speed_bps,
            "iphone_connected": _progress.iphone_connected,
            "error": _progress.error,
            "job_id": _progress.job_id,
        }


def pause_import():
    _pause_event.set()
    with _progress_lock:
        _progress.phase = "paused"


def resume_import():
    _pause_event.clear()


def cancel_import():
    _cancel_event.set()
    _pause_event.set()  # Unblock any pause wait


# ── iPhone connection helpers ────────────────────────────────────────

def _run_async(coro):
    """Run an async coroutine from sync context, handling nested event loops."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        # Already inside an event loop (e.g. FastAPI) — run in a new thread
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result(timeout=10)
    return asyncio.run(coro)


def _check_iphone_connected() -> bool:
    """Check if an iPhone is connected via USB."""
    try:
        from pymobiledevice3.usbmux import list_devices
        devices = _run_async(list_devices())
        return len(devices) > 0
    except Exception:
        return False


def _get_afc_service():
    """Create a new AFC service connection to the iPhone."""
    from pymobiledevice3.lockdown import create_using_usbmux
    from pymobiledevice3.services.afc import AfcService
    return create_using_usbmux, AfcService


async def list_iphone_files() -> list[IPhoneFile]:
    """List all media files in iPhone DCIM folder."""
    create_using_usbmux, AfcService = _get_afc_service()
    ld = await create_using_usbmux()
    files: list[IPhoneFile] = []

    async with AfcService(ld) as afc:
        folders = await afc.listdir(DCIM_ROOT + "/")
        for folder in sorted(folders):
            if folder.startswith("."):
                continue
            folder_path = f"{DCIM_ROOT}/{folder}"
            try:
                entries = await afc.listdir(folder_path + "/")
            except Exception as e:
                logger.warning("Cannot list %s: %s", folder_path, e)
                continue
            for entry in sorted(entries):
                if entry.startswith("."):
                    continue
                afc_path = f"{folder_path}/{entry}"
                try:
                    info = await afc.stat(afc_path)
                    size = info.get("st_size", 0)
                    mtime = info.get("st_mtime", 0.0)
                    files.append(IPhoneFile(
                        afc_path=afc_path,
                        filename=entry,
                        size=size,
                        mtime=mtime,
                    ))
                except Exception as e:
                    logger.warning("Cannot stat %s: %s", afc_path, e)

    logger.info("Found %d files on iPhone (%s)", len(files),
                _fmt_bytes(sum(f.size for f in files)))
    return files


async def _download_file(afc_path: str, dest: Path) -> bool:
    """Download a single file from iPhone via AFC."""
    create_using_usbmux, AfcService = _get_afc_service()
    ld = await create_using_usbmux()
    async with AfcService(ld) as afc:
        data = await afc.get_file_contents(afc_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
    return True


def _determine_dest_path(config: IPhoneImportConfig, exif: ExifMeta | None,
                         filename: str) -> str:
    """Calculate destination path based on EXIF date or filename."""
    import re
    from datetime import datetime

    date = None
    if exif and exif.date_original:
        try:
            date = datetime.strptime(exif.date_original, "%Y:%m:%d %H:%M:%S")
        except (ValueError, TypeError):
            pass

    if date is None:
        # Try parsing from filename: IMG_20210315_...
        m = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
        if m:
            try:
                date = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                pass

    if config.structure_pattern == "flat":
        return f"{config.dest_path}/{filename}"
    elif config.structure_pattern == "original":
        return f"{config.dest_path}/iPhone/{filename}"

    # Default: year_month
    if date:
        return f"{config.dest_path}/{date.year}/{date.year}-{date.month:02d}/{filename}"
    return f"{config.dest_path}/Unsorted/{filename}"


def _fmt_bytes(b: int) -> str:
    if b < 1024:
        return f"{b} B"
    if b < 1024 ** 2:
        return f"{b / 1024:.1f} KB"
    if b < 1024 ** 3:
        return f"{b / 1024 ** 2:.1f} MB"
    return f"{b / 1024 ** 3:.2f} GB"


MEDIA_EXTS = {
    ".jpg", ".jpeg", ".png", ".heic", ".heif", ".tif", ".tiff",
    ".gif", ".bmp", ".webp", ".dng", ".cr2", ".cr3", ".nef", ".arw", ".raw",
    ".mov", ".mp4", ".m4v", ".avi", ".mkv", ".3gp",
    ".mp3", ".m4a", ".aac", ".wav",
    ".aae",
}


def _is_media(filename: str) -> bool:
    return Path(filename).suffix.lower() in MEDIA_EXTS


# ── Main pipeline ────────────────────────────────────────────────────

def run_import(
    catalog_path: str,
    config: IPhoneImportConfig | None = None,
    progress_fn: Callable[[dict], None] | None = None,
) -> dict:
    """Main iPhone import pipeline. Runs synchronously (call from background thread).

    Returns summary dict with counts.
    """
    global _progress
    config = config or IPhoneImportConfig()
    _pause_event.clear()
    _cancel_event.clear()

    def _report(**kwargs):
        with _progress_lock:
            for k, v in kwargs.items():
                setattr(_progress, k, v)
        if progress_fn:
            progress_fn(get_progress())

    _report(phase="listing", iphone_connected=True, error=None)

    # 1. List files on iPhone
    try:
        iphone_files = asyncio.run(list_iphone_files())
    except Exception as e:
        _report(phase="error", error=f"Nelze se připojit k iPhone: {e}", iphone_connected=False)
        return {"error": str(e)}

    if config.media_only:
        iphone_files = [f for f in iphone_files if _is_media(f.filename)]

    total_bytes = sum(f.size for f in iphone_files)
    _report(
        total_files=len(iphone_files),
        bytes_total=total_bytes,
        phase="transferring",
    )

    # 2. Open catalog, create/resume job
    cat = Catalog(catalog_path)
    cat.open()
    temp_dir = Path(config.temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Find existing running/paused job or create new
        existing_jobs = ckpt.list_jobs(cat, status="running") + ckpt.list_jobs(cat, status="paused")
        iphone_jobs = [j for j in existing_jobs if j.job_type == IPHONE_JOB_TYPE]
        if iphone_jobs:
            job = iphone_jobs[0]
            ckpt.update_job(cat, job.job_id, status="running")
            logger.info("Resuming iPhone import job %s", job.job_id)
        else:
            job = ckpt.create_job(cat, IPHONE_JOB_TYPE, config={
                "dest_remote": config.dest_remote,
                "dest_path": config.dest_path,
                "structure_pattern": config.structure_pattern,
            })
            ckpt.update_job(cat, job.job_id, status="running")
            logger.info("Created new iPhone import job %s", job.job_id)

        _report(job_id=job.job_id)

        # 3. Get already completed files (for resume)
        progress = ckpt.get_job_progress(cat, job.job_id, STEP_TRANSFER)
        completed_sources = set()
        if progress.get("completed", 0) > 0:
            cur = cat.conn.cursor()
            cur.execute(
                "SELECT source_location FROM consolidation_file_state WHERE job_id = ? AND step_name = ? AND status = 'completed'",
                (job.job_id, STEP_TRANSFER),
            )
            completed_sources = {row[0] for row in cur.fetchall()}
            _report(
                completed_files=len(completed_sources),
                skipped_files=len(completed_sources),
                bytes_transferred=progress.get("total_bytes", 0),
            )
            logger.info("Resuming: %d files already completed", len(completed_sources))

        # 4. Process files with parallel pipeline:
        #    - Download from iPhone + hash/EXIF/phash (sequential, fast ~31 MB/s)
        #    - Upload via rclone (concurrent pool, bottleneck ~1 MB/s each)
        import concurrent.futures
        from .utils import utc_stamp

        start_time = time.monotonic()
        bytes_done = _progress.bytes_transferred
        upload_semaphore = threading.Semaphore(MAX_PREFETCH)
        catalog_lock = threading.Lock()  # Serialize catalog writes
        abort = False

        def _upload_one(prepared):
            """Upload a prepared file to Google Drive + catalog. Runs in thread pool."""
            nonlocal bytes_done
            ifile, local_temp, file_hash, exif, probe, phash, dest_p = prepared
            try:
                rclone_result = rclone_copyto(
                    src_remote="",
                    src_path=str(local_temp),
                    dst_remote=config.dest_remote,
                    dst_path=dest_p,
                    bwlimit=config.bwlimit,
                    file_size=ifile.size,
                )
                if not rclone_result.get("success", False):
                    raise RuntimeError(rclone_result.get("error", "rclone upload failed"))

                now_ts = time.time()
                now_str = utc_stamp()
                row = CatalogFileRow(
                    id=None,
                    path=f"{config.dest_remote}:{dest_p}",
                    size=ifile.size,
                    mtime=ifile.mtime or now_ts,
                    ctime=now_ts,
                    birthtime=ifile.mtime or now_ts,
                    ext=local_temp.suffix.lower().lstrip("."),
                    sha256=file_hash,
                    inode=None, device=None, nlink=1,
                    asset_key=None, asset_component=False, xattr_count=0,
                    first_seen=now_str, last_scanned=now_str,
                    duration_seconds=probe.duration_seconds if probe else None,
                    width=probe.width if probe else None,
                    height=probe.height if probe else None,
                    video_codec=probe.video_codec if probe else None,
                    audio_codec=probe.audio_codec if probe else None,
                    bitrate=probe.bitrate if probe else None,
                    phash=str(phash) if phash else None,
                    date_original=exif.date_original if exif else None,
                    camera_make=exif.camera_make if exif else None,
                    camera_model=exif.camera_model if exif else None,
                    gps_latitude=exif.gps_latitude if exif else None,
                    gps_longitude=exif.gps_longitude if exif else None,
                )
                with catalog_lock:
                    cat.upsert_file(row)
                    ckpt.mark_file(cat, job.job_id, ifile.afc_path, ifile.afc_path,
                                   STEP_TRANSFER, "completed",
                                   dest=f"{config.dest_remote}:{dest_p}",
                                   bytes_transferred=ifile.size)

                bytes_done += ifile.size
                elapsed = time.monotonic() - start_time
                speed = bytes_done / elapsed if elapsed > 0 else 0

                with _progress_lock:
                    _progress.completed_files += 1
                    _progress.bytes_transferred = bytes_done
                    _progress.speed_bps = speed

                if progress_fn:
                    progress_fn(get_progress())

                logger.info("Transferred %s → %s:%s (%s)",
                            ifile.filename, config.dest_remote, dest_p,
                            _fmt_bytes(ifile.size))

            except Exception as e:
                logger.warning("Upload failed %s: %s", ifile.filename, e)
                with catalog_lock:
                    ckpt.mark_file(cat, job.job_id, ifile.afc_path, ifile.afc_path,
                                   STEP_TRANSFER, "failed", error=str(e))
                with _progress_lock:
                    _progress.failed_files += 1
            finally:
                _cleanup_temp(local_temp)
                upload_semaphore.release()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=config.upload_workers, thread_name_prefix="iphone-upload"
        ) as upload_pool:
            futures: list[concurrent.futures.Future] = []

            for i, ifile in enumerate(iphone_files):
                if abort:
                    break

                # Check pause/cancel
                while _pause_event.is_set() and not _cancel_event.is_set():
                    _report(phase="paused")
                    time.sleep(1)
                if _cancel_event.is_set():
                    _report(phase="paused")
                    ckpt.update_job(cat, job.job_id, status="paused")
                    abort = True
                    break

                # Skip already completed
                if ifile.afc_path in completed_sources:
                    continue

                _report(current_file=ifile.filename, phase="transferring")

                # Check disk space (account for prefetched files)
                disk = shutil.disk_usage(str(temp_dir))
                if disk.free < max(MIN_FREE_BYTES, ifile.size * 2):
                    _report(phase="error",
                            error=f"Nedostatek místa na disku ({_fmt_bytes(disk.free)} volných)")
                    ckpt.update_job(cat, job.job_id, status="paused", error="Nedostatek místa")
                    abort = True
                    break

                # Check iPhone still connected
                if not _check_iphone_connected():
                    _report(phase="paused", iphone_connected=False,
                            error="iPhone odpojen — připojte jej a pokračujte")
                    ckpt.update_job(cat, job.job_id, status="paused",
                                    error="iPhone disconnected")
                    while not _check_iphone_connected() and not _cancel_event.is_set():
                        time.sleep(5)
                    if _cancel_event.is_set():
                        abort = True
                        break
                    _report(phase="transferring", iphone_connected=True, error=None)
                    ckpt.update_job(cat, job.job_id, status="running")

                # ── Stage 1: Download + prepare (sequential, fast) ──
                # Semaphore limits prefetched files on disk (released after upload)
                upload_semaphore.acquire()
                local_temp = temp_dir / ifile.filename
                try:
                    with catalog_lock:
                        ckpt.mark_file(cat, job.job_id, ifile.afc_path, ifile.afc_path,
                                       STEP_TRANSFER, "in_progress")
                    asyncio.run(_download_file(ifile.afc_path, local_temp))
                except Exception as e:
                    logger.warning("Download failed %s: %s", ifile.filename, e)
                    with catalog_lock:
                        ckpt.mark_file(cat, job.job_id, ifile.afc_path, ifile.afc_path,
                                       STEP_TRANSFER, "failed", error=str(e))
                    with _progress_lock:
                        _progress.failed_files += 1
                    _cleanup_temp(local_temp)
                    upload_semaphore.release()
                    continue

                try:
                    # SHA-256 hash
                    file_hash = sha256_file(local_temp)

                    # Dedup check
                    with catalog_lock:
                        existing = cat.get_file_by_hash(file_hash) if hasattr(cat, "get_file_by_hash") else None
                    if existing:
                        logger.info("Dedup skip: %s (hash %s)", ifile.filename, file_hash[:12])
                        with catalog_lock:
                            ckpt.mark_file(cat, job.job_id, ifile.afc_path, ifile.afc_path,
                                           STEP_TRANSFER, "skipped", dest="dedup")
                        with _progress_lock:
                            _progress.skipped_files += 1
                            _progress.completed_files += 1
                        _cleanup_temp(local_temp)
                        upload_semaphore.release()
                        continue

                    # EXIF
                    exif = None
                    if can_read_exif(local_temp.suffix.lstrip(".")):
                        try:
                            exif = read_exif(local_temp)
                        except Exception:
                            pass

                    # Media probe
                    probe = None
                    try:
                        probe = probe_file(local_temp)
                    except Exception:
                        pass

                    # Perceptual hash
                    phash = None
                    try:
                        from .perceptual_hash import dhash, is_image_ext
                        from .video_hash import video_dhash
                        ext = local_temp.suffix.lower().lstrip(".")
                        if is_image_ext(ext):
                            phash = dhash(local_temp)
                        elif ext in ("mp4", "mov", "m4v", "avi", "mkv"):
                            phash = video_dhash(local_temp)
                    except Exception:
                        pass

                    # Destination path
                    dest_path = _determine_dest_path(config, exif, ifile.filename)

                except Exception as e:
                    logger.warning("Prepare failed %s: %s", ifile.filename, e)
                    with catalog_lock:
                        ckpt.mark_file(cat, job.job_id, ifile.afc_path, ifile.afc_path,
                                       STEP_TRANSFER, "failed", error=str(e))
                    with _progress_lock:
                        _progress.failed_files += 1
                    _cleanup_temp(local_temp)
                    upload_semaphore.release()
                    continue

                # ── Stage 2: Submit to upload pool (semaphore already held) ──
                prepared = (ifile, local_temp, file_hash, exif, probe, phash, dest_path)
                futures.append(upload_pool.submit(_upload_one, prepared))

            # Wait for all uploads to finish
            for fut in concurrent.futures.as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    logger.error("Upload future error: %s", e)

        # 5. Complete job
        if not _cancel_event.is_set():
            ckpt.update_job(cat, job.job_id, status="completed")
            _report(phase="completed")
        else:
            _report(phase="paused")

    finally:
        cat.close()

    return get_progress()


def _cleanup_temp(path: Path):
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


def get_iphone_status(catalog_path: str) -> dict:
    """Get current iPhone import status for the FE."""
    connected = _check_iphone_connected()
    result = {
        "connected": connected,
        "device_name": None,
        "progress": get_progress(),
        "jobs": [],
    }

    if connected:
        try:
            from pymobiledevice3.usbmux import list_devices
            devices = _run_async(list_devices())
            if devices:
                result["device_name"] = getattr(devices[0], "name", "iPhone")
        except Exception:
            pass

    try:
        cat = Catalog(catalog_path)
        cat.open()
        try:
            jobs = ckpt.list_jobs(cat)
            result["jobs"] = [
                {
                    "job_id": j.job_id,
                    "status": j.status,
                    "created_at": j.created_at,
                    "updated_at": j.updated_at,
                    "progress": ckpt.get_job_progress(cat, j.job_id, STEP_TRANSFER),
                }
                for j in jobs
                if j.job_type == IPHONE_JOB_TYPE
            ]
        finally:
            cat.close()
    except Exception:
        pass

    return result
