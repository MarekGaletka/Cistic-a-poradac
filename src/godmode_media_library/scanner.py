"""Incremental filesystem scanner backed by the SQLite catalog."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .catalog import Catalog, CatalogFileRow, ScanStats
from .exif_reader import ExifMeta, can_read_exif, read_exif
from .media_probe import MediaMeta, is_media_ext, probe_file
from .perceptual_hash import dhash, is_image_ext
from .utils import iter_files, meaningful_xattr_count, safe_stat_birthtime, sha256_file
from .video_hash import is_video_ext, video_dhash

logger = logging.getLogger(__name__)


_BATCH_SIZE = 500  # Files per batch — commit after each batch to survive disk failures


class _DiskKeepAlive:
    """Periodically read from disk roots to prevent USB HDD firmware sleep."""

    def __init__(self, roots: list[Path], interval: float = 5.0):
        self._roots = roots
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self):
        import os

        while not self._stop.wait(self._interval):
            for root in self._roots:
                try:
                    # Read directory listing to generate disk I/O
                    entries = os.listdir(root)
                    # Also read first few bytes of a real file to force platter activity
                    if entries:
                        for entry in entries[:3]:
                            fpath = os.path.join(str(root), entry)
                            if os.path.isfile(fpath):
                                with open(fpath, "rb") as f:
                                    f.read(512)
                                break
                except OSError:
                    pass


def _process_batch(
    batch_infos: list[dict],
    *,
    effective_workers: int,
    extract_media: bool,
    compute_phash: bool,
    min_size_bytes: int,
    stats: ScanStats,
) -> None:
    """Hash, probe media, and compute phash for a batch of file_infos in-place."""

    paths_to_hash = []
    paths_for_media = []
    paths_for_phash = []

    for i, info in enumerate(batch_infos):
        if info["needs_hash"] and info["size"] >= min_size_bytes:
            paths_to_hash.append((i, info["path"], info["size"]))
        if info["is_new_or_changed"] and extract_media:
            paths_for_media.append((i, info["path"], info["ext"]))
        if info["is_new_or_changed"] and compute_phash and (is_image_ext(info["ext"]) or is_video_ext(info["ext"])):
            paths_for_phash.append((i, info["path"], info["ext"]))

    # SHA-256 hashing
    if paths_to_hash:
        if effective_workers > 1:
            with ThreadPoolExecutor(max_workers=effective_workers) as pool:
                futures = {pool.submit(sha256_file, p): (idx, sz) for idx, p, sz in paths_to_hash}
                for future in as_completed(futures):
                    idx, sz = futures[future]
                    try:
                        batch_infos[idx]["sha256"] = future.result()
                        stats.bytes_hashed += sz
                    except OSError:
                        logger.warning("Cannot hash %s", batch_infos[idx]["path"])
        else:
            for idx, p, sz in paths_to_hash:
                try:
                    batch_infos[idx]["sha256"] = sha256_file(p)
                    stats.bytes_hashed += sz
                except OSError:
                    logger.warning("Cannot hash %s", p)

    # Media probe + EXIF
    if paths_for_media:

        def _extract(idx: int, p: Path, ext: str) -> tuple[int, MediaMeta | None, ExifMeta | None]:
            mm = probe_file(p) if is_media_ext(ext) else None
            em = read_exif(p) if can_read_exif(ext) else None
            return idx, mm, em

        if effective_workers > 1:
            with ThreadPoolExecutor(max_workers=effective_workers) as pool:
                futs = [pool.submit(_extract, idx, p, ext) for idx, p, ext in paths_for_media]
                for future in as_completed(futs):
                    try:
                        idx, mm, em = future.result()
                    except Exception:
                        logger.warning("Media extraction failed, skipping", exc_info=True)
                        continue
                    batch_infos[idx]["media_meta"] = mm
                    batch_infos[idx]["exif_meta"] = em
        else:
            for idx, p, ext in paths_for_media:
                _, mm, em = _extract(idx, p, ext)
                batch_infos[idx]["media_meta"] = mm
                batch_infos[idx]["exif_meta"] = em

    # Perceptual hash
    def _phash(p: Path, ext: str) -> str | None:
        if is_image_ext(ext):
            return dhash(p)
        if is_video_ext(ext):
            return video_dhash(p)
        return None

    if paths_for_phash:
        if effective_workers > 1:
            with ThreadPoolExecutor(max_workers=effective_workers) as pool:
                futs = {pool.submit(_phash, p, ext): idx for idx, p, ext in paths_for_phash}
                for future in as_completed(futs):
                    idx = futs[future]
                    try:
                        batch_infos[idx]["phash"] = future.result()
                    except Exception:
                        logger.warning("Perceptual hash failed for %s", batch_infos[idx]["path"], exc_info=True)
        else:
            for idx, p, ext in paths_for_phash:
                batch_infos[idx]["phash"] = _phash(p, ext)


def _upsert_batch(catalog: Catalog, batch_infos: list[dict]) -> None:
    """Upsert a batch of file_infos into the catalog and commit."""
    for info in batch_infos:
        row = CatalogFileRow(
            id=None,
            path=info["path_str"],
            size=info["size"],
            mtime=info["mtime"],
            ctime=info["ctime"],
            birthtime=info["birthtime"],
            ext=info["ext"],
            sha256=info["sha256"],
            inode=info["inode"],
            device=info["device"],
            nlink=info["nlink"],
            asset_key=(f"{info['path'].parent}\t{info['path'].stem}" if info["asset_key"] else None),
            asset_component=info["is_component"],
            xattr_count=info["xattr"],
            first_seen="",
            last_scanned="",
        )

        media_meta = info["media_meta"]
        exif_meta = info["exif_meta"]
        phash_val = info["phash"]

        if media_meta:
            row.duration_seconds = media_meta.duration_seconds
            row.video_codec = media_meta.video_codec
            row.audio_codec = media_meta.audio_codec
            row.bitrate = media_meta.bitrate
            if media_meta.width:
                row.width = media_meta.width
            if media_meta.height:
                row.height = media_meta.height

        if exif_meta:
            row.date_original = exif_meta.date_original
            row.camera_make = exif_meta.camera_make
            row.camera_model = exif_meta.camera_model
            if exif_meta.image_width and not row.width:
                row.width = exif_meta.image_width
            if exif_meta.image_height and not row.height:
                row.height = exif_meta.image_height
            row.gps_latitude = exif_meta.gps_latitude
            row.gps_longitude = exif_meta.gps_longitude

        if phash_val:
            row.phash = phash_val

        catalog.upsert_file(row)
    catalog.commit()


def incremental_scan(
    catalog: Catalog,
    roots: list[Path],
    *,
    force_rehash: bool = False,
    min_size_bytes: int = 0,
    extract_media: bool = True,
    compute_phash: bool = True,
    extract_exiftool: bool = False,
    exiftool_bin: str = "exiftool",
    workers: int = 1,
    progress_callback: Callable[[dict], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> ScanStats:
    """Scan filesystem roots and update catalog incrementally.

    Processes files in batches of _BATCH_SIZE, committing each batch to the
    database immediately. This ensures that if the disk disconnects or the
    process is killed, already-scanned files are preserved in the catalog.

    Only computes SHA-256 for files whose mtime or size changed since last scan,
    or for newly discovered files. Detects deletions (files in catalog but gone
    from disk).
    """
    stats = ScanStats(root=";".join(str(r) for r in roots))
    effective_workers = max(1, workers)

    # Keep USB HDDs awake during long hash operations
    keepalive = _DiskKeepAlive(roots)
    keepalive.start()

    # Start scan record
    scan_id = catalog.start_scan(stats.root)

    # ── Pre-load existing mtime/size for all roots (batch, avoids N+1) ──
    _existing_mtime_size: dict[str, tuple[float, int]] = {}
    for root in roots:
        _existing_mtime_size.update(catalog.get_all_mtime_size_for_root(str(root)))

    # ── Stream files in batches — stat, hash, extract, upsert, commit ──
    seen_paths: set[str] = set()
    batch: list[dict] = []
    total_discovered = 0
    batches_committed = 0

    if progress_callback:
        progress_callback({"phase": "scanning", "total": 0, "processed": 0})

    _SQLITE_MAX_INT = 2**63 - 1

    for path in iter_files(roots):
        if cancel_event and cancel_event.is_set():
            logger.info("Scan cancelled by pause signal")
            if batch:
                _process_batch(
                    batch,
                    effective_workers=effective_workers,
                    extract_media=extract_media,
                    compute_phash=compute_phash,
                    min_size_bytes=min_size_bytes,
                    stats=stats,
                )
                _upsert_batch(catalog, batch)
            catalog.commit()
            return stats

        total_discovered += 1

        try:
            st = path.stat()
        except OSError:
            logger.debug("Cannot stat %s, skipping", path)
            continue

        path_str = str(path)
        seen_paths.add(path_str)
        stats.files_scanned += 1

        size = min(int(st.st_size), _SQLITE_MAX_INT)
        mtime = float(st.st_mtime)
        ctime = float(st.st_ctime)
        ext = path.suffix.lower().lstrip(".")
        birthtime = safe_stat_birthtime(path)
        xattr = meaningful_xattr_count(path)
        inode = min(int(st.st_ino), _SQLITE_MAX_INT)
        device = min(int(st.st_dev), _SQLITE_MAX_INT)
        nlink = min(int(st.st_nlink), _SQLITE_MAX_INT)

        existing = _existing_mtime_size.get(path_str)
        needs_hash = force_rehash
        is_new_or_changed = False

        if existing is None:
            stats.files_new += 1
            needs_hash = True
            is_new_or_changed = True
        elif existing[0] != mtime or existing[1] != size:
            stats.files_changed += 1
            needs_hash = True
            is_new_or_changed = True
        elif not existing[2]:
            # File exists but has no hash — need to compute it
            needs_hash = True

        info = {
            "path": path,
            "path_str": path_str,
            "size": size,
            "mtime": mtime,
            "ctime": ctime,
            "ext": ext,
            "birthtime": birthtime,
            "xattr": xattr,
            "inode": inode,
            "device": device,
            "nlink": nlink,
            "asset_key": None,
            "is_component": False,
            "needs_hash": needs_hash,
            "is_new_or_changed": is_new_or_changed,
            "sha256": None,
            "media_meta": None,
            "exif_meta": None,
            "phash": None,
        }
        batch.append(info)

        if len(batch) >= _BATCH_SIZE:
            if progress_callback:
                progress_callback({"phase": "scanning", "total": total_discovered, "processed": stats.files_scanned})
            _process_batch(
                batch,
                effective_workers=effective_workers,
                extract_media=extract_media,
                compute_phash=compute_phash,
                min_size_bytes=min_size_bytes,
                stats=stats,
            )
            _upsert_batch(catalog, batch)
            batches_committed += 1
            logger.info("Batch %d committed (%d files so far)", batches_committed, stats.files_scanned)
            batch = []

    # Flush remaining batch
    if batch:
        if progress_callback:
            progress_callback({"phase": "scanning", "total": total_discovered, "processed": stats.files_scanned})
        _process_batch(
            batch,
            effective_workers=effective_workers,
            extract_media=extract_media,
            compute_phash=compute_phash,
            min_size_bytes=min_size_bytes,
            stats=stats,
        )
        _upsert_batch(catalog, batch)
        batches_committed += 1
        batch = []

    logger.info("All %d batches committed, %d files total", batches_committed, stats.files_scanned)

    # Detect removals — query only paths under scanned roots (avoids loading entire catalog)
    # Skip removal detection for roots that are not accessible (e.g. disconnected USB disks)
    catalog_paths_in_scope: set[str] = set()
    for root in roots:
        root_path = Path(root) if not isinstance(root, Path) else root
        if not root_path.exists() or not root_path.is_dir():
            logger.warning("Root %s not accessible — skipping removal detection for this root", root)
            continue
        prefix = str(root).rstrip("/") + "/"
        cur = catalog.conn.execute("SELECT path FROM files WHERE path LIKE ? || '%'", (prefix,))
        for row in cur:
            catalog_paths_in_scope.add(row[0])
    removed_paths = catalog_paths_in_scope - seen_paths

    if removed_paths:
        # Safety check: if >50% of catalog paths would be removed, likely a disconnect — skip
        if len(removed_paths) > len(catalog_paths_in_scope) * 0.5 and len(removed_paths) > 1000:
            logger.warning(
                "Skipping removal of %d/%d files — likely disconnected disk, not actual deletions",
                len(removed_paths),
                len(catalog_paths_in_scope),
            )
        else:
            stats.files_removed = catalog.mark_removed(list(removed_paths))
            logger.info("Removed %d files no longer on disk", stats.files_removed)

    # Detect duplicates from catalog
    _update_duplicate_groups(catalog)

    # Optional deep ExifTool extraction for files without metadata
    if extract_exiftool:
        _run_exiftool_extraction(catalog, exiftool_bin)

    # Backfill date_original from filesystem dates for files that still lack it
    _backfill_dates_from_filesystem(catalog)

    catalog.commit()
    catalog.finish_scan(scan_id, stats)
    keepalive.stop()

    logger.info(
        "Scan complete: %d scanned, %d new, %d changed, %d removed, %d bytes hashed",
        stats.files_scanned,
        stats.files_new,
        stats.files_changed,
        stats.files_removed,
        stats.bytes_hashed,
    )
    return stats


def _run_exiftool_extraction(catalog: Catalog, exiftool_bin: str = "exiftool") -> int:
    """Run batch ExifTool extraction for catalog files without metadata.

    Returns number of files with metadata extracted.
    """
    import json

    from .exiftool_extract import extract_all_metadata
    from .metadata_richness import compute_richness

    paths_needing = [Path(p) for p in catalog.paths_without_metadata()]
    if not paths_needing:
        logger.debug("All catalog files already have ExifTool metadata")
        return 0

    logger.info("Extracting ExifTool metadata for %d files", len(paths_needing))
    all_meta = extract_all_metadata(paths_needing, bin_path=exiftool_bin)
    extracted = 0
    for path, meta in all_meta.items():
        richness = compute_richness(meta)
        catalog.upsert_file_metadata(str(path), json.dumps(meta))
        catalog.update_metadata_richness(str(path), richness.total)
        # Backfill date_original and GPS from ExifTool when missing in files table
        _backfill_from_exiftool(catalog, str(path), meta)
        extracted += 1

    if extracted:
        catalog.commit()
        logger.info("ExifTool metadata extracted for %d files", extracted)
    return extracted


# Keys to try for date_original (priority order)
_DATE_KEYS = [
    "EXIF:DateTimeOriginal",
    "DateTimeOriginal",
    "EXIF:CreateDate",
    "CreateDate",
    "XMP:DateTimeOriginal",
    "XMP:CreateDate",
    "QuickTime:CreateDate",
    "QuickTime:MediaCreateDate",
    "Composite:SubSecDateTimeOriginal",
    "H264:DateTimeOriginal",
]

# Keys to try for GPS latitude/longitude
_GPS_LAT_KEYS = [
    "Composite:GPSLatitude",
    "EXIF:GPSLatitude",
    "GPSLatitude",
    "XMP:GPSLatitude",
]
_GPS_LON_KEYS = [
    "Composite:GPSLongitude",
    "EXIF:GPSLongitude",
    "GPSLongitude",
    "XMP:GPSLongitude",
]


def _backfill_from_exiftool(catalog: Catalog, path_str: str, meta: dict) -> None:
    """Fill in date_original and GPS in the files table from ExifTool metadata."""
    row = catalog.get_file_by_path(path_str)
    if row is None:
        return

    updates: dict[str, object] = {}

    # Backfill date_original
    if not row.date_original:
        for key in _DATE_KEYS:
            val = meta.get(key)
            if val and isinstance(val, str) and len(val) >= 10:
                # Normalize to YYYY:MM:DD HH:MM:SS format
                updates["date_original"] = val.strip()
                break

    # Backfill GPS
    if not row.gps_latitude:
        lat = _extract_gps_float(meta, _GPS_LAT_KEYS)
        lon = _extract_gps_float(meta, _GPS_LON_KEYS)
        if lat is not None and lon is not None:
            updates["gps_latitude"] = lat
            updates["gps_longitude"] = lon

    if updates:
        set_clause = ", ".join(f"{k}=?" for k in updates)
        catalog.conn.execute(
            f"UPDATE files SET {set_clause} WHERE path=?",  # noqa: S608
            [*updates.values(), path_str],
        )


def _extract_gps_float(meta: dict, keys: list[str]) -> float | None:
    """Extract GPS coordinate as float from ExifTool metadata."""
    for key in keys:
        val = meta.get(key)
        if val is None:
            continue
        if isinstance(val, int | float):
            return float(val)
        if isinstance(val, str):
            # Handle "49.1234 N" or "49 deg 7' 25.08\" N" formats
            import re

            # Try simple float
            try:
                return float(val)
            except ValueError:
                pass
            # Try DMS format
            m = re.match(r"([+-]?\d+(?:\.\d+)?)", val.replace(",", "."))
            if m:
                try:
                    return float(m.group(1))
                except ValueError:
                    pass
    return None


def backfill_metadata_from_stored(catalog: Catalog) -> dict:
    """Backfill date_original and GPS from already-stored ExifTool metadata.

    Reads file_metadata.raw_json for files missing date or GPS,
    without re-running ExifTool.
    Returns {"dates_filled": int, "gps_filled": int}.
    """
    import json as _json

    # Files missing date_original but having ExifTool metadata
    # Include date_original and gps_latitude in the JOIN to avoid N+1 get_file_by_path() calls
    cur = catalog.conn.execute(
        "SELECT f.path, fm.raw_json, f.date_original, f.gps_latitude FROM files f "
        "JOIN file_metadata fm ON fm.file_id = f.id "
        "WHERE f.date_original IS NULL OR f.gps_latitude IS NULL"
    )

    dates_filled = 0
    gps_filled = 0

    for path_str, raw, existing_date, existing_lat in cur.fetchall():
        try:
            meta = _json.loads(raw)
        except (ValueError, TypeError):
            continue

        updates: dict[str, object] = {}

        if not existing_date:
            for key in _DATE_KEYS:
                val = meta.get(key)
                if val and isinstance(val, str) and len(val) >= 10:
                    updates["date_original"] = val.strip()
                    dates_filled += 1
                    break

        if not existing_lat:
            lat = _extract_gps_float(meta, _GPS_LAT_KEYS)
            lon = _extract_gps_float(meta, _GPS_LON_KEYS)
            if lat is not None and lon is not None:
                updates["gps_latitude"] = lat
                updates["gps_longitude"] = lon
                gps_filled += 1

        if updates:
            set_clause = ", ".join(f"{k}=?" for k in updates)
            catalog.conn.execute(
                f"UPDATE files SET {set_clause} WHERE path=?",  # noqa: S608
                [*updates.values(), path_str],
            )

    if dates_filled or gps_filled:
        catalog.commit()
        logger.info("Backfilled from stored metadata: %d dates, %d GPS", dates_filled, gps_filled)

    return {"dates_filled": dates_filled, "gps_filled": gps_filled}


def _backfill_dates_from_filesystem(catalog: Catalog) -> int:
    """Fill date_original from birthtime/mtime for files that still lack it."""
    from datetime import datetime, timezone

    count = catalog.conn.execute(
        "SELECT COUNT(*) FROM files WHERE date_original IS NULL AND (birthtime IS NOT NULL OR mtime IS NOT NULL)"
    ).fetchone()[0]

    if count == 0:
        return 0

    logger.info("Backfilling date_original from filesystem dates for %d files", count)

    rows = catalog.conn.execute(
        "SELECT id, birthtime, mtime FROM files WHERE date_original IS NULL AND (birthtime IS NOT NULL OR mtime IS NOT NULL)"
    ).fetchall()

    for row_id, birthtime, mtime in rows:
        ts = birthtime if birthtime else mtime
        if ts:
            try:
                ts = float(ts)
            except (ValueError, TypeError):
                continue
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            date_str = dt.strftime("%Y:%m:%d %H:%M:%S")
            catalog.conn.execute(
                "UPDATE files SET date_original=? WHERE id=?",
                (date_str, row_id),
            )

    logger.info("Backfilled %d files with filesystem dates", count)
    return count


def _update_duplicate_groups(catalog: Catalog) -> int:
    """Detect exact duplicate groups from SHA-256 hashes in catalog."""
    cur = catalog.conn.execute(
        "SELECT sha256, GROUP_CONCAT(id, ',') as ids, COUNT(*) as cnt FROM files WHERE sha256 IS NOT NULL GROUP BY sha256 HAVING cnt >= 2"
    )
    groups = 0
    for row in cur.fetchall():
        sha256 = row[0]
        file_ids = [int(x) for x in row[1].split(",")]
        # First file is considered primary (lowest id = first seen)
        catalog.upsert_duplicate_group(sha256, file_ids, primary_id=file_ids[0])
        groups += 1
    return groups
