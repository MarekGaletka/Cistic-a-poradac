"""Incremental filesystem scanner backed by the SQLite catalog."""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .asset_sets import build_asset_membership
from .catalog import Catalog, CatalogFileRow, ScanStats
from .exif_reader import ExifMeta, can_read_exif, read_exif
from .media_probe import MediaMeta, is_media_ext, probe_file
from .perceptual_hash import dhash, is_image_ext
from .utils import iter_files, meaningful_xattr_count, safe_stat_birthtime, sha256_file
from .video_hash import is_video_ext, video_dhash

logger = logging.getLogger(__name__)


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
) -> ScanStats:
    """Scan filesystem roots and update catalog incrementally.

    Only computes SHA-256 for files whose mtime or size changed since last scan,
    or for newly discovered files. Detects deletions (files in catalog but gone
    from disk).

    Args:
        catalog: Open catalog instance.
        roots: Directories to scan recursively.
        force_rehash: If True, recompute SHA-256 for all files regardless of mtime/size.
        min_size_bytes: Minimum file size for hashing (smaller files get sha256=None).
        extract_media: If True, extract media metadata (ffprobe + EXIF) for new/changed files.
        compute_phash: If True, compute perceptual hash for image files.
        extract_exiftool: If True, run ExifTool batch extraction after scan for files without metadata.
        exiftool_bin: ExifTool binary path (used only when extract_exiftool=True).
    """
    stats = ScanStats(root=";".join(str(r) for r in roots))
    effective_workers = max(1, workers)

    # Collect all current disk paths
    all_paths = list(iter_files(roots))
    logger.info("Discovered %d files across %d roots", len(all_paths), len(roots))
    if progress_callback:
        progress_callback({"phase": "discovery", "total": len(all_paths), "processed": 0})

    # Build asset membership
    path_to_key, path_is_component, _ = build_asset_membership(all_paths)

    # Start scan record
    scan_id = catalog.start_scan(stats.root)

    # ── Pre-load existing mtime/size for all roots (batch, avoids N+1) ──
    _existing_mtime_size: dict[str, tuple[float, int]] = {}
    for root in roots:
        _existing_mtime_size.update(catalog.get_all_mtime_size_for_root(str(root)))

    # ── Phase 1: Stat & classify (sequential, fast) ──────────────────
    seen_paths: set[str] = set()
    file_infos: list[dict] = []
    paths_to_hash: list[tuple[int, Path, int]] = []  # (index, path, size)
    paths_for_media: list[tuple[int, Path, str]] = []  # (index, path, ext)
    paths_for_phash: list[tuple[int, Path]] = []  # (index, path)

    for idx, path in enumerate(all_paths):
        try:
            st = path.stat()
        except OSError:
            logger.debug("Cannot stat %s, skipping", path)
            continue

        path_str = str(path)
        seen_paths.add(path_str)
        stats.files_scanned += 1

        size = int(st.st_size)
        mtime = float(st.st_mtime)
        ctime = float(st.st_ctime)
        ext = path.suffix.lower().lstrip(".")
        birthtime = safe_stat_birthtime(path)
        xattr = meaningful_xattr_count(path)
        inode = int(st.st_ino)
        device = int(st.st_dev)
        nlink = int(st.st_nlink)
        asset_key = path_to_key.get(path)
        is_component = path_is_component.get(path, False)

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

        info = {
            "idx": idx,
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
            "asset_key": asset_key,
            "is_component": is_component,
            "needs_hash": needs_hash,
            "is_new_or_changed": is_new_or_changed,
            "sha256": None,
            "media_meta": None,
            "exif_meta": None,
            "phash": None,
        }
        file_infos.append(info)

        if needs_hash and size >= min_size_bytes:
            paths_to_hash.append((len(file_infos) - 1, path, size))
        elif not needs_hash and existing is not None:
            existing_row = catalog.get_file_by_path(path_str)
            if existing_row:
                info["sha256"] = existing_row.sha256
                if existing_row.duration_seconds or existing_row.width:
                    info["media_meta"] = MediaMeta(
                        duration_seconds=existing_row.duration_seconds,
                        width=existing_row.width,
                        height=existing_row.height,
                        video_codec=existing_row.video_codec,
                        audio_codec=existing_row.audio_codec,
                        bitrate=existing_row.bitrate,
                    )
                info["phash"] = existing_row.phash
                if existing_row.date_original or existing_row.camera_make:
                    info["exif_meta"] = ExifMeta(
                        date_original=existing_row.date_original,
                        camera_make=existing_row.camera_make,
                        camera_model=existing_row.camera_model,
                        image_width=existing_row.width,
                        image_height=existing_row.height,
                        gps_latitude=existing_row.gps_latitude,
                        gps_longitude=existing_row.gps_longitude,
                    )

        if is_new_or_changed and extract_media:
            paths_for_media.append((len(file_infos) - 1, path, ext))
        if is_new_or_changed and compute_phash and (is_image_ext(ext) or is_video_ext(ext)):
            paths_for_phash.append((len(file_infos) - 1, path, ext))

    # ── Phase 2: Parallel SHA-256 hashing ─────────────────────────────
    if progress_callback:
        progress_callback({"phase": "hashing", "total": len(all_paths), "processed": stats.files_scanned, "to_hash": len(paths_to_hash)})
    if paths_to_hash:
        logger.info("Hashing %d files (workers=%d)", len(paths_to_hash), effective_workers)
        if effective_workers > 1:
            with ThreadPoolExecutor(max_workers=effective_workers) as pool:
                futures = {pool.submit(sha256_file, p): (fi_idx, sz) for fi_idx, p, sz in paths_to_hash}
                for future in as_completed(futures):
                    fi_idx, sz = futures[future]
                    try:
                        file_infos[fi_idx]["sha256"] = future.result()
                        stats.bytes_hashed += sz
                    except OSError:
                        logger.warning("Cannot hash %s", file_infos[fi_idx]["path"])
        else:
            for fi_idx, p, sz in paths_to_hash:
                try:
                    file_infos[fi_idx]["sha256"] = sha256_file(p)
                    stats.bytes_hashed += sz
                except OSError:
                    logger.warning("Cannot hash %s", p)

    # ── Phase 3: Parallel media probe + EXIF ──────────────────────────
    if paths_for_media:

        def _extract_media(fi_idx: int, p: Path, ext: str) -> tuple[int, MediaMeta | None, ExifMeta | None]:
            mm = probe_file(p) if is_media_ext(ext) else None
            em = read_exif(p) if can_read_exif(ext) else None
            return fi_idx, mm, em

        if effective_workers > 1:
            with ThreadPoolExecutor(max_workers=effective_workers) as pool:
                futures = [pool.submit(_extract_media, fi_idx, p, ext) for fi_idx, p, ext in paths_for_media]
                for future in as_completed(futures):
                    try:
                        fi_idx, mm, em = future.result()
                    except Exception:
                        logger.warning("Media extraction failed for a file, skipping", exc_info=True)
                        continue
                    file_infos[fi_idx]["media_meta"] = mm
                    file_infos[fi_idx]["exif_meta"] = em
        else:
            for fi_idx, p, ext in paths_for_media:
                _, mm, em = _extract_media(fi_idx, p, ext)
                file_infos[fi_idx]["media_meta"] = mm
                file_infos[fi_idx]["exif_meta"] = em

    # ── Phase 4: Parallel perceptual hash (image + video) ──────────────
    def _compute_phash(p: Path, ext: str) -> str | None:
        if is_image_ext(ext):
            return dhash(p)
        if is_video_ext(ext):
            return video_dhash(p)
        return None

    if paths_for_phash:
        if effective_workers > 1:
            with ThreadPoolExecutor(max_workers=effective_workers) as pool:
                futures = {pool.submit(_compute_phash, p, ext): fi_idx for fi_idx, p, ext in paths_for_phash}
                for future in as_completed(futures):
                    fi_idx = futures[future]
                    try:
                        file_infos[fi_idx]["phash"] = future.result()
                    except Exception:
                        logger.warning("Perceptual hash failed for %s, skipping", file_infos[fi_idx]["path"], exc_info=True)
        else:
            for fi_idx, p, ext in paths_for_phash:
                file_infos[fi_idx]["phash"] = _compute_phash(p, ext)

    # ── Phase 5: Sequential catalog upsert ────────────────────────────
    if progress_callback:
        progress_callback({"phase": "saving", "total": len(file_infos), "processed": 0})
    for upsert_idx, info in enumerate(file_infos):
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

        if progress_callback and (upsert_idx + 1) % 100 == 0:
            progress_callback({"phase": "saving", "total": len(file_infos), "processed": upsert_idx + 1})

    if stats.files_scanned % 1000 == 0 and stats.files_scanned > 0:
        catalog.commit()

    # Detect removals
    catalog_paths = catalog.all_paths()
    # Only consider paths under the scanned roots
    root_prefixes = [str(r) for r in roots]
    catalog_paths_in_scope = {p for p in catalog_paths if any(p.startswith(rp) for rp in root_prefixes)}
    removed_paths = catalog_paths_in_scope - seen_paths

    if removed_paths:
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
        if isinstance(val, (int, float)):
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
    cur = catalog.conn.execute(
        "SELECT f.path, fm.raw_json FROM files f "
        "JOIN file_metadata fm ON fm.file_id = f.id "
        "WHERE f.date_original IS NULL OR f.gps_latitude IS NULL"
    )

    dates_filled = 0
    gps_filled = 0

    for path_str, raw in cur.fetchall():
        try:
            meta = _json.loads(raw)
        except (ValueError, TypeError):
            continue

        row = catalog.get_file_by_path(path_str)
        if row is None:
            continue

        updates: dict[str, object] = {}

        if not row.date_original:
            for key in _DATE_KEYS:
                val = meta.get(key)
                if val and isinstance(val, str) and len(val) >= 10:
                    updates["date_original"] = val.strip()
                    dates_filled += 1
                    break

        if not row.gps_latitude:
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
