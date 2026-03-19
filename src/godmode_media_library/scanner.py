"""Incremental filesystem scanner backed by the SQLite catalog."""

from __future__ import annotations

import logging
from pathlib import Path

from .asset_sets import build_asset_membership
from .catalog import Catalog, CatalogFileRow, ScanStats
from .exif_reader import ExifMeta, can_read_exif, read_exif
from .media_probe import MediaMeta, is_media_ext, probe_file
from .perceptual_hash import dhash, is_image_ext
from .utils import iter_files, meaningful_xattr_count, safe_stat_birthtime, sha256_file

logger = logging.getLogger(__name__)


def incremental_scan(
    catalog: Catalog,
    roots: list[Path],
    *,
    force_rehash: bool = False,
    min_size_bytes: int = 0,
    extract_media: bool = True,
    compute_phash: bool = True,
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
    """
    stats = ScanStats(root=";".join(str(r) for r in roots))

    # Collect all current disk paths
    all_paths = list(iter_files(roots))
    logger.info("Discovered %d files across %d roots", len(all_paths), len(roots))

    # Build asset membership
    path_to_key, path_is_component, _ = build_asset_membership(all_paths)

    # Start scan record
    scan_id = catalog.start_scan(stats.root)

    # Track which catalog paths we've seen (for deletion detection)
    seen_paths: set[str] = set()

    for path in all_paths:
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

        # Check if file is already in catalog and unchanged
        existing = catalog.get_file_mtime_size(path_str)
        needs_hash = force_rehash
        is_new_or_changed = False

        if existing is None:
            # New file
            stats.files_new += 1
            needs_hash = True
            is_new_or_changed = True
        elif existing[0] != mtime or existing[1] != size:
            # Changed file
            stats.files_changed += 1
            needs_hash = True
            is_new_or_changed = True

        # Compute hash if needed
        sha256 = None
        if needs_hash and size >= min_size_bytes:
            try:
                sha256 = sha256_file(path)
                stats.bytes_hashed += size
            except OSError:
                logger.warning("Cannot hash %s", path)

        # If not rehashing, preserve existing hash and media metadata
        existing_row = None
        if not needs_hash and existing is not None:
            existing_row = catalog.get_file_by_path(path_str)
            if existing_row:
                sha256 = existing_row.sha256

        # Extract media metadata for new/changed files
        media_meta: MediaMeta | None = None
        exif_meta: ExifMeta | None = None
        phash_val: str | None = None

        if is_new_or_changed and extract_media:
            if is_media_ext(ext):
                media_meta = probe_file(path)
            if can_read_exif(ext):
                exif_meta = read_exif(path)
            if compute_phash and is_image_ext(ext):
                phash_val = dhash(path)
        elif existing_row:
            # Preserve existing media metadata for unchanged files
            media_meta = MediaMeta(
                duration_seconds=existing_row.duration_seconds,
                width=existing_row.width,
                height=existing_row.height,
                video_codec=existing_row.video_codec,
                audio_codec=existing_row.audio_codec,
                bitrate=existing_row.bitrate,
            ) if existing_row.duration_seconds or existing_row.width else None
            phash_val = existing_row.phash
            if existing_row.date_original or existing_row.camera_make:
                exif_meta = ExifMeta(
                    date_original=existing_row.date_original,
                    camera_make=existing_row.camera_make,
                    camera_model=existing_row.camera_model,
                    image_width=existing_row.width,
                    image_height=existing_row.height,
                    gps_latitude=existing_row.gps_latitude,
                    gps_longitude=existing_row.gps_longitude,
                )

        # Build row with media metadata
        row = CatalogFileRow(
            id=None,
            path=path_str,
            size=size,
            mtime=mtime,
            ctime=ctime,
            birthtime=birthtime,
            ext=ext,
            sha256=sha256,
            inode=inode,
            device=device,
            nlink=nlink,
            asset_key=f"{path.parent}\t{path.stem}" if asset_key else None,
            asset_component=is_component,
            xattr_count=xattr,
            first_seen="",  # upsert_file handles this
            last_scanned="",
        )

        # Apply media metadata
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

        if stats.files_scanned % 1000 == 0:
            catalog.commit()
            logger.info("Progress: %d files scanned, %d new, %d changed", stats.files_scanned, stats.files_new, stats.files_changed)

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

    catalog.commit()
    catalog.finish_scan(scan_id, stats)

    logger.info(
        "Scan complete: %d scanned, %d new, %d changed, %d removed, %d bytes hashed",
        stats.files_scanned, stats.files_new, stats.files_changed, stats.files_removed, stats.bytes_hashed,
    )
    return stats


def _update_duplicate_groups(catalog: Catalog) -> int:
    """Detect exact duplicate groups from SHA-256 hashes in catalog."""
    cur = catalog.conn.execute(
        "SELECT sha256, GROUP_CONCAT(id, ',') as ids, COUNT(*) as cnt "
        "FROM files WHERE sha256 IS NOT NULL "
        "GROUP BY sha256 HAVING cnt >= 2"
    )
    groups = 0
    for row in cur.fetchall():
        sha256 = row[0]
        file_ids = [int(x) for x in row[1].split(",")]
        # First file is considered primary (lowest id = first seen)
        catalog.upsert_duplicate_group(sha256, file_ids, primary_id=file_ids[0])
        groups += 1
    return groups
