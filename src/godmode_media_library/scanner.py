"""Incremental filesystem scanner backed by the SQLite catalog."""

from __future__ import annotations

import logging
from pathlib import Path

from .asset_sets import build_asset_membership
from .catalog import Catalog, CatalogFileRow, ScanStats
from .utils import iter_files, meaningful_xattr_count, safe_stat_birthtime, sha256_file

logger = logging.getLogger(__name__)


def incremental_scan(
    catalog: Catalog,
    roots: list[Path],
    *,
    force_rehash: bool = False,
    min_size_bytes: int = 0,
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

        if existing is None:
            # New file
            stats.files_new += 1
            needs_hash = True
        elif existing[0] != mtime or existing[1] != size:
            # Changed file
            stats.files_changed += 1
            needs_hash = True

        # Compute hash if needed
        sha256 = None
        if needs_hash and size >= min_size_bytes:
            try:
                sha256 = sha256_file(path)
                stats.bytes_hashed += size
            except OSError:
                logger.warning("Cannot hash %s", path)

        # If not rehashing, preserve existing hash
        if not needs_hash and existing is not None:
            existing_row = catalog.get_file_by_path(path_str)
            if existing_row:
                sha256 = existing_row.sha256

        catalog.upsert_file(CatalogFileRow(
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
        ))

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
