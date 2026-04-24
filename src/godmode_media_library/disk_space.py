"""Disk space check utility for quarantine operations."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


def check_disk_space(dest_dir: Path, file_size: int, margin: float = 1.1) -> bool:
    """Check whether *dest_dir* has enough free space for a file of *file_size* bytes.

    Args:
        dest_dir: The target directory (or an ancestor that already exists).
        file_size: Size of the file to be moved, in bytes.
        margin: Multiplier applied to *file_size* as a safety margin (default 1.1 = 10 %).

    Returns:
        ``True`` if there is enough space, ``False`` otherwise.
        Also returns ``True`` if the free-space check cannot be performed (e.g. the
        filesystem does not exist yet), so that callers fall through to the normal
        OS error path instead of silently skipping.

    Note:
        This function uses ``shutil.disk_usage`` which reports apparent disk usage
        and does not account for sparse files or filesystems with transparent
        compression (e.g., ZFS, Btrfs). Sparse files may report a larger ``file_size``
        than the actual blocks allocated on disk, causing this check to overestimate
        the space required. For most media-library workloads this is not an issue.
    """
    # Walk up to find an existing ancestor directory for the stat call
    check_path = dest_dir
    while not check_path.exists():
        parent = check_path.parent
        if parent == check_path:
            # Reached filesystem root without finding an existing dir
            return True
        check_path = parent

    try:
        usage = shutil.disk_usage(check_path)
    except OSError:
        # If we can't determine free space, don't block the operation
        return True

    needed = int(file_size * margin)
    if usage.free < needed:
        logger.warning(
            "Insufficient disk space for quarantine move: need %d bytes (file %d + margin), only %d bytes free on %s",
            needed,
            file_size,
            usage.free,
            check_path,
        )
        return False
    return True
