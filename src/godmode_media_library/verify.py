"""Catalog integrity verification against filesystem."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from .catalog import Catalog
from .utils import sha256_file

logger = logging.getLogger(__name__)


@dataclass
class VerifyResult:
    """Result of catalog verification."""
    total_checked: int = 0
    missing_files: list[str] = field(default_factory=list)
    size_mismatches: list[tuple[str, int, int]] = field(default_factory=list)  # path, catalog_size, actual_size
    hash_mismatches: list[tuple[str, str, str]] = field(default_factory=list)  # path, catalog_hash, actual_hash
    orphaned_metadata: list[str] = field(default_factory=list)
    ok: int = 0

    @property
    def has_issues(self) -> bool:
        return bool(self.missing_files or self.size_mismatches or self.hash_mismatches or self.orphaned_metadata)


def verify_catalog(
    catalog: Catalog,
    *,
    check_hashes: bool = False,
    limit: int = 0,
    progress_callback=None,
) -> VerifyResult:
    """Verify catalog entries match the filesystem.

    Args:
        catalog: Open catalog instance.
        check_hashes: If True, recompute SHA-256 and compare (slow).
        limit: Max files to check (0 = all).
        progress_callback: Optional callable(dict) for progress updates.
    """
    result = VerifyResult()
    rows = catalog.query_files(limit=limit if limit > 0 else 1_000_000)
    total = len(rows)

    for i, row in enumerate(rows):
        result.total_checked += 1
        path = Path(row.path)

        if progress_callback and i % 100 == 0:
            progress_callback({"phase": "verify", "processed": i, "total": total})

        if not path.exists():
            result.missing_files.append(row.path)
            continue

        try:
            stat = path.stat()
        except OSError:
            result.missing_files.append(row.path)
            continue

        actual_size = stat.st_size
        if actual_size != row.size:
            result.size_mismatches.append((row.path, row.size, actual_size))
            continue

        if check_hashes and row.sha256:
            try:
                actual_hash = sha256_file(path)
            except OSError:
                result.missing_files.append(row.path)
                continue
            if actual_hash != row.sha256:
                result.hash_mismatches.append((row.path, row.sha256, actual_hash))
                continue

        result.ok += 1

    if progress_callback:
        progress_callback({"phase": "verify", "processed": total, "total": total})

    return result
