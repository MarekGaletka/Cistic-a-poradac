"""Bit rot detection — periodic SHA256 re-verification of cataloged files."""

from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1 << 16  # 64 KB


@dataclass
class BitrotResult:
    """Result of a bit rot scan."""

    total_checked: int = 0
    healthy: int = 0
    corrupted: int = 0
    missing: int = 0
    errors: int = 0
    bytes_verified: int = 0
    elapsed_seconds: float = 0
    corrupted_files: list[dict] = field(default_factory=list)
    missing_files: list[dict] = field(default_factory=list)


def _sha256_file(path: str) -> str | None:
    """Compute SHA256 of a file."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except (OSError, IOError):
        return None


def scan_bitrot(
    catalog,
    *,
    limit: int = 0,
    oldest_first: bool = True,
    progress_fn: Callable | None = None,
) -> BitrotResult:
    """Scan catalog files for bit rot by re-computing SHA256.

    Args:
        catalog: Open Catalog instance
        limit: Max files to check (0 = all)
        oldest_first: If True, check files that haven't been verified longest
        progress_fn: Optional progress callback

    Returns:
        BitrotResult with counts and lists of corrupted/missing files
    """
    # Ensure verification columns exist
    for col_name, col_type in [("last_verified", "TEXT"), ("verify_count", "INTEGER DEFAULT 0")]:
        try:
            catalog.conn.execute(f"ALTER TABLE files ADD COLUMN {col_name} {col_type}")
            catalog.conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists

    # Get files to verify — oldest verified first, then never-verified
    order = "COALESCE(last_verified, '1970-01-01') ASC" if oldest_first else "RANDOM()"

    sql = f"""
        SELECT id, path, sha256, size FROM files
        WHERE sha256 IS NOT NULL AND size > 0
        ORDER BY {order}
    """
    params: tuple = ()
    if limit:
        sql += " LIMIT ?"
        params = (int(limit),)

    rows = catalog.conn.execute(sql, params).fetchall()

    result = BitrotResult()
    start = time.monotonic()
    total = len(rows)

    for i, (file_id, path, stored_hash, size) in enumerate(rows):
        if progress_fn and i % 50 == 0:
            progress_fn(
                {
                    "phase": "verifying",
                    "current": i,
                    "total": total,
                    "progress_pct": int((i / max(total, 1)) * 100),
                    "healthy": result.healthy,
                    "corrupted": result.corrupted,
                }
            )

        if not os.path.isfile(path):
            result.missing += 1
            result.missing_files.append({"id": file_id, "path": path, "size": size})
            continue

        computed = _sha256_file(path)
        if computed is None:
            result.errors += 1
            continue

        result.total_checked += 1
        result.bytes_verified += size or 0

        if computed == stored_hash:
            result.healthy += 1
            # Update verification timestamp
            catalog.conn.execute(
                "UPDATE files SET last_verified = ?, verify_count = COALESCE(verify_count, 0) + 1 WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), file_id),
            )
        else:
            result.corrupted += 1
            result.corrupted_files.append(
                {
                    "id": file_id,
                    "path": path,
                    "size": size,
                    "stored_hash": stored_hash,
                    "actual_hash": computed,
                }
            )
            logger.warning("BIT ROT DETECTED: %s (stored=%s, actual=%s)", path, stored_hash[:12], computed[:12])

        # Commit every 100 files
        if i % 100 == 0:
            catalog.conn.commit()

    catalog.conn.commit()
    result.elapsed_seconds = round(time.monotonic() - start, 2)

    if progress_fn:
        progress_fn(
            {
                "phase": "complete",
                "progress_pct": 100,
                "total_checked": result.total_checked,
                "corrupted": result.corrupted,
                "missing": result.missing,
            }
        )

    return result


def get_verification_stats(catalog) -> dict:
    """Get verification statistics from the catalog."""
    total = catalog.conn.execute("SELECT COUNT(*) FROM files WHERE sha256 IS NOT NULL").fetchone()[0]
    verified = catalog.conn.execute("SELECT COUNT(*) FROM files WHERE last_verified IS NOT NULL").fetchone()[0]
    never_verified = total - verified

    # Oldest verification
    oldest = catalog.conn.execute("SELECT MIN(last_verified) FROM files WHERE last_verified IS NOT NULL").fetchone()[0]

    # Average verify count
    avg_count = catalog.conn.execute("SELECT AVG(COALESCE(verify_count, 0)) FROM files WHERE sha256 IS NOT NULL").fetchone()[0] or 0

    return {
        "total_files": total,
        "verified": verified,
        "never_verified": never_verified,
        "verification_pct": round((verified / max(total, 1)) * 100, 1),
        "oldest_verification": oldest,
        "avg_verify_count": round(avg_count, 1),
    }
