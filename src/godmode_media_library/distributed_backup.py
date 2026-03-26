"""Distributed cloud backup — spread files across multiple cloud remotes."""

from __future__ import annotations

import json
import logging
import os
import subprocess
from dataclasses import dataclass, field
from typing import Callable

from .catalog import Catalog
from .cloud import list_remotes, rclone_about, _rclone_bin

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


@dataclass
class BackupTarget:
    """A cloud remote configured as a backup destination."""

    remote_name: str
    remote_path: str = "GML-Backup"
    total_bytes: int = 0
    used_bytes: int = 0
    free_bytes: int = 0
    enabled: bool = True
    priority: int = 0  # lower = fill first
    encrypted: bool = False
    crypt_remote: str = ""

    @property
    def available_bytes(self) -> int:
        return max(0, self.free_bytes - 500_000_000)  # Keep 500MB reserve


@dataclass
class BackupManifestEntry:
    """Record of a single file backed up to a specific remote."""

    file_id: int
    path: str
    sha256: str
    size: int
    remote_name: str
    remote_path: str
    backed_up_at: str  # ISO timestamp
    verified: bool = False
    verified_at: str | None = None


@dataclass
class BackupPlan:
    """Plan for distributing files across remotes."""

    entries: list[dict] = field(default_factory=list)
    total_files: int = 0
    total_bytes: int = 0
    targets_used: list[str] = field(default_factory=list)
    overflow_files: int = 0  # Files that don't fit anywhere
    overflow_bytes: int = 0


@dataclass
class BackupStats:
    """Overall backup health stats."""

    total_files_in_catalog: int = 0
    backed_up_files: int = 0
    not_backed_up: int = 0
    backup_coverage_pct: float = 0.0
    total_backup_size: int = 0
    encrypted_files: int = 0
    remotes_used: int = 0
    remotes_healthy: int = 0
    last_backup_at: str | None = None
    files_by_remote: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Manifest storage (in catalog DB)
# ---------------------------------------------------------------------------


def ensure_backup_tables(catalog: Catalog) -> None:
    """Create backup tables if they don't exist."""
    catalog.conn.execute("""
        CREATE TABLE IF NOT EXISTS backup_targets (
            remote_name TEXT PRIMARY KEY,
            remote_path TEXT DEFAULT 'GML-Backup',
            enabled INTEGER DEFAULT 1,
            priority INTEGER DEFAULT 0,
            total_bytes INTEGER DEFAULT 0,
            used_bytes INTEGER DEFAULT 0,
            free_bytes INTEGER DEFAULT 0,
            encrypted INTEGER DEFAULT 0,
            crypt_remote TEXT DEFAULT '',
            last_probed_at TEXT
        )
    """)
    catalog.conn.execute("""
        CREATE TABLE IF NOT EXISTS backup_manifest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            sha256 TEXT,
            size INTEGER NOT NULL,
            remote_name TEXT NOT NULL,
            remote_path TEXT NOT NULL,
            backed_up_at TEXT NOT NULL,
            verified INTEGER DEFAULT 0,
            verified_at TEXT,
            UNIQUE(file_id, remote_name)
        )
    """)
    catalog.conn.execute("CREATE INDEX IF NOT EXISTS idx_bm_file ON backup_manifest(file_id)")
    catalog.conn.execute("CREATE INDEX IF NOT EXISTS idx_bm_remote ON backup_manifest(remote_name)")
    catalog.conn.execute("CREATE INDEX IF NOT EXISTS idx_bm_sha ON backup_manifest(sha256)")
    catalog.conn.commit()


# ---------------------------------------------------------------------------
# Target management
# ---------------------------------------------------------------------------


def probe_targets(catalog: Catalog) -> list[BackupTarget]:
    """Probe all rclone remotes for storage capacity and update backup_targets table."""
    ensure_backup_tables(catalog)
    remotes = list_remotes()
    targets = []

    for r in remotes:
        try:
            about = rclone_about(r.name)
            total = about.get("total", 0) or 0
            used = about.get("used", 0) or 0
            free = about.get("free", 0) or total - used

            target = BackupTarget(
                remote_name=r.name,
                total_bytes=total,
                used_bytes=used,
                free_bytes=free,
            )

            # Check for crypt overlay using rclone listremotes with type filter
            try:
                all_config = subprocess.run(
                    [_rclone_bin(), "config", "dump"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if all_config.returncode == 0:
                    config_data = json.loads(all_config.stdout)
                    for crypt_name, crypt_cfg in config_data.items():
                        if crypt_cfg.get("type") == "crypt" and crypt_cfg.get("remote", "").startswith(f"{r.name}:"):
                            target.encrypted = True
                            target.crypt_remote = crypt_name
                            break
                    # Explicitly clear parsed config to avoid credential leaks
                    del config_data
            except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError) as exc:
                logger.debug("Cannot check crypt overlay for %s: %s", r.name, type(exc).__name__)

            # Update or insert in DB
            catalog.conn.execute(
                """
                INSERT INTO backup_targets
                    (remote_name, total_bytes, used_bytes, free_bytes,
                     encrypted, crypt_remote, last_probed_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(remote_name) DO UPDATE SET
                    total_bytes=excluded.total_bytes,
                    used_bytes=excluded.used_bytes,
                    free_bytes=excluded.free_bytes,
                    encrypted=excluded.encrypted,
                    crypt_remote=excluded.crypt_remote,
                    last_probed_at=excluded.last_probed_at
            """,
                (r.name, total, used, free, int(target.encrypted), target.crypt_remote),
            )

            targets.append(target)
            logger.info("Probed %s: %d total, %d free", r.name, total, free)
        except Exception as e:
            logger.warning("Failed to probe %s: %s", r.name, e)

    catalog.conn.commit()
    return targets


def get_targets(catalog: Catalog) -> list[BackupTarget]:
    """Get all configured backup targets from DB."""
    ensure_backup_tables(catalog)
    rows = catalog.conn.execute("""
        SELECT remote_name, remote_path, enabled, priority,
               total_bytes, used_bytes, free_bytes, encrypted, crypt_remote
        FROM backup_targets ORDER BY priority, remote_name
    """).fetchall()

    return [
        BackupTarget(
            remote_name=r[0],
            remote_path=r[1] or "GML-Backup",
            enabled=bool(r[2]),
            priority=r[3],
            total_bytes=r[4] or 0,
            used_bytes=r[5] or 0,
            free_bytes=r[6] or 0,
            encrypted=bool(r[7]),
            crypt_remote=r[8] or "",
        )
        for r in rows
    ]


def set_target_enabled(catalog: Catalog, remote_name: str, enabled: bool) -> None:
    """Enable or disable a remote as backup target."""
    ensure_backup_tables(catalog)
    catalog.conn.execute(
        "UPDATE backup_targets SET enabled = ? WHERE remote_name = ?",
        (int(enabled), remote_name),
    )
    catalog.conn.commit()


def set_target_priority(catalog: Catalog, remote_name: str, priority: int) -> None:
    """Set fill priority for a target (lower = fill first)."""
    ensure_backup_tables(catalog)
    catalog.conn.execute(
        "UPDATE backup_targets SET priority = ? WHERE remote_name = ?",
        (priority, remote_name),
    )
    catalog.conn.commit()


# ---------------------------------------------------------------------------
# File prioritization
# ---------------------------------------------------------------------------


def _compute_file_priority(row: dict) -> int:
    """Compute backup priority for a file. Lower = more important = back up first.

    Priority tiers:
      0-99:   Critical -- unique photos with rich EXIF/GPS, no duplicates
      100-199: Important -- photos/videos with some metadata
      200-299: Normal -- files with duplicates elsewhere
      300-399: Low -- screenshots, memes, low-quality
      400+:    Lowest -- already backed up elsewhere
    """
    score = 200  # default: normal

    ext = (row.get("ext") or "").lower()

    # Boost photos and videos
    if ext in ("jpg", "jpeg", "heic", "heif", "raw", "cr2", "nef", "arw", "dng"):
        score -= 80
    elif ext in ("mp4", "mov", "avi", "mkv", "m4v"):
        score -= 60
    elif ext in ("png", "tiff", "tif", "webp"):
        score -= 40

    # Boost files with rich metadata
    richness = row.get("metadata_richness") or 0
    if richness > 0.7:
        score -= 50
    elif richness > 0.4:
        score -= 25

    # Boost files with GPS
    if row.get("gps_latitude") and row.get("gps_longitude"):
        score -= 20

    # Boost files with original date
    if row.get("date_original"):
        score -= 15

    # Penalize screenshots and memes
    quality_cat = row.get("quality_category") or ""
    if quality_cat in ("screenshot", "meme"):
        score += 150
    elif quality_cat == "blurry":
        score += 50

    return score


def get_files_for_backup(catalog: Catalog, limit: int = 0) -> list[dict]:
    """Get files that need backup, ordered by priority (most important first).

    Returns files NOT yet in backup_manifest.
    """
    ensure_backup_tables(catalog)

    sql = """
        SELECT f.id, f.path, f.sha256, f.size, f.ext, f.date_original,
               f.gps_latitude, f.gps_longitude, f.metadata_richness,
               f.quality_category
        FROM files f
        LEFT JOIN backup_manifest bm ON f.id = bm.file_id
        WHERE bm.id IS NULL
          AND f.sha256 IS NOT NULL
          AND f.size > 0
    """
    if limit:
        sql += f" LIMIT {limit}"

    rows = catalog.conn.execute(sql).fetchall()
    cols = [
        "id",
        "path",
        "sha256",
        "size",
        "ext",
        "date_original",
        "gps_latitude",
        "gps_longitude",
        "metadata_richness",
        "quality_category",
    ]

    files = []
    for r in rows:
        d = dict(zip(cols, r))
        d["priority"] = _compute_file_priority(d)
        files.append(d)

    # Sort by priority (lower = more important)
    files.sort(key=lambda f: (f["priority"], -(f.get("size") or 0)))
    return files


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def create_backup_plan(catalog: Catalog) -> BackupPlan:
    """Create a plan for distributing files across available targets.

    Uses a greedy bin-packing algorithm:
    - Files sorted by priority (most important first)
    - Each file assigned to the target with most free space that can fit it
    - 500MB reserve kept on each target
    """
    targets = [t for t in get_targets(catalog) if t.enabled and t.available_bytes > 0]
    if not targets:
        return BackupPlan()

    files = get_files_for_backup(catalog)
    if not files:
        return BackupPlan()

    # Track remaining capacity per target
    remaining = {t.remote_name: t.available_bytes for t in targets}

    plan = BackupPlan()
    targets_used: set[str] = set()

    for f in files:
        size = f.get("size") or 0
        if size <= 0:
            continue

        # Find best target: has enough space, prefer the one with most remaining
        best = None
        best_remaining = -1
        for t in targets:
            r = remaining.get(t.remote_name, 0)
            if r >= size and r > best_remaining:
                best = t
                best_remaining = r

        if best is None:
            plan.overflow_files += 1
            plan.overflow_bytes += size
            continue

        # Determine remote path based on file date
        date_part = (f.get("date_original") or "")[:7].replace(":", "-")
        if not date_part or date_part < "1900":
            date_part = "unsorted"
        remote_sub = f"{best.remote_path}/{date_part}"

        plan.entries.append(
            {
                "file_id": f["id"],
                "path": f["path"],
                "size": size,
                "sha256": f.get("sha256", ""),
                "priority": f["priority"],
                "target_remote": best.remote_name,
                "target_path": remote_sub,
            }
        )

        remaining[best.remote_name] -= size
        targets_used.add(best.remote_name)
        plan.total_files += 1
        plan.total_bytes += size

    plan.targets_used = sorted(targets_used)
    return plan


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


def execute_backup_plan(
    catalog: Catalog,
    plan: BackupPlan | None = None,
    *,
    dry_run: bool = False,
    progress_fn: Callable | None = None,
) -> dict:
    """Execute a backup plan -- upload files to their assigned remotes.

    Returns summary dict.
    """
    ensure_backup_tables(catalog)

    if plan is None:
        plan = create_backup_plan(catalog)

    if not plan.entries:
        return {
            "uploaded": 0,
            "errors": 0,
            "skipped": 0,
            "message": "No files to back up",
        }

    uploaded = 0
    errors = 0
    skipped = 0
    bytes_uploaded = 0
    encrypted_files = 0

    # Build mapping of remote_name -> crypt_remote from targets
    targets_list = get_targets(catalog)
    crypt_map: dict[str, str] = {t.remote_name: t.crypt_remote for t in targets_list if t.crypt_remote}

    # Group entries by (remote, remote_path) for batch uploads
    groups: dict[tuple[str, str], list[dict]] = {}
    for entry in plan.entries:
        key = (entry["target_remote"], entry["target_path"])
        groups.setdefault(key, []).append(entry)

    total_groups = len(groups)

    for gi, ((remote, remote_path), entries) in enumerate(groups.items()):
        if progress_fn:
            progress_fn(
                {
                    "phase": "uploading",
                    "remote": remote,
                    "group": gi + 1,
                    "total_groups": total_groups,
                    "files_in_group": len(entries),
                    "progress_pct": int((gi / max(total_groups, 1)) * 100),
                }
            )

        for entry in entries:
            file_path = entry["path"]

            if not os.path.isfile(file_path):
                logger.warning("File not found, skipping: %s", file_path)
                skipped += 1
                continue

            if dry_run:
                uploaded += 1
                bytes_uploaded += entry["size"]
                continue

            try:
                # Determine upload target (prefer encrypted)
                upload_remote = remote
                upload_path = remote_path
                if remote in crypt_map:
                    upload_remote = crypt_map[remote]
                    # For crypt remotes, use relative path (crypt remote already has base path)
                    date_part = remote_path.rsplit("/", 1)[-1] if "/" in remote_path else remote_path
                    upload_path = date_part

                # Upload individual file using rclone copy
                cmd = [
                    _rclone_bin(),
                    "copy",
                    file_path,
                    f"{upload_remote}:{upload_path}/",
                    "--no-traverse",
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

                if result.returncode == 0:
                    # Record in manifest
                    catalog.conn.execute(
                        """
                        INSERT INTO backup_manifest
                            (file_id, path, sha256, size, remote_name,
                             remote_path, backed_up_at)
                        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                        ON CONFLICT(file_id, remote_name) DO UPDATE SET
                            backed_up_at=datetime('now'), verified=0
                    """,
                        (
                            entry["file_id"],
                            file_path,
                            entry.get("sha256", ""),
                            entry["size"],
                            remote,
                            remote_path,
                        ),
                    )

                    uploaded += 1
                    bytes_uploaded += entry["size"]
                    if remote in crypt_map:
                        encrypted_files += 1
                    logger.info("Backed up: %s -> %s:%s", file_path, upload_remote, upload_path)
                else:
                    errors += 1
                    logger.error(
                        "Upload failed for %s: %s",
                        file_path,
                        result.stderr[:200],
                    )
            except Exception as e:
                errors += 1
                logger.error("Upload error for %s: %s", file_path, e)

        # Batch commit after each group instead of per-file
        if not dry_run:
            catalog.conn.commit()

    if progress_fn:
        progress_fn(
            {
                "phase": "complete",
                "progress_pct": 100,
                "uploaded": uploaded,
                "errors": errors,
            }
        )

    return {
        "uploaded": uploaded,
        "bytes_uploaded": bytes_uploaded,
        "encrypted_files": encrypted_files,
        "errors": errors,
        "skipped": skipped,
        "dry_run": dry_run,
        "targets_used": plan.targets_used,
    }


# ---------------------------------------------------------------------------
# Backup stats & health
# ---------------------------------------------------------------------------


def get_backup_stats(catalog: Catalog) -> BackupStats:
    """Get overall backup health statistics."""
    ensure_backup_tables(catalog)

    total = catalog.conn.execute("SELECT COUNT(*) FROM files WHERE sha256 IS NOT NULL").fetchone()[0]
    backed_up = catalog.conn.execute("SELECT COUNT(DISTINCT file_id) FROM backup_manifest").fetchone()[0]
    total_size = catalog.conn.execute("SELECT COALESCE(SUM(size), 0) FROM backup_manifest").fetchone()[0]

    # Per-remote breakdown
    remote_rows = catalog.conn.execute("""
        SELECT remote_name, COUNT(*) as cnt, SUM(size) as total_size
        FROM backup_manifest GROUP BY remote_name
    """).fetchall()

    files_by_remote = {}
    for r in remote_rows:
        files_by_remote[r[0]] = {"files": r[1], "size": r[2] or 0}

    # Last backup time
    last = catalog.conn.execute("SELECT MAX(backed_up_at) FROM backup_manifest").fetchone()[0]

    # Count healthy remotes (those that are still accessible)
    targets = get_targets(catalog)
    healthy = sum(1 for t in targets if t.enabled and t.free_bytes > 0)

    return BackupStats(
        total_files_in_catalog=total,
        backed_up_files=backed_up,
        not_backed_up=total - backed_up,
        backup_coverage_pct=round((backed_up / max(total, 1)) * 100, 1),
        total_backup_size=total_size,
        remotes_used=len(files_by_remote),
        remotes_healthy=healthy,
        last_backup_at=last,
        files_by_remote=files_by_remote,
    )


def get_manifest_for_file(catalog: Catalog, file_id: int) -> list[dict]:
    """Get all backup locations for a specific file."""
    ensure_backup_tables(catalog)
    rows = catalog.conn.execute(
        """
        SELECT remote_name, remote_path, backed_up_at, verified, verified_at
        FROM backup_manifest WHERE file_id = ?
    """,
        (file_id,),
    ).fetchall()

    return [
        {
            "remote": r[0],
            "path": r[1],
            "backed_up_at": r[2],
            "verified": bool(r[3]),
            "verified_at": r[4],
        }
        for r in rows
    ]


def verify_backups(
    catalog: Catalog,
    remote_name: str | None = None,
    progress_fn: Callable | None = None,
) -> dict:
    """Verify that backed up files still exist on remotes.

    Checks a sample of files per remote using rclone ls.
    """
    ensure_backup_tables(catalog)

    where = "WHERE remote_name = ?" if remote_name else ""
    params = (remote_name,) if remote_name else ()

    rows = catalog.conn.execute(
        f"""
        SELECT id, file_id, remote_name, remote_path, path, sha256
        FROM backup_manifest {where}
        ORDER BY backed_up_at DESC LIMIT 100
    """,
        params,
    ).fetchall()

    verified = 0
    missing = 0
    errors = 0

    for i, r in enumerate(rows):
        manifest_id, file_id, rname, rpath, fpath, sha = r
        fname = os.path.basename(fpath)

        if progress_fn:
            progress_fn(
                {
                    "phase": "verifying",
                    "current": i + 1,
                    "total": len(rows),
                    "progress_pct": int(((i + 1) / len(rows)) * 100),
                }
            )

        try:
            result = subprocess.run(
                [_rclone_bin(), "ls", f"{rname}:{rpath}/{fname}"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                catalog.conn.execute(
                    "UPDATE backup_manifest SET verified = 1, verified_at = datetime('now') WHERE id = ?",
                    (manifest_id,),
                )
                verified += 1
            else:
                missing += 1
                logger.warning("Backup missing: %s on %s:%s", fname, rname, rpath)
        except Exception as e:
            errors += 1
            logger.error("Verify error: %s", e)

    catalog.conn.commit()
    return {
        "verified": verified,
        "missing": missing,
        "errors": errors,
        "total_checked": len(rows),
    }


def auto_heal(catalog: Catalog, progress_fn: Callable | None = None) -> dict:
    """Auto-heal: find files whose backup remote is unreachable and re-plan them to other remotes.

    Steps:
    1. Check which remotes are currently accessible
    2. Find manifest entries on inaccessible remotes
    3. Remove those entries from manifest
    4. Re-run create_backup_plan to assign them to healthy remotes

    Returns summary dict.
    """
    ensure_backup_tables(catalog)
    targets = get_targets(catalog)

    # 1. Check accessibility
    healthy_remotes = set()
    unhealthy_remotes = set()

    for t in targets:
        if not t.enabled:
            continue
        try:
            result = subprocess.run(
                [_rclone_bin(), "lsd", f"{t.remote_name}:", "--max-depth", "1"],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if result.returncode == 0:
                healthy_remotes.add(t.remote_name)
            else:
                unhealthy_remotes.add(t.remote_name)
        except (OSError, subprocess.SubprocessError):
            unhealthy_remotes.add(t.remote_name)

    if not unhealthy_remotes:
        return {"status": "ok", "message": "V\u0161echny remoty jsou zdrav\u00e9", "healed": 0}

    # 2. Find affected files
    placeholders = ",".join("?" for _ in unhealthy_remotes)
    affected = catalog.conn.execute(
        f"""
        SELECT id, file_id, remote_name FROM backup_manifest
        WHERE remote_name IN ({placeholders})
    """,
        list(unhealthy_remotes),
    ).fetchall()

    if not affected:
        return {"status": "ok", "message": "\u017d\u00e1dn\u00e9 soubory na nezdrav\u00fdch remotech", "healed": 0}

    # 3. Remove affected entries
    for row_id, file_id, remote_name in affected:
        catalog.conn.execute("DELETE FROM backup_manifest WHERE id = ?", (row_id,))
    catalog.conn.commit()

    logger.info("Auto-heal: removed %d entries from unhealthy remotes %s", len(affected), unhealthy_remotes)

    # 4. Re-plan (these files will now be picked up by get_files_for_backup)
    plan = create_backup_plan(catalog)

    return {
        "status": "healed",
        "unhealthy_remotes": list(unhealthy_remotes),
        "healthy_remotes": list(healthy_remotes),
        "affected_files": len(affected),
        "new_plan_files": plan.total_files,
        "message": f"P\u0159erozd\u011blen{len(affected)} soubor\u016f z nezdrav\u00fdch remot\u016f",
    }


def remove_backup_entry(catalog: Catalog, file_id: int, remote_name: str) -> bool:
    """Remove a backup manifest entry (doesn't delete the remote file)."""
    ensure_backup_tables(catalog)
    catalog.conn.execute(
        "DELETE FROM backup_manifest WHERE file_id = ? AND remote_name = ?",
        (file_id, remote_name),
    )
    catalog.conn.commit()
    return True
