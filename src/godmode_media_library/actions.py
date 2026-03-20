from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

from .utils import ensure_dir, read_tsv_dict, sha256_file, write_tsv

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ApplyResult:
    moved: int
    skipped: int
    moved_bytes: int
    rolled_back: int = 0
    error: str | None = None


def _quarantine_path(quarantine_root: Path, original_path: Path) -> Path:
    # Preserve absolute path shape under quarantine root.
    path_text = str(original_path)
    drive = ""
    rest = path_text
    if ":" in path_text[:3]:
        drive, rest = path_text.split(":", 1)
        drive = drive.replace(":", "")
        rest = rest.lstrip("\\/")
    else:
        rest = rest.lstrip("/")
    if drive:
        return quarantine_root / "_drive_" / drive / rest
    return quarantine_root / rest


def _rollback_moves(move_log: list[tuple[Path, Path]]) -> int:
    """Undo file moves recorded in move_log. Returns count of restored files."""
    restored = 0
    for original_path, quarantine_dest in reversed(move_log):
        try:
            if quarantine_dest.exists() and not original_path.exists():
                ensure_dir(original_path.parent)
                shutil.move(str(quarantine_dest), str(original_path))
                restored += 1
        except OSError as e:
            logger.error("Rollback failed for %s → %s: %s", quarantine_dest, original_path, e)
    return restored


def apply_plan(
    plan_path: Path,
    quarantine_root: Path,
    executed_log_path: Path,
    skipped_log_path: Path,
    dry_run: bool = False,
) -> ApplyResult:
    rows = read_tsv_dict(plan_path)

    executed_rows: list[tuple[object, ...]] = []
    skipped_rows: list[tuple[object, ...]] = []
    # Track actual file moves for rollback on unexpected failure
    move_log: list[tuple[Path, Path]] = []

    moved = 0
    skipped = 0
    moved_bytes = 0
    rolled_back = 0
    error_msg: str | None = None

    try:
        for row in rows:
            digest = row.get("hash", "")
            size = int(row.get("size", "0"))
            keep_path = Path(row.get("keep_path", ""))
            move_path = Path(row.get("move_path", ""))
            reason = row.get("reason", "")

            if not move_path.exists():
                skipped += 1
                skipped_rows.append((digest, size, str(keep_path), str(move_path), reason, "move_path_missing"))
                continue
            if not keep_path.exists():
                skipped += 1
                skipped_rows.append((digest, size, str(keep_path), str(move_path), reason, "keep_path_missing"))
                continue

            try:
                keep_hash = sha256_file(keep_path)
                move_hash = sha256_file(move_path)
            except OSError:
                skipped += 1
                skipped_rows.append((digest, size, str(keep_path), str(move_path), reason, "hash_read_error"))
                continue

            if keep_hash != move_hash:
                skipped += 1
                skipped_rows.append((digest, size, str(keep_path), str(move_path), reason, "hash_mismatch"))
                continue

            dest = _quarantine_path(quarantine_root, move_path)

            if not dry_run:
                ensure_dir(dest.parent)
                if dest.exists():
                    # Keep deterministic non-destructive behavior.
                    suffix = 1
                    candidate = Path(f"{dest}.dup{suffix}")
                    while candidate.exists():
                        suffix += 1
                        candidate = Path(f"{dest}.dup{suffix}")
                    dest = candidate
                shutil.move(str(move_path), str(dest))
                move_log.append((move_path, dest))

            moved += 1
            moved_bytes += size
            executed_rows.append((digest, size, str(keep_path), str(move_path), str(dest), reason, keep_hash))
    except Exception as exc:
        error_msg = f"Apply failed after {moved} moves: {exc}"
        logger.error(error_msg)
        if move_log and not dry_run:
            logger.info("Rolling back %d file moves...", len(move_log))
            rolled_back = _rollback_moves(move_log)
            logger.info("Rolled back %d/%d moves", rolled_back, len(move_log))

    write_tsv(
        executed_log_path,
        ["hash", "size", "keep_path", "move_path", "quarantine_path", "reason", "verified_hash"],
        executed_rows,
    )
    write_tsv(
        skipped_log_path,
        ["hash", "size", "keep_path", "move_path", "reason", "skip_reason"],
        skipped_rows,
    )

    return ApplyResult(moved=moved, skipped=skipped, moved_bytes=moved_bytes, rolled_back=rolled_back, error=error_msg)


def restore_from_log(log_path: Path, dry_run: bool = False) -> tuple[int, int]:
    rows = read_tsv_dict(log_path)
    restored = 0
    skipped = 0

    for row in rows:
        move_path = Path(row.get("move_path", ""))
        quarantine_path = Path(row.get("quarantine_path", ""))

        if not quarantine_path.exists():
            skipped += 1
            continue
        if move_path.exists():
            skipped += 1
            continue

        if not dry_run:
            ensure_dir(move_path.parent)
            shutil.move(str(quarantine_path), str(move_path))
        restored += 1

    return restored, skipped


def promote_from_manifest(
    manifest_path: Path,
    backup_root: Path,
    executed_log_path: Path,
    skipped_log_path: Path,
    dry_run: bool = False,
) -> tuple[int, int, int]:
    """Promote richer copy from quarantine into primary location.

    Manifest columns:
    - size
    - moved_from
    - quarantine_path
    - primary_path
    """
    rows = read_tsv_dict(manifest_path)

    executed_rows: list[tuple[object, ...]] = []
    skipped_rows: list[tuple[object, ...]] = []

    swapped = 0
    skipped = 0
    bytes_swapped = 0

    for row in rows:
        size = int(row.get("size", "0"))
        quarantine_path = Path(row.get("quarantine_path", ""))
        primary_path = Path(row.get("primary_path", ""))

        if not quarantine_path.exists() or not primary_path.exists():
            skipped += 1
            skipped_rows.append((size, str(quarantine_path), str(primary_path), "missing_path"))
            continue

        try:
            q_hash = sha256_file(quarantine_path)
            p_hash = sha256_file(primary_path)
        except OSError:
            skipped += 1
            skipped_rows.append((size, str(quarantine_path), str(primary_path), "hash_read_error"))
            continue

        if q_hash != p_hash:
            skipped += 1
            skipped_rows.append((size, str(quarantine_path), str(primary_path), "hash_mismatch"))
            continue

        backup_path = _quarantine_path(backup_root, primary_path)
        if not dry_run:
            ensure_dir(backup_path.parent)
            if backup_path.exists():
                suffix = 1
                candidate = Path(f"{backup_path}.dup{suffix}")
                while candidate.exists():
                    suffix += 1
                    candidate = Path(f"{backup_path}.dup{suffix}")
                backup_path = candidate
            shutil.move(str(primary_path), str(backup_path))
            ensure_dir(primary_path.parent)
            shutil.move(str(quarantine_path), str(primary_path))

        swapped += 1
        bytes_swapped += size
        executed_rows.append((size, str(quarantine_path), str(primary_path), str(backup_path), q_hash))

    write_tsv(
        executed_log_path,
        ["size", "quarantine_path", "primary_path", "backup_path", "verified_hash"],
        executed_rows,
    )
    write_tsv(
        skipped_log_path,
        ["size", "quarantine_path", "primary_path", "skip_reason"],
        skipped_rows,
    )

    return swapped, skipped, bytes_swapped
