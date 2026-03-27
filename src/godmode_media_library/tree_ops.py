from __future__ import annotations

import datetime as dt
import hashlib
import os
import re
import shutil
from collections import defaultdict
from pathlib import Path

import logging

from .audit import collect_file_records
from .disk_space import check_disk_space
from .models import FileRecord, TreePlanRow
from .utils import ensure_dir, read_tsv_dict, write_tsv

_logger = logging.getLogger(__name__)

_IMAGE_EXTS = {"jpg", "jpeg", "png", "heic", "gif", "tif", "tiff", "bmp", "webp"}
_VIDEO_EXTS = {"mov", "mp4", "m4v", "avi", "mkv", "mts", "3gp"}
_RAW_EXTS = {"raw", "dng", "cr2", "cr3", "nef", "arw", "orf", "rw2"}
_AUDIO_EXTS = {"mp3", "wav", "aac", "m4a", "flac"}
_DOC_EXTS = {"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "md", "pages", "numbers", "key"}
_ARCHIVE_EXTS = {"zip", "rar", "7z", "tar", "gz", "bz2", "xz"}


def _origin_ts(rec: FileRecord) -> float:
    if rec.birthtime is not None and rec.birthtime > 0:
        return rec.birthtime
    return rec.mtime


def _sanitize_segment(value: str) -> str:
    cleaned = value.strip()
    cleaned = re.sub(r"[\\/:*?\"<>|]", "_", cleaned)
    cleaned = cleaned.replace("\t", "_")
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned[:200] if cleaned else "Unknown"
    return cleaned or "Unknown"


def _date_bucket(ts: float, granularity: str) -> str:
    d = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
    if granularity == "year":
        return f"{d:%Y}"
    if granularity == "month":
        return f"{d:%Y}/{d:%m}"
    return f"{d:%Y}/{d:%m}/{d:%d}"


def _file_category(ext: str) -> str:
    e = ext.lower()
    if e in _RAW_EXTS:
        return "raw"
    if e in _IMAGE_EXTS:
        return "images"
    if e in _VIDEO_EXTS:
        return "videos"
    if e in _AUDIO_EXTS:
        return "audio"
    if e in _DOC_EXTS:
        return "documents"
    if e in _ARCHIVE_EXTS:
        return "archives"
    return "other"


def _unit_id(key: str) -> str:
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def _load_labels(path: Path | None) -> dict[Path, dict[str, str]]:
    if path is None:
        return {}
    rows = read_tsv_dict(path)
    labels: dict[Path, dict[str, str]] = {}
    for row in rows:
        raw_path = row.get("path", "").strip()
        if not raw_path:
            continue
        p = Path(raw_path).expanduser().resolve()
        labels[p] = {
            "people": row.get("people", "").strip(),
            "place": row.get("place", "").strip(),
        }
    return labels


def _pick_anchor(records: list[FileRecord]) -> FileRecord:
    def prio(rec: FileRecord) -> tuple[int, float, str]:
        ext = rec.ext.lower()
        if ext in _RAW_EXTS:
            p = 0
        elif ext in _IMAGE_EXTS:
            p = 1
        elif ext in _VIDEO_EXTS:
            p = 2
        else:
            p = 3
        return (p, _origin_ts(rec), str(rec.path))

    return sorted(records, key=prio)[0]


def _bucket_for(
    rec: FileRecord,
    mode: str,
    granularity: str,
    labels: dict[Path, dict[str, str]],
    unknown_label: str,
) -> str:
    if mode == "time":
        return _date_bucket(_origin_ts(rec), granularity)
    if mode == "modified":
        return _date_bucket(rec.mtime, granularity)
    if mode == "type":
        ext = rec.ext.lower() if rec.ext else "noext"
        return f"{_file_category(ext)}/{_sanitize_segment(ext or 'noext')}"
    if mode == "people":
        v = labels.get(rec.path, {}).get("people", "")
        return _sanitize_segment(v if v else unknown_label)
    if mode == "place":
        v = labels.get(rec.path, {}).get("place", "")
        return _sanitize_segment(v if v else unknown_label)
    raise ValueError(f"Unsupported mode: {mode}")


def _bucket_for_type_unit(members: list[FileRecord]) -> str:
    exts = {m.ext.lower() for m in members}
    has_img = any(e in _IMAGE_EXTS for e in exts)
    has_video = any(e in _VIDEO_EXTS for e in exts)
    has_raw = any(e in _RAW_EXTS for e in exts)

    if has_img and has_video:
        return "asset_sets/live_photo"
    if has_raw and has_img:
        return "asset_sets/raw_plus_preview"
    if has_video and not has_img and len(exts) > 1:
        return "asset_sets/video_bundle"
    if len(exts) > 1:
        return "asset_sets/mixed"

    anchor = _pick_anchor(members)
    ext = anchor.ext.lower() if anchor.ext else "noext"
    return f"{_file_category(ext)}/{_sanitize_segment(ext or 'noext')}"


def _allocate_destination(
    dest: Path,
    reserved: set[Path],
    reserved_norm: set[str] | None = None,
) -> Path:
    # Use normcase for case-insensitive comparison on macOS/Windows.
    # If a persistent reserved_norm set is passed, use it to avoid O(N)
    # rebuild on every call.
    if reserved_norm is None:
        reserved_norm = {os.path.normcase(str(p)) for p in reserved}
    dest_norm = os.path.normcase(str(dest))

    if dest_norm not in reserved_norm and not dest.exists():
        reserved.add(dest)
        reserved_norm.add(dest_norm)
        return dest

    stem = dest.stem
    suffix = dest.suffix
    parent = dest.parent
    for n in range(1, 100_000):
        cand = parent / f"{stem} ({n}){suffix}"
        cand_norm = os.path.normcase(str(cand))
        if cand_norm not in reserved_norm and not cand.exists():
            reserved.add(cand)
            reserved_norm.add(cand_norm)
            return cand
    raise RuntimeError(f"Cannot allocate destination after 100000 attempts: {dest}")


def create_tree_plan(
    roots: list[Path],
    target_root: Path,
    mode: str,
    granularity: str = "day",
    protect_asset_sets: bool = True,
    labels_tsv: Path | None = None,
    unknown_label: str = "Unknown",
) -> list[TreePlanRow]:
    records = collect_file_records(roots)
    labels = _load_labels(labels_tsv)

    units: dict[str, list[FileRecord]] = defaultdict(list)
    unit_asset_key: dict[str, str | None] = {}

    for rec in records:
        if protect_asset_sets and rec.asset_key and rec.asset_component:
            uid = f"asset::{_unit_id(rec.asset_key)}"
            unit_asset_key[uid] = rec.asset_key
        else:
            uid = f"file::{_unit_id(str(rec.path))}"
            unit_asset_key[uid] = rec.asset_key
        units[uid].append(rec)

    reserved: set[Path] = set()
    reserved_norm: set[str] = set()
    rows: list[TreePlanRow] = []
    mode_root = _sanitize_segment(f"by_{mode}")

    for uid, members in sorted(units.items(), key=lambda x: x[0]):
        anchor = _pick_anchor(members)
        if mode == "type":
            bucket = _bucket_for_type_unit(members)
        else:
            bucket = _bucket_for(anchor, mode=mode, granularity=granularity, labels=labels, unknown_label=unknown_label)

        for rec in sorted(members, key=lambda x: str(x.path)):
            target_dir = target_root / mode_root / bucket
            desired = target_dir / rec.path.name
            desired = _allocate_destination(desired, reserved, reserved_norm)

            if rec.path.resolve() == desired.resolve():
                continue

            rows.append(
                TreePlanRow(
                    unit_id=uid,
                    source_path=rec.path,
                    destination_path=desired,
                    mode=mode,
                    bucket=bucket,
                    asset_key=unit_asset_key.get(uid),
                    is_asset_component=bool(rec.asset_component),
                )
            )

    return rows


def write_tree_plan(plan_path: Path, rows: list[TreePlanRow]) -> None:
    write_tsv(
        plan_path,
        [
            "unit_id",
            "source_path",
            "destination_path",
            "mode",
            "bucket",
            "asset_key",
            "is_asset_component",
        ],
        (
            (
                row.unit_id,
                str(row.source_path),
                str(row.destination_path),
                row.mode,
                row.bucket,
                row.asset_key or "",
                int(row.is_asset_component),
            )
            for row in rows
        ),
    )


def apply_tree_plan(
    plan_path: Path,
    operation: str,
    dry_run: bool,
    collision_policy: str,
    log_path: Path,
) -> tuple[int, int]:
    rows = read_tsv_dict(plan_path)
    applied = 0
    skipped = 0
    log_rows: list[tuple[object, ...]] = []
    # Shared sets to track renames across all rows, preventing collisions
    _collision_reserved: set[Path] = set()
    _collision_reserved_norm: set[str] = set()

    for row in rows:
        src = Path(row["source_path"])
        dst = Path(row["destination_path"])

        if not src.exists():
            skipped += 1
            log_rows.append((str(src), str(dst), operation, "skip", "source_missing"))
            continue

        # Prevent moving a directory into itself or a subdirectory of itself
        if src.is_dir() and operation == "move":
            try:
                if dst.resolve().is_relative_to(src.resolve()):
                    skipped += 1
                    log_rows.append((str(src), str(dst), operation, "skip", "cannot_move_into_self"))
                    continue
            except (OSError, ValueError):
                pass

        final_dst = dst
        _overwrite_backup: Path | None = None

        if final_dst.exists():
            # Check if src and dst are the same file (same inode) — but allow case-only renames
            try:
                same_inode = os.stat(src).st_ino == os.stat(final_dst).st_ino and os.stat(src).st_dev == os.stat(final_dst).st_dev
            except OSError:
                same_inode = False
            if same_inode and str(src) == str(final_dst):
                skipped += 1
                log_rows.append((str(src), str(final_dst), operation, "skip", "already_in_place"))
                continue

            if collision_policy == "skip":
                skipped += 1
                log_rows.append((str(src), str(final_dst), operation, "skip", "collision"))
                continue
            if collision_policy == "rename":
                final_dst = _allocate_destination(final_dst, _collision_reserved, _collision_reserved_norm)
            elif collision_policy == "overwrite" and not dry_run:
                if final_dst.is_dir():
                    skipped += 1
                    log_rows.append((str(src), str(final_dst), operation, "skip", "collision_is_directory"))
                    continue
                # Safe swap: move target to temp, then move source, then delete temp.
                # If the source move fails, restore target from temp.
                _overwrite_backup = final_dst.parent / (final_dst.name + ".__gml_overwrite_bak__")
                try:
                    shutil.move(str(final_dst), str(_overwrite_backup))
                except OSError:
                    skipped += 1
                    log_rows.append((str(src), str(final_dst), operation, "skip", "overwrite_backup_failed"))
                    continue

        if not dry_run:
            ensure_dir(final_dst.parent)
            try:
                if operation == "move":
                    # Handle case-only rename on case-insensitive FS (e.g. Photo.jpg -> photo.JPG)
                    if (
                        os.path.normcase(str(src)) == os.path.normcase(str(final_dst))
                        and str(src) != str(final_dst)
                    ):
                        # Same inode, different case — two-step rename via temp name
                        tmp = final_dst.parent / (final_dst.name + ".__gml_tmp__")
                        shutil.move(str(src), str(tmp))
                        shutil.move(str(tmp), str(final_dst))
                    else:
                        # Check disk space before cross-filesystem move
                        try:
                            src_dev = os.stat(src).st_dev
                            dst_dev = os.stat(final_dst.parent).st_dev
                            if src_dev != dst_dev:
                                file_size = os.path.getsize(src) if src.is_file() else 0
                                if file_size and not check_disk_space(final_dst.parent, file_size):
                                    skipped += 1
                                    log_rows.append((str(src), str(final_dst), operation, "skip", "insufficient_disk_space"))
                                    continue
                        except OSError:
                            pass  # If stat fails, proceed and let the move report the error
                        shutil.move(str(src), str(final_dst))
                elif operation == "copy":
                    shutil.copy2(str(src), str(final_dst))
                elif operation == "hardlink":
                    os.link(src, final_dst)
                elif operation == "symlink":
                    os.symlink(src, final_dst)
                else:
                    raise ValueError(f"Unsupported operation: {operation}")
            except OSError as exc:
                import errno as errno_mod

                # If overwrite was in progress, restore the backup
                if _overwrite_backup is not None:
                    try:
                        if _overwrite_backup.exists() and not final_dst.exists():
                            shutil.move(str(_overwrite_backup), str(final_dst))
                    except OSError:
                        pass

                skipped += 1
                detail = f"os_error:{exc.errno}"
                if exc.errno == errno_mod.EXDEV:
                    detail = "cross_device_link:hardlinks_cannot_span_filesystems"
                log_rows.append((str(src), str(final_dst), operation, "skip", detail))
                continue

        # Clean up overwrite backup on success
        if _overwrite_backup is not None:
            try:
                if _overwrite_backup.exists():
                    _overwrite_backup.unlink()
            except OSError:
                pass

        applied += 1
        log_rows.append((str(src), str(final_dst), operation, "applied", "ok"))

    write_tsv(log_path, ["source_path", "destination_path", "operation", "status", "message"], log_rows)
    return applied, skipped
