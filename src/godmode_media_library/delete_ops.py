from __future__ import annotations

import hashlib
import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

from .asset_sets import build_asset_membership
from .disk_space import check_disk_space
from .utils import ensure_dir, iter_files, path_startswith, read_tsv_dict, write_tsv

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeletePlanResult:
    plan_path: Path
    summary_path: Path
    selected_seed_paths: int
    expanded_paths_total: int
    expanded_by_asset: int
    expanded_by_hardlink: int
    inode_units: int
    external_link_units: int
    reclaimable_unique_bytes: int


@dataclass(frozen=True)
class DeleteApplyResult:
    moved_primary: int
    unlinked_aliases: int
    skipped: int
    manual_review: int
    log_path: Path


def _inode_key(path: Path) -> tuple[int, int] | None:
    try:
        st = path.stat()
    except OSError:
        return None
    return (int(st.st_dev), int(st.st_ino))


def _inode_id(key: tuple[int, int]) -> str:
    raw = f"{key[0]}:{key[1]}".encode()
    return hashlib.sha256(raw).hexdigest()[:24]


def _quarantine_path(quarantine_root: Path, original_path: Path) -> Path:
    path_text = str(original_path)
    drive = ""
    rest = path_text
    if ":" in path_text[:3]:
        drive, rest = path_text.split(":", 1)
        drive = drive.replace(":", "")
        rest = rest.lstrip("\\/")
    else:
        rest = rest.lstrip("/")

    # Reject path traversal components to prevent escaping quarantine_root
    rest_parts = Path(rest).parts
    if ".." in rest_parts:
        raise ValueError(f"Path traversal detected: refusing to quarantine path with '..' components: {original_path}")

    result = quarantine_root / "_drive_" / drive / rest if drive else quarantine_root / rest

    # Final safety check: resolved destination must be under quarantine_root
    resolved = result.resolve()
    resolved_root = quarantine_root.resolve()
    if not (resolved == resolved_root or str(resolved).startswith(str(resolved_root) + "/")):
        raise ValueError(f"Path traversal detected: quarantine destination {resolved} escapes root {resolved_root}")

    return result


def _allocate_dest(dest: Path) -> Path:
    if not dest.exists():
        return dest
    stem = dest.stem
    ext = dest.suffix
    parent = dest.parent
    suffix = 1
    max_attempts = 10000
    candidate = parent / f"{stem}.dup{suffix}{ext}"
    while candidate.exists():
        suffix += 1
        if suffix > max_attempts:
            raise RuntimeError(f"Failed to allocate quarantine destination after {max_attempts} attempts: {dest}")
        candidate = parent / f"{stem}.dup{suffix}{ext}"
    return candidate


def _load_selected_paths(select_paths: Path | None, recommendations: Path | None) -> tuple[set[Path], list[str]]:
    selected: set[Path] = set()
    warnings: list[str] = []

    if select_paths and select_paths.exists():
        for line in select_paths.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            selected.add(Path(text).expanduser().resolve())

    if recommendations and recommendations.exists():
        rows = read_tsv_dict(recommendations)
        for row in rows:
            action = (row.get("action") or "").strip()
            manual = (row.get("requires_manual_review") or "").strip()
            path_text = (row.get("path") or "").strip()
            if not path_text:
                continue
            if action != "quarantine_candidate":
                continue
            if manual in {"1", "true", "True", "yes"}:
                continue
            selected.add(Path(path_text).expanduser().resolve())

    if not selected:
        warnings.append("No selected paths loaded from inputs.")
    return selected, warnings


def _pick_primary(paths: list[Path], prefer_roots: tuple[str, ...]) -> Path:
    def score(p: Path) -> tuple[float, str]:
        s = 0.0
        rank = path_startswith(p, prefer_roots)
        if rank is not None:
            s += 1000.0 - (rank * 50.0)
        s += -(len(str(p)) / 1000.0)
        return (s, str(p))

    return sorted(paths, key=score, reverse=True)[0]


def create_delete_plan(
    *,
    roots: list[Path],
    plan_path: Path,
    summary_path: Path,
    select_paths: Path | None = None,
    recommendations_tsv: Path | None = None,
    include_asset_sets: bool = True,
    prefer_roots: tuple[str, ...] = (),
    allow_external_links: bool = False,
) -> DeletePlanResult:
    all_paths = sorted({p.resolve() for p in iter_files(roots)})
    all_path_set = set(all_paths)

    inode_to_paths: dict[tuple[int, int], set[Path]] = {}
    inode_nlink: dict[tuple[int, int], int] = {}
    inode_size: dict[tuple[int, int], int] = {}
    path_to_inode: dict[Path, tuple[int, int]] = {}

    for path in all_paths:
        key = _inode_key(path)
        if key is None:
            continue
        path_to_inode[path] = key
        inode_to_paths.setdefault(key, set()).add(path)
        try:
            st = path.stat()
            inode_nlink[key] = int(st.st_nlink)
            inode_size[key] = int(st.st_size)
        except OSError:
            inode_nlink.setdefault(key, 1)
            inode_size.setdefault(key, 0)

    path_to_key, _, _ = build_asset_membership(all_paths)
    key_to_paths: dict[str, set[Path]] = {}
    for p, key in path_to_key.items():
        key_to_paths.setdefault(key, set()).add(p)

    seed_selected, warnings = _load_selected_paths(select_paths, recommendations_tsv)
    seed_selected = {p for p in seed_selected if p in all_path_set}

    expanded = set(seed_selected)
    queue = list(seed_selected)
    added_by_asset = 0
    added_by_hardlink = 0

    while queue:
        current = queue.pop()

        if include_asset_sets:
            asset_key = path_to_key.get(current)
            if asset_key:
                for sibling in key_to_paths.get(asset_key, set()):
                    if sibling not in expanded:
                        expanded.add(sibling)
                        queue.append(sibling)
                        added_by_asset += 1

        inode = path_to_inode.get(current)
        if inode:
            for alias in inode_to_paths.get(inode, set()):
                if alias not in expanded:
                    expanded.add(alias)
                    queue.append(alias)
                    added_by_hardlink += 1

    selected_inodes = sorted({path_to_inode[p] for p in expanded if p in path_to_inode})
    reclaimable_unique_bytes = sum(inode_size.get(k, 0) for k in selected_inodes)

    rows: list[tuple[object, ...]] = []
    external_link_units = 0
    for key in selected_inodes:
        unit_paths = sorted(inode_to_paths.get(key, set()), key=str)
        if not unit_paths:
            continue
        inode_id = _inode_id(key)
        expected_nlink = inode_nlink.get(key, len(unit_paths))
        found_in_scan = len(unit_paths)
        external_links = max(0, expected_nlink - found_in_scan)
        if external_links > 0:
            external_link_units += 1

        primary = _pick_primary(unit_paths, prefer_roots=prefer_roots)
        asset_key = path_to_key.get(primary, "")
        unit_size = inode_size.get(key, 0)

        for path in unit_paths:
            is_seed = int(path in seed_selected)
            if external_links > 0 and not allow_external_links:
                action = "manual_review_external_links"
                note = f"nlink={expected_nlink};scanned_links={found_in_scan}"
            else:
                if path == primary:
                    action = "move_primary"
                    note = "quarantine_primary_copy"
                else:
                    action = "unlink_alias"
                    note = "remove_extra_hardlink_alias"

            rows.append(
                (
                    inode_id,
                    str(path),
                    action,
                    str(primary),
                    asset_key,
                    is_seed,
                    unit_size,
                    expected_nlink,
                    found_in_scan,
                    external_links,
                    note,
                )
            )

    write_tsv(
        plan_path,
        [
            "inode_id",
            "path",
            "action",
            "primary_path",
            "asset_key",
            "selected_seed",
            "unit_size",
            "nlink_expected",
            "nlink_scanned",
            "external_links",
            "note",
        ],
        rows,
    )

    ensure_dir(summary_path.parent)
    summary = {
        "roots": [str(r) for r in roots],
        "plan_path": str(plan_path),
        "selected_seed_paths": len(seed_selected),
        "expanded_paths_total": len(expanded),
        "expanded_by_asset": added_by_asset,
        "expanded_by_hardlink": added_by_hardlink,
        "inode_units": len(selected_inodes),
        "external_link_units": external_link_units,
        "allow_external_links": allow_external_links,
        "reclaimable_unique_bytes": reclaimable_unique_bytes,
        "warnings": warnings,
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")

    return DeletePlanResult(
        plan_path=plan_path,
        summary_path=summary_path,
        selected_seed_paths=len(seed_selected),
        expanded_paths_total=len(expanded),
        expanded_by_asset=added_by_asset,
        expanded_by_hardlink=added_by_hardlink,
        inode_units=len(selected_inodes),
        external_link_units=external_link_units,
        reclaimable_unique_bytes=reclaimable_unique_bytes,
    )


def _format_bytes(n: int) -> str:
    """Format byte count for human-readable display."""
    if n < 1024:
        return f"{n} B"
    elif n < 1024**2:
        return f"{n / 1024:.1f} KiB"
    elif n < 1024**3:
        return f"{n / 1024**2:.1f} MiB"
    else:
        return f"{n / 1024**3:.2f} GiB"


def apply_delete_plan(
    *,
    plan_path: Path,
    quarantine_root: Path,
    log_path: Path,
    dry_run: bool = False,
    yes: bool = False,
) -> DeleteApplyResult:
    rows = read_tsv_dict(plan_path)
    ensure_dir(log_path.parent)

    order = {
        "move_primary": 0,
        "unlink_alias": 1,
        "manual_review_external_links": 2,
    }
    rows_sorted = sorted(rows, key=lambda r: (order.get(r.get("action", ""), 9), r.get("inode_id", ""), r.get("path", "")))

    # Confirmation prompt unless --yes or dry_run
    if not dry_run and not yes:
        move_count = sum(1 for r in rows_sorted if r.get("action") == "move_primary")
        unlink_count = sum(1 for r in rows_sorted if r.get("action") == "unlink_alias")
        review_count = sum(1 for r in rows_sorted if r.get("action") == "manual_review_external_links")
        total_bytes = sum(int(r.get("unit_size", 0)) for r in rows_sorted if r.get("action") == "move_primary")
        print(f"Delete plan: {plan_path}")
        print(f"  Quarantine to: {quarantine_root}")
        print(f"  Files to move (primary):  {move_count}")
        print(f"  Hardlink aliases to unlink: {unlink_count}")
        print(f"  Manual review (skipped):  {review_count}")
        print(f"  Estimated data moved: {_format_bytes(total_bytes)}")
        try:
            answer = input("\nProceed with destructive deletion? [y/N] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            answer = ""
        if answer not in ("y", "yes"):
            print("Aborted.")
            return DeleteApplyResult(
                moved_primary=0,
                unlinked_aliases=0,
                skipped=len(rows_sorted),
                manual_review=0,
                log_path=log_path,
            )

    moved_primary = 0
    unlinked_aliases = 0
    skipped = 0
    manual_review = 0
    log_rows: list[tuple[object, ...]] = []
    _primary_moved_inodes: dict[str, int] = {}  # Track inodes with successful primary moves (inode_id -> count)

    for row in rows_sorted:
        action = row.get("action", "")
        path = Path(row.get("path", ""))
        inode_id = row.get("inode_id", "")

        if action == "manual_review_external_links":
            manual_review += 1
            log_rows.append((inode_id, str(path), action, "manual_review", "", row.get("note", "")))
            continue

        if not path.exists():
            skipped += 1
            log_rows.append((inode_id, str(path), action, "skipped", "", "path_missing"))
            continue

        if action == "move_primary":
            dest = _allocate_dest(_quarantine_path(quarantine_root, path))
            if not dry_run:
                # Check disk space before moving
                try:
                    file_size = path.stat().st_size
                except OSError:
                    file_size = 0
                if file_size and not check_disk_space(dest.parent, file_size):
                    skipped += 1
                    log_rows.append((inode_id, str(path), action, "skipped", "", "insufficient_disk_space"))
                    continue
                ensure_dir(dest.parent)
                try:
                    shutil.move(str(path), str(dest))
                except Exception:
                    skipped += 1
                    log_rows.append((inode_id, str(path), action, "skipped", "", "move_failed"))
                    continue
            _primary_moved_inodes[inode_id] = _primary_moved_inodes.get(inode_id, 0) + 1
            moved_primary += 1
            log_rows.append((inode_id, str(path), action, "applied", str(dest), "ok"))
            continue

        if action == "unlink_alias":
            # Only unlink aliases for inodes whose primary was successfully moved
            if inode_id not in _primary_moved_inodes:
                skipped += 1
                log_rows.append((inode_id, str(path), action, "skipped", "", "primary_move_not_confirmed"))
                continue
            # Hardlink safety: verify the inode still has the expected nlink
            # before unlinking, to prevent accidental data loss.
            # After move_primary, nlink may decrease by 1 per cross-FS move
            # (same-FS rename preserves nlink). Account for moved primaries.
            try:
                st = path.stat()
                current_nlink = int(st.st_nlink)
                expected = int(row.get("nlink_expected", "0"))
                moved_count = _primary_moved_inodes.get(inode_id, 0)
                if expected > 0 and not (expected - moved_count <= current_nlink <= expected):
                    skipped += 1
                    log_rows.append(
                        (
                            inode_id,
                            str(path),
                            action,
                            "skipped",
                            "",
                            f"nlink_changed:expected={expected},actual={current_nlink}",
                        )
                    )
                    continue
            except OSError:
                skipped += 1
                log_rows.append((inode_id, str(path), action, "skipped", "", "stat_failed"))
                continue

            if not dry_run:
                path.unlink()
            unlinked_aliases += 1
            log_rows.append((inode_id, str(path), action, "applied", "", "ok"))
            continue

        skipped += 1
        log_rows.append((inode_id, str(path), action, "skipped", "", "unknown_action"))

    write_tsv(
        log_path,
        ["inode_id", "path", "action", "status", "quarantine_path", "message"],
        log_rows,
    )

    return DeleteApplyResult(
        moved_primary=moved_primary,
        unlinked_aliases=unlinked_aliases,
        skipped=skipped,
        manual_review=manual_review,
        log_path=log_path,
    )
