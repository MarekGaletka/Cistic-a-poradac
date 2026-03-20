from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

from .asset_sets import build_asset_membership
from .utils import ensure_dir, iter_files, path_startswith, read_tsv_dict, write_tsv


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
    return hashlib.sha1(raw).hexdigest()[:16]


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
    if drive:
        return quarantine_root / "_drive_" / drive / rest
    return quarantine_root / rest


def _allocate_dest(dest: Path) -> Path:
    if not dest.exists():
        return dest
    suffix = 1
    candidate = Path(f"{dest}.dup{suffix}")
    while candidate.exists():
        suffix += 1
        candidate = Path(f"{dest}.dup{suffix}")
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


def apply_delete_plan(
    *,
    plan_path: Path,
    quarantine_root: Path,
    log_path: Path,
    dry_run: bool = False,
) -> DeleteApplyResult:
    rows = read_tsv_dict(plan_path)
    ensure_dir(log_path.parent)

    order = {
        "move_primary": 0,
        "unlink_alias": 1,
        "manual_review_external_links": 2,
    }
    rows_sorted = sorted(rows, key=lambda r: (order.get(r.get("action", ""), 9), r.get("inode_id", ""), r.get("path", "")))

    moved_primary = 0
    unlinked_aliases = 0
    skipped = 0
    manual_review = 0
    log_rows: list[tuple[object, ...]] = []

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
                ensure_dir(dest.parent)
                shutil.move(str(path), str(dest))
            moved_primary += 1
            log_rows.append((inode_id, str(path), action, "applied", str(dest), "ok"))
            continue

        if action == "unlink_alias":
            # Hardlink safety: verify the inode still has the expected nlink
            # before unlinking, to prevent accidental data loss
            try:
                st = path.stat()
                current_nlink = int(st.st_nlink)
                expected = int(row.get("nlink_expected", "0"))
                if expected > 0 and current_nlink != expected:
                    skipped += 1
                    log_rows.append((
                        inode_id, str(path), action, "skipped", "",
                        f"nlink_changed:expected={expected},actual={current_nlink}",
                    ))
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
