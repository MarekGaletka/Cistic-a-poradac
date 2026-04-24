from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

from .catalog import CatalogFileRow
from .models import DuplicateRow, FileRecord, ManualReviewRow, PlanPolicy, PlanRow
from .utils import path_startswith, write_tsv

if TYPE_CHECKING:
    from .catalog import Catalog


def _origin_time(rec: FileRecord) -> float:
    # Lower timestamp means older/original source.
    if rec.birthtime is not None and rec.birthtime > 0:
        return rec.birthtime
    return rec.mtime


def _score(
    rec: FileRecord,
    policy: PlanPolicy,
    file_cache: dict[str, CatalogFileRow] | None = None,
    catalog: Catalog | None = None,
) -> float:
    """Score a file for primary selection. Higher score = more likely to be kept.

    Uses file_cache (batch-loaded) for O(1) lookup. Falls back to catalog
    individual queries only when cache is not provided.
    """
    score = 0.0

    # 1. Preferred roots (highest priority — user explicitly chose these)
    rank = path_startswith(rec.path, policy.prefer_roots)
    if rank is not None:
        score += 1000.0 - (rank * 50.0)

    # 2. Origin time — older file is more likely the original
    if policy.prefer_earliest_origin_time:
        score += -_origin_time(rec) / 1_000_000_000.0

    # 3. Metadata richness + resolution + bitrate
    if policy.prefer_richer_metadata:
        file_row = None
        path_str = str(rec.path)

        # Try batch cache first (O(1)), then individual query
        if file_cache is not None:
            file_row = file_cache.get(path_str)
        elif catalog is not None:
            file_row = catalog.get_file_by_path(path_str)

        if file_row is not None:
            # Metadata richness from ExifTool scoring
            richness = getattr(file_row, "metadata_richness", None)
            if richness is not None:
                score += richness * 5.0
            else:
                score += rec.meaningful_xattr_count * 3.0

            # Resolution preference — higher resolution = better quality
            w = file_row.width or 0
            h = file_row.height or 0
            megapixels = (w * h) / 1_000_000
            score += min(megapixels, 50.0)

            # Bitrate preference for video/audio
            if file_row.bitrate:
                score += min(file_row.bitrate / 1_000_000, 30.0)
        else:
            score += rec.meaningful_xattr_count * 3.0

    # 4. Path length penalty — shorter paths are slightly preferred (tiebreaker)
    score += -(len(str(rec.path)) / 10_000.0)

    return score


def create_plan(
    duplicates: list[DuplicateRow],
    inventory: dict[Path, FileRecord],
    policy: PlanPolicy,
    catalog: Catalog | None = None,
) -> tuple[list[PlanRow], list[ManualReviewRow]]:
    by_hash: dict[str, list[DuplicateRow]] = defaultdict(list)
    for row in duplicates:
        by_hash[row.digest].append(row)

    # Batch-load all file rows from catalog to avoid N+1 queries
    file_cache: dict[str, CatalogFileRow] | None = None
    if catalog is not None:
        all_paths = [str(row.path) for row in duplicates]
        file_cache = catalog.get_files_by_paths(all_paths)

    plan: list[PlanRow] = []
    manual: list[ManualReviewRow] = []

    for digest, rows in by_hash.items():
        group_recs: list[FileRecord] = []
        missing = False
        for row in rows:
            rec = inventory.get(row.path)
            if rec is None:
                missing = True
                break
            group_recs.append(rec)

        if missing or len(group_recs) < 2:
            for row in rows:
                manual.append(
                    ManualReviewRow(
                        digest=digest,
                        size=row.size,
                        path=row.path,
                        reason="missing_inventory_record",
                    )
                )
            continue

        if policy.protect_asset_components and any(r.asset_component for r in group_recs):
            for rec in group_recs:
                manual.append(
                    ManualReviewRow(
                        digest=digest,
                        size=rec.size,
                        path=rec.path,
                        reason="asset_component_protected",
                    )
                )
            continue

        scored = sorted(
            ((rec, _score(rec, policy, file_cache=file_cache, catalog=catalog)) for rec in group_recs),
            key=lambda x: x[1],
            reverse=True,
        )

        keep_rec, keep_score = scored[0]
        for move_rec, move_score in scored[1:]:
            plan.append(
                PlanRow(
                    digest=digest,
                    size=move_rec.size,
                    keep_path=keep_rec.path,
                    move_path=move_rec.path,
                    reason="score_based_primary_selection",
                    keep_score=keep_score,
                    move_score=move_score,
                )
            )

    plan = _resolve_dependency_order(plan)
    manual.sort(key=lambda x: (str(x.path), x.digest))
    return plan, manual


def _resolve_dependency_order(plan: list[PlanRow]) -> list[PlanRow]:
    """Sort plan rows so that dependency chains are respected.

    A dependency exists when row A moves file X, and row B's keep_path is X.
    In that case, B must execute before A (we must finish using X as a "keep"
    reference before X gets moved away by another row).

    Uses topological sort; warns and falls back to stable sort if cycles exist.
    """
    import logging as _log

    _logger = _log.getLogger(__name__)

    # Build a map: move_path -> index of the row that moves it
    move_index: dict[Path, int] = {}
    for i, row in enumerate(plan):
        move_index[row.move_path] = i

    # Build adjacency: if row[i].keep_path == row[j].move_path,
    # then row[i] depends on row[j] (j must come first).
    from collections import defaultdict, deque

    adj: dict[int, list[int]] = defaultdict(list)  # j -> [i, ...] (j before i)
    in_degree: dict[int, int] = defaultdict(int)

    for i, row in enumerate(plan):
        j = move_index.get(row.keep_path)
        if j is not None and j != i:
            # row i depends on row j's move_path still existing as keep reference.
            # Actually: row i uses keep_path which is row j's move_path.
            # Row j will remove that file. So row i must run BEFORE row j.
            adj[i].append(j)
            in_degree[j] = in_degree.get(j, 0) + 1

    # Kahn's algorithm for topological sort
    queue: deque[int] = deque()
    for i in range(len(plan)):
        if in_degree.get(i, 0) == 0:
            queue.append(i)

    ordered_indices: list[int] = []
    while queue:
        node = queue.popleft()
        ordered_indices.append(node)
        for neighbor in adj.get(node, []):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(ordered_indices) != len(plan):
        # Cycle detected — log warning and use original order
        cycle_nodes = set(range(len(plan))) - set(ordered_indices)
        cycle_paths = [str(plan[i].move_path) for i in list(cycle_nodes)[:5]]
        _logger.warning(
            "Dependency cycle detected in plan involving %d rows (e.g. %s). Using default sort order; manual review recommended.",
            len(cycle_nodes),
            ", ".join(cycle_paths),
        )
        plan.sort(key=lambda x: (str(x.move_path), x.digest))
        return plan

    return [plan[i] for i in ordered_indices]


def write_plan_files(
    run_dir: Path,
    plan_rows: list[PlanRow],
    manual_rows: list[ManualReviewRow],
) -> None:
    write_tsv(
        run_dir / "plan_quarantine.tsv",
        ["hash", "size", "keep_path", "move_path", "reason", "keep_score", "move_score"],
        (
            (
                row.digest,
                row.size,
                str(row.keep_path),
                str(row.move_path),
                row.reason,
                f"{row.keep_score:.6f}",
                f"{row.move_score:.6f}",
            )
            for row in plan_rows
        ),
    )

    write_tsv(
        run_dir / "manual_review.tsv",
        ["hash", "size", "path", "reason"],
        ((row.digest, row.size, str(row.path), row.reason) for row in manual_rows),
    )
