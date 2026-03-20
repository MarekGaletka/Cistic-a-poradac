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

    plan.sort(key=lambda x: (str(x.move_path), x.digest))
    manual.sort(key=lambda x: (str(x.path), x.digest))
    return plan, manual


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
