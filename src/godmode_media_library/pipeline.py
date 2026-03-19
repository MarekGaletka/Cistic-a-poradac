"""Unified pipeline: scan → extract → diff → plan → merge → apply in one command."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from .catalog import Catalog, default_catalog_path
from .exiftool_extract import extract_all_metadata
from .metadata_merge import create_merge_plan, execute_merge
from .metadata_richness import compute_group_diff, compute_richness, richest_file
from .scanner import incremental_scan

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for the unified pipeline."""

    roots: list[Path]
    catalog_path: Path | None = None
    exiftool_bin: str = "exiftool"
    auto_merge: bool = False
    dry_run: bool = False
    interactive: bool = True
    workers: int = 1
    min_size_bytes: int = 0
    skip_steps: set[str] = field(default_factory=set)


@dataclass
class PipelineResult:
    """Result of a pipeline run."""

    files_scanned: int = 0
    files_new: int = 0
    metadata_extracted: int = 0
    duplicate_groups: int = 0
    merge_plans_created: int = 0
    tags_merged: int = 0
    errors: list[str] = field(default_factory=list)


def run_pipeline(
    config: PipelineConfig,
    confirm_fn: Callable[[str], bool] | None = None,
) -> PipelineResult:
    """Execute full deduplication pipeline.

    Steps:
    1. Scan (incremental with ExifTool extraction)
    2. Metadata extraction (for files without metadata)
    3. Duplicate group analysis (diff + merge plans)
    4. [Checkpoint] Show summary, ask confirmation
    5. Execute merges + report

    Args:
        config: Pipeline configuration.
        confirm_fn: Callback for confirmation prompts. Returns True to proceed.
                   If None and interactive=True, uses input().
    """
    result = PipelineResult()
    catalog_path = config.catalog_path or default_catalog_path()
    catalog = Catalog(catalog_path)

    if confirm_fn is None and config.interactive:
        def confirm_fn(msg: str) -> bool:
            answer = input(f"{msg} [y/N]: ").strip().lower()
            return answer in ("y", "yes")

    with catalog:
        # ── Step 1: Scan ──────────────────────────────────────────────
        if "scan" not in config.skip_steps:
            logger.info("Pipeline step 1/4: Scanning...")
            stats = incremental_scan(
                catalog,
                config.roots,
                min_size_bytes=config.min_size_bytes,
                extract_media=True,
                compute_phash=True,
                extract_exiftool=True,
                exiftool_bin=config.exiftool_bin,
                workers=config.workers,
            )
            result.files_scanned = stats.files_scanned
            result.files_new = stats.files_new

        # ── Step 2: Metadata extraction (catch any missed) ────────────
        if "extract" not in config.skip_steps:
            logger.info("Pipeline step 2/4: Metadata extraction...")
            paths_needing = [Path(p) for p in catalog.paths_without_metadata()]
            if paths_needing:
                all_meta = extract_all_metadata(paths_needing, bin_path=config.exiftool_bin)
                for path, meta in all_meta.items():
                    richness = compute_richness(meta)
                    catalog.upsert_file_metadata(str(path), json.dumps(meta))
                    catalog.update_metadata_richness(str(path), richness.total)
                    result.metadata_extracted += 1
                catalog.commit()

        # ── Step 3: Analyze duplicate groups ──────────────────────────
        if "diff" not in config.skip_steps:
            logger.info("Pipeline step 3/4: Analyzing duplicate groups...")
            group_ids = catalog.get_all_duplicate_group_ids()
            result.duplicate_groups = len(group_ids)

            merge_plans = []
            for gid in group_ids:
                group_meta = catalog.get_group_metadata(gid)
                if len(group_meta) < 2:
                    continue

                diff = compute_group_diff(group_meta)
                survivor = richest_file(group_meta)
                if not survivor:
                    continue

                survivor_meta = dict(next((m for p, m in group_meta if p == survivor), {}))
                plan = create_merge_plan(survivor, survivor_meta, diff)

                if plan.actions:
                    merge_plans.append(plan)
                    result.merge_plans_created += 1

            # ── Checkpoint ────────────────────────────────────────────
            if config.interactive and confirm_fn and merge_plans:
                summary = (
                    f"Found {result.duplicate_groups} duplicate groups, "
                    f"{result.merge_plans_created} need metadata merge "
                    f"({sum(len(p.actions) for p in merge_plans)} tags total). "
                    f"Proceed with merge?"
                )
                if not confirm_fn(summary):
                    logger.info("Pipeline aborted by user at merge checkpoint")
                    return result

            # ── Step 4: Execute merges ────────────────────────────────
            if "merge" not in config.skip_steps:
                logger.info("Pipeline step 4/4: Executing metadata merges...")
                for plan in merge_plans:
                    merge_result = execute_merge(plan, bin_path=config.exiftool_bin, dry_run=config.dry_run)
                    if merge_result.error:
                        result.errors.append(f"{plan.survivor_path}: {merge_result.error}")
                    else:
                        result.tags_merged += merge_result.applied

    logger.info(
        "Pipeline complete: scanned=%d, extracted=%d, groups=%d, merged=%d tags, errors=%d",
        result.files_scanned, result.metadata_extracted,
        result.duplicate_groups, result.tags_merged, len(result.errors),
    )
    return result
