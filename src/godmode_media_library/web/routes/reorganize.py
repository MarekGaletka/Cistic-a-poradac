from __future__ import annotations

import time
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from ..shared import (
    DedupRulesRequest,
    ReorganizeConfigRequest,
    ReorganizeExecuteRequest,
    _create_task,
    _evict_old_plans,
    _finish_task,
    _reorganize_plans,
    _reorganize_plans_lock,
    _update_progress,
)

router = APIRouter()


@router.get("/config/dedup-rules")
async def get_dedup_rules(request: Request):
    """Get current deduplication rules."""
    from ...config import load_config

    config = load_config()
    return {
        "strategy": config.dedup_strategy,
        "similarity_threshold": config.dedup_similarity_threshold,
        "auto_resolve": config.dedup_auto_resolve,
        "merge_metadata": config.dedup_merge_metadata,
        "quarantine_path": config.dedup_quarantine_path,
        "exclude_extensions": config.dedup_exclude_extensions,
        "exclude_paths": config.dedup_exclude_paths,
        "min_file_size_kb": config.dedup_min_file_size_kb,
    }


@router.put("/config/dedup-rules")
async def put_dedup_rules(request: Request, body: DedupRulesRequest):
    """Update deduplication rules. Saves to global config.toml."""
    import tomllib  # Lazy import: Python 3.11+ only

    from ...config import _global_config_path

    config_path = _global_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)

    # Read existing config
    existing = {}
    if config_path.is_file():
        with config_path.open("rb") as f:
            existing = tomllib.load(f)

    # Update dedup fields
    existing["dedup_strategy"] = body.strategy
    existing["dedup_similarity_threshold"] = body.similarity_threshold
    existing["dedup_auto_resolve"] = body.auto_resolve
    existing["dedup_merge_metadata"] = body.merge_metadata
    existing["dedup_quarantine_path"] = body.quarantine_path
    existing["dedup_exclude_extensions"] = body.exclude_extensions
    existing["dedup_exclude_paths"] = body.exclude_paths
    existing["dedup_min_file_size_kb"] = body.min_file_size_kb

    # Write back as TOML
    lines = []
    for key, value in existing.items():
        if isinstance(value, bool):
            lines.append(f"{key} = {'true' if value else 'false'}")
        elif isinstance(value, str):
            lines.append(f'{key} = "{value}"')
        elif isinstance(value, list):
            items = ", ".join(f'"{v}"' for v in value)
            lines.append(f"{key} = [{items}]")
        else:
            lines.append(f"{key} = {value}")

    config_path.write_text("\n".join(lines) + "\n")

    return {"status": "ok"}


@router.get("/reorganize/sources")
def get_reorganize_sources():
    """Detect available media sources (mounted volumes, common folders)."""
    from ...reorganize import detect_sources

    return {"sources": detect_sources()}


@router.post("/reorganize/plan")
def start_reorganize_plan(request: Request, background_tasks: BackgroundTasks, config: ReorganizeConfigRequest):
    """Start planning reorganization (background task)."""
    from ...reorganize import ReorganizeConfig, plan_reorganization

    task = _create_task("reorganize-plan")

    def run_plan():
        try:
            rc = ReorganizeConfig(
                sources=[Path(s) for s in config.sources],
                destination=Path(config.destination),
                structure_pattern=config.structure_pattern,
                deduplicate=config.deduplicate,
                merge_metadata=config.merge_metadata,
                delete_originals=config.delete_originals,
                dry_run=True,  # planning is always dry
                workers=config.workers,
                exclude_patterns=config.exclude_patterns,
            )
            cat_path = request.app.state.catalog_path

            def on_progress(info):
                _update_progress(task.id, info)

            plan = plan_reorganization(rc, catalog_path=cat_path, progress_fn=on_progress)

            # Store plan for later execution (with eviction)
            with _reorganize_plans_lock:
                _evict_old_plans()
                _reorganize_plans[task.id] = (time.monotonic(), plan)

            # Build summary for the client
            summary = {
                "total_files": plan.total_files,
                "unique_files": plan.unique_files,
                "duplicate_files": plan.duplicate_files,
                "total_size": plan.total_size,
                "unique_size": plan.unique_size,
                "duplicate_size": plan.duplicate_size,
                "categories": plan.categories,
                "source_stats": {str(k): v for k, v in plan.source_stats.items()},
                "errors": plan.errors[:50],
                "plan_id": task.id,
            }
            _finish_task(task.id, result=summary)
        except Exception as e:
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run_plan)
    return {"task_id": task.id, "status": "started"}


@router.post("/reorganize/execute")
def start_reorganize_execute(request: Request, background_tasks: BackgroundTasks, body: ReorganizeExecuteRequest):
    """Execute a previously planned reorganization."""
    with _reorganize_plans_lock:
        _evict_old_plans()
        plan_entry = _reorganize_plans.get(body.plan_id)
    if not plan_entry:
        raise HTTPException(status_code=404, detail="Plan not found. Please re-scan.")
    plan = plan_entry[1]  # unwrap (timestamp, plan) tuple

    from ...reorganize import execute_reorganization

    task = _create_task("reorganize-execute")

    # Override delete_originals from execution request
    plan.config.delete_originals = body.delete_originals
    plan.config.dry_run = False

    def run_execute():
        try:

            def on_progress(info):
                _update_progress(task.id, info)

            result = execute_reorganization(plan, progress_fn=on_progress)

            _finish_task(
                task.id,
                result={
                    "files_processed": result.files_processed,
                    "files_copied": result.files_copied,
                    "files_skipped": result.files_skipped,
                    "originals_deleted": result.originals_deleted,
                    "space_saved": result.space_saved,
                    "errors": result.errors[:50],
                },
            )

            # Clean up the plan
            with _reorganize_plans_lock:
                _reorganize_plans.pop(body.plan_id, None)
        except Exception as e:
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run_execute)
    return {"task_id": task.id, "status": "started"}
