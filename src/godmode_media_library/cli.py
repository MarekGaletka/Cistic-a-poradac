from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from .actions import apply_plan, promote_from_manifest, restore_from_log, selective_restore
from .audit import collect_file_records, load_exact_duplicates, load_inventory, write_audit_run
from .autolabel_people import auto_people_labels
from .autolabel_place import auto_place_labels
from .catalog import Catalog, default_catalog_path
from .config import format_config, load_config
from .delete_ops import apply_delete_plan, create_delete_plan
from .exiftool_extract import extract_all_metadata
from .logging_config import setup_logging
from .metadata_merge import create_merge_plan, execute_merge, write_merge_plan_tsv
from .metadata_richness import compute_group_diff, compute_richness
from .models import PlanPolicy
from .perceptual_hash import find_similar
from .planning import create_plan, write_plan_files
from .prune_recommend import recommend_prune
from .scanner import incremental_scan
from .tree_ops import apply_tree_plan, create_tree_plan, write_tree_plan
from .utils import ensure_dir, utc_stamp, write_tsv
from .verify import verify_catalog

logger = logging.getLogger(__name__)


def _parse_roots(value: list[str]) -> list[Path]:
    roots = [Path(v).expanduser().resolve() for v in value]
    return roots


def _build_policy(args: argparse.Namespace) -> PlanPolicy:
    prefer_roots = tuple(Path(p).expanduser().resolve().as_posix() for p in (args.prefer_root or []))
    return PlanPolicy(
        protect_asset_components=not args.allow_asset_component_dedupe,
        prefer_earliest_origin_time=not args.no_prefer_earliest_origin,
        prefer_richer_metadata=not args.no_prefer_richer_metadata,
        prefer_roots=prefer_roots,
    )


def cmd_audit(args: argparse.Namespace) -> int:
    roots = _parse_roots(args.roots)
    out_dir = Path(args.out_dir).expanduser().resolve()
    ensure_dir(out_dir)

    run_dir = write_audit_run(
        roots=roots,
        out_dir=out_dir,
        min_size_bytes=args.min_size_kb * 1024,
        large_file_threshold_bytes=args.large_file_threshold_mb * 1024 * 1024,
        run_name=args.run_name,
    )

    inventory = load_inventory(run_dir / "file_inventory.tsv")
    duplicates = load_exact_duplicates(run_dir / "exact_duplicates.tsv")
    policy = _build_policy(args)
    plan_rows, manual_rows = create_plan(duplicates, inventory, policy)
    write_plan_files(run_dir, plan_rows, manual_rows)

    summary = {
        "run_dir": str(run_dir),
        "plan_rows": len(plan_rows),
        "manual_review_rows": len(manual_rows),
        "policy": {
            "protect_asset_components": policy.protect_asset_components,
            "prefer_earliest_origin_time": policy.prefer_earliest_origin_time,
            "prefer_richer_metadata": policy.prefer_richer_metadata,
            "prefer_roots": list(policy.prefer_roots),
        },
    }
    with (run_dir / "plan_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=True, indent=2)

    print(f"run_dir={run_dir}")
    print(f"plan_rows={len(plan_rows)}")
    print(f"manual_review_rows={len(manual_rows)}")
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir).expanduser().resolve()
    inventory = load_inventory(run_dir / "file_inventory.tsv")
    duplicates = load_exact_duplicates(run_dir / "exact_duplicates.tsv")
    policy = _build_policy(args)

    plan_rows, manual_rows = create_plan(duplicates, inventory, policy)
    write_plan_files(run_dir, plan_rows, manual_rows)

    print(f"run_dir={run_dir}")
    print(f"plan_rows={len(plan_rows)}")
    print(f"manual_review_rows={len(manual_rows)}")
    return 0


def cmd_apply(args: argparse.Namespace) -> int:
    plan_path = Path(args.plan).expanduser().resolve()
    run_dir = plan_path.parent

    if args.quarantine_root:
        quarantine_root = Path(args.quarantine_root).expanduser().resolve()
    else:
        quarantine_root = run_dir / "quarantine" / f"apply_{utc_stamp()}"

    ensure_dir(quarantine_root)

    executed_log = run_dir / "executed_moves.tsv"
    skipped_log = run_dir / "skipped_moves.tsv"

    result = apply_plan(
        plan_path=plan_path,
        quarantine_root=quarantine_root,
        executed_log_path=executed_log,
        skipped_log_path=skipped_log,
        dry_run=args.dry_run,
    )

    print(f"quarantine_root={quarantine_root}")
    print(f"moved={result.moved}")
    print(f"skipped={result.skipped}")
    print(f"moved_bytes={result.moved_bytes}")
    print(f"executed_log={executed_log}")
    print(f"skipped_log={skipped_log}")
    return 0


def cmd_restore(args: argparse.Namespace) -> int:
    log_path = Path(args.log).expanduser().resolve()

    if getattr(args, "last", None) is not None or getattr(args, "file", None):
        file_paths = [Path(p).expanduser().resolve() for p in args.file] if args.file else None
        restored, skipped = selective_restore(
            log_path,
            last_n=args.last,
            file_paths=file_paths,
            dry_run=args.dry_run,
        )
    else:
        restored, skipped = restore_from_log(log_path, dry_run=args.dry_run)

    print(f"restored={restored}")
    print(f"skipped={skipped}")
    return 0


def cmd_promote(args: argparse.Namespace) -> int:
    manifest_path = Path(args.manifest).expanduser().resolve()
    run_dir = manifest_path.parent

    if args.backup_root:
        backup_root = Path(args.backup_root).expanduser().resolve()
    else:
        backup_root = run_dir / "quarantine" / f"promote_backup_{utc_stamp()}"

    ensure_dir(backup_root)
    executed_log = run_dir / "promote_executed.tsv"
    skipped_log = run_dir / "promote_skipped.tsv"

    swapped, skipped, bytes_swapped = promote_from_manifest(
        manifest_path=manifest_path,
        backup_root=backup_root,
        executed_log_path=executed_log,
        skipped_log_path=skipped_log,
        dry_run=args.dry_run,
    )

    print(f"backup_root={backup_root}")
    print(f"swapped={swapped}")
    print(f"skipped={skipped}")
    print(f"swapped_bytes={bytes_swapped}")
    print(f"executed_log={executed_log}")
    print(f"skipped_log={skipped_log}")
    return 0


def cmd_tree_plan(args: argparse.Namespace) -> int:
    roots = _parse_roots(args.roots)
    target_root = Path(args.target_root).expanduser().resolve()
    run_dir = Path(args.out_dir).expanduser().resolve()
    ensure_dir(run_dir)
    plan_dir = run_dir / args.run_name if args.run_name else run_dir / f"tree_{args.mode}_{utc_stamp()}"
    ensure_dir(plan_dir)

    labels_tsv = Path(args.labels_tsv).expanduser().resolve() if args.labels_tsv else None
    rows = create_tree_plan(
        roots=roots,
        target_root=target_root,
        mode=args.mode,
        granularity=args.granularity,
        protect_asset_sets=not args.allow_asset_set_split,
        labels_tsv=labels_tsv,
        unknown_label=args.unknown_label,
    )
    plan_path = plan_dir / "tree_plan.tsv"
    write_tree_plan(plan_path, rows)

    print(f"plan_dir={plan_dir}")
    print(f"plan_path={plan_path}")
    print(f"rows={len(rows)}")
    return 0


def cmd_tree_apply(args: argparse.Namespace) -> int:
    plan_path = Path(args.plan).expanduser().resolve()
    log_path = Path(args.log).expanduser().resolve() if args.log else (plan_path.parent / "tree_apply_log.tsv")
    ensure_dir(log_path.parent)

    applied, skipped = apply_tree_plan(
        plan_path=plan_path,
        operation=args.operation,
        dry_run=args.dry_run,
        collision_policy=args.collision_policy,
        log_path=log_path,
    )
    print(f"applied={applied}")
    print(f"skipped={skipped}")
    print(f"log_path={log_path}")
    return 0


def cmd_labels_template(args: argparse.Namespace) -> int:
    roots = _parse_roots(args.roots)
    out_path = Path(args.out).expanduser().resolve()
    ensure_dir(out_path.parent)

    media_exts = {
        "jpg",
        "jpeg",
        "png",
        "heic",
        "gif",
        "mov",
        "mp4",
        "m4v",
        "avi",
        "mkv",
        "raw",
        "dng",
        "cr2",
        "cr3",
        "nef",
        "arw",
    }

    records = collect_file_records(roots)
    rows = []
    for rec in sorted(records, key=lambda x: str(x.path)):
        if not args.include_all and rec.ext.lower() not in media_exts:
            continue
        rows.append((str(rec.path), "", ""))

    with out_path.open("w", encoding="utf-8", newline="") as f:
        f.write("path\tpeople\tplace\n")
        for path, people, place in rows:
            f.write(f"{path}\t{people}\t{place}\n")

    print(f"labels_template={out_path}")
    print(f"rows={len(rows)}")
    return 0


def cmd_auto_place(args: argparse.Namespace) -> int:
    roots = _parse_roots(args.roots)
    labels_in = Path(args.labels_in).expanduser().resolve() if args.labels_in else None
    labels_out = Path(args.labels_out).expanduser().resolve()
    ensure_dir(labels_out.parent)

    report_dir = Path(args.report_dir).expanduser().resolve() if args.report_dir else (labels_out.parent / "auto_place_reports")
    ensure_dir(report_dir)
    geocode_cache = Path(args.geocode_cache).expanduser().resolve() if args.geocode_cache else None

    try:
        result = auto_place_labels(
            roots=roots,
            labels_in=labels_in,
            labels_out=labels_out,
            report_dir=report_dir,
            exiftool_bin=args.exiftool_bin,
            reverse_geocode=args.reverse_geocode,
            gdpr_acknowledged=args.gdpr_consent,
            geocode_cache_path=geocode_cache,
            geocode_min_delay_seconds=args.geocode_min_delay_seconds,
            overwrite_place=args.overwrite_place,
        )
    except RuntimeError as exc:
        print(f"error={exc}")
        return 2

    print(f"labels_out={result.labels_out}")
    print(f"report_path={result.report_path}")
    print(f"missing_path={result.missing_path}")
    print(f"scanned_files={result.scanned_files}")
    print(f"candidate_files={result.candidate_files}")
    print(f"gps_files={result.gps_files}")
    print(f"reverse_geocoded={result.reverse_geocoded}")
    print(f"touched_labels={result.touched_labels}")
    print(f"changed_labels={result.changed_labels}")
    print(f"unresolved_candidates={result.unresolved_candidates}")
    print(f"exiftool_used={result.exiftool_used}")
    return 0


def cmd_auto_people(args: argparse.Namespace) -> int:
    roots = _parse_roots(args.roots)
    labels_in = Path(args.labels_in).expanduser().resolve() if args.labels_in else None
    labels_out = Path(args.labels_out).expanduser().resolve()
    ensure_dir(labels_out.parent)

    report_dir = Path(args.report_dir).expanduser().resolve() if args.report_dir else (labels_out.parent / "auto_people_reports")
    ensure_dir(report_dir)

    try:
        result = auto_people_labels(
            roots=roots,
            labels_in=labels_in,
            labels_out=labels_out,
            report_dir=report_dir,
            model=args.model,
            max_dimension=args.max_dimension,
            eps=args.eps,
            min_samples=args.min_samples,
            person_prefix=args.person_prefix,
            overwrite_people=args.overwrite_people,
        )
    except RuntimeError as exc:
        print(f"error={exc}")
        return 2

    print(f"labels_out={result.labels_out}")
    print(f"report_path={result.report_path}")
    print(f"faces_path={result.faces_path}")
    print(f"clusters_path={result.clusters_path}")
    print(f"missing_path={result.missing_path}")
    print(f"scanned_files={result.scanned_files}")
    print(f"candidate_files={result.candidate_files}")
    print(f"processed_images={result.processed_images}")
    print(f"faces_detected={result.faces_detected}")
    print(f"clusters={result.clusters}")
    print(f"touched_labels={result.touched_labels}")
    print(f"changed_labels={result.changed_labels}")
    print(f"unresolved_candidates={result.unresolved_candidates}")
    print(f"model_engine={result.model_engine}")
    return 0


def cmd_prune_recommend(args: argparse.Namespace) -> int:
    roots = _parse_roots(args.roots)
    out_dir = Path(args.out_dir).expanduser().resolve()
    ensure_dir(out_dir)
    run_dir = out_dir / args.run_name if args.run_name else out_dir / f"prune_recommend_{utc_stamp()}"
    ensure_dir(run_dir)

    policy = _build_policy(args)
    result = recommend_prune(
        roots=roots,
        run_dir=run_dir,
        policy=policy,
        min_size_bytes=args.min_size_kb * 1024,
        include_system_noise=not args.no_system_noise,
    )

    print(f"run_dir={result.run_dir}")
    print(f"recommendations_tsv={result.recommendations_tsv}")
    print(f"recommended_paths_txt={result.recommended_paths_txt}")
    print(f"summary_json={result.summary_json}")
    print(f"total_recommendations={result.total_recommendations}")
    print(f"quarantine_candidates={result.quarantine_candidates}")
    print(f"manual_review={result.manual_review}")
    print(f"estimated_reclaim_bytes={result.estimated_reclaim_bytes}")
    return 0


def cmd_delete_plan(args: argparse.Namespace) -> int:
    roots = _parse_roots(args.roots)
    plan_path = Path(args.out).expanduser().resolve()
    ensure_dir(plan_path.parent)
    summary_path = (
        Path(args.summary_out).expanduser().resolve()
        if args.summary_out
        else (plan_path.parent / "delete_plan_summary.json")
    )

    select_paths = Path(args.select_paths).expanduser().resolve() if args.select_paths else None
    recommendations = Path(args.recommendations).expanduser().resolve() if args.recommendations else None
    if select_paths is None and recommendations is None:
        print("error=Provide at least one of --select-paths or --recommendations")
        return 2

    prefer_roots = tuple(Path(p).expanduser().resolve().as_posix() for p in (args.prefer_root or []))
    result = create_delete_plan(
        roots=roots,
        plan_path=plan_path,
        summary_path=summary_path,
        select_paths=select_paths,
        recommendations_tsv=recommendations,
        include_asset_sets=not args.no_asset_set_expansion,
        prefer_roots=prefer_roots,
        allow_external_links=args.allow_external_links,
    )

    print(f"plan_path={result.plan_path}")
    print(f"summary_path={result.summary_path}")
    print(f"selected_seed_paths={result.selected_seed_paths}")
    print(f"expanded_paths_total={result.expanded_paths_total}")
    print(f"expanded_by_asset={result.expanded_by_asset}")
    print(f"expanded_by_hardlink={result.expanded_by_hardlink}")
    print(f"inode_units={result.inode_units}")
    print(f"external_link_units={result.external_link_units}")
    print(f"reclaimable_unique_bytes={result.reclaimable_unique_bytes}")
    return 0


def cmd_config_show(args: argparse.Namespace) -> int:
    config = load_config()
    print(format_config(config), end="")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    try:
        import uvicorn
    except ImportError:
        print("Web UI requires: pip install godmode-media-library[web]")
        print("  or: pip install fastapi uvicorn[standard]")
        return 2

    from .web.app import create_app

    catalog_path = Path(args.catalog) if args.catalog else None
    app = create_app(catalog_path=catalog_path)

    if not args.no_browser:
        import threading
        import webbrowser
        threading.Timer(1.0, lambda: webbrowser.open(f"http://{args.host}:{args.port}")).start()

    print("GOD MODE Media Library — Web UI")
    print(f"http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


def cmd_watch(args: argparse.Namespace) -> int:
    from .watcher import watch_roots

    roots = _parse_roots(args.roots)
    catalog = _get_catalog(args)
    print(f"Watching {len(roots)} roots for changes...")
    print("Press Ctrl+C to stop.")
    watch_roots(roots, catalog.db_path)
    return 0


def cmd_auto(args: argparse.Namespace) -> int:
    from .pipeline import PipelineConfig, run_pipeline

    roots = _parse_roots(args.roots)
    catalog_path = Path(args.catalog) if args.catalog else None

    config = PipelineConfig(
        roots=roots,
        catalog_path=catalog_path,
        exiftool_bin=args.exiftool_bin,
        dry_run=args.dry_run,
        interactive=not args.no_interactive,
        workers=args.workers,
        min_size_bytes=args.min_size_kb * 1024,
        skip_steps=set(args.skip),
    )

    result = run_pipeline(config)

    print(f"files_scanned={result.files_scanned}")
    print(f"files_new={result.files_new}")
    print(f"metadata_extracted={result.metadata_extracted}")
    print(f"duplicate_groups={result.duplicate_groups}")
    print(f"merge_plans={result.merge_plans_created}")
    print(f"tags_merged={result.tags_merged}")
    if result.errors:
        print(f"errors={len(result.errors)}")
        for err in result.errors[:10]:
            print(f"  {err}")
    return 1 if result.errors else 0


def cmd_doctor(args: argparse.Namespace) -> int:
    from .deps import check_all, format_report

    exiftool_bin = getattr(args, "exiftool_bin", "exiftool")
    statuses = check_all(exiftool_bin=exiftool_bin)
    print(format_report(statuses), end="")
    missing = [s for s in statuses if not s.available]
    return 1 if missing else 0


def cmd_cloud(args: argparse.Namespace) -> int:
    from .cloud import check_rclone, format_cloud_guide, list_remotes, mount_command

    if not check_rclone():
        print("rclone is not installed.\n")
        print(format_cloud_guide())
        return 1

    remotes = list_remotes()
    if not remotes:
        print("rclone is installed but no remotes configured.")
        print("Run: rclone config")
        return 1

    print(f"rclone available — {len(remotes)} remote(s) configured:\n")
    for r in remotes:
        print(f"  {r.name} ({r.type})")
        print(f"    Mount: {mount_command(r.name)}")
    print()
    print("After mounting, scan with: gml scan --roots ~/mnt/<remote>/Photos")
    return 0


def _get_catalog(args: argparse.Namespace, *, exclusive: bool = False) -> Catalog:
    db_path = Path(args.catalog) if hasattr(args, "catalog") and args.catalog else default_catalog_path()
    return Catalog(db_path, exclusive=exclusive)


def cmd_scan(args: argparse.Namespace) -> int:
    roots = _parse_roots(args.roots)
    catalog = _get_catalog(args, exclusive=True)
    with catalog:
        stats = incremental_scan(
            catalog,
            roots,
            force_rehash=args.force_rehash,
            min_size_bytes=args.min_size_kb * 1024,
            extract_media=not args.no_media,
            compute_phash=not args.no_phash,
            extract_exiftool=getattr(args, "exiftool", False),
            exiftool_bin=getattr(args, "exiftool_bin", "exiftool"),
            workers=getattr(args, "workers", 1),
        )
    print(f"catalog={catalog.db_path}")
    print(f"files_scanned={stats.files_scanned}")
    print(f"files_new={stats.files_new}")
    print(f"files_changed={stats.files_changed}")
    print(f"files_removed={stats.files_removed}")
    print(f"bytes_hashed={stats.bytes_hashed}")
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    catalog = _get_catalog(args)
    with catalog:
        if args.duplicates:
            groups = catalog.query_duplicates()
            for group_id, files in groups:
                for f in files:
                    print(f"{group_id}\t{f.size}\t{f.path}")
            print(f"\n# {len(groups)} duplicate groups")
            return 0

        rows = catalog.query_files(
            ext=args.ext,
            date_from=args.date_from,
            date_to=args.date_to,
            min_size=args.min_size * 1024 if args.min_size else None,
            max_size=args.max_size * 1024 if args.max_size else None,
            path_contains=args.path_contains,
            camera=args.camera,
            min_duration=args.duration_min,
            max_duration=args.duration_max,
            min_width=args.resolution_min,
            has_gps=False if args.no_gps else None,
            limit=args.limit,
        )
        for f in rows:
            extra = ""
            if f.camera_model:
                extra += f"\t{f.camera_model}"
            if f.duration_seconds:
                extra += f"\t{f.duration_seconds:.1f}s"
            print(f"{f.path}\t{f.size}\t{f.ext}\t{f.sha256 or ''}{extra}")
        print(f"\n# {len(rows)} files")
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    catalog = _get_catalog(args)
    with catalog:
        s = catalog.stats()
    print(json.dumps(s, indent=2, ensure_ascii=False))
    return 0


def cmd_vacuum(args: argparse.Namespace) -> int:
    catalog = _get_catalog(args, exclusive=True)
    with catalog:
        catalog.vacuum()
    print(f"catalog={catalog.db_path}")
    print("VACUUM completed.")
    return 0


def cmd_catalog_import(args: argparse.Namespace) -> int:
    inventory_path = Path(args.inventory).expanduser().resolve()
    catalog = _get_catalog(args, exclusive=True)
    with catalog:
        count = catalog.import_from_inventory_tsv(inventory_path)
    print(f"catalog={catalog.db_path}")
    print(f"imported={count}")
    return 0


def cmd_catalog_export(args: argparse.Namespace) -> int:
    out_path = Path(args.out).expanduser().resolve()
    catalog = _get_catalog(args)
    with catalog:
        count = catalog.export_inventory_tsv(out_path)
    print(f"exported={count}")
    print(f"path={out_path}")
    return 0


def cmd_metadata_extract(args: argparse.Namespace) -> int:
    catalog = _get_catalog(args, exclusive=True)
    with catalog:
        if args.force:
            # Re-extract all
            rows = catalog.query_files(limit=1_000_000)
            paths = [Path(r.path) for r in rows]
        else:
            # Only extract for files without metadata
            path_strs = catalog.paths_without_metadata()
            paths = [Path(p) for p in path_strs]

        if not paths:
            print("All files already have metadata extracted.")
            return 0

        print(f"Extracting metadata for {len(paths)} files...")
        all_meta = extract_all_metadata(paths, bin_path=args.exiftool_bin)

        count = 0
        for path, meta in all_meta.items():
            path_str = str(path)
            richness = compute_richness(meta)
            catalog.upsert_file_metadata(path_str, json.dumps(meta, ensure_ascii=False))
            catalog.update_metadata_richness(path_str, richness.total)
            count += 1

        catalog.commit()
    print(f"catalog={catalog.db_path}")
    print(f"extracted={count}")
    print(f"total_paths={len(paths)}")
    return 0


def cmd_metadata_diff(args: argparse.Namespace) -> int:
    catalog = _get_catalog(args)
    with catalog:
        group_ids = [args.group] if args.group else catalog.get_all_duplicate_group_ids()

        if not group_ids:
            print("No duplicate groups found.")
            return 0

        report = []
        for gid in group_ids:
            group_meta = catalog.get_group_metadata(gid)
            if len(group_meta) < 2:
                continue
            diff = compute_group_diff(group_meta)
            group_report = {
                "group_id": gid,
                "files": len(group_meta),
                "scores": diff.scores,
                "unanimous_tags": len(diff.unanimous),
                "partial_tags": len(diff.partial),
                "conflict_tags": len(diff.conflicts),
                "partial_details": {tag: list(paths.keys()) for tag, paths in diff.partial.items()},
                "conflict_details": {
                    tag: {p: str(v) for p, v in paths.items()}
                    for tag, paths in diff.conflicts.items()
                },
            }
            report.append(group_report)

        if args.out:
            out_path = Path(args.out).expanduser().resolve()
            ensure_dir(out_path.parent)
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            print(f"out={out_path}")
        else:
            for r in report:
                print(f"group={r['group_id']} files={r['files']} partial={r['partial_tags']} conflicts={r['conflict_tags']}")
                for path, score in r["scores"].items():
                    print(f"  {score:.1f}  {path}")

        print(f"\n# {len(report)} groups analyzed")
    return 0


def cmd_metadata_merge(args: argparse.Namespace) -> int:
    catalog = _get_catalog(args, exclusive=True)
    with catalog:
        group_ids = [args.group] if args.group else catalog.get_all_duplicate_group_ids()

        if not group_ids:
            print("No duplicate groups found.")
            return 0

        total_merged = 0
        total_conflicts = 0
        total_skipped = 0
        plans_written = 0

        out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else None
        if out_dir:
            ensure_dir(out_dir)

        for gid in group_ids:
            group_meta = catalog.get_group_metadata(gid)
            if len(group_meta) < 2:
                continue
            # Skip groups where no file has metadata
            if all(not meta for _, meta in group_meta):
                continue

            diff = compute_group_diff(group_meta)
            if not diff.partial and not diff.conflicts:
                continue

            # Choose richest file as survivor
            scored = sorted(diff.scores.items(), key=lambda x: x[1], reverse=True)
            survivor_path = scored[0][0]
            survivor_meta = dict(next(meta for p, meta in group_meta if p == survivor_path))

            plan = create_merge_plan(survivor_path, survivor_meta, diff)

            if out_dir:
                plan_path = out_dir / f"merge_{gid[:16]}.tsv"
                write_merge_plan_tsv(plan_path, plan)
                plans_written += 1

            if args.apply:
                result = execute_merge(plan, bin_path=args.exiftool_bin, dry_run=args.dry_run)
                total_merged += result.applied
                total_conflicts += result.conflicts
                total_skipped += result.skipped
                if result.error:
                    print(f"error={result.error} group={gid}")
            else:
                total_merged += len(plan.actions)
                total_conflicts += len(plan.conflicts)
                total_skipped += len(plan.skipped)

    print(f"groups_processed={len(group_ids)}")
    print(f"tags_to_merge={total_merged}")
    print(f"conflicts={total_conflicts}")
    print(f"skipped={total_skipped}")
    if out_dir:
        print(f"plans_written={plans_written}")
        print(f"out_dir={out_dir}")
    return 0


def cmd_metadata_write(args: argparse.Namespace) -> int:
    from .exiftool_extract import write_tags

    paths = [Path(p).expanduser().resolve() for p in args.files]
    tags = {}
    for tag_spec in args.tags:
        if "=" not in tag_spec:
            print(f"Invalid tag format: {tag_spec} (expected TAG=VALUE)")
            return 1
        key, value = tag_spec.split("=", 1)
        tags[key] = value

    if not tags:
        print("No tags specified. Use --tag TAG=VALUE")
        return 1

    total_ok = 0
    total_fail = 0
    for path in paths:
        if not path.exists():
            print(f"skip: {path} (not found)")
            total_fail += 1
            continue
        ok, msg = write_tags(path, tags, bin_path=args.exiftool_bin, overwrite_original=args.overwrite_original)
        if ok:
            print(f"ok: {path}")
            total_ok += 1
        else:
            print(f"fail: {path} ({msg})")
            total_fail += 1

    print(f"\nwritten={total_ok} failed={total_fail}")
    return 0 if total_fail == 0 else 1


def cmd_similar(args: argparse.Namespace) -> int:
    catalog = _get_catalog(args)
    out_path = Path(args.out).expanduser().resolve() if args.out else None
    with catalog:
        hashes = catalog.get_all_phashes()
        if not hashes:
            print("No perceptual hashes in catalog. Run 'gml scan' first.")
            return 0
        pairs = find_similar(hashes, threshold=args.threshold)
        for pair in pairs:
            print(f"dist={pair.distance}\t{pair.path_a}\t{pair.path_b}")
        if out_path:
            ensure_dir(out_path.parent)
            write_tsv(
                out_path,
                ["distance", "path_a", "path_b", "hash_a", "hash_b"],
                [(p.distance, p.path_a, p.path_b, p.hash_a, p.hash_b) for p in pairs],
            )
            print(f"out={out_path}")
        print(f"\n# {len(pairs)} similar pairs (threshold={args.threshold})")
    return 0


def cmd_delete_apply(args: argparse.Namespace) -> int:
    plan_path = Path(args.plan).expanduser().resolve()
    quarantine_root = Path(args.quarantine_root).expanduser().resolve()
    ensure_dir(quarantine_root)
    log_path = Path(args.log).expanduser().resolve() if args.log else (plan_path.parent / "delete_apply_log.tsv")
    ensure_dir(log_path.parent)

    result = apply_delete_plan(
        plan_path=plan_path,
        quarantine_root=quarantine_root,
        log_path=log_path,
        dry_run=args.dry_run,
    )

    print(f"moved_primary={result.moved_primary}")
    print(f"unlinked_aliases={result.unlinked_aliases}")
    print(f"manual_review={result.manual_review}")
    print(f"skipped={result.skipped}")
    print(f"log_path={result.log_path}")
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    catalog = _get_catalog(args)
    with catalog:
        result = verify_catalog(
            catalog,
            check_hashes=args.check_hashes,
            limit=args.limit,
        )
    print(f"total_checked={result.total_checked}")
    print(f"ok={result.ok}")
    print(f"missing={len(result.missing_files)}")
    print(f"size_mismatches={len(result.size_mismatches)}")
    print(f"hash_mismatches={len(result.hash_mismatches)}")
    if result.missing_files:
        print("\n# Missing files:")
        for p in result.missing_files[:20]:
            print(f"  {p}")
        if len(result.missing_files) > 20:
            print(f"  ... and {len(result.missing_files) - 20} more")
    if result.size_mismatches:
        print("\n# Size mismatches:")
        for p, cat_sz, act_sz in result.size_mismatches[:20]:
            print(f"  {p}: catalog={cat_sz} actual={act_sz}")
    if result.hash_mismatches:
        print("\n# Hash mismatches:")
        for p, cat_h, act_h in result.hash_mismatches[:20]:
            print(f"  {p}: catalog={cat_h[:16]}... actual={act_h[:16]}...")
    return 1 if result.has_issues else 0


def cmd_export(args: argparse.Namespace) -> int:
    import csv

    catalog = _get_catalog(args)
    out_path = Path(args.out).expanduser().resolve()
    fmt = args.format

    with catalog:
        if args.what == "files":
            rows = catalog.query_files(limit=args.limit)
            headers = ["path", "size", "ext", "sha256", "date_original", "camera_make", "camera_model",
                       "width", "height", "gps_latitude", "gps_longitude"]
            data = []
            for r in rows:
                data.append([r.path, r.size, r.ext, r.sha256 or "", r.date_original or "",
                             r.camera_make or "", r.camera_model or "",
                             r.width or "", r.height or "",
                             r.gps_latitude or "", r.gps_longitude or ""])
        elif args.what == "duplicates":
            groups = catalog.query_duplicates()
            headers = ["group_id", "path", "size", "is_primary"]
            data = []
            for gid, files in groups:
                for i, f in enumerate(files):
                    data.append([gid, f.path, f.size, 1 if i == 0 else 0])
        else:
            print(f"Unknown export type: {args.what}")
            return 1

    ensure_dir(out_path.parent)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f) if fmt == "csv" else csv.writer(f, delimiter="\t")
        writer.writerow(headers)
        writer.writerows(data)

    print(f"exported={len(data)} rows")
    print(f"format={fmt}")
    print(f"path={out_path}")
    return 0


def cmd_batch_rename(args: argparse.Namespace) -> int:
    from .batch_rename import apply_renames, plan_renames

    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        print(f"Not a directory: {root}")
        return 1

    # Collect files
    files = sorted(root.glob(f"*.{args.ext}")) if args.ext else sorted(f for f in root.iterdir() if f.is_file())

    if not files:
        print("No files found.")
        return 0

    actions = plan_renames(files, args.pattern, start_number=args.start)

    if not actions:
        print("No renames needed.")
        return 0

    # Show preview
    for a in actions[:20]:
        print(f"  {a.original.name} → {a.new_name}")
    if len(actions) > 20:
        print(f"  ... and {len(actions) - 20} more")
    print(f"\nTotal: {len(actions)} renames")

    if args.dry_run:
        print("[DRY RUN] No files were renamed.")
        return 0

    result = apply_renames(actions)
    print(f"renamed={result.renamed}")
    print(f"skipped={result.skipped}")
    if result.errors:
        for err in result.errors[:5]:
            print(f"error: {err}")
    return 0 if result.skipped == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    from .i18n import t

    parser = argparse.ArgumentParser(
        prog="gml",
        description="GOD MODE media organizer with metadata-first safety",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity (-v INFO, -vv DEBUG)")
    parser.add_argument("--log-file", default=None, help="Path for JSON-formatted log file")
    parser.add_argument("--lang", choices=["en", "cs"], default=None, help="Language (en/cs)")

    sub = parser.add_subparsers(dest="command", required=True)

    pcfg = sub.add_parser("config", help="Show resolved configuration")
    pcfg.set_defaults(func=cmd_config_show)

    pdoc = sub.add_parser("doctor", help=t("help.doctor"))
    pdoc.add_argument("--exiftool-bin", default="exiftool", help="ExifTool binary path to check")
    pdoc.set_defaults(func=cmd_doctor)

    pauto = sub.add_parser("auto", help=t("help.auto"))
    pauto.add_argument("--roots", nargs="+", required=True, help=t("help.scan.roots"))
    pauto.add_argument("--catalog", default=None, help=t("help.scan.catalog"))
    pauto.add_argument("--exiftool-bin", default="exiftool", help="ExifTool binary path")
    pauto.add_argument("--workers", type=int, default=1, help=t("help.scan.workers"))
    pauto.add_argument("--min-size-kb", type=int, default=0, help="Min file size for hashing (KB)")
    pauto.add_argument("--dry-run", action="store_true", help="Simulate merges without writing")
    pauto.add_argument("--no-interactive", action="store_true", help="Skip confirmation prompts")
    pauto.add_argument("--skip", nargs="*", default=[], help="Steps to skip: scan, extract, diff, merge")
    pauto.set_defaults(func=cmd_auto)

    pcld = sub.add_parser("cloud", help=t("help.cloud"))
    pcld.set_defaults(func=cmd_cloud)

    psrv = sub.add_parser("serve", help=t("help.serve"))
    psrv.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    psrv.add_argument("--port", type=int, default=8080, help="Bind port (default: 8080)")
    psrv.add_argument("--catalog", default=None, help=t("help.scan.catalog"))
    psrv.add_argument("--no-browser", action="store_true", help="Don't open browser automatically")
    psrv.set_defaults(func=cmd_serve)

    pw = sub.add_parser("watch", help="Watch directories for changes and auto-scan")
    pw.add_argument("--roots", nargs="+", required=True, help="Directories to watch")
    pw.add_argument("--catalog", default=None, help="Catalog DB path")
    pw.set_defaults(func=cmd_watch)

    # ── Catalog commands ─────────────────────────────────────────────

    ps = sub.add_parser("scan", help=t("help.scan"))
    ps.add_argument("--roots", nargs="+", required=True, help=t("help.scan.roots"))
    ps.add_argument("--catalog", default=None, help=t("help.scan.catalog"))
    ps.add_argument("--force-rehash", action="store_true", help=t("help.scan.force_rehash"))
    ps.add_argument("--min-size-kb", type=int, default=0, help="Min file size for hashing (KB)")
    ps.add_argument("--no-media", action="store_true", help="Skip media metadata extraction (ffprobe/EXIF)")
    ps.add_argument("--no-phash", action="store_true", help="Skip perceptual hash computation")
    ps.add_argument("--exiftool", action="store_true", help=t("help.scan.exiftool"))
    ps.add_argument("--exiftool-bin", default="exiftool", help="ExifTool binary path")
    ps.add_argument("--workers", type=int, default=1, help=t("help.scan.workers"))
    ps.set_defaults(func=cmd_scan)

    pq = sub.add_parser("query", help=t("help.query"))
    pq.add_argument("--catalog", default=None, help="Catalog DB path")
    pq.add_argument("--ext", default=None, help="Filter by extension (e.g. jpg)")
    pq.add_argument("--date-from", default=None, help="Filter by date (YYYY-MM-DD)")
    pq.add_argument("--date-to", default=None, help="Filter by date (YYYY-MM-DD)")
    pq.add_argument("--min-size", type=int, default=None, help="Min file size (KB)")
    pq.add_argument("--max-size", type=int, default=None, help="Max file size (KB)")
    pq.add_argument("--path-contains", default=None, help="Path substring filter")
    pq.add_argument("--duplicates", action="store_true", help="List duplicate groups")
    pq.add_argument("--camera", default=None, help="Filter by camera make/model substring")
    pq.add_argument("--duration-min", type=float, default=None, help="Min duration in seconds")
    pq.add_argument("--duration-max", type=float, default=None, help="Max duration in seconds")
    pq.add_argument("--resolution-min", type=int, default=None, help="Min width in pixels")
    pq.add_argument("--no-gps", action="store_true", help="Only files without GPS data")
    pq.add_argument("--limit", type=int, default=10000, help="Max results")
    pq.set_defaults(func=cmd_query)

    pst = sub.add_parser("stats", help=t("help.stats"))
    pst.add_argument("--catalog", default=None, help="Catalog DB path")
    pst.set_defaults(func=cmd_stats)

    pvac = sub.add_parser("vacuum", help="Compact and defragment catalog database")
    pvac.add_argument("--catalog", default=None, help="Catalog DB path")
    pvac.set_defaults(func=cmd_vacuum)

    pci = sub.add_parser("catalog-import", help="Import audit inventory TSV into catalog")
    pci.add_argument("--inventory", required=True, help="Path to file_inventory.tsv")
    pci.add_argument("--catalog", default=None, help="Catalog DB path")
    pci.set_defaults(func=cmd_catalog_import)

    pce = sub.add_parser("catalog-export", help="Export catalog to inventory TSV")
    pce.add_argument("--out", required=True, help="Output TSV path")
    pce.add_argument("--catalog", default=None, help="Catalog DB path")
    pce.set_defaults(func=cmd_catalog_export)

    psim = sub.add_parser("similar", help=t("help.similar"))
    psim.add_argument("--catalog", default=None, help="Catalog DB path")
    psim.add_argument("--threshold", type=int, default=10, help="Max Hamming distance (0=identical, lower=stricter)")
    psim.add_argument("--out", default=None, help="Optional output TSV path")
    psim.set_defaults(func=cmd_similar)

    # ── Metadata precision commands ──────────────────────────────────

    pme = sub.add_parser("metadata-extract", help=t("help.extract"))
    pme.add_argument("--catalog", default=None, help="Catalog DB path")
    pme.add_argument("--exiftool-bin", default="exiftool", help="ExifTool binary path")
    pme.add_argument("--force", action="store_true", help="Re-extract metadata for all files")
    pme.set_defaults(func=cmd_metadata_extract)

    pmd = sub.add_parser("metadata-diff", help=t("help.diff"))
    pmd.add_argument("--catalog", default=None, help="Catalog DB path")
    pmd.add_argument("--group", default=None, help="Specific duplicate group ID to analyze")
    pmd.add_argument("--out", default=None, help="Output JSON report path")
    pmd.set_defaults(func=cmd_metadata_diff)

    pmm = sub.add_parser("metadata-merge", help=t("help.merge"))
    pmm.add_argument("--catalog", default=None, help="Catalog DB path")
    pmm.add_argument("--group", default=None, help="Specific duplicate group ID to merge")
    pmm.add_argument("--out-dir", default=None, help="Directory for merge plan TSV files")
    pmm.add_argument("--exiftool-bin", default="exiftool", help="ExifTool binary path")
    pmm.add_argument("--apply", action="store_true", help="Execute the merge (without this, only plans are generated)")
    pmm.add_argument("--dry-run", action="store_true", help="Simulate merge execution")
    pmm.set_defaults(func=cmd_metadata_merge)

    pmw = sub.add_parser("metadata-write", help="Write metadata tags to files via ExifTool")
    pmw.add_argument("files", nargs="+", help="Files to write tags to")
    pmw.add_argument("--tag", dest="tags", action="append", required=True, help="Tag to write (format: TAG=VALUE, repeatable)")
    pmw.add_argument("--exiftool-bin", default="exiftool", help="ExifTool binary path")
    pmw.add_argument("--overwrite-original", action="store_true", help="Overwrite in place (no _original backup)")
    pmw.set_defaults(func=cmd_metadata_write)

    pv = sub.add_parser("verify", help="Verify catalog integrity against filesystem")
    pv.add_argument("--catalog", default=None, help="Catalog DB path")
    pv.add_argument("--check-hashes", action="store_true", help="Recompute SHA-256 hashes (slow)")
    pv.add_argument("--limit", type=int, default=0, help="Max files to check (0 = all)")
    pv.set_defaults(func=cmd_verify)

    # ── Legacy commands ──────────────────────────────────────────────

    pa = sub.add_parser("audit", help="Scan roots, detect duplicates, create safe plan")
    pa.add_argument("--roots", nargs="+", required=True, help="Root directories to scan")
    pa.add_argument("--out-dir", default=".", help="Where audit run folder is created")
    pa.add_argument("--run-name", default=None, help="Optional run directory name")
    pa.add_argument("--min-size-kb", type=int, default=500, help="Min file size for duplicate hashing")
    pa.add_argument("--large-file-threshold-mb", type=int, default=500, help="Large-file report threshold")
    pa.add_argument("--allow-asset-component-dedupe", action="store_true", help="Allow dedupe of Live/RAW components")
    pa.add_argument("--no-prefer-earliest-origin", action="store_true", help="Disable preference for earliest source time")
    pa.add_argument("--no-prefer-richer-metadata", action="store_true", help="Disable preference for richer metadata")
    pa.add_argument("--prefer-root", action="append", default=[], help="Preferred keep-root in priority order")
    pa.set_defaults(func=cmd_audit)

    pp = sub.add_parser("plan", help="Rebuild plan from existing audit data")
    pp.add_argument("--run-dir", required=True, help="Audit run directory")
    pp.add_argument("--allow-asset-component-dedupe", action="store_true", help="Allow dedupe of Live/RAW components")
    pp.add_argument("--no-prefer-earliest-origin", action="store_true", help="Disable preference for earliest source time")
    pp.add_argument("--no-prefer-richer-metadata", action="store_true", help="Disable preference for richer metadata")
    pp.add_argument("--prefer-root", action="append", default=[], help="Preferred keep-root in priority order")
    pp.set_defaults(func=cmd_plan)

    pap = sub.add_parser("apply", help="Apply quarantine move plan")
    pap.add_argument("--plan", required=True, help="Path to plan_quarantine.tsv")
    pap.add_argument("--quarantine-root", default=None, help="Target quarantine root")
    pap.add_argument("--dry-run", action="store_true", help="Do not move files, only simulate")
    pap.set_defaults(func=cmd_apply)

    pr = sub.add_parser("restore", help="Restore moved files from executed log")
    pr.add_argument("--log", required=True, help="Path to executed_moves.tsv")
    pr.add_argument("--last", type=int, default=None, help="Restore only the last N moves")
    pr.add_argument("--file", nargs="*", default=None, help="Restore specific files by original path")
    pr.add_argument("--dry-run", action="store_true", help="Do not move files, only simulate")
    pr.set_defaults(func=cmd_restore)

    ppro = sub.add_parser("promote", help="Promote richer copy from quarantine to primary")
    ppro.add_argument("--manifest", required=True, help="TSV with: size,moved_from,quarantine_path,primary_path")
    ppro.add_argument("--backup-root", default=None, help="Backup location for current primary copies")
    ppro.add_argument("--dry-run", action="store_true", help="Do not move files, only simulate")
    ppro.set_defaults(func=cmd_promote)

    ptp = sub.add_parser("tree-plan", help="Create tree restructuring plan (time/type/modified/people/place)")
    ptp.add_argument("--roots", nargs="+", required=True, help="Source roots for files")
    ptp.add_argument("--target-root", required=True, help="Target root for new tree structure")
    ptp.add_argument("--mode", required=True, choices=["time", "type", "modified", "people", "place"], help="Tree mode")
    ptp.add_argument("--granularity", choices=["year", "month", "day"], default="day", help="Date granularity for time modes")
    ptp.add_argument("--labels-tsv", default=None, help="Optional TSV labels with columns: path,people,place")
    ptp.add_argument("--unknown-label", default="Unknown", help="Fallback label for people/place without metadata")
    ptp.add_argument("--allow-asset-set-split", action="store_true", help="Allow splitting Live/RAW multi-file asset sets")
    ptp.add_argument("--out-dir", default=".", help="Directory where tree plan run folder is created")
    ptp.add_argument("--run-name", default=None, help="Optional run folder name")
    ptp.set_defaults(func=cmd_tree_plan)

    pta = sub.add_parser("tree-apply", help="Apply tree restructuring plan")
    pta.add_argument("--plan", required=True, help="Path to tree_plan.tsv")
    pta.add_argument("--operation", choices=["move", "copy", "hardlink", "symlink"], default="move", help="Apply operation")
    pta.add_argument("--collision-policy", choices=["skip", "rename", "overwrite"], default="rename", help="Collision policy")
    pta.add_argument("--log", default=None, help="Optional output log path")
    pta.add_argument("--dry-run", action="store_true", help="Do not modify filesystem, only simulate")
    pta.set_defaults(func=cmd_tree_apply)

    plt = sub.add_parser("labels-template", help="Generate labels TSV template for people/place tree modes")
    plt.add_argument("--roots", nargs="+", required=True, help="Source roots for file discovery")
    plt.add_argument("--out", required=True, help="Output TSV path")
    plt.add_argument("--include-all", action="store_true", help="Include all files, not only media-like extensions")
    plt.set_defaults(func=cmd_labels_template)

    papl = sub.add_parser("auto-place", help="Auto-fill place labels from EXIF/QuickTime GPS metadata")
    papl.add_argument("--roots", nargs="+", required=True, help="Source roots for file discovery")
    papl.add_argument("--labels-out", required=True, help="Output TSV path with merged labels")
    papl.add_argument("--labels-in", default=None, help="Optional existing labels TSV to merge into")
    papl.add_argument("--report-dir", default=None, help="Directory for auto-place reports")
    papl.add_argument("--exiftool-bin", default="exiftool", help="ExifTool binary path")
    papl.add_argument("--reverse-geocode", action="store_true", help="Resolve GPS to city/country labels via Nominatim")
    papl.add_argument(
        "--gdpr-consent", action="store_true",
        help="Acknowledge GDPR implications of sending GPS to external API",
    )
    papl.add_argument("--geocode-cache", default=None, help="Optional JSON cache path for reverse geocoding")
    papl.add_argument("--geocode-min-delay-seconds", type=float, default=1.1, help="Rate-limit delay for reverse geocode API calls")
    papl.add_argument("--overwrite-place", action="store_true", help="Overwrite existing place labels")
    papl.set_defaults(func=cmd_auto_place)

    pape = sub.add_parser("auto-people", help="Auto-fill people labels using face detection and clustering")
    pape.add_argument("--roots", nargs="+", required=True, help="Source roots for image discovery")
    pape.add_argument("--labels-out", required=True, help="Output TSV path with merged labels")
    pape.add_argument("--labels-in", default=None, help="Optional existing labels TSV to merge into")
    pape.add_argument("--report-dir", default=None, help="Directory for auto-people reports")
    pape.add_argument("--model", choices=["hog", "cnn"], default="hog", help="Face detector model (hog is CPU-friendly)")
    pape.add_argument("--max-dimension", type=int, default=1600, help="Resize long edge before detection to speed up processing")
    pape.add_argument("--eps", type=float, default=0.5, help="DBSCAN distance threshold for same-person clustering")
    pape.add_argument("--min-samples", type=int, default=2, help="DBSCAN min samples to form a person cluster")
    pape.add_argument("--person-prefix", default="Person", help="Prefix for generated person IDs")
    pape.add_argument("--overwrite-people", action="store_true", help="Overwrite existing people labels")
    pape.set_defaults(func=cmd_auto_people)

    ppr = sub.add_parser("prune-recommend", help="Generate conservative deletion recommendations")
    ppr.add_argument("--roots", nargs="+", required=True, help="Roots to scan")
    ppr.add_argument("--out-dir", default=".", help="Where recommendation run folder is created")
    ppr.add_argument("--run-name", default=None, help="Optional run folder name")
    ppr.add_argument("--min-size-kb", type=int, default=500, help="Min file size for duplicate hash recommendation")
    ppr.add_argument("--allow-asset-component-dedupe", action="store_true", help="Allow duplicate recommendation for asset components")
    ppr.add_argument("--no-prefer-earliest-origin", action="store_true", help="Disable preference for earliest source time")
    ppr.add_argument("--no-prefer-richer-metadata", action="store_true", help="Disable preference for richer metadata")
    ppr.add_argument("--prefer-root", action="append", default=[], help="Preferred keep-root in priority order")
    ppr.add_argument("--no-system-noise", action="store_true", help="Disable system noise file recommendations")
    ppr.set_defaults(func=cmd_prune_recommend)

    pdp = sub.add_parser("delete-plan", help="Create safe deletion plan with hardlink and asset-set expansion")
    pdp.add_argument("--roots", nargs="+", required=True, help="Roots where links/files should be considered")
    pdp.add_argument("--out", required=True, help="Output delete plan TSV")
    pdp.add_argument("--summary-out", default=None, help="Optional summary JSON output path")
    pdp.add_argument("--select-paths", default=None, help="Text file with one selected path per line")
    pdp.add_argument("--recommendations", default=None, help="Recommendations TSV from prune-recommend")
    pdp.add_argument("--prefer-root", action="append", default=[], help="Preferred path roots for primary quarantine copy")
    pdp.add_argument("--no-asset-set-expansion", action="store_true", help="Do not expand selection to sibling asset components")
    pdp.add_argument("--allow-external-links", action="store_true", help="Allow plans even if inode has links outside scanned roots")
    pdp.set_defaults(func=cmd_delete_plan)

    pda = sub.add_parser("delete-apply", help="Apply delete plan: quarantine one primary link and unlink aliases")
    pda.add_argument("--plan", required=True, help="Path to delete plan TSV")
    pda.add_argument("--quarantine-root", required=True, help="Where primary copies are moved")
    pda.add_argument("--log", default=None, help="Optional execution log path")
    pda.add_argument("--dry-run", action="store_true", help="Simulate only")
    pda.set_defaults(func=cmd_delete_apply)

    pexp = sub.add_parser("export", help="Export catalog data to CSV/TSV")
    pexp.add_argument("what", choices=["files", "duplicates"], help="What to export")
    pexp.add_argument("--out", required=True, help="Output file path")
    pexp.add_argument("--format", choices=["csv", "tsv"], default="csv", help="Output format (default: csv)")
    pexp.add_argument("--catalog", default=None, help="Catalog DB path")
    pexp.add_argument("--limit", type=int, default=100000, help="Max rows")
    pexp.set_defaults(func=cmd_export)

    pbr = sub.add_parser("batch-rename", help="Bulk rename files by pattern")
    pbr.add_argument("root", help="Directory containing files to rename")
    pbr.add_argument("--pattern", required=True, help="Rename pattern (e.g. '{date}_{n:03d}')")
    pbr.add_argument("--ext", default=None, help="Filter by extension (e.g. jpg)")
    pbr.add_argument("--start", type=int, default=1, help="Starting number for {n}")
    pbr.add_argument("--dry-run", action="store_true", help="Preview without renaming")
    pbr.set_defaults(func=cmd_batch_rename)

    return parser


def main() -> int:
    from .i18n import set_lang

    parser = build_parser()
    args = parser.parse_args()
    if args.lang:
        set_lang(args.lang)
    log_file = Path(args.log_file) if args.log_file else None
    setup_logging(verbosity=args.verbose, log_file=log_file)
    logger.debug("command=%s args=%s", args.command, vars(args))
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
