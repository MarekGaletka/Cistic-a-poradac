from __future__ import annotations

import argparse
import json
from pathlib import Path

from .actions import apply_plan, promote_from_manifest, restore_from_log
from .autolabel_people import auto_people_labels
from .autolabel_place import auto_place_labels
from .audit import collect_file_records, load_exact_duplicates, load_inventory, write_audit_run
from .delete_ops import apply_delete_plan, create_delete_plan
from .models import PlanPolicy
from .planning import create_plan, write_plan_files
from .prune_recommend import recommend_prune
from .tree_ops import apply_tree_plan, create_tree_plan, write_tree_plan
from .utils import ensure_dir, utc_stamp


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
    if args.run_name:
        plan_dir = run_dir / args.run_name
    else:
        plan_dir = run_dir / f"tree_{args.mode}_{utc_stamp()}"
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
    if args.run_name:
        run_dir = out_dir / args.run_name
    else:
        run_dir = out_dir / f"prune_recommend_{utc_stamp()}"
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gml",
        description="GOD MODE media organizer with metadata-first safety",
    )

    sub = parser.add_subparsers(dest="command", required=True)

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
    pta.add_argument("--collision-policy", choices=["skip", "rename", "overwrite"], default="rename", help="What to do on destination collision")
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

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
