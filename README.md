# GOD MODE Media Library

Cross-platform CLI app for safe media organization with metadata-first rules.

## Core principles

- No destructive deletion in normal flow.
- Exact duplicate checks use SHA-256 content hash.
- Live Photo / RAW component protection is enabled by default.
- Metadata merge is avoided by default to prevent incompatible hybrids.
- Prefer earliest-origin source and richer metadata when selecting primary copy.

## Install

```bash
cd godmode_media_library
python -m pip install -e .
```

Optional extras:

```bash
python -m pip install -e ".[geo]"
python -m pip install -e ".[people]"
python -m pip install -e ".[full]"
```

For broad EXIF/RAW/QuickTime GPS support, install ExifTool:

```bash
brew install exiftool
```

After install, CLI command is:

```bash
gml --help
```

## Typical workflow

1) Create an audit and plan

```bash
gml audit \
  --roots "/path/to/Pictures" "/path/to/Downloads" "/path/to/Desktop" \
  --out-dir "/path/to/runs" \
  --prefer-root "/path/to/Pictures" \
  --prefer-root "/path/to/Documents"
```

Example for external drive:

```bash
gml audit \
  --roots "/Volumes/ExternalArchive" \
  --out-dir "/Volumes/ExternalArchive/_gml_runs" \
  --prefer-root "/Volumes/ExternalArchive/Master"
```

2) Review generated files in the run folder

- `summary.json`
- `file_inventory.tsv`
- `exact_duplicates.tsv`
- `duplicate_groups_summary.tsv`
- `asset_sets.tsv`
- `plan_quarantine.tsv`
- `manual_review.tsv`

3) Apply plan to quarantine (safe move)

```bash
gml apply \
  --plan "/path/to/runs/audit_YYYYMMDD_HHMMSS/plan_quarantine.tsv" \
  --quarantine-root "/path/to/quarantine"
```

4) Restore if needed

```bash
gml restore --log "/path/to/runs/audit_YYYYMMDD_HHMMSS/executed_moves.tsv"
```

## Tree restructuring (debordelization views)

Create plan (safe, no changes yet):

```bash
gml tree-plan \
  --roots "/path/to/Master" \
  --target-root "/path/to/LibraryViews" \
  --mode time \
  --granularity day \
  --out-dir "/path/to/runs"
```

Supported `--mode` values:

- `time` (origin-oriented date tree)
- `modified` (change date tree)
- `type` (category/extension tree)
- `people` (label-driven)
- `place` (label-driven)

In `type` mode, protected multi-file assets (for example Live Photo pairs) are grouped under
`by_type/asset_sets/...` to keep components together.

Apply plan:

```bash
gml tree-apply \
  --plan "/path/to/runs/tree_time_YYYYMMDD_HHMMSS/tree_plan.tsv" \
  --operation move \
  --collision-policy rename
```

For `people/place` modes, use optional labels TSV with columns:

- `path`
- `people`
- `place`

You can generate the template automatically:

```bash
gml labels-template \
  --roots "/path/to/Master" \
  --out "/path/to/labels.tsv"
```

Example:

```bash
gml tree-plan \
  --roots "/path/to/Master" \
  --target-root "/path/to/LibraryViews" \
  --mode people \
  --labels-tsv "/path/to/labels.tsv" \
  --out-dir "/path/to/runs"
```

To keep source files untouched, you can create browseable view trees using hardlinks:

```bash
gml tree-apply \
  --plan "/path/to/runs/tree_time_YYYYMMDD_HHMMSS/tree_plan.tsv" \
  --operation hardlink \
  --collision-policy rename
```

This works for all modes, including `people` and `place`.

## Auto labels (GPS place + people)

`auto-place` reads GPS metadata from files (ExifTool), generates place labels, and merges them into `labels.tsv`.
It does not modify media files.
When `--reverse-geocode` is enabled, GPS coordinates are sent to a geocoding service (Nominatim), so use it only when this is acceptable for your privacy policy.

```bash
gml auto-place \
  --roots "/path/to/Master" \
  --labels-in "/path/to/labels.tsv" \
  --labels-out "/path/to/labels.tsv" \
  --report-dir "/path/to/runs/auto_place"
```

With optional reverse geocode (city/country):

```bash
gml auto-place \
  --roots "/path/to/Master" \
  --labels-out "/path/to/labels.tsv" \
  --reverse-geocode \
  --report-dir "/path/to/runs/auto_place"
```

`auto-people` uses optional face recognition + clustering, writes review reports, then merges people labels into `labels.tsv`.
It does not modify media files.
For HEIC support in people detection, install `pillow-heif` (included in `.[people]` and `.[full]` extras).

```bash
gml auto-people \
  --roots "/path/to/Master" \
  --labels-in "/path/to/labels.tsv" \
  --labels-out "/path/to/labels.tsv" \
  --report-dir "/path/to/runs/auto_people"
```

Tuning example:

```bash
gml auto-people \
  --roots "/path/to/Master" \
  --labels-out "/path/to/labels.tsv" \
  --model hog \
  --eps 0.5 \
  --min-samples 2 \
  --person-prefix Person \
  --report-dir "/path/to/runs/auto_people"
```

## Prune recommendations and hardlink-safe delete flow

Generate conservative recommendations (exact duplicates + optional noise files):

```bash
gml prune-recommend \
  --roots "/path/to/Master" \
  --out-dir "/path/to/runs" \
  --prefer-root "/path/to/Master"
```

Output files:

- `prune_recommendations.tsv`
- `recommended_paths.txt`
- `prune_summary.json`

Build deletion plan that expands:

- all hardlink aliases
- all sibling asset components (Live Photo/RAW sidecars)

```bash
gml delete-plan \
  --roots "/path/to/Master" "/path/to/LibraryViews" \
  --recommendations "/path/to/runs/prune_recommend_YYYYMMDD_HHMMSS/prune_recommendations.tsv" \
  --out "/path/to/runs/prune_recommend_YYYYMMDD_HHMMSS/delete_plan.tsv" \
  --prefer-root "/path/to/Master"
```

Apply safely (move one primary link to quarantine, unlink aliases):

```bash
gml delete-apply \
  --plan "/path/to/runs/prune_recommend_YYYYMMDD_HHMMSS/delete_plan.tsv" \
  --quarantine-root "/path/to/quarantine/delete_run_01"
```

Important:

- If you only delete inside a hardlink view manually, source data may still exist elsewhere.
- `delete-plan` + `delete-apply` handles this by deleting all known links in scanned roots.
- Space is actually freed only when quarantined data is permanently removed.

## Promote richer metadata copy (optional)

Use when you intentionally want to promote a quarantined copy as primary.

Manifest must be TSV with columns:

- `size`
- `moved_from`
- `quarantine_path`
- `primary_path`

Then run:

```bash
gml promote --manifest "/path/to/manifest.tsv" --backup-root "/path/to/backup"
```

## Multi-platform note (Windows/macOS/iOS/Android ecosystem)

Recommended architecture:

- Master archive: maximum data fidelity, no destructive transforms.
- Compatibility exports: secondary derivatives for broad platform viewing.

If there is a conflict, keep master archive fidelity first.

## GitHub publishing

```bash
cd godmode_media_library
./scripts/publish_to_github.sh <github_repo_url>
```

This initializes git (if needed), commits current state, sets `origin`, and pushes `main`.

## Classic macOS installer (.pkg + .dmg)

Build installer artifacts:

```bash
cd godmode_media_library
./installer/macos/build_installer.sh
```

Generated files:

- `dist/GodModeMediaLibrary-<version>-macos.pkg`
- `dist/GodModeMediaLibrary-<version>-macos.dmg`

Install flow for end users:

1. Double-click the `.dmg`.
2. Double-click the `.pkg`.
3. Follow Installer steps (no terminal needed).

## Docker (run outside local Python environment)

Build:

```bash
docker build -t gml:latest .
```

Run:

```bash
docker run --rm \
  -v "/path/to/data:/data" \
  -v "/path/to/runs:/runs" \
  gml:latest audit --roots /data --out-dir /runs
```
