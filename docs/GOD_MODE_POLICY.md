# GOD MODE Policy

## Priority order

1. Preserve full data fidelity (content + sidecars + platform metadata where possible).
2. Preserve temporal truth (earliest credible origin time).
3. Keep cross-platform readability as a secondary layer.

## Hard safety rules

- Never delete as first action.
- Never merge metadata from one file into another by default.
- Treat multi-file assets as atomic sets:
  - Apple Live Photo: image + video (+ AAE)
  - RAW workflows: RAW + XMP (+ optional sidecars)
- When uncertain, route to manual review.
- Auto label pipelines (`auto-place`, `auto-people`) write labels manifests only; they do not alter original media bytes.
- Hardlink view deletions should use plan/apply workflow that removes all aliases and keeps full asset sets intact.

## Storage model

- Master archive:
  - no lossy transforms
  - no metadata flattening
  - canonical source of truth
- Compatibility exports:
  - derived copies for broad platform access
  - may normalize formats for specific clients

## Cross-platform note

Different systems store metadata differently (macOS xattrs, Windows alternate streams,
vendor tags in EXIF/QuickTime, library DB metadata). A single perfect representation
for every platform may not exist. Therefore, master fidelity has priority.
