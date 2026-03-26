# Changelog

## [1.0.0] — 2026-03-26

First production release.

### Security
- Timing-safe token comparison (`hmac.compare_digest`) in web auth
- WebSocket auth enforcement (was previously bypassed)
- Path traversal protection in web preview endpoint and quarantine builder
- Stack traces no longer leaked to HTTP clients
- Delete confirmation prompt before destructive operations (`-y/--yes` to skip)

### Stability
- SQLite connection cleanup on `catalog.open()` failure
- Atomic schema migrations with SAVEPOINT rollback
- Bounded collision-safe retry loop (max 10k attempts)
- Top-level CLI exception handler (no raw tracebacks)
- rclone mock tests fixed (cache reset + `_rclone_bin` mock)

### Infrastructure
- CI: added `ruff format --check`, pip caching, coverage report artifact upload
- Log rotation: `RotatingFileHandler` (10 MB, 3 backups) replaces plain `FileHandler`
- CLI: `--log-max-size` and `--log-backups` parameters
- Version bump to 1.0.0

### Documentation
- Error recovery & resume section in README (audit, apply, cloud consolidation)
- Logging configuration examples

### Tests
- 528 passed, 0 failed, 2 skipped (3× stable)
- Schema version assertion updated (v9 → v10)
- Cloud test cache isolation fixed

## [0.1.0] — pre-release

### Features
- 40+ CLI subcommands for media library management
- SHA-256 content hashing with Live Photo / RAW component protection
- Duplicate detection with perceptual hashing (pHash)
- Face recognition + DBSCAN clustering with Fernet encryption
- 10-phase cloud consolidation pipeline with checkpoint/resume
- Multi-cloud support via rclone (Google Drive, MEGA, pCloud, etc.)
- Tree restructuring modes: time, modified, type, people, place
- Auto-labeling: GPS reverse geocode + face clustering
- Web UI with FastAPI + WebSocket live progress
- macOS .pkg/.dmg installer
- Docker containerization
- Google Workspace Shared Drive + E2E encryption (rclone crypt)
- Backup health monitoring
