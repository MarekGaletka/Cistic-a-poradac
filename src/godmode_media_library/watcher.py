"""Filesystem watcher for automatic duplicate detection.

Uses watchdog library for cross-platform FSEvents/inotify support.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_DEBOUNCE_SECONDS = 5.0
_BATCH_INTERVAL = 30.0


class _ChangeCollector:
    """Collects filesystem changes and batches them for processing."""

    def __init__(self):
        self._lock = threading.Lock()
        self._changed_paths: set[Path] = set()
        self._last_event_time: float = 0

    def add(self, path: Path) -> None:
        with self._lock:
            self._changed_paths.add(path)
            self._last_event_time = time.monotonic()

    def flush(self) -> set[Path]:
        """Return collected paths if debounce period has passed."""
        with self._lock:
            if not self._changed_paths:
                return set()
            if time.monotonic() - self._last_event_time < _DEBOUNCE_SECONDS:
                return set()
            paths = self._changed_paths.copy()
            self._changed_paths.clear()
            return paths


def _is_media_file(path: Path) -> bool:
    """Check if path looks like a media file."""
    media_exts = {
        ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif",
        ".webp", ".heic", ".heif", ".avif", ".svg",
        ".mp4", ".mov", ".avi", ".mkv", ".wmv", ".flv", ".webm",
        ".mp3", ".wav", ".aac", ".flac", ".ogg", ".m4a",
        ".cr2", ".nef", ".arw", ".dng", ".orf", ".rw2",
        ".pdf", ".psd", ".ai",
    }
    return path.suffix.lower() in media_exts


def watch_roots(
    roots: list[Path],
    catalog_path: Path,
    *,
    on_scan_complete=None,
    stop_event: threading.Event | None = None,
) -> None:
    """Watch roots for changes and auto-scan new/modified files.

    Args:
        roots: Directories to watch.
        catalog_path: Path to catalog DB.
        on_scan_complete: Optional callback(stats) after each scan.
        stop_event: Event to signal stop. If None, runs forever.
    """
    try:
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError:
        logger.error("watchdog not installed. Run: pip install watchdog")
        print("Error: watchdog library required. Install with: pip install 'godmode-media-library[watch]'")
        return

    collector = _ChangeCollector()
    if stop_event is None:
        stop_event = threading.Event()

    class Handler(FileSystemEventHandler):
        def on_created(self, event):
            if not event.is_directory:
                p = Path(event.src_path)
                if _is_media_file(p):
                    collector.add(p)

        def on_modified(self, event):
            if not event.is_directory:
                p = Path(event.src_path)
                if _is_media_file(p):
                    collector.add(p)

        def on_moved(self, event):
            if not event.is_directory:
                p = Path(event.dest_path)
                if _is_media_file(p):
                    collector.add(p)

    observer = Observer()
    handler = Handler()
    for root in roots:
        if root.is_dir():
            observer.schedule(handler, str(root), recursive=True)
            logger.info("Watching: %s", root)

    observer.start()
    logger.info("Watcher started for %d roots. Press Ctrl+C to stop.", len(roots))

    try:
        while not stop_event.is_set():
            stop_event.wait(timeout=_BATCH_INTERVAL)
            changed = collector.flush()
            if not changed:
                continue

            logger.info("Detected %d changed files, triggering scan...", len(changed))
            try:
                from .catalog import Catalog
                from .scanner import incremental_scan

                # Determine roots that have changes
                affected_roots = set()
                for p in changed:
                    for root in roots:
                        try:
                            p.relative_to(root)
                            affected_roots.add(root)
                            break
                        except ValueError:
                            continue

                cat = Catalog(catalog_path, exclusive=True)
                with cat:
                    stats = incremental_scan(cat, list(affected_roots), workers=1)

                logger.info(
                    "Auto-scan complete: scanned=%d new=%d changed=%d",
                    stats.files_scanned, stats.files_new, stats.files_changed,
                )
                if on_scan_complete:
                    on_scan_complete(stats)
            except Exception:
                logger.exception("Auto-scan failed")
    except KeyboardInterrupt:
        logger.info("Watch stopped by user")
    finally:
        observer.stop()
        observer.join()
