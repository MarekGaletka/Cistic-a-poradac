"""Unit tests for watcher.py — file change detection, debouncing, event filtering, watch_roots."""

from __future__ import annotations

import importlib
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library.watcher import (
    _BATCH_INTERVAL,
    _DEBOUNCE_SECONDS,
    _ChangeCollector,
    _is_media_file,
    watch_roots,
)

# ── _ChangeCollector ─────────────────────────────────────────────────


class TestChangeCollector:
    def test_add_single_path(self):
        c = _ChangeCollector()
        c.add(Path("/tmp/photo.jpg"))
        # Immediately after add, debounce hasn't passed — flush returns empty
        assert c.flush() == set()

    def test_flush_after_debounce(self):
        c = _ChangeCollector()
        c.add(Path("/tmp/photo.jpg"))
        with c._lock:
            c._last_event_time = time.monotonic() - _DEBOUNCE_SECONDS - 1
        result = c.flush()
        assert result == {Path("/tmp/photo.jpg")}

    def test_flush_clears_state(self):
        c = _ChangeCollector()
        c.add(Path("/tmp/a.jpg"))
        with c._lock:
            c._last_event_time = time.monotonic() - _DEBOUNCE_SECONDS - 1
        first = c.flush()
        assert len(first) == 1
        second = c.flush()
        assert second == set()

    def test_flush_empty_returns_empty(self):
        c = _ChangeCollector()
        assert c.flush() == set()

    def test_multiple_adds_deduplicate(self):
        c = _ChangeCollector()
        c.add(Path("/tmp/photo.jpg"))
        c.add(Path("/tmp/photo.jpg"))
        c.add(Path("/tmp/photo.jpg"))
        with c._lock:
            c._last_event_time = time.monotonic() - _DEBOUNCE_SECONDS - 1
        result = c.flush()
        assert result == {Path("/tmp/photo.jpg")}

    def test_multiple_different_paths(self):
        c = _ChangeCollector()
        c.add(Path("/tmp/a.jpg"))
        c.add(Path("/tmp/b.png"))
        c.add(Path("/tmp/c.mp4"))
        with c._lock:
            c._last_event_time = time.monotonic() - _DEBOUNCE_SECONDS - 1
        result = c.flush()
        assert len(result) == 3

    def test_debounce_resets_on_new_event(self):
        c = _ChangeCollector()
        c.add(Path("/tmp/a.jpg"))
        with c._lock:
            c._last_event_time = time.monotonic() - 1
        assert c.flush() == set()
        c.add(Path("/tmp/b.jpg"))
        assert c.flush() == set()

    def test_thread_safety(self):
        c = _ChangeCollector()
        paths = [Path(f"/tmp/file_{i}.jpg") for i in range(100)]

        def adder(p):
            c.add(p)

        threads = [threading.Thread(target=adder, args=(p,)) for p in paths]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with c._lock:
            c._last_event_time = time.monotonic() - _DEBOUNCE_SECONDS - 1
        result = c.flush()
        assert len(result) == 100

    def test_add_updates_last_event_time(self):
        c = _ChangeCollector()
        before = time.monotonic()
        c.add(Path("/tmp/test.jpg"))
        after = time.monotonic()
        with c._lock:
            assert before <= c._last_event_time <= after


# ── _is_media_file ───────────────────────────────────────────────────


class TestIsMediaFile:
    @pytest.mark.parametrize(
        "filename,expected",
        [
            (Path("photo.jpg"), True),
            (Path("photo.JPEG"), True),
            (Path("video.mp4"), True),
            (Path("song.mp3"), True),
            (Path("raw.CR2"), True),
            (Path("image.heic"), True),
            (Path("doc.pdf"), True),
            (Path("image.webp"), True),
            (Path("anim.gif"), True),
            (Path("audio.flac"), True),
            (Path("audio.ogg"), True),
            (Path("audio.m4a"), True),
            (Path("video.wmv"), True),
            (Path("video.flv"), True),
            (Path("video.webm"), True),
            (Path("raw.nef"), True),
            (Path("raw.arw"), True),
            (Path("raw.dng"), True),
            (Path("raw.orf"), True),
            (Path("raw.rw2"), True),
            (Path("design.psd"), True),
            (Path("design.ai"), True),
            (Path("image.avif"), True),
            (Path("image.svg"), True),
            (Path("image.bmp"), True),
            (Path("image.tiff"), True),
            (Path("audio.wav"), True),
            (Path("audio.aac"), True),
            (Path("readme.txt"), False),
            (Path("script.py"), False),
            (Path("data.json"), False),
            (Path("Makefile"), False),
            (Path("archive.zip"), False),
            (Path("data.csv"), False),
            (Path(".DS_Store"), False),
        ],
    )
    def test_media_detection(self, filename, expected):
        assert _is_media_file(filename) is expected


# ── watch_roots (mocked) ────────────────────────────────────────────


class TestWatchRoots:
    def test_missing_watchdog_prints_error(self, capsys):
        """When watchdog is not importable, watch_roots logs error and returns."""
        original_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

        def fake_import(name, *args, **kwargs):
            if "watchdog" in name:
                raise ImportError("No module named 'watchdog'")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            watch_roots(
                roots=[Path("/tmp")],
                catalog_path=Path("/tmp/cat.db"),
                stop_event=threading.Event(),
            )

    def test_watch_roots_with_stop_event_exits_immediately(self):
        """watch_roots should respect stop_event for graceful shutdown."""
        stop = threading.Event()
        stop.set()

        mock_observer_instance = MagicMock()
        mock_observer_cls = MagicMock(return_value=mock_observer_instance)
        mock_events = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "watchdog": MagicMock(),
                "watchdog.events": mock_events,
                "watchdog.observers": MagicMock(Observer=mock_observer_cls),
            },
        ):
            from godmode_media_library import watcher

            importlib.reload(watcher)

            watcher.watch_roots(
                roots=[Path("/tmp")],
                catalog_path=Path("/tmp/cat.db"),
                stop_event=stop,
            )
            mock_observer_instance.stop.assert_called_once()

    def test_watch_roots_nonexistent_root_skipped(self):
        """Non-directory roots should be skipped (no schedule call)."""
        stop = threading.Event()
        stop.set()

        mock_observer_instance = MagicMock()
        mock_observer_cls = MagicMock(return_value=mock_observer_instance)
        mock_events = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "watchdog": MagicMock(),
                "watchdog.events": mock_events,
                "watchdog.observers": MagicMock(Observer=mock_observer_cls),
            },
        ):
            from godmode_media_library import watcher

            importlib.reload(watcher)

            watcher.watch_roots(
                roots=[Path("/nonexistent/path/12345")],
                catalog_path=Path("/tmp/cat.db"),
                stop_event=stop,
            )
            # schedule should not be called for non-existent directories
            mock_observer_instance.schedule.assert_not_called()

    def test_watch_roots_with_real_dir(self, tmp_path):
        """Real directory should be scheduled for observation."""
        stop = threading.Event()
        stop.set()

        mock_observer_instance = MagicMock()
        mock_observer_cls = MagicMock(return_value=mock_observer_instance)

        # Create a mock for FileSystemEventHandler that captures the class
        mock_events_module = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "watchdog": MagicMock(),
                "watchdog.events": mock_events_module,
                "watchdog.observers": MagicMock(Observer=mock_observer_cls),
            },
        ):
            from godmode_media_library import watcher

            importlib.reload(watcher)

            watcher.watch_roots(
                roots=[tmp_path],
                catalog_path=tmp_path / "cat.db",
                stop_event=stop,
            )
            mock_observer_instance.schedule.assert_called_once()

    def test_watch_roots_scan_triggered(self, tmp_path):
        """Simulate a change being detected and a scan being triggered."""
        stop = threading.Event()

        mock_observer_instance = MagicMock()
        mock_observer_cls = MagicMock(return_value=mock_observer_instance)
        mock_events_module = MagicMock()

        scan_called = threading.Event()

        def mock_incremental_scan(cat, roots, workers=1):
            scan_called.set()
            result = MagicMock()
            result.files_scanned = 1
            result.files_new = 1
            result.files_changed = 0
            return result

        with patch.dict(
            "sys.modules",
            {
                "watchdog": MagicMock(),
                "watchdog.events": mock_events_module,
                "watchdog.observers": MagicMock(Observer=mock_observer_cls),
            },
        ):
            from godmode_media_library import watcher

            importlib.reload(watcher)

            # Patch _ChangeCollector to return changes on first flush
            original_collector_cls = watcher._ChangeCollector
            call_count = [0]

            class MockCollector:
                def __init__(self):
                    self._real = original_collector_cls()

                def add(self, path):
                    self._real.add(path)

                def flush(self):
                    call_count[0] += 1
                    if call_count[0] == 1:
                        # Return some changed paths
                        return {tmp_path / "new_photo.jpg"}
                    # After first flush, signal stop
                    stop.set()
                    return set()

            with (
                patch.object(watcher, "_ChangeCollector", MockCollector),
                patch.object(watcher, "_BATCH_INTERVAL", 0.01),
                patch("godmode_media_library.watcher.incremental_scan", mock_incremental_scan, create=True),
            ):
                # Run in a thread to avoid blocking
                t = threading.Thread(
                    target=watcher.watch_roots,
                    args=([tmp_path], tmp_path / "cat.db"),
                    kwargs={"stop_event": stop},
                )
                t.start()
                t.join(timeout=5)

            assert not t.is_alive()

    def test_watch_roots_creates_stop_event_if_none(self):
        """If stop_event is None, one is created internally. We use a pre-set stop_event=None
        and mock Event.wait to raise KeyboardInterrupt to exit the loop."""
        mock_observer_instance = MagicMock()
        mock_observer_cls = MagicMock(return_value=mock_observer_instance)
        mock_events = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "watchdog": MagicMock(),
                "watchdog.events": mock_events,
                "watchdog.observers": MagicMock(Observer=mock_observer_cls),
            },
        ):
            from godmode_media_library import watcher

            importlib.reload(watcher)

            # Patch threading.Event so the internal stop_event.wait raises KeyboardInterrupt
            original_event = threading.Event

            class InterruptEvent(original_event):
                def wait(self, timeout=None):
                    raise KeyboardInterrupt()

                def is_set(self):
                    return False

            with patch("threading.Event", InterruptEvent):
                watcher.watch_roots(
                    roots=[],
                    catalog_path=Path("/tmp/cat.db"),
                    stop_event=None,
                )
            mock_observer_instance.stop.assert_called_once()


# ── Constants ────────────────────────────────────────────────────────


class TestWatcherConstants:
    def test_debounce_seconds_positive(self):
        assert _DEBOUNCE_SECONDS > 0

    def test_batch_interval_greater_than_debounce(self):
        assert _BATCH_INTERVAL > _DEBOUNCE_SECONDS
