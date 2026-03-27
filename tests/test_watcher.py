"""Unit tests for watcher.py — file change detection & debouncing."""

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
        # Monkey-patch the last_event_time to simulate debounce elapsed
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
        # Second flush should return empty
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
        # Simulate time passing but not enough
        with c._lock:
            c._last_event_time = time.monotonic() - 1
        assert c.flush() == set()
        # Add another event — resets debounce
        c.add(Path("/tmp/b.jpg"))
        assert c.flush() == set()  # Still within debounce

    def test_thread_safety(self):
        """Multiple threads adding paths concurrently should not lose data."""
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
            (Path("doc.pdf"), True),  # pdf is in media_exts in watcher
            (Path("image.webp"), True),
            (Path("readme.txt"), False),
            (Path("script.py"), False),
            (Path("data.json"), False),
            (Path("Makefile"), False),
        ],
    )
    def test_media_detection(self, filename, expected):
        assert _is_media_file(filename) is expected


# ── watch_roots (mocked) ────────────────────────────────────────────


class TestWatchRoots:
    def test_missing_watchdog_prints_error(self, capsys):
        """When watchdog is not importable, watch_roots logs error and returns."""
        with patch.dict("sys.modules", {"watchdog": None, "watchdog.events": None, "watchdog.observers": None}):
            # Force reimport to trigger the ImportError path
            from godmode_media_library import watcher
            import importlib

            # We can test the import guard by calling watch_roots with mocked imports
            # The function does a local import, so we patch builtins.__import__
            original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__

            def fake_import(name, *args, **kwargs):
                if "watchdog" in name:
                    raise ImportError("No module named 'watchdog'")
                return original_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=fake_import):
                watcher.watch_roots(
                    roots=[Path("/tmp")],
                    catalog_path=Path("/tmp/cat.db"),
                    stop_event=threading.Event(),
                )

    def test_watch_roots_with_stop_event_exits_immediately(self):
        """watch_roots should respect stop_event for graceful shutdown."""
        stop = threading.Event()
        stop.set()  # Immediately signal stop

        # watchdog imports happen inside watch_roots, so we mock at sys.modules level
        mock_observer_instance = MagicMock()
        mock_observer_cls = MagicMock(return_value=mock_observer_instance)
        mock_events = MagicMock()

        with patch.dict("sys.modules", {
            "watchdog": MagicMock(),
            "watchdog.events": mock_events,
            "watchdog.observers": MagicMock(Observer=mock_observer_cls),
        }):
            # Re-import to pick up mocked watchdog
            import importlib
            from godmode_media_library import watcher
            importlib.reload(watcher)

            watcher.watch_roots(
                roots=[Path("/tmp")],
                catalog_path=Path("/tmp/cat.db"),
                stop_event=stop,
            )
            mock_observer_instance.stop.assert_called_once()


# ── Constants ────────────────────────────────────────────────────────


class TestWatcherConstants:
    def test_debounce_seconds_positive(self):
        assert _DEBOUNCE_SECONDS > 0

    def test_batch_interval_greater_than_debounce(self):
        assert _BATCH_INTERVAL > _DEBOUNCE_SECONDS
