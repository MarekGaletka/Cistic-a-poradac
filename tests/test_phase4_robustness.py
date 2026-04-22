"""Regression tests for Phase 4 robustness fixes.

Each test verifies that a previously-identified bug remains fixed.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ── 1. metadata_merge: group prefix preserved ───────────────────────────
# Issue: EXIF: and XMP: prefixes were stripped, causing both tags to collide
# as bare -DateTimeOriginal=.


def test_group_prefix_not_stripped_regression():
    """EXIF: and XMP: group prefixes must survive into the exiftool command."""
    from godmode_media_library.metadata_merge import MergeAction, MergePlan, execute_merge

    plan = MergePlan(
        survivor_path="/survivor.jpg",
        survivor_hash="abc123",
        actions=[
            MergeAction(tag="EXIF:DateTimeOriginal", value="2024:01:01", source_path="/d.jpg", action_type="copy"),
            MergeAction(tag="XMP:DateTimeOriginal", value="2024-01-01", source_path="/d.jpg", action_type="copy"),
        ],
    )
    mock_proc = MagicMock(returncode=0)
    with (
        patch("godmode_media_library.metadata_merge.sha256_file", return_value="abc123"),
        patch("godmode_media_library.metadata_merge.shutil.which", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc) as mock_run,
        patch("pathlib.Path.exists", return_value=False),
    ):
        execute_merge(plan, dry_run=False)

    cmd = mock_run.call_args[0][0]
    # Both tags must retain their group prefix — they must not collide
    assert "-EXIF:DateTimeOriginal=2024:01:01" in cmd
    assert "-XMP:DateTimeOriginal=2024-01-01" in cmd


# ── 2. metadata_merge: list values not joined into single string ─────────
# Issue: list values were joined with ", " losing multi-value structure.


def test_list_values_written_as_separate_args_regression():
    """Each list element must produce its own -tag=val argument."""
    from godmode_media_library.metadata_merge import MergeAction, MergePlan, execute_merge

    plan = MergePlan(
        survivor_path="/survivor.jpg",
        survivor_hash="abc123",
        actions=[
            MergeAction(tag="IPTC:Keywords", value=["alpha", "beta", "gamma"], source_path="/d.jpg", action_type="copy"),
        ],
    )
    mock_proc = MagicMock(returncode=0)
    with (
        patch("godmode_media_library.metadata_merge.sha256_file", return_value="abc123"),
        patch("godmode_media_library.metadata_merge.shutil.which", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc) as mock_run,
        patch("pathlib.Path.exists", return_value=False),
    ):
        execute_merge(plan, dry_run=False)

    cmd = mock_run.call_args[0][0]
    # Must NOT have a single joined value like "alpha, beta, gamma"
    assert "-IPTC:Keywords=alpha, beta, gamma" not in cmd
    # Each value is its own argument
    assert "-IPTC:Keywords=alpha" in cmd
    assert "-IPTC:Keywords=beta" in cmd
    assert "-IPTC:Keywords=gamma" in cmd


# ── 3. perceptual_hash uses PILLOW_IMAGE_EXTS (not full IMAGE_EXTS) ─────
# Issue: phash used a different IMAGE_EXTS missing RAW formats (or including
# formats Pillow can't decode). Fix: import PILLOW_IMAGE_EXTS from asset_sets.


def test_phash_image_exts_matches_pillow_subset():
    """perceptual_hash._IMAGE_EXTS must equal asset_sets.PILLOW_IMAGE_EXTS."""
    from godmode_media_library.asset_sets import PILLOW_IMAGE_EXTS
    from godmode_media_library.perceptual_hash import _IMAGE_EXTS

    assert _IMAGE_EXTS is PILLOW_IMAGE_EXTS or _IMAGE_EXTS == PILLOW_IMAGE_EXTS


def test_phash_rejects_raw_formats():
    """RAW camera formats like .cr2/.nef should not be considered hashable."""
    from godmode_media_library.perceptual_hash import is_image_ext

    for raw_ext in ("cr2", "cr3", "nef", "arw", "raw", "dng"):
        assert not is_image_ext(raw_ext), f"{raw_ext} should not be hashable by Pillow"


def test_phash_accepts_pillow_formats():
    """Common Pillow-decodable formats should be accepted."""
    from godmode_media_library.perceptual_hash import is_image_ext

    for ext in ("jpg", "jpeg", "png", "bmp", "tiff", "gif", "webp", "heic"):
        assert is_image_ext(ext), f"{ext} should be hashable"


# ── 4. exiftool_extract: paths with leading dash get ./ prefix ───────────
# Issue: a file named "-photo.jpg" would be treated as an ExifTool flag.


def test_exiftool_dash_path_prefixed_regression():
    """Paths starting with '-' must be prefixed with './' in the exiftool cmd."""
    import json

    from godmode_media_library.exiftool_extract import extract_all_metadata

    exiftool_output = json.dumps([{"SourceFile": "./-evil.jpg", "EXIF:Make": "Nikon"}])
    mock_proc = MagicMock(returncode=0, stdout=exiftool_output)
    with (
        patch("godmode_media_library.exiftool_extract.exiftool_available", return_value="/usr/bin/exiftool"),
        patch("godmode_media_library.exiftool_extract.subprocess.run", return_value=mock_proc) as mock_run,
    ):
        extract_all_metadata([Path("-evil.jpg")])
        cmd = mock_run.call_args[0][0]
        # The path argument must be ./-evil.jpg, not -evil.jpg
        assert "./-evil.jpg" in cmd
        assert cmd[-1] == "./-evil.jpg"


# ── 5. backup_monitor: no AppleScript string injection ──────────────────
# Issue: title/message were interpolated into AppleScript, allowing injection.
# Fix: use JXA with environment variables.


@patch("subprocess.run")
@patch("platform.system", return_value="Darwin")
def test_notification_uses_env_vars_not_interpolation(mock_sys, mock_run):
    """Notification values must be passed via env vars, not interpolated into script."""
    import godmode_media_library.backup_monitor as bm

    # Use a message with AppleScript injection characters
    evil_title = 'Test"; do shell script "rm -rf /'
    evil_msg = "hello' & run script"

    bm._send_notification(title=evil_title, message=evil_msg, severity="info")
    mock_run.assert_called_once()

    call_kwargs = mock_run.call_args
    # The script source (input kwarg) must NOT contain the title/message text
    script_input = call_kwargs.kwargs.get("input") or call_kwargs[1].get("input", "")
    assert evil_title not in script_input, "Title must not be interpolated into script"
    assert evil_msg not in script_input, "Message must not be interpolated into script"

    # Values must be passed via environment variables
    env = call_kwargs.kwargs.get("env") or call_kwargs[1].get("env", {})
    assert env.get("GML_NOTIFY_TITLE") == evil_title
    assert env.get("GML_NOTIFY_MSG") == evil_msg


# ── 6. catalog.py: fcntl import does not crash on Windows ───────────────
# Issue: bare 'import fcntl' crashes on Windows. Fix: try/except ImportError.


def test_fcntl_conditional_import_regression():
    """If fcntl is unavailable (like Windows), open() must not crash."""
    import builtins
    import os
    import sqlite3
    import tempfile

    from godmode_media_library.catalog import Catalog

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        cat = Catalog(db_path, exclusive=True)

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "fcntl":
                raise ImportError("No fcntl on this platform")
            if name == "msvcrt":
                raise ImportError("No msvcrt either")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            # Should not raise even without fcntl or msvcrt
            cat.open(exclusive=True)

        cat.close()


# ── 7. _date_to_timestamp uses explicit UTC ─────────────────────────────
# Issue: naive datetime.strptime gave local-time timestamps. Fix: explicit UTC.


def test_date_to_timestamp_is_utc_regression():
    """_date_to_timestamp must produce UTC timestamps, not local time."""
    from godmode_media_library.catalog import _date_to_timestamp

    ts = _date_to_timestamp("2024-01-01")
    # UTC midnight 2024-01-01 = 1704067200
    expected = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc).timestamp()
    assert ts == expected, f"Expected UTC timestamp {expected}, got {ts}"


def test_date_to_timestamp_known_value():
    """Verify against a well-known epoch value."""
    from godmode_media_library.catalog import _date_to_timestamp

    # 1970-01-01 UTC midnight = 0.0
    assert _date_to_timestamp("1970-01-01") == 0.0
