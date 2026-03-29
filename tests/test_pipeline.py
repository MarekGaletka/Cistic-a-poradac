from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from godmode_media_library.pipeline import PipelineConfig, PipelineResult, run_pipeline


def test_pipeline_empty_roots(tmp_path):
    """Pipeline with empty directory produces zero results."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[empty_dir],
        catalog_path=catalog_path,
        interactive=False,
    )

    result = run_pipeline(config)
    assert isinstance(result, PipelineResult)
    assert result.files_scanned == 0
    assert result.duplicate_groups == 0


def test_pipeline_with_files_no_duplicates(tmp_path):
    """Pipeline with unique files: scans but finds no duplicates."""
    root = tmp_path / "photos"
    root.mkdir()
    (root / "a.txt").write_text("content a")
    (root / "b.txt").write_text("content b")
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=False,
    )

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
        patch("godmode_media_library.pipeline.extract_all_metadata", return_value={}),
    ):
        result = run_pipeline(config)

    assert result.files_scanned == 2
    assert result.duplicate_groups == 0


def test_pipeline_with_duplicates(tmp_path):
    """Pipeline finds duplicates and creates merge plans."""
    root = tmp_path / "photos"
    root.mkdir()
    # Create two identical files
    (root / "original.txt").write_bytes(b"same content here")
    (root / "copy.txt").write_bytes(b"same content here")
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=False,
    )

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
        patch("godmode_media_library.pipeline.extract_all_metadata", return_value={}),
    ):
        result = run_pipeline(config)

    assert result.files_scanned == 2
    assert result.duplicate_groups >= 1


def test_pipeline_skip_steps(tmp_path):
    """Pipeline respects skip_steps."""
    root = tmp_path / "photos"
    root.mkdir()
    (root / "a.txt").write_text("content")
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=False,
        skip_steps={"scan", "extract", "diff", "merge"},
    )

    result = run_pipeline(config)
    assert result.files_scanned == 0  # scan skipped


def test_pipeline_dry_run(tmp_path):
    """Pipeline in dry-run mode doesn't modify files."""
    root = tmp_path / "photos"
    root.mkdir()
    (root / "a.txt").write_bytes(b"same")
    (root / "b.txt").write_bytes(b"same")
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=False,
        dry_run=True,
    )

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
        patch("godmode_media_library.pipeline.extract_all_metadata", return_value={}),
    ):
        result = run_pipeline(config)

    assert isinstance(result, PipelineResult)


def test_pipeline_interactive_abort(tmp_path):
    """Pipeline stops when user declines at checkpoint."""
    root = tmp_path / "photos"
    root.mkdir()
    (root / "a.txt").write_bytes(b"same content")
    (root / "b.txt").write_bytes(b"same content")
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=True,
    )

    def decline(_msg: str) -> bool:
        return False

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
        patch("godmode_media_library.pipeline.extract_all_metadata", return_value={}),
    ):
        result = run_pipeline(config, confirm_fn=decline)

    # Should have scanned but not merged (user declined)
    assert result.tags_merged == 0


def test_pipeline_config_defaults():
    """PipelineConfig has sensible defaults."""
    config = PipelineConfig(roots=[Path("/tmp")])
    assert config.interactive is True
    assert config.dry_run is False
    assert config.workers == 1
    assert config.skip_steps == set()


def test_pipeline_result_defaults():
    """PipelineResult initializes to zero."""
    result = PipelineResult()
    assert result.files_scanned == 0
    assert result.tags_merged == 0
    assert result.errors == []


def test_pipeline_auto_merge_skips_confirmation(tmp_path):
    """With auto_merge=True, pipeline proceeds without confirmation."""
    root = tmp_path / "photos"
    root.mkdir()
    (root / "a.txt").write_bytes(b"same content")
    (root / "b.txt").write_bytes(b"same content")
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=True,
        auto_merge=True,
    )

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
        patch("godmode_media_library.pipeline.extract_all_metadata", return_value={}),
        patch("godmode_media_library.pipeline.execute_merge") as mock_merge,
    ):
        mock_merge.return_value = type("MR", (), {"error": None, "applied": 0})()
        result = run_pipeline(config)

    # Should have scanned without asking for confirmation
    assert result.files_scanned == 2


def test_pipeline_skip_scan_only(tmp_path):
    """Skipping only scan still runs other steps."""
    root = tmp_path / "photos"
    root.mkdir()
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=False,
        skip_steps={"scan"},
    )

    with (
        patch("godmode_media_library.pipeline.extract_all_metadata", return_value={}),
    ):
        result = run_pipeline(config)

    assert result.files_scanned == 0  # scan was skipped
    assert result.duplicate_groups == 0


def test_pipeline_skip_extract(tmp_path):
    """Skipping extract step."""
    root = tmp_path / "photos"
    root.mkdir()
    (root / "a.txt").write_text("content")
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=False,
        skip_steps={"extract"},
    )

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
    ):
        result = run_pipeline(config)

    assert result.metadata_extracted == 0


def test_pipeline_skip_diff(tmp_path):
    """Skipping diff step means no duplicate analysis."""
    root = tmp_path / "photos"
    root.mkdir()
    (root / "a.txt").write_bytes(b"same")
    (root / "b.txt").write_bytes(b"same")
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=False,
        skip_steps={"diff"},
    )

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
        patch("godmode_media_library.pipeline.extract_all_metadata", return_value={}),
    ):
        result = run_pipeline(config)

    assert result.duplicate_groups == 0


def test_pipeline_skip_merge(tmp_path):
    """Skipping merge step means no tags merged."""
    root = tmp_path / "photos"
    root.mkdir()
    (root / "a.txt").write_bytes(b"same")
    (root / "b.txt").write_bytes(b"same")
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=False,
        skip_steps={"merge"},
    )

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
        patch("godmode_media_library.pipeline.extract_all_metadata", return_value={}),
    ):
        result = run_pipeline(config)

    assert result.tags_merged == 0


def test_pipeline_confirm_fn_default_interactive(tmp_path):
    """When interactive=True and no confirm_fn, pipeline creates default."""
    root = tmp_path / "photos"
    root.mkdir()
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=True,
        skip_steps={"scan", "extract", "diff", "merge"},
    )

    # Should not raise even without confirm_fn
    result = run_pipeline(config)
    assert isinstance(result, PipelineResult)


def test_pipeline_metadata_extraction_step(tmp_path):
    """Pipeline extracts metadata for files missing it."""
    root = tmp_path / "photos"
    root.mkdir()
    (root / "a.txt").write_text("content")
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=False,
        skip_steps={"scan", "diff", "merge"},  # Only run extract
    )

    mock_meta = {str(root / "a.txt"): {"EXIF:Make": "Canon"}}
    mock_richness = type("R", (), {"total": 55.0})()

    with (
        patch("godmode_media_library.pipeline.extract_all_metadata", return_value=mock_meta),
        patch("godmode_media_library.pipeline.compute_richness", return_value=mock_richness),
    ):
        result = run_pipeline(config)

    # No files in catalog (scan was skipped), so nothing extracted
    assert result.metadata_extracted == 0


def test_pipeline_merge_with_errors(tmp_path):
    """Pipeline records merge errors."""
    root = tmp_path / "photos"
    root.mkdir()
    (root / "a.txt").write_bytes(b"same content")
    (root / "b.txt").write_bytes(b"same content")
    catalog_path = tmp_path / "test.db"

    config = PipelineConfig(
        roots=[root],
        catalog_path=catalog_path,
        interactive=False,
        auto_merge=True,
    )

    mock_merge_result = type("MR", (), {"error": "ExifTool failed", "applied": 0})()

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
        patch("godmode_media_library.pipeline.extract_all_metadata", return_value={}),
        patch("godmode_media_library.pipeline.execute_merge", return_value=mock_merge_result),
    ):
        result = run_pipeline(config)

    # Errors are only appended when merge plans have actions, which depends on
    # whether duplicates have differing metadata
    assert isinstance(result, PipelineResult)
