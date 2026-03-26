"""Tests for consolidation_types enums and constants (Session 5, item 5.6)."""

from godmode_media_library.consolidation_types import (
    CONSOLIDATION_JOB_TYPES,
    DEDUP_TIMEOUT,
    ERROR_TRUNCATE_LEN,
    JOB_TYPE_CLOUD_STREAM,
    JOB_TYPE_ULTIMATE,
    MAX_RETRY_ATTEMPTS,
    MAX_SOURCE_FAILURES,
    MEDIA_EXTENSIONS,
    QUOTA_ERRORS,
    STREAM_BATCH_SIZE,
    VERIFY_FAIL_THRESHOLD_PCT,
    DedupStrategy,
    FileStatus,
    JobStatus,
    Phase,
    StructurePattern,
)


# ── StrEnum backward compatibility ──


def test_job_status_is_string():
    """JobStatus values must be plain strings for DB compatibility."""
    assert JobStatus.RUNNING == "running"
    assert isinstance(JobStatus.RUNNING, str)
    assert JobStatus.PAUSED in ("paused",)


def test_file_status_is_string():
    assert FileStatus.PENDING == "pending"
    assert FileStatus.COMPLETED == "completed"
    assert FileStatus.FAILED == "failed"
    assert FileStatus.SKIPPED == "skipped"
    assert FileStatus.IN_PROGRESS == "in_progress"


def test_phase_is_string():
    """Phase enum values must match the string keys used in checkpoint DB."""
    assert Phase.WAIT_FOR_SOURCES == "wait_for_sources"
    assert Phase.STREAM == "stream"
    assert Phase.VERIFY == "verify"
    assert Phase.COMPLETE == "complete"


def test_all_phases_have_unique_values():
    values = [p.value for p in Phase]
    assert len(values) == len(set(values))


def test_structure_pattern_values():
    assert StructurePattern.YEAR_MONTH == "year_month"
    assert StructurePattern.YEAR == "year"
    assert StructurePattern.FLAT == "flat"


def test_dedup_strategy_values():
    assert DedupStrategy.RICHNESS == "richness"
    assert DedupStrategy.LARGEST == "largest"


# ── Constants ──


def test_media_extensions_contains_common_types():
    assert ".jpg" in MEDIA_EXTENSIONS
    assert ".mp4" in MEDIA_EXTENSIONS
    assert ".heic" in MEDIA_EXTENSIONS
    assert ".pdf" in MEDIA_EXTENSIONS
    # Non-media should not be present
    assert ".py" not in MEDIA_EXTENSIONS
    assert ".json" not in MEDIA_EXTENSIONS


def test_quota_errors_are_lowercase():
    for q in QUOTA_ERRORS:
        assert q == q.lower(), f"QUOTA_ERRORS entry '{q}' should be lowercase"


def test_constants_are_positive():
    assert STREAM_BATCH_SIZE > 0
    assert MAX_RETRY_ATTEMPTS > 0
    assert MAX_SOURCE_FAILURES > 0
    assert ERROR_TRUNCATE_LEN > 0
    assert DEDUP_TIMEOUT > 0
    assert VERIFY_FAIL_THRESHOLD_PCT > 0


def test_job_type_constants():
    assert JOB_TYPE_ULTIMATE == "ultimate_consolidation"
    assert JOB_TYPE_CLOUD_STREAM == "cloud_stream_reorganize"
    assert JOB_TYPE_ULTIMATE in CONSOLIDATION_JOB_TYPES
    assert JOB_TYPE_CLOUD_STREAM in CONSOLIDATION_JOB_TYPES


# ── StrEnum membership ──


def test_file_status_membership():
    """All standard status strings should be valid FileStatus members."""
    for s in ("pending", "in_progress", "completed", "failed", "skipped"):
        assert s in FileStatus.__members__.values() or FileStatus(s)
