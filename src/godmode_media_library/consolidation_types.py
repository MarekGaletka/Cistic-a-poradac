"""Type definitions, enums, and constants for the consolidation pipeline."""

from __future__ import annotations

from enum import StrEnum


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class JobStatus(StrEnum):
    """Status of a consolidation job."""

    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class FileStatus(StrEnum):
    """Status of a single file transfer."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class Phase(StrEnum):
    """Consolidation pipeline phases (order matters)."""

    WAIT_FOR_SOURCES = "wait_for_sources"
    CLOUD_CATALOG_SCAN = "cloud_catalog_scan"
    LOCAL_SCAN = "local_scan"
    DEDUP = "dedup"
    STREAM = "stream"
    RETRY_FAILED = "retry_failed"
    VERIFY = "verify"
    POST_TRANSFER_DEDUP = "post_transfer_dedup"
    SYNC_TO_DISK = "sync_to_disk"
    REPORT = "report"
    COMPLETE = "complete"


class StructurePattern(StrEnum):
    """Destination directory structure patterns."""

    YEAR_MONTH = "year_month"
    YEAR = "year"
    FLAT = "flat"


class DedupStrategy(StrEnum):
    """Deduplication strategies."""

    RICHNESS = "richness"
    LARGEST = "largest"
    NEWEST = "newest"


# ---------------------------------------------------------------------------
# Phase labels (Czech, for UI)
# ---------------------------------------------------------------------------

PHASE_LABELS: dict[str, str] = {
    Phase.WAIT_FOR_SOURCES: "Cekani na zdroje",
    Phase.CLOUD_CATALOG_SCAN: "Katalogizace vzdalených zdroju",
    Phase.LOCAL_SCAN: "Skenovani lokalnich souboru",
    Phase.DEDUP: "Pre-transfer deduplikace",
    Phase.STREAM: "Streamovani cloud->cloud",
    Phase.RETRY_FAILED: "Opakovani neúspesných prenosu",
    Phase.VERIFY: "Overovani integrity na cíli",
    Phase.POST_TRANSFER_DEDUP: "Post-transfer deduplikace",
    Phase.SYNC_TO_DISK: "Synchronizace na disk",
    Phase.REPORT: "Záverecný report",
    Phase.COMPLETE: "Konsolidace dokoncena",
}

# Phase number (1-indexed) for progress reporting
PHASE_NUMBER: dict[str, int] = {
    Phase.WAIT_FOR_SOURCES: 1,
    Phase.CLOUD_CATALOG_SCAN: 2,
    Phase.LOCAL_SCAN: 3,
    Phase.DEDUP: 4,
    Phase.STREAM: 5,
    Phase.RETRY_FAILED: 6,
    Phase.VERIFY: 7,
    Phase.POST_TRANSFER_DEDUP: 8,
    Phase.SYNC_TO_DISK: 9,
    Phase.REPORT: 10,
}

# ---------------------------------------------------------------------------
# Named constants (previously magic numbers)
# ---------------------------------------------------------------------------

# Batch sizes
STREAM_BATCH_SIZE = 200
CATALOG_COMMIT_INTERVAL = 1000
VERIFY_REPORT_INTERVAL = 50

# Retry/failure thresholds
MAX_RETRY_ATTEMPTS = 5
MAX_SOURCE_FAILURES = 5
VERIFY_FAIL_THRESHOLD_PCT = 5.0

# Timeouts (seconds)
STALE_IN_PROGRESS_SECONDS = 1800  # 30 min
SOURCE_CONNECTIVITY_WAIT = 60
RETRY_CONNECTIVITY_WAIT = 120
DEST_CONNECTIVITY_TIMEOUT = 15
DEDUP_TIMEOUT = 7200  # 2 hours
DEFAULT_RETRY_TIMEOUT = 600

# Error message truncation
ERROR_TRUNCATE_LEN = 200
ERROR_TRUNCATE_SHORT = 150
ERROR_TRUNCATE_MEDIUM = 100

# Quota/rate-limit error keywords (lowercase)
QUOTA_ERRORS = (
    "quota",
    "insufficient storage",
    "no space",
    "storage limit",
    "rate limit exceeded",
    "user rate limit",
)

# Media file extensions
MEDIA_EXTENSIONS = frozenset(
    {
        # Images
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".bmp",
        ".tiff",
        ".tif",
        ".webp",
        ".heic",
        ".heif",
        ".avif",
        ".raw",
        ".cr2",
        ".cr3",
        ".nef",
        ".arw",
        ".dng",
        ".orf",
        ".rw2",
        ".pef",
        ".srw",
        # Video
        ".mp4",
        ".mov",
        ".avi",
        ".mkv",
        ".wmv",
        ".flv",
        ".webm",
        ".m4v",
        ".3gp",
        ".mts",
        ".m2ts",
        ".mpg",
        ".mpeg",
        ".vob",
        # Audio
        ".mp3",
        ".aac",
        ".flac",
        ".wav",
        ".ogg",
        ".m4a",
        ".wma",
        ".aiff",
        # Documents
        ".pdf",
    }
)

# Job types
JOB_TYPE_ULTIMATE = "ultimate_consolidation"
JOB_TYPE_CLOUD_STREAM = "cloud_stream_reorganize"
CONSOLIDATION_JOB_TYPES = (JOB_TYPE_ULTIMATE, JOB_TYPE_CLOUD_STREAM)
