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
    """Consolidation pipeline phases (order matters).

    New flow (v2):
      1. wait_for_sources
      2. cloud_catalog_scan
      3. local_scan
      4. register_files (was pre_dedup — just registers, NO dedup)
      5. stream (transfer ALL files, no dedup skipping)
      6. retry_failed
      7. verify
      8. extract_archives (NEW — unpack .zip/.rar/.7z/.tar on destination)
      9. dedup (moved here — final dedup over ALL data)
     10. organize (NEW — categorize into Media/Documents/Software/Other)
     11. report

    sync_to_disk is now a standalone function, NOT a pipeline phase.
    """

    WAIT_FOR_SOURCES = "wait_for_sources"
    CLOUD_CATALOG_SCAN = "cloud_catalog_scan"
    LOCAL_SCAN = "local_scan"
    REGISTER_FILES = "register_files"
    STREAM = "stream"
    RETRY_FAILED = "retry_failed"
    VERIFY = "verify"
    EXTRACT_ARCHIVES = "extract_archives"
    DEDUP = "dedup"
    ORGANIZE = "organize"
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
    Phase.REGISTER_FILES: "Registrace souboru pro prenos",
    Phase.STREAM: "Streamovani cloud->cloud",
    Phase.RETRY_FAILED: "Opakovani neúspesných prenosu",
    Phase.VERIFY: "Overovani integrity na cíli",
    Phase.EXTRACT_ARCHIVES: "Rozbalovani archivu",
    Phase.DEDUP: "Finalni deduplikace nad vsemi daty",
    Phase.ORGANIZE: "Organizace souboru podle kategorii",
    Phase.REPORT: "Záverecný report",
    Phase.COMPLETE: "Konsolidace dokoncena",
}

# Phase number (1-indexed) for progress reporting
PHASE_NUMBER: dict[str, int] = {
    Phase.WAIT_FOR_SOURCES: 1,
    Phase.CLOUD_CATALOG_SCAN: 2,
    Phase.LOCAL_SCAN: 3,
    Phase.REGISTER_FILES: 4,
    Phase.STREAM: 5,
    Phase.RETRY_FAILED: 6,
    Phase.VERIFY: 7,
    Phase.EXTRACT_ARCHIVES: 8,
    Phase.DEDUP: 9,
    Phase.ORGANIZE: 10,
    Phase.REPORT: 11,
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

# Google Drive daily upload limit (750 GB), safety margin at 700 GB
GOOGLE_DAILY_UPLOAD_LIMIT = 750 * 1024 * 1024 * 1024  # 750 GB
GOOGLE_DAILY_UPLOAD_SAFETY = 700 * 1024 * 1024 * 1024  # 700 GB safety margin
DAILY_LIMIT_PAUSE_SECONDS = 24 * 3600  # 24 hours

# Watchdog stall detection
WATCHDOG_STALL_SECONDS = 60  # warn after 60s without transfer

# Quota/rate-limit error keywords (lowercase)
QUOTA_ERRORS = (
    "quota",
    "insufficient storage",
    "no space",
    "storage limit",
    "rate limit exceeded",
    "user rate limit",
)

# ---------------------------------------------------------------------------
# File categorization extensions
# ---------------------------------------------------------------------------

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
        ".svg",
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
        # Documents (legacy — kept for _is_media_file compat)
        ".pdf",
    }
)

DOCUMENT_EXTENSIONS = frozenset(
    {
        ".pdf",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx",
        ".txt",
        ".rtf",
        ".odt",
        ".ods",
        ".odp",
        ".csv",
        ".pages",
        ".numbers",
        ".keynote",
        ".md",
    }
)

SOFTWARE_EXTENSIONS = frozenset(
    {
        ".app",
        ".dmg",
        ".pkg",
        ".exe",
        ".msi",
        ".iso",
        ".img",
        ".deb",
        ".rpm",
    }
)

ARCHIVE_EXTENSIONS = frozenset(
    {
        ".zip",
        ".rar",
        ".7z",
        ".tar",
        ".gz",
        ".bz2",
    }
)

# Compound archive suffixes (checked by endswith, not suffix)
ARCHIVE_COMPOUND_SUFFIXES = (
    ".tar.gz",
    ".tar.bz2",
)

# Bundle directory extensions (transfer as unit, never split)
BUNDLE_EXTENSIONS = frozenset(
    {
        ".app",
        ".xcodeproj",
        ".xcworkspace",
        ".lproj",
        ".bundle",
        ".framework",
        ".kext",
        ".plugin",
        ".photoslibrary",
    }
)

# macOS software extensions (for Software/macOS/ organization)
MACOS_SOFTWARE_EXTENSIONS = frozenset({".app", ".dmg", ".pkg"})
WINDOWS_SOFTWARE_EXTENSIONS = frozenset({".exe", ".msi"})

# Job types
JOB_TYPE_ULTIMATE = "ultimate_consolidation"
JOB_TYPE_CLOUD_STREAM = "cloud_stream_reorganize"
CONSOLIDATION_JOB_TYPES = (JOB_TYPE_ULTIMATE, JOB_TYPE_CLOUD_STREAM)
