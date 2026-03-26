from __future__ import annotations

import json
import logging
import logging.handlers
import sys
from pathlib import Path


class _JsonFormatter(logging.Formatter):
    """JSON log formatter for file output."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, ensure_ascii=True)


def setup_logging(
    verbosity: int = 0,
    log_file: Path | None = None,
    log_max_bytes: int = 10 * 1024 * 1024,
    log_backup_count: int = 3,
) -> None:
    """Configure logging for the application.

    Args:
        verbosity: 0=WARNING, 1=INFO, 2+=DEBUG
        log_file: Optional path for JSON-formatted file logging.
        log_max_bytes: Max size per log file before rotation (default 10 MB).
        log_backup_count: Number of rotated backup files to keep (default 3).
    """
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    root_logger = logging.getLogger("godmode_media_library")
    root_logger.setLevel(level)
    root_logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    console_fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    console_handler.setFormatter(console_fmt)
    root_logger.addHandler(console_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=log_max_bytes,
            backupCount=log_backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(_JsonFormatter())
        root_logger.addHandler(file_handler)
