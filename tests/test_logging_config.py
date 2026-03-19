from __future__ import annotations

import json
import logging
from pathlib import Path

from godmode_media_library.logging_config import _JsonFormatter, setup_logging


def test_setup_logging_default_level():
    setup_logging(verbosity=0)
    logger = logging.getLogger("godmode_media_library")
    assert logger.level == logging.WARNING


def test_setup_logging_verbose():
    setup_logging(verbosity=1)
    logger = logging.getLogger("godmode_media_library")
    assert logger.level == logging.INFO


def test_setup_logging_very_verbose():
    setup_logging(verbosity=2)
    logger = logging.getLogger("godmode_media_library")
    assert logger.level == logging.DEBUG


def test_setup_logging_file(tmp_path: Path):
    log_file = tmp_path / "logs" / "test.log"
    setup_logging(verbosity=2, log_file=log_file)
    assert log_file.parent.exists()

    logger = logging.getLogger("godmode_media_library")
    logger.debug("test message for file")

    # Flush handlers
    for handler in logger.handlers:
        handler.flush()

    content = log_file.read_text(encoding="utf-8")
    assert content.strip()  # file is not empty
    entry = json.loads(content.strip().splitlines()[0])
    assert entry["level"] == "DEBUG"
    assert "test message for file" in entry["message"]


def test_json_formatter():
    formatter = _JsonFormatter()
    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )
    output = formatter.format(record)
    parsed = json.loads(output)
    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "test.logger"
    assert parsed["message"] == "hello world"
    assert "timestamp" in parsed
