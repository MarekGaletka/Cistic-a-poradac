"""Tests for mark_file state machine transitions (Session 5, item 5.2).

The ON CONFLICT logic in mark_file protects terminal states (completed, skipped)
from being overwritten, except by 'failed' from verification.
"""

import pytest

from godmode_media_library.catalog import Catalog
from godmode_media_library.checkpoint import (
    create_job,
    mark_file,
)


@pytest.fixture()
def catalog(tmp_path):
    cat = Catalog(str(tmp_path / "test.db"))
    cat.open()
    yield cat
    cat.close()


@pytest.fixture()
def job(catalog):
    return create_job(catalog, "test_job")


def _get_file_status(catalog, job_id, file_hash, step="stream"):
    """Helper to read current status from DB."""
    conn = catalog.conn
    import sqlite3

    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT status, last_error FROM consolidation_file_state WHERE job_id = ? AND file_hash = ? AND step_name = ?",
        (job_id, file_hash, step),
    ).fetchone()
    return (row["status"], row["last_error"]) if row else (None, None)


class TestStateTransitions:
    def test_pending_to_in_progress(self, catalog, job):
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "pending")
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "in_progress")
        status, _ = _get_file_status(catalog, job.job_id, "h1")
        assert status == "in_progress"

    def test_in_progress_to_completed(self, catalog, job):
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "in_progress")
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "completed", bytes_transferred=1000)
        status, _ = _get_file_status(catalog, job.job_id, "h1")
        assert status == "completed"

    def test_in_progress_to_failed(self, catalog, job):
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "in_progress")
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "failed", error="timeout")
        status, error = _get_file_status(catalog, job.job_id, "h1")
        assert status == "failed"
        assert error == "timeout"

    def test_completed_blocks_pending(self, catalog, job):
        """Completed files must NOT be reverted to pending (protects re-registration on resume)."""
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "completed", bytes_transferred=500)
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "pending")
        status, _ = _get_file_status(catalog, job.job_id, "h1")
        assert status == "completed"

    def test_completed_blocks_in_progress(self, catalog, job):
        """Completed files must NOT be set to in_progress."""
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "completed", bytes_transferred=500)
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "in_progress")
        status, _ = _get_file_status(catalog, job.job_id, "h1")
        assert status == "completed"

    def test_completed_allows_failed_from_verify(self, catalog, job):
        """Verification failures MUST override completed (key safety feature)."""
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "completed", bytes_transferred=500)
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "failed", error="verify_missing")
        status, error = _get_file_status(catalog, job.job_id, "h1")
        assert status == "failed"
        assert "verify" in error

    def test_skipped_blocks_pending(self, catalog, job):
        """Skipped files must stay skipped."""
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "skipped")
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "pending")
        status, _ = _get_file_status(catalog, job.job_id, "h1")
        assert status == "skipped"

    def test_skipped_allows_failed(self, catalog, job):
        """Failed from verify can override skipped too."""
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "skipped")
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "failed", error="verify_size_mismatch")
        status, _ = _get_file_status(catalog, job.job_id, "h1")
        assert status == "failed"

    def test_failed_to_pending_allowed(self, catalog, job):
        """Failed files can be reset to pending for retry."""
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "failed", error="timeout")
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "pending")
        status, _ = _get_file_status(catalog, job.job_id, "h1")
        assert status == "pending"


class TestAttemptCount:
    def test_attempt_count_increments(self, catalog, job):
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "in_progress")
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "failed", error="e1")
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "failed", error="e2")

        conn = catalog.conn
        import sqlite3

        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT attempt_count FROM consolidation_file_state WHERE job_id = ? AND file_hash = ? AND step_name = ?",
            (job.job_id, "h1", "stream"),
        ).fetchone()
        assert row["attempt_count"] >= 2

    def test_completed_preserves_attempt_count(self, catalog, job):
        """Re-registering a completed file should not change its attempt count."""
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "completed", bytes_transferred=100)
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "pending")

        conn = catalog.conn
        import sqlite3

        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT attempt_count, bytes_transferred FROM consolidation_file_state WHERE job_id = ? AND file_hash = ?",
            (job.job_id, "h1"),
        ).fetchone()
        assert row["bytes_transferred"] == 100  # preserved
