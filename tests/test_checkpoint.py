"""Tests for checkpoint module (consolidation pipeline)."""

from __future__ import annotations

import contextlib
import sqlite3

import pytest

from godmode_media_library import checkpoint as cp

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeCatalog:
    """Lightweight stand-in for Catalog that wraps a real sqlite3 connection."""

    def __init__(self, db_path):
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row


@pytest.fixture(autouse=True)
def _reset_tables_flag():
    """No-op — ensure_tables now uses setattr on conn, no global state to patch."""
    yield


@pytest.fixture()
def catalog(tmp_path):
    db = tmp_path / "test.db"
    cat = FakeCatalog(db)
    yield cat
    cat.conn.close()


# ---------------------------------------------------------------------------
# ensure_tables
# ---------------------------------------------------------------------------


def test_ensure_tables_creates_schema(catalog):
    cp.ensure_tables(catalog.conn)
    tables = [r[0] for r in catalog.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert "consolidation_jobs" in tables
    assert "consolidation_file_state" in tables


def test_ensure_tables_idempotent(catalog):
    cp.ensure_tables(catalog.conn)
    # Clear the flag to allow re-entry for the idempotency test
    with contextlib.suppress(AttributeError):
        delattr(catalog.conn, cp._TABLES_OK_ATTR)
    cp.ensure_tables(catalog.conn)  # should not raise


# ---------------------------------------------------------------------------
# Job CRUD
# ---------------------------------------------------------------------------


def test_create_job_returns_job(catalog):
    job = cp.create_job(catalog, "consolidate", config={"src": "/media"})
    assert job.job_type == "consolidate"
    assert job.status == "created"
    assert len(job.job_id) == 16
    assert job.config == {"src": "/media"}


def test_job_id_is_hex(catalog):
    job = cp.create_job(catalog, "consolidate")
    int(job.job_id, 16)  # should not raise


def test_get_job(catalog):
    job = cp.create_job(catalog, "backup")
    fetched = cp.get_job(catalog, job.job_id)
    assert fetched is not None
    assert fetched.job_id == job.job_id
    assert fetched.job_type == "backup"


def test_get_job_missing_returns_none(catalog):
    cp.ensure_tables(catalog.conn)
    assert cp.get_job(catalog, "nonexistent") is None


def test_update_job_changes_status(catalog):
    job = cp.create_job(catalog, "consolidate")
    cp.update_job(catalog, job.job_id, status="running", current_step="copy")
    updated = cp.get_job(catalog, job.job_id)
    assert updated.status == "running"
    assert updated.current_step == "copy"


def test_complete_job_success(catalog):
    job = cp.create_job(catalog, "consolidate")
    cp.complete_job(catalog, job.job_id)
    done = cp.get_job(catalog, job.job_id)
    assert done.status == "completed"
    assert done.completed_at is not None


def test_complete_job_with_error(catalog):
    job = cp.create_job(catalog, "consolidate")
    cp.complete_job(catalog, job.job_id, error="disk full")
    done = cp.get_job(catalog, job.job_id)
    assert done.status == "failed"
    assert done.error == "disk full"


def test_list_jobs_filter_by_status(catalog):
    cp.create_job(catalog, "a")
    j2 = cp.create_job(catalog, "b")
    cp.update_job(catalog, j2.job_id, status="running")
    running = cp.list_jobs(catalog, status="running")
    assert len(running) == 1
    assert running[0].job_id == j2.job_id


# ---------------------------------------------------------------------------
# File state tracking
# ---------------------------------------------------------------------------


def test_mark_file_and_get_pending(catalog):
    job = cp.create_job(catalog, "consolidate")
    cp.mark_file(catalog, job.job_id, "abc123", "/src/a.jpg", "copy", "pending")
    pending = cp.get_pending_files(catalog, job.job_id, "copy")
    assert len(pending) == 1
    assert pending[0].file_hash == "abc123"
    assert pending[0].status == "pending"


def test_mark_file_updates_on_conflict(catalog):
    job = cp.create_job(catalog, "consolidate")
    cp.mark_file(catalog, job.job_id, "abc", "/src/a.jpg", "copy", "pending")
    cp.mark_file(catalog, job.job_id, "abc", "/src/a.jpg", "copy", "completed", dest="/dst/a.jpg")
    pending = cp.get_pending_files(catalog, job.job_id, "copy")
    assert len(pending) == 0  # no longer pending


def test_get_failed_files(catalog):
    job = cp.create_job(catalog, "consolidate")
    cp.mark_file(catalog, job.job_id, "f1", "/src/1.jpg", "copy", "failed", error="io error")
    cp.mark_file(catalog, job.job_id, "f2", "/src/2.jpg", "copy", "completed")
    failed = cp.get_failed_files(catalog, job.job_id, "copy")
    assert len(failed) == 1
    assert failed[0].file_hash == "f1"


def test_get_job_progress(catalog):
    job = cp.create_job(catalog, "consolidate")
    cp.mark_file(catalog, job.job_id, "a", "/s/a", "copy", "completed", bytes_transferred=100)
    cp.mark_file(catalog, job.job_id, "b", "/s/b", "copy", "pending")
    cp.mark_file(catalog, job.job_id, "c", "/s/c", "copy", "failed", error="x")
    progress = cp.get_job_progress(catalog, job.job_id, "copy")
    assert progress["completed"] == 1
    assert progress["pending"] == 1
    assert progress["failed"] == 1
    assert progress["total"] == 3


# ---------------------------------------------------------------------------
# Phase tracking
# ---------------------------------------------------------------------------


def test_mark_phase_done_and_is_phase_done(catalog):
    job = cp.create_job(catalog, "consolidate")
    assert cp.is_phase_done(catalog, job.job_id, "scan") is False
    cp.mark_phase_done(catalog, job.job_id, "scan")
    assert cp.is_phase_done(catalog, job.job_id, "scan") is True


def test_mark_phase_done_idempotent(catalog):
    job = cp.create_job(catalog, "consolidate")
    cp.mark_phase_done(catalog, job.job_id, "scan")
    cp.mark_phase_done(catalog, job.job_id, "scan")  # no error
    assert cp.is_phase_done(catalog, job.job_id, "scan") is True
