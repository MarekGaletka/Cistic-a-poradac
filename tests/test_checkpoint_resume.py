"""Tests for checkpoint/resume system (Session 5, item 5.1)."""

import sqlite3

import pytest

from godmode_media_library.catalog import Catalog
from godmode_media_library.checkpoint import (
    check_db_integrity,
    complete_job,
    create_job,
    ensure_tables,
    get_job,
    get_job_progress,
    get_resumable_jobs,
    is_phase_done,
    list_jobs,
    mark_phase_done,
    pause_job,
    reset_stale_in_progress,
    resume_job,
    update_job,
    wal_checkpoint,
)


@pytest.fixture()
def catalog(tmp_path):
    cat = Catalog(str(tmp_path / "test.db"))
    cat.open()
    yield cat
    cat.close()


class TestJobLifecycle:
    def test_create_and_get(self, catalog):
        job = create_job(catalog, "test_job", config={"key": "value"})
        assert job.job_id
        assert job.job_type == "test_job"
        assert job.status == "created"
        assert job.config == {"key": "value"}

        retrieved = get_job(catalog, job.job_id)
        assert retrieved is not None
        assert retrieved.job_id == job.job_id
        assert retrieved.config == {"key": "value"}

    def test_update_status(self, catalog):
        job = create_job(catalog, "test_job")
        update_job(catalog, job.job_id, status="running", current_step="phase_1")

        retrieved = get_job(catalog, job.job_id)
        assert retrieved.status == "running"
        assert retrieved.current_step == "phase_1"

    def test_pause_and_resume(self, catalog):
        job = create_job(catalog, "test_job")
        update_job(catalog, job.job_id, status="running")

        pause_job(catalog, job.job_id)
        retrieved = get_job(catalog, job.job_id)
        assert retrieved.status == "paused"

        resume_job(catalog, job.job_id)
        retrieved = get_job(catalog, job.job_id)
        assert retrieved.status == "running"

    def test_complete_success(self, catalog):
        job = create_job(catalog, "test_job")
        complete_job(catalog, job.job_id)

        retrieved = get_job(catalog, job.job_id)
        assert retrieved.status == "completed"
        assert retrieved.completed_at is not None
        assert retrieved.error is None

    def test_complete_with_error(self, catalog):
        job = create_job(catalog, "test_job")
        complete_job(catalog, job.job_id, error="something failed")

        retrieved = get_job(catalog, job.job_id)
        assert retrieved.status == "failed"
        assert retrieved.error == "something failed"

    def test_get_nonexistent_job(self, catalog):
        assert get_job(catalog, "nonexistent") is None


class TestResumableJobs:
    def test_get_resumable_jobs(self, catalog):
        j1 = create_job(catalog, "job_type_a")
        update_job(catalog, j1.job_id, status="running")
        j2 = create_job(catalog, "job_type_b")
        update_job(catalog, j2.job_id, status="paused")
        j3 = create_job(catalog, "job_type_c")
        complete_job(catalog, j3.job_id)

        resumable = get_resumable_jobs(catalog)
        resumable_ids = {j.job_id for j in resumable}
        assert j1.job_id in resumable_ids
        assert j2.job_id in resumable_ids
        assert j3.job_id not in resumable_ids  # completed = not resumable

    def test_list_jobs_by_status(self, catalog):
        j1 = create_job(catalog, "a")
        update_job(catalog, j1.job_id, status="running")
        j2 = create_job(catalog, "b")
        complete_job(catalog, j2.job_id)

        running = list_jobs(catalog, status="running")
        assert len(running) == 1
        assert running[0].job_id == j1.job_id


class TestPhaseTracking:
    def test_mark_and_check_phase_done(self, catalog):
        job = create_job(catalog, "test_job")
        assert not is_phase_done(catalog, job.job_id, "scan")

        mark_phase_done(catalog, job.job_id, "scan")
        assert is_phase_done(catalog, job.job_id, "scan")
        assert not is_phase_done(catalog, job.job_id, "transfer")

    def test_mark_phase_idempotent(self, catalog):
        job = create_job(catalog, "test_job")
        mark_phase_done(catalog, job.job_id, "scan")
        mark_phase_done(catalog, job.job_id, "scan")  # should not duplicate

        retrieved = get_job(catalog, job.job_id)
        assert retrieved.config.get("completed_phases", []).count("scan") == 1

    def test_multiple_phases(self, catalog):
        job = create_job(catalog, "test_job")
        mark_phase_done(catalog, job.job_id, "phase_1")
        mark_phase_done(catalog, job.job_id, "phase_2")
        mark_phase_done(catalog, job.job_id, "phase_3")

        assert is_phase_done(catalog, job.job_id, "phase_1")
        assert is_phase_done(catalog, job.job_id, "phase_2")
        assert is_phase_done(catalog, job.job_id, "phase_3")
        assert not is_phase_done(catalog, job.job_id, "phase_4")


class TestStaleReset:
    def test_reset_stale_in_progress(self, catalog):
        from godmode_media_library.checkpoint import mark_file
        job = create_job(catalog, "test_job")
        # Create a file in in_progress state
        mark_file(catalog, job.job_id, "hash1", "remote:path1", "stream", "in_progress")

        # With a very short stale threshold, it should be reset
        count = reset_stale_in_progress(catalog, job.job_id, "stream", stale_after_seconds=0)
        assert count == 1

    def test_no_reset_for_fresh_files(self, catalog):
        from godmode_media_library.checkpoint import mark_file
        job = create_job(catalog, "test_job")
        mark_file(catalog, job.job_id, "hash1", "remote:path1", "stream", "in_progress")

        # With a large threshold, nothing should be reset
        count = reset_stale_in_progress(catalog, job.job_id, "stream", stale_after_seconds=9999)
        assert count == 0


class TestDBIntegrity:
    def test_integrity_check_healthy(self, catalog):
        assert check_db_integrity(catalog) is True

    def test_wal_checkpoint_no_error(self, catalog):
        wal_checkpoint(catalog)  # should not raise


class TestProgress:
    def test_get_job_progress(self, catalog):
        from godmode_media_library.checkpoint import mark_file
        job = create_job(catalog, "test_job")
        mark_file(catalog, job.job_id, "h1", "r:p1", "stream", "completed", bytes_transferred=100)
        mark_file(catalog, job.job_id, "h2", "r:p2", "stream", "completed", bytes_transferred=200)
        mark_file(catalog, job.job_id, "h3", "r:p3", "stream", "failed", error="timeout")
        mark_file(catalog, job.job_id, "h4", "r:p4", "stream", "pending")

        progress = get_job_progress(catalog, job.job_id, "stream")
        assert progress["completed"] == 2
        assert progress["failed"] == 1
        assert progress["pending"] == 1
        assert progress["bytes_transferred"] == 300
        assert progress["total"] == 4
