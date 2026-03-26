"""Checkpoint/resume system for consolidation pipeline."""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .catalog import Catalog

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CHECKPOINT_SQL = """
CREATE TABLE IF NOT EXISTS consolidation_jobs (
    job_id TEXT PRIMARY KEY,
    scenario_id TEXT,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'created',
    current_step TEXT DEFAULT '',
    total_steps INTEGER DEFAULT 0,
    config_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS consolidation_file_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES consolidation_jobs(job_id),
    file_hash TEXT NOT NULL,
    source_location TEXT NOT NULL,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    dest_location TEXT,
    dest_verified INTEGER DEFAULT 0,
    bytes_transferred INTEGER DEFAULT 0,
    attempt_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(job_id, file_hash, step_name)
);

CREATE INDEX IF NOT EXISTS idx_cfs_job_step ON consolidation_file_state(job_id, step_name);
CREATE INDEX IF NOT EXISTS idx_cfs_status ON consolidation_file_state(status);
CREATE INDEX IF NOT EXISTS idx_cfs_job_step_status ON consolidation_file_state(job_id, step_name, status);
"""

# Cache keyed by connection id() — cleared when ensure_tables sees a new connection
_tables_cache: dict[int, bool] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ConsolidationJob:
    job_id: str
    job_type: str
    status: str = "created"
    scenario_id: str | None = None
    current_step: str = ""
    total_steps: int = 0
    config: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""
    completed_at: str | None = None
    error: str | None = None


@dataclass
class FileTransferState:
    id: int | None
    job_id: str
    file_hash: str
    source_location: str
    step_name: str
    status: str = "pending"
    dest_location: str | None = None
    dest_verified: bool = False
    bytes_transferred: int = 0
    attempt_count: int = 0
    last_error: str | None = None
    created_at: str = ""
    updated_at: str = ""


# ---------------------------------------------------------------------------
# Table setup
# ---------------------------------------------------------------------------

def ensure_tables(conn: sqlite3.Connection) -> None:
    cid = id(conn)
    if cid in _tables_cache:
        return
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA journal_mode=WAL")  # WAL for concurrent reads + crash safety
    conn.executescript(_CHECKPOINT_SQL)
    _tables_cache[cid] = True


def _ensure(catalog: Catalog) -> None:
    ensure_tables(catalog.conn)


# ---------------------------------------------------------------------------
# Job row helpers
# ---------------------------------------------------------------------------

def _row_to_job(row: sqlite3.Row) -> ConsolidationJob:
    config = {}
    if row["config_json"]:
        try:
            config = json.loads(row["config_json"])
        except (json.JSONDecodeError, TypeError):
            pass
    return ConsolidationJob(
        job_id=row["job_id"],
        job_type=row["job_type"],
        status=row["status"],
        scenario_id=row["scenario_id"],
        current_step=row["current_step"] or "",
        total_steps=row["total_steps"] or 0,
        config=config,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row["completed_at"],
        error=row["error"],
    )


def _row_to_file_state(row: sqlite3.Row) -> FileTransferState:
    return FileTransferState(
        id=row["id"],
        job_id=row["job_id"],
        file_hash=row["file_hash"],
        source_location=row["source_location"],
        step_name=row["step_name"],
        status=row["status"],
        dest_location=row["dest_location"],
        dest_verified=bool(row["dest_verified"]),
        bytes_transferred=row["bytes_transferred"] or 0,
        attempt_count=row["attempt_count"] or 0,
        last_error=row["last_error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ---------------------------------------------------------------------------
# Job CRUD
# ---------------------------------------------------------------------------

def create_job(
    catalog: Catalog,
    job_type: str,
    config: dict[str, Any] | None = None,
    scenario_id: str | None = None,
) -> ConsolidationJob:
    _ensure(catalog)
    now = _now()
    job_id = uuid4().hex[:8]
    config = config or {}
    conn = catalog.conn
    with conn:
        conn.execute(
            """INSERT INTO consolidation_jobs
               (job_id, scenario_id, job_type, status, config_json, created_at, updated_at)
               VALUES (?, ?, ?, 'created', ?, ?, ?)""",
            (job_id, scenario_id, job_type, json.dumps(config), now, now),
        )
    logger.info("Created consolidation job %s (type=%s)", job_id, job_type)
    return ConsolidationJob(
        job_id=job_id,
        job_type=job_type,
        scenario_id=scenario_id,
        config=config,
        created_at=now,
        updated_at=now,
    )


def get_job(catalog: Catalog, job_id: str) -> ConsolidationJob | None:
    _ensure(catalog)
    conn = catalog.conn
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT * FROM consolidation_jobs WHERE job_id = ?", (job_id,))
    row = cur.fetchone()
    return _row_to_job(row) if row else None


def list_jobs(catalog: Catalog, status: str | None = None) -> list[ConsolidationJob]:
    _ensure(catalog)
    conn = catalog.conn
    conn.row_factory = sqlite3.Row
    if status:
        cur = conn.execute(
            "SELECT * FROM consolidation_jobs WHERE status = ? ORDER BY created_at DESC",
            (status,),
        )
    else:
        cur = conn.execute(
            "SELECT * FROM consolidation_jobs ORDER BY created_at DESC",
        )
    return [_row_to_job(r) for r in cur.fetchall()]


def update_job(
    catalog: Catalog,
    job_id: str,
    *,
    status: str | None = None,
    current_step: str | None = None,
    error: str | None = None,
) -> None:
    _ensure(catalog)
    now = _now()
    sets: list[str] = ["updated_at = ?"]
    params: list[Any] = [now]
    if status is not None:
        sets.append("status = ?")
        params.append(status)
    if current_step is not None:
        sets.append("current_step = ?")
        params.append(current_step)
    if error is not None:
        sets.append("error = ?")
        params.append(error)
    params.append(job_id)
    conn = catalog.conn
    with conn:
        conn.execute(
            f"UPDATE consolidation_jobs SET {', '.join(sets)} WHERE job_id = ?",
            params,
        )


def pause_job(catalog: Catalog, job_id: str) -> None:
    update_job(catalog, job_id, status="paused")
    logger.info("Paused job %s", job_id)


def resume_job(catalog: Catalog, job_id: str) -> None:
    update_job(catalog, job_id, status="running")
    logger.info("Resumed job %s", job_id)


def complete_job(catalog: Catalog, job_id: str, error: str | None = None) -> None:
    _ensure(catalog)
    now = _now()
    status = "failed" if error else "completed"
    conn = catalog.conn
    with conn:
        conn.execute(
            """UPDATE consolidation_jobs
               SET status = ?, completed_at = ?, updated_at = ?, error = ?
               WHERE job_id = ?""",
            (status, now, now, error, job_id),
        )
    logger.info("Completed job %s (status=%s)", job_id, status)


# ---------------------------------------------------------------------------
# File state tracking
# ---------------------------------------------------------------------------

def mark_file(
    catalog: Catalog,
    job_id: str,
    file_hash: str,
    source: str,
    step: str,
    status: str,
    *,
    dest: str | None = None,
    bytes_transferred: int = 0,
    error: str | None = None,
) -> None:
    _ensure(catalog)
    now = _now()
    conn = catalog.conn
    with conn:
        conn.execute(
            """INSERT INTO consolidation_file_state
               (job_id, file_hash, source_location, step_name, status,
                dest_location, bytes_transferred, last_error, attempt_count,
                created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
               ON CONFLICT(job_id, file_hash, step_name) DO UPDATE SET
                   status = CASE
                       -- Terminal states: only 'failed' from verify can override 'completed'
                       WHEN consolidation_file_state.status IN ('completed', 'skipped')
                            AND excluded.status NOT LIKE 'failed'
                       THEN consolidation_file_state.status
                       ELSE excluded.status
                   END,
                   dest_location = COALESCE(excluded.dest_location, dest_location),
                   bytes_transferred = CASE
                       WHEN consolidation_file_state.status IN ('completed', 'skipped')
                            AND excluded.status NOT LIKE 'failed'
                       THEN consolidation_file_state.bytes_transferred
                       ELSE excluded.bytes_transferred
                   END,
                   last_error = CASE
                       WHEN consolidation_file_state.status IN ('completed', 'skipped')
                            AND excluded.status NOT LIKE 'failed'
                       THEN consolidation_file_state.last_error
                       ELSE excluded.last_error
                   END,
                   attempt_count = CASE
                       WHEN consolidation_file_state.status IN ('completed', 'skipped')
                            AND excluded.status NOT LIKE 'failed'
                       THEN consolidation_file_state.attempt_count
                       ELSE attempt_count + 1
                   END,
                   updated_at = excluded.updated_at""",
            (job_id, file_hash, source, step, status, dest, bytes_transferred, error, now, now),
        )


def get_pending_files(
    catalog: Catalog,
    job_id: str,
    step: str,
    limit: int = 500,
) -> list[FileTransferState]:
    _ensure(catalog)
    conn = catalog.conn
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """SELECT * FROM consolidation_file_state
           WHERE job_id = ? AND step_name = ? AND status IN ('pending', 'in_progress')
           ORDER BY id
           LIMIT ?""",
        (job_id, step, limit),
    )
    return [_row_to_file_state(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Progress & resume
# ---------------------------------------------------------------------------

def get_job_progress(
    catalog: Catalog,
    job_id: str,
    step: str | None = None,
) -> dict[str, int]:
    _ensure(catalog)
    conn = catalog.conn
    conn.row_factory = sqlite3.Row
    base = "SELECT status, COUNT(*) as cnt, SUM(bytes_transferred) as total_bytes FROM consolidation_file_state WHERE job_id = ?"
    params: list[Any] = [job_id]
    if step is not None:
        base += " AND step_name = ?"
        params.append(step)
    base += " GROUP BY status"
    cur = conn.execute(base, params)
    rows = cur.fetchall()
    counts: dict[str, int] = {
        "total": 0,
        "pending": 0,
        "in_progress": 0,
        "completed": 0,
        "failed": 0,
        "skipped": 0,
        "bytes_transferred": 0,
    }
    for r in rows:
        s = r["status"]
        c = r["cnt"]
        b = r["total_bytes"] or 0
        counts["total"] += c
        counts["bytes_transferred"] += b
        if s in counts:
            counts[s] = c
    return counts


def get_resumable_jobs(catalog: Catalog) -> list[ConsolidationJob]:
    _ensure(catalog)
    conn = catalog.conn
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """SELECT * FROM consolidation_jobs
           WHERE status IN ('created', 'running', 'paused')
           ORDER BY updated_at DESC""",
    )
    return [_row_to_job(r) for r in cur.fetchall()]


def mark_phase_done(catalog: Catalog, job_id: str, phase: str) -> None:
    """Record that a phase has completed (stored in config_json).

    Uses BEGIN IMMEDIATE to prevent read-modify-write races on config_json.
    """
    _ensure(catalog)
    conn = catalog.conn
    conn.row_factory = sqlite3.Row
    # Atomic read-modify-write with BEGIN IMMEDIATE
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT config_json FROM consolidation_jobs WHERE job_id = ?", (job_id,)).fetchone()
        config = {}
        if row and row["config_json"]:
            try:
                config = json.loads(row["config_json"])
            except (json.JSONDecodeError, TypeError):
                pass
        completed = config.get("completed_phases", [])
        if phase not in completed:
            completed.append(phase)
        config["completed_phases"] = completed
        now = _now()
        conn.execute(
            "UPDATE consolidation_jobs SET config_json = ?, updated_at = ? WHERE job_id = ?",
            (json.dumps(config), now, job_id),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    logger.info("Phase '%s' marked done for job %s", phase, job_id)


def is_phase_done(catalog: Catalog, job_id: str, phase: str) -> bool:
    """Check if a phase has already been completed."""
    _ensure(catalog)
    conn = catalog.conn
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT config_json FROM consolidation_jobs WHERE job_id = ?", (job_id,)).fetchone()
    if not row or not row["config_json"]:
        return False
    try:
        config = json.loads(row["config_json"])
        return phase in config.get("completed_phases", [])
    except (json.JSONDecodeError, TypeError):
        return False


def get_failed_files(
    catalog: Catalog,
    job_id: str,
    step: str,
    limit: int = 5000,
) -> list[FileTransferState]:
    """Get files that failed transfer (for retry pass)."""
    _ensure(catalog)
    conn = catalog.conn
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """SELECT * FROM consolidation_file_state
           WHERE job_id = ? AND step_name = ? AND status = 'failed'
           ORDER BY attempt_count ASC, id
           LIMIT ?""",
        (job_id, step, limit),
    )
    return [_row_to_file_state(r) for r in cur.fetchall()]


def get_files_by_source(
    catalog: Catalog,
    job_id: str,
    step: str,
    source_prefix: str,
    status: str | None = None,
) -> list[FileTransferState]:
    """Get files from a specific source remote."""
    _ensure(catalog)
    conn = catalog.conn
    conn.row_factory = sqlite3.Row
    query = """SELECT * FROM consolidation_file_state
               WHERE job_id = ? AND step_name = ? AND source_location LIKE ?"""
    params: list[Any] = [job_id, step, f"{source_prefix}:%"]
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY id"
    cur = conn.execute(query, params)
    return [_row_to_file_state(r) for r in cur.fetchall()]


def reset_stale_in_progress(
    catalog: Catalog,
    job_id: str,
    step: str,
    stale_after_seconds: int = 1800,
) -> int:
    """Reset files stuck in 'in_progress' back to 'pending' for retry.

    Only resets files whose updated_at is older than *stale_after_seconds*
    (default 30 min) to avoid resetting files that are genuinely transferring.
    """
    _ensure(catalog)
    from datetime import timedelta
    now_dt = datetime.now(timezone.utc)
    stale_threshold = (now_dt - timedelta(seconds=stale_after_seconds)).isoformat()
    now = now_dt.isoformat()
    conn = catalog.conn
    with conn:
        cur = conn.execute(
            """UPDATE consolidation_file_state
               SET status = 'pending', updated_at = ?
               WHERE job_id = ? AND step_name = ? AND status = 'in_progress'
                 AND updated_at < ?""",
            (now, job_id, step, stale_threshold),
        )
    count = cur.rowcount
    if count:
        logger.info("Reset %d stale in_progress files (>%ds old) for job %s step %s",
                     count, stale_after_seconds, job_id, step)
    return count


# ---------------------------------------------------------------------------
# WAL checkpoint (4.4) — call periodically during long jobs
# ---------------------------------------------------------------------------

def wal_checkpoint(catalog: Catalog) -> None:
    """Trigger a WAL checkpoint to keep the WAL file from growing unbounded.

    Safe to call during a running job — uses PASSIVE mode (non-blocking).
    """
    _ensure(catalog)
    try:
        catalog.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except sqlite3.OperationalError as exc:
        logger.debug("WAL checkpoint skipped: %s", exc)


# ---------------------------------------------------------------------------
# DB integrity check (4.5) — run on resume
# ---------------------------------------------------------------------------

def check_db_integrity(catalog: Catalog) -> bool:
    """Quick integrity check on the checkpoint tables.

    Returns True if DB is healthy. Logs warnings on issues.
    """
    _ensure(catalog)
    conn = catalog.conn
    try:
        result = conn.execute("PRAGMA integrity_check(1)").fetchone()
        if result and result[0] == "ok":
            return True
        logger.error("DB integrity check FAILED: %s", result)
        return False
    except sqlite3.DatabaseError as exc:
        logger.error("DB integrity check error: %s", exc)
        return False
