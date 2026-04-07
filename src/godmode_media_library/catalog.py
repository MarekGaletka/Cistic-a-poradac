"""Persistent SQLite catalog for GOD MODE Media Library."""

from __future__ import annotations

import contextlib
import logging
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .utils import utc_stamp

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 12

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    path            TEXT    NOT NULL UNIQUE,
    size            INTEGER NOT NULL,
    mtime           REAL    NOT NULL,
    ctime           REAL    NOT NULL,
    birthtime       REAL,
    ext             TEXT    NOT NULL DEFAULT '',
    sha256          TEXT,
    inode           INTEGER,
    device          INTEGER,
    nlink           INTEGER DEFAULT 1,
    asset_key       TEXT,
    asset_component INTEGER DEFAULT 0,
    xattr_count     INTEGER DEFAULT 0,
    first_seen      TEXT    NOT NULL,
    last_scanned    TEXT    NOT NULL,
    -- Media metadata (Phase 3)
    duration_seconds REAL,
    width           INTEGER,
    height          INTEGER,
    video_codec     TEXT,
    audio_codec     TEXT,
    bitrate         INTEGER,
    phash           TEXT,
    date_original   TEXT,
    camera_make     TEXT,
    camera_model    TEXT,
    gps_latitude    REAL,
    gps_longitude   REAL,
    metadata_richness REAL,
    quality_blur REAL,
    quality_brightness REAL,
    quality_category TEXT,
    source_remote TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_sha256    ON files(sha256);
CREATE INDEX IF NOT EXISTS idx_files_ext       ON files(ext);
CREATE INDEX IF NOT EXISTS idx_files_size      ON files(size);
CREATE INDEX IF NOT EXISTS idx_files_phash     ON files(phash);
CREATE INDEX IF NOT EXISTS idx_files_richness  ON files(metadata_richness);
CREATE INDEX IF NOT EXISTS idx_files_mtime     ON files(mtime);
CREATE INDEX IF NOT EXISTS idx_files_birthtime ON files(birthtime);
CREATE INDEX IF NOT EXISTS idx_files_date_orig ON files(date_original);

CREATE TABLE IF NOT EXISTS file_metadata (
    file_id      INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
    raw_json     TEXT    NOT NULL,
    extracted_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS labels (
    file_id    INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    people     TEXT    NOT NULL DEFAULT '',
    place      TEXT    NOT NULL DEFAULT '',
    updated_at TEXT    NOT NULL,
    PRIMARY KEY (file_id)
);

CREATE TABLE IF NOT EXISTS scans (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    root           TEXT    NOT NULL,
    started_at     TEXT    NOT NULL,
    finished_at    TEXT,
    files_scanned  INTEGER DEFAULT 0,
    files_new      INTEGER DEFAULT 0,
    files_changed  INTEGER DEFAULT 0,
    files_removed  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS duplicates (
    group_id  TEXT    NOT NULL,
    file_id   INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    is_primary INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_dup_group ON duplicates(group_id);
CREATE INDEX IF NOT EXISTS idx_dup_file ON duplicates(file_id);

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    color TEXT NOT NULL DEFAULT '#58a6ff'
);

CREATE TABLE IF NOT EXISTS file_tags (
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (file_id, tag_id)
);

CREATE INDEX IF NOT EXISTS idx_file_tags_tag ON file_tags(tag_id);

CREATE TABLE IF NOT EXISTS file_notes (
    file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
    note TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS file_ratings (
    file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
    rating INTEGER NOT NULL CHECK(rating >= 1 AND rating <= 5)
);

CREATE TABLE IF NOT EXISTS persons (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL DEFAULT '',
    sample_face_id INTEGER,
    face_count  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS faces (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    face_index  INTEGER NOT NULL DEFAULT 0,
    person_id   INTEGER REFERENCES persons(id) ON DELETE SET NULL,
    bbox_top    INTEGER NOT NULL,
    bbox_right  INTEGER NOT NULL,
    bbox_bottom INTEGER NOT NULL,
    bbox_left   INTEGER NOT NULL,
    encoding    BLOB,
    cluster_id  INTEGER DEFAULT -1,
    confidence  REAL,
    created_at  TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_faces_file ON faces(file_id);
CREATE INDEX IF NOT EXISTS idx_faces_person ON faces(person_id);
CREATE INDEX IF NOT EXISTS idx_faces_cluster ON faces(cluster_id);

CREATE TABLE IF NOT EXISTS face_privacy (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    key   TEXT NOT NULL UNIQUE,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shares (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    token TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL DEFAULT '',
    password_hash TEXT,
    expires_at TEXT,
    max_downloads INTEGER,
    download_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shares_token ON shares(token);
CREATE INDEX IF NOT EXISTS idx_shares_file ON shares(file_id);

CREATE TABLE IF NOT EXISTS backup_targets (
    remote_name TEXT PRIMARY KEY,
    remote_path TEXT DEFAULT 'GML-Backup',
    enabled INTEGER DEFAULT 1,
    priority INTEGER DEFAULT 0,
    total_bytes INTEGER DEFAULT 0,
    used_bytes INTEGER DEFAULT 0,
    free_bytes INTEGER DEFAULT 0,
    last_probed_at TEXT,
    encrypted INTEGER DEFAULT 0,
    crypt_remote TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS backup_manifest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT,
    size INTEGER NOT NULL,
    remote_name TEXT NOT NULL,
    remote_path TEXT NOT NULL,
    backed_up_at TEXT NOT NULL,
    verified INTEGER DEFAULT 0,
    verified_at TEXT,
    UNIQUE(file_id, remote_name)
);
CREATE INDEX IF NOT EXISTS idx_bm_file ON backup_manifest(file_id);
CREATE INDEX IF NOT EXISTS idx_bm_remote ON backup_manifest(remote_name);
CREATE INDEX IF NOT EXISTS idx_bm_sha ON backup_manifest(sha256);

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

CREATE UNIQUE INDEX IF NOT EXISTS idx_dup_group_file ON duplicates(group_id, file_id);
CREATE INDEX IF NOT EXISTS idx_labels_people ON labels(people);
CREATE INDEX IF NOT EXISTS idx_labels_place ON labels(place);
"""


@dataclass
class CatalogFileRow:
    """Represents a file row in the catalog."""

    id: int | None
    path: str
    size: int
    mtime: float
    ctime: float
    birthtime: float | None
    ext: str
    sha256: str | None
    inode: int | None
    device: int | None
    nlink: int
    asset_key: str | None
    asset_component: bool
    xattr_count: int
    first_seen: str
    last_scanned: str
    # Media metadata (Phase 3)
    duration_seconds: float | None = None
    width: int | None = None
    height: int | None = None
    video_codec: str | None = None
    audio_codec: str | None = None
    bitrate: int | None = None
    phash: str | None = None
    date_original: str | None = None
    camera_make: str | None = None
    camera_model: str | None = None
    gps_latitude: float | None = None
    gps_longitude: float | None = None
    metadata_richness: float | None = None


@dataclass
class ScanStats:
    """Statistics for an incremental scan."""

    root: str
    files_scanned: int = 0
    files_new: int = 0
    files_changed: int = 0
    files_removed: int = 0
    bytes_hashed: int = 0


class Catalog:
    """SQLite-backed persistent catalog."""

    def __init__(self, db_path: Path | str, *, exclusive: bool = False) -> None:
        self._db_path = Path(db_path) if not isinstance(db_path, Path) else db_path
        self._conn: sqlite3.Connection | None = None
        self._lock_fd: int | None = None
        self._exclusive = exclusive

    @property
    def db_path(self) -> Path:
        return self._db_path

    def open(self, exclusive: bool = False) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        if exclusive:
            lock_path = self._db_path.with_suffix(".lock")
            self._lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
            try:
                import fcntl

                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except ImportError:
                # fcntl not available on Windows — skip file locking
                pass
            except OSError:
                os.close(self._lock_fd)
                self._lock_fd = None
                raise RuntimeError(
                    "Another gml process is writing to the catalog. Try again later."
                ) from None
            except BaseException:
                os.close(self._lock_fd)
                self._lock_fd = None
                raise
        conn = sqlite3.connect(str(self._db_path))
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(_SCHEMA_SQL)
            # Schema version management and migration
            cur = conn.execute("SELECT value FROM meta WHERE key='schema_version'")
            row = cur.fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO meta (key, value) VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
                conn.commit()
            else:
                current_version = int(row[0])
                if current_version < SCHEMA_VERSION:
                    self._conn = conn  # _migrate needs self._conn
                    self._migrate(current_version)
                    conn.execute(
                        "UPDATE meta SET value = ? WHERE key = 'schema_version'",
                        (str(SCHEMA_VERSION),),
                    )
                    conn.commit()
        except Exception:
            logger.exception("Catalog open failed")
            conn.close()
            self._conn = None
            self._release_lock()
            raise
        self._conn = conn

    def _migrate(self, from_version: int) -> None:
        """Apply schema migrations from from_version to SCHEMA_VERSION.

        Each migration step runs inside a SAVEPOINT so that a failure
        mid-step rolls back only that step's partial changes, leaving
        the database in the last successfully-migrated state.
        """
        assert self._conn is not None
        if from_version < 2:
            logger.info("Migrating catalog schema v%d -> v2: adding media metadata columns", from_version)
            self._conn.execute("SAVEPOINT migrate_v2")
            try:
                media_columns = [
                    ("duration_seconds", "REAL"),
                    ("width", "INTEGER"),
                    ("height", "INTEGER"),
                    ("video_codec", "TEXT"),
                    ("audio_codec", "TEXT"),
                    ("bitrate", "INTEGER"),
                    ("phash", "TEXT"),
                    ("date_original", "TEXT"),
                    ("camera_make", "TEXT"),
                    ("camera_model", "TEXT"),
                    ("gps_latitude", "REAL"),
                    ("gps_longitude", "REAL"),
                ]
                for col_name, col_type in media_columns:
                    with contextlib.suppress(sqlite3.OperationalError):
                        # Safe: col_name/col_type are hardcoded constants above, not user input
                        self._conn.execute(f"ALTER TABLE files ADD COLUMN {col_name} {col_type}")  # noqa: S608
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_files_phash ON files(phash)")
                self._conn.execute("RELEASE SAVEPOINT migrate_v2")
            except sqlite3.Error:
                logger.exception("Migration v2 failed")
                self._conn.execute("ROLLBACK TO SAVEPOINT migrate_v2")
                raise
        if from_version < 3:
            logger.info("Migrating catalog schema v%d -> v3: adding metadata richness and file_metadata table", from_version)
            self._conn.execute("SAVEPOINT migrate_v3")
            try:
                with contextlib.suppress(sqlite3.OperationalError):
                    self._conn.execute("ALTER TABLE files ADD COLUMN metadata_richness REAL")
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_files_richness ON files(metadata_richness)")
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS file_metadata (
                        file_id      INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
                        raw_json     TEXT    NOT NULL,
                        extracted_at TEXT    NOT NULL
                    )
                """)
                self._conn.execute("RELEASE SAVEPOINT migrate_v3")
            except sqlite3.Error:
                logger.exception("Migration v3 failed")
                self._conn.execute("ROLLBACK TO SAVEPOINT migrate_v3")
                raise
        if from_version < 4:
            logger.info("Migrating catalog schema v%d -> v4: adding tags and file_tags tables", from_version)
            self._conn.execute("SAVEPOINT migrate_v4")
            try:
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS tags (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL UNIQUE,
                        color TEXT NOT NULL DEFAULT '#58a6ff'
                    )
                """)
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS file_tags (
                        file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                        tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                        PRIMARY KEY (file_id, tag_id)
                    )
                """)
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_file_tags_tag ON file_tags(tag_id)")
                self._conn.execute("RELEASE SAVEPOINT migrate_v4")
            except sqlite3.Error:
                logger.exception("Migration v4 failed")
                self._conn.execute("ROLLBACK TO SAVEPOINT migrate_v4")
                raise
        if from_version < 5:
            logger.info("Migrating catalog schema v%d -> v5: adding file_notes and file_ratings tables", from_version)
            self._conn.execute("SAVEPOINT migrate_v5")
            try:
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS file_notes (
                        file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
                        note TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                """)
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS file_ratings (
                        file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
                        rating INTEGER NOT NULL CHECK(rating >= 1 AND rating <= 5)
                    )
                """)
                self._conn.execute("RELEASE SAVEPOINT migrate_v5")
            except sqlite3.Error:
                logger.exception("Migration v5 failed")
                self._conn.execute("ROLLBACK TO SAVEPOINT migrate_v5")
                raise
        if from_version < 6:
            logger.info("Migrating catalog schema v%d -> v6: adding persons, faces, face_privacy tables", from_version)
            self._conn.execute("SAVEPOINT migrate_v6")
            try:
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS persons (
                        id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        name        TEXT    NOT NULL DEFAULT '',
                        sample_face_id INTEGER,
                        face_count  INTEGER NOT NULL DEFAULT 0,
                        created_at  TEXT    NOT NULL,
                        updated_at  TEXT    NOT NULL
                    )
                """)
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS faces (
                        id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                        face_index  INTEGER NOT NULL DEFAULT 0,
                        person_id   INTEGER REFERENCES persons(id) ON DELETE SET NULL,
                        bbox_top    INTEGER NOT NULL,
                        bbox_right  INTEGER NOT NULL,
                        bbox_bottom INTEGER NOT NULL,
                        bbox_left   INTEGER NOT NULL,
                        encoding    BLOB,
                        cluster_id  INTEGER DEFAULT -1,
                        confidence  REAL,
                        created_at  TEXT    NOT NULL
                    )
                """)
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_faces_file ON faces(file_id)")
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_faces_person ON faces(person_id)")
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_faces_cluster ON faces(cluster_id)")
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS face_privacy (
                        id    INTEGER PRIMARY KEY AUTOINCREMENT,
                        key   TEXT NOT NULL UNIQUE,
                        value TEXT NOT NULL
                    )
                """)
                self._conn.execute("RELEASE SAVEPOINT migrate_v6")
            except sqlite3.Error:
                logger.exception("Migration v6 failed")
                self._conn.execute("ROLLBACK TO SAVEPOINT migrate_v6")
                raise
        if from_version < 7:
            logger.info("Migrating catalog schema v%d -> v7: adding shares table", from_version)
            self._conn.execute("SAVEPOINT migrate_v7")
            try:
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS shares (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                        token TEXT NOT NULL UNIQUE,
                        label TEXT NOT NULL DEFAULT '',
                        password_hash TEXT,
                        expires_at TEXT,
                        max_downloads INTEGER,
                        download_count INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL
                    )
                """)
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_shares_token ON shares(token)")
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_shares_file ON shares(file_id)")
                self._conn.execute("RELEASE SAVEPOINT migrate_v7")
            except sqlite3.Error:
                logger.exception("Migration v7 failed")
                self._conn.execute("ROLLBACK TO SAVEPOINT migrate_v7")
                raise
        if from_version < 8:
            logger.info("Migrating catalog schema v%d -> v8: adding quality scoring columns", from_version)
            self._conn.execute("SAVEPOINT migrate_v8")
            try:
                for col_name, col_type in [("quality_blur", "REAL"), ("quality_brightness", "REAL"), ("quality_category", "TEXT")]:
                    with contextlib.suppress(sqlite3.OperationalError):
                        # Safe: col_name/col_type are hardcoded constants above, not user input
                        self._conn.execute(f"ALTER TABLE files ADD COLUMN {col_name} {col_type}")  # noqa: S608
                self._conn.execute("RELEASE SAVEPOINT migrate_v8")
            except sqlite3.Error:
                logger.exception("Migration v8 failed")
                self._conn.execute("ROLLBACK TO SAVEPOINT migrate_v8")
                raise
        if from_version < 9:
            logger.info("Migrating catalog schema v%d -> v9: adding distributed backup tables", from_version)
            self._conn.execute("SAVEPOINT migrate_v9")
            try:
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS backup_targets (
                        remote_name TEXT PRIMARY KEY,
                        remote_path TEXT DEFAULT 'GML-Backup',
                        enabled INTEGER DEFAULT 1,
                        priority INTEGER DEFAULT 0,
                        total_bytes INTEGER DEFAULT 0,
                        used_bytes INTEGER DEFAULT 0,
                        free_bytes INTEGER DEFAULT 0,
                        last_probed_at TEXT
                    )
                """)
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS backup_manifest (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        file_id INTEGER NOT NULL,
                        path TEXT NOT NULL,
                        sha256 TEXT,
                        size INTEGER NOT NULL,
                        remote_name TEXT NOT NULL,
                        remote_path TEXT NOT NULL,
                        backed_up_at TEXT NOT NULL,
                        verified INTEGER DEFAULT 0,
                        verified_at TEXT,
                        UNIQUE(file_id, remote_name)
                    )
                """)
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_bm_file ON backup_manifest(file_id)")
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_bm_remote ON backup_manifest(remote_name)")
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_bm_sha ON backup_manifest(sha256)")
                self._conn.execute("RELEASE SAVEPOINT migrate_v9")
            except sqlite3.Error:
                logger.exception("Migration v9 failed")
                self._conn.execute("ROLLBACK TO SAVEPOINT migrate_v9")
                raise
        if from_version < 10:
            logger.info("Migrating catalog schema v%d -> v10: adding source_remote column + consolidation tables", from_version)
            self._conn.execute("SAVEPOINT migrate_v10")
            try:
                # Add source_remote to files table (tracks which remote/source a file came from)
                with contextlib.suppress(sqlite3.OperationalError):
                    self._conn.execute("ALTER TABLE files ADD COLUMN source_remote TEXT")
                # Consolidation tables (managed by checkpoint.py, created here for schema completeness)
                self._conn.execute("""
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
                    )
                """)
                self._conn.execute("""
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
                    )
                """)
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_cfs_job_step ON consolidation_file_state(job_id, step_name)")
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_cfs_status ON consolidation_file_state(status)")
                self._conn.execute("RELEASE SAVEPOINT migrate_v10")
            except sqlite3.Error:
                logger.exception("Migration v10 failed")
                self._conn.execute("ROLLBACK TO SAVEPOINT migrate_v10")
                raise
        if from_version < 11:
            logger.info("Migrating catalog schema v%d -> v11: adding encrypted/crypt_remote to backup_targets", from_version)
            self._conn.execute("SAVEPOINT migrate_v11")
            try:
                with contextlib.suppress(sqlite3.OperationalError):
                    self._conn.execute("ALTER TABLE backup_targets ADD COLUMN encrypted INTEGER DEFAULT 0")
                with contextlib.suppress(sqlite3.OperationalError):
                    self._conn.execute("ALTER TABLE backup_targets ADD COLUMN crypt_remote TEXT DEFAULT ''")
                self._conn.execute("RELEASE SAVEPOINT migrate_v11")
            except sqlite3.Error:
                logger.exception("Migration v11 failed")
                self._conn.execute("ROLLBACK TO SAVEPOINT migrate_v11")
                raise
        if from_version < 12:
            logger.info("Migrating catalog schema v%d -> v12: adding unique dup index + label indexes", from_version)
            self._conn.execute("SAVEPOINT migrate_v12")
            try:
                # Remove duplicate rows (keep MIN(rowid) per group_id+file_id)
                self._conn.execute("""
                    DELETE FROM duplicates WHERE rowid NOT IN (
                        SELECT MIN(rowid) FROM duplicates GROUP BY group_id, file_id
                    )
                """)
                self._conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dup_group_file ON duplicates(group_id, file_id)")
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_labels_people ON labels(people)")
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_labels_place ON labels(place)")
                self._conn.execute("RELEASE SAVEPOINT migrate_v12")
            except sqlite3.Error:
                logger.exception("Migration v12 failed")
                self._conn.execute("ROLLBACK TO SAVEPOINT migrate_v12")
                raise

    def _release_lock(self) -> None:
        """Release the advisory file lock if held."""
        if self._lock_fd is not None:
            try:
                import fcntl

                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            except ImportError:
                pass  # fcntl not available on Windows
            os.close(self._lock_fd)
            self._lock_fd = None

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
        self._release_lock()

    def __enter__(self) -> Catalog:
        self.open(exclusive=self._exclusive)
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Catalog is not open. Call open() or use as context manager.")
        return self._conn

    # ── File operations ──────────────────────────────────────────────

    def upsert_file(self, row: CatalogFileRow) -> int:
        """Insert or update a file record. Returns the row id."""
        now = utc_stamp()
        cur = self.conn.execute("SELECT id, first_seen FROM files WHERE path = ?", (row.path,))
        existing = cur.fetchone()

        media_cols = (
            row.duration_seconds,
            row.width,
            row.height,
            row.video_codec,
            row.audio_codec,
            row.bitrate,
            row.phash,
            row.date_original,
            row.camera_make,
            row.camera_model,
            row.gps_latitude,
            row.gps_longitude,
        )

        if existing:
            file_id = existing[0]
            first_seen = existing[1]
            self.conn.execute(
                """UPDATE files SET
                    size=?, mtime=?, ctime=?, birthtime=?, ext=?,
                    sha256=COALESCE(?, sha256),
                    inode=?, device=?, nlink=?, asset_key=?, asset_component=?,
                    xattr_count=?, first_seen=?, last_scanned=?,
                    duration_seconds=?, width=?, height=?, video_codec=?,
                    audio_codec=?, bitrate=?,
                    phash=COALESCE(?, phash),
                    date_original=COALESCE(?, date_original),
                    camera_make=COALESCE(?, camera_make),
                    camera_model=COALESCE(?, camera_model),
                    gps_latitude=COALESCE(?, gps_latitude),
                    gps_longitude=COALESCE(?, gps_longitude)
                WHERE id=?""",
                (
                    row.size,
                    row.mtime,
                    row.ctime,
                    row.birthtime,
                    row.ext,
                    row.sha256,
                    row.inode,
                    row.device,
                    row.nlink,
                    row.asset_key,
                    int(row.asset_component),
                    row.xattr_count,
                    first_seen,
                    now,
                    *media_cols,
                    file_id,
                ),
            )
            return file_id
        else:
            cur = self.conn.execute(
                """INSERT INTO files
                    (path, size, mtime, ctime, birthtime, ext, sha256,
                     inode, device, nlink, asset_key, asset_component,
                     xattr_count, first_seen, last_scanned,
                     duration_seconds, width, height, video_codec,
                     audio_codec, bitrate, phash, date_original,
                     camera_make, camera_model, gps_latitude, gps_longitude)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    row.path,
                    row.size,
                    row.mtime,
                    row.ctime,
                    row.birthtime,
                    row.ext,
                    row.sha256,
                    row.inode,
                    row.device,
                    row.nlink,
                    row.asset_key,
                    int(row.asset_component),
                    row.xattr_count,
                    now,
                    now,
                    *media_cols,
                ),
            )
            return cur.lastrowid  # type: ignore[return-value]

    def get_file_by_path(self, path: str) -> CatalogFileRow | None:
        cur = self.conn.execute("SELECT * FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        if row is None:
            return None
        return self._row_to_catalog_file(row)

    def get_files_by_paths(self, paths: list[str]) -> dict[str, CatalogFileRow]:
        """Batch-load multiple files by path in a single query. Returns path→row dict."""
        if not paths:
            return {}
        result: dict[str, CatalogFileRow] = {}
        # Process in chunks of 500 to avoid SQLite variable limit
        chunk_size = 500
        for i in range(0, len(paths), chunk_size):
            chunk = paths[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            cur = self.conn.execute(f"SELECT * FROM files WHERE path IN ({placeholders})", chunk)  # noqa: S608
            for row in cur.fetchall():
                file_row = self._row_to_catalog_file(row)
                result[file_row.path] = file_row
        return result

    def get_file_mtime_size(self, path: str) -> tuple[float, int] | None:
        """Fast lookup: returns (mtime, size) or None."""
        cur = self.conn.execute("SELECT mtime, size FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        return (row[0], row[1]) if row else None

    def get_all_mtime_size_for_root(self, root: str) -> dict[str, tuple[float, int, bool]]:
        """Batch lookup: returns {path: (mtime, size, has_hash)} for all files under *root*.

        Uses a prefix query (LIKE 'root%') so it works for any subtree.
        """
        prefix = root.rstrip("/") + "/"
        escaped_prefix = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        cur = self.conn.execute(
            "SELECT path, mtime, size, sha256 IS NOT NULL FROM files WHERE path LIKE ? || '%' ESCAPE '\\'",
            (escaped_prefix,),
        )
        return {row[0]: (row[1], row[2], bool(row[3])) for row in cur.fetchall()}

    def mark_removed(self, paths: list[str]) -> int:
        """Delete catalog entries for removed files. Returns count deleted.

        Also decrements nlink on remaining hardlinks sharing the same inode/device.
        """
        if not paths:
            return 0
        # Process in chunks to avoid SQLite variable limit (~32K)
        _CHUNK = 500
        inode_rows: list[tuple] = []
        deleted = 0
        for i in range(0, len(paths), _CHUNK):
            chunk = paths[i : i + _CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            inode_rows.extend(
                self.conn.execute(
                    f"SELECT DISTINCT inode, device FROM files WHERE path IN ({placeholders}) AND inode IS NOT NULL AND device IS NOT NULL",  # noqa: S608
                    chunk,
                ).fetchall()
            )
            cur = self.conn.execute(f"DELETE FROM files WHERE path IN ({placeholders})", chunk)  # noqa: S608
            deleted += cur.rowcount
        # Decrement nlink on remaining files that shared the same inode/device
        for inode, device in inode_rows:
            self.conn.execute(
                "UPDATE files SET nlink = MAX(nlink - 1, 1) WHERE inode = ? AND device = ? AND nlink > 1",
                (inode, device),
            )
        return deleted

    def delete_file_by_path(self, path: str) -> bool:
        """Remove a file entry from catalog. Returns True if found and deleted.

        Also decrements nlink on remaining hardlinks sharing the same inode/device.
        """
        # Look up inode/device before deletion
        inode_row = self.conn.execute(
            "SELECT inode, device FROM files WHERE path = ? AND inode IS NOT NULL AND device IS NOT NULL",
            (path,),
        ).fetchone()
        cur = self.conn.execute("DELETE FROM files WHERE path = ?", (path,))
        if cur.rowcount > 0 and inode_row is not None:
            inode, device = inode_row
            self.conn.execute(
                "UPDATE files SET nlink = MAX(nlink - 1, 1) WHERE inode = ? AND device = ? AND nlink > 1",
                (inode, device),
            )
            return True
        return cur.rowcount > 0

    def update_file_path(self, old_path: str, new_path: str) -> bool:
        """Update a file's path in catalog after rename/move. Returns True if found and updated."""
        cur = self.conn.execute("UPDATE files SET path = ? WHERE path = ?", (new_path, old_path))
        return cur.rowcount > 0

    def commit(self) -> None:
        self.conn.commit()

    def vacuum(self) -> None:
        """Run VACUUM to reclaim space and defragment the database."""
        logger.info("Running VACUUM on catalog %s", self._db_path)
        self.conn.execute("VACUUM")

    # ── File metadata (deep ExifTool) ─────────────────────────────────

    def upsert_file_metadata(self, path: str, raw_json: str) -> None:
        """Store full ExifTool metadata JSON for a file."""
        cur = self.conn.execute("SELECT id FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        if row is None:
            return
        file_id = row[0]
        now = utc_stamp()
        self.conn.execute(
            """INSERT INTO file_metadata (file_id, raw_json, extracted_at)
               VALUES (?, ?, ?)
               ON CONFLICT(file_id) DO UPDATE SET raw_json=excluded.raw_json, extracted_at=excluded.extracted_at""",
            (file_id, raw_json, now),
        )

    def get_file_metadata(self, path: str) -> dict | None:
        """Retrieve full ExifTool metadata dict for a file. Returns None if not available."""
        import json

        cur = self.conn.execute(
            "SELECT fm.raw_json FROM file_metadata fm JOIN files f ON fm.file_id = f.id WHERE f.path = ?",
            (path,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return None

    def get_metadata_richness(self, path: str) -> float | None:
        """Get metadata richness score for a file."""
        cur = self.conn.execute("SELECT metadata_richness FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        return float(row[0])

    def update_metadata_richness(self, path: str, score: float) -> None:
        """Update metadata richness score for a file."""
        self.conn.execute("UPDATE files SET metadata_richness = ? WHERE path = ?", (score, path))

    def get_group_metadata(self, group_id: str) -> list[tuple[str, dict]]:
        """Get full metadata for all files in a duplicate group."""
        import json

        cur = self.conn.execute(
            """SELECT f.path, fm.raw_json
               FROM duplicates d
               JOIN files f ON d.file_id = f.id
               LEFT JOIN file_metadata fm ON fm.file_id = f.id
               WHERE d.group_id = ?
               ORDER BY f.path""",
            (group_id,),
        )
        result = []
        for row in cur.fetchall():
            try:
                meta = json.loads(row[1]) if row[1] else {}
            except (json.JSONDecodeError, TypeError):
                meta = {}
            result.append((row[0], meta))
        return result

    def get_all_duplicate_group_ids(self) -> list[str]:
        """Return all unique duplicate group IDs."""
        cur = self.conn.execute("SELECT DISTINCT group_id FROM duplicates ORDER BY group_id")
        return [row[0] for row in cur.fetchall()]

    def get_duplicate_group_ids_for_paths(self, paths: list[str]) -> dict[str, str]:
        """Return a mapping of path -> group_id for paths that are in duplicate groups."""
        if not paths:
            return {}
        result: dict[str, str] = {}
        _CHUNK = 500
        for i in range(0, len(paths), _CHUNK):
            chunk = paths[i : i + _CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            cur = self.conn.execute(
                f"SELECT f.path, d.group_id FROM duplicates d JOIN files f ON d.file_id = f.id WHERE f.path IN ({placeholders})",  # noqa: S608
                chunk,
            )
            result.update({row[0]: row[1] for row in cur.fetchall()})
        return result

    def paths_without_metadata(self) -> list[str]:
        """Return paths of files that don't have ExifTool metadata extracted yet."""
        cur = self.conn.execute(
            "SELECT f.path FROM files f LEFT JOIN file_metadata fm ON fm.file_id = f.id WHERE fm.file_id IS NULL ORDER BY f.path"
        )
        return [row[0] for row in cur.fetchall()]

    # ── Scan tracking ────────────────────────────────────────────────

    def start_scan(self, root: str) -> int:
        cur = self.conn.execute(
            "INSERT INTO scans (root, started_at) VALUES (?, ?)",
            (root, utc_stamp()),
        )
        self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def finish_scan(self, scan_id: int, stats: ScanStats) -> None:
        self.conn.execute(
            """UPDATE scans SET
                finished_at=?, files_scanned=?, files_new=?, files_changed=?, files_removed=?
            WHERE id=?""",
            (utc_stamp(), stats.files_scanned, stats.files_new, stats.files_changed, stats.files_removed, scan_id),
        )
        self.conn.commit()

    # ── Duplicate tracking ───────────────────────────────────────────

    def upsert_duplicate_group(self, group_id: str, file_ids: list[int], primary_id: int | None = None) -> None:
        self.conn.execute("DELETE FROM duplicates WHERE group_id = ?", (group_id,))
        for fid in file_ids:
            is_primary = 1 if fid == primary_id else 0
            self.conn.execute(
                "INSERT INTO duplicates (group_id, file_id, is_primary) VALUES (?, ?, ?)",
                (group_id, fid, is_primary),
            )

    # ── Label operations ─────────────────────────────────────────────

    def upsert_label(self, file_id: int, people: str = "", place: str = "") -> None:
        self.conn.execute(
            """INSERT INTO labels (file_id, people, place, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(file_id) DO UPDATE SET
                   people=excluded.people, place=excluded.place, updated_at=excluded.updated_at""",
            (file_id, people, place, utc_stamp()),
        )

    # ── Tag operations ──────────────────────────────────────────────

    def get_all_tags(self) -> list[dict]:
        """Return all tags with file counts."""
        cur = self.conn.execute(
            "SELECT t.id, t.name, t.color, COUNT(ft.file_id) AS file_count "
            "FROM tags t LEFT JOIN file_tags ft ON t.id = ft.tag_id "
            "GROUP BY t.id ORDER BY t.name"
        )
        return [{"id": row[0], "name": row[1], "color": row[2], "file_count": row[3]} for row in cur.fetchall()]

    def get_file_tags(self, path: str) -> list[dict]:
        """Return tags for a specific file."""
        cur = self.conn.execute(
            "SELECT t.id, t.name, t.color FROM tags t "
            "JOIN file_tags ft ON t.id = ft.tag_id "
            "JOIN files f ON ft.file_id = f.id "
            "WHERE f.path = ? ORDER BY t.name",
            (path,),
        )
        return [{"id": row[0], "name": row[1], "color": row[2]} for row in cur.fetchall()]

    def get_files_tags_bulk(self, paths: list[str]) -> dict[str, list[dict]]:
        """Return tags for multiple files. Returns path -> list of tag dicts."""
        if not paths:
            return {}
        result: dict[str, list[dict]] = {}
        chunk_size = 500
        for i in range(0, len(paths), chunk_size):
            chunk = paths[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            cur = self.conn.execute(
                f"SELECT f.path, t.id, t.name, t.color FROM tags t "  # noqa: S608
                f"JOIN file_tags ft ON t.id = ft.tag_id "
                f"JOIN files f ON ft.file_id = f.id "
                f"WHERE f.path IN ({placeholders}) ORDER BY t.name",
                chunk,
            )
            for row in cur.fetchall():
                result.setdefault(row[0], []).append({"id": row[1], "name": row[2], "color": row[3]})
        return result

    def add_tag(self, name: str, color: str = "#58a6ff") -> dict:
        """Create a new tag. Returns the tag dict."""
        cur = self.conn.execute("INSERT INTO tags (name, color) VALUES (?, ?)", (name, color))
        self.conn.commit()
        return {"id": cur.lastrowid, "name": name, "color": color, "file_count": 0}

    def delete_tag(self, tag_id: int) -> None:
        """Delete a tag and all its file associations."""
        self.conn.execute("DELETE FROM file_tags WHERE tag_id = ?", (tag_id,))
        self.conn.execute("DELETE FROM tags WHERE id = ?", (tag_id,))
        self.conn.commit()

    def tag_file(self, path: str, tag_id: int) -> None:
        """Add a tag to a file."""
        cur = self.conn.execute("SELECT id FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        if row is None:
            return
        file_id = row[0]
        self.conn.execute(
            "INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?, ?)",
            (file_id, tag_id),
        )

    def untag_file(self, path: str, tag_id: int) -> None:
        """Remove a tag from a file."""
        cur = self.conn.execute("SELECT id FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        if row is None:
            return
        file_id = row[0]
        self.conn.execute(
            "DELETE FROM file_tags WHERE file_id = ? AND tag_id = ?",
            (file_id, tag_id),
        )

    def bulk_tag(self, paths: list[str], tag_id: int) -> int:
        """Add a tag to multiple files. Returns count of files tagged."""
        count = 0
        for path in paths:
            cur = self.conn.execute("SELECT id FROM files WHERE path = ?", (path,))
            row = cur.fetchone()
            if row is None:
                continue
            file_id = row[0]
            self.conn.execute(
                "INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?, ?)",
                (file_id, tag_id),
            )
            count += 1
        self.conn.commit()
        return count

    def bulk_untag(self, paths: list[str], tag_id: int) -> int:
        """Remove a tag from multiple files. Returns count of files untagged."""
        count = 0
        for path in paths:
            cur = self.conn.execute("SELECT id FROM files WHERE path = ?", (path,))
            row = cur.fetchone()
            if row is None:
                continue
            file_id = row[0]
            c = self.conn.execute(
                "DELETE FROM file_tags WHERE file_id = ? AND tag_id = ?",
                (file_id, tag_id),
            )
            count += c.rowcount
        self.conn.commit()
        return count

    def query_files_by_tag(self, tag_id: int, limit: int = 10000, offset: int = 0) -> list[CatalogFileRow]:
        """Return files that have a specific tag."""
        cur = self.conn.execute(
            "SELECT f.* FROM files f JOIN file_tags ft ON f.id = ft.file_id WHERE ft.tag_id = ? ORDER BY f.path LIMIT ? OFFSET ?",
            (tag_id, limit, offset),
        )
        return [self._row_to_catalog_file(row) for row in cur.fetchall()]

    # ── Share operations ─────────────────────────────────────────────

    def create_share(
        self,
        path: str,
        label: str = "",
        password: str | None = None,
        expires_hours: float | None = None,
        max_downloads: int | None = None,
    ) -> dict:
        """Create a share link for a file. Returns share dict with token."""
        import datetime as dt
        import hashlib as _hl
        import os as _os
        import secrets

        cur = self.conn.execute("SELECT id FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"File not found in catalog: {path}")
        file_id = row[0]

        token = secrets.token_hex(16)
        password_hash = None
        if password:
            salt = _os.urandom(16)
            dk = _hl.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 600_000)
            password_hash = salt.hex() + ":600000:" + dk.hex()

        expires_at = None
        if expires_hours is not None and expires_hours > 0:
            expires_at = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=expires_hours)).isoformat()

        now = utc_stamp()
        cur = self.conn.execute(
            """INSERT INTO shares (file_id, token, label, password_hash, expires_at, max_downloads, download_count, created_at)
               VALUES (?, ?, ?, ?, ?, ?, 0, ?)""",
            (file_id, token, label, password_hash, expires_at, max_downloads, now),
        )
        self.conn.commit()
        return {
            "id": cur.lastrowid,
            "file_id": file_id,
            "path": path,
            "token": token,
            "label": label,
            "has_password": password_hash is not None,
            "expires_at": expires_at,
            "max_downloads": max_downloads,
            "download_count": 0,
            "created_at": now,
        }

    def get_share_by_token(self, token: str) -> dict | None:
        """Look up a share by its token. Returns share info with file path, or None."""
        import datetime as dt

        cur = self.conn.execute(
            "SELECT s.id, s.file_id, s.token, s.label, s.password_hash, "
            "s.expires_at, s.max_downloads, s.download_count, s.created_at, f.path "
            "FROM shares s JOIN files f ON s.file_id = f.id WHERE s.token = ?",
            (token,),
        )
        row = cur.fetchone()
        if row is None:
            return None

        share = {
            "id": row[0],
            "file_id": row[1],
            "token": row[2],
            "label": row[3],
            "password_hash": row[4],
            "has_password": row[4] is not None,
            "expires_at": row[5],
            "max_downloads": row[6],
            "download_count": row[7],
            "created_at": row[8],
            "path": row[9],
        }

        # Check expiry
        if share["expires_at"]:
            try:
                exp = dt.datetime.fromisoformat(share["expires_at"])
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=dt.timezone.utc)
                if dt.datetime.now(dt.timezone.utc) > exp:
                    share["expired"] = True
            except (ValueError, TypeError):
                pass

        # Check max downloads
        if share["max_downloads"] is not None and share["download_count"] >= share["max_downloads"]:
            share["max_downloads_reached"] = True

        return share

    def get_shares_for_file(self, path: str) -> list[dict]:
        """Return all shares for a specific file."""
        cur = self.conn.execute("SELECT id FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        if row is None:
            return []
        file_id = row[0]
        cur = self.conn.execute(
            "SELECT id, file_id, token, label, password_hash, expires_at, "
            "max_downloads, download_count, created_at "
            "FROM shares WHERE file_id = ? ORDER BY created_at DESC",
            (file_id,),
        )
        return [
            {
                "id": r[0],
                "file_id": r[1],
                "token": r[2],
                "label": r[3],
                "has_password": r[4] is not None,
                "expires_at": r[5],
                "max_downloads": r[6],
                "download_count": r[7],
                "created_at": r[8],
                "path": path,
            }
            for r in cur.fetchall()
        ]

    def get_all_shares(self, limit: int = 100, offset: int = 0) -> list[dict]:
        """Return all shares with file info."""
        cur = self.conn.execute(
            "SELECT s.id, s.file_id, s.token, s.label, s.password_hash, "
            "s.expires_at, s.max_downloads, s.download_count, s.created_at, f.path "
            "FROM shares s JOIN files f ON s.file_id = f.id "
            "ORDER BY s.created_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
        return [
            {
                "id": r[0],
                "file_id": r[1],
                "token": r[2],
                "label": r[3],
                "has_password": r[4] is not None,
                "expires_at": r[5],
                "max_downloads": r[6],
                "download_count": r[7],
                "created_at": r[8],
                "path": r[9],
            }
            for r in cur.fetchall()
        ]

    def delete_share(self, share_id: int) -> None:
        """Delete a share link."""
        self.conn.execute("DELETE FROM shares WHERE id = ?", (share_id,))
        self.conn.commit()

    def increment_download(self, share_id: int) -> None:
        """Increment the download counter for a share."""
        self.conn.execute(
            "UPDATE shares SET download_count = download_count + 1 WHERE id = ?",
            (share_id,),
        )
        self.conn.commit()

    def cleanup_expired_shares(self) -> int:
        """Delete expired shares. Returns count deleted."""
        import datetime as dt

        now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
        cur = self.conn.execute(
            "DELETE FROM shares WHERE expires_at IS NOT NULL AND expires_at < ?",
            (now_iso,),
        )
        self.conn.commit()
        return cur.rowcount

    # ── Note operations ──────────────────────────────────────────────

    def get_file_note(self, path: str) -> tuple[str, str] | None:
        """Return (note, updated_at) for a file, or None."""
        cur = self.conn.execute(
            "SELECT fn.note, fn.updated_at FROM file_notes fn JOIN files f ON fn.file_id = f.id WHERE f.path = ?",
            (path,),
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else None

    def set_file_note(self, path: str, note: str) -> None:
        """Set or update a note for a file."""
        cur = self.conn.execute("SELECT id FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        if row is None:
            return
        file_id = row[0]
        now = utc_stamp()
        self.conn.execute(
            "INSERT INTO file_notes (file_id, note, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(file_id) DO UPDATE SET note=excluded.note, updated_at=excluded.updated_at",
            (file_id, note, now),
        )
        self.conn.commit()

    def delete_file_note(self, path: str) -> bool:
        """Remove a note from a file. Returns True if deleted."""
        cur = self.conn.execute(
            "DELETE FROM file_notes WHERE file_id = (SELECT id FROM files WHERE path = ?)",
            (path,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_files_notes_bulk(self, paths: list[str]) -> set[str]:
        """Return set of paths that have notes."""
        if not paths:
            return set()
        result: set[str] = set()
        chunk_size = 500
        for i in range(0, len(paths), chunk_size):
            chunk = paths[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            cur = self.conn.execute(
                f"SELECT f.path FROM file_notes fn JOIN files f ON fn.file_id = f.id "  # noqa: S608
                f"WHERE f.path IN ({placeholders})",
                chunk,
            )
            for row in cur.fetchall():
                result.add(row[0])
        return result

    # ── Rating operations ─────────────────────────────────────────────

    def get_file_rating(self, path: str) -> int | None:
        """Return rating (1-5) for a file, or None."""
        cur = self.conn.execute(
            "SELECT fr.rating FROM file_ratings fr JOIN files f ON fr.file_id = f.id WHERE f.path = ?",
            (path,),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def set_file_rating(self, path: str, rating: int) -> None:
        """Set a rating (1-5) for a file."""
        cur = self.conn.execute("SELECT id FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        if row is None:
            return
        file_id = row[0]
        self.conn.execute(
            "INSERT INTO file_ratings (file_id, rating) VALUES (?, ?) ON CONFLICT(file_id) DO UPDATE SET rating=excluded.rating",
            (file_id, rating),
        )
        self.conn.commit()

    def delete_file_rating(self, path: str) -> bool:
        """Remove a rating from a file. Returns True if deleted."""
        cur = self.conn.execute(
            "DELETE FROM file_ratings WHERE file_id = (SELECT id FROM files WHERE path = ?)",
            (path,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_files_ratings_bulk(self, paths: list[str]) -> dict[str, int]:
        """Return path -> rating dict for files that have ratings."""
        if not paths:
            return {}
        result: dict[str, int] = {}
        chunk_size = 500
        for i in range(0, len(paths), chunk_size):
            chunk = paths[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            cur = self.conn.execute(
                f"SELECT f.path, fr.rating FROM file_ratings fr "  # noqa: S608
                f"JOIN files f ON fr.file_id = f.id "
                f"WHERE f.path IN ({placeholders})",
                chunk,
            )
            for row in cur.fetchall():
                result[row[0]] = row[1]
        return result

    # ── Face / Person operations ────────────────────────────────────

    def insert_face(
        self,
        file_id: int,
        face_index: int,
        bbox: tuple[int, int, int, int],
        encoding_blob: bytes | None = None,
        cluster_id: int = -1,
        confidence: float | None = None,
    ) -> int:
        """Insert a detected face. bbox = (top, right, bottom, left). Returns face id."""
        now = utc_stamp()
        cur = self.conn.execute(
            """INSERT INTO faces
               (file_id, face_index, bbox_top, bbox_right, bbox_bottom, bbox_left,
                encoding, cluster_id, confidence, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (file_id, face_index, bbox[0], bbox[1], bbox[2], bbox[3], encoding_blob, cluster_id, confidence, now),
        )
        return cur.lastrowid  # type: ignore[return-value]

    def get_faces_for_file(self, file_id: int) -> list[dict]:
        cur = self.conn.execute(
            """SELECT f.id, f.face_index, f.person_id, f.bbox_top, f.bbox_right,
                      f.bbox_bottom, f.bbox_left, f.cluster_id, f.confidence,
                      p.name as person_name
               FROM faces f LEFT JOIN persons p ON f.person_id = p.id
               WHERE f.file_id = ? ORDER BY f.face_index""",
            (file_id,),
        )
        return [
            {
                "id": r[0],
                "face_index": r[1],
                "person_id": r[2],
                "bbox": {"top": r[3], "right": r[4], "bottom": r[5], "left": r[6]},
                "cluster_id": r[7],
                "confidence": r[8],
                "person_name": r[9] or "",
            }
            for r in cur.fetchall()
        ]

    def get_faces_for_person(self, person_id: int, limit: int = 100, offset: int = 0) -> list[dict]:
        cur = self.conn.execute(
            """SELECT f.id, f.file_id, f.face_index, f.bbox_top, f.bbox_right,
                      f.bbox_bottom, f.bbox_left, f.confidence, fi.path
               FROM faces f JOIN files fi ON f.file_id = fi.id
               WHERE f.person_id = ? ORDER BY f.id LIMIT ? OFFSET ?""",
            (person_id, limit, offset),
        )
        return [
            {
                "id": r[0],
                "file_id": r[1],
                "face_index": r[2],
                "bbox": {"top": r[3], "right": r[4], "bottom": r[5], "left": r[6]},
                "confidence": r[7],
                "path": r[8],
            }
            for r in cur.fetchall()
        ]

    def get_unidentified_faces(self, limit: int = 100, offset: int = 0) -> list[dict]:
        cur = self.conn.execute(
            """SELECT f.id, f.file_id, f.face_index, f.bbox_top, f.bbox_right,
                      f.bbox_bottom, f.bbox_left, f.confidence, f.cluster_id, fi.path
               FROM faces f JOIN files fi ON f.file_id = fi.id
               WHERE f.person_id IS NULL ORDER BY f.cluster_id, f.id LIMIT ? OFFSET ?""",
            (limit, offset),
        )
        return [
            {
                "id": r[0],
                "file_id": r[1],
                "face_index": r[2],
                "bbox": {"top": r[3], "right": r[4], "bottom": r[5], "left": r[6]},
                "confidence": r[7],
                "cluster_id": r[8],
                "path": r[9],
            }
            for r in cur.fetchall()
        ]

    def get_all_encodings(self) -> list[tuple[int, bytes]]:
        """Return all (face_id, encoding_blob) pairs where encoding is not NULL."""
        cur = self.conn.execute("SELECT id, encoding FROM faces WHERE encoding IS NOT NULL")
        return [(r[0], r[1]) for r in cur.fetchall()]

    def get_face_by_id(self, face_id: int) -> dict | None:
        cur = self.conn.execute(
            """SELECT f.id, f.file_id, f.face_index, f.person_id,
                      f.bbox_top, f.bbox_right, f.bbox_bottom, f.bbox_left,
                      f.confidence, fi.path, p.name as person_name
               FROM faces f
               JOIN files fi ON f.file_id = fi.id
               LEFT JOIN persons p ON f.person_id = p.id
               WHERE f.id = ?""",
            (face_id,),
        )
        r = cur.fetchone()
        if r is None:
            return None
        return {
            "id": r[0],
            "file_id": r[1],
            "face_index": r[2],
            "person_id": r[3],
            "bbox": {"top": r[4], "right": r[5], "bottom": r[6], "left": r[7]},
            "confidence": r[8],
            "path": r[9],
            "person_name": r[10] or "",
        }

    def assign_face_to_person(self, face_id: int, person_id: int) -> None:
        self.conn.execute("UPDATE faces SET person_id = ? WHERE id = ?", (person_id, face_id))
        self._refresh_person_counts([person_id])

    def set_face_cluster(self, face_id: int, cluster_id: int) -> None:
        self.conn.execute("UPDATE faces SET cluster_id = ? WHERE id = ?", (cluster_id, face_id))

    # ── Quality scoring ──

    def update_quality(self, file_id: int, blur: float, brightness: float, category: str) -> None:
        """Update quality scoring columns for a file."""
        self.conn.execute(
            "UPDATE files SET quality_blur = ?, quality_brightness = ?, quality_category = ? WHERE id = ?",
            (blur, brightness, category, file_id),
        )

    def files_without_quality(self, exts: set[str] | None = None) -> list[tuple[int, str, int | None, int | None, int, str | None]]:
        """Return (file_id, path, width, height, size, camera_make) for image files without quality analysis."""
        if exts is None:
            exts = {"jpg", "jpeg", "png", "bmp", "tif", "tiff", "webp", "heic", "heif", "gif"}
        placeholders = ",".join("?" for _ in exts)
        cur = self.conn.execute(
            f"SELECT id, path, width, height, size, camera_make FROM files "  # noqa: S608
            f"WHERE ext IN ({placeholders}) AND quality_category IS NULL ORDER BY path",
            list(exts),
        )
        return [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in cur.fetchall()]

    def files_without_faces(self, exts: set[str] | None = None) -> list[tuple[int, str]]:
        """Return (file_id, path) for image files that have no faces detected yet."""
        if exts is None:
            exts = {"jpg", "jpeg", "png", "bmp", "tif", "tiff", "webp", "heic", "heif"}
        placeholders = ",".join("?" for _ in exts)
        cur = self.conn.execute(
            f"SELECT f.id, f.path FROM files f "  # noqa: S608
            f"LEFT JOIN faces fa ON f.id = fa.file_id "
            f"WHERE f.ext IN ({placeholders}) AND fa.id IS NULL ORDER BY f.path",
            list(exts),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]

    # ── Person CRUD ──

    def upsert_person(self, name: str, sample_face_id: int | None = None) -> int:
        """Find existing person by name (case-insensitive) or create new. Returns person id."""
        # Check for existing person with same name
        row = self.conn.execute(
            "SELECT id FROM persons WHERE LOWER(name) = LOWER(?)", (name,)
        ).fetchone()
        if row:
            return row[0]
        now = utc_stamp()
        cur = self.conn.execute(
            "INSERT INTO persons (name, sample_face_id, face_count, created_at, updated_at) VALUES (?, ?, 0, ?, ?)",
            (name, sample_face_id, now, now),
        )
        return cur.lastrowid  # type: ignore[return-value]

    def get_all_persons(self) -> list[dict]:
        cur = self.conn.execute(
            """SELECT p.id, p.name, p.sample_face_id, p.face_count, p.created_at, p.updated_at
               FROM persons p ORDER BY p.face_count DESC, p.name"""
        )
        return [
            {"id": r[0], "name": r[1], "sample_face_id": r[2], "face_count": r[3], "created_at": r[4], "updated_at": r[5]}
            for r in cur.fetchall()
        ]

    def get_person(self, person_id: int) -> dict | None:
        cur = self.conn.execute(
            "SELECT id, name, sample_face_id, face_count, created_at, updated_at FROM persons WHERE id = ?",
            (person_id,),
        )
        r = cur.fetchone()
        if r is None:
            return None
        return {"id": r[0], "name": r[1], "sample_face_id": r[2], "face_count": r[3], "created_at": r[4], "updated_at": r[5]}

    def update_person_name(self, person_id: int, name: str) -> None:
        self.conn.execute(
            "UPDATE persons SET name = ?, updated_at = ? WHERE id = ?",
            (name, utc_stamp(), person_id),
        )

    def merge_persons(self, keep_id: int, merge_ids: list[int]) -> int:
        """Merge other persons into keep_id. Returns number of faces reassigned."""
        if not merge_ids:
            return 0
        placeholders = ",".join("?" for _ in merge_ids)
        cur = self.conn.execute(
            f"UPDATE faces SET person_id = ? WHERE person_id IN ({placeholders})",  # noqa: S608
            [keep_id, *merge_ids],
        )
        reassigned = cur.rowcount
        self.conn.execute(
            f"DELETE FROM persons WHERE id IN ({placeholders})",  # noqa: S608
            merge_ids,
        )
        self._refresh_person_counts([keep_id])
        return reassigned

    def delete_person(self, person_id: int) -> None:
        """Delete a person. Faces become unidentified."""
        self.conn.execute("UPDATE faces SET person_id = NULL WHERE person_id = ?", (person_id,))
        self.conn.execute("DELETE FROM persons WHERE id = ?", (person_id,))

    def _refresh_person_counts(self, person_ids: list[int] | None = None) -> None:
        """Recalculate face_count for given persons (or all)."""
        if person_ids:
            for pid in person_ids:
                cnt = self.conn.execute("SELECT COUNT(*) FROM faces WHERE person_id = ?", (pid,)).fetchone()[0]
                self.conn.execute("UPDATE persons SET face_count = ? WHERE id = ?", (cnt, pid))
                # Update sample face if needed
                sample = self.conn.execute("SELECT id FROM faces WHERE person_id = ? LIMIT 1", (pid,)).fetchone()
                if sample:
                    self.conn.execute(
                        "UPDATE persons SET sample_face_id = ? WHERE id = ? "
                        "AND (sample_face_id IS NULL "
                        "OR sample_face_id NOT IN (SELECT id FROM faces WHERE person_id = ?))",
                        (sample[0], pid, pid),
                    )
        else:
            self.conn.execute("""
                UPDATE persons SET face_count = (
                    SELECT COUNT(*) FROM faces WHERE faces.person_id = persons.id
                )
            """)

    def face_stats(self) -> dict:
        """Face/person counts for dashboard."""
        conn = self.conn
        total_faces = conn.execute("SELECT COUNT(*) FROM faces").fetchone()[0]
        total_persons = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        identified = conn.execute("SELECT COUNT(*) FROM faces WHERE person_id IS NOT NULL").fetchone()[0]
        unidentified = total_faces - identified
        named_persons = conn.execute("SELECT COUNT(*) FROM persons WHERE name != ''").fetchone()[0]
        return {
            "total_faces": total_faces,
            "total_persons": total_persons,
            "identified_faces": identified,
            "unidentified_faces": unidentified,
            "named_persons": named_persons,
        }

    # ── Face privacy ──

    def set_privacy_flag(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO face_privacy (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def get_privacy_flag(self, key: str) -> str | None:
        cur = self.conn.execute("SELECT value FROM face_privacy WHERE key = ?", (key,))
        r = cur.fetchone()
        return r[0] if r else None

    def wipe_face_encodings(self) -> int:
        """NULL out all face encodings for privacy. Returns count affected."""
        cur = self.conn.execute("UPDATE faces SET encoding = NULL WHERE encoding IS NOT NULL")
        self.conn.commit()
        return cur.rowcount

    # ── Query operations ─────────────────────────────────────────────

    def query_files(
        self,
        *,
        ext: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
        path_contains: str | None = None,
        has_sha256: bool | None = None,
        camera: str | None = None,
        min_duration: float | None = None,
        max_duration: float | None = None,
        min_width: int | None = None,
        has_gps: bool | None = None,
        has_phash: bool | None = None,
        quality_category: str | None = None,
        sort: str | None = None,
        order: str | None = None,
        limit: int = 10000,
        offset: int = 0,
    ) -> list[CatalogFileRow]:
        conditions: list[str] = []
        params: list[object] = []

        if ext is not None:
            ext_list = [e.strip().lower().lstrip(".") for e in ext.split(",") if e.strip()]
            if len(ext_list) == 1:
                conditions.append("ext = ?")
                params.append(ext_list[0])
            elif ext_list:
                placeholders = ",".join("?" for _ in ext_list)
                conditions.append(f"ext IN ({placeholders})")
                params.extend(ext_list)
        if date_from is not None:
            conditions.append("birthtime >= ?")
            params.append(_date_to_timestamp(date_from))
        if date_to is not None:
            conditions.append("birthtime <= ?")
            params.append(_date_to_timestamp(date_to) + 86400)  # end of day
        if min_size is not None:
            conditions.append("size >= ?")
            params.append(min_size)
        if max_size is not None:
            conditions.append("size <= ?")
            params.append(max_size)
        if path_contains is not None:
            escaped_contains = path_contains.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            conditions.append("path LIKE ? ESCAPE '\\'")
            params.append(f"%{escaped_contains}%")
        if has_sha256 is True:
            conditions.append("sha256 IS NOT NULL")
        elif has_sha256 is False:
            conditions.append("sha256 IS NULL")
        if camera is not None:
            escaped_camera = camera.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            conditions.append("(camera_make LIKE ? ESCAPE '\\' OR camera_model LIKE ? ESCAPE '\\')")
            params.extend([f"%{escaped_camera}%", f"%{escaped_camera}%"])
        if min_duration is not None:
            conditions.append("duration_seconds >= ?")
            params.append(min_duration)
        if max_duration is not None:
            conditions.append("duration_seconds <= ?")
            params.append(max_duration)
        if min_width is not None:
            conditions.append("width >= ?")
            params.append(min_width)
        if has_gps is True:
            conditions.append("gps_latitude IS NOT NULL")
        elif has_gps is False:
            conditions.append("gps_latitude IS NULL")
        if has_phash is True:
            conditions.append("phash IS NOT NULL")
        elif has_phash is False:
            conditions.append("phash IS NULL")
        if quality_category is not None:
            conditions.append("quality_category = ?")
            params.append(quality_category)

        where = " AND ".join(conditions) if conditions else "1=1"

        # Dynamic sort — whitelist columns to prevent SQL injection
        _SORT_MAP = {
            "date": "COALESCE(date_original, birthtime, mtime)",
            "name": "path",
            "size": "size",
            "ext": "ext",
            "rating": "metadata_richness",
            "path": "path",
        }
        sort_col = _SORT_MAP.get(sort or "", "COALESCE(date_original, birthtime, mtime)")
        sort_dir = "ASC" if (order or "").lower() == "asc" else "DESC"

        sql = f"SELECT * FROM files WHERE {where} ORDER BY {sort_col} {sort_dir} LIMIT ? OFFSET ?"  # noqa: S608
        params.append(limit)
        params.append(offset)

        cur = self.conn.execute(sql, params)
        return [self._row_to_catalog_file(row) for row in cur.fetchall()]

    def query_duplicates(self) -> list[tuple[str, list[CatalogFileRow]]]:
        """Return duplicate groups with their file rows."""
        cur = self.conn.execute(
            """SELECT d.group_id, f.*
               FROM duplicates d JOIN files f ON d.file_id = f.id
               ORDER BY d.group_id, f.path"""
        )
        groups: dict[str, list[CatalogFileRow]] = {}
        for row in cur.fetchall():
            group_id = row[0]
            file_row = self._row_to_catalog_file(row[1:])
            groups.setdefault(group_id, []).append(file_row)
        return list(groups.items())

    def get_all_phashes(self) -> dict[str, str]:
        """Return dict of path → phash for all files with a phash."""
        cur = self.conn.execute("SELECT path, phash FROM files WHERE phash IS NOT NULL")
        return {row[0]: row[1] for row in cur.fetchall()}

    def stats(self) -> dict[str, object]:
        """Return library statistics from catalog."""
        conn = self.conn
        total_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        total_size = conn.execute("SELECT COALESCE(SUM(size), 0) FROM files").fetchone()[0]
        hashed_files = conn.execute("SELECT COUNT(*) FROM files WHERE sha256 IS NOT NULL").fetchone()[0]
        dup_groups = conn.execute("SELECT COUNT(DISTINCT group_id) FROM duplicates").fetchone()[0]
        dup_files = conn.execute("SELECT COUNT(*) FROM duplicates").fetchone()[0]
        labeled_files = conn.execute("SELECT COUNT(*) FROM labels WHERE people != '' OR place != ''").fetchone()[0]
        scans = conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0]
        last_scan = conn.execute("SELECT MAX(finished_at) FROM scans").fetchone()[0]
        phashed_files = conn.execute("SELECT COUNT(*) FROM files WHERE phash IS NOT NULL").fetchone()[0]
        media_probed = conn.execute("SELECT COUNT(*) FROM files WHERE duration_seconds IS NOT NULL OR width IS NOT NULL").fetchone()[0]
        gps_files = conn.execute("SELECT COUNT(*) FROM files WHERE gps_latitude IS NOT NULL").fetchone()[0]

        ext_counts = []
        for row in conn.execute("SELECT ext, COUNT(*) as cnt FROM files GROUP BY ext ORDER BY cnt DESC LIMIT 20"):
            ext_counts.append([row[0] or "(noext)", row[1]])

        camera_counts = []
        cam_sql = (
            "SELECT camera_model, COUNT(*) as cnt FROM files "
            "WHERE camera_model IS NOT NULL GROUP BY camera_model ORDER BY cnt DESC LIMIT 10"
        )
        for row in conn.execute(cam_sql):
            camera_counts.append([row[0], row[1]])

        # Last scan root for pipeline re-use
        last_scan_root_row = conn.execute("SELECT root FROM scans ORDER BY id DESC LIMIT 1").fetchone()
        last_scan_root = last_scan_root_row[0] if last_scan_root_row else ""

        # Face stats (safe — tables may not exist in older catalogs)
        try:
            total_faces = conn.execute("SELECT COUNT(*) FROM faces").fetchone()[0]
            total_persons = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        except (sqlite3.OperationalError, sqlite3.DatabaseError):
            total_faces = 0
            total_persons = 0

        return {
            "total_files": total_files,
            "total_size_bytes": total_size,
            "hashed_files": hashed_files,
            "phashed_files": phashed_files,
            "media_probed": media_probed,
            "gps_files": gps_files,
            "duplicate_groups": dup_groups,
            "duplicate_files": dup_files,
            "labeled_files": labeled_files,
            "total_scans": scans,
            "last_scan": last_scan,
            "last_scan_root": last_scan_root,
            "top_extensions": ext_counts,
            "top_cameras": camera_counts,
            "total_faces": total_faces,
            "total_persons": total_persons,
        }

    def all_paths(self) -> set[str]:
        """Return all file paths currently in catalog."""
        cur = self.conn.execute("SELECT path FROM files")
        return {row[0] for row in cur.fetchall()}

    # ── Export / Import ──────────────────────────────────────────────

    def export_inventory_tsv(self, out_path: Path) -> int:
        """Export catalog to TSV matching audit file_inventory format. Returns row count."""
        from .utils import write_tsv

        header = [
            "path",
            "size",
            "mtime",
            "ctime",
            "birthtime",
            "ext",
            "meaningful_xattr_count",
            "asset_key",
            "asset_component",
        ]
        cur = self.conn.execute("SELECT * FROM files ORDER BY path")
        rows = []
        for db_row in cur.fetchall():
            f = self._row_to_catalog_file(db_row)
            rows.append(
                (
                    f.path,
                    f.size,
                    f"{f.mtime:.6f}",
                    f"{f.ctime:.6f}",
                    "" if f.birthtime is None else f"{f.birthtime:.6f}",
                    f.ext,
                    f.xattr_count,
                    f.asset_key or "",
                    int(f.asset_component),
                )
            )
        write_tsv(out_path, header, rows)
        return len(rows)

    def import_from_inventory_tsv(self, inventory_path: Path) -> int:
        """Import from audit file_inventory.tsv. Returns imported count."""
        from .utils import read_tsv_dict

        rows = read_tsv_dict(inventory_path)
        now = utc_stamp()
        count = 0
        for row in rows:
            birth_str = row.get("birthtime", "")
            cf = CatalogFileRow(
                id=None,
                path=row["path"],
                size=int(row["size"]),
                mtime=float(row["mtime"]),
                ctime=float(row["ctime"]),
                birthtime=float(birth_str) if birth_str else None,
                ext=row.get("ext", ""),
                sha256=None,
                inode=None,
                device=None,
                nlink=1,
                asset_key=row.get("asset_key") or None,
                asset_component=bool(int(row.get("asset_component", "0"))),
                xattr_count=int(row.get("meaningful_xattr_count", "0")),
                first_seen=now,
                last_scanned=now,
            )
            self.upsert_file(cf)
            count += 1
        self.commit()
        return count

    # ── Internal ─────────────────────────────────────────────────────

    @staticmethod
    def _row_to_catalog_file(row: tuple) -> CatalogFileRow:
        return CatalogFileRow(
            id=row[0],
            path=row[1],
            size=row[2],
            mtime=row[3],
            ctime=row[4],
            birthtime=row[5],
            ext=row[6],
            sha256=row[7],
            inode=row[8],
            device=row[9],
            nlink=row[10],
            asset_key=row[11],
            asset_component=bool(row[12]),
            xattr_count=row[13],
            first_seen=row[14],
            last_scanned=row[15],
            duration_seconds=row[16] if len(row) > 16 else None,
            width=row[17] if len(row) > 17 else None,
            height=row[18] if len(row) > 18 else None,
            video_codec=row[19] if len(row) > 19 else None,
            audio_codec=row[20] if len(row) > 20 else None,
            bitrate=row[21] if len(row) > 21 else None,
            phash=row[22] if len(row) > 22 else None,
            date_original=row[23] if len(row) > 23 else None,
            camera_make=row[24] if len(row) > 24 else None,
            camera_model=row[25] if len(row) > 25 else None,
            gps_latitude=row[26] if len(row) > 26 else None,
            gps_longitude=row[27] if len(row) > 27 else None,
            metadata_richness=row[28] if len(row) > 28 else None,
        )


def default_catalog_path() -> Path:
    """Default catalog location: ~/.config/gml/catalog.db"""
    return Path.home() / ".config" / "gml" / "catalog.db"


def _date_to_timestamp(date_str: str) -> float:
    """Convert YYYY-MM-DD to Unix timestamp (UTC)."""
    import datetime as dt

    d = dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    return d.timestamp()
