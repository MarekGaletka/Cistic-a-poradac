"""Persistent SQLite catalog for GOD MODE Media Library."""

from __future__ import annotations

import contextlib
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .utils import utc_stamp

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 3

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
    metadata_richness REAL
);

CREATE INDEX IF NOT EXISTS idx_files_sha256    ON files(sha256);
CREATE INDEX IF NOT EXISTS idx_files_ext       ON files(ext);
CREATE INDEX IF NOT EXISTS idx_files_size      ON files(size);
CREATE INDEX IF NOT EXISTS idx_files_phash     ON files(phash);
CREATE INDEX IF NOT EXISTS idx_files_richness  ON files(metadata_richness);

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

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    @property
    def db_path(self) -> Path:
        return self._db_path

    def open(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA_SQL)
        # Schema version management and migration
        cur = self._conn.execute("SELECT value FROM meta WHERE key='schema_version'")
        row = cur.fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO meta (key, value) VALUES ('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            self._conn.commit()
        else:
            current_version = int(row[0])
            if current_version < SCHEMA_VERSION:
                self._migrate(current_version)
                self._conn.execute(
                    "UPDATE meta SET value = ? WHERE key = 'schema_version'",
                    (str(SCHEMA_VERSION),),
                )
                self._conn.commit()

    def _migrate(self, from_version: int) -> None:
        """Apply schema migrations from from_version to SCHEMA_VERSION."""
        assert self._conn is not None
        if from_version < 2:
            logger.info("Migrating catalog schema v%d → v2: adding media metadata columns", from_version)
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
                with contextlib.suppress(Exception):
                    self._conn.execute(f"ALTER TABLE files ADD COLUMN {col_name} {col_type}")  # noqa: S608
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_files_phash ON files(phash)")
            self._conn.commit()
        if from_version < 3:
            logger.info("Migrating catalog schema v%d → v3: adding metadata richness and file_metadata table", from_version)
            with contextlib.suppress(Exception):
                self._conn.execute("ALTER TABLE files ADD COLUMN metadata_richness REAL")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_files_richness ON files(metadata_richness)")
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS file_metadata (
                    file_id      INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
                    raw_json     TEXT    NOT NULL,
                    extracted_at TEXT    NOT NULL
                )
            """)
            self._conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> Catalog:
        self.open()
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
            row.duration_seconds, row.width, row.height, row.video_codec,
            row.audio_codec, row.bitrate, row.phash, row.date_original,
            row.camera_make, row.camera_model, row.gps_latitude, row.gps_longitude,
        )

        if existing:
            file_id = existing[0]
            first_seen = existing[1]
            self.conn.execute(
                """UPDATE files SET
                    size=?, mtime=?, ctime=?, birthtime=?, ext=?, sha256=?,
                    inode=?, device=?, nlink=?, asset_key=?, asset_component=?,
                    xattr_count=?, first_seen=?, last_scanned=?,
                    duration_seconds=?, width=?, height=?, video_codec=?,
                    audio_codec=?, bitrate=?, phash=?, date_original=?,
                    camera_make=?, camera_model=?, gps_latitude=?, gps_longitude=?
                WHERE id=?""",
                (
                    row.size, row.mtime, row.ctime, row.birthtime, row.ext, row.sha256,
                    row.inode, row.device, row.nlink, row.asset_key, int(row.asset_component),
                    row.xattr_count, first_seen, now,
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
                    row.path, row.size, row.mtime, row.ctime, row.birthtime, row.ext, row.sha256,
                    row.inode, row.device, row.nlink, row.asset_key, int(row.asset_component),
                    row.xattr_count, now, now,
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

    def get_file_mtime_size(self, path: str) -> tuple[float, int] | None:
        """Fast lookup: returns (mtime, size) or None."""
        cur = self.conn.execute("SELECT mtime, size FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        return (row[0], row[1]) if row else None

    def mark_removed(self, paths: list[str]) -> int:
        """Delete catalog entries for removed files. Returns count deleted."""
        if not paths:
            return 0
        placeholders = ",".join("?" for _ in paths)
        cur = self.conn.execute(f"DELETE FROM files WHERE path IN ({placeholders})", paths)  # noqa: S608
        return cur.rowcount

    def commit(self) -> None:
        self.conn.commit()

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
        limit: int = 10000,
        offset: int = 0,
    ) -> list[CatalogFileRow]:
        conditions: list[str] = []
        params: list[object] = []

        if ext is not None:
            conditions.append("ext = ?")
            params.append(ext.lower().lstrip("."))
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
            conditions.append("path LIKE ?")
            params.append(f"%{path_contains}%")
        if has_sha256 is True:
            conditions.append("sha256 IS NOT NULL")
        elif has_sha256 is False:
            conditions.append("sha256 IS NULL")
        if camera is not None:
            conditions.append("(camera_make LIKE ? OR camera_model LIKE ?)")
            params.extend([f"%{camera}%", f"%{camera}%"])
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

        where = " AND ".join(conditions) if conditions else "1=1"
        sql = f"SELECT * FROM files WHERE {where} ORDER BY path LIMIT ? OFFSET ?"  # noqa: S608
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

        ext_counts = {}
        for row in conn.execute("SELECT ext, COUNT(*) as cnt FROM files GROUP BY ext ORDER BY cnt DESC LIMIT 20"):
            ext_counts[row[0] or "(noext)"] = row[1]

        camera_counts = {}
        cam_sql = (
            "SELECT camera_model, COUNT(*) as cnt FROM files "
            "WHERE camera_model IS NOT NULL GROUP BY camera_model ORDER BY cnt DESC LIMIT 10"
        )
        for row in conn.execute(cam_sql):
            camera_counts[row[0]] = row[1]

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
            "top_extensions": ext_counts,
            "top_cameras": camera_counts,
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
            "path", "size", "mtime", "ctime", "birthtime", "ext",
            "meaningful_xattr_count", "asset_key", "asset_component",
        ]
        cur = self.conn.execute("SELECT * FROM files ORDER BY path")
        rows = []
        for db_row in cur.fetchall():
            f = self._row_to_catalog_file(db_row)
            rows.append((
                f.path, f.size, f"{f.mtime:.6f}", f"{f.ctime:.6f}",
                "" if f.birthtime is None else f"{f.birthtime:.6f}",
                f.ext, f.xattr_count, f.asset_key or "", int(f.asset_component),
            ))
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
        )


def default_catalog_path() -> Path:
    """Default catalog location: ~/.config/gml/catalog.db"""
    return Path.home() / ".config" / "gml" / "catalog.db"


def _date_to_timestamp(date_str: str) -> float:
    """Convert YYYY-MM-DD to Unix timestamp."""
    import datetime as dt

    d = dt.datetime.strptime(date_str, "%Y-%m-%d")
    return d.timestamp()
