"""Tests for backup route endpoints (web/routes/backup.py).

Targets coverage improvement from ~67% to 85%+.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

try:
    from fastapi.testclient import TestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

from godmode_media_library.catalog import Catalog

pytestmark = pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi not installed")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_db(tmp_path):
    db_path = tmp_path / "backup_test.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()
    return db_path


@pytest.fixture
def catalog_db_with_files(tmp_path):
    """Catalog with sample files for backup stats tests."""
    db_path = tmp_path / "backup_files_test.db"
    cat = Catalog(db_path)
    cat.open()
    conn = cat._conn
    now_iso = "2024-01-15T12:00:00+00:00"
    for i in range(5):
        conn.execute(
            "INSERT INTO files (path, size, ext, sha256, mtime, ctime, first_seen, last_scanned) "
            "VALUES (?, ?, '.jpg', ?, 1704067200.0, 1704067200.0, ?, ?)",
            (f"/photos/img{i:03d}.jpg", 1024 * (i + 1), f"{'a' * 63}{i}", now_iso, now_iso),
        )
    conn.commit()
    cat.close()
    return db_path


@pytest.fixture
def client(catalog_db):
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_db)
    return TestClient(app)


@pytest.fixture
def client_with_files(catalog_db_with_files):
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_db_with_files)
    return TestClient(app)


# ---------------------------------------------------------------------------
# Helper: mock BackupStats dataclass
# ---------------------------------------------------------------------------


def _mock_backup_stats(**overrides):
    defaults = dict(
        total_files_in_catalog=100,
        backed_up_files=80,
        not_backed_up=20,
        backup_coverage_pct=80.0,
        total_backup_size=1024000,
        remotes_used=2,
        remotes_healthy=2,
        last_backup_at="2024-01-15T12:00:00",
        files_by_remote={"gdrive": 50, "s3": 30},
    )
    defaults.update(overrides)
    m = MagicMock()
    for k, v in defaults.items():
        setattr(m, k, v)
    return m


def _mock_backup_target(name="gdrive", **overrides):
    defaults = dict(
        remote_name=name,
        remote_path=f"{name}:backup",
        enabled=True,
        priority=1,
        total_bytes=1000000000,
        used_bytes=500000000,
        free_bytes=500000000,
        available_bytes=500000000,
        encrypted=False,
        crypt_remote=None,
    )
    defaults.update(overrides)
    m = MagicMock()
    for k, v in defaults.items():
        setattr(m, k, v)
    return m


# ---------------------------------------------------------------------------
# GET /api/backup/stats
# ---------------------------------------------------------------------------


class TestBackupStats:
    def test_backup_stats(self, client):
        mock_stats = _mock_backup_stats()
        with (
            patch(
                "godmode_media_library.distributed_backup.get_backup_stats",
                return_value=mock_stats,
            ),
            patch(
                "godmode_media_library.distributed_backup.ensure_backup_tables",
            ),
        ):
            resp = client.get("/api/backup/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_files"] == 100
        assert data["backed_up"] == 80
        assert data["coverage_pct"] == 80.0
        assert data["remotes_used"] == 2

    def test_backup_stats_empty(self, client):
        mock_stats = _mock_backup_stats(
            total_files_in_catalog=0,
            backed_up_files=0,
            not_backed_up=0,
            backup_coverage_pct=0.0,
            total_backup_size=0,
            remotes_used=0,
            remotes_healthy=0,
            last_backup_at=None,
            files_by_remote={},
        )
        with (
            patch(
                "godmode_media_library.distributed_backup.get_backup_stats",
                return_value=mock_stats,
            ),
            patch(
                "godmode_media_library.distributed_backup.ensure_backup_tables",
            ),
        ):
            resp = client.get("/api/backup/stats")
        assert resp.status_code == 200
        assert resp.json()["total_files"] == 0


# ---------------------------------------------------------------------------
# GET /api/backup/targets
# ---------------------------------------------------------------------------


class TestBackupTargets:
    def test_backup_targets(self, client):
        targets = [_mock_backup_target("gdrive"), _mock_backup_target("s3")]
        with (
            patch(
                "godmode_media_library.distributed_backup.get_targets",
                return_value=targets,
            ),
            patch(
                "godmode_media_library.distributed_backup.ensure_backup_tables",
            ),
        ):
            resp = client.get("/api/backup/targets")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["targets"]) == 2
        assert data["targets"][0]["remote_name"] == "gdrive"

    def test_backup_targets_empty(self, client):
        with (
            patch(
                "godmode_media_library.distributed_backup.get_targets",
                return_value=[],
            ),
            patch(
                "godmode_media_library.distributed_backup.ensure_backup_tables",
            ),
        ):
            resp = client.get("/api/backup/targets")
        assert resp.status_code == 200
        assert resp.json()["targets"] == []


# ---------------------------------------------------------------------------
# POST /api/backup/probe
# ---------------------------------------------------------------------------


class TestBackupProbe:
    def test_probe_targets(self, client):
        targets = [_mock_backup_target("gdrive")]
        with patch(
            "godmode_media_library.distributed_backup.probe_targets",
            return_value=targets,
        ):
            resp = client.post("/api/backup/probe")
        assert resp.status_code == 200
        data = resp.json()
        assert data["probed"] == 1
        assert data["targets"][0]["remote_name"] == "gdrive"


# ---------------------------------------------------------------------------
# PUT /api/backup/targets/{remote_name}
# ---------------------------------------------------------------------------


class TestUpdateBackupTarget:
    def test_update_target_enabled(self, client):
        with (
            patch(
                "godmode_media_library.distributed_backup.ensure_backup_tables",
            ),
            patch(
                "godmode_media_library.distributed_backup.set_target_enabled",
            ) as mock_enable,
        ):
            resp = client.put("/api/backup/targets/gdrive", json={"enabled": False})
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        mock_enable.assert_called_once()

    def test_update_target_priority(self, client):
        with (
            patch(
                "godmode_media_library.distributed_backup.ensure_backup_tables",
            ),
            patch(
                "godmode_media_library.distributed_backup.set_target_priority",
            ) as mock_prio,
        ):
            resp = client.put("/api/backup/targets/gdrive", json={"priority": 5})
        assert resp.status_code == 200
        mock_prio.assert_called_once()

    def test_update_target_bytes(self, client, catalog_db):
        """Update total_bytes and free_bytes directly."""
        # Ensure backup_targets table exists first
        cat = Catalog(catalog_db)
        cat.open()
        from godmode_media_library.distributed_backup import ensure_backup_tables

        ensure_backup_tables(cat)
        # Insert a target row to update
        cat.conn.execute(
            "INSERT OR IGNORE INTO backup_targets (remote_name, remote_path, enabled, priority) VALUES ('gdrive', 'gdrive:backup', 1, 1)"
        )
        cat.conn.commit()
        cat.close()

        with patch(
            "godmode_media_library.distributed_backup.ensure_backup_tables",
        ):
            resp = client.put(
                "/api/backup/targets/gdrive",
                json={"total_bytes": 2000000000, "free_bytes": 1000000000},
            )
        assert resp.status_code == 200

    def test_update_target_empty_body(self, client):
        """Empty body should still return ok."""
        with patch(
            "godmode_media_library.distributed_backup.ensure_backup_tables",
        ):
            resp = client.put("/api/backup/targets/gdrive", json={})
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# POST /api/backup/plan
# ---------------------------------------------------------------------------


class TestBackupPlan:
    def test_backup_plan(self, client):
        mock_plan = MagicMock()
        mock_plan.entries = [
            {"target_remote": "gdrive", "path": "/a.jpg", "size": 1024},
            {"target_remote": "gdrive", "path": "/b.jpg", "size": 2048},
            {"target_remote": "s3", "path": "/c.jpg", "size": 512},
        ]
        mock_plan.total_files = 3
        mock_plan.total_bytes = 3584
        mock_plan.targets_used = 2
        mock_plan.overflow_files = 0
        mock_plan.overflow_bytes = 0

        with patch(
            "godmode_media_library.distributed_backup.create_backup_plan",
            return_value=mock_plan,
        ):
            resp = client.post("/api/backup/plan")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_files"] == 3
        assert data["targets_used"] == 2
        assert "gdrive" in data["by_remote"]
        assert data["by_remote"]["gdrive"]["files"] == 2


# ---------------------------------------------------------------------------
# POST /api/backup/execute
# ---------------------------------------------------------------------------


class TestBackupExecute:
    def test_execute_starts_task(self, client):
        resp = client.post("/api/backup/execute", json={})
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"
        assert data["dry_run"] is False

    def test_execute_dry_run(self, client):
        resp = client.post("/api/backup/execute", json={"dry_run": True})
        assert resp.status_code == 200
        assert resp.json()["dry_run"] is True


# ---------------------------------------------------------------------------
# POST /api/backup/verify
# ---------------------------------------------------------------------------


class TestBackupVerify:
    def test_verify_starts_task(self, client):
        resp = client.post("/api/backup/verify")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"


# ---------------------------------------------------------------------------
# GET /api/backup/manifest
# ---------------------------------------------------------------------------


class TestBackupManifest:
    def test_manifest_empty(self, client):
        resp = client.get("/api/backup/manifest")
        assert resp.status_code == 200
        data = resp.json()
        assert data["entries"] == []
        assert data["total"] == 0
        assert data["page"] == 1

    def test_manifest_with_data(self, catalog_db):
        """Insert manifest rows and verify pagination."""
        cat = Catalog(catalog_db)
        cat.open()
        from godmode_media_library.distributed_backup import ensure_backup_tables

        ensure_backup_tables(cat)
        # Insert dummy files first so we have valid file_ids
        now_iso = "2024-01-15T12:00:00+00:00"
        for i in range(5):
            cat.conn.execute(
                "INSERT INTO files (id, path, size, ext, mtime, ctime, first_seen, last_scanned) "
                "VALUES (?, ?, ?, '.jpg', 1704067200.0, 1704067200.0, ?, ?)",
                (i + 1, f"/photos/img{i:03d}.jpg", 1024 * (i + 1), now_iso, now_iso),
            )
            cat.conn.execute(
                "INSERT INTO backup_manifest (file_id, path, size, remote_name, remote_path, backed_up_at, verified, verified_at) "
                "VALUES (?, ?, ?, 'gdrive', 'gdrive:backup', '2024-01-15T12:00:00', 0, NULL)",
                (i + 1, f"/photos/img{i:03d}.jpg", 1024 * (i + 1)),
            )
        cat.conn.commit()
        cat.close()

        from godmode_media_library.web.app import create_app

        app = create_app(catalog_path=catalog_db)
        cl = TestClient(app)

        resp = cl.get("/api/backup/manifest?page=1&limit=3")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 5
        assert len(data["entries"]) == 3
        assert data["pages"] == 2

    def test_manifest_search(self, catalog_db):
        cat = Catalog(catalog_db)
        cat.open()
        from godmode_media_library.distributed_backup import ensure_backup_tables

        ensure_backup_tables(cat)
        now_iso = "2024-01-15T12:00:00+00:00"
        cat.conn.execute(
            "INSERT INTO files (id, path, size, ext, mtime, ctime, first_seen, last_scanned) "
            "VALUES (101, '/photos/sunset.jpg', 2048, '.jpg', 1704067200.0, 1704067200.0, ?, ?)",
            (now_iso, now_iso),
        )
        cat.conn.execute(
            "INSERT INTO files (id, path, size, ext, mtime, ctime, first_seen, last_scanned) "
            "VALUES (102, '/docs/report.pdf', 4096, '.pdf', 1704067200.0, 1704067200.0, ?, ?)",
            (now_iso, now_iso),
        )
        cat.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, size, remote_name, remote_path, backed_up_at, verified, verified_at) "
            "VALUES (101, '/photos/sunset.jpg', 2048, 'gdrive', 'gdrive:backup', '2024-01-15T12:00:00', 0, NULL)"
        )
        cat.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, size, remote_name, remote_path, backed_up_at, verified, verified_at) "
            "VALUES (102, '/docs/report.pdf', 4096, 'gdrive', 'gdrive:backup', '2024-01-15T12:00:00', 0, NULL)"
        )
        cat.conn.commit()
        cat.close()

        from godmode_media_library.web.app import create_app

        app = create_app(catalog_path=catalog_db)
        cl = TestClient(app)

        resp = cl.get("/api/backup/manifest?search=sunset")
        data = resp.json()
        assert data["total"] == 1
        assert data["entries"][0]["filename"] == "sunset.jpg"


# ---------------------------------------------------------------------------
# GET /api/backup/monitor
# ---------------------------------------------------------------------------


class TestBackupMonitor:
    def test_monitor_status(self, client):
        mock_status = {"healthy": True, "alerts": [], "last_check": None}
        with patch(
            "godmode_media_library.backup_monitor.get_monitor_status",
            return_value=mock_status,
        ):
            resp = client.get("/api/backup/monitor")
        assert resp.status_code == 200
        assert resp.json()["healthy"] is True


# ---------------------------------------------------------------------------
# POST /api/backup/monitor/check
# ---------------------------------------------------------------------------


class TestBackupMonitorCheck:
    def test_monitor_check_starts_task(self, client):
        with patch(
            "godmode_media_library.backup_monitor.run_health_checks",
            return_value=[],
        ):
            resp = client.post("/api/backup/monitor/check")
        assert resp.status_code == 200
        assert "task_id" in resp.json()


# ---------------------------------------------------------------------------
# POST /api/backup/monitor/acknowledge
# ---------------------------------------------------------------------------


class TestBackupMonitorAck:
    def test_acknowledge_alerts(self, client):
        with patch(
            "godmode_media_library.backup_monitor.acknowledge_all_alerts",
            return_value=3,
        ):
            resp = client.post("/api/backup/monitor/acknowledge")
        assert resp.status_code == 200
        assert resp.json()["acknowledged"] == 3


# ---------------------------------------------------------------------------
# POST /api/backup/monitor/test-notification
# ---------------------------------------------------------------------------


class TestBackupTestNotification:
    def test_send_test_notification(self, client):
        with patch(
            "godmode_media_library.backup_monitor.send_test_notification",
            return_value={"sent": True, "method": "desktop"},
        ):
            resp = client.post("/api/backup/monitor/test-notification")
        assert resp.status_code == 200
        assert resp.json()["sent"] is True


# ---------------------------------------------------------------------------
# GET /api/bitrot/stats
# ---------------------------------------------------------------------------


class TestBitrotStats:
    def test_bitrot_stats(self, client):
        mock_stats = {"total_verified": 0, "corrupted": 0, "last_scan": None}
        with patch(
            "godmode_media_library.bitrot.get_verification_stats",
            return_value=mock_stats,
        ):
            resp = client.get("/api/bitrot/stats")
        assert resp.status_code == 200
        assert resp.json()["total_verified"] == 0


# ---------------------------------------------------------------------------
# POST /api/bitrot/scan
# ---------------------------------------------------------------------------


class TestBitrotScan:
    def test_bitrot_scan_starts_task(self, client):
        resp = client.post("/api/bitrot/scan?limit=100")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["limit"] == 100


# ---------------------------------------------------------------------------
# GET /api/integrity-score
# ---------------------------------------------------------------------------


class TestIntegrityScore:
    def test_integrity_score_empty_catalog(self, client):
        resp = client.get("/api/integrity-score")
        assert resp.status_code == 200
        data = resp.json()
        assert data["score"] == 0
        assert data["grade"] == "N/A"

    def test_integrity_score_with_files(self, client_with_files):
        resp = client_with_files.get("/api/integrity-score")
        assert resp.status_code == 200
        data = resp.json()
        assert 0 <= data["score"] <= 100
        assert data["grade"] in ("A+", "A", "B", "C", "D", "F")
        assert "factors" in data
        assert data["total_files"] == 5
        # All files have sha256, so hash_coverage should be 100%
        assert data["factors"]["hash_coverage"]["value"] == 100.0


# ---------------------------------------------------------------------------
# POST /api/backup/auto-heal
# ---------------------------------------------------------------------------


class TestBackupAutoHeal:
    def test_auto_heal_starts_task(self, client):
        resp = client.post("/api/backup/auto-heal")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"
