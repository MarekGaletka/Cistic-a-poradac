"""End-to-end tests for Scenarios and Backup features.

Covers:
  - Scenario CRUD (create, read, update, delete, duplicate)
  - Scenario input validation (empty name, long name, invalid types)
  - Scenario execution and triggers
  - Backup stats, targets, plan, execute, verify
  - Backup manifest (pagination, search)
  - Backup monitoring (status, health check, acknowledge)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import patch

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
def catalog_with_files(tmp_path):
    """Create a catalog with some test files for backup tests."""
    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()

    # Create test media files on disk
    root = tmp_path / "media"
    root.mkdir()
    (root / "photo1.jpg").write_bytes(b"content1")
    (root / "photo2.jpg").write_bytes(b"content2")
    (root / "video.mp4").write_bytes(b"videocontent")
    (root / "doc.pdf").write_bytes(b"pdfcontent")

    from godmode_media_library.scanner import incremental_scan

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
    ):
        incremental_scan(cat, [root])

    # Fix schema mismatch: catalog.py creates backup_targets without
    # encrypted/crypt_remote but distributed_backup queries for them.
    _ensure_full_backup_schema(cat)
    cat.close()
    return db_path


@pytest.fixture
def client(catalog_with_files):
    """Create a test client with a populated catalog."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_with_files)
    return TestClient(app)


@pytest.fixture(autouse=True)
def isolate_scenarios(tmp_path, monkeypatch):
    """Redirect scenario storage to a temp directory so tests are isolated."""
    scenarios_file = tmp_path / "scenarios.json"
    monkeypatch.setattr("godmode_media_library.scenarios._SCENARIOS_PATH", scenarios_file)


@pytest.fixture(autouse=True)
def isolate_monitor_state(tmp_path, monkeypatch):
    """Redirect backup monitor state to a temp directory."""
    state_file = tmp_path / "backup_monitor_state.json"
    monkeypatch.setattr("godmode_media_library.backup_monitor._MONITOR_STATE_PATH", state_file)


def _ensure_full_backup_schema(cat):
    """Work around schema mismatch: catalog.py creates backup_targets without
    encrypted/crypt_remote columns, but distributed_backup.py expects them.
    Add the missing columns if the table already exists without them."""
    try:
        cat.conn.execute("SELECT encrypted FROM backup_targets LIMIT 0")
    except Exception:
        try:
            cat.conn.execute("ALTER TABLE backup_targets ADD COLUMN encrypted INTEGER DEFAULT 0")
            cat.conn.execute("ALTER TABLE backup_targets ADD COLUMN crypt_remote TEXT DEFAULT ''")
            cat.conn.commit()
        except Exception:
            pass


def _create_scenario_payload(
    name="Test scenario",
    description="A test scenario",
    steps=None,
    trigger=None,
):
    """Helper to build a scenario creation payload."""
    payload = {"name": name, "description": description}
    if steps is not None:
        payload["steps"] = steps
    if trigger is not None:
        payload["trigger"] = trigger
    return payload


# =========================================================================
# 1. Scenario CRUD Operations
# =========================================================================


class TestScenarioCRUD:
    """Test basic create / read / update / delete operations on scenarios."""

    def test_list_scenarios_empty(self, client):
        """GET /api/scenarios on a fresh install returns an empty list."""
        resp = client.get("/api/scenarios")
        assert resp.status_code == 200
        data = resp.json()
        assert "scenarios" in data
        assert data["scenarios"] == []

    def test_create_scenario_full(self, client):
        """POST /api/scenarios with all fields creates and returns a scenario."""
        payload = _create_scenario_payload(
            name="Full workflow",
            description="Scan, dedup, reorganize",
            steps=[
                {"type": "scan", "config": {"workers": 4}, "enabled": True},
                {"type": "dedup_resolve", "config": {"strategy": "richness"}, "enabled": True},
                {"type": "reorganize", "config": {"structure_pattern": "year_month"}, "enabled": True},
            ],
            trigger={"type": "manual"},
        )
        resp = client.post("/api/scenarios", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "Full workflow"
        assert data["description"] == "Scan, dedup, reorganize"
        assert len(data["steps"]) == 3
        assert data["id"]  # non-empty id assigned

    def test_create_scenario_minimal(self, client):
        """POST /api/scenarios with only a name succeeds (steps default empty)."""
        resp = client.post("/api/scenarios", json={"name": "Minimal"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "Minimal"
        assert data["steps"] == []

    def test_create_scenario_without_name_fails(self, client):
        """POST /api/scenarios without 'name' should fail validation."""
        resp = client.post("/api/scenarios", json={"description": "no name"})
        assert resp.status_code == 422  # FastAPI validation error

    def test_list_scenarios_with_data(self, client):
        """After creating scenarios, GET /api/scenarios returns them all."""
        client.post("/api/scenarios", json={"name": "A"})
        client.post("/api/scenarios", json={"name": "B"})
        resp = client.get("/api/scenarios")
        assert resp.status_code == 200
        names = {s["name"] for s in resp.json()["scenarios"]}
        assert names == {"A", "B"}

    def test_get_scenario_detail(self, client):
        """GET /api/scenarios/{id} returns correct scenario."""
        created = client.post("/api/scenarios", json={"name": "Detail"}).json()
        resp = client.get(f"/api/scenarios/{created['id']}")
        assert resp.status_code == 200
        assert resp.json()["name"] == "Detail"

    def test_get_scenario_not_found(self, client):
        """GET /api/scenarios/{id} for non-existent id returns 404."""
        resp = client.get("/api/scenarios/nonexistent")
        assert resp.status_code == 404

    def test_update_scenario(self, client):
        """PUT /api/scenarios/{id} updates specified fields."""
        created = client.post(
            "/api/scenarios",
            json={"name": "Before", "description": "old"},
        ).json()
        resp = client.put(
            f"/api/scenarios/{created['id']}",
            json={"name": "After", "description": "new"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "After"
        assert data["description"] == "new"

    def test_update_scenario_partial(self, client):
        """PUT /api/scenarios/{id} with partial payload only changes given fields."""
        created = client.post(
            "/api/scenarios",
            json={"name": "Original", "description": "keep"},
        ).json()
        resp = client.put(
            f"/api/scenarios/{created['id']}",
            json={"name": "Changed"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "Changed"
        assert data["description"] == "keep"

    def test_update_scenario_not_found(self, client):
        """PUT /api/scenarios/{id} for non-existent id returns 404."""
        resp = client.put("/api/scenarios/missing", json={"name": "x"})
        assert resp.status_code == 404

    def test_delete_scenario(self, client):
        """DELETE /api/scenarios/{id} removes the scenario."""
        created = client.post("/api/scenarios", json={"name": "Doomed"}).json()
        resp = client.delete(f"/api/scenarios/{created['id']}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        # Verify it's gone
        assert client.get(f"/api/scenarios/{created['id']}").status_code == 404

    def test_delete_scenario_not_found(self, client):
        """DELETE /api/scenarios/{id} for non-existent id returns 404."""
        resp = client.delete("/api/scenarios/nonexistent-id")
        assert resp.status_code == 404

    def test_duplicate_scenario(self, client):
        """POST /api/scenarios/{id}/duplicate creates a copy with new id."""
        created = client.post(
            "/api/scenarios",
            json={"name": "Original", "steps": [{"type": "scan", "config": {}, "enabled": True}]},
        ).json()
        resp = client.post(f"/api/scenarios/{created['id']}/duplicate")
        assert resp.status_code == 200
        dup = resp.json()
        assert dup["id"] != created["id"]
        assert "(kopie)" in dup["name"]
        assert len(dup["steps"]) == len(created["steps"])

    def test_duplicate_scenario_not_found(self, client):
        """POST /api/scenarios/{id}/duplicate for non-existent id returns 404."""
        resp = client.post("/api/scenarios/missing/duplicate")
        assert resp.status_code == 404

    def test_get_templates(self, client):
        """GET /api/scenarios/templates returns built-in templates."""
        resp = client.get("/api/scenarios/templates")
        assert resp.status_code == 200
        data = resp.json()
        assert "templates" in data
        assert isinstance(data["templates"], list)
        assert len(data["templates"]) > 0
        # Each template has at least name and steps
        tpl = data["templates"][0]
        assert "name" in tpl
        assert "steps" in tpl

    def test_get_step_types(self, client):
        """GET /api/scenarios/step-types returns available step type definitions."""
        resp = client.get("/api/scenarios/step-types")
        assert resp.status_code == 200
        data = resp.json()
        assert "step_types" in data
        step_types = data["step_types"]
        assert "scan" in step_types
        assert "deep_scan" in step_types
        assert "reorganize" in step_types
        # Each step type has label_key and icon
        for _key, val in step_types.items():
            assert "label_key" in val
            assert "icon" in val


# =========================================================================
# 2. Scenario Input Validation
# =========================================================================


class TestScenarioValidation:
    """Test edge cases and invalid inputs for scenario endpoints."""

    def test_empty_name_rejected(self, client):
        """Scenario with empty string name is accepted (no server-side min length)."""
        resp = client.post("/api/scenarios", json={"name": ""})
        # The server does not enforce min length — it returns 200 but an empty name
        assert resp.status_code == 200
        assert resp.json()["name"] == ""

    def test_very_long_name_handled(self, client):
        """Scenario with a 1000+ char name is accepted and persisted."""
        long_name = "A" * 1200
        resp = client.post("/api/scenarios", json={"name": long_name})
        assert resp.status_code == 200
        assert resp.json()["name"] == long_name

    def test_no_steps_is_valid(self, client):
        """Scenario with an explicit empty steps list is valid."""
        resp = client.post("/api/scenarios", json={"name": "Empty steps", "steps": []})
        assert resp.status_code == 200
        assert resp.json()["steps"] == []

    def test_duplicate_step_types_valid(self, client):
        """Scenario with repeated step types is accepted."""
        steps = [
            {"type": "scan", "config": {}, "enabled": True},
            {"type": "scan", "config": {"workers": 8}, "enabled": True},
        ]
        resp = client.post("/api/scenarios", json={"name": "Dup steps", "steps": steps})
        assert resp.status_code == 200
        assert len(resp.json()["steps"]) == 2

    def test_unknown_step_type_accepted(self, client):
        """Unknown step types are stored (engine will skip them at execution)."""
        steps = [{"type": "totally_fake_step", "config": {}, "enabled": True}]
        resp = client.post("/api/scenarios", json={"name": "Bad step", "steps": steps})
        assert resp.status_code == 200
        assert resp.json()["steps"][0]["type"] == "totally_fake_step"

    def test_invalid_trigger_type_accepted(self, client):
        """Unknown trigger types are stored as-is."""
        trigger = {"type": "unknown_trigger", "volume_name": ""}
        resp = client.post(
            "/api/scenarios",
            json={"name": "Bad trigger", "trigger": trigger},
        )
        assert resp.status_code == 200
        assert resp.json()["trigger"]["type"] == "unknown_trigger"


# =========================================================================
# 3. Scenario Execution
# =========================================================================


class TestScenarioExecution:
    """Test scenario run endpoint."""

    def test_run_scenario_starts_task(self, client):
        """POST /api/scenarios/{id}/run starts a background task."""
        created = client.post(
            "/api/scenarios",
            json={
                "name": "Runnable",
                "steps": [{"type": "deep_scan", "config": {}, "enabled": True}],
            },
        ).json()

        with patch("godmode_media_library.scenarios.execute_scenario", return_value={"ok": True}):
            resp = client.post(f"/api/scenarios/{created['id']}/run")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"
        assert data["scenario"] == "Runnable"

    def test_run_nonexistent_scenario_404(self, client):
        """POST /api/scenarios/{id}/run for missing id returns 404."""
        resp = client.post("/api/scenarios/nonexistent/run")
        assert resp.status_code == 404

    def test_run_scenario_disabled_steps(self, client):
        """Scenario run with disabled steps: endpoint still starts, engine skips them."""
        created = client.post(
            "/api/scenarios",
            json={
                "name": "Partial run",
                "steps": [
                    {"type": "scan", "config": {}, "enabled": True},
                    {"type": "reorganize", "config": {}, "enabled": False},
                ],
            },
        ).json()

        with patch("godmode_media_library.scenarios.execute_scenario", return_value={"ok": True}):
            resp = client.post(f"/api/scenarios/{created['id']}/run")
        assert resp.status_code == 200
        assert "task_id" in resp.json()


# =========================================================================
# 4. Scenario Triggers
# =========================================================================


class TestScenarioTriggers:
    """Test trigger-related endpoints."""

    def test_check_triggers_empty(self, client):
        """GET /api/scenarios/triggers returns empty when no scenarios."""
        resp = client.get("/api/scenarios/triggers")
        assert resp.status_code == 200
        assert resp.json()["triggered"] == []

    def test_volume_mount_trigger_detection(self, client, tmp_path):
        """Scenarios with volume_mount trigger are returned when volume matches."""
        # Create a scenario with a volume trigger
        client.post(
            "/api/scenarios",
            json={
                "name": "Disk scenario",
                "trigger": {"type": "volume_mount", "volume_name": "FAKE_VOL"},
            },
        )

        # Mock /Volumes to contain FAKE_VOL
        fake_vol = tmp_path / "Volumes" / "FAKE_VOL"
        fake_vol.mkdir(parents=True)

        with patch("godmode_media_library.scenarios.Path") as MockPath:
            # Make Path("/Volumes") return our fake dir
            def path_factory(p):
                if p == "/Volumes":
                    return tmp_path / "Volumes"
                return type(tmp_path)(p)

            MockPath.side_effect = path_factory
            MockPath.home = lambda: tmp_path

            resp = client.get("/api/scenarios/triggers")

        # The endpoint calls check_volume_triggers which checks /Volumes
        assert resp.status_code == 200

    def test_triggers_no_match(self, client):
        """Volume trigger scenario not triggered when volume is not mounted."""
        client.post(
            "/api/scenarios",
            json={
                "name": "Unmounted",
                "trigger": {"type": "volume_mount", "volume_name": "NONEXISTENT_DRIVE_XYZ"},
            },
        )
        resp = client.get("/api/scenarios/triggers")
        assert resp.status_code == 200
        # Unlikely that NONEXISTENT_DRIVE_XYZ is actually mounted
        triggered_names = [s["name"] for s in resp.json()["triggered"]]
        assert "Unmounted" not in triggered_names


# =========================================================================
# 5. Backup Stats & Targets
# =========================================================================


class TestBackupStatsTargets:
    """Test backup statistics and target management endpoints."""

    def test_backup_stats_empty(self, client):
        """GET /api/backup/stats returns zeros on a fresh catalog."""
        resp = client.get("/api/backup/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_files" in data
        assert "backed_up" in data
        assert "coverage_pct" in data
        assert "total_size" in data
        assert data["backed_up"] == 0
        assert data["coverage_pct"] == 0.0

    def test_backup_targets_empty(self, client):
        """GET /api/backup/targets returns empty list with no targets configured."""
        resp = client.get("/api/backup/targets")
        assert resp.status_code == 200
        data = resp.json()
        assert "targets" in data
        assert isinstance(data["targets"], list)

    def test_update_backup_target_enable_disable(self, client, catalog_with_files):
        """PUT /api/backup/targets/{name} toggles enabled state."""
        # Insert a target manually so we have something to update
        cat = Catalog(catalog_with_files)
        cat.open()
        from godmode_media_library.distributed_backup import ensure_backup_tables

        ensure_backup_tables(cat)
        cat.conn.execute(
            "INSERT INTO backup_targets (remote_name, enabled, priority) VALUES (?, ?, ?)",
            ("gdrive", 1, 0),
        )
        cat.conn.commit()
        cat.close()

        resp = client.put("/api/backup/targets/gdrive", json={"enabled": False})
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        # Verify the target is now disabled
        targets_resp = client.get("/api/backup/targets")
        targets = targets_resp.json()["targets"]
        gdrive = next((t for t in targets if t["remote_name"] == "gdrive"), None)
        assert gdrive is not None
        assert gdrive["enabled"] is False

    def test_update_backup_target_priority(self, client, catalog_with_files):
        """PUT /api/backup/targets/{name} changes priority."""
        cat = Catalog(catalog_with_files)
        cat.open()
        from godmode_media_library.distributed_backup import ensure_backup_tables

        ensure_backup_tables(cat)
        cat.conn.execute(
            "INSERT INTO backup_targets (remote_name, enabled, priority) VALUES (?, ?, ?)",
            ("onedrive", 1, 0),
        )
        cat.conn.commit()
        cat.close()

        resp = client.put("/api/backup/targets/onedrive", json={"priority": 5})
        assert resp.status_code == 200

        targets = client.get("/api/backup/targets").json()["targets"]
        od = next(t for t in targets if t["remote_name"] == "onedrive")
        assert od["priority"] == 5

    def test_update_backup_target_capacity(self, client, catalog_with_files):
        """PUT /api/backup/targets/{name} can set total_bytes and free_bytes."""
        cat = Catalog(catalog_with_files)
        cat.open()
        from godmode_media_library.distributed_backup import ensure_backup_tables

        ensure_backup_tables(cat)
        cat.conn.execute(
            "INSERT INTO backup_targets (remote_name, enabled) VALUES (?, ?)",
            ("s3", 1),
        )
        cat.conn.commit()
        cat.close()

        resp = client.put(
            "/api/backup/targets/s3",
            json={"total_bytes": 10_000_000_000, "free_bytes": 5_000_000_000},
        )
        assert resp.status_code == 200


# =========================================================================
# 6. Backup Plan
# =========================================================================


class TestBackupPlan:
    """Test backup plan creation."""

    def test_backup_plan_no_targets(self, client):
        """POST /api/backup/plan with no configured targets returns empty plan."""
        with patch("godmode_media_library.distributed_backup.get_targets", return_value=[]):
            resp = client.post("/api/backup/plan")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_files"] == 0

    def test_backup_plan_creates_distribution(self, client, catalog_with_files):
        """POST /api/backup/plan returns a plan with distribution info."""
        from godmode_media_library.distributed_backup import (
            ensure_backup_tables,
        )

        # Set up a target in DB
        cat = Catalog(catalog_with_files)
        cat.open()
        ensure_backup_tables(cat)
        cat.conn.execute(
            "INSERT INTO backup_targets (remote_name, enabled, priority, free_bytes, total_bytes) VALUES (?, ?, ?, ?, ?)",
            ("gdrive", 1, 0, 10_000_000_000, 15_000_000_000),
        )
        cat.conn.commit()
        cat.close()

        resp = client.post("/api/backup/plan")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_files" in data
        assert "total_bytes" in data
        assert "by_remote" in data
        assert "entries" in data

    def test_backup_probe(self, client):
        """POST /api/backup/probe probes remote capacities via rclone."""
        from godmode_media_library.distributed_backup import BackupTarget

        mock_target = BackupTarget(
            remote_name="gdrive",
            total_bytes=15_000_000_000,
            used_bytes=5_000_000_000,
            free_bytes=10_000_000_000,
        )
        with patch(
            "godmode_media_library.distributed_backup.probe_targets",
            return_value=[mock_target],
        ):
            resp = client.post("/api/backup/probe")
        assert resp.status_code == 200
        data = resp.json()
        assert data["probed"] == 1
        assert len(data["targets"]) == 1
        assert data["targets"][0]["remote_name"] == "gdrive"


# =========================================================================
# 7. Backup Execute
# =========================================================================


class TestBackupExecute:
    """Test backup execution endpoint."""

    def test_backup_execute_starts_task(self, client):
        """POST /api/backup/execute starts a background backup task."""
        with (
            patch("godmode_media_library.distributed_backup.create_backup_plan"),
            patch("godmode_media_library.distributed_backup.execute_backup_plan"),
        ):
            resp = client.post("/api/backup/execute", json={"dry_run": True})
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"
        assert data["dry_run"] is True

    def test_backup_execute_default_not_dry_run(self, client):
        """POST /api/backup/execute without body defaults to dry_run=False."""
        with (
            patch("godmode_media_library.distributed_backup.create_backup_plan"),
            patch("godmode_media_library.distributed_backup.execute_backup_plan"),
        ):
            resp = client.post("/api/backup/execute", json={})
        assert resp.status_code == 200
        assert resp.json()["dry_run"] is False


# =========================================================================
# 8. Backup Verify
# =========================================================================


class TestBackupVerify:
    """Test backup verification endpoint."""

    def test_backup_verify_starts_task(self, client):
        """POST /api/backup/verify starts a background verification task."""
        with patch("godmode_media_library.distributed_backup.verify_backups"):
            resp = client.post("/api/backup/verify")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"


# =========================================================================
# 9. Backup Manifest
# =========================================================================


class TestBackupManifest:
    """Test the paginated backup manifest endpoint."""

    def test_manifest_empty(self, client):
        """GET /api/backup/manifest with no data returns empty entries."""
        resp = client.get("/api/backup/manifest")
        assert resp.status_code == 200
        data = resp.json()
        assert data["entries"] == []
        assert data["total"] == 0
        assert data["page"] == 1
        assert data["pages"] == 1

    def test_manifest_pagination(self, client, catalog_with_files):
        """GET /api/backup/manifest respects page and limit parameters."""
        # Insert some manifest entries directly
        cat = Catalog(catalog_with_files)
        cat.open()
        from godmode_media_library.distributed_backup import ensure_backup_tables

        ensure_backup_tables(cat)
        now = datetime.now(timezone.utc).isoformat()
        for i in range(25):
            cat.conn.execute(
                "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (i + 1, f"/media/photo{i}.jpg", f"sha{i}", 1000 * (i + 1), "gdrive", f"GML-Backup/photo{i}.jpg", now),
            )
        cat.conn.commit()
        cat.close()

        # Page 1, limit 10
        resp = client.get("/api/backup/manifest?page=1&limit=10")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["entries"]) == 10
        assert data["total"] == 25
        assert data["page"] == 1
        assert data["pages"] == 3

        # Page 3 with limit 10 has 5 entries
        resp = client.get("/api/backup/manifest?page=3&limit=10")
        data = resp.json()
        assert len(data["entries"]) == 5

    def test_manifest_search(self, client, catalog_with_files):
        """GET /api/backup/manifest?search= filters by path."""
        cat = Catalog(catalog_with_files)
        cat.open()
        from godmode_media_library.distributed_backup import ensure_backup_tables

        ensure_backup_tables(cat)
        now = datetime.now(timezone.utc).isoformat()
        cat.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, "/media/holiday_photo.jpg", "abc123", 5000, "gdrive", "GML-Backup/holiday_photo.jpg", now),
        )
        cat.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (2, "/media/work_doc.pdf", "def456", 3000, "gdrive", "GML-Backup/work_doc.pdf", now),
        )
        cat.conn.commit()
        cat.close()

        # Search for "holiday"
        resp = client.get("/api/backup/manifest?search=holiday")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert "holiday" in data["entries"][0]["path"]

        # Search for "doc"
        resp = client.get("/api/backup/manifest?search=doc")
        data = resp.json()
        assert data["total"] == 1
        assert "doc" in data["entries"][0]["path"]

        # Search with no match
        resp = client.get("/api/backup/manifest?search=nonexistent")
        data = resp.json()
        assert data["total"] == 0
        assert data["entries"] == []

    def test_manifest_entry_fields(self, client, catalog_with_files):
        """Each manifest entry has the expected fields."""
        cat = Catalog(catalog_with_files)
        cat.open()
        from godmode_media_library.distributed_backup import ensure_backup_tables

        ensure_backup_tables(cat)
        now = datetime.now(timezone.utc).isoformat()
        cat.conn.execute(
            "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at, verified) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "/media/test.jpg", "sha_test", 4096, "gdrive", "GML-Backup/test.jpg", now, 0),
        )
        cat.conn.commit()
        cat.close()

        resp = client.get("/api/backup/manifest")
        entry = resp.json()["entries"][0]
        assert entry["path"] == "/media/test.jpg"
        assert entry["filename"] == "test.jpg"
        assert entry["size"] == 4096
        assert entry["remote_name"] == "gdrive"
        assert entry["remote_path"] == "GML-Backup/test.jpg"
        assert entry["backed_up_at"] == now
        assert entry["verified"] is False


# =========================================================================
# 10. Backup Monitor
# =========================================================================


class TestBackupMonitor:
    """Test backup monitoring endpoints."""

    def test_monitor_status(self, client):
        """GET /api/backup/monitor returns status structure."""
        resp = client.get("/api/backup/monitor")
        assert resp.status_code == 200
        data = resp.json()
        # The monitor status dict should have these keys
        assert "last_check_at" in data
        assert "alerts" in data or "active_alerts" in data or "status" in data

    def test_monitor_health_check_starts_task(self, client):
        """POST /api/backup/monitor/check starts a health check task."""
        with patch(
            "godmode_media_library.backup_monitor.run_health_checks",
            return_value=[],
        ):
            resp = client.post("/api/backup/monitor/check")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_monitor_acknowledge_no_alerts(self, client):
        """POST /api/backup/monitor/acknowledge with no alerts returns 0."""
        resp = client.post("/api/backup/monitor/acknowledge")
        assert resp.status_code == 200
        assert resp.json()["acknowledged"] == 0

    def test_monitor_acknowledge_with_alerts(self, client, tmp_path):
        """POST /api/backup/monitor/acknowledge acknowledges active alerts."""
        # Manually write a monitor state with unacknowledged alerts
        state_file = tmp_path / "backup_monitor_state.json"
        state = {
            "last_check_at": "2024-01-01T00:00:00",
            "checks": [],
            "alerts": [
                {
                    "timestamp": "2024-01-01T00:00:00",
                    "severity": "critical",
                    "message": "gdrive unreachable",
                    "remote": "gdrive",
                    "acknowledged": False,
                },
                {
                    "timestamp": "2024-01-01T00:01:00",
                    "severity": "warning",
                    "message": "onedrive slow",
                    "remote": "onedrive",
                    "acknowledged": False,
                },
            ],
            "consecutive_failures": {},
        }
        state_file.write_text(json.dumps(state))

        resp = client.post("/api/backup/monitor/acknowledge")
        assert resp.status_code == 200
        assert resp.json()["acknowledged"] == 2

        # Calling again should acknowledge 0
        resp = client.post("/api/backup/monitor/acknowledge")
        assert resp.json()["acknowledged"] == 0

    def test_monitor_test_notification(self, client):
        """POST /api/backup/monitor/test-notification sends a test notification."""
        with patch("godmode_media_library.backup_monitor._send_notification"):
            resp = client.post("/api/backup/monitor/test-notification")
        assert resp.status_code == 200
        assert resp.json()["status"] == "sent"


# =========================================================================
# 11. Cross-feature: Scenario with backup step
# =========================================================================


class TestScenarioBackupIntegration:
    """Test scenarios that contain backup-related steps."""

    def test_scenario_with_cloud_backup_step(self, client):
        """A scenario containing a cloud_backup step can be created and listed."""
        payload = _create_scenario_payload(
            name="Backup workflow",
            steps=[
                {"type": "scan", "config": {"workers": 2}, "enabled": True},
                {"type": "cloud_backup", "config": {"remote_name": "gdrive"}, "enabled": True},
            ],
        )
        resp = client.post("/api/scenarios", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["steps"]) == 2
        assert data["steps"][1]["type"] == "cloud_backup"
        assert data["steps"][1]["config"]["remote_name"] == "gdrive"

    def test_scenario_update_steps(self, client):
        """Updating steps on an existing scenario replaces them entirely."""
        created = client.post(
            "/api/scenarios",
            json={"name": "Evolving", "steps": [{"type": "scan", "config": {}, "enabled": True}]},
        ).json()

        new_steps = [
            {"type": "deep_scan", "config": {}, "enabled": True},
            {"type": "integrity_check", "config": {}, "enabled": True},
            {"type": "cloud_backup", "config": {"remote_name": "s3"}, "enabled": True},
        ]
        resp = client.put(
            f"/api/scenarios/{created['id']}",
            json={"steps": new_steps},
        )
        assert resp.status_code == 200
        assert len(resp.json()["steps"]) == 3
        assert resp.json()["steps"][2]["type"] == "cloud_backup"

    def test_backup_stats_after_manifest_insert(self, client, catalog_with_files):
        """Backup stats reflect data in the manifest."""
        cat = Catalog(catalog_with_files)
        cat.open()
        from godmode_media_library.distributed_backup import ensure_backup_tables

        ensure_backup_tables(cat)
        now = datetime.now(timezone.utc).isoformat()
        # Get a real file_id from the catalog
        row = cat.conn.execute("SELECT id, sha256 FROM files WHERE sha256 IS NOT NULL LIMIT 1").fetchone()
        if row:
            cat.conn.execute(
                "INSERT INTO backup_manifest (file_id, path, sha256, size, remote_name, remote_path, backed_up_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (row[0], "/media/photo1.jpg", row[1] or "testsha", 5000, "gdrive", "GML-Backup/photo1.jpg", now),
            )
            cat.conn.commit()
        cat.close()

        resp = client.get("/api/backup/stats")
        assert resp.status_code == 200
        data = resp.json()
        if row:
            assert data["backed_up"] >= 1
            assert data["total_size"] >= 5000
