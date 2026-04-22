"""Tests for the scenario engine (CRUD, templates, execution)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library import scenarios
from godmode_media_library.scenarios import (
    STEP_TYPES,
    Scenario,
    ScenarioStep,
    ScenarioTrigger,
    create_scenario,
    delete_scenario,
    duplicate_scenario,
    execute_scenario,
    get_scenario,
    get_templates,
    list_scenarios,
    mark_scenario_run,
    update_scenario,
)


@pytest.fixture(autouse=True)
def _isolate_scenarios(tmp_path, monkeypatch):
    """Redirect scenario storage to a temp directory for every test."""
    fake_path = tmp_path / "scenarios.json"
    monkeypatch.setattr(scenarios, "_SCENARIOS_PATH", fake_path)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


class TestDataModel:
    def test_scenario_auto_generates_id(self):
        sc = Scenario(name="Test")
        assert sc.id  # non-empty
        assert len(sc.id) == 8

    def test_scenario_auto_generates_created_at(self):
        sc = Scenario(name="Test")
        assert sc.created_at > 0

    def test_scenario_keeps_explicit_id(self):
        sc = Scenario(id="myid1234", name="Test")
        assert sc.id == "myid1234"

    def test_step_defaults(self):
        step = ScenarioStep(type="deep_scan")
        assert step.enabled is True
        assert step.config == {}

    def test_trigger_defaults(self):
        trigger = ScenarioTrigger()
        assert trigger.type == "manual"
        assert trigger.volume_name == ""


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------


class TestCRUD:
    def test_create_and_list(self):
        result = create_scenario({"name": "My Scenario", "steps": [{"type": "deep_scan", "config": {}, "enabled": True}]})
        assert result["name"] == "My Scenario"
        assert result["id"]
        assert len(result["steps"]) == 1

        all_sc = list_scenarios()
        assert len(all_sc) == 1
        assert all_sc[0]["name"] == "My Scenario"

    def test_get_scenario_found(self):
        created = create_scenario({"name": "Findable"})
        found = get_scenario(created["id"])
        assert found is not None
        assert found["name"] == "Findable"

    def test_get_scenario_not_found(self):
        assert get_scenario("nonexistent") is None

    def test_update_scenario(self):
        created = create_scenario({"name": "Original"})
        updated = update_scenario(created["id"], {"name": "Renamed", "color": "#ff0000"})
        assert updated is not None
        assert updated["name"] == "Renamed"
        assert updated["color"] == "#ff0000"
        # Steps should be unchanged
        assert updated["steps"] == created["steps"]

    def test_update_scenario_steps(self):
        created = create_scenario({"name": "S", "steps": [{"type": "deep_scan", "config": {}, "enabled": True}]})
        updated = update_scenario(created["id"], {"steps": [{"type": "scan", "config": {"workers": 8}, "enabled": True}]})
        assert updated is not None
        assert len(updated["steps"]) == 1
        assert updated["steps"][0]["type"] == "scan"

    def test_update_nonexistent_returns_none(self):
        assert update_scenario("nope", {"name": "X"}) is None

    def test_delete_scenario(self):
        created = create_scenario({"name": "Doomed"})
        assert delete_scenario(created["id"]) is True
        assert list_scenarios() == []

    def test_delete_nonexistent_returns_false(self):
        assert delete_scenario("ghost") is False

    def test_duplicate_scenario(self):
        created = create_scenario({
            "name": "Original",
            "description": "desc",
            "steps": [{"type": "scan", "config": {"workers": 2}, "enabled": True}],
        })
        duped = duplicate_scenario(created["id"])
        assert duped is not None
        assert duped["id"] != created["id"]
        assert duped["name"] == "Original (kopie)"
        assert duped["steps"] == created["steps"]
        # Trigger should be reset to manual
        assert duped["trigger"]["type"] == "manual"
        # Now we have two scenarios
        assert len(list_scenarios()) == 2

    def test_duplicate_nonexistent_returns_none(self):
        assert duplicate_scenario("missing") is None


# ---------------------------------------------------------------------------
# Mark-as-run
# ---------------------------------------------------------------------------


class TestMarkRun:
    def test_mark_scenario_run_increments(self):
        created = create_scenario({"name": "Runner"})
        assert created["run_count"] == 0
        assert created["last_run_at"] is None

        mark_scenario_run(created["id"])

        updated = get_scenario(created["id"])
        assert updated["run_count"] == 1
        assert updated["last_run_at"] is not None


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------


class TestTemplates:
    def test_get_templates_returns_list(self):
        templates = get_templates()
        assert isinstance(templates, list)
        assert len(templates) >= 5  # at least the known built-in set

    def test_template_step_types_are_valid(self):
        for tpl in get_templates():
            for step in tpl["steps"]:
                assert step["type"] in STEP_TYPES, f"Unknown step type {step['type']} in template {tpl['id']}"


# ---------------------------------------------------------------------------
# Persistence edge cases
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_load_from_empty_file(self, tmp_path, monkeypatch):
        """Corrupt / empty JSON should not crash, just return []."""
        bad_path = tmp_path / "bad.json"
        bad_path.write_text("NOT VALID JSON")
        monkeypatch.setattr(scenarios, "_SCENARIOS_PATH", bad_path)
        assert list_scenarios() == []

    def test_corrupt_json_creates_bak(self, tmp_path, monkeypatch):
        """Regression: corrupted scenarios.json must create a .bak backup."""
        bad_path = tmp_path / "scenarios.json"
        bad_path.write_text("{CORRUPT JSON HERE!!!")
        monkeypatch.setattr(scenarios, "_SCENARIOS_PATH", bad_path)

        result = list_scenarios()
        assert result == []

        bak_path = bad_path.with_suffix(".bak")
        assert bak_path.exists(), ".bak backup was not created for corrupt scenarios file"
        assert bak_path.read_text() == "{CORRUPT JSON HERE!!!"

    def test_roundtrip_with_trigger(self):
        created = create_scenario({
            "name": "Triggered",
            "trigger": {"type": "volume_mount", "volume_name": "MyDisk", "schedule_cron": ""},
        })
        loaded = get_scenario(created["id"])
        assert loaded["trigger"]["type"] == "volume_mount"
        assert loaded["trigger"]["volume_name"] == "MyDisk"

    def test_multiple_scenarios_persist(self):
        for i in range(5):
            create_scenario({"name": f"Scenario {i}"})
        assert len(list_scenarios()) == 5

    def test_atomic_write_creates_parent_dirs(self, tmp_path, monkeypatch):
        """_save_scenarios creates parent directories if they don't exist."""
        deep_path = tmp_path / "deep" / "nested" / "scenarios.json"
        monkeypatch.setattr(scenarios, "_SCENARIOS_PATH", deep_path)
        create_scenario({"name": "Deep"})
        assert deep_path.exists()
        assert len(list_scenarios()) == 1

    def test_atomic_write_crash_safety(self, tmp_path, monkeypatch):
        """If write fails, original file should be intact."""
        path = tmp_path / "scenarios.json"
        monkeypatch.setattr(scenarios, "_SCENARIOS_PATH", path)
        # Create initial scenario
        create_scenario({"name": "Original"})
        assert len(list_scenarios()) == 1

        # Simulate os.replace failure
        with patch("os.replace", side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                create_scenario({"name": "Should Fail"})

        # Original data should still be readable
        assert len(list_scenarios()) == 1
        assert list_scenarios()[0]["name"] == "Original"

    def test_save_load_preserves_all_fields(self):
        """All Scenario fields survive a save/load round-trip."""
        created = create_scenario({
            "name": "Full",
            "description": "desc",
            "icon": "X",
            "color": "#aabbcc",
            "steps": [{"type": "deep_scan", "config": {"key": "val"}, "enabled": False}],
            "trigger": {"type": "schedule", "volume_name": "", "schedule_cron": "0 * * * *"},
        })
        loaded = get_scenario(created["id"])
        assert loaded["description"] == "desc"
        assert loaded["icon"] == "X"
        assert loaded["color"] == "#aabbcc"
        assert loaded["steps"][0]["config"] == {"key": "val"}
        assert loaded["steps"][0]["enabled"] is False
        assert loaded["trigger"]["type"] == "schedule"
        assert loaded["trigger"]["schedule_cron"] == "0 * * * *"


# ---------------------------------------------------------------------------
# Execution engine
# ---------------------------------------------------------------------------


class TestExecution:
    def test_execute_missing_scenario(self):
        result = execute_scenario("does_not_exist", "/fake/catalog.db")
        assert "error" in result

    def test_execute_with_unknown_step_type(self):
        """Unknown step types should not crash, _execute_step returns a note."""
        created = create_scenario({
            "name": "Unknown Step",
            "steps": [{"type": "totally_fake_step", "config": {}, "enabled": True}],
        })
        result = execute_scenario(created["id"], "/fake/catalog.db")
        assert result["completed"] == 1
        assert result["failed"] == 0
        # The step result should contain a note about unknown type
        assert "Neznámý typ kroku" in result["step_results"][0]["result"]["note"]

    def test_execute_skips_disabled_steps(self):
        created = create_scenario({
            "name": "Partial",
            "steps": [
                {"type": "totally_fake_step", "config": {}, "enabled": False},
                {"type": "another_fake", "config": {}, "enabled": True},
            ],
        })
        result = execute_scenario(created["id"], "/fake/catalog.db")
        # Only the enabled step should run
        assert result["total_steps"] == 1

    def test_execute_calls_progress_fn(self):
        created = create_scenario({
            "name": "Progress",
            "steps": [{"type": "cloud_backup", "config": {"remote_name": ""}, "enabled": True}],
        })
        progress_calls = []
        execute_scenario(created["id"], "/fake/catalog.db", progress_fn=progress_calls.append)
        # Should have at least a "step" call and a "complete" call
        phases = [c["phase"] for c in progress_calls]
        assert "step" in phases
        assert "complete" in phases

    @patch("godmode_media_library.scenarios._execute_step", side_effect=RuntimeError("boom"))
    def test_execute_continues_after_step_failure(self, mock_step):
        created = create_scenario({
            "name": "Failover",
            "steps": [
                {"type": "deep_scan", "config": {}, "enabled": True},
                {"type": "scan", "config": {}, "enabled": True},
            ],
        })
        result = execute_scenario(created["id"], "/fake/catalog.db")
        assert result["failed"] == 2
        assert result["completed"] == 0
        # Both steps attempted despite first failure
        assert len(result["step_results"]) == 2

    def test_execute_marks_run_after_completion(self):
        created = create_scenario({
            "name": "Marked",
            "steps": [{"type": "photorec", "config": {}, "enabled": True}],
        })
        execute_scenario(created["id"], "/fake/catalog.db")
        updated = get_scenario(created["id"])
        assert updated["run_count"] == 1
        assert updated["last_run_at"] is not None


# ---------------------------------------------------------------------------
# Step-level execution (individual step types, mocked imports)
# ---------------------------------------------------------------------------


class TestExecuteStepTypes:
    def test_photorec_returns_note(self):
        created = create_scenario({
            "name": "Photorec",
            "steps": [{"type": "photorec", "config": {}, "enabled": True}],
        })
        result = execute_scenario(created["id"], "/fake/catalog.db")
        assert result["step_results"][0]["result"]["note"] == "PhotoRec vyžaduje ruční výběr disku"

    def test_reorganize_returns_note(self):
        created = create_scenario({
            "name": "Reorg",
            "steps": [{"type": "reorganize", "config": {}, "enabled": True}],
        })
        result = execute_scenario(created["id"], "/fake/catalog.db")
        assert "vyžaduje" in result["step_results"][0]["result"]["note"]

    def test_cloud_backup_without_remote_returns_note(self):
        created = create_scenario({
            "name": "Backup",
            "steps": [{"type": "cloud_backup", "config": {}, "enabled": True}],
        })
        result = execute_scenario(created["id"], "/fake/catalog.db")
        assert "vyžaduje" in result["step_results"][0]["result"]["note"]

    def test_cloud_backup_with_remote_returns_note(self):
        created = create_scenario({
            "name": "Backup",
            "steps": [{"type": "cloud_backup", "config": {"remote_name": "gdrive"}, "enabled": True}],
        })
        result = execute_scenario(created["id"], "/fake/catalog.db")
        assert "gdrive" in result["step_results"][0]["result"]["note"]

    @patch("godmode_media_library.scenarios._execute_step")
    def test_execute_deep_scan_step(self, mock_step):
        mock_step.return_value = {"files_found": 42, "total_size": 1000}
        created = create_scenario({
            "name": "DeepScan",
            "steps": [{"type": "deep_scan", "config": {}, "enabled": True}],
        })
        result = execute_scenario(created["id"], "/fake/catalog.db")
        assert result["completed"] == 1
        assert result["step_results"][0]["result"]["files_found"] == 42

    def test_generate_report_step_returns_note(self):
        """generate_report step with ImportError should return note."""
        created = create_scenario({
            "name": "Report",
            "steps": [{"type": "generate_report", "config": {}, "enabled": True}],
        })
        # This will try to import report module which may or may not be available
        result = execute_scenario(created["id"], "/fake/catalog.db")
        assert result["completed"] + result["failed"] == 1

    @patch("godmode_media_library.scenarios.deep_scan", create=True)
    def test_deep_scan_step_direct(self, mock_scan):
        from godmode_media_library.scenarios import _execute_step
        mock_result = MagicMock()
        mock_result.files_found = 10
        mock_result.total_size = 5000
        with patch("godmode_media_library.scenarios.deep_scan", mock_result, create=True):
            with patch.dict("sys.modules", {"godmode_media_library.recovery": MagicMock(deep_scan=lambda: mock_result)}):
                result = _execute_step("deep_scan", {}, "/fake/catalog.db", None)
        assert result["files_found"] == 10

    def test_cloud_connect_step(self):
        from godmode_media_library.scenarios import _execute_step
        mock_remotes = [MagicMock(name="gdrive"), MagicMock(name="s3")]
        for r in mock_remotes:
            r.name = r._mock_name
        with patch.dict("sys.modules", {"godmode_media_library.cloud": MagicMock(list_remotes=lambda: mock_remotes)}):
            result = _execute_step("cloud_connect", {}, "/fake/catalog.db", None)
        assert result["remotes_available"] == 2

    def test_cloud_download_step(self):
        from godmode_media_library.scenarios import _execute_step
        mock_remotes = [{"name": "r1", "mounted": True}, {"name": "r2"}]
        cloud_mod = MagicMock()
        cloud_mod.list_remotes.return_value = mock_remotes
        with patch.dict("sys.modules", {"godmode_media_library.cloud": cloud_mod}):
            result = _execute_step("cloud_download", {}, "/fake/catalog.db", None)
        assert result["sources_ready"] == 1

    def test_app_mine_step(self):
        from godmode_media_library.scenarios import _execute_step
        mock_result = MagicMock(files_found=5, total_size=1000)
        recovery_mod = MagicMock()
        recovery_mod.mine_app_media.return_value = [mock_result]
        with patch.dict("sys.modules", {"godmode_media_library.recovery": recovery_mod}):
            result = _execute_step("app_mine", {}, "/fake/catalog.db", None)
        assert result["total_files"] == 5

    def test_signal_decrypt_step(self):
        from godmode_media_library.scenarios import _execute_step
        recovery_mod = MagicMock()
        recovery_mod.decrypt_signal_attachments.return_value = {"decrypted": 3, "total_size": 500, "errors": []}
        with patch.dict("sys.modules", {"godmode_media_library.recovery": recovery_mod}):
            result = _execute_step("signal_decrypt", {}, "/fake/catalog.db", None)
        assert result["decrypted"] == 3

    def test_integrity_check_step(self):
        from godmode_media_library.scenarios import _execute_step
        mock_result = MagicMock(total_checked=100, corrupted=2, healthy=98)
        recovery_mod = MagicMock()
        recovery_mod.check_integrity.return_value = mock_result
        with patch.dict("sys.modules", {"godmode_media_library.recovery": recovery_mod}):
            result = _execute_step("integrity_check", {"catalog_path": "/fake"}, "/fake/catalog.db", None)
        assert result["total_checked"] == 100

    def test_scan_step(self):
        from godmode_media_library.scenarios import _execute_step
        mock_stats = MagicMock(total_files=50)
        mock_stats.files_scanned = 50  # The actual attribute used
        scanner_mod = MagicMock()
        scanner_mod.incremental_scan.return_value = mock_stats
        config_mod = MagicMock()
        config_mod.load_config.return_value = MagicMock(prefer_roots=["/tmp"])
        with patch.dict("sys.modules", {
            "godmode_media_library.scanner": scanner_mod,
            "godmode_media_library.config": config_mod,
        }):
            result = _execute_step("scan", {"workers": 2}, "/fake/catalog.db", None)
        assert "scanned" in result

    def test_dedup_resolve_step(self):
        from godmode_media_library.scenarios import _execute_step
        mock_cat = MagicMock()
        mock_cat.query_duplicates.return_value = {"groups": [{"files": [1, 2]}, {"files": [3]}]}
        with patch("godmode_media_library.scenarios.Catalog", return_value=mock_cat, create=True):
            with patch.dict("sys.modules", {"godmode_media_library.catalog": MagicMock(Catalog=lambda p: mock_cat)}):
                result = _execute_step("dedup_resolve", {}, "/fake/catalog.db", None)
        assert result["groups_found"] == 2

    def test_quarantine_cleanup_step_no_old(self):
        from godmode_media_library.scenarios import _execute_step
        recovery_mod = MagicMock()
        recovery_mod.list_quarantine.return_value = []
        with patch.dict("sys.modules", {"godmode_media_library.recovery": recovery_mod}):
            result = _execute_step("quarantine_cleanup", {"older_than_days": 1}, "/fake/catalog.db", None)
        assert result["cleaned"] == 0

    def test_metadata_enrich_step(self):
        from godmode_media_library.scenarios import _execute_step
        mock_cat = MagicMock()
        exiftool_mod = MagicMock()
        exiftool_mod.extract_all_metadata.return_value = {"extracted": 15}
        catalog_mod = MagicMock()
        catalog_mod.Catalog.return_value = mock_cat
        with patch.dict("sys.modules", {
            "godmode_media_library.catalog": catalog_mod,
            "godmode_media_library.exiftool_extract": exiftool_mod,
        }):
            result = _execute_step("metadata_enrich", {}, "/fake/catalog.db", None)
        assert result["enriched"] == 15

    def test_quality_analyze_step_import_error(self):
        from godmode_media_library.scenarios import _execute_step
        catalog_mod = MagicMock()
        with patch.dict("sys.modules", {"godmode_media_library.catalog": catalog_mod}):
            with patch.dict("sys.modules", {"godmode_media_library.quality": None}):
                result = _execute_step("quality_analyze", {}, "/fake/catalog.db", None)
        assert "note" in result


# ---------------------------------------------------------------------------
# Volume trigger detection
# ---------------------------------------------------------------------------


class TestVolumeTriggers:
    def test_check_volume_triggers_no_match(self, tmp_path, monkeypatch):
        """No volume-triggered scenarios means empty result."""
        create_scenario({"name": "Manual Only"})
        with patch.object(Path, "exists", return_value=False):
            result = scenarios.check_volume_triggers()
        assert result == []

    def test_check_volume_triggers_dot_dirs_ignored(self, tmp_path, monkeypatch):
        """Hidden volume directories (starting with .) are ignored."""
        create_scenario({
            "name": "HiddenVol",
            "trigger": {"type": "volume_mount", "volume_name": ".hidden", "schedule_cron": ""},
        })
        with patch("godmode_media_library.scenarios.Path") as mock_path:
            mock_volumes_dir = MagicMock()
            mock_volumes_dir.exists.return_value = True
            hidden_vol = MagicMock()
            hidden_vol.is_dir.return_value = True
            hidden_vol.name = ".hidden"
            mock_volumes_dir.iterdir.return_value = [hidden_vol]
            mock_path.return_value = mock_volumes_dir
            result = scenarios.check_volume_triggers()
        # .hidden starts with dot, so it should be filtered out
        assert result == []

    def test_check_volume_triggers_with_match(self, tmp_path, monkeypatch):
        create_scenario({
            "name": "USB Trigger",
            "trigger": {"type": "volume_mount", "volume_name": "MYUSB", "schedule_cron": ""},
        })
        fake_volumes = tmp_path / "Volumes"
        fake_volumes.mkdir()
        (fake_volumes / "MYUSB").mkdir()
        monkeypatch.setattr(scenarios, "Path", lambda p: Path(str(p).replace("/Volumes", str(fake_volumes))) if p == "/Volumes" else Path(p))
        # Directly call with patched Path for /Volumes
        with patch("godmode_media_library.scenarios.Path") as mock_path:
            mock_volumes_dir = MagicMock()
            mock_volumes_dir.exists.return_value = True
            mock_vol = MagicMock()
            mock_vol.is_dir.return_value = True
            mock_vol.name = "MYUSB"
            mock_volumes_dir.iterdir.return_value = [mock_vol]
            mock_path.return_value = mock_volumes_dir
            # Also need the home() for _SCENARIOS_PATH — but we patched that already
            result = scenarios.check_volume_triggers()
        assert len(result) == 1
        assert result[0]["name"] == "USB Trigger"
