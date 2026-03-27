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
