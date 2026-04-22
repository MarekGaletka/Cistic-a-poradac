"""Extended scenarios tests targeting uncovered lines.

Covers: Scenario dataclass, ScenarioStep, ScenarioTrigger, _load_scenarios,
_save_scenarios, list_scenarios, get_scenario, create_scenario, update_scenario,
delete_scenario, duplicate_scenario, mark_scenario_run, get_templates,
execute_scenario, _execute_step for simple step types: reorganize, photorec,
cloud_connect, cloud_backup, generate_report, cloud_download, metadata_enrich.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library.scenarios import (
    STEP_TYPES,
    Scenario,
    ScenarioStep,
    ScenarioTrigger,
    _execute_step,
    _load_scenarios,
    _save_scenarios,
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


# ── Data model tests ───────────────────────────────────────────────


class TestDataModel:
    def test_scenario_auto_id(self):
        sc = Scenario(name="Test")
        assert len(sc.id) == 8
        assert sc.created_at > 0

    def test_scenario_explicit_id(self):
        sc = Scenario(id="custom", name="Test")
        assert sc.id == "custom"

    def test_scenario_step(self):
        step = ScenarioStep(type="scan", config={"workers": 4})
        assert step.type == "scan"
        assert step.enabled is True

    def test_scenario_trigger_default(self):
        t = ScenarioTrigger()
        assert t.type == "manual"
        assert t.volume_name == ""

    def test_scenario_trigger_volume(self):
        t = ScenarioTrigger(type="volume_mount", volume_name="MyDisk")
        assert t.volume_name == "MyDisk"


# ── Step types ─────────────────────────────────────────────────────


class TestStepTypes:
    def test_all_step_types_have_label(self):
        for name, info in STEP_TYPES.items():
            assert "label_key" in info, f"Missing label_key for {name}"
            assert "icon" in info, f"Missing icon for {name}"


# ── Storage (with mocked path) ─────────────────────────────────────


class TestStorage:
    def test_load_empty(self, tmp_path):
        fake_path = tmp_path / "scenarios.json"
        with patch("godmode_media_library.scenarios._SCENARIOS_PATH", fake_path):
            result = _load_scenarios()
        assert result == []

    def test_save_and_load(self, tmp_path):
        fake_path = tmp_path / ".config" / "gml" / "scenarios.json"
        sc = Scenario(name="TestSave", steps=[ScenarioStep(type="scan")])
        with patch("godmode_media_library.scenarios._SCENARIOS_PATH", fake_path):
            _save_scenarios([sc])
            loaded = _load_scenarios()
        assert len(loaded) == 1
        assert loaded[0].name == "TestSave"

    def test_load_corrupted_json(self, tmp_path):
        fake_path = tmp_path / "scenarios.json"
        fake_path.write_text("INVALID JSON{{{")
        with patch("godmode_media_library.scenarios._SCENARIOS_PATH", fake_path):
            result = _load_scenarios()
        assert result == []
        # Should have created a backup
        assert fake_path.with_suffix(".bak").exists()


# ── CRUD operations ────────────────────────────────────────────────


class TestCRUD:
    @pytest.fixture(autouse=True)
    def setup_path(self, tmp_path):
        self.fake_path = tmp_path / ".config" / "gml" / "scenarios.json"
        self._patcher = patch("godmode_media_library.scenarios._SCENARIOS_PATH", self.fake_path)
        self._patcher.start()
        yield
        self._patcher.stop()

    def test_list_empty(self):
        assert list_scenarios() == []

    def test_create_and_get(self):
        result = create_scenario({
            "name": "My Scenario",
            "steps": [{"type": "scan", "config": {}, "enabled": True}],
        })
        assert result["name"] == "My Scenario"
        assert len(result["steps"]) == 1

        # Get by ID
        fetched = get_scenario(result["id"])
        assert fetched is not None
        assert fetched["name"] == "My Scenario"

    def test_get_nonexistent(self):
        assert get_scenario("nonexistent") is None

    def test_update(self):
        created = create_scenario({"name": "Original"})
        updated = update_scenario(created["id"], {"name": "Updated"})
        assert updated is not None
        assert updated["name"] == "Updated"

    def test_update_nonexistent(self):
        result = update_scenario("nonexistent", {"name": "Nope"})
        assert result is None

    def test_update_steps_and_trigger(self):
        created = create_scenario({"name": "Test"})
        updated = update_scenario(created["id"], {
            "steps": [{"type": "scan", "config": {"workers": 8}, "enabled": True}],
            "trigger": {"type": "volume_mount", "volume_name": "MyDisk"},
        })
        assert updated is not None
        assert len(updated["steps"]) == 1
        assert updated["trigger"]["type"] == "volume_mount"

    def test_delete(self):
        created = create_scenario({"name": "ToDelete"})
        assert delete_scenario(created["id"]) is True
        assert get_scenario(created["id"]) is None

    def test_delete_nonexistent(self):
        assert delete_scenario("nonexistent") is False

    def test_duplicate(self):
        created = create_scenario({
            "name": "Original",
            "steps": [{"type": "scan", "config": {"workers": 4}, "enabled": True}],
        })
        duped = duplicate_scenario(created["id"])
        assert duped is not None
        assert "(kopie)" in duped["name"]
        assert duped["id"] != created["id"]
        # Steps should be copied
        assert len(duped["steps"]) == 1

    def test_duplicate_nonexistent(self):
        assert duplicate_scenario("nonexistent") is None

    def test_mark_scenario_run(self):
        created = create_scenario({"name": "RunMe"})
        mark_scenario_run(created["id"])
        fetched = get_scenario(created["id"])
        assert fetched["run_count"] == 1
        assert fetched["last_run_at"] is not None

    def test_list_multiple(self):
        create_scenario({"name": "A"})
        create_scenario({"name": "B"})
        all_sc = list_scenarios()
        assert len(all_sc) == 2


# ── Templates ──────────────────────────────────────────────────────


class TestTemplates:
    def test_get_templates_returns_list(self):
        templates = get_templates()
        assert isinstance(templates, list)
        assert len(templates) >= 5

    def test_template_structure(self):
        templates = get_templates()
        for t in templates:
            assert "id" in t
            assert "name" in t
            assert "steps" in t
            assert isinstance(t["steps"], list)

    def test_all_template_steps_are_valid(self):
        templates = get_templates()
        for t in templates:
            for step in t["steps"]:
                assert step["type"] in STEP_TYPES, (
                    f"Template '{t['name']}' has unknown step type: {step['type']}"
                )


# ── Step execution (simple steps) ──────────────────────────────────


class TestExecuteStep:
    def test_reorganize_step(self):
        result = _execute_step("reorganize", {}, "/tmp/cat.db", None)
        assert "note" in result

    def test_photorec_step(self):
        result = _execute_step("photorec", {}, "/tmp/cat.db", None)
        assert "note" in result

    def test_cloud_backup_no_remote(self):
        result = _execute_step("cloud_backup", {}, "/tmp/cat.db", None)
        assert "note" in result

    def test_cloud_backup_with_remote(self):
        result = _execute_step("cloud_backup", {"remote_name": "gdrive"}, "/tmp/cat.db", None)
        assert "gdrive" in result["note"]

    def test_cloud_connect(self):
        with patch("godmode_media_library.cloud.list_remotes", return_value=[]):
            result = _execute_step("cloud_connect", {}, "/tmp/cat.db", None)
        assert "remotes_available" in result

    def test_cloud_download(self):
        with patch("godmode_media_library.cloud.list_remotes", return_value=[]):
            result = _execute_step("cloud_download", {}, "/tmp/cat.db", None)
        assert "sources_ready" in result


# ── Execute scenario (integration with mocks) ─────────────────────


class TestExecuteScenario:
    @pytest.fixture(autouse=True)
    def setup_path(self, tmp_path):
        self.fake_path = tmp_path / ".config" / "gml" / "scenarios.json"
        self._patcher = patch("godmode_media_library.scenarios._SCENARIOS_PATH", self.fake_path)
        self._patcher.start()
        yield
        self._patcher.stop()

    def test_execute_nonexistent(self):
        result = execute_scenario("nonexistent", "/tmp/cat.db")
        assert "error" in result

    def test_execute_simple_scenario(self):
        sc = create_scenario({
            "name": "Simple",
            "steps": [
                {"type": "reorganize", "config": {}, "enabled": True},
                {"type": "photorec", "config": {}, "enabled": True},
            ],
        })
        result = execute_scenario(sc["id"], "/tmp/cat.db")
        assert result["completed"] == 2
        assert result["failed"] == 0

    def test_execute_with_disabled_steps(self):
        sc = create_scenario({
            "name": "MixedSteps",
            "steps": [
                {"type": "reorganize", "config": {}, "enabled": True},
                {"type": "photorec", "config": {}, "enabled": False},  # Disabled
            ],
        })
        result = execute_scenario(sc["id"], "/tmp/cat.db")
        assert result["total_steps"] == 1  # Only enabled steps

    def test_execute_with_progress(self):
        sc = create_scenario({
            "name": "WithProgress",
            "steps": [{"type": "reorganize", "config": {}, "enabled": True}],
        })
        progress_calls = []
        result = execute_scenario(sc["id"], "/tmp/cat.db", progress_fn=progress_calls.append)
        assert result["completed"] == 1
        assert len(progress_calls) >= 2  # step + complete

    def test_execute_step_that_fails(self):
        sc = create_scenario({
            "name": "FailStep",
            "steps": [
                {"type": "deep_scan", "config": {}, "enabled": True},  # Will fail (no recovery module mock)
            ],
        })
        # deep_scan tries to import recovery.deep_scan which may fail
        result = execute_scenario(sc["id"], "/tmp/cat.db")
        # Either completed or failed, but should not crash
        assert result["total_steps"] == 1
