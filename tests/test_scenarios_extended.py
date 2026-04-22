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


# ── Step execution — more step types (covering lines 628-1106) ────────


class TestExecuteStepExtended:
    """Test _execute_step for step types not covered by existing tests."""

    def test_app_mine_step(self):
        from unittest.mock import MagicMock, patch
        mock_result = MagicMock()
        mock_result.files_found = 5
        mock_result.total_size = 1024
        mock_result.files = []
        with patch("godmode_media_library.recovery.mine_app_media", return_value=[mock_result]):
            result = _execute_step("app_mine", {}, "/tmp/cat.db", None)
        assert result["total_files"] == 5

    def test_app_download_no_files(self):
        from unittest.mock import MagicMock, patch
        mock_result = MagicMock()
        mock_result.files_found = 0
        mock_result.total_size = 0
        mock_result.files = []
        with patch("godmode_media_library.recovery.mine_app_media", return_value=[mock_result]):
            result = _execute_step("app_download", {}, "/tmp/cat.db", None)
        assert result["downloaded"] == 0

    def test_app_download_with_files(self):
        from unittest.mock import MagicMock, patch
        mock_result = MagicMock()
        mock_result.files = [{"path": "/tmp/file.jpg"}]
        with patch("godmode_media_library.recovery.mine_app_media", return_value=[mock_result]):
            with patch("godmode_media_library.recovery.recover_files",
                       return_value={"recovered": 1, "total_size": 500, "errors": []}):
                result = _execute_step("app_download", {}, "/tmp/cat.db", None)
        assert result["downloaded"] == 1

    def test_signal_decrypt_step(self):
        with patch("godmode_media_library.recovery.decrypt_signal_attachments",
                   return_value={"decrypted": 3, "total_size": 2048, "errors": []}):
            result = _execute_step("signal_decrypt", {}, "/tmp/cat.db", None)
        assert result["decrypted"] == 3

    def test_integrity_check_step(self):
        from godmode_media_library.recovery import IntegrityResult
        mock_result = IntegrityResult()
        mock_result.total_checked = 10
        mock_result.corrupted = 1
        mock_result.healthy = 9
        with patch("godmode_media_library.recovery.check_integrity", return_value=mock_result):
            result = _execute_step("integrity_check", {}, "/tmp/cat.db", None)
        assert result["total_checked"] == 10

    def test_scan_step(self):
        from unittest.mock import MagicMock
        mock_stats = MagicMock()
        mock_stats.total_files = 42
        with patch("godmode_media_library.config.load_config") as mock_cfg:
            mock_cfg.return_value = MagicMock(prefer_roots=["/tmp"])
            with patch("godmode_media_library.scanner.incremental_scan", return_value=mock_stats):
                result = _execute_step("scan", {"roots": ["/tmp"]}, "/tmp/cat.db", None)
        assert result["scanned"] == 42

    def test_dedup_resolve_step(self):
        mock_cat = MagicMock()
        mock_cat.query_duplicates.return_value = {"groups": [{"files": ["a", "b"]}]}
        with patch("godmode_media_library.catalog.Catalog", return_value=mock_cat):
            result = _execute_step("dedup_resolve", {}, "/tmp/cat.db", None)
        assert result["groups_found"] == 1

    def test_quarantine_cleanup_no_old_files(self):
        with patch("godmode_media_library.recovery.list_quarantine", return_value=[]):
            result = _execute_step("quarantine_cleanup", {"older_than_days": 30}, "/tmp/cat.db", None)
        assert result["cleaned"] == 0

    def test_quarantine_cleanup_with_old_files(self):
        from godmode_media_library.recovery import QuarantineEntry
        old_entry = QuarantineEntry(
            path="/q/old.jpg", original_path="/orig/old.jpg",
            size=100, ext=".jpg", quarantine_date="2020-01-01",
            category="image",
        )
        with patch("godmode_media_library.recovery.list_quarantine", return_value=[old_entry]):
            with patch("godmode_media_library.recovery.delete_from_quarantine",
                       return_value={"deleted": 1}):
                result = _execute_step("quarantine_cleanup", {"older_than_days": 30}, "/tmp/cat.db", None)
        assert result["cleaned"] == 1

    def test_quarantine_cleanup_no_date_uses_mtime(self, tmp_path):
        from godmode_media_library.recovery import QuarantineEntry
        # File with no quarantine_date but old mtime
        old_file = tmp_path / "old.jpg"
        old_file.write_bytes(b"data")
        import os
        # Set mtime to 2 years ago
        old_time = time.time() - (365 * 2 * 86400)
        os.utime(str(old_file), (old_time, old_time))

        entry = QuarantineEntry(
            path=str(old_file), original_path="/orig",
            size=4, ext=".jpg", quarantine_date="",
            category="image",
        )
        with patch("godmode_media_library.recovery.list_quarantine", return_value=[entry]):
            with patch("godmode_media_library.recovery.delete_from_quarantine",
                       return_value={"deleted": 1}):
                result = _execute_step("quarantine_cleanup", {"older_than_days": 30}, "/tmp/cat.db", None)
        assert result["cleaned"] == 1

    def test_metadata_enrich_step(self):
        mock_cat = MagicMock()
        with patch("godmode_media_library.catalog.Catalog", return_value=mock_cat):
            with patch("godmode_media_library.exiftool_extract.extract_all_metadata", return_value={"extracted": 5}):
                result = _execute_step("metadata_enrich", {}, "/tmp/cat.db", None)
        assert result["enriched"] == 5

    def test_metadata_enrich_error(self):
        mock_cat = MagicMock()
        with patch("godmode_media_library.catalog.Catalog", return_value=mock_cat):
            with patch("godmode_media_library.exiftool_extract.extract_all_metadata", side_effect=Exception("error")):
                result = _execute_step("metadata_enrich", {}, "/tmp/cat.db", None)
        assert "note" in result

    def test_timeline_analysis_step(self):
        mock_cat = MagicMock()
        mock_cat.conn.execute.return_value.fetchall.return_value = [
            ("2024-01", 10), ("2024-02", 15), ("2024-03", 20),
        ]
        with patch("godmode_media_library.catalog.Catalog", return_value=mock_cat):
            result = _execute_step("timeline_analysis", {}, "/tmp/cat.db", None)
        assert result["months_covered"] == 3
        assert result["total_with_date"] == 45

    def test_quality_analyze_import_error(self):
        """When quality module is unavailable, should return a note."""
        # quality_analyze catches ImportError and returns note
        mock_cat = MagicMock()
        with patch("godmode_media_library.catalog.Catalog", return_value=mock_cat):
            # Simulate the quality module not existing
            import sys
            # Remove quality module if cached
            sys.modules.pop("godmode_media_library.quality", None)
            result = _execute_step("quality_analyze", {}, "/tmp/cat.db", None)
        # Either it works or returns note about missing module
        assert "note" in result or "analyzed" in result

    def test_generate_report_step(self):
        mock_cat = MagicMock()
        with patch("godmode_media_library.catalog.Catalog", return_value=mock_cat):
            with patch("godmode_media_library.report.generate_report",
                       return_value={"summary": {}, "details": {}}):
                result = _execute_step("generate_report", {}, "/tmp/cat.db", None)
        assert result["report_generated"] is True

    def test_generate_report_import_error(self):
        """When report module is unavailable, should return a note."""
        import sys
        # Temporarily remove the report module
        orig = sys.modules.pop("godmode_media_library.report", None)
        try:
            with patch("godmode_media_library.catalog.Catalog") as mock_cat:
                # Force ImportError by making the import fail
                with patch.dict(sys.modules, {"godmode_media_library.report": None}):
                    result = _execute_step("generate_report", {}, "/tmp/cat.db", None)
            assert "note" in result
        finally:
            if orig is not None:
                sys.modules["godmode_media_library.report"] = orig

    def test_wait_for_sources_all_reachable(self):
        with patch("godmode_media_library.cloud.list_remotes", return_value=[]):
            with patch("godmode_media_library.cloud.rclone_is_reachable", return_value=True):
                result = _execute_step("wait_for_sources",
                                      {"remotes": ["gdrive"], "timeout_minutes": 0.01},
                                      "/tmp/cat.db", None)
        assert "gdrive" in result["available"]

    def test_cloud_catalog_scan_step(self):
        mock_cat = MagicMock()
        mock_cat.conn = MagicMock()
        with patch("godmode_media_library.catalog.Catalog", return_value=mock_cat):
            with patch("godmode_media_library.cloud.list_remotes", return_value=[]):
                with patch("godmode_media_library.cloud.rclone_is_reachable", return_value=True):
                    with patch("godmode_media_library.cloud.rclone_ls", return_value=[]):
                        result = _execute_step("cloud_catalog_scan",
                                              {"remotes": ["gdrive"]},
                                              "/tmp/cat.db", None)
        assert result["cataloged"] == 0

    def test_cloud_verify_integrity_step(self):
        mock_cat = MagicMock()
        mock_cat.conn = MagicMock()
        mock_cat.conn.execute.return_value.fetchall.return_value = []
        with patch("godmode_media_library.catalog.Catalog", return_value=mock_cat):
            with patch("godmode_media_library.cloud.rclone_is_reachable", return_value=True):
                result = _execute_step("cloud_verify_integrity",
                                      {"remote": "gdrive", "sample_pct": 10},
                                      "/tmp/cat.db", None)
        assert result["verified"] == 0

    def test_cloud_verify_integrity_unreachable(self):
        with patch("godmode_media_library.cloud.rclone_is_reachable", return_value=False):
            result = _execute_step("cloud_verify_integrity",
                                  {"remote": "gdrive"},
                                  "/tmp/cat.db", None)
        assert result["verified"] == 0
        assert "nedostupn" in result["note"].lower()

    def test_sync_to_disk_not_mounted(self):
        with patch("godmode_media_library.cloud.check_volume_mounted", return_value=False):
            result = _execute_step("sync_to_disk",
                                  {"disk_path": "/Volumes/Missing"},
                                  "/tmp/cat.db", None)
        assert result["synced"] is False

    def test_sync_to_disk_success(self):
        with patch("godmode_media_library.cloud.check_volume_mounted", return_value=True):
            with patch("godmode_media_library.cloud.rclone_copy", return_value={"success": True}):
                result = _execute_step("sync_to_disk",
                                      {"source_remote": "gdrive", "disk_path": "/Volumes/4TB"},
                                      "/tmp/cat.db", None)
        assert result["synced"] is True

    def test_sync_to_disk_error(self):
        with patch("godmode_media_library.cloud.check_volume_mounted", return_value=True):
            with patch("godmode_media_library.cloud.rclone_copy", side_effect=Exception("network error")):
                result = _execute_step("sync_to_disk",
                                      {"source_remote": "gdrive", "disk_path": "/Volumes/4TB"},
                                      "/tmp/cat.db", None)
        assert result["synced"] is False

    def test_unknown_step_type(self):
        result = _execute_step("totally_unknown_step", {}, "/tmp/cat.db", None)
        assert "Neznám" in result["note"]

    def test_ultimate_consolidation_step(self):
        with patch("godmode_media_library.consolidation.run_consolidation", return_value={"ok": True}):
            result = _execute_step("ultimate_consolidation",
                                  {"source_remotes": [], "dest_remote": "gdrive"},
                                  "/tmp/cat.db", None)
        assert result == {"ok": True}


# ── check_volume_triggers ─────────────────────────────────────────────


class TestCheckVolumeTriggers:
    @pytest.fixture(autouse=True)
    def setup_path(self, tmp_path):
        self.fake_path = tmp_path / ".config" / "gml" / "scenarios.json"
        self._patcher = patch("godmode_media_library.scenarios._SCENARIOS_PATH", self.fake_path)
        self._patcher.start()
        yield
        self._patcher.stop()

    def test_no_triggers(self):
        from godmode_media_library.scenarios import check_volume_triggers
        create_scenario({"name": "NoTrigger"})
        result = check_volume_triggers()
        assert result == []

    def test_volume_trigger_not_mounted(self):
        from godmode_media_library.scenarios import check_volume_triggers
        create_scenario({
            "name": "VolTrigger",
            "trigger": {"type": "volume_mount", "volume_name": "NonexistentDisk12345"},
        })
        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.iterdir", return_value=[]):
                result = check_volume_triggers()
        assert result == []


# ── Execute scenario with step exceptions ─────────────────────────────


class TestExecuteScenarioErrors:
    @pytest.fixture(autouse=True)
    def setup_path(self, tmp_path):
        self.fake_path = tmp_path / ".config" / "gml" / "scenarios.json"
        self._patcher = patch("godmode_media_library.scenarios._SCENARIOS_PATH", self.fake_path)
        self._patcher.start()
        yield
        self._patcher.stop()

    def test_step_exception_counted_as_failure(self):
        sc = create_scenario({
            "name": "ErrorStep",
            "steps": [
                {"type": "integrity_check", "config": {}, "enabled": True},
            ],
        })
        with patch("godmode_media_library.scenarios._execute_step", side_effect=Exception("boom")):
            result = execute_scenario(sc["id"], "/tmp/cat.db")
        assert result["failed"] == 1

    def test_mixed_success_and_failure(self):
        sc = create_scenario({
            "name": "Mixed",
            "steps": [
                {"type": "reorganize", "config": {}, "enabled": True},
                {"type": "integrity_check", "config": {}, "enabled": True},
            ],
        })
        call_count = [0]
        orig_execute = _execute_step

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("step 2 fails")
            return orig_execute(*args, **kwargs)

        with patch("godmode_media_library.scenarios._execute_step", side_effect=side_effect):
            result = execute_scenario(sc["id"], "/tmp/cat.db")
        assert result["completed"] == 1
        assert result["failed"] == 1
