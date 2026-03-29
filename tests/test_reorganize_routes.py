"""Tests for the reorganize route endpoints (/api/config/dedup-rules, /api/reorganize/*).

Targets coverage of src/godmode_media_library/web/routes/reorganize.py from ~58% to 80%+.
"""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

try:
    from fastapi.testclient import TestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

pytestmark = pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi not installed")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_db(tmp_path):
    """Create a minimal catalog database and return its path."""
    from godmode_media_library.catalog import Catalog

    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()
    return db_path


@pytest.fixture
def client(catalog_db):
    """TestClient backed by a fresh app (no auth)."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_db)
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# GET /api/config/dedup-rules
# ---------------------------------------------------------------------------


class TestGetDedupRules:
    def test_returns_default_values(self, client):
        """Mock load_config to return a pristine default GMLConfig."""
        from godmode_media_library.config import GMLConfig

        with patch("godmode_media_library.config.load_config", return_value=GMLConfig()):
            resp = client.get("/api/config/dedup-rules")
        assert resp.status_code == 200
        data = resp.json()
        assert data["strategy"] == "richness"
        assert data["similarity_threshold"] == 10
        assert data["auto_resolve"] is False
        assert data["merge_metadata"] is True
        assert data["quarantine_path"] == ""
        assert data["exclude_extensions"] == []
        assert data["exclude_paths"] == []
        assert data["min_file_size_kb"] == 0

    def test_returns_all_expected_keys(self, client):
        resp = client.get("/api/config/dedup-rules")
        data = resp.json()
        expected_keys = {
            "strategy",
            "similarity_threshold",
            "auto_resolve",
            "merge_metadata",
            "quarantine_path",
            "exclude_extensions",
            "exclude_paths",
            "min_file_size_kb",
        }
        assert set(data.keys()) == expected_keys

    def test_reflects_custom_config(self, client):
        """Verify endpoint returns values from a customised config."""
        from godmode_media_library.config import GMLConfig

        custom = GMLConfig(
            dedup_strategy="newest",
            dedup_similarity_threshold=25,
            dedup_auto_resolve=True,
            dedup_merge_metadata=False,
            dedup_quarantine_path="/custom/q",
            dedup_exclude_extensions=[".raw"],
            dedup_exclude_paths=["/skip"],
            dedup_min_file_size_kb=512,
        )
        with patch("godmode_media_library.config.load_config", return_value=custom):
            resp = client.get("/api/config/dedup-rules")
        assert resp.status_code == 200
        data = resp.json()
        assert data["strategy"] == "newest"
        assert data["similarity_threshold"] == 25
        assert data["auto_resolve"] is True
        assert data["merge_metadata"] is False
        assert data["quarantine_path"] == "/custom/q"
        assert data["exclude_extensions"] == [".raw"]
        assert data["exclude_paths"] == ["/skip"]
        assert data["min_file_size_kb"] == 512


# ---------------------------------------------------------------------------
# PUT /api/config/dedup-rules
# ---------------------------------------------------------------------------


class TestPutDedupRules:
    def test_update_rules_returns_ok(self, client, tmp_path):
        config_toml = tmp_path / "dedup_cfg" / "config.toml"
        with patch(
            "godmode_media_library.config._global_config_path",
            return_value=config_toml,
        ):
            resp = client.put(
                "/api/config/dedup-rules",
                json={
                    "strategy": "newest",
                    "similarity_threshold": 20,
                    "auto_resolve": True,
                    "merge_metadata": False,
                    "quarantine_path": "/tmp/q",
                    "exclude_extensions": [".raw"],
                    "exclude_paths": ["/skip"],
                    "min_file_size_kb": 100,
                },
            )
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    def test_update_rules_persists_to_file(self, client, tmp_path):
        config_toml = tmp_path / "config.toml"
        with patch(
            "godmode_media_library.config._global_config_path",
            return_value=config_toml,
        ):
            client.put(
                "/api/config/dedup-rules",
                json={
                    "strategy": "largest",
                    "similarity_threshold": 5,
                    "auto_resolve": False,
                    "merge_metadata": True,
                    "quarantine_path": "",
                    "exclude_extensions": [".tmp", ".bak"],
                    "exclude_paths": [],
                    "min_file_size_kb": 50,
                },
            )

            # Verify the file was written
            assert config_toml.exists()
            content = config_toml.read_text()
            assert 'dedup_strategy = "largest"' in content
            assert "dedup_similarity_threshold = 5" in content
            assert "dedup_auto_resolve = false" in content
            assert "dedup_merge_metadata = true" in content
            assert "dedup_min_file_size_kb = 50" in content

    def test_update_rules_with_existing_config(self, client, tmp_path):
        config_toml = tmp_path / "config.toml"
        # Write an existing config with some other keys
        config_toml.write_text('log_level = "debug"\n')

        with patch(
            "godmode_media_library.config._global_config_path",
            return_value=config_toml,
        ):
            resp = client.put(
                "/api/config/dedup-rules",
                json={
                    "strategy": "manual",
                    "similarity_threshold": 15,
                    "auto_resolve": False,
                    "merge_metadata": True,
                    "quarantine_path": "",
                    "exclude_extensions": [],
                    "exclude_paths": [],
                    "min_file_size_kb": 0,
                },
            )

        assert resp.status_code == 200
        content = config_toml.read_text()
        # Original key preserved
        assert 'log_level = "debug"' in content
        # New key added
        assert 'dedup_strategy = "manual"' in content

    def test_update_with_defaults_only(self, client, tmp_path):
        """PUT with default model values works fine."""
        config_toml = tmp_path / "config.toml"
        with patch(
            "godmode_media_library.config._global_config_path",
            return_value=config_toml,
        ):
            resp = client.put("/api/config/dedup-rules", json={})
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    def test_update_creates_parent_directory(self, client, tmp_path):
        """PUT creates parent directories if they do not exist."""
        config_toml = tmp_path / "a" / "b" / "config.toml"
        with patch(
            "godmode_media_library.config._global_config_path",
            return_value=config_toml,
        ):
            resp = client.put(
                "/api/config/dedup-rules",
                json={"strategy": "richness"},
            )
        assert resp.status_code == 200
        assert config_toml.exists()


# ---------------------------------------------------------------------------
# GET /api/reorganize/sources
# ---------------------------------------------------------------------------


class TestGetReorganizeSources:
    def test_returns_sources_list(self, client):
        resp = client.get("/api/reorganize/sources")
        assert resp.status_code == 200
        data = resp.json()
        assert "sources" in data
        assert isinstance(data["sources"], list)

    def test_sources_with_mock(self, client):
        fake_sources = [
            {"name": "Photos", "path": "/Users/test/Photos", "icon": "photo", "type": "mac", "available": True, "file_count": 42},
        ]
        with patch("godmode_media_library.reorganize.detect_sources", return_value=fake_sources):
            resp = client.get("/api/reorganize/sources")
        assert resp.status_code == 200
        sources = resp.json()["sources"]
        assert len(sources) == 1
        assert sources[0]["name"] == "Photos"
        assert sources[0]["file_count"] == 42

    def test_sources_empty(self, client):
        """When no sources are detected, returns empty list."""
        with patch("godmode_media_library.reorganize.detect_sources", return_value=[]):
            resp = client.get("/api/reorganize/sources")
        assert resp.status_code == 200
        assert resp.json()["sources"] == []


# ---------------------------------------------------------------------------
# POST /api/reorganize/plan
# ---------------------------------------------------------------------------


class TestPostReorganizePlan:
    def test_plan_returns_task_id(self, client, tmp_path):
        """A valid plan request returns a task_id and status='started'."""
        src = tmp_path / "source"
        src.mkdir()
        (src / "photo.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)
        dest = tmp_path / "dest"
        dest.mkdir()

        resp = client.post(
            "/api/reorganize/plan",
            json={
                "sources": [str(src)],
                "destination": str(dest),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_plan_with_all_options(self, client, tmp_path):
        """Plan request with all optional fields succeeds."""
        src = tmp_path / "src"
        src.mkdir()
        (src / "img.png").write_bytes(b"\x89PNG" + b"\x00" * 50)
        dest = tmp_path / "organized"
        dest.mkdir()

        resp = client.post(
            "/api/reorganize/plan",
            json={
                "sources": [str(src)],
                "destination": str(dest),
                "structure_pattern": "year_month",
                "deduplicate": True,
                "merge_metadata": False,
                "delete_originals": False,
                "dry_run": True,
                "workers": 2,
                "exclude_patterns": ["*.tmp"],
            },
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "started"

    def test_plan_empty_sources_starts_task(self, client, tmp_path):
        """Empty sources list still starts a task (error handled in background)."""
        dest = tmp_path / "dest"
        dest.mkdir()
        resp = client.post(
            "/api/reorganize/plan",
            json={
                "sources": [],
                "destination": str(dest),
            },
        )
        assert resp.status_code == 200
        assert "task_id" in resp.json()

    def test_plan_missing_destination_field(self, client):
        """Missing required 'destination' field returns 422."""
        resp = client.post(
            "/api/reorganize/plan",
            json={"sources": ["/some/path"]},
        )
        assert resp.status_code == 422

    def test_plan_missing_sources_field(self, client):
        """Missing required 'sources' field returns 422."""
        resp = client.post(
            "/api/reorganize/plan",
            json={"destination": "/some/dest"},
        )
        assert resp.status_code == 422

    def test_plan_background_task_completes(self, client, tmp_path):
        """Verify the background task runs and produces a result via task status."""
        src = tmp_path / "media_src"
        src.mkdir()
        for i in range(3):
            (src / f"file{i}.jpg").write_bytes(b"\xff\xd8\xff" + bytes([i]) * 50)
        dest = tmp_path / "media_dest"
        dest.mkdir()

        from godmode_media_library.reorganize import ReorganizeConfig, ReorganizePlan

        mock_plan = ReorganizePlan(
            config=ReorganizeConfig(sources=[src], destination=dest),
            total_files=3,
            unique_files=2,
            duplicate_files=1,
            total_size=150,
            unique_size=100,
            duplicate_size=50,
            categories={"image": 3},
            source_stats={str(src): 3},
            errors=[],
        )
        with patch(
            "godmode_media_library.reorganize.plan_reorganization",
            return_value=mock_plan,
        ):
            resp = client.post(
                "/api/reorganize/plan",
                json={
                    "sources": [str(src)],
                    "destination": str(dest),
                },
            )
        assert resp.status_code == 200
        task_id = resp.json()["task_id"]

        # The background task should have completed (TestClient runs sync)
        status_resp = client.get(f"/api/tasks/{task_id}")
        assert status_resp.status_code == 200
        task_data = status_resp.json()
        assert task_data["id"] == task_id

    def test_plan_nonexistent_source_starts_task(self, client, tmp_path):
        """Non-existent source path still starts a task (error handled in background)."""
        dest = tmp_path / "dest"
        dest.mkdir()
        resp = client.post(
            "/api/reorganize/plan",
            json={
                "sources": ["/nonexistent/path/xyz123"],
                "destination": str(dest),
            },
        )
        assert resp.status_code == 200
        assert "task_id" in resp.json()

    def test_plan_multiple_sources(self, client, tmp_path):
        """Plan with multiple source directories."""
        src1 = tmp_path / "src1"
        src1.mkdir()
        (src1 / "a.jpg").write_bytes(b"\xff" * 20)
        src2 = tmp_path / "src2"
        src2.mkdir()
        (src2 / "b.jpg").write_bytes(b"\xff" * 20)
        dest = tmp_path / "dest"
        dest.mkdir()

        resp = client.post(
            "/api/reorganize/plan",
            json={
                "sources": [str(src1), str(src2)],
                "destination": str(dest),
            },
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "started"


# ---------------------------------------------------------------------------
# POST /api/reorganize/execute
# ---------------------------------------------------------------------------


class TestPostReorganizeExecute:
    def test_execute_plan_not_found(self, client):
        """Execute with unknown plan_id returns 404."""
        resp = client.post(
            "/api/reorganize/execute",
            json={"plan_id": "nonexistent-plan-id"},
        )
        assert resp.status_code == 404
        assert "Plan not found" in resp.json()["detail"]

    def test_execute_missing_plan_id(self, client):
        """Missing plan_id field returns 422."""
        resp = client.post("/api/reorganize/execute", json={})
        assert resp.status_code == 422

    def test_execute_expired_plan(self, client):
        """A plan that has expired (past TTL) is evicted and returns 404."""
        from godmode_media_library.web.shared import (
            _reorganize_plans,
            _reorganize_plans_lock,
        )

        fake_plan = MagicMock()
        # Insert a plan with a timestamp far in the past (expired)
        with _reorganize_plans_lock:
            _reorganize_plans["expired-plan"] = (time.monotonic() - 7200, fake_plan)

        try:
            resp = client.post(
                "/api/reorganize/execute",
                json={"plan_id": "expired-plan"},
            )
            assert resp.status_code == 404
            assert "Plan not found" in resp.json()["detail"]
        finally:
            with _reorganize_plans_lock:
                _reorganize_plans.pop("expired-plan", None)

    def test_execute_valid_plan(self, client, tmp_path):
        """A valid stored plan can be executed and returns a task_id."""
        from godmode_media_library.reorganize import ReorganizeConfig, ReorganizePlan
        from godmode_media_library.web.shared import (
            _reorganize_plans,
            _reorganize_plans_lock,
        )

        src = tmp_path / "src"
        src.mkdir()
        dest = tmp_path / "dest"
        dest.mkdir()

        plan = ReorganizePlan(
            config=ReorganizeConfig(sources=[src], destination=dest),
            total_files=1,
            unique_files=1,
        )

        plan_id = "test-valid-plan-123"
        with _reorganize_plans_lock:
            _reorganize_plans[plan_id] = (time.monotonic(), plan)

        mock_result = MagicMock()
        mock_result.files_processed = 1
        mock_result.files_copied = 1
        mock_result.files_skipped = 0
        mock_result.originals_deleted = 0
        mock_result.space_saved = 0
        mock_result.errors = []

        try:
            with patch(
                "godmode_media_library.reorganize.execute_reorganization",
                return_value=mock_result,
            ):
                resp = client.post(
                    "/api/reorganize/execute",
                    json={"plan_id": plan_id, "delete_originals": False},
                )

            assert resp.status_code == 200
            data = resp.json()
            assert "task_id" in data
            assert data["status"] == "started"
        finally:
            with _reorganize_plans_lock:
                _reorganize_plans.pop(plan_id, None)

    def test_execute_sets_delete_originals(self, client, tmp_path):
        """Execute request properly passes delete_originals to the plan config."""
        from godmode_media_library.reorganize import ReorganizeConfig, ReorganizePlan
        from godmode_media_library.web.shared import (
            _reorganize_plans,
            _reorganize_plans_lock,
        )

        src = tmp_path / "src"
        src.mkdir()
        dest = tmp_path / "dest"
        dest.mkdir()

        config = ReorganizeConfig(sources=[src], destination=dest, delete_originals=False)
        plan = ReorganizePlan(config=config, total_files=0, unique_files=0)

        plan_id = "test-delete-flag-plan"
        with _reorganize_plans_lock:
            _reorganize_plans[plan_id] = (time.monotonic(), plan)

        mock_result = MagicMock()
        mock_result.files_processed = 0
        mock_result.files_copied = 0
        mock_result.files_skipped = 0
        mock_result.originals_deleted = 0
        mock_result.space_saved = 0
        mock_result.errors = []

        try:
            with patch(
                "godmode_media_library.reorganize.execute_reorganization",
                return_value=mock_result,
            ):
                resp = client.post(
                    "/api/reorganize/execute",
                    json={"plan_id": plan_id, "delete_originals": True},
                )

            assert resp.status_code == 200
            # The plan config should have been updated before the bg task runs
            assert plan.config.delete_originals is True
            assert plan.config.dry_run is False
        finally:
            with _reorganize_plans_lock:
                _reorganize_plans.pop(plan_id, None)

    def test_execute_with_delete_originals_default_false(self, client, tmp_path):
        """Execute without explicit delete_originals defaults to False."""
        from godmode_media_library.reorganize import ReorganizeConfig, ReorganizePlan
        from godmode_media_library.web.shared import (
            _reorganize_plans,
            _reorganize_plans_lock,
        )

        src = tmp_path / "src"
        src.mkdir()
        dest = tmp_path / "dest"
        dest.mkdir()

        config = ReorganizeConfig(sources=[src], destination=dest, delete_originals=True)
        plan = ReorganizePlan(config=config)

        plan_id = "test-default-delete-plan"
        with _reorganize_plans_lock:
            _reorganize_plans[plan_id] = (time.monotonic(), plan)

        mock_result = MagicMock()
        mock_result.files_processed = 0
        mock_result.files_copied = 0
        mock_result.files_skipped = 0
        mock_result.originals_deleted = 0
        mock_result.space_saved = 0
        mock_result.errors = []

        try:
            with patch(
                "godmode_media_library.reorganize.execute_reorganization",
                return_value=mock_result,
            ):
                resp = client.post(
                    "/api/reorganize/execute",
                    json={"plan_id": plan_id},
                )

            assert resp.status_code == 200
            # delete_originals should be False (the default from ReorganizeExecuteRequest)
            assert plan.config.delete_originals is False
        finally:
            with _reorganize_plans_lock:
                _reorganize_plans.pop(plan_id, None)

    def test_execute_invalid_json(self, client):
        """Malformed JSON body returns an error."""
        resp = client.post(
            "/api/reorganize/execute",
            content=b"not json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 422
