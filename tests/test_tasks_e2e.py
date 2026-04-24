"""End-to-end tests for the Task tracking system and WebSocket updates."""

from __future__ import annotations

import threading
import time
import uuid
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
    """Create a catalog with some test files for scanning."""
    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()

    root = tmp_path / "media"
    root.mkdir()
    (root / "photo1.jpg").write_bytes(b"content1")
    (root / "photo2.jpg").write_bytes(b"content2")
    (root / "photo3.jpg").write_bytes(b"content3")

    from godmode_media_library.scanner import incremental_scan

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
    ):
        incremental_scan(cat, [root])
    cat.close()
    return db_path, root


@pytest.fixture
def client(catalog_with_files):
    """Create a test client with a populated catalog."""
    from godmode_media_library.web.app import create_app

    db_path, _ = catalog_with_files
    app = create_app(catalog_path=db_path)
    return TestClient(app)


@pytest.fixture
def client_and_root(catalog_with_files):
    """Create a test client and return the media root for scan tests."""
    from godmode_media_library.web.app import create_app

    db_path, root = catalog_with_files
    app = create_app(catalog_path=db_path)
    return TestClient(app), root


@pytest.fixture(autouse=True)
def _clear_tasks():
    """Clear the global task store before each test to avoid cross-contamination."""
    from godmode_media_library.web import api as api_mod

    with api_mod._tasks_lock:
        api_mod._tasks.clear()
    yield
    with api_mod._tasks_lock:
        api_mod._tasks.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _inject_task(command="test", status="running", progress=None, result=None, error=None, finished_at=None, created_ts_offset=0):
    """Directly inject a task into the global store for deterministic tests."""
    from datetime import datetime, timezone

    from godmode_media_library.web import api as api_mod

    task = api_mod.TaskStatus(
        id=str(uuid.uuid4())[:8],
        command=command,
        status=status,
        progress=progress,
        result=result,
        started_at=datetime.now(timezone.utc).isoformat(),
        finished_at=finished_at,
        error=error,
    )
    if created_ts_offset:
        task._created_ts = time.monotonic() + created_ts_offset
    with api_mod._tasks_lock:
        api_mod._tasks[task.id] = task
    return task


def _wait_for_task(client, task_id, timeout=10):
    """Poll GET /api/tasks/{id} until terminal state or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(f"/api/tasks/{task_id}")
        assert resp.status_code == 200
        data = resp.json()
        if data["status"] in ("completed", "failed"):
            return data
        time.sleep(0.15)
    raise TimeoutError(f"Task {task_id} did not finish within {timeout}s")


# ===========================================================================
# 1. Task Lifecycle
# ===========================================================================


class TestTaskLifecycle:
    """Tests for task creation, status transitions, and completion."""

    def test_created_task_has_running_status(self, client):
        """A freshly created task starts in 'running' status."""
        task = _inject_task(status="running")
        resp = client.get(f"/api/tasks/{task.id}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "running"

    def test_task_transitions_to_completed(self, client):
        """Finishing a task without error sets status to 'completed'."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task(status="running")
        api_mod._finish_task(task.id, result={"ok": True})

        resp = client.get(f"/api/tasks/{task.id}")
        data = resp.json()
        assert data["status"] == "completed"
        assert data["result"] == {"ok": True}
        assert data["finished_at"] is not None

    def test_task_transitions_to_failed(self, client):
        """Finishing a task with an error sets status to 'failed'."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task(status="running")
        api_mod._finish_task(task.id, error="Something went wrong")

        resp = client.get(f"/api/tasks/{task.id}")
        data = resp.json()
        assert data["status"] == "failed"
        assert data["error"] == "Something went wrong"
        assert data["finished_at"] is not None

    def test_task_includes_progress_field(self, client):
        """Progress dict is returned in task status."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task(status="running")
        api_mod._update_progress(task.id, {"pct": 42, "message": "Scanning files..."})

        resp = client.get(f"/api/tasks/{task.id}")
        data = resp.json()
        assert data["progress"]["pct"] == 42
        assert "Scanning" in data["progress"]["message"]

    def test_task_has_started_at_timestamp(self, client):
        """Every task has a started_at ISO timestamp."""
        task = _inject_task()
        resp = client.get(f"/api/tasks/{task.id}")
        data = resp.json()
        assert data["started_at"] is not None
        assert "T" in data["started_at"]  # ISO format

    def test_scan_creates_task_and_returns_id(self, client_and_root):
        """POST /api/scan returns a task_id with status 'started'."""
        client, root = client_and_root
        with (
            patch("godmode_media_library.scanner.probe_file", return_value=None),
            patch("godmode_media_library.scanner.read_exif", return_value=None),
            patch("godmode_media_library.scanner.dhash", return_value=None),
            patch("godmode_media_library.scanner.video_dhash", return_value=None),
        ):
            resp = client.post("/api/scan", json={"roots": [str(root)]})
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_scan_task_reaches_terminal_state(self, client_and_root):
        """A scan task eventually completes or fails."""
        client, root = client_and_root
        with (
            patch("godmode_media_library.scanner.probe_file", return_value=None),
            patch("godmode_media_library.scanner.read_exif", return_value=None),
            patch("godmode_media_library.scanner.dhash", return_value=None),
            patch("godmode_media_library.scanner.video_dhash", return_value=None),
        ):
            resp = client.post("/api/scan", json={"roots": [str(root)]})
        task_id = resp.json()["task_id"]
        result = _wait_for_task(client, task_id)
        assert result["status"] in ("completed", "failed")


# ===========================================================================
# 2. Task Listing
# ===========================================================================


class TestTaskListing:
    """Tests for the GET /api/tasks endpoint."""

    def test_list_tasks_empty(self, client):
        """Empty task list returns empty array."""
        resp = client.get("/api/tasks")
        assert resp.status_code == 200
        assert resp.json()["tasks"] == []

    def test_list_tasks_returns_injected_tasks(self, client):
        """All injected tasks appear in listing."""
        _inject_task(command="scan")
        _inject_task(command="pipeline")
        _inject_task(command="verify")

        resp = client.get("/api/tasks")
        assert resp.status_code == 200
        tasks = resp.json()["tasks"]
        assert len(tasks) == 3
        commands = {t["command"] for t in tasks}
        assert commands == {"scan", "pipeline", "verify"}

    def test_list_tasks_have_timestamps(self, client):
        """Each listed task has started_at field."""
        _inject_task(command="scan")
        resp = client.get("/api/tasks")
        tasks = resp.json()["tasks"]
        assert all("started_at" in t for t in tasks)
        assert all(t["started_at"] for t in tasks)

    def test_list_tasks_includes_status(self, client):
        """Listed tasks include their status."""
        _inject_task(command="a", status="running")
        _inject_task(command="b", status="completed")
        _inject_task(command="c", status="failed")

        resp = client.get("/api/tasks")
        statuses = {t["status"] for t in resp.json()["tasks"]}
        assert statuses == {"running", "completed", "failed"}


# ===========================================================================
# 3. Task Progress
# ===========================================================================


class TestTaskProgress:
    """Tests for progress tracking."""

    def test_progress_has_percentage(self, client):
        """Progress dict can carry a percentage value."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task()
        api_mod._update_progress(task.id, {"pct": 50, "message": "half done"})

        resp = client.get(f"/api/tasks/{task.id}")
        assert resp.json()["progress"]["pct"] == 50

    def test_progress_has_message(self, client):
        """Progress dict can carry a descriptive message."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task()
        api_mod._update_progress(task.id, {"pct": 10, "message": "Indexing files..."})

        resp = client.get(f"/api/tasks/{task.id}")
        assert resp.json()["progress"]["message"] == "Indexing files..."

    def test_progress_updates_overwrite_previous(self, client):
        """Successive progress updates replace the previous value."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task()
        api_mod._update_progress(task.id, {"pct": 10, "message": "step 1"})
        api_mod._update_progress(task.id, {"pct": 80, "message": "step 2"})

        resp = client.get(f"/api/tasks/{task.id}")
        progress = resp.json()["progress"]
        assert progress["pct"] == 80
        assert progress["message"] == "step 2"

    def test_progress_zero_and_hundred(self, client):
        """Progress can be set to 0% and 100%."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task()
        api_mod._update_progress(task.id, {"pct": 0, "message": "starting"})
        resp = client.get(f"/api/tasks/{task.id}")
        assert resp.json()["progress"]["pct"] == 0

        api_mod._update_progress(task.id, {"pct": 100, "message": "done"})
        resp = client.get(f"/api/tasks/{task.id}")
        assert resp.json()["progress"]["pct"] == 100


# ===========================================================================
# 4. Task Error Handling
# ===========================================================================


class TestTaskErrorHandling:
    """Tests for error reporting in failed tasks."""

    def test_failed_task_includes_error_message(self, client):
        """A failed task surfaces a human-readable error string."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task()
        api_mod._finish_task(task.id, error="Disk full")

        resp = client.get(f"/api/tasks/{task.id}")
        data = resp.json()
        assert data["status"] == "failed"
        assert data["error"] == "Disk full"

    def test_completed_task_has_no_error(self, client):
        """A completed task has error=None."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task()
        api_mod._finish_task(task.id, result={"rows": 5})

        resp = client.get(f"/api/tasks/{task.id}")
        data = resp.json()
        assert data["status"] == "completed"
        assert data["error"] is None

    def test_scan_with_no_roots_fails_gracefully(self, tmp_path):
        """Scan with no configured roots returns a task that fails with a message."""
        from godmode_media_library.web.app import create_app

        # Use a fresh catalog with no previous scan roots
        db_path = tmp_path / "empty.db"
        cat = Catalog(db_path)
        cat.open()
        cat.close()

        app = create_app(catalog_path=db_path)
        fresh_client = TestClient(app)

        resp = fresh_client.post("/api/scan", json={"roots": []})
        assert resp.status_code == 200
        task_id = resp.json()["task_id"]

        result = _wait_for_task(fresh_client, task_id)
        assert result["status"] == "failed"
        assert "roots" in result["error"].lower() or "configured" in result["error"].lower()


# ===========================================================================
# 5. Task Cleanup / TTL
# ===========================================================================


class TestTaskCleanupTTL:
    """Tests for task eviction and not-found handling."""

    def test_nonexistent_task_returns_404(self, client):
        """Requesting a task ID that doesn't exist gives 404."""
        resp = client.get("/api/tasks/nonexistent")
        assert resp.status_code == 404

    def test_random_uuid_returns_404(self, client):
        """A random UUID-style ID that was never created gives 404."""
        fake_id = str(uuid.uuid4())[:8]
        resp = client.get(f"/api/tasks/{fake_id}")
        assert resp.status_code == 404

    def test_eviction_removes_old_completed_tasks(self, client):
        """Old completed tasks are evicted when _evict_old_tasks runs."""
        from godmode_media_library.web import api as api_mod

        # Inject a completed task with a very old created_ts
        task = _inject_task(status="completed")
        task._created_ts = time.monotonic() - (api_mod._TASK_TTL_SECONDS + 100)

        # Trigger eviction by creating a new task
        api_mod._create_task("trigger-eviction")

        resp = client.get(f"/api/tasks/{task.id}")
        assert resp.status_code == 404

    def test_eviction_does_not_remove_running_tasks(self, client):
        """Running tasks are never evicted regardless of age."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task(status="running")
        task._created_ts = time.monotonic() - (api_mod._TASK_TTL_SECONDS + 100)

        api_mod._create_task("trigger-eviction")

        resp = client.get(f"/api/tasks/{task.id}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "running"

    def test_hard_cap_eviction(self, client):
        """When completed tasks exceed _MAX_COMPLETED_TASKS, oldest are removed."""
        from godmode_media_library.web import api as api_mod

        # Inject more than the max
        injected = []
        for i in range(api_mod._MAX_COMPLETED_TASKS + 5):
            t = _inject_task(status="completed")
            t._created_ts = time.monotonic() - (api_mod._MAX_COMPLETED_TASKS - i)
            injected.append(t)

        # Trigger eviction
        api_mod._create_task("trigger-eviction")

        # The oldest 5 should have been evicted
        for t in injected[:5]:
            resp = client.get(f"/api/tasks/{t.id}")
            assert resp.status_code == 404, f"Task {t.id} should have been evicted"


# ===========================================================================
# 6. Concurrent Tasks
# ===========================================================================


class TestConcurrentTasks:
    """Tests for multiple simultaneous tasks."""

    def test_multiple_tasks_have_unique_ids(self, client):
        """Each created task gets a unique identifier."""
        from godmode_media_library.web import api as api_mod

        ids = set()
        for _ in range(20):
            t = api_mod._create_task("test")
            ids.add(t.id)
        assert len(ids) == 20

    def test_tasks_do_not_interfere(self, client):
        """Updating one task does not affect another."""
        from godmode_media_library.web import api as api_mod

        t1 = _inject_task(command="scan")
        t2 = _inject_task(command="pipeline")

        api_mod._update_progress(t1.id, {"pct": 50})
        api_mod._finish_task(t2.id, error="boom")

        resp1 = client.get(f"/api/tasks/{t1.id}")
        resp2 = client.get(f"/api/tasks/{t2.id}")

        assert resp1.json()["status"] == "running"
        assert resp1.json()["progress"]["pct"] == 50
        assert resp2.json()["status"] == "failed"
        assert resp2.json()["error"] == "boom"

    def test_concurrent_task_creation_is_threadsafe(self, client):
        """Creating tasks from multiple threads doesn't corrupt the store."""
        from godmode_media_library.web import api as api_mod

        results = []
        errors = []

        def create_many():
            try:
                for _ in range(10):
                    t = api_mod._create_task("stress")
                    results.append(t.id)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=create_many) for _ in range(5)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert not errors
        assert len(results) == 50
        assert len(set(results)) == 50  # all unique

    def test_concurrent_progress_updates_are_threadsafe(self, client):
        """Updating progress from multiple threads doesn't crash."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task()
        errors = []

        def update_many(start):
            try:
                for i in range(20):
                    api_mod._update_progress(task.id, {"pct": start + i})
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=update_many, args=(i * 20,)) for i in range(3)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert not errors
        # Task should still be readable
        resp = client.get(f"/api/tasks/{task.id}")
        assert resp.status_code == 200


# ===========================================================================
# 7. WebSocket Integration
# ===========================================================================


class TestWebSocketIntegration:
    """Tests for WebSocket task streaming at /api/ws/tasks/{id}."""

    def test_ws_receives_task_status(self, client):
        """WebSocket connection receives at least one status message."""

        task = _inject_task(status="completed", command="test-ws")

        with client.websocket_connect(f"/api/ws/tasks/{task.id}") as ws:
            msg = ws.receive_json()
            assert msg["id"] == task.id
            assert msg["status"] == "completed"

    def test_ws_closes_on_completed_task(self, client):
        """WebSocket connection closes after sending a completed task."""
        task = _inject_task(status="completed")

        with client.websocket_connect(f"/api/ws/tasks/{task.id}") as ws:
            msg = ws.receive_json()
            assert msg["status"] == "completed"
            # The server should close the connection after sending completed status

    def test_ws_closes_on_failed_task(self, client):
        """WebSocket connection closes after sending a failed task."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task(status="running")
        api_mod._finish_task(task.id, error="oops")

        with client.websocket_connect(f"/api/ws/tasks/{task.id}") as ws:
            msg = ws.receive_json()
            assert msg["status"] == "failed"
            assert msg["error"] == "oops"

    def test_ws_nonexistent_task_sends_error(self, client):
        """Connecting to a non-existent task ID sends an error message."""
        with client.websocket_connect("/api/ws/tasks/does-not-exist") as ws:
            msg = ws.receive_json()
            assert "error" in msg

    def test_ws_message_has_all_fields(self, client):
        """WebSocket messages contain all expected task fields."""
        task = _inject_task(status="completed", command="verify", result={"ok": True})

        with client.websocket_connect(f"/api/ws/tasks/{task.id}") as ws:
            msg = ws.receive_json()
            assert "id" in msg
            assert "command" in msg
            assert "status" in msg
            assert "progress" in msg
            assert "result" in msg
            assert "error" in msg
            assert "started_at" in msg
            assert "finished_at" in msg

    def test_ws_receives_progress_for_running_task(self, client):
        """WebSocket sends current progress for a running task, then finish."""
        from godmode_media_library.web import api as api_mod

        task = _inject_task(status="running")
        api_mod._update_progress(task.id, {"pct": 75, "message": "almost"})

        # Finish the task after a short delay so ws loop terminates
        def finish_soon():
            time.sleep(0.3)
            api_mod._finish_task(task.id, result={"done": True})

        t = threading.Thread(target=finish_soon)
        t.start()

        messages = []
        with client.websocket_connect(f"/api/ws/tasks/{task.id}") as ws:
            while True:
                msg = ws.receive_json()
                messages.append(msg)
                if msg["status"] in ("completed", "failed"):
                    break

        t.join()
        # Should have at least one running + the completed message
        assert any(m["status"] == "running" for m in messages)
        assert messages[-1]["status"] == "completed"


# ===========================================================================
# 8. Task API Input Validation
# ===========================================================================


class TestTaskInputValidation:
    """Tests for invalid inputs to task endpoints."""

    def test_invalid_uuid_returns_404(self, client):
        """A clearly invalid task ID returns 404."""
        resp = client.get("/api/tasks/!!!invalid!!!")
        assert resp.status_code == 404

    def test_empty_task_id_returns_404_or_not_found(self, client):
        """Requesting /api/tasks/ without an ID doesn't match the detail route."""
        # /api/tasks/ with no ID should hit the list endpoint, not 404
        resp = client.get("/api/tasks/")
        # This should either return the list or a 404, not a 500
        assert resp.status_code in (200, 307, 404)

    def test_very_long_task_id_returns_404(self, client):
        """An extremely long task ID doesn't cause a server error."""
        long_id = "a" * 500
        resp = client.get(f"/api/tasks/{long_id}")
        assert resp.status_code == 404

    def test_special_characters_in_task_id(self, client):
        """Special characters in task ID return 404, not 500."""
        resp = client.get("/api/tasks/<script>alert(1)</script>")
        # URL encoding may cause a 404 or 422, but never 500
        assert resp.status_code < 500


# ===========================================================================
# 9. Long-Running Operations as Tasks
# ===========================================================================


class TestOperationsCreateTasks:
    """Tests that various operations create background tasks."""

    def test_scan_returns_task_id(self, client_and_root):
        """POST /api/scan returns a task_id."""
        client, root = client_and_root
        with (
            patch("godmode_media_library.scanner.probe_file", return_value=None),
            patch("godmode_media_library.scanner.read_exif", return_value=None),
            patch("godmode_media_library.scanner.dhash", return_value=None),
            patch("godmode_media_library.scanner.video_dhash", return_value=None),
        ):
            resp = client.post("/api/scan", json={"roots": [str(root)]})
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert len(data["task_id"]) > 0

    def test_pipeline_returns_task_id(self, client_and_root):
        """POST /api/pipeline returns a task_id."""
        client, root = client_and_root
        resp = client.post("/api/pipeline", json={"roots": [str(root)]})
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_verify_returns_task_id(self, client):
        """POST /api/verify returns a task_id."""
        resp = client.post("/api/verify")
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_scan_task_appears_in_list(self, client_and_root):
        """After triggering a scan, the task appears in GET /api/tasks."""
        client, root = client_and_root
        with (
            patch("godmode_media_library.scanner.probe_file", return_value=None),
            patch("godmode_media_library.scanner.read_exif", return_value=None),
            patch("godmode_media_library.scanner.dhash", return_value=None),
            patch("godmode_media_library.scanner.video_dhash", return_value=None),
        ):
            resp = client.post("/api/scan", json={"roots": [str(root)]})
        task_id = resp.json()["task_id"]

        tasks_resp = client.get("/api/tasks")
        task_ids = [t["id"] for t in tasks_resp.json()["tasks"]]
        assert task_id in task_ids

    def test_scan_task_has_scan_command(self, client_and_root):
        """Scan task has command='scan'."""
        client, root = client_and_root
        with (
            patch("godmode_media_library.scanner.probe_file", return_value=None),
            patch("godmode_media_library.scanner.read_exif", return_value=None),
            patch("godmode_media_library.scanner.dhash", return_value=None),
            patch("godmode_media_library.scanner.video_dhash", return_value=None),
        ):
            resp = client.post("/api/scan", json={"roots": [str(root)]})
        task_id = resp.json()["task_id"]

        detail = client.get(f"/api/tasks/{task_id}")
        assert detail.json()["command"] == "scan"

    def test_pipeline_task_has_pipeline_command(self, client_and_root):
        """Pipeline task has command='pipeline'."""
        client, root = client_and_root
        resp = client.post("/api/pipeline", json={"roots": [str(root)]})
        task_id = resp.json()["task_id"]

        detail = client.get(f"/api/tasks/{task_id}")
        assert detail.json()["command"] == "pipeline"

    def test_scan_completed_task_has_result(self, client_and_root):
        """A completed scan task includes result with file counts."""
        client, root = client_and_root
        with (
            patch("godmode_media_library.scanner.probe_file", return_value=None),
            patch("godmode_media_library.scanner.read_exif", return_value=None),
            patch("godmode_media_library.scanner.dhash", return_value=None),
            patch("godmode_media_library.scanner.video_dhash", return_value=None),
        ):
            resp = client.post("/api/scan", json={"roots": [str(root)]})
        task_id = resp.json()["task_id"]
        result = _wait_for_task(client, task_id)
        if result["status"] == "completed":
            assert "files_scanned" in result["result"]


# ===========================================================================
# 10. _create_task / _finish_task / _update_progress unit-level integration
# ===========================================================================


class TestTaskInternalAPI:
    """Tests for the internal task management functions."""

    def test_create_task_returns_task_with_id(self):
        """_create_task returns a TaskStatus with a non-empty id."""
        from godmode_media_library.web import api as api_mod

        task = api_mod._create_task("test-op")
        assert task.id
        assert task.command == "test-op"
        assert task.status == "running"

    def test_finish_task_with_result(self):
        """_finish_task sets status and result."""
        from godmode_media_library.web import api as api_mod

        task = api_mod._create_task("op")
        api_mod._finish_task(task.id, result={"count": 5})

        with api_mod._tasks_lock:
            t = api_mod._tasks[task.id]
        assert t.status == "completed"
        assert t.result == {"count": 5}
        assert t.error is None

    def test_finish_task_with_error(self):
        """_finish_task with error sets status to failed."""
        from godmode_media_library.web import api as api_mod

        task = api_mod._create_task("op")
        api_mod._finish_task(task.id, error="fail!")

        with api_mod._tasks_lock:
            t = api_mod._tasks[task.id]
        assert t.status == "failed"
        assert t.error == "fail!"

    def test_update_progress_on_missing_task_does_not_crash(self):
        """Updating progress for a non-existent task is a no-op."""
        from godmode_media_library.web import api as api_mod

        # Should not raise
        api_mod._update_progress("nonexistent-id", {"pct": 50})

    def test_finish_task_on_missing_task_does_not_crash(self):
        """Finishing a non-existent task is a no-op."""
        from godmode_media_library.web import api as api_mod

        # Should not raise
        api_mod._finish_task("nonexistent-id", error="nope")

    def test_task_to_msg_serialization(self):
        """_task_to_msg returns a complete dict with all fields."""
        from godmode_media_library.web import api as api_mod

        task = api_mod._create_task("serialize-test")
        msg = api_mod._task_to_msg(task)

        assert msg["id"] == task.id
        assert msg["command"] == "serialize-test"
        assert msg["status"] == "running"
        assert "progress" in msg
        assert "result" in msg
        assert "error" in msg
        assert "started_at" in msg
        assert "finished_at" in msg
