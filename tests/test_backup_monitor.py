"""Tests for backup_monitor module."""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library import backup_monitor as bm


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_dedup():
    """Clear the notification dedup list before each test."""
    bm._recent_notifications.clear()
    yield
    bm._recent_notifications.clear()


@pytest.fixture()
def state_path(tmp_path, monkeypatch):
    """Redirect monitor state to a temp file."""
    p = tmp_path / "backup_monitor_state.json"
    monkeypatch.setattr(bm, "_MONITOR_STATE_PATH", p)
    return p


# ---------------------------------------------------------------------------
# Notification deduplication
# ---------------------------------------------------------------------------


def test_first_notification_is_not_duplicate():
    assert bm._is_duplicate_notification("alert-A") is False


def test_same_message_within_window_is_duplicate():
    bm._is_duplicate_notification("alert-B")
    assert bm._is_duplicate_notification("alert-B") is True


def test_different_message_is_not_duplicate():
    bm._is_duplicate_notification("alert-C")
    assert bm._is_duplicate_notification("alert-D") is False


def test_old_entries_pruned(monkeypatch):
    """Messages older than the dedup window should be pruned."""
    # Insert an "old" entry with a timestamp in the past
    old_ts = time.monotonic() - bm._DEDUP_WINDOW_SECONDS - 10
    bm._recent_notifications.append(("old-msg", old_ts))
    # The same message should NOT be seen as a duplicate after pruning
    assert bm._is_duplicate_notification("old-msg") is False


def test_dedup_max_entries_cap():
    """Dedup list is capped at _DEDUP_MAX_ENTRIES."""
    for i in range(bm._DEDUP_MAX_ENTRIES + 20):
        bm._is_duplicate_notification(f"msg-{i}")
    assert len(bm._recent_notifications) <= bm._DEDUP_MAX_ENTRIES


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def test_load_state_returns_default_when_missing(state_path):
    state = bm._load_state()
    assert state.last_check_at == ""
    assert state.checks == []


def test_save_and_load_round_trip(state_path):
    state = bm.MonitorState(last_check_at="2025-01-01T00:00:00Z")
    state.alerts.append({"severity": "warning", "acknowledged": False})
    bm._save_state(state)

    loaded = bm._load_state()
    assert loaded.last_check_at == "2025-01-01T00:00:00Z"
    assert len(loaded.alerts) == 1


def test_load_state_handles_corrupt_json(state_path):
    state_path.write_text("{invalid json!!")
    state = bm._load_state()
    assert state.last_check_at == ""  # fallback default


# ---------------------------------------------------------------------------
# Alert thresholds (run_health_checks logic)
# ---------------------------------------------------------------------------


@patch.object(bm, "_send_notification")
@patch.object(bm, "check_remote_health")
def test_first_failure_sends_warning(mock_check, mock_notify, state_path):
    mock_check.return_value = bm.HealthCheck(
        remote_name="gdrive", timestamp="t1", accessible=False, error="timeout"
    )
    bm.run_health_checks(remote_names=["gdrive"])

    # First failure => warning notification
    mock_notify.assert_called_once()
    assert mock_notify.call_args.kwargs.get("severity") == "warning"


@patch.object(bm, "_send_notification")
@patch.object(bm, "check_remote_health")
def test_three_consecutive_failures_sends_critical(mock_check, mock_notify, state_path):
    mock_check.return_value = bm.HealthCheck(
        remote_name="gdrive", timestamp="t1", accessible=False, error="down"
    )
    # Simulate 3 consecutive failures via pre-set state
    pre_state = bm.MonitorState(consecutive_failures={"gdrive": 2})
    bm._save_state(pre_state)

    bm.run_health_checks(remote_names=["gdrive"])

    # Should have sent a critical notification
    calls = mock_notify.call_args_list
    assert any(c.kwargs.get("severity") == "critical" for c in calls)


@patch.object(bm, "_send_notification")
@patch.object(bm, "check_remote_health")
def test_success_resets_failure_counter(mock_check, mock_notify, state_path):
    pre_state = bm.MonitorState(consecutive_failures={"gdrive": 5})
    bm._save_state(pre_state)

    mock_check.return_value = bm.HealthCheck(
        remote_name="gdrive", timestamp="t1", accessible=True,
        write_ok=True, read_ok=True, free_bytes=10_000_000_000,
    )
    bm.run_health_checks(remote_names=["gdrive"])

    loaded = bm._load_state()
    assert loaded.consecutive_failures["gdrive"] == 0


# ---------------------------------------------------------------------------
# Acknowledge alerts
# ---------------------------------------------------------------------------


def test_acknowledge_alert(state_path):
    state = bm.MonitorState(
        alerts=[
            {"severity": "warning", "acknowledged": False},
            {"severity": "critical", "acknowledged": False},
        ]
    )
    bm._save_state(state)
    assert bm.acknowledge_alert(0) is True
    loaded = bm._load_state()
    acked = [a for a in loaded.alerts if a.get("acknowledged")]
    assert len(acked) == 1


def test_acknowledge_all_alerts(state_path):
    state = bm.MonitorState(
        alerts=[
            {"severity": "warning", "acknowledged": False},
            {"severity": "critical", "acknowledged": False},
        ]
    )
    bm._save_state(state)
    count = bm.acknowledge_all_alerts()
    assert count == 2


# ---------------------------------------------------------------------------
# _send_notification mocks osascript
# ---------------------------------------------------------------------------


@patch("subprocess.run")
@patch("platform.system", return_value="Darwin")
def test_send_notification_calls_osascript(mock_sys, mock_run):
    bm._send_notification(title="Test", message="hello", severity="info")
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "osascript"


@patch("subprocess.run", side_effect=Exception("no osascript"))
@patch("platform.system", return_value="Darwin")
def test_send_notification_handles_osascript_failure(mock_sys, mock_run):
    # Should not raise
    bm._send_notification(title="Test", message="fail-msg", severity="critical")
