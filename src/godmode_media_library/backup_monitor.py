"""Backup monitoring — periodic health checks and alerting."""

from __future__ import annotations

import json
import logging
import subprocess
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .cloud import _rclone_bin

logger = logging.getLogger(__name__)

_MONITOR_STATE_PATH = Path.home() / ".config" / "gml" / "backup_monitor_state.json"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


@dataclass
class HealthCheck:
    """Result of a single health check."""

    remote_name: str
    timestamp: str
    accessible: bool
    write_ok: bool = False
    read_ok: bool = False
    latency_ms: int = 0
    free_bytes: int = 0
    error: str = ""
    sample_verified: int = 0
    sample_missing: int = 0


@dataclass
class MonitorState:
    """Persistent monitoring state."""

    last_check_at: str = ""
    checks: list[dict] = field(default_factory=list)
    alerts: list[dict] = field(default_factory=list)  # {timestamp, severity, message, remote, acknowledged}
    consecutive_failures: dict = field(default_factory=dict)  # remote_name -> int


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def _load_state() -> MonitorState:
    if _MONITOR_STATE_PATH.exists():
        try:
            data = json.loads(_MONITOR_STATE_PATH.read_text())
            return MonitorState(
                last_check_at=data.get("last_check_at", ""),
                checks=data.get("checks", []),
                alerts=data.get("alerts", []),
                consecutive_failures=data.get("consecutive_failures", {}),
            )
        except Exception as e:
            logger.warning("Failed to load monitor state: %s", e)
    return MonitorState()


def _save_state(state: MonitorState) -> None:
    _MONITOR_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _MONITOR_STATE_PATH.write_text(json.dumps(asdict(state), indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Health check execution
# ---------------------------------------------------------------------------


def check_remote_health(remote_name: str) -> HealthCheck:
    """Check if a remote is accessible and functional."""
    now = datetime.now(timezone.utc).isoformat()
    check = HealthCheck(remote_name=remote_name, timestamp=now, accessible=False)

    start = time.monotonic()

    try:
        # 1. Test accessibility with rclone lsd (fast, read-only)
        result = subprocess.run(
            [_rclone_bin(), "lsd", f"{remote_name}:", "--max-depth", "1"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        elapsed_ms = int((time.monotonic() - start) * 1000)
        check.latency_ms = elapsed_ms

        if result.returncode != 0:
            check.error = result.stderr[:200].strip()
            return check

        check.accessible = True

        # 2. Test write access
        test_content = f"gml-health-{now}"
        write_result = subprocess.run(
            [_rclone_bin(), "rcat", f"{remote_name}:.gml_health_check.txt"],
            input=test_content,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if write_result.returncode == 0:
            check.write_ok = True
            # 3. Test read back
            read_result = subprocess.run(
                [_rclone_bin(), "cat", f"{remote_name}:.gml_health_check.txt"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if read_result.returncode == 0 and test_content in read_result.stdout:
                check.read_ok = True
            # Clean up
            subprocess.run(
                [_rclone_bin(), "deletefile", f"{remote_name}:.gml_health_check.txt"],
                capture_output=True,
                timeout=15,
            )

        # 4. Check free space
        try:
            about = subprocess.run(
                [_rclone_bin(), "about", f"{remote_name}:", "--json"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if about.returncode == 0:
                info = json.loads(about.stdout)
                check.free_bytes = info.get("free", 0) or 0
        except (subprocess.SubprocessError, json.JSONDecodeError, OSError) as exc:
            logger.debug("rclone about failed for %s: %s", remote_name, exc)

    except subprocess.TimeoutExpired:
        check.error = "Timeout (30s)"
    except Exception as e:
        check.error = str(e)[:200]

    return check


def run_health_checks(remote_names: list[str] | None = None) -> list[HealthCheck]:
    """Run health checks on all or specified remotes.

    Returns list of HealthCheck results.
    Updates monitor state and creates alerts for failures.
    """
    from .cloud import list_remotes

    if remote_names is None:
        remotes = list_remotes()
        remote_names = [r.name for r in remotes]

    state = _load_state()
    checks = []
    new_alerts = []

    for name in remote_names:
        logger.info("Health check: %s", name)
        check = check_remote_health(name)
        checks.append(check)

        asdict(check)

        if not check.accessible:
            # Increment failure counter
            failures = state.consecutive_failures.get(name, 0) + 1
            state.consecutive_failures[name] = failures

            alert = {
                "timestamp": check.timestamp,
                "severity": "critical" if failures >= 3 else "warning",
                "message": f"{name}: nedostupný ({check.error})" if check.error else f"{name}: nedostupný",
                "remote": name,
                "failures": failures,
                "acknowledged": False,
            }
            new_alerts.append(alert)

            # Send notification for critical (3+ consecutive failures)
            if failures >= 3:
                _send_notification(
                    title="GML Záloha — KRITICKÉ",
                    message=f"Úložiště {name} je nedostupné již {failures}× po sobě!",
                    severity="critical",
                )
            elif failures == 1:
                _send_notification(
                    title="GML Záloha — Varování",
                    message=f"Úložiště {name} není dostupné: {check.error or 'timeout'}",
                    severity="warning",
                )
        else:
            # Reset failure counter on success
            state.consecutive_failures[name] = 0

            # Check if free space is critically low
            if check.free_bytes > 0 and check.free_bytes < 500_000_000:  # < 500MB
                alert = {
                    "timestamp": check.timestamp,
                    "severity": "warning",
                    "message": f"{name}: téměř plný (zbývá {check.free_bytes // 1_000_000} MB)",
                    "remote": name,
                    "acknowledged": False,
                }
                new_alerts.append(alert)
                _send_notification(
                    title="GML Záloha — Málo místa",
                    message=f"Úložiště {name} má méně než 500 MB volného místa",
                    severity="warning",
                )

        if not check.write_ok and check.accessible:
            alert = {
                "timestamp": check.timestamp,
                "severity": "warning",
                "message": f"{name}: čtení OK, ale zápis selhal",
                "remote": name,
                "acknowledged": False,
            }
            new_alerts.append(alert)

    # Update state
    state.last_check_at = datetime.now(timezone.utc).isoformat()
    # Keep last 50 check records
    state.checks = [asdict(c) for c in checks] + state.checks[:50]
    state.alerts = new_alerts + state.alerts
    # Keep last 200 alerts
    state.alerts = state.alerts[:200]

    _save_state(state)

    return checks


def get_monitor_status() -> dict:
    """Get current monitor status for UI display."""
    state = _load_state()

    # Count unacknowledged alerts
    active_alerts = [a for a in state.alerts if not a.get("acknowledged")]
    critical_count = sum(1 for a in active_alerts if a.get("severity") == "critical")
    warning_count = sum(1 for a in active_alerts if a.get("severity") == "warning")

    # Latest check per remote
    latest_by_remote = {}
    for c in state.checks:
        rname = c.get("remote_name", "")
        if rname and rname not in latest_by_remote:
            latest_by_remote[rname] = c

    overall = "ok"
    if warning_count > 0:
        overall = "warning"
    if critical_count > 0:
        overall = "critical"

    return {
        "overall": overall,
        "last_check_at": state.last_check_at,
        "active_alerts": active_alerts[:20],
        "critical_count": critical_count,
        "warning_count": warning_count,
        "latest_checks": latest_by_remote,
        "consecutive_failures": state.consecutive_failures,
    }


def acknowledge_alert(index: int) -> bool:
    """Acknowledge an alert by index."""
    state = _load_state()
    active = [a for a in state.alerts if not a.get("acknowledged")]
    if 0 <= index < len(active):
        active[index]["acknowledged"] = True
        _save_state(state)
        return True
    return False


def acknowledge_all_alerts() -> int:
    """Acknowledge all alerts. Returns count."""
    state = _load_state()
    count = 0
    for a in state.alerts:
        if not a.get("acknowledged"):
            a["acknowledged"] = True
            count += 1
    _save_state(state)
    return count


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------

# Deduplication: track recent notifications to avoid repeating the same alert
# within _DEDUP_WINDOW_SECONDS.  Stores (message, timestamp) tuples.
_DEDUP_WINDOW_SECONDS = 3600  # 1 hour
_DEDUP_MAX_ENTRIES = 50
_recent_notifications: list[tuple[str, float]] = []


def _is_duplicate_notification(message: str) -> bool:
    """Return True if the same message was sent within the dedup window."""
    now = time.monotonic()
    # Prune old entries
    cutoff = now - _DEDUP_WINDOW_SECONDS
    _recent_notifications[:] = [
        (msg, ts) for msg, ts in _recent_notifications if ts >= cutoff
    ]
    for msg, _ts in _recent_notifications:
        if msg == message:
            return True
    # Record this message
    _recent_notifications.append((message, now))
    # Cap size
    if len(_recent_notifications) > _DEDUP_MAX_ENTRIES:
        _recent_notifications[:] = _recent_notifications[-_DEDUP_MAX_ENTRIES:]
    return False


def _send_notification(title: str, message: str, severity: str = "info") -> None:
    """Send a macOS desktop notification (deduplicated within 1 hour)."""
    if _is_duplicate_notification(message):
        logger.debug("Suppressed duplicate notification: %s", message)
        return

    import platform

    if platform.system() != "Darwin":
        logger.info("Notification [%s]: %s — %s", severity, title, message)
        return

    try:
        sound = "Basso" if severity == "critical" else "Purr" if severity == "warning" else "default"
        # Pass AppleScript via stdin to avoid shell injection through arguments.
        # Escape backslashes and double quotes for AppleScript string literals.
        safe_title = title.replace("\\", "\\\\").replace('"', '\\"')
        safe_message = message.replace("\\", "\\\\").replace('"', '\\"')
        script = f'display notification "{safe_message}" with title "{safe_title}" sound name "{sound}"'
        subprocess.run(
            ["osascript"],
            input=script,
            capture_output=True,
            text=True,
            timeout=5,
        )
        logger.info("macOS notification sent: %s", title)
    except Exception as e:
        logger.warning("Failed to send notification: %s", e)


def send_test_notification() -> dict:
    """Send a test notification to verify alerting works."""
    _send_notification(
        title="GML Záloha — Test",
        message="Notifikace fungují správně ✅",
        severity="info",
    )
    return {"status": "sent"}
