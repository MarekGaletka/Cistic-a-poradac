/* GOD MODE Media Library — Activity Feed widget */

import { api } from "./api.js";
import { formatBytes } from "./utils.js";
import { t } from "./i18n.js";

/**
 * Render an activity feed into a container element.
 * Shows recent tasks, scans, backups, and alerts.
 */
export async function renderActivityFeed(container) {
  container.innerHTML = `<div class="loading-sm">${t("activity.loading")}</div>`;

  try {
    const [tasksData, monitorData] = await Promise.all([
      api("/tasks").catch(() => ({ tasks: [] })),
      api("/backup/monitor").catch(() => ({ active_alerts: [], checks: [] })),
    ]);

    const events = [];

    // Add completed tasks
    for (const task of (tasksData.tasks || [])) {
      events.push({
        time: task.finished_at || task.started_at || "",
        icon: _taskIcon(task.command),
        label: _taskLabel(task.command),
        detail: task.status === "completed"
          ? _taskDetail(task)
          : task.status === "failed" ? t("activity.error_status", { error: task.error || "?" }) : t("activity.in_progress"),
        severity: task.status === "failed" ? "error" : task.status === "completed" ? "ok" : "running",
      });
    }

    // Add alerts
    for (const a of (monitorData.active_alerts || []).slice(0, 5)) {
      events.push({
        time: a.timestamp || "",
        icon: a.severity === "critical" ? "🔴" : "🟡",
        label: t("activity.backup_alert"),
        detail: a.message,
        severity: a.severity === "critical" ? "error" : "warning",
      });
    }

    // Sort by time descending
    events.sort((a, b) => (b.time || "").localeCompare(a.time || ""));

    if (events.length === 0) {
      container.innerHTML = `<div class="activity-empty">${t("activity.empty")}</div>`;
      return;
    }

    container.innerHTML = events.slice(0, 15).map(ev => {
      const time = ev.time ? new Date(ev.time).toLocaleString("cs-CZ", { day: "numeric", month: "numeric", hour: "2-digit", minute: "2-digit" }) : "";
      const borderColor = ev.severity === "error" ? "#f85149" : ev.severity === "warning" ? "#d29922" : ev.severity === "running" ? "#58a6ff" : "var(--border)";
      return `<div class="activity-item" style="border-left-color:${borderColor}">
        <span class="activity-icon">${ev.icon}</span>
        <div class="activity-content">
          <span class="activity-label">${ev.label}</span>
          <span class="activity-detail">${ev.detail}</span>
        </div>
        <span class="activity-time">${time}</span>
      </div>`;
    }).join("");

  } catch (e) {
    container.innerHTML = `<div class="activity-empty">${t("activity.error_unknown", { message: e.message })}</div>`;
  }
}

function _taskIcon(command) {
  if (!command) return "⚙️";
  if (command.includes("scan")) return "📷";
  if (command.includes("backup")) return "☁️";
  if (command.includes("bitrot")) return "🔬";
  if (command.includes("scenario")) return "🎬";
  if (command.includes("report")) return "📊";
  if (command.includes("pipeline")) return "⚡";
  if (command.includes("dedup")) return "📋";
  if (command.includes("health")) return "🩺";
  return "⚙️";
}

function _taskLabel(command) {
  if (!command) return t("activity.task_label");
  if (command.includes("scan")) return t("activity.label_scan");
  if (command.includes("backup:distribute")) return t("activity.label_backup_distribute");
  if (command.includes("backup:verify")) return t("activity.label_backup_verify");
  if (command.includes("backup:health")) return t("activity.label_backup_health");
  if (command.includes("bitrot")) return t("activity.label_bitrot");
  if (command.includes("scenario")) return command.replace("scenario:", t("activity.label_scenario_prefix"));
  if (command.includes("report")) return t("activity.label_report");
  if (command.includes("pipeline")) return t("activity.label_pipeline");
  return command;
}

function _taskDetail(task) {
  const r = task.result;
  if (!r) return t("activity.detail_completed");
  if (r.total_checked !== undefined) return t("activity.detail_corrupted", { healthy: r.healthy || 0, corrupted: r.corrupted || 0 });
  if (r.uploaded !== undefined) return t("activity.detail_uploaded", { uploaded: r.uploaded, errors: r.errors || 0 });
  if (r.scanned !== undefined) return t("activity.detail_scanned", { scanned: r.scanned });
  if (r.verified !== undefined) return t("activity.detail_verified", { verified: r.verified, missing: r.missing || 0 });
  if (r.checked !== undefined) return t("activity.detail_health", { healthy: r.healthy || 0, checked: r.checked });
  return t("activity.detail_completed");
}
