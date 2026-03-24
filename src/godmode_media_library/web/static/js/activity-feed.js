/* GOD MODE Media Library — Activity Feed widget */

import { api } from "./api.js";
import { formatBytes } from "./utils.js";

/**
 * Render an activity feed into a container element.
 * Shows recent tasks, scans, backups, and alerts.
 */
export async function renderActivityFeed(container) {
  container.innerHTML = '<div class="loading-sm">Načítání...</div>';

  try {
    const [tasksData, monitorData] = await Promise.all([
      api("/tasks").catch(() => ({ tasks: [] })),
      api("/backup/monitor").catch(() => ({ active_alerts: [], checks: [] })),
    ]);

    const events = [];

    // Add completed tasks
    for (const t of (tasksData.tasks || [])) {
      events.push({
        time: t.finished_at || t.started_at || "",
        icon: _taskIcon(t.command),
        label: _taskLabel(t.command),
        detail: t.status === "completed"
          ? _taskDetail(t)
          : t.status === "failed" ? `Chyba: ${t.error || "neznámá"}` : "Probíhá...",
        severity: t.status === "failed" ? "error" : t.status === "completed" ? "ok" : "running",
      });
    }

    // Add alerts
    for (const a of (monitorData.active_alerts || []).slice(0, 5)) {
      events.push({
        time: a.timestamp || "",
        icon: a.severity === "critical" ? "🔴" : "🟡",
        label: "Upozornění zálohy",
        detail: a.message,
        severity: a.severity === "critical" ? "error" : "warning",
      });
    }

    // Sort by time descending
    events.sort((a, b) => (b.time || "").localeCompare(a.time || ""));

    if (events.length === 0) {
      container.innerHTML = '<div class="activity-empty">Zatím žádná aktivita</div>';
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
    container.innerHTML = `<div class="activity-empty">Chyba: ${e.message}</div>`;
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
  if (!command) return "Úloha";
  if (command.includes("scan")) return "Skenování";
  if (command.includes("backup:distribute")) return "Distribuovaná záloha";
  if (command.includes("backup:verify")) return "Ověření záloh";
  if (command.includes("backup:health")) return "Kontrola zdraví";
  if (command.includes("bitrot")) return "Bit rot sken";
  if (command.includes("scenario")) return command.replace("scenario:", "Scénář: ");
  if (command.includes("report")) return "Report";
  if (command.includes("pipeline")) return "Pipeline";
  return command;
}

function _taskDetail(task) {
  const r = task.result;
  if (!r) return "Dokončeno";
  if (r.total_checked !== undefined) return `${r.healthy || 0} OK, ${r.corrupted || 0} poškozených`;
  if (r.uploaded !== undefined) return `${r.uploaded} nahráno, ${r.errors || 0} chyb`;
  if (r.scanned !== undefined) return `${r.scanned} souborů`;
  if (r.verified !== undefined) return `${r.verified} ověřeno, ${r.missing || 0} chybí`;
  if (r.checked !== undefined) return `${r.healthy || 0}/${r.checked} zdravých`;
  return "Dokončeno";
}
