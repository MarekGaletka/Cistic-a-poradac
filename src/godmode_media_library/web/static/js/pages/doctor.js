/* GOD MODE Media Library — Doctor / System Health Dashboard */

import { api } from "../api.js";
import { escapeHtml, formatBytes, showToast } from "../utils.js";
import { t } from "../i18n.js";

// ── State ───────────────────────────────────────────────
let _container = null;
let _tasksPollTimer = null;

// ── Public API ──────────────────────────────────────────

export async function render(container) {
  _container = container;
  container.innerHTML = `<div class="loading"><div class="spinner"></div></div>`;

  try {
    const [depsData, sysData, tasksData] = await Promise.all([
      api("/deps"),
      api("/system-info").catch(() => null),
      api("/tasks").catch(() => ({ tasks: [] })),
    ]);

    _renderAll(depsData, sysData, tasksData);
    _startTasksPoll();
  } catch (e) {
    container.innerHTML = `<div class="empty" style="padding:16px">${t("general.error", { message: e.message })}</div>`;
  }
}

export function cleanup() {
  _stopTasksPoll();
  _container = null;
}

// ── Render ──────────────────────────────────────────────

function _renderAll(depsData, sysData, tasksData) {
  if (!_container) return;

  const deps = depsData.dependencies || [];
  const missingCount = deps.filter(d => !d.available).length;
  const score = _calcHealthScore(deps, sysData);
  const runningTasks = (tasksData.tasks || []).filter(t => t.status === "running");

  _container.innerHTML = `
    ${_renderHealthRing(score, sysData)}
    ${_renderSystemInfo(sysData)}
    ${_renderDeps(deps, missingCount)}
    ${_renderRunningTasks(runningTasks)}
  `;

  _bindEvents();

  // Store data for re-renders from poll
  _container._depsData = depsData;
  _container._sysData = sysData;
}

function _renderHealthRing(score, sysData) {
  const circumference = 2 * Math.PI * 40;
  const offset = circumference * (1 - score / 100);
  const color = score >= 80 ? "var(--green)" : score >= 60 ? "var(--yellow)" : "var(--red)";
  const grade = score >= 90 ? "A" : score >= 80 ? "B" : score >= 60 ? "C" : score >= 40 ? "D" : "F";

  const totalFiles = sysData?.total_files || 0;
  const catSize = sysData?.catalog_size || 0;
  const quarantine = sysData?.quarantine_size || 0;
  const diskFree = sysData?.disk_free || 0;

  return `<div class="doctor-health-header">
    <div class="doctor-health-ring-wrap">
      <svg viewBox="0 0 100 100" class="doctor-health-ring">
        <circle cx="50" cy="50" r="40" fill="none" stroke="var(--border)" stroke-width="8"/>
        <circle cx="50" cy="50" r="40" fill="none" stroke="${color}" stroke-width="8"
          stroke-dasharray="${circumference}" stroke-dashoffset="${offset}" stroke-linecap="round"
          style="transition:stroke-dashoffset 1s ease"/>
      </svg>
      <div class="doctor-health-ring-label">
        <span class="doctor-health-grade" style="color:${color}">${grade}</span>
        <span class="doctor-health-pct">${score}%</span>
      </div>
    </div>
    <div class="doctor-stats-grid">
      <div class="doctor-stat-card">
        <div class="doctor-stat-value">${formatBytes(catSize)}</div>
        <div class="doctor-stat-label">${t("doctor.catalog_size")}</div>
      </div>
      <div class="doctor-stat-card">
        <div class="doctor-stat-value">${totalFiles.toLocaleString()}</div>
        <div class="doctor-stat-label">${t("doctor.total_files")}</div>
      </div>
      <div class="doctor-stat-card">
        <div class="doctor-stat-value">${formatBytes(quarantine)}</div>
        <div class="doctor-stat-label">${t("doctor.quarantine_size")}</div>
      </div>
      <div class="doctor-stat-card">
        <div class="doctor-stat-value">${formatBytes(diskFree)}</div>
        <div class="doctor-stat-label">${t("doctor.disk_free")}</div>
      </div>
    </div>
  </div>`;
}

function _renderSystemInfo(sysData) {
  if (!sysData) return "";

  const rows = [
    { icon: "\u{1F40D}", label: t("doctor.python_version"), value: sysData.python_version || "\u2014" },
    { icon: "\u{1F4BB}", label: t("doctor.platform"), value: sysData.platform || "\u2014" },
    { icon: "\u{1F4C4}", label: t("doctor.catalog_path"), value: sysData.catalog_path || "\u2014" },
    { icon: "\u{1F4BE}", label: t("doctor.catalog_size"), value: formatBytes(sysData.catalog_size) },
    { icon: "\u{1F4F7}", label: t("doctor.total_files"), value: String(sysData.total_files || 0) },
    { icon: "\u{1F5D1}", label: t("doctor.quarantine_size"), value: formatBytes(sysData.quarantine_size) },
    { icon: "\u{1F4BF}", label: t("doctor.disk_free"), value: `${formatBytes(sysData.disk_free)} / ${formatBytes(sysData.disk_total)}` },
  ];

  const rowsHtml = rows.map(r => `
    <div class="doctor-sys-row">
      <span class="doctor-sys-icon">${r.icon}</span>
      <span class="doctor-sys-label">${escapeHtml(r.label)}</span>
      <span class="doctor-sys-value">${escapeHtml(r.value)}</span>
    </div>
  `).join("");

  return `<div class="doctor-sys-section">
    <div class="doctor-sys-header">
      <label class="form-label">${t("doctor.system_info")}</label>
      <button class="doctor-refresh-btn" id="btn-doctor-refresh">${t("doctor.refresh")}</button>
    </div>
    <div class="doctor-sys-table">${rowsHtml}</div>
  </div>`;
}

function _renderDeps(deps, missingCount) {
  const healthBadge = missingCount === 0
    ? `<span class="doctor-badge doctor-badge-ok">\u2713 ${t("doctor.all_ok")}</span>`
    : `<span class="doctor-badge doctor-badge-warn">\u26A0 ${t("doctor.issues_found", { count: missingCount })}</span>`;

  // Sort: missing first
  const sorted = [...deps].sort((a, b) => {
    if (a.available === b.available) return a.name.localeCompare(b.name);
    return a.available ? 1 : -1;
  });

  const cardsHtml = sorted.map(d => {
    const statusIcon = d.available ? "\u2713" : "\u2717";
    const statusClass = d.available ? "doctor-dep-ok" : "doctor-dep-missing";
    const ver = d.version ? ` (${escapeHtml(d.version)})` : "";

    let installHtml = "";
    if (!d.available && d.install_hint) {
      installHtml = `<div class="doctor-install-box">
        <code>${escapeHtml(d.install_hint)}</code>
        <button class="btn-copy-cmd" data-cmd="${escapeHtml(d.install_hint)}">${t("doctor.copy_command")}</button>
      </div>`;
    }

    return `<div class="doctor-dep-card ${statusClass}">
      <div class="doctor-dep-header">
        <span class="doctor-dep-icon">${statusIcon}</span>
        <strong class="doctor-dep-name">${escapeHtml(d.name)}</strong>
        <span class="doctor-dep-ver">${ver}</span>
      </div>
      <div class="doctor-dep-status">${d.available ? t("doctor.available") : t("doctor.missing")}</div>
      ${installHtml}
    </div>`;
  }).join("");

  return `<div class="doctor-deps-section">
    <div class="doctor-deps-header">
      <label class="form-label">${t("doctor.title")}</label>
      ${healthBadge}
    </div>
    <div class="doctor-dep-grid">${cardsHtml}</div>
  </div>`;
}

function _renderRunningTasks(tasks) {
  if (!tasks.length) return "";

  const rows = tasks.map(task => {
    const cmd = task.command || "unknown";
    const started = task.started_at ? new Date(task.started_at).toLocaleString() : "\u2014";
    return `<div class="doctor-task-row">
      <span class="doctor-task-dot">\u{1F7E1}</span>
      <span class="doctor-task-cmd">${escapeHtml(cmd)}</span>
      <span class="doctor-task-time">${started}</span>
      <div class="progress-bar doctor-task-bar"><div class="progress-fill" style="width:30%"></div></div>
    </div>`;
  }).join("");

  return `<div class="doctor-tasks-section">
    <label class="form-label">${t("doctor.running_tasks")}</label>
    ${rows}
  </div>`;
}

// ── Events ──────────────────────────────────────────────

function _bindEvents() {
  if (!_container) return;

  // Refresh button
  _container.querySelector("#btn-doctor-refresh")?.addEventListener("click", async () => {
    const btn = _container.querySelector("#btn-doctor-refresh");
    if (btn) { btn.disabled = true; btn.textContent = "\u2026"; }
    try {
      const [depsData, sysData, tasksData] = await Promise.all([
        api("/deps"),
        api("/system-info").catch(() => null),
        api("/tasks").catch(() => ({ tasks: [] })),
      ]);
      _renderAll(depsData, sysData, tasksData);
    } catch (e) {
      showToast(t("general.error", { message: e.message }), "error");
      if (btn) { btn.disabled = false; btn.textContent = t("doctor.refresh"); }
    }
  });

  // Copy buttons
  _container.querySelectorAll(".btn-copy-cmd").forEach(btn => {
    btn.addEventListener("click", () => {
      navigator.clipboard.writeText(btn.dataset.cmd).then(() => {
        showToast(t("doctor.copy_command"), "info");
      }).catch(() => {
        const range = document.createRange();
        const codeEl = btn.previousElementSibling;
        if (codeEl) {
          range.selectNode(codeEl);
          window.getSelection().removeAllRanges();
          window.getSelection().addRange(range);
          document.execCommand("copy");
          window.getSelection().removeAllRanges();
        }
      });
    });
  });
}

// ── Tasks polling ───────────────────────────────────────

function _startTasksPoll() {
  _stopTasksPoll();
  _tasksPollTimer = setInterval(async () => {
    if (!_container) { _stopTasksPoll(); return; }
    try {
      const tasksData = await api("/tasks");
      const runningTasks = (tasksData.tasks || []).filter(t => t.status === "running");
      const section = _container.querySelector(".doctor-tasks-section");
      if (runningTasks.length) {
        if (section) {
          section.innerHTML = `<label class="form-label">${t("doctor.running_tasks")}</label>` +
            runningTasks.map(task => {
              const cmd = task.command || "unknown";
              const started = task.started_at ? new Date(task.started_at).toLocaleString() : "\u2014";
              return `<div class="doctor-task-row">
                <span class="doctor-task-dot">\u{1F7E1}</span>
                <span class="doctor-task-cmd">${escapeHtml(cmd)}</span>
                <span class="doctor-task-time">${started}</span>
                <div class="progress-bar doctor-task-bar"><div class="progress-fill" style="width:30%"></div></div>
              </div>`;
            }).join("");
        }
      } else if (section) {
        section.remove();
      }
    } catch { /* silent */ }
  }, 5000);
}

function _stopTasksPoll() {
  if (_tasksPollTimer) {
    clearInterval(_tasksPollTimer);
    _tasksPollTimer = null;
  }
}

// ── Health score calculation ────────────────────────────

function _calcHealthScore(deps, sysData) {
  let score = 0;

  // Dependencies (40 points)
  const totalDeps = deps.length || 1;
  const availDeps = deps.filter(d => d.available).length;
  score += Math.round((availDeps / totalDeps) * 40);

  // Catalog accessible (20 points)
  if (sysData && sysData.catalog_size > 0) score += 20;

  // Disk free > 10% (20 points)
  if (sysData && sysData.disk_total > 0) {
    const freePct = sysData.disk_free / sysData.disk_total;
    if (freePct > 0.1) score += 20;
    else score += Math.round(freePct * 200); // proportional
  }

  // Quarantine small (10 points) — < 1GB is fine
  if (sysData) {
    const q = sysData.quarantine_size || 0;
    if (q < 1024 * 1024 * 1024) score += 10;
    else if (q < 5 * 1024 * 1024 * 1024) score += 5;
  } else {
    score += 10;
  }

  // No missing deps (10 points)
  const missing = deps.filter(d => !d.available).length;
  if (missing === 0) score += 10;

  return Math.min(100, score);
}
