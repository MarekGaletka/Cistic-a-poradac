/* GOD MODE Media Library — Task management with global progress bar */

import { api } from "./api.js";
import { $, escapeHtml, showToast } from "./utils.js";
import { t } from "./i18n.js";

let _pollInterval = null;
let _pollErrorCount = 0;

// ── Global progress bar ──────────────────────────────

let _globalProgressActive = false;
let _globalPollInterval = null;
let _currentTaskId = null;

export function initGlobalProgress() {
  // Nothing needed at init, the bar is hidden by default
}

export function showGlobalProgress(taskId) {
  _currentTaskId = taskId;
  _globalProgressActive = true;
  const bar = $("#global-progress");
  if (bar) {
    bar.classList.remove("hidden");
    bar.classList.add("animating");
    const fill = bar.querySelector(".global-progress-fill");
    if (fill) fill.style.width = "10%";
  }

  // Poll task status for the global bar
  if (_globalPollInterval) clearInterval(_globalPollInterval);
  _globalPollInterval = setInterval(async () => {
    try {
      const data = await api(`/tasks/${encodeURIComponent(taskId)}`);
      updateGlobalProgress(data);
      if (data.status === "completed") {
        completeGlobalProgress();
        showToast(t("task.completed_toast"), "success");
        clearInterval(_globalPollInterval);
        _globalPollInterval = null;
        // Auto-navigate to dashboard after scan
        if (window._godmodeNavigate) {
          setTimeout(() => window._godmodeNavigate("dashboard"), 1500);
        }
      } else if (data.status === "failed") {
        failGlobalProgress();
        showToast(t("task.failed_toast", { error: data.error || "unknown" }), "error");
        clearInterval(_globalPollInterval);
        _globalPollInterval = null;
      }
    } catch {
      // Silent retry
    }
  }, 2000);
}

function updateGlobalProgress(data) {
  const bar = $("#global-progress");
  if (!bar) return;
  const fill = bar.querySelector(".global-progress-fill");
  if (!fill) return;

  if (data.progress && data.progress.total > 0) {
    const pct = Math.round((data.progress.processed / data.progress.total) * 100);
    fill.style.width = `${Math.max(10, pct)}%`;
    bar.setAttribute("aria-valuenow", pct);
  }
}

function completeGlobalProgress() {
  const bar = $("#global-progress");
  if (!bar) return;
  const fill = bar.querySelector(".global-progress-fill");
  if (fill) {
    fill.style.width = "100%";
    fill.classList.add("complete");
  }
  bar.classList.remove("animating");
  setTimeout(() => {
    bar.classList.add("hidden");
    if (fill) {
      fill.style.width = "0%";
      fill.classList.remove("complete");
    }
    _globalProgressActive = false;
  }, 2000);
}

function failGlobalProgress() {
  const bar = $("#global-progress");
  if (!bar) return;
  const fill = bar.querySelector(".global-progress-fill");
  if (fill) fill.classList.add("failed");
  bar.classList.remove("animating");
  setTimeout(() => {
    bar.classList.add("hidden");
    if (fill) {
      fill.style.width = "0%";
      fill.classList.remove("failed");
    }
    _globalProgressActive = false;
  }, 3000);
}

// ── Task polling (for settings panel task-output) ────

export function cleanupTasks() {
  if (_pollInterval) {
    clearInterval(_pollInterval);
    _pollInterval = null;
  }
}

export function pollTask(taskId) {
  if (_pollInterval) clearInterval(_pollInterval);
  _pollErrorCount = 0;

  // Always show global progress bar
  showGlobalProgress(taskId);

  const el = $("#task-output");
  if (!el) return;
  el.innerHTML = `<div class="task-status running">${t("task.connecting", { id: taskId })}</div>`;

  // Try WebSocket first
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = `${proto}//${location.host}/api/ws/tasks/${encodeURIComponent(taskId)}`;
  let ws;
  try {
    ws = new WebSocket(wsUrl);
  } catch (e) {
    _fallbackPollTask(taskId);
    return;
  }
  ws.onmessage = (event) => {
    if (!document.getElementById("task-output")) { ws.close(); return; }
    const data = JSON.parse(event.data);
    if (data.error && !data.status) {
      el.innerHTML = `<div class="task-status failed">${t("general.error", { message: data.error })}</div>`;
      return;
    }
    renderTaskStatus(el, taskId, data);
  };
  ws.onerror = () => { ws.close(); };
  ws.onclose = () => {
    const el2 = $("#task-output");
    if (el2 && el2.querySelector(".task-status.running")) {
      _fallbackPollTask(taskId);
    }
  };
}

export function renderTaskStatus(el, taskId, data) {
  if (data.status === "running") {
    let progressHtml = "";
    if (data.progress) {
      const p = data.progress;
      const pct = p.total > 0 ? Math.round((p.processed / p.total) * 100) : 0;
      progressHtml = `<div class="progress-bar" role="progressbar" aria-valuenow="${pct}" aria-valuemin="0" aria-valuemax="100"><div class="progress-fill" style="width:${pct}%"></div></div>
        <div style="font-size:12px;color:var(--text-muted);margin-top:4px">${escapeHtml(data.progress.phase)}: ${data.progress.processed.toLocaleString()} / ${data.progress.total.toLocaleString()} (${pct}%)</div>`;
    }
    el.innerHTML = `<div class="task-status running">${t("task.running", { id: taskId, started: data.started_at })}${progressHtml}</div>`;
  } else if (data.status === "completed") {
    let resultHtml = "";
    if (data.result) {
      resultHtml = "<pre>" + escapeHtml(JSON.stringify(data.result, null, 2)) + "</pre>";
    }
    el.innerHTML = `<div class="task-status completed">${t("task.completed", { id: taskId })}${resultHtml}</div>`;
  } else if (data.status === "failed") {
    el.innerHTML = `<div class="task-status failed">${t("task.failed", { id: taskId, error: data.error })}</div>`;
  }
}

function _fallbackPollTask(taskId) {
  const el = $("#task-output");
  if (!el) return;
  _pollInterval = setInterval(async () => {
    if (!document.getElementById("task-output")) {
      clearInterval(_pollInterval);
      _pollInterval = null;
      return;
    }
    try {
      const data = await api(`/tasks/${encodeURIComponent(taskId)}`);
      _pollErrorCount = 0;
      renderTaskStatus(el, taskId, data);
      if (data.status !== "running") {
        clearInterval(_pollInterval);
        _pollInterval = null;
      }
    } catch (e) {
      _pollErrorCount++;
      if (_pollErrorCount >= 5) {
        clearInterval(_pollInterval);
        _pollInterval = null;
        el.innerHTML = `<div class="task-status failed">${t("task.lost_connection", { count: _pollErrorCount, message: e.message })}</div>`;
      }
    }
  }, 2000);
}

// ── Legacy drawer exports (kept for compatibility) ───

export function openTaskDrawer() {}
export function closeTaskDrawer() {}
