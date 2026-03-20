/* GOD MODE Media Library — Task management */

import { api } from "./api.js";
import { $, escapeHtml, showToast } from "./utils.js";
import { t } from "./i18n.js";

let _pollInterval = null;
let _pollErrorCount = 0;

export function cleanupTasks() {
  if (_pollInterval) {
    clearInterval(_pollInterval);
    _pollInterval = null;
  }
}

export function pollTask(taskId) {
  if (_pollInterval) clearInterval(_pollInterval);
  _pollErrorCount = 0;
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
    showToast(t("task.completed_toast"), "success");
  } else if (data.status === "failed") {
    el.innerHTML = `<div class="task-status failed">${t("task.failed", { id: taskId, error: data.error })}</div>`;
    showToast(t("task.failed_toast", { error: data.error || "unknown" }), "error");
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

// ── Task drawer ─────────────────────────────────────

export function openTaskDrawer() {
  const drawer = $("#task-drawer");
  if (drawer) {
    drawer.classList.add("open");
    drawer.setAttribute("aria-hidden", "false");
  }
}

export function closeTaskDrawer() {
  const drawer = $("#task-drawer");
  if (drawer) {
    drawer.classList.remove("open");
    drawer.setAttribute("aria-hidden", "true");
  }
}
