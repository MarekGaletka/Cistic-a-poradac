/* GOD MODE Media Library — Pipeline / Scan Control Center */

import { api, apiPost, apiDelete } from "../api.js";
import { $, escapeHtml, formatBytes, showToast } from "../utils.js";
import { t } from "../i18n.js";
import { showGlobalProgress } from "../tasks.js";
import { openFolderPicker } from "../folder-picker.js";

// ── State ───────────────────────────────────────────────
let _roots = [];
let _sources = [];
let _stats = null;
let _activeTaskId = null;
let _ws = null;
let _wsReconnectDelay = 1000;
let _taskHistory = [];
let _running = false;
let _container = null;
let _httpPollTimer = null;
let _historyExpanded = {};

// ── Public API ──────────────────────────────────────────

export async function render(container) {
  _container = container;

  container.innerHTML = `<div class="loading"><div class="spinner"></div></div>`;

  // Parallel data fetch
  const [rootsData, sourcesData, statsData, tasksData] = await Promise.all([
    api("/roots").catch(() => ({ roots: [] })),
    api("/sources").catch(() => ({ sources: [] })),
    api("/stats").catch(() => null),
    api("/tasks").catch(() => ({ tasks: [] })),
  ]);

  _roots = rootsData.roots || [];
  _sources = sourcesData.sources || [];
  _stats = statsData;
  _taskHistory = (tasksData.tasks || [])
    .filter(t => ["scan", "pipeline", "verify", "quality_analyze"].includes(t.command))
    .sort((a, b) => (b.started_at || "").localeCompare(a.started_at || ""))
    .slice(0, 5);

  // Check if a task is currently running
  const runningTask = _taskHistory.find(t => t.status === "running");
  if (runningTask) {
    _activeTaskId = runningTask.id;
    _running = true;
  }

  _renderAll();

  // If we have a running task, connect WS
  if (_activeTaskId) {
    _connectWs(_activeTaskId);
  }
}

export function cleanup() {
  _disconnectWs();
  if (_httpPollTimer) {
    clearInterval(_httpPollTimer);
    _httpPollTimer = null;
  }
  _container = null;
}

// ── Render ──────────────────────────────────────────────

function _renderAll() {
  if (!_container) return;

  const onlineSources = _sources.filter(s => s.online);
  const totalFiles = _stats?.total_files || 0;
  const totalSize = _stats?.total_size_bytes || 0;

  _container.innerHTML = `
    ${_renderStatCards(totalFiles, totalSize, onlineSources.length, _sources.length)}
    ${_renderSourceChips()}
    ${_renderRoots()}
    ${_renderActions()}
    ${_renderProgress()}
    ${_renderHistory()}
  `;

  _bindEvents();
}

function _renderStatCards(totalFiles, totalSize, online, total) {
  return `<div class="pipeline-stats-row">
    <div class="pipeline-stat-card">
      <div class="pipeline-stat-value">${totalFiles.toLocaleString()}</div>
      <div class="pipeline-stat-label">${t("pipeline.stat_files")}</div>
    </div>
    <div class="pipeline-stat-card">
      <div class="pipeline-stat-value">${formatBytes(totalSize)}</div>
      <div class="pipeline-stat-label">${t("pipeline.stat_size")}</div>
    </div>
    <div class="pipeline-stat-card">
      <div class="pipeline-stat-value">${online}/${total}</div>
      <div class="pipeline-stat-label">${t("pipeline.stat_sources")}</div>
    </div>
  </div>`;
}

function _renderSourceChips() {
  if (!_sources.length) return "";
  const chips = _sources.map(s => {
    const dotClass = s.online ? "source-dot-online" : "source-dot-offline";
    const lastScan = s.last_scan ? new Date(s.last_scan).toLocaleDateString() : "\u2014";
    return `<span class="pipeline-source-chip">
      <span class="pipeline-source-dot ${dotClass}"></span>
      ${escapeHtml(s.name || s.path)}
      <span class="pipeline-source-date">${lastScan}</span>
    </span>`;
  }).join("");
  return `<div class="pipeline-source-chips">${chips}</div>`;
}

function _renderRoots() {
  let chipsHtml = "";
  if (_roots.length > 0) {
    chipsHtml = `<div class="folder-chips">`;
    for (const root of _roots) {
      const name = root.split("/").pop() || root;
      chipsHtml += `<span class="folder-chip">
        <span class="folder-chip-icon">\u{1F4C1}</span> ${escapeHtml(name)}
        <span class="folder-chip-path">${escapeHtml(root)}</span>
        <button class="folder-chip-remove" data-path="${escapeHtml(root)}" aria-label="${t("folder.remove")}">&times;</button>
      </span>`;
    }
    chipsHtml += "</div>";
  } else {
    chipsHtml = `<p class="pipeline-no-roots">${t("pipeline.no_roots")}</p>`;
  }

  return `<div class="pipeline-roots-section">
    <label class="form-label">${t("pipeline.configured_roots")}</label>
    ${chipsHtml}
    <div class="pipeline-roots-buttons">
      <button class="pipeline-add-folder-btn" id="btn-pipeline-add-folder">\u{1F4C1} ${t("folder.add_folder")}</button>
      <button class="pipeline-add-folder-btn" id="btn-pipeline-detect">\u{1F50D} ${t("pipeline.auto_detect")}</button>
    </div>
  </div>`;
}

function _renderActions() {
  const disabled = _running || _roots.length === 0 ? "disabled" : "";

  const actions = [
    { id: "pipeline", icon: "\u{1F680}", label: t("pipeline.start_pipeline"), desc: t("pipeline.action_pipeline_desc") },
    { id: "scan", icon: "\u{1F50D}", label: t("pipeline.scan_only"), desc: t("pipeline.action_scan_desc") },
    { id: "verify", icon: "\u2705", label: t("pipeline.verify"), desc: t("pipeline.action_verify_desc") },
    { id: "quality", icon: "\u{1F3A8}", label: t("pipeline.quality"), desc: t("pipeline.action_quality_desc") },
    { id: "backfill", icon: "\u{1F4DD}", label: t("pipeline.backfill"), desc: t("pipeline.action_backfill_desc") },
  ];

  const cardsHtml = actions.map(a => `
    <button class="pipeline-action-card" id="btn-action-${a.id}" ${disabled}>
      <span class="pipeline-action-icon">${a.icon}</span>
      <span class="pipeline-action-label">${a.label}</span>
      <span class="pipeline-action-desc">${a.desc}</span>
    </button>
  `).join("");

  return `<div class="pipeline-actions-section">
    <label class="form-label">${t("pipeline.actions")}</label>
    <div class="pipeline-actions-grid">${cardsHtml}</div>
    <div class="pipeline-config-row">
      <label class="pipeline-workers-stepper">
        <span>${t("pipeline.workers")}:</span>
        <button class="pipeline-stepper-btn" id="btn-workers-dec">&minus;</button>
        <span id="pipeline-workers-val">4</span>
        <button class="pipeline-stepper-btn" id="btn-workers-inc">+</button>
      </label>
      <label class="pipeline-exiftool-toggle">
        <input type="checkbox" id="pipeline-exiftool" checked>
        <span>${t("pipeline.exiftool")}</span>
      </label>
    </div>
  </div>`;
}

function _renderProgress() {
  if (!_activeTaskId) return `<div id="pipeline-progress" class="pipeline-progress-section hidden"></div>`;

  return `<div id="pipeline-progress" class="pipeline-progress-section">
    <div class="pipeline-progress-header">
      <span class="pipeline-progress-phase" id="progress-phase">${t("pipeline.running")}</span>
      <button class="pipeline-cancel-btn" id="btn-cancel-task">${t("pipeline.cancel")}</button>
    </div>
    <div class="progress-bar pipeline-progress-bar" role="progressbar" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100">
      <div class="progress-fill" id="progress-fill" style="width:0%"></div>
    </div>
    <div class="pipeline-progress-stats" id="progress-stats"></div>
  </div>`;
}

function _renderHistory() {
  if (!_taskHistory.length) return "";

  const rows = _taskHistory.map(task => {
    const statusClass = task.status === "completed" ? "completed" : task.status === "failed" ? "failed" : "running";
    const statusDot = task.status === "completed" ? "\u2705" : task.status === "failed" ? "\u274C" : "\u{1F7E1}";
    const cmdLabel = _commandLabel(task.command);
    const startTime = task.started_at ? new Date(task.started_at).toLocaleString() : "\u2014";
    const duration = task.started_at && task.finished_at
      ? _formatDuration(new Date(task.finished_at) - new Date(task.started_at))
      : task.status === "running" ? "\u2026" : "\u2014";
    const isExpanded = _historyExpanded[task.id];

    let detailHtml = "";
    if (isExpanded) {
      if (task.error) {
        detailHtml = `<div class="pipeline-history-detail pipeline-history-error">${escapeHtml(task.error)}</div>`;
      } else {
        detailHtml = `<div class="pipeline-history-detail">${t("pipeline.history_no_detail")}</div>`;
      }
    }

    return `<div class="pipeline-history-item ${statusClass}" data-task-id="${task.id}">
      <div class="pipeline-history-row">
        <span class="pipeline-history-dot">${statusDot}</span>
        <span class="pipeline-history-cmd">${cmdLabel}</span>
        <span class="pipeline-history-time">${startTime}</span>
        <span class="pipeline-history-duration">${duration}</span>
      </div>
      ${detailHtml}
    </div>`;
  }).join("");

  return `<div class="pipeline-history-section">
    <label class="form-label">${t("pipeline.history")}</label>
    ${rows}
  </div>`;
}

// ── Event binding ───────────────────────────────────────

function _bindEvents() {
  if (!_container) return;

  // Add folder
  _container.querySelector("#btn-pipeline-add-folder")?.addEventListener("click", () => {
    openFolderPicker(async (paths) => {
      _roots = [...new Set([..._roots, ...paths])];
      try { await apiPost("/roots", { roots: _roots }); } catch { /* silent */ }
      _renderAll();
    }, _roots);
  });

  // Auto-detect
  _container.querySelector("#btn-pipeline-detect")?.addEventListener("click", async () => {
    try {
      const data = await api("/sources");
      const paths = (data.sources || []).filter(s => s.online).map(s => s.path);
      if (paths.length) {
        _roots = [...new Set([..._roots, ...paths])];
        try { await apiPost("/roots", { roots: _roots }); } catch { /* silent */ }
        showToast(t("pipeline.sources_detected", { count: paths.length }), "success");
        _renderAll();
      } else {
        showToast(t("pipeline.no_sources"), "info");
      }
    } catch (e) {
      showToast(t("general.error", { message: e.message }), "error");
    }
  });

  // Remove folder chips
  _container.querySelectorAll(".folder-chip-remove").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const path = btn.dataset.path;
      _roots = _roots.filter(r => r !== path);
      try { await apiDelete("/roots", { path }); } catch { /* silent */ }
      _renderAll();
    });
  });

  // Action cards
  _container.querySelector("#btn-action-pipeline")?.addEventListener("click", () => _startAction("/pipeline", "pipeline"));
  _container.querySelector("#btn-action-scan")?.addEventListener("click", () => _startAction("/scan", "scan"));
  _container.querySelector("#btn-action-verify")?.addEventListener("click", () => _startAction("/verify", "verify"));
  _container.querySelector("#btn-action-quality")?.addEventListener("click", () => _startAction("/quality/analyze", "quality"));
  _container.querySelector("#btn-action-backfill")?.addEventListener("click", _startBackfill);

  // Workers stepper
  const workersVal = _container.querySelector("#pipeline-workers-val");
  _container.querySelector("#btn-workers-dec")?.addEventListener("click", () => {
    let v = parseInt(workersVal.textContent, 10);
    if (v > 1) workersVal.textContent = --v;
  });
  _container.querySelector("#btn-workers-inc")?.addEventListener("click", () => {
    let v = parseInt(workersVal.textContent, 10);
    if (v < 8) workersVal.textContent = ++v;
  });

  // Cancel
  _container.querySelector("#btn-cancel-task")?.addEventListener("click", _cancelTask);

  // History expand/collapse
  _container.querySelectorAll(".pipeline-history-item").forEach(item => {
    item.addEventListener("click", () => {
      const id = item.dataset.taskId;
      _historyExpanded[id] = !_historyExpanded[id];
      _renderAll();
    });
  });
}

// ── Actions ─────────────────────────────────────────────

function _getScanConfig() {
  const workers = parseInt(_container?.querySelector("#pipeline-workers-val")?.textContent || "4", 10);
  const exiftool = _container?.querySelector("#pipeline-exiftool")?.checked ?? true;
  return { roots: _roots, workers, extract_exiftool: exiftool };
}

async function _startAction(endpoint, label) {
  try {
    _running = true;
    _renderAll();
    const data = await apiPost(endpoint, _getScanConfig());
    _activeTaskId = data.task_id;
    showToast(t("pipeline.started"), "info");
    showGlobalProgress(data.task_id);
    _connectWs(data.task_id);
    _renderAll();
  } catch (e) {
    _running = false;
    showToast(t("pipeline.start_failed", { message: e.message }), "error");
    _renderAll();
  }
}

async function _startBackfill() {
  try {
    _running = true;
    _renderAll();
    const result = await apiPost("/backfill-metadata", _getScanConfig());
    showToast(t("pipeline.backfill_done"), "success");
    _running = false;
    _renderAll();
  } catch (e) {
    _running = false;
    showToast(t("pipeline.start_failed", { message: e.message }), "error");
    _renderAll();
  }
}

function _cancelTask() {
  _disconnectWs();
  _running = false;
  _activeTaskId = null;
  showToast(t("pipeline.cancelled"), "info");
  _renderAll();
}

// ── WebSocket ───────────────────────────────────────────

function _connectWs(taskId) {
  _disconnectWs();
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const url = `${proto}//${location.host}/api/ws/tasks/${encodeURIComponent(taskId)}`;

  try {
    _ws = new WebSocket(url);
  } catch {
    _fallbackPoll(taskId);
    return;
  }

  _ws.onmessage = (event) => {
    if (!_container) { _disconnectWs(); return; }
    let data;
    try { data = JSON.parse(event.data); } catch { return; }

    if (data.error && !data.status) return;

    _updateProgressUI(data);

    if (data.status === "completed" || data.status === "failed") {
      _running = false;
      _activeTaskId = null;
      _disconnectWs();
      // Refresh history
      api("/tasks").then(d => {
        _taskHistory = (d.tasks || [])
          .filter(t => ["scan", "pipeline", "verify", "quality_analyze"].includes(t.command))
          .sort((a, b) => (b.started_at || "").localeCompare(a.started_at || ""))
          .slice(0, 5);
        _renderAll();
      }).catch(() => _renderAll());
    }
  };

  _ws.onerror = () => { _ws?.close(); };
  _ws.onclose = () => {
    _ws = null;
    if (_running && _activeTaskId) {
      // Reconnect or fallback
      setTimeout(() => {
        if (_running && _activeTaskId) _fallbackPoll(_activeTaskId);
      }, _wsReconnectDelay);
      _wsReconnectDelay = Math.min(_wsReconnectDelay * 2, 10000);
    }
  };
}

function _disconnectWs() {
  if (_ws) {
    try { _ws.close(); } catch { /* ignore */ }
    _ws = null;
  }
  if (_httpPollTimer) {
    clearInterval(_httpPollTimer);
    _httpPollTimer = null;
  }
}

function _fallbackPoll(taskId) {
  if (_httpPollTimer) clearInterval(_httpPollTimer);
  _httpPollTimer = setInterval(async () => {
    if (!_container || !_running) { clearInterval(_httpPollTimer); _httpPollTimer = null; return; }
    try {
      const data = await api(`/tasks/${encodeURIComponent(taskId)}`);
      _updateProgressUI(data);
      if (data.status === "completed" || data.status === "failed") {
        _running = false;
        _activeTaskId = null;
        clearInterval(_httpPollTimer);
        _httpPollTimer = null;
        _renderAll();
      }
    } catch { /* retry */ }
  }, 2000);
}

// ── Progress UI updates ─────────────────────────────────

let _lastProcessed = 0;
let _lastTimestamp = 0;

function _updateProgressUI(data) {
  const section = _container?.querySelector("#pipeline-progress");
  if (!section) return;
  section.classList.remove("hidden");

  const phaseEl = section.querySelector("#progress-phase");
  const fillEl = section.querySelector("#progress-fill");
  const statsEl = section.querySelector("#progress-stats");

  if (data.status === "completed") {
    if (phaseEl) phaseEl.textContent = t("pipeline.completed");
    if (fillEl) fillEl.style.width = "100%";
    const r = data.result || {};
    if (statsEl) {
      const parts = [];
      if (r.files_scanned != null) parts.push(`${t("pipeline.result_scanned")}: ${r.files_scanned}`);
      if (r.files_new != null) parts.push(`${t("pipeline.result_new")}: ${r.files_new}`);
      if (r.duplicate_groups != null) parts.push(`${t("pipeline.result_dupes")}: ${r.duplicate_groups}`);
      if (r.total_checked != null) parts.push(`${t("pipeline.result_checked")}: ${r.total_checked}`);
      if (r.missing != null) parts.push(`${t("pipeline.result_missing")}: ${r.missing}`);
      statsEl.innerHTML = parts.join(" &middot; ");
    }
    return;
  }

  if (data.status === "failed") {
    if (phaseEl) phaseEl.textContent = t("pipeline.failed");
    if (fillEl) { fillEl.style.width = "100%"; fillEl.classList.add("failed"); }
    if (statsEl) statsEl.textContent = data.error || "";
    return;
  }

  // Running
  if (data.progress) {
    const p = data.progress;
    const pct = p.total > 0 ? Math.round((p.processed / p.total) * 100) : 0;
    if (phaseEl) phaseEl.textContent = p.phase || t("pipeline.running");
    if (fillEl) fillEl.style.width = `${pct}%`;

    // Speed calculation
    const now = Date.now();
    let speedStr = "";
    if (_lastTimestamp > 0 && p.processed > _lastProcessed) {
      const elapsed = (now - _lastTimestamp) / 1000;
      const speed = (p.processed - _lastProcessed) / elapsed;
      speedStr = `${speed.toFixed(1)} ${t("pipeline.files_per_sec")}`;
      if (speed > 0 && p.total > p.processed) {
        const eta = Math.round((p.total - p.processed) / speed);
        speedStr += ` \u2022 ETA ${_formatDuration(eta * 1000)}`;
      }
    }
    _lastProcessed = p.processed;
    _lastTimestamp = now;

    if (statsEl) {
      statsEl.innerHTML = `${(p.processed || 0).toLocaleString()} / ${(p.total || 0).toLocaleString()} (${pct}%)${speedStr ? ` &middot; ${speedStr}` : ""}`;
    }
  }
}

// ── Helpers ─────────────────────────────────────────────

function _commandLabel(cmd) {
  const labels = {
    scan: t("pipeline.scan_only"),
    pipeline: t("pipeline.start_pipeline"),
    verify: t("pipeline.verify"),
    quality_analyze: t("pipeline.quality"),
  };
  return labels[cmd] || cmd;
}

function _formatDuration(ms) {
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rs = s % 60;
  if (m < 60) return `${m}m ${rs}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}
