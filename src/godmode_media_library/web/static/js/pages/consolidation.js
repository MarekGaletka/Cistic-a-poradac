/* GOD MODE Media Library — Consolidation Wizard */

import { t } from "../i18n.js";
import { $, showToast, formatBytes } from "../utils.js";
import { api, apiPost } from "../api.js";
import { openFolderPicker } from "../folder-picker.js";

let _container = null;
let _pollTimer = null;
let _currentPhase = 0; // 0=A, 1=B, 2=C, 3=D, 4=E, 5=F
let _transferActive = false;
let _lastActivity = 0;
let _localRoots = [];
let _pollFailCount = 0;      // Issue #8: consecutive poll failures
let _lastPollSuccess = 0;    // Issue #9: timestamp of last successful poll

/**
 * Pick the most relevant job to display.
 * Priority: running > paused > created > failed (most recent only).
 * This prevents old failed jobs from overshadowing a currently running job.
 */
function _pickActiveJob(jobs) {
  const priority = { running: 0, paused: 1, created: 2, completed: 3, failed: 4 };
  const candidates = jobs
    .filter(j => j.status in priority)
    .sort((a, b) => {
      const pa = priority[a.status], pb = priority[b.status];
      if (pa !== pb) return pa - pb;
      // Same priority — prefer most recently updated
      return (b.updated_at || "").localeCompare(a.updated_at || "");
    });
  return candidates[0] || null;
}

function _renderLocalChips() {
  const el = document.getElementById("wiz-local-chips");
  if (!el) return;
  if (_localRoots.length === 0) {
    el.innerHTML = `<span class="text-muted">${t("consolidation.no_local_roots")}</span>`;
    return;
  }
  el.innerHTML = _localRoots.map((p, i) => `
    <span class="wiz-chip">
      <span class="wiz-chip-text">${escapeHtml(p)}</span>
      <button type="button" class="wiz-chip-remove" data-idx="${i}">\u00D7</button>
    </span>
  `).join("");
  el.querySelectorAll(".wiz-chip-remove").forEach(btn => {
    btn.addEventListener("click", () => {
      _localRoots.splice(Number(btn.dataset.idx), 1);
      _renderLocalChips();
    });
  });
}

async function _autoDetectDisks(showToastMsg = false) {
  try {
    const data = await api("/consolidation/available-disks");
    const disks = data.disks || [];
    let added = 0;
    for (const d of disks) {
      if (!_localRoots.includes(d.path)) {
        _localRoots.push(d.path);
        added++;
      }
    }
    _renderLocalChips();
    if (showToastMsg) {
      showToast(added > 0
        ? t("consolidation.disks_detected", { count: added })
        : t("consolidation.no_new_disks"), added > 0 ? "success" : "info");
    }
  } catch (_) { /* ignore */ }
}

const WIZARD_PHASES = [
  { key: "A", icon: "\uD83D\uDCE1", label: () => t("consolidation.phase_a") },
  { key: "B", icon: "\uD83E\uDDF9", label: () => t("consolidation.phase_b") },
  { key: "C", icon: "\uD83D\uDCC1", label: () => t("consolidation.phase_c") },
  { key: "D", icon: "\u270D\uFE0F",  label: () => t("consolidation.phase_d") },
  { key: "E", icon: "\uD83D\uDCE5", label: () => t("consolidation.phase_e") },
  { key: "F", icon: "\uD83D\uDD04", label: () => t("consolidation.phase_f") },
];

// Phase key mapping from API status
const API_PHASE_MAP = {
  // Phase A: Collect data from all sources
  wait_for_sources: 0,
  cloud_catalog_scan: 0,
  local_scan: 0,
  register_files: 0,
  stream: 0,
  retry_failed: 0,
  // Phase B: Cleanup & verification
  extract_archives: 1,
  dedup: 1,
  verify: 1,
  report: 1,
  // Phase C: Organize
  organize: 2,
  // Phase E: Sync to disk
  sync_to_disk: 4,
};

export async function render(container) {
  _container = container;
  stopPolling();

  container.innerHTML = `
    <div class="consolidation-page">
      <div class="consolidation-header">
        <h2>\uD83C\uDF0D ${t("consolidation.title")} \uD83D\uDE80</h2>
        <p class="consolidation-subtitle">${t("consolidation.subtitle")}</p>
      </div>

      <div class="wizard-steps" id="wizard-steps"></div>
      <div class="wizard-content" id="wizard-content">
        <div class="loading-sm">${t("general.loading")}</div>
      </div>
    </div>`;

  await initWizard();
}

async function initWizard() {
  try {
    const data = await api("/consolidation/status");
    const active = _pickActiveJob(data.jobs || []);

    // Detect current phase from active job
    if (active) {
      const phase = active.progress?.phase || "";
      if (API_PHASE_MAP[phase] !== undefined) {
        _currentPhase = API_PHASE_MAP[phase];
      }
      _transferActive = active.status === "running";
    }

    renderStepIndicator();
    renderPhaseContent(data, active);

    if (active && (active.status === "running" || active.status === "paused")) {
      startPolling();
    }
  } catch (e) {
    const el = $("#wizard-content");
    if (el) el.innerHTML = `<div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}

// ---------------------------------------------------------------------------
// Step indicator
// ---------------------------------------------------------------------------

function renderStepIndicator() {
  const el = $("#wizard-steps");
  if (!el) return;

  el.innerHTML = WIZARD_PHASES.map((p, i) => {
    const done = i < _currentPhase;
    const active = i === _currentPhase;
    const cls = done ? "wiz-step-done" : active ? "wiz-step-active" : "wiz-step-pending";
    const statusIcon = done ? "\u2705" : active ? "\uD83D\uDFE2" : "\u26AA";
    return `
      <div class="wiz-step ${cls}" data-phase="${i}">
        <span class="wiz-step-status">${statusIcon}</span>
        <span class="wiz-step-icon">${p.icon}</span>
        <span class="wiz-step-label">${p.key}: ${p.label()}</span>
      </div>`;
  }).join('<div class="wiz-step-connector"></div>');

  // Allow clicking completed / current steps
  el.querySelectorAll(".wiz-step").forEach(step => {
    step.addEventListener("click", () => {
      const idx = parseInt(step.dataset.phase, 10);
      if (idx <= _currentPhase) {
        _currentPhase = idx;
        renderStepIndicator();
        reloadPhase();
      }
    });
  });
}

async function reloadPhase() {
  try {
    const data = await api("/consolidation/status");
    const active = _pickActiveJob(data.jobs || []);
    // Merge detailed task progress (files_cataloged, phase_label, etc.) into job progress
    if (active) {
      try {
        const tasks = await api("/tasks");
        const task = (tasks.tasks || []).find(t => t.command === "consolidation:ultimate" && t.status === "running");
        if (task) {
          const taskDetail = await api(`/tasks/${task.id}`);
          if (taskDetail?.progress) {
            active._taskProgress = taskDetail.progress;
          }
        }
      } catch (_) { /* task fetch is best-effort */ }
    }
    renderPhaseContent(data, active);
  } catch (e) {
    const el = $("#wizard-content");
    if (el) el.innerHTML = `<div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}

// ---------------------------------------------------------------------------
// Phase content rendering
// ---------------------------------------------------------------------------

function renderPhaseContent(data, activeJob) {
  const el = $("#wizard-content");
  if (!el) return;

  switch (_currentPhase) {
    case 0: renderPhaseA(el, data, activeJob); break;
    case 1: renderPhaseB(el, data, activeJob); break;
    case 2: renderPhaseC(el, data, activeJob); break;
    case 3: renderPhaseD(el, data, activeJob); break;
    case 4: renderPhaseE(el, data, activeJob); break;
    case 5: renderPhaseF(el, data, activeJob); break;
  }
}

// ---------------------------------------------------------------------------
// PHASE A - Sources & Transfer
// ---------------------------------------------------------------------------

function renderPhaseA(el, data, activeJob) {
  const sourcesAvail = data.sources_available || [];
  const sourcesUnavail = data.sources_unavailable || [];
  const allSources = [...sourcesAvail, ...sourcesUnavail];
  const isRunning = activeJob && (activeJob.status === "running" || activeJob.status === "paused" || activeJob.status === "created" || activeJob.status === "failed");
  const checkpointProgress = activeJob?.progress || {};
  const taskProgress = activeJob?.task_progress || {};
  // Merge: task progress has the detailed fields, checkpoint progress has transfer totals
  const progress = { ...checkpointProgress, ...taskProgress };
  const isPaused = activeJob?.status === "paused" || progress.paused;

  // If transfer is active, show transfer view
  if (isRunning) {
    renderPhaseATransfer(el, activeJob, progress, isPaused);
    return;
  }

  const savedConfig = activeJob?.config || {};
  const hasUnavailable = sourcesUnavail.length > 0;

  el.innerHTML = `
    <div class="wiz-phase-card">
      <div class="wiz-phase-header">
        <h3>\uD83D\uDCE1 ${t("consolidation.sources_title")}</h3>
        <p class="wiz-phase-desc">${t("consolidation.sources_desc")}</p>
      </div>

      ${hasUnavailable ? `<div class="wiz-warning-bar">\u26A0\uFE0F ${t("consolidation.sources_warning")}</div>` : ""}
      ${!hasUnavailable && allSources.length > 0 ? `<div class="wiz-success-bar">\u2705 ${t("consolidation.sources_all_ok")}</div>` : ""}
      ${allSources.length === 0 ? `<div class="wiz-warning-bar">\u26A0\uFE0F ${t("consolidation.sources_none")}</div>` : ""}

      <div class="wiz-section">
        <label class="wiz-section-label">${t("consolidation.source_remotes")}</label>
        <div class="wiz-source-list" id="wiz-sources">
          ${allSources.length > 0 ? allSources.map(s => {
            const online = sourcesAvail.includes(s);
            const checked = savedConfig.source_remotes?.includes(s) ? "checked" : (online ? "checked" : "");
            return `<label class="wiz-source-item ${online ? "" : "wiz-source-offline"}">
              <input type="checkbox" name="source" value="${escapeHtml(s)}" ${checked}>
              <span class="wiz-source-name">${escapeHtml(s)}</span>
              <span class="wiz-source-status">${online ? "\uD83D\uDFE2" : "\uD83D\uDD34"}</span>
            </label>`;
          }).join("") : `<span class="text-muted">${t("consolidation.no_remotes")}</span>`}
        </div>
      </div>

      <div class="wiz-section">
        <label class="wiz-section-label">${t("consolidation.local_roots")}</label>
        <div id="wiz-local-chips" class="wiz-chips"></div>
        <div class="wiz-hint-row">
          <button type="button" id="btn-wiz-add-folder" class="wiz-btn-secondary wiz-btn-sm">\uD83D\uDCC2 ${t("consolidation.add_folder")}</button>
          <button type="button" id="btn-wiz-detect-disks" class="wiz-btn-secondary wiz-btn-sm">\uD83D\uDD0D ${t("consolidation.detect_disks")}</button>
        </div>
      </div>

      <div class="wiz-section wiz-section-row">
        <div class="wiz-field">
          <label class="wiz-section-label">${t("consolidation.dest_remote")}</label>
          <input type="text" id="wiz-dest-remote" class="wiz-input" value="${escapeHtml(savedConfig.dest_remote || "gws-backup")}" placeholder="gws-backup">
        </div>
        <div class="wiz-field">
          <label class="wiz-section-label">${t("consolidation.dest_path")}</label>
          <input type="text" id="wiz-dest-path" class="wiz-input" value="${escapeHtml(savedConfig.dest_path || "GML-Consolidated")}" placeholder="GML-Consolidated">
        </div>
      </div>

      <div class="wiz-section wiz-section-row">
        <div class="wiz-field">
          <label class="wiz-section-label">${t("consolidation.bwlimit")}</label>
          <input type="text" id="wiz-bwlimit" class="wiz-input" value="${escapeHtml(savedConfig.bwlimit || "")}" placeholder="50M">
          <span class="wiz-hint">${t("consolidation.bwlimit_hint")}</span>
        </div>
        <div class="wiz-field">
          <label class="wiz-toggle">
            <input type="checkbox" id="wiz-media-only">
            <span>${t("consolidation.media_only")}</span>
          </label>
          <span class="wiz-hint">${t("consolidation.all_files_note")}</span>
          <label class="wiz-toggle wiz-mt-sm">
            <input type="checkbox" id="wiz-dry-run">
            <span>${t("consolidation.dry_run")}</span>
          </label>
        </div>
      </div>

      <div class="wiz-nav">
        <div></div>
        <button id="btn-wiz-start" class="wiz-btn-primary">\uD83D\uDE80 ${t("consolidation.start")}</button>
      </div>
    </div>`;

  // Render initial local root chips
  _localRoots = [...(savedConfig.local_roots || [])];
  _renderLocalChips();

  // Auto-detect connected disks and add them
  _autoDetectDisks();

  // Bind add folder via picker
  bindButton("#btn-wiz-add-folder", () => {
    openFolderPicker((paths) => {
      for (const p of paths) {
        if (!_localRoots.includes(p)) _localRoots.push(p);
      }
      _renderLocalChips();
    }, _localRoots);
  });

  // Bind detect disks
  bindButton("#btn-wiz-detect-disks", () => _autoDetectDisks(true));

  // Bind start
  bindButton("#btn-wiz-start", doStart);
}

function renderPhaseATransfer(el, activeJob, progress, isPaused) {
  const checkpointProg = activeJob?.progress || {};
  const transferred = checkpointProg.completed || progress.files_transferred || 0;
  const totalFiles = checkpointProg.total || 0;
  const failed = checkpointProg.failed || progress.files_failed || 0;
  const speed = progress.transfer_speed_bps || 0;
  const speedExplicitlyZero = "transfer_speed_bps" in progress && progress.transfer_speed_bps === 0;
  const isServerSideMove = speedExplicitlyZero && transferred > 0;
  const eta = progress.eta_seconds || 0;
  const currentFile = progress.current_file || "";
  const bytesTransferred = checkpointProg.bytes_transferred || progress.bytes_transferred || 0;
  const bytesTotal = progress.bytes_total_estimate || 0;
  const pct = totalFiles > 0 ? Math.min((transferred / totalFiles) * 100, 100)
    : bytesTotal > 0 ? Math.min((bytesTransferred / bytesTotal) * 100, 100) : 0;

  // Pipeline step mapping — Czech labels for display (#12)
  const STEP_ORDER = [
    "wait_for_sources", "cloud_catalog_scan", "local_scan", "register_files",
    "stream", "extract_archives", "dedup", "retry_failed", "verify", "organize", "report", "complete"
  ];
  const STEP_LABELS = {
    wait_for_sources: "Čekám na zdroje",
    cloud_catalog_scan: "Skenuji cloudové zdroje",
    local_scan: "Skenuji lokální soubory",
    register_files: "Registruji soubory",
    stream: "Přenáším soubory",
    extract_archives: "Rozbaluji archivy",
    dedup: "Odstraňuji duplikáty",
    retry_failed: "Opakuji selhané",
    verify: "Ověřuji integritu",
    organize: "Organizuji soubory",
    report: "Generuji report",
    complete: "Hotovo"
  };
  const currentStepName = activeJob?.current_step || progress.phase || "";
  const stepIdx = STEP_ORDER.indexOf(currentStepName);
  const totalSteps = STEP_ORDER.length;
  const phasePct = stepIdx >= 0 ? Math.min(((stepIdx + 1) / totalSteps) * 100, 100)
    : progress.total_steps > 0 ? Math.min((progress.current_step / progress.total_steps) * 100, 100) : 0;
  const stepDisplay = stepIdx >= 0 ? stepIdx + 1 : Math.max(progress.current_step || 0, 1);
  // Use Czech label, fall back to phase_label from server, then raw step name (#12)
  const stepLabel = STEP_LABELS[currentStepName] || progress.phase_label || currentStepName || "";

  // Sub-phase label from server: e.g. "dropbox: 234/1560 (1.2 GB) [3.5 MB/s]" (#3, #12)
  const subPhaseLabel = progress.phase_label || "";
  const showSubPhase = currentStepName === "stream" || currentStepName === "retry_failed" || currentStepName === "organize";

  // Phase type detection
  const phase = currentStepName || progress.phase || "";
  const isTransferPhase = phase === "stream" || phase === "stream_cloud" || phase === "retry_failed" || phase === "download_to_disk";
  const isMetadataPhase = phase === "organize" || phase === "dedup" || phase === "verify" || phase === "extract_archives" || phase === "report";
  const now = Date.now();
  if (speed > 0 || transferred > 0) _lastActivity = now;
  const stalled = !isPaused && _transferActive && isTransferPhase && _lastActivity > 0 && (now - _lastActivity > 60000);

  // Google limit detection
  const googleLimit = progress.google_limit_reached || false;

  // Status detection
  const isFailed = activeJob?.status === "failed";
  const isCompleted = activeJob?.status === "completed";
  const jobError = activeJob?.error || progress.error || "";
  const statusLabel = isCompleted ? "DOKONČENO"
    : isFailed ? "SELHALO"
    : isPaused ? "POZASTAVENO"
    : "BĚŽÍ";
  const statusColor = isCompleted ? "var(--color-success, #38a169)"
    : isFailed ? "var(--color-danger, #e53e3e)"
    : isPaused ? "var(--color-warning)"
    : "var(--color-success)";

  // Failed state reason (#10)
  let failedReason = "";
  if (isFailed && jobError) {
    failedReason = jobError;
  } else if (isPaused && jobError) {
    failedReason = jobError;
  }

  // Per-remote scan progress
  const scanProgress = progress.scan_progress || {};
  const scanRemotes = Object.keys(scanProgress);

  el.innerHTML = `
    <div class="wiz-phase-card">
      <div class="wiz-phase-header">
        <h3>\u2601\uFE0F Přenos dat na Google 6TB</h3>
        <p class="wiz-phase-desc">Všechna data ze všech zdrojů se kopírují na cílový cloud. Bez deduplikace.</p>
      </div>

      <div class="wiz-status-badge" style="border-color:${statusColor}">
        <span class="wiz-badge" style="background:${statusColor}">${statusLabel}</span>
        <span class="wiz-job-id">${escapeHtml((activeJob.job_id || "").slice(0, 8))}</span>
      </div>

      ${failedReason ? `<div class="wiz-error-msg" style="margin:12px 0;padding:12px 16px;background:${isFailed ? "var(--color-danger-bg, #fff5f5)" : "var(--color-warning-bg, #fffaf0)"};border:1px solid ${isFailed ? "var(--color-danger, #e53e3e)" : "var(--color-warning, #dd6b20)"};border-radius:var(--radius-sm);font-size:0.9em;">
        ${isFailed ? "\u274C" : "\u23F8\uFE0F"} <strong>${isFailed ? "Důvod selhání" : "Důvod pozastavení"}:</strong> ${escapeHtml(failedReason)}
      </div>` : ""}
      ${stalled ? `<div class="wiz-watchdog-bar">\u26A0\uFE0F Přenos se zastavil — zkontrolujte připojení</div>` : ""}
      ${googleLimit ? `<div class="wiz-warning-bar">\uD83D\uDEAB Denní limit Google uploadu (750 GB) — pokračuje automaticky zítra</div>` : ""}

      <div class="wiz-phase-stepper">
        <div class="wiz-stepper-label">${stepLabel}</div>
        <div class="wiz-stepper-track">
          ${STEP_ORDER.map((s, i) => {
            const done = i < stepIdx;
            const active = i === stepIdx;
            const cls = done ? "wiz-step-done" : active ? "wiz-step-active" : "wiz-step-pending";
            return `<div class="wiz-step-dot ${cls}" title="${STEP_LABELS[s] || s}"></div>`;
          }).join("")}
        </div>
        <div class="wiz-stepper-count">${stepDisplay} / ${totalSteps}</div>
      </div>

      ${showSubPhase && subPhaseLabel ? `<div class="wiz-sub-phase"><span class="wiz-sub-phase-dot"></span> ${escapeHtml(subPhaseLabel)}</div>` : ""}
      ${!isPaused && !isFailed && !isCompleted ? `<div class="wiz-activity-pulse">
        <span class="wiz-pulse-dot${_lastPollSuccess > 0 && (Date.now() - _lastPollSuccess > 30000) ? ' wiz-pulse-warn' : ''}"></span>
        <span>${_lastPollSuccess > 0 ? `Poslední aktualizace: před ${_formatSecondsAgo(_lastPollSuccess)}` : "Probíhají operace..."}</span>
      </div>` : ""}

      <div class="wiz-phase-progress">
        ${progress.files_cataloged > 0 ? `<div class="wiz-phase-detail">\uD83D\uDCC2 Zkatalogizováno: <strong>${(progress.files_cataloged || 0).toLocaleString("cs-CZ")}</strong></div>` : ""}
        ${progress.files_unique > 0 ? `<div class="wiz-phase-detail">\u2728 Unikátních: <strong>${(progress.files_unique || 0).toLocaleString("cs-CZ")}</strong></div>` : ""}
        ${progress.files_verified > 0 ? `<div class="wiz-phase-detail">\u2705 Ověřeno: <strong>${(progress.files_verified || 0).toLocaleString("cs-CZ")}</strong></div>` : ""}
        ${progress.files_retried > 0 ? `<div class="wiz-phase-detail">\uD83D\uDD04 Opakováno: <strong>${(progress.files_retried || 0).toLocaleString("cs-CZ")}</strong></div>` : ""}
      </div>

      ${scanRemotes.length > 0 ? `
        <div class="wiz-scan-section">
          <label class="wiz-section-label">Skenování zdrojů</label>
          ${scanRemotes.map(r => {
            const s = scanProgress[r];
            const sPct = s.total > 0 ? Math.min((s.done / s.total) * 100, 100) : 0;
            return `<div class="wiz-scan-row">
              <span class="wiz-scan-remote">${escapeHtml(r)}</span>
              <div class="wiz-progress-track"><div class="wiz-progress-fill" style="width:${sPct.toFixed(1)}%"></div></div>
              <span class="wiz-scan-count">${(s.done || 0).toLocaleString("cs-CZ")} / ${(s.total || 0).toLocaleString("cs-CZ")}</span>
            </div>`;
          }).join("")}
        </div>` : ""}

      <div class="wiz-transfer-stats">
        <div class="wiz-progress-text-main">
          ${totalFiles > 0
            ? `Přeneseno <strong>${transferred.toLocaleString("cs-CZ")} / ${totalFiles.toLocaleString("cs-CZ")}</strong> souborů <span class="wiz-progress-pct-inline">(${pct.toFixed(1)}\u00A0%)</span>`
            : bytesTotal > 0
              ? `Přeneseno <strong>${formatBytes(bytesTransferred)} / ${formatBytes(bytesTotal)}</strong> <span class="wiz-progress-pct-inline">(${pct.toFixed(1)}\u00A0%)</span>`
              : `Přeneseno <strong>${transferred.toLocaleString("cs-CZ")}</strong> souborů`}
        </div>
        <div class="wiz-metrics-grid">
          <div class="wiz-metric">
            <span class="wiz-metric-value">${totalFiles > 0 ? `${transferred.toLocaleString("cs-CZ")} / ${totalFiles.toLocaleString("cs-CZ")}` : transferred.toLocaleString("cs-CZ")}</span>
            <span class="wiz-metric-label">Přenesených souborů</span>
          </div>
          <div class="wiz-metric">
            <span class="wiz-metric-value">${bytesTotal > 0 ? `${formatBytes(bytesTransferred)} / ${formatBytes(bytesTotal)}` : formatBytes(bytesTransferred)}</span>
            <span class="wiz-metric-label">Přeneseno dat</span>
          </div>
          ${isTransferPhase ? `
          <div class="wiz-metric">
            <span class="wiz-metric-value">${speed > 0 ? formatBytes(speed) + "/s" : (isServerSideMove ? "Okamžité operace (server-side move)" : "\u2014")}</span>
            <span class="wiz-metric-label">${isServerSideMove ? "Okamžité operace" : "Rychlost"}</span>
          </div>
          ${!(isMetadataPhase || isServerSideMove) ? `<div class="wiz-metric">
            <span class="wiz-metric-value">${speed > 0 && eta > 0 ? formatEta(eta) : "\u2014"}</span>
            <span class="wiz-metric-label">Zbývající čas</span>
          </div>` : ""}` : ""}
          <div class="wiz-metric ${failed > 0 ? "wiz-metric-warn" : ""}">
            <span class="wiz-metric-value">${failed.toLocaleString("cs-CZ")}</span>
            <span class="wiz-metric-label">Selhalo</span>
          </div>
        </div>
        ${currentFile ? `<div class="wiz-current-file"><span>Aktuální:</span> <code>${escapeHtml(currentFile)}</code></div>` : ""}
      </div>

      ${isCompleted ? `
      <div class="wiz-completed-summary" style="margin:16px 0;padding:16px;background:var(--color-success-bg, #f0fff4);border:1px solid var(--color-success, #38a169);border-radius:var(--radius-sm);">
        \u2705 <strong>Přenos dokončen úspěšně.</strong> ${checkpointProg.skipped > 0 ? ` ${checkpointProg.skipped.toLocaleString("cs-CZ")} souborů přeskočeno (neexistující cache).` : ""}
      </div>` : ""}

      <div class="wiz-nav">
        <div></div>
        ${isCompleted ? `
        <div class="wiz-btn-group">
          <button id="btn-wiz-restart" class="wiz-btn-primary">\u2795 Nový job</button>
        </div>` : isFailed ? `
        <div class="wiz-btn-group">
          <button id="btn-wiz-restart" class="wiz-btn-primary">\uD83D\uDD04 Spustit znovu</button>
        </div>` : `
        <div class="wiz-btn-group">
          <button id="btn-wiz-pause" class="wiz-btn-warning" ${isPaused ? "disabled" : ""}>\u23F8\uFE0F Pozastavit</button>
          <button id="btn-wiz-resume" class="wiz-btn-secondary" ${!isPaused ? "disabled" : ""}>\u25B6\uFE0F Pokračovat</button>
        </div>`}
      </div>
    </div>`;

  if (isFailed || isCompleted) {
    bindButton("#btn-wiz-restart", doStart);
  } else {
    bindButton("#btn-wiz-pause", doPause);
    bindButton("#btn-wiz-resume", doResume);
  }
}

// ---------------------------------------------------------------------------
// PHASE B - Cleanup
// ---------------------------------------------------------------------------

function renderPhaseB(el, data, activeJob) {
  const progress = activeJob?.progress || {};
  const archives = progress.archives || {};
  const dedup = progress.dedup || {};
  const verify = progress.verify || {};

  const archPct = archives.total > 0 ? Math.min((archives.done / archives.total) * 100, 100) : 0;
  const dedupPct = dedup.total > 0 ? Math.min((dedup.done / dedup.total) * 100, 100) : 0;
  const verifyPct = verify.total > 0 ? Math.min((verify.done / verify.total) * 100, 100) : 0;

  el.innerHTML = `
    <div class="wiz-phase-card">
      <div class="wiz-phase-header">
        <h3>\uD83E\uDDF9 ${t("consolidation.cleanup_title")}</h3>
        <p class="wiz-phase-desc">${t("consolidation.cleanup_desc")}</p>
      </div>

      <div class="wiz-progress-section">
        <div class="wiz-progress-item">
          <div class="wiz-progress-item-header">
            <span>\uD83D\uDCE6 ${t("consolidation.archives_extracting")}</span>
            <span>${archPct.toFixed(0)}%</span>
          </div>
          <div class="wiz-progress-track"><div class="wiz-progress-fill" style="width:${archPct.toFixed(1)}%"></div></div>
          <div class="wiz-progress-detail">
            <span>${t("consolidation.archives_found")}: ${(archives.total || 0).toLocaleString("cs-CZ")}</span>
            <span>${t("consolidation.archives_extracted")}: ${(archives.extracted_files || 0).toLocaleString("cs-CZ")}</span>
          </div>
        </div>

        <div class="wiz-progress-item">
          <div class="wiz-progress-item-header">
            <span>\uD83D\uDD0D ${t("consolidation.dedup_progress")}</span>
            <span>${dedupPct.toFixed(0)}%</span>
          </div>
          <div class="wiz-progress-track"><div class="wiz-progress-fill" style="width:${dedupPct.toFixed(1)}%"></div></div>
          <div class="wiz-progress-detail">
            <span>${t("consolidation.dedup_groups")}: ${(dedup.groups_found || 0).toLocaleString("cs-CZ")}</span>
            <span>${t("consolidation.dedup_removed")}: ${(dedup.removed || 0).toLocaleString("cs-CZ")}</span>
            <span>${t("consolidation.dedup_freed")}: ${formatBytes(dedup.space_freed || 0)}</span>
          </div>
        </div>

        <div class="wiz-progress-item">
          <div class="wiz-progress-item-header">
            <span>\u2705 ${t("consolidation.verify_progress")}</span>
            <span>${verifyPct.toFixed(0)}%</span>
          </div>
          <div class="wiz-progress-track"><div class="wiz-progress-fill" style="width:${verifyPct.toFixed(1)}%"></div></div>
          <div class="wiz-progress-detail">
            <span>${t("consolidation.verify_checked")}: ${(verify.done || 0).toLocaleString("cs-CZ")}</span>
            <span>${t("consolidation.verify_ok")}: ${(verify.ok || 0).toLocaleString("cs-CZ")}</span>
            <span>${t("consolidation.verify_errors")}: ${(verify.errors || 0).toLocaleString("cs-CZ")}</span>
          </div>
        </div>
      </div>

      ${renderReportDashboard(progress)}

      <div class="wiz-nav">
        <button class="wiz-btn-secondary" data-prev>\u2190 ${t("consolidation.phase_a")}</button>
        <button id="btn-wiz-next-c" class="wiz-btn-primary">${t("consolidation.report_confirm")} \u2192</button>
      </div>
    </div>`;

  bindButton("#btn-wiz-next-c", () => advancePhase(2));
  el.querySelector("[data-prev]")?.addEventListener("click", () => { _currentPhase = 0; renderStepIndicator(); reloadPhase(); });
}

function renderReportDashboard(progress) {
  const report = progress.report || {};
  if (!report.total_files && !progress.files_transferred) return "";

  return `
    <div class="wiz-report-dashboard">
      <h4>\uD83D\uDCCA ${t("consolidation.report_title")}</h4>
      <div class="wiz-report-grid">
        <div class="wiz-report-card">
          <span class="wiz-report-value">${(report.total_files || progress.files_transferred || 0).toLocaleString("cs-CZ")}</span>
          <span class="wiz-report-label">${t("consolidation.report_total_files")}</span>
        </div>
        <div class="wiz-report-card">
          <span class="wiz-report-value">${(report.duplicates_removed || 0).toLocaleString("cs-CZ")}</span>
          <span class="wiz-report-label">${t("consolidation.report_duplicates_removed")}</span>
        </div>
        <div class="wiz-report-card">
          <span class="wiz-report-value">${formatBytes(report.space_freed || 0)}</span>
          <span class="wiz-report-label">${t("consolidation.report_space_freed")}</span>
        </div>
        <div class="wiz-report-card">
          <span class="wiz-report-value">${(report.archives_extracted || 0).toLocaleString("cs-CZ")}</span>
          <span class="wiz-report-label">${t("consolidation.report_archives_extracted")}</span>
        </div>
        <div class="wiz-report-card wiz-report-card-warn">
          <span class="wiz-report-value">${(report.transfers_failed || progress.files_failed || 0).toLocaleString("cs-CZ")}</span>
          <span class="wiz-report-label">${t("consolidation.report_transfers_failed")}</span>
        </div>
        <div class="wiz-report-card">
          <span class="wiz-report-value">${(report.files_verified || progress.files_verified || 0).toLocaleString("cs-CZ")}</span>
          <span class="wiz-report-label">${t("consolidation.report_files_verified")}</span>
        </div>
      </div>
    </div>`;
}

// ---------------------------------------------------------------------------
// PHASE C - Organization
// ---------------------------------------------------------------------------

async function renderPhaseC(el, data, activeJob) {
  const progress = activeJob?.progress || {};
  const orgProgress = progress.organize || {};
  const orgPct = orgProgress.total > 0 ? Math.min((orgProgress.done / orgProgress.total) * 100, 100) : 0;

  // Fetch real category data from catalog-stats endpoint
  let catData = { media: 0, documents: 0, software: 0, other: 0 };
  try {
    const stats = await api("consolidation/catalog-stats");
    for (const c of (stats.categories || [])) {
      const key = (c.category || "").toLowerCase();
      if (key in catData) catData[key] = c.count;
    }
  } catch { /* fallback to zeros */ }

  const cats = [
    { key: "media", icon: "\uD83C\uDFA5", label: t("consolidation.org_media"), count: catData.media },
    { key: "documents", icon: "\uD83D\uDCC4", label: t("consolidation.org_documents"), count: catData.documents },
    { key: "software", icon: "\uD83D\uDCBB", label: t("consolidation.org_software"), count: catData.software },
    { key: "other", icon: "\uD83D\uDCE6", label: t("consolidation.org_other"), count: catData.other },
  ];
  const totalFiles = cats.reduce((sum, c) => sum + c.count, 0);

  el.innerHTML = `
    <div class="wiz-phase-card">
      <div class="wiz-phase-header">
        <h3>\uD83D\uDCC1 ${t("consolidation.org_title")}</h3>
        <p class="wiz-phase-desc">${t("consolidation.org_desc")}</p>
      </div>

      <div class="wiz-category-grid">
        ${cats.map(c => {
          const pct = totalFiles > 0 ? ((c.count / totalFiles) * 100).toFixed(0) : 0;
          return `<div class="wiz-category-card">
            <span class="wiz-cat-icon">${c.icon}</span>
            <span class="wiz-cat-label">${c.label}</span>
            <span class="wiz-cat-count">${c.count.toLocaleString("cs-CZ")}</span>
            <div class="wiz-cat-bar"><div class="wiz-cat-bar-fill" style="width:${pct}%"></div></div>
          </div>`;
        }).join("")}
      </div>

      <div class="wiz-progress-item wiz-mt-lg">
        <div class="wiz-progress-item-header">
          <span>${t("consolidation.org_progress")}</span>
          <span>${orgPct.toFixed(0)}%</span>
        </div>
        <div class="wiz-progress-track"><div class="wiz-progress-fill" style="width:${orgPct.toFixed(1)}%"></div></div>
      </div>

      <div class="wiz-nav">
        <button class="wiz-btn-secondary" data-prev>\u2190 ${t("consolidation.phase_b")}</button>
        <button id="btn-wiz-next-d" class="wiz-btn-primary">${t("consolidation.continue")} \u2192</button>
      </div>
    </div>`;

  bindButton("#btn-wiz-next-d", () => advancePhase(3));
  el.querySelector("[data-prev]")?.addEventListener("click", () => { _currentPhase = 1; renderStepIndicator(); reloadPhase(); });
}

// ---------------------------------------------------------------------------
// PHASE D - Manual cleanup
// ---------------------------------------------------------------------------

function renderPhaseD(el) {
  el.innerHTML = `
    <div class="wiz-phase-card">
      <div class="wiz-phase-header">
        <h3>\u270D\uFE0F ${t("consolidation.manual_title")}</h3>
        <p class="wiz-phase-desc">${t("consolidation.manual_desc")}</p>
      </div>

      <div class="wiz-manual-steps">
        <div class="wiz-manual-step">
          <span class="wiz-manual-num">1</span>
          <span>${t("consolidation.manual_step1")}</span>
        </div>
        <div class="wiz-manual-step">
          <span class="wiz-manual-num">2</span>
          <span>${t("consolidation.manual_step2")}</span>
        </div>
        <div class="wiz-manual-step">
          <span class="wiz-manual-num">3</span>
          <span>${t("consolidation.manual_step3")}</span>
        </div>
        <div class="wiz-manual-step">
          <span class="wiz-manual-num">4</span>
          <span>${t("consolidation.manual_step4")}</span>
        </div>
      </div>

      <div class="wiz-nav">
        <button class="wiz-btn-secondary" data-prev>\u2190 ${t("consolidation.phase_c")}</button>
        <button id="btn-wiz-next-e" class="wiz-btn-primary">${t("consolidation.manual_done")} \u2192</button>
      </div>
    </div>`;

  bindButton("#btn-wiz-next-e", () => advancePhase(4));
  el.querySelector("[data-prev]")?.addEventListener("click", () => { _currentPhase = 2; renderStepIndicator(); reloadPhase(); });
}

// ---------------------------------------------------------------------------
// PHASE E - Download to disk
// ---------------------------------------------------------------------------

function renderPhaseE(el, data, activeJob) {
  const progress = activeJob?.progress || {};
  const download = progress.download || {};
  const dlPct = download.total > 0 ? Math.min((download.done / download.total) * 100, 100) : 0;
  const savedConfig = activeJob?.config || {};

  el.innerHTML = `
    <div class="wiz-phase-card">
      <div class="wiz-phase-header">
        <h3>\uD83D\uDCE5 ${t("consolidation.download_title")}</h3>
        <p class="wiz-phase-desc">${t("consolidation.download_desc")}</p>
      </div>

      <div class="wiz-section">
        <label class="wiz-section-label">${t("consolidation.download_select_disk")}</label>
        <input type="text" id="wiz-disk-path" class="wiz-input" value="${escapeHtml(savedConfig.disk_path || "/Volumes/4TB/GML-Library")}" placeholder="/Volumes/4TB/GML-Library">
      </div>

      ${download.done > 0 ? `
        <div class="wiz-progress-item">
          <div class="wiz-progress-item-header">
            <span>${t("consolidation.download_progress")}</span>
            <span>${dlPct.toFixed(0)}%</span>
          </div>
          <div class="wiz-progress-track"><div class="wiz-progress-fill" style="width:${dlPct.toFixed(1)}%"></div></div>
          <div class="wiz-progress-detail">
            <span>${(download.done || 0).toLocaleString("cs-CZ")} / ${(download.total || 0).toLocaleString("cs-CZ")}</span>
            <span>${formatBytes(download.bytes_done || 0)} / ${formatBytes(download.bytes_total || 0)}</span>
            ${download.speed ? `<span>${formatBytes(download.speed)}/s</span>` : ""}
          </div>
        </div>` : ""}

      <div class="wiz-nav">
        <button class="wiz-btn-secondary" data-prev>\u2190 ${t("consolidation.phase_d")}</button>
        <button id="btn-wiz-download" class="wiz-btn-primary">\uD83D\uDCE5 ${t("consolidation.download_start")}</button>
      </div>
    </div>`;

  bindButton("#btn-wiz-download", doSyncToDisk);
  el.querySelector("[data-prev]")?.addEventListener("click", () => { _currentPhase = 3; renderStepIndicator(); reloadPhase(); });
}

// ---------------------------------------------------------------------------
// PHASE F - Ongoing sync
// ---------------------------------------------------------------------------

function renderPhaseF(el, data, activeJob) {
  const progress = activeJob?.progress || {};
  const sync = progress.sync || {};
  const syncPct = sync.total > 0 ? Math.min((sync.done / sync.total) * 100, 100) : 0;
  const lastSync = sync.last_sync || null;

  el.innerHTML = `
    <div class="wiz-phase-card">
      <div class="wiz-phase-header">
        <h3>\uD83D\uDD04 ${t("consolidation.sync_title")}</h3>
        <p class="wiz-phase-desc">${t("consolidation.sync_desc")}</p>
      </div>

      <div class="wiz-sync-info">
        <div class="wiz-sync-info-row">
          <span class="wiz-sync-info-label">${t("consolidation.sync_last")}:</span>
          <span class="wiz-sync-info-value">${lastSync ? new Date(lastSync).toLocaleString("cs-CZ") : t("consolidation.sync_never")}</span>
        </div>
        <p class="wiz-hint">${t("consolidation.sync_reminder_hint")}</p>
      </div>

      ${sync.done > 0 ? `
        <div class="wiz-progress-item">
          <div class="wiz-progress-item-header">
            <span>${t("consolidation.sync_progress")}</span>
            <span>${syncPct.toFixed(0)}%</span>
          </div>
          <div class="wiz-progress-track"><div class="wiz-progress-fill" style="width:${syncPct.toFixed(1)}%"></div></div>
          <div class="wiz-metrics-grid wiz-mt-sm">
            <div class="wiz-metric">
              <span class="wiz-metric-value">${(sync.new_files || 0).toLocaleString("cs-CZ")}</span>
              <span class="wiz-metric-label">${t("consolidation.sync_new_files")}</span>
            </div>
            <div class="wiz-metric">
              <span class="wiz-metric-value">${(sync.deleted || 0).toLocaleString("cs-CZ")}</span>
              <span class="wiz-metric-label">${t("consolidation.sync_deleted")}</span>
            </div>
            <div class="wiz-metric">
              <span class="wiz-metric-value">${(sync.verified || 0).toLocaleString("cs-CZ")}</span>
              <span class="wiz-metric-label">${t("consolidation.sync_verified")}</span>
            </div>
          </div>
        </div>` : ""}

      <div class="wiz-nav">
        <button class="wiz-btn-secondary" data-prev>\u2190 ${t("consolidation.phase_e")}</button>
        <button id="btn-wiz-sync" class="wiz-btn-primary">\uD83D\uDD04 ${t("consolidation.sync_start")}</button>
      </div>
    </div>`;

  bindButton("#btn-wiz-sync", doSync);
  el.querySelector("[data-prev]")?.addEventListener("click", () => { _currentPhase = 4; renderStepIndicator(); reloadPhase(); });
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

function getWizardConfig() {
  const sources = [];
  document.querySelectorAll('#wiz-sources input[name="source"]:checked').forEach(cb => {
    sources.push(cb.value);
  });

  return {
    source_remotes: sources,
    local_roots: _localRoots,
    dest_remote: $("#wiz-dest-remote")?.value || "gws-backup",
    dest_path: $("#wiz-dest-path")?.value || "GML-Consolidated",
    disk_path: $("#wiz-disk-path")?.value || "/Volumes/4TB/GML-Library",
    structure_pattern: "year_month",
    dedup_strategy: "richness",
    verify_pct: 100,
    bwlimit: $("#wiz-bwlimit")?.value || null,
    media_only: $("#wiz-media-only")?.checked ?? false,
    dry_run: $("#wiz-dry-run")?.checked ?? false,
  };
}

async function doStart() {
  if (!confirm(t("consolidation.start_confirm"))) return;

  const btn = $("#btn-wiz-start") || $("#btn-wiz-restart");
  if (btn) {
    btn.disabled = true;
    btn.textContent = `\u23F3 ${t("consolidation.starting")}`;
  }

  try {
    const config = getWizardConfig();
    const result = await apiPost("/consolidation/start", config);
    showToast(`${t("consolidation.started")} (${result.task_id || result.job_id || ""})`, "success");
    _transferActive = true;
    _lastActivity = Date.now();
    startPolling();
    setTimeout(() => reloadPhase(), 500);
  } catch (e) {
    showToast(t("general.error", { message: e.message }), "error");
    if (btn) {
      btn.disabled = false;
      btn.textContent = `\uD83D\uDE80 ${t("consolidation.start")}`;
    }
  }
}

async function doPause() {
  const btn = $("#btn-wiz-pause");
  if (btn) btn.disabled = true;

  try {
    const result = await apiPost("/consolidation/pause");
    if (result.paused) {
      showToast(t("consolidation.paused"), "success");
    } else {
      showToast(result.note || t("consolidation.pause_failed"), "warning");
    }
    _transferActive = false;
    await reloadPhase();
  } catch (e) {
    showToast(t("general.error", { message: e.message }), "error");
    if (btn) btn.disabled = false;
  }
}

async function doResume() {
  const btn = $("#btn-wiz-resume");
  if (btn) {
    btn.disabled = true;
    btn.textContent = `\u23F3 ${t("consolidation.resuming")}`;
  }

  try {
    const result = await apiPost("/consolidation/resume");
    showToast(`${t("consolidation.resumed")} (${result.task_id || ""})`, "success");
    _transferActive = true;
    _lastActivity = Date.now();
    startPolling();
    await reloadPhase();
  } catch (e) {
    showToast(t("general.error", { message: e.message }), "error");
    if (btn) {
      btn.disabled = false;
      btn.textContent = `\u25B6\uFE0F ${t("consolidation.resume")}`;
    }
  }
}

async function doSyncToDisk() {
  const btn = $("#btn-wiz-download");
  if (!btn) return;
  btn.disabled = true;
  btn.textContent = `\u23F3 ${t("consolidation.starting")}`;

  try {
    const diskPath = $("#wiz-disk-path")?.value || "/Volumes/4TB/GML-Library";
    const result = await apiPost("/consolidation/sync-to-disk", { disk_path: diskPath });
    showToast(t("consolidation.started"), "success");
    startPolling();
    await reloadPhase();
  } catch (e) {
    showToast(t("general.error", { message: e.message }), "error");
    btn.disabled = false;
    btn.textContent = `\uD83D\uDCE5 ${t("consolidation.download_start")}`;
  }
}

async function doSync() {
  const btn = $("#btn-wiz-sync");
  if (!btn) return;
  btn.disabled = true;
  btn.textContent = `\u23F3 ${t("consolidation.starting")}`;

  try {
    const diskPath = $("#wiz-disk-path")?.value || "/Volumes/4TB/GML-Library";
    const result = await apiPost("/consolidation/sync-to-disk", { disk_path: diskPath });
    showToast(t("consolidation.started"), "success");
    startPolling();
    await reloadPhase();
  } catch (e) {
    showToast(t("general.error", { message: e.message }), "error");
    btn.disabled = false;
    btn.textContent = `\uD83D\uDD04 ${t("consolidation.sync_start")}`;
  }
}

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

function startPolling() {
  stopPolling();
  _pollFailCount = 0;
  _lastPollSuccess = Date.now();
  _pollTimer = setInterval(pollStatus, 2000);
}

function stopPolling() {
  if (_pollTimer) {
    clearInterval(_pollTimer);
    _pollTimer = null;
  }
}

async function pollStatus() {
  try {
    const fetchOpts = _pollFailCount >= 10 ? { timeout: 10000 } : {};
    const data = await api("/consolidation/status", fetchOpts);
    const active = _pickActiveJob(data.jobs || []);

    // Issue #8: successful poll — reset failure counter and hide warning
    _pollFailCount = 0;
    _lastPollSuccess = Date.now();
    _hideDisconnectWarning();

    if (!active) {
      stopPolling();
      _transferActive = false;
      // Auto-advance if completed
      const completed = (data.jobs || []).find(j => j.status === "completed");
      if (completed) {
        const phase = completed.progress?.phase || "";
        const completedPhaseIdx = API_PHASE_MAP[phase];
        if (completedPhaseIdx !== undefined && completedPhaseIdx >= _currentPhase) {
          _currentPhase = Math.min(completedPhaseIdx + 1, WIZARD_PHASES.length - 1);
          renderStepIndicator();
        }
      }
    } else {
      // Auto-detect phase from status
      const phase = active.progress?.phase || "";
      if (API_PHASE_MAP[phase] !== undefined) {
        const detectedPhase = API_PHASE_MAP[phase];
        if (detectedPhase > _currentPhase) {
          _currentPhase = detectedPhase;
          renderStepIndicator();
        }
      }
    }

    renderPhaseContent(data, active);
  } catch {
    // Issue #8: track consecutive failures
    _pollFailCount++;
    if (_pollFailCount >= 3) {
      _showDisconnectWarning();
    }
    if (_pollFailCount >= 10) {
      // Try a recovery reload with longer timeout on next tick
      // (already handled above via fetchOpts)
    }
  }
}

// Issue #8: disconnect warning bar
function _showDisconnectWarning() {
  if (document.getElementById("wiz-disconnect-bar")) return;
  const bar = document.createElement("div");
  bar.id = "wiz-disconnect-bar";
  bar.className = "wiz-disconnect-bar";
  bar.textContent = "\u26A0\uFE0F Odpojeno \u2014 obnovuji spojen\u00ED...";
  const page = _container?.querySelector(".consolidation-page");
  if (page) {
    page.insertBefore(bar, page.querySelector(".wizard-content") || page.firstChild);
  }
}

function _hideDisconnectWarning() {
  const bar = document.getElementById("wiz-disconnect-bar");
  if (bar) bar.remove();
}

// ---------------------------------------------------------------------------
// Phase navigation
// ---------------------------------------------------------------------------

function advancePhase(targetPhase) {
  _currentPhase = targetPhase;
  renderStepIndicator();
  reloadPhase();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatEta(seconds) {
  if (seconds <= 0) return "\u2014";
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m`;
  return `${seconds}s`;
}

// Issue #9: format "Xs" / "Xmin" ago from a timestamp
function _formatSecondsAgo(ts) {
  const sec = Math.max(0, Math.round((Date.now() - ts) / 1000));
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  return `${min}min`;
}

function escapeHtml(str) {
  if (typeof str !== "string") return String(str ?? "");
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function bindButton(sel, handler) {
  const btn = $(sel);
  if (btn) btn.addEventListener("click", handler);
}
