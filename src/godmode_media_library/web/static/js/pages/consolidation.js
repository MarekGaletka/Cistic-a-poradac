/* GOD MODE Media Library — Consolidation Wizard */

import { t } from "../i18n.js";
import { $, showToast, formatBytes } from "../utils.js";
import { api, apiPost } from "../api.js";

let _container = null;
let _pollTimer = null;
let _currentPhase = 0; // 0=A, 1=B, 2=C, 3=D, 4=E, 5=F
let _transferActive = false;
let _lastActivity = 0;

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
    const active = (data.jobs || []).find(
      j => j.status === "running" || j.status === "paused" || j.status === "created"
    );

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
    const active = (data.jobs || []).find(
      j => j.status === "running" || j.status === "paused" || j.status === "created"
    );
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
  const isRunning = activeJob && (activeJob.status === "running" || activeJob.status === "paused");
  const progress = activeJob?.progress || {};
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
        <textarea id="wiz-local-roots" class="wiz-textarea" rows="4"
          placeholder="${t("consolidation.local_roots_placeholder")}">${escapeHtml((savedConfig.local_roots || []).join("\n"))}</textarea>
        <div class="wiz-hint-row">
          <span class="wiz-hint">${t("consolidation.local_roots_hint")}</span>
          <button type="button" id="btn-wiz-common" class="wiz-hint-btn">${t("consolidation.common_paths")}</button>
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
          <label class="wiz-toggle" style="margin-top:8px">
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

  // Bind common paths
  bindButton("#btn-wiz-common", () => {
    const ta = $("#wiz-local-roots");
    if (!ta) return;
    const paths = [
      "/Users/$USER/Pictures",
      "/Users/$USER/Downloads",
      "/Users/$USER/Desktop",
      "/Volumes/iPhone/DCIM",
      "/Volumes/4TB/Photos",
    ].join("\n");
    showToast(`${t("consolidation.common_paths")}:\n${paths}`, "info", 8000);
  });

  // Bind start
  bindButton("#btn-wiz-start", doStart);
}

function renderPhaseATransfer(el, activeJob, progress, isPaused) {
  const transferred = progress.files_transferred || 0;
  const failed = progress.files_failed || 0;
  const speed = progress.transfer_speed_bps || 0;
  const eta = progress.eta_seconds || 0;
  const currentFile = progress.current_file || "";
  const bytesTransferred = progress.bytes_transferred || 0;
  const bytesTotal = progress.bytes_total_estimate || 0;
  const pct = bytesTotal > 0 ? Math.min((bytesTransferred / bytesTotal) * 100, 100) : 0;

  // Watchdog check
  const now = Date.now();
  if (speed > 0 || transferred > 0) _lastActivity = now;
  const stalled = !isPaused && _transferActive && _lastActivity > 0 && (now - _lastActivity > 60000);

  // Google limit detection
  const googleLimit = progress.google_limit_reached || false;

  const statusLabel = isPaused ? t("consolidation.paused") : t("consolidation.running");
  const statusColor = isPaused ? "var(--color-warning)" : "var(--color-success)";

  // Per-remote scan progress
  const scanProgress = progress.scan_progress || {};
  const scanRemotes = Object.keys(scanProgress);

  el.innerHTML = `
    <div class="wiz-phase-card">
      <div class="wiz-phase-header">
        <h3>\u2601\uFE0F ${t("consolidation.transfer_title")}</h3>
        <p class="wiz-phase-desc">${t("consolidation.transfer_desc")}</p>
      </div>

      <div class="wiz-status-badge" style="border-color:${statusColor}">
        <span class="wiz-badge" style="background:${statusColor}">${statusLabel}</span>
        <span class="wiz-job-id">${escapeHtml((activeJob.job_id || "").slice(0, 8))}</span>
      </div>

      ${stalled ? `<div class="wiz-watchdog-bar">\u26A0\uFE0F ${t("consolidation.watchdog_warning")}</div>` : ""}
      ${googleLimit ? `<div class="wiz-warning-bar">\uD83D\uDEAB ${t("consolidation.google_limit_warning")}</div>` : ""}

      ${scanRemotes.length > 0 ? `
        <div class="wiz-scan-section">
          <label class="wiz-section-label">${t("consolidation.transfer_scanning")}</label>
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
        <div class="wiz-progress-main">
          <div class="wiz-progress-track wiz-progress-track-lg"><div class="wiz-progress-fill" style="width:${pct.toFixed(1)}%"></div></div>
          <span class="wiz-progress-pct">${pct.toFixed(1)}%</span>
        </div>
        <div class="wiz-metrics-grid">
          <div class="wiz-metric">
            <span class="wiz-metric-value">${transferred.toLocaleString("cs-CZ")}</span>
            <span class="wiz-metric-label">${t("consolidation.transfer_files")}</span>
          </div>
          <div class="wiz-metric">
            <span class="wiz-metric-value">${formatBytes(bytesTransferred)}</span>
            <span class="wiz-metric-label">${t("consolidation.transfer_bytes")}</span>
          </div>
          <div class="wiz-metric">
            <span class="wiz-metric-value">${speed > 0 ? formatBytes(speed) + "/s" : "\u2014"}</span>
            <span class="wiz-metric-label">${t("consolidation.transfer_speed")}</span>
          </div>
          <div class="wiz-metric">
            <span class="wiz-metric-value">${eta > 0 ? formatEta(eta) : "\u2014"}</span>
            <span class="wiz-metric-label">${t("consolidation.transfer_eta")}</span>
          </div>
          <div class="wiz-metric wiz-metric-warn">
            <span class="wiz-metric-value">${failed}</span>
            <span class="wiz-metric-label">${t("consolidation.files_failed")}</span>
          </div>
        </div>
        ${currentFile ? `<div class="wiz-current-file"><span>${t("consolidation.transfer_current")}:</span> <code>${escapeHtml(currentFile)}</code></div>` : ""}
        ${progress.error ? `<div class="wiz-error-msg">\u274C ${escapeHtml(progress.error)}</div>` : ""}
      </div>

      <div class="wiz-nav">
        <div></div>
        <div class="wiz-btn-group">
          <button id="btn-wiz-pause" class="wiz-btn-warning" ${isPaused ? "disabled" : ""}>\u23F8\uFE0F ${t("consolidation.pause")}</button>
          <button id="btn-wiz-resume" class="wiz-btn-secondary" ${!isPaused ? "disabled" : ""}>\u25B6\uFE0F ${t("consolidation.resume")}</button>
        </div>
      </div>
    </div>`;

  bindButton("#btn-wiz-pause", doPause);
  bindButton("#btn-wiz-resume", doResume);
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

      <div class="wiz-progress-item" style="margin-top:24px">
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
          <div class="wiz-metrics-grid" style="margin-top:12px">
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
    local_roots: ($("#wiz-local-roots")?.value || "").split("\n").map(s => s.trim()).filter(Boolean),
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

  const btn = $("#btn-wiz-start");
  if (!btn) return;
  btn.disabled = true;
  btn.textContent = `\u23F3 ${t("consolidation.starting")}`;

  try {
    const config = getWizardConfig();
    const result = await apiPost("/consolidation/start", config);
    showToast(`${t("consolidation.started")} (${result.task_id || result.job_id || ""})`, "success");
    _transferActive = true;
    _lastActivity = Date.now();
    startPolling();
    await reloadPhase();
  } catch (e) {
    showToast(t("general.error", { message: e.message }), "error");
    btn.disabled = false;
    btn.textContent = `\uD83D\uDE80 ${t("consolidation.start")}`;
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
    const data = await api("/consolidation/status");
    const active = (data.jobs || []).find(
      j => j.status === "running" || j.status === "paused" || j.status === "created"
    );

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
    // Silent fail during poll
  }
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

function escapeHtml(str) {
  if (typeof str !== "string") return String(str ?? "");
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function bindButton(sel, handler) {
  const btn = $(sel);
  if (btn) btn.addEventListener("click", handler);
}
