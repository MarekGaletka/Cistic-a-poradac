/* GOD MODE Media Library — Reorganization wizard */

import { api, apiPost } from "../api.js";
import { $, escapeHtml, formatBytes, showToast } from "../utils.js";
import { t } from "../i18n.js";
import { openFolderPicker } from "../folder-picker.js";
import { pollTask } from "../tasks.js";

let _step = 0;
let _sources = [];
let _detectedSources = [];
let _destination = "";
let _config = {
  structure_pattern: "year_month",
  deduplicate: true,
  merge_metadata: true,
  delete_originals: false,
  workers: 4,
  exclude_patterns: [],
};
let _planResult = null;
let _planId = null;
let _container = null;

const STEPS = ["sources", "destination", "configure", "preview", "execute"];

export async function render(container) {
  _container = container;
  _step = 0;
  _planResult = null;
  _planId = null;

  container.innerHTML = `<div class="loading"><div class="spinner"></div>${t("general.loading")}</div>`;

  // Load detected sources
  try {
    const data = await api("/reorganize/sources");
    _detectedSources = data.sources || [];
  } catch (e) {
    _detectedSources = [];
    showToast(t("general.error", { message: e.message }), "error");
  }

  _renderWizard();
}

function _renderWizard() {
  if (!_container) return;

  // Stepper
  let stepperHtml = '<div class="reorg-stepper">';
  for (let i = 0; i < STEPS.length; i++) {
    const key = `reorg.step_${STEPS[i]}`;
    const cls = i === _step ? "active" : i < _step ? "done" : "";
    const num = i < _step ? "\u2713" : String(i + 1);
    stepperHtml += `<div class="reorg-step ${cls}"><span class="reorg-step-num">${num}</span><span class="reorg-step-label">${t(key)}</span></div>`;
    if (i < STEPS.length - 1) stepperHtml += '<div class="reorg-step-line"></div>';
  }
  stepperHtml += "</div>";

  let contentHtml = "";
  switch (_step) {
    case 0: contentHtml = _renderSourcesStep(); break;
    case 1: contentHtml = _renderDestinationStep(); break;
    case 2: contentHtml = _renderConfigureStep(); break;
    case 3: contentHtml = _renderPreviewStep(); break;
    case 4: contentHtml = _renderExecuteStep(); break;
  }

  _container.innerHTML = `
    <div class="reorg-page">
      <div class="reorg-header">
        <h2 class="reorg-title">${t("reorg.title")}</h2>
        <p class="reorg-subtitle">${t("reorg.subtitle")}</p>
      </div>
      ${stepperHtml}
      <div class="reorg-content">${contentHtml}</div>
      <div class="reorg-nav">
        ${_step > 0 ? `<button class="reorg-btn-back" id="reorg-back">${t("reorg.back")}</button>` : '<div></div>'}
        ${_step < 4 ? `<button class="reorg-btn-next primary" id="reorg-next" ${_canAdvance() ? "" : "disabled"}>${_step === 3 && !_planResult ? t("reorg.start_scan") : t("reorg.next")}</button>` : ""}
      </div>
    </div>`;

  _bindEvents();
}

function _canAdvance() {
  switch (_step) {
    case 0: return _sources.length > 0;
    case 1: return _destination.length > 0;
    case 2: return true;
    case 3: return _planResult !== null;
    default: return false;
  }
}

function _bindEvents() {
  const backBtn = _container.querySelector("#reorg-back");
  const nextBtn = _container.querySelector("#reorg-next");
  if (backBtn) backBtn.addEventListener("click", () => { _step--; _renderWizard(); });
  if (nextBtn) nextBtn.addEventListener("click", _onNext);

  // Step-specific bindings
  switch (_step) {
    case 0: _bindSourcesStep(); break;
    case 1: _bindDestinationStep(); break;
    case 2: _bindConfigureStep(); break;
    case 3: _bindPreviewStep(); break;
    case 4: _bindExecuteStep(); break;
  }
}

async function _onNext() {
  if (_step === 3 && !_planResult) {
    // Start scan
    await _startPlan();
    return;
  }
  if (_canAdvance()) {
    _step++;
    _renderWizard();
  }
}

// ── Step 1: Sources ──────────────────────────────────

function _renderSourcesStep() {
  let html = `<p class="reorg-step-hint">${t("reorg.select_sources_hint")}</p>`;

  // Auto-detected sources
  if (_detectedSources.length > 0) {
    html += `<h4 class="reorg-section-label">${t("reorg.auto_detected")}</h4>`;
    html += '<div class="reorg-source-grid">';
    for (const src of _detectedSources) {
      const isSelected = _sources.includes(src.path);
      const typeLabel = {mac: t("reorg.source_mac"), external: t("reorg.source_external"), iphone: t("reorg.source_iphone"), icloud: t("reorg.source_icloud")}[src.type] || src.type;
      html += `<button class="reorg-source-card ${isSelected ? "selected" : ""} ${!src.available ? "unavailable" : ""}" data-source-path="${escapeHtml(src.path)}">
        <span class="reorg-source-icon">${src.icon}</span>
        <div class="reorg-source-info">
          <span class="reorg-source-name">${escapeHtml(src.name)}</span>
          <span class="reorg-source-path">${escapeHtml(src.path)}</span>
          <span class="reorg-source-type">${typeLabel}${src.file_count > 0 ? ` \u00B7 ~${src.file_count} ${t("reorganize.files_approx")}` : ""}</span>
        </div>
        <span class="reorg-source-check">${isSelected ? "\u2713" : ""}</span>
      </button>`;
    }
    html += "</div>";
  }

  // Custom folder button
  html += `<button class="reorg-add-source" id="reorg-add-custom">\uD83D\uDCC1 ${t("reorg.add_custom")}</button>`;

  // Selected sources summary
  if (_sources.length > 0) {
    html += `<h4 class="reorg-section-label">${t("reorg.selected_sources", { count: _sources.length })}</h4>`;
    html += '<div class="reorg-selected-list">';
    for (const s of _sources) {
      const name = s.split("/").pop() || s;
      html += `<div class="reorg-selected-item"><span>\uD83D\uDCC1 ${escapeHtml(name)}</span><span class="reorg-selected-path">${escapeHtml(s)}</span><button class="reorg-remove-source" data-path="${escapeHtml(s)}">\u00D7</button></div>`;
    }
    html += "</div>";
  }

  return html;
}

function _bindSourcesStep() {
  _container.querySelectorAll(".reorg-source-card").forEach(card => {
    if (card.classList.contains("unavailable")) return;
    card.addEventListener("click", () => {
      const path = card.dataset.sourcePath;
      if (_sources.includes(path)) {
        _sources = _sources.filter(s => s !== path);
      } else {
        _sources.push(path);
      }
      _renderWizard();
    });
  });

  const addBtn = _container.querySelector("#reorg-add-custom");
  if (addBtn) {
    addBtn.addEventListener("click", () => {
      openFolderPicker((paths) => {
        _sources = [...new Set([..._sources, ...paths])];
        _renderWizard();
      }, _sources);
    });
  }

  _container.querySelectorAll(".reorg-remove-source").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      _sources = _sources.filter(s => s !== btn.dataset.path);
      _renderWizard();
    });
  });
}

// ── Step 2: Destination ──────────────────────────────

function _renderDestinationStep() {
  let html = `<p class="reorg-step-hint">${t("reorg.select_destination_hint")}</p>`;

  html += `<button class="reorg-add-source" id="reorg-pick-dest">\uD83D\uDCC1 ${t("reorg.select_destination")}</button>`;

  if (_destination) {
    const name = _destination.split("/").pop() || _destination;
    html += `<div class="reorg-dest-preview">
      <span class="reorg-dest-icon">\uD83D\uDCBE</span>
      <div class="reorg-dest-info">
        <span class="reorg-dest-name">${escapeHtml(name)}</span>
        <span class="reorg-dest-path">${escapeHtml(_destination)}</span>
      </div>
    </div>`;
  } else {
    html += `<p class="reorg-empty">${t("reorg.no_destination")}</p>`;
  }

  return html;
}

function _bindDestinationStep() {
  const pickBtn = _container.querySelector("#reorg-pick-dest");
  if (pickBtn) {
    pickBtn.addEventListener("click", () => {
      openFolderPicker((paths) => {
        if (paths.length > 0) {
          _destination = paths[0];
          _renderWizard();
        }
      }, _destination ? [_destination] : []);
    });
  }
}

// ── Step 3: Configure ────────────────────────────────

function _renderConfigureStep() {
  const patterns = [
    { value: "year_month", label: t("reorg.pattern_year_month"), example: t("reorg.pattern_year_month_example") },
    { value: "year_type", label: t("reorg.pattern_year_type"), example: t("reorg.pattern_year_type_example") },
    { value: "year_month_day", label: t("reorg.pattern_year_month_day"), example: t("reorg.pattern_year_month_day_example") },
    { value: "type_year", label: t("reorg.pattern_type_year"), example: t("reorg.pattern_type_year_example") },
    { value: "flat", label: t("reorg.pattern_flat"), example: t("reorg.pattern_flat_example") },
  ];

  let patternHtml = "";
  for (const p of patterns) {
    const active = _config.structure_pattern === p.value ? "active" : "";
    patternHtml += `<button class="reorg-pattern-card ${active}" data-pattern="${p.value}">
      <span class="reorg-pattern-label">${p.label}</span>
      <code class="reorg-pattern-example">${escapeHtml(p.example)}</code>
    </button>`;
  }

  return `
    <div class="reorg-config-section">
      <h4 class="reorg-section-label">${t("reorg.structure_pattern")}</h4>
      <div class="reorg-pattern-grid">${patternHtml}</div>
    </div>
    <div class="reorg-config-section">
      <label class="reorg-toggle">
        <input type="checkbox" id="reorg-dedup" ${_config.deduplicate ? "checked" : ""}>
        <span class="dedup-toggle-switch"></span>
        <div>
          <span class="reorg-toggle-label">${t("reorg.deduplicate")}</span>
          <span class="reorg-toggle-hint">${t("reorg.deduplicate_hint")}</span>
        </div>
      </label>
      <label class="reorg-toggle">
        <input type="checkbox" id="reorg-merge" ${_config.merge_metadata ? "checked" : ""}>
        <span class="dedup-toggle-switch"></span>
        <div>
          <span class="reorg-toggle-label">${t("reorg.merge_metadata")}</span>
          <span class="reorg-toggle-hint">${t("reorg.merge_metadata_hint")}</span>
        </div>
      </label>
      <label class="reorg-toggle reorg-toggle-danger">
        <input type="checkbox" id="reorg-delete" ${_config.delete_originals ? "checked" : ""}>
        <span class="dedup-toggle-switch"></span>
        <div>
          <span class="reorg-toggle-label">${t("reorg.delete_originals")}</span>
          <span class="reorg-toggle-hint reorg-danger-text">${t("reorg.delete_originals_warning")}</span>
        </div>
      </label>
    </div>`;
}

function _bindConfigureStep() {
  _container.querySelectorAll(".reorg-pattern-card").forEach(card => {
    card.addEventListener("click", () => {
      _config.structure_pattern = card.dataset.pattern;
      _container.querySelectorAll(".reorg-pattern-card").forEach(c => c.classList.remove("active"));
      card.classList.add("active");
    });
  });

  const dedupEl = _container.querySelector("#reorg-dedup");
  const mergeEl = _container.querySelector("#reorg-merge");
  const deleteEl = _container.querySelector("#reorg-delete");
  if (dedupEl) dedupEl.addEventListener("change", () => { _config.deduplicate = dedupEl.checked; });
  if (mergeEl) mergeEl.addEventListener("change", () => { _config.merge_metadata = mergeEl.checked; });
  if (deleteEl) deleteEl.addEventListener("change", () => { _config.delete_originals = deleteEl.checked; });
}

// ── Step 4: Preview / Scan ───────────────────────────

function _renderPreviewStep() {
  if (!_planResult) {
    return `<div class="reorg-scan-prompt">
      <p class="reorg-step-hint">${t("reorg.scanning")}</p>
      <div class="loading"><div class="spinner"></div></div>
      <div id="reorg-scan-status"></div>
    </div>`;
  }

  const r = _planResult;
  return `
    <div class="reorg-results">
      <div class="reorg-stat-grid">
        <div class="reorg-stat-card">
          <span class="reorg-stat-value">${r.total_files.toLocaleString("cs-CZ")}</span>
          <span class="reorg-stat-label">${t("reorg.files_found")}</span>
        </div>
        <div class="reorg-stat-card">
          <span class="reorg-stat-value">${r.unique_files.toLocaleString("cs-CZ")}</span>
          <span class="reorg-stat-label">${t("reorg.unique_files")}</span>
        </div>
        <div class="reorg-stat-card reorg-stat-accent">
          <span class="reorg-stat-value">${r.duplicate_files.toLocaleString("cs-CZ")}</span>
          <span class="reorg-stat-label">${t("reorg.duplicates_found")}</span>
        </div>
        <div class="reorg-stat-card reorg-stat-green">
          <span class="reorg-stat-value">${formatBytes(r.duplicate_size)}</span>
          <span class="reorg-stat-label">${t("reorg.space_saved")}</span>
        </div>
      </div>

      ${_renderCategoryBreakdown(r)}
      ${_renderSourceBreakdown(r)}

      ${r.errors && r.errors.length > 0 ? `<div class="reorg-errors"><h4>${t("reorg.errors_title")} (${r.errors.length})</h4><ul>${r.errors.slice(0, 10).map(e => `<li>${escapeHtml(e)}</li>`).join("")}</ul></div>` : ""}
    </div>`;
}

function _renderCategoryBreakdown(r) {
  if (!r.categories || Object.keys(r.categories).length === 0) return "";
  const catLabels = { images: t("reorg.cat_images"), videos: t("reorg.cat_videos"), audio: t("reorg.cat_audio"), documents: t("reorg.cat_documents"), other: t("reorg.cat_other") };
  let html = `<h4 class="reorg-section-label">${t("reorg.by_category")}</h4><div class="reorg-breakdown">`;
  for (const [cat, count] of Object.entries(r.categories)) {
    html += `<div class="reorg-breakdown-row"><span>${catLabels[cat] || cat}</span><span class="reorg-breakdown-val">${count.toLocaleString("cs-CZ")}</span></div>`;
  }
  html += "</div>";
  return html;
}

function _renderSourceBreakdown(r) {
  if (!r.source_stats || Object.keys(r.source_stats).length === 0) return "";
  let html = `<h4 class="reorg-section-label">${t("reorg.by_source")}</h4><div class="reorg-breakdown">`;
  for (const [src, stats] of Object.entries(r.source_stats)) {
    const name = src.split("/").pop() || src;
    html += `<div class="reorg-breakdown-row">
      <span>\uD83D\uDCC1 ${escapeHtml(name)}</span>
      <span class="reorg-breakdown-val">${stats.files.toLocaleString("cs-CZ")} ${t("reorganize.files_approx")} \u00B7 ${formatBytes(stats.size)}${stats.duplicates > 0 ? ` \u00B7 ${stats.duplicates} duplicit` : ""}</span>
    </div>`;
  }
  html += "</div>";
  return html;
}

function _bindPreviewStep() {
  // Nothing special needed — next button handles execution
}

async function _startPlan() {
  const nextBtn = _container.querySelector("#reorg-next");
  if (nextBtn) { nextBtn.disabled = true; nextBtn.textContent = t("reorg.scanning"); }

  _planResult = null;
  _renderWizard();

  try {
    const data = await apiPost("/reorganize/plan", {
      sources: _sources,
      destination: _destination,
      structure_pattern: _config.structure_pattern,
      deduplicate: _config.deduplicate,
      merge_metadata: _config.merge_metadata,
      delete_originals: _config.delete_originals,
      workers: _config.workers,
      exclude_patterns: _config.exclude_patterns,
    });

    // Poll for completion
    const taskId = data.task_id;
    _pollPlan(taskId);
  } catch (e) {
    showToast(t("reorg.plan_error", { message: e.message }), "error");
    _step = 2;
    _renderWizard();
  }
}

async function _pollPlan(taskId) {
  const check = async () => {
    try {
      const task = await api(`/tasks/${taskId}`);
      if (task.status === "completed" && task.result) {
        _planResult = task.result;
        _planId = task.result.plan_id || taskId;
        _renderWizard();
      } else if (task.status === "failed") {
        showToast(t("reorg.plan_error", { message: task.error || "Unknown" }), "error");
        _step = 2;
        _renderWizard();
      } else {
        // Update progress display
        const statusEl = _container?.querySelector("#reorg-scan-status");
        if (statusEl && task.progress) {
          const p = task.progress;
          statusEl.textContent = p.phase ? `${p.phase}: ${p.current || 0}/${p.total || "?"}` : "";
        }
        setTimeout(check, 1000);
      }
    } catch {
      setTimeout(check, 2000);
    }
  };
  check();
}

// ── Step 5: Execute ──────────────────────────────────

function _renderExecuteStep() {
  return `
    <div class="reorg-execute-panel">
      <div class="reorg-execute-summary">
        <h3>${t("reorg.execute")}</h3>
        <p>${_planResult ? t("reorg.complete_stats", {
          copied: _planResult.unique_files.toLocaleString("cs-CZ"),
          skipped: _planResult.duplicate_files.toLocaleString("cs-CZ"),
          saved: formatBytes(_planResult.duplicate_size),
        }) : ""}</p>
        <p>\uD83D\uDCC1 ${escapeHtml(_destination)}</p>
      </div>

      ${_config.delete_originals ? `
        <label class="reorg-confirm-delete">
          <input type="checkbox" id="reorg-confirm-del">
          <span class="reorg-danger-text">${t("reorg.confirm_delete")}</span>
        </label>` : ""}

      <button class="primary reorg-execute-btn" id="reorg-execute-btn" ${_config.delete_originals ? "disabled" : ""}>
        \uD83D\uDE80 ${t("reorg.execute")}
      </button>

      <div id="reorg-exec-status" class="reorg-exec-status"></div>
    </div>`;
}

function _bindExecuteStep() {
  const confirmEl = _container.querySelector("#reorg-confirm-del");
  const execBtn = _container.querySelector("#reorg-execute-btn");

  if (confirmEl && execBtn) {
    confirmEl.addEventListener("change", () => {
      execBtn.disabled = !confirmEl.checked;
    });
  }

  if (execBtn) {
    execBtn.addEventListener("click", async () => {
      execBtn.disabled = true;
      execBtn.textContent = t("reorg.executing");

      try {
        const data = await apiPost("/reorganize/execute", {
          plan_id: _planId,
          delete_originals: _config.delete_originals,
        });
        _pollExecute(data.task_id);
      } catch (e) {
        showToast(t("reorg.exec_error", { message: e.message }), "error");
        execBtn.disabled = false;
        execBtn.textContent = t("reorg.execute");
      }
    });
  }
}

async function _pollExecute(taskId) {
  const statusEl = _container?.querySelector("#reorg-exec-status");

  const check = async () => {
    try {
      const task = await api(`/tasks/${taskId}`);
      if (task.status === "completed" && task.result) {
        const r = task.result;
        if (statusEl) {
          statusEl.innerHTML = `<div class="reorg-complete-box">
            <span class="reorg-complete-icon">\u2705</span>
            <h3>${t("reorg.complete")}</h3>
            <p>${t("reorg.complete_stats", {
              copied: r.files_copied.toLocaleString("cs-CZ"),
              skipped: r.files_skipped.toLocaleString("cs-CZ"),
              saved: formatBytes(r.space_saved),
            })}</p>
          </div>`;
        }
        showToast(t("reorg.complete"), "success");
      } else if (task.status === "failed") {
        showToast(t("reorg.exec_error", { message: task.error || "Unknown" }), "error");
        if (statusEl) statusEl.textContent = task.error || "Error";
      } else {
        if (statusEl && task.progress) {
          const p = task.progress;
          statusEl.textContent = `${p.phase || ""}: ${p.current || 0}/${p.total || "?"} ${p.current_file ? "— " + p.current_file.split("/").pop() : ""}`;
        }
        setTimeout(check, 1000);
      }
    } catch {
      setTimeout(check, 2000);
    }
  };
  check();
}
