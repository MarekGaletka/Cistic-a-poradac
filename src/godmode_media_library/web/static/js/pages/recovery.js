/* GOD MODE Media Library — Recovery page */

import { t } from "../i18n.js";
import { api, apiPost } from "../api.js";
import { showToast } from "../utils.js";

let _activeTab = "quarantine";
let _container = null;

// State
let _quarantineEntries = [];
let _deepScanResult = null;
let _integrityResult = null;
let _photorec = { available: false };
let _selectedQuarantine = new Set();
let _selectedDeepScan = new Set();

export async function render(container) {
  _container = container;
  container.innerHTML = `
    <div class="recovery-page">
      <div class="recovery-header">
        <h2>${t("recovery.title")}</h2>
        <p class="recovery-subtitle">${t("recovery.subtitle")}</p>
      </div>
      <div class="recovery-tabs" id="recovery-tabs"></div>
      <div class="recovery-content" id="recovery-content"></div>
    </div>`;

  _renderTabs();
  await _switchTab(_activeTab);
}

const _tabs = [
  { id: "quarantine", icon: "\u{1F5C3}\uFE0F", label: "recovery.tab_quarantine" },
  { id: "deep_scan", icon: "\u{1F50D}", label: "recovery.tab_deep_scan" },
  { id: "integrity", icon: "\u{1F6E1}\uFE0F", label: "recovery.tab_integrity" },
  { id: "photorec", icon: "\u{1F4BE}", label: "recovery.tab_photorec" },
];

function _renderTabs() {
  const tabsEl = document.getElementById("recovery-tabs");
  if (!tabsEl) return;
  tabsEl.innerHTML = _tabs.map(tab =>
    `<button class="recovery-tab${tab.id === _activeTab ? " active" : ""}" data-tab="${tab.id}">
      <span class="recovery-tab-icon">${tab.icon}</span>
      <span class="recovery-tab-label">${t(tab.label)}</span>
    </button>`
  ).join("");

  tabsEl.querySelectorAll(".recovery-tab").forEach(btn => {
    btn.addEventListener("click", () => _switchTab(btn.dataset.tab));
  });
}

async function _switchTab(tabId) {
  _activeTab = tabId;
  _renderTabs();
  const content = document.getElementById("recovery-content");
  if (!content) return;

  content.innerHTML = `<div class="loading"><div class="spinner"></div>${t("general.loading")}</div>`;

  switch (tabId) {
    case "quarantine": await _renderQuarantine(content); break;
    case "deep_scan": _renderDeepScan(content); break;
    case "integrity": _renderIntegrity(content); break;
    case "photorec": await _renderPhotoRec(content); break;
  }
}

// ── Quarantine ───────────────────────────────────────

async function _renderQuarantine(container) {
  try {
    const data = await api("/recovery/quarantine");
    _quarantineEntries = data.entries || [];

    if (_quarantineEntries.length === 0) {
      container.innerHTML = `
        <div class="recovery-empty-state">
          <div class="recovery-empty-icon">\u2705</div>
          <h3>${t("recovery.quarantine_empty")}</h3>
          <p>${t("recovery.quarantine_empty_hint")}</p>
        </div>`;
      return;
    }

    const totalSize = _formatSize(data.total_size);

    container.innerHTML = `
      <div class="recovery-section">
        <div class="recovery-stats-row">
          <div class="recovery-stat-card">
            <span class="recovery-stat-value">${_quarantineEntries.length}</span>
            <span class="recovery-stat-label">${t("recovery.quarantine_files")}</span>
          </div>
          <div class="recovery-stat-card">
            <span class="recovery-stat-value">${totalSize}</span>
            <span class="recovery-stat-label">${t("recovery.quarantine_size")}</span>
          </div>
        </div>
        <div class="recovery-actions-bar">
          <button class="btn-secondary" id="q-select-all">${t("action.select_all")}</button>
          <button class="btn-primary" id="q-restore-selected" disabled>${t("recovery.restore_selected")}</button>
          <button class="btn-danger" id="q-delete-selected" disabled>${t("recovery.delete_selected")}</button>
        </div>
        <div class="recovery-file-list" id="quarantine-list">
          ${_quarantineEntries.map((e, i) => `
            <div class="recovery-file-item" data-idx="${i}">
              <label class="recovery-file-check">
                <input type="checkbox" data-path="${_escHtml(e.path)}" class="q-check">
              </label>
              <div class="recovery-file-icon">${_categoryIcon(e.category)}</div>
              <div class="recovery-file-info">
                <span class="recovery-file-name">${_escHtml(e.path.split("/").pop())}</span>
                <span class="recovery-file-meta">${_escHtml(e.original_path)} \u2022 ${_formatSize(e.size)}</span>
              </div>
              <div class="recovery-file-actions">
                <button class="btn-sm btn-restore" data-path="${_escHtml(e.path)}" title="${t("recovery.restore")}">\u21A9\uFE0F</button>
              </div>
            </div>
          `).join("")}
        </div>
      </div>`;

    _bindQuarantineEvents(container);
  } catch (e) {
    container.innerHTML = `<div class="recovery-error">${t("general.error", { message: e.message })}</div>`;
  }
}

function _bindQuarantineEvents(container) {
  const restoreBtn = container.querySelector("#q-restore-selected");
  const deleteBtn = container.querySelector("#q-delete-selected");
  const selectAll = container.querySelector("#q-select-all");

  function updateButtons() {
    const checked = container.querySelectorAll(".q-check:checked");
    _selectedQuarantine = new Set([...checked].map(c => c.dataset.path));
    const hasSelection = _selectedQuarantine.size > 0;
    if (restoreBtn) restoreBtn.disabled = !hasSelection;
    if (deleteBtn) deleteBtn.disabled = !hasSelection;
  }

  container.querySelectorAll(".q-check").forEach(cb => {
    cb.addEventListener("change", updateButtons);
  });

  if (selectAll) {
    selectAll.addEventListener("click", () => {
      const checks = container.querySelectorAll(".q-check");
      const allChecked = [...checks].every(c => c.checked);
      checks.forEach(c => { c.checked = !allChecked; });
      updateButtons();
    });
  }

  // Single restore buttons
  container.querySelectorAll(".btn-restore").forEach(btn => {
    btn.addEventListener("click", async () => {
      try {
        await apiPost("/recovery/quarantine/restore", { paths: [btn.dataset.path] });
        showToast(t("recovery.restored_ok"), "success");
        await _renderQuarantine(container);
      } catch (e) {
        showToast(t("general.error", { message: e.message }), "error");
      }
    });
  });

  if (restoreBtn) {
    restoreBtn.addEventListener("click", async () => {
      if (_selectedQuarantine.size === 0) return;
      try {
        await apiPost("/recovery/quarantine/restore", { paths: [..._selectedQuarantine] });
        showToast(t("recovery.restored_count", { count: _selectedQuarantine.size }), "success");
        _selectedQuarantine.clear();
        await _renderQuarantine(container);
      } catch (e) {
        showToast(t("general.error", { message: e.message }), "error");
      }
    });
  }

  if (deleteBtn) {
    deleteBtn.addEventListener("click", async () => {
      if (_selectedQuarantine.size === 0) return;
      if (!confirm(t("recovery.delete_confirm", { count: _selectedQuarantine.size }))) return;
      try {
        await apiPost("/recovery/quarantine/delete", { paths: [..._selectedQuarantine] });
        showToast(t("recovery.deleted_count", { count: _selectedQuarantine.size }), "success");
        _selectedQuarantine.clear();
        await _renderQuarantine(container);
      } catch (e) {
        showToast(t("general.error", { message: e.message }), "error");
      }
    });
  }
}

// ── Deep Scan ────────────────────────────────────────

function _renderDeepScan(container) {
  if (_deepScanResult) {
    _renderDeepScanResults(container);
    return;
  }

  container.innerHTML = `
    <div class="recovery-section">
      <div class="recovery-card-grid">
        <div class="recovery-feature-card">
          <div class="recovery-feature-icon">\u{1F5D1}\uFE0F</div>
          <h4>${t("recovery.deep_trash")}</h4>
          <p>${t("recovery.deep_trash_desc")}</p>
        </div>
        <div class="recovery-feature-card">
          <div class="recovery-feature-icon">\u{1F4C2}</div>
          <h4>${t("recovery.deep_cache")}</h4>
          <p>${t("recovery.deep_cache_desc")}</p>
        </div>
        <div class="recovery-feature-card">
          <div class="recovery-feature-icon">\u{1F4F1}</div>
          <h4>${t("recovery.deep_apps")}</h4>
          <p>${t("recovery.deep_apps_desc")}</p>
        </div>
        <div class="recovery-feature-card">
          <div class="recovery-feature-icon">\u2601\uFE0F</div>
          <h4>${t("recovery.deep_cloud")}</h4>
          <p>${t("recovery.deep_cloud_desc")}</p>
        </div>
      </div>
      <div class="recovery-action-center">
        <button class="btn-primary btn-lg" id="btn-deep-scan">
          \u{1F50D} ${t("recovery.start_deep_scan")}
        </button>
        <p class="recovery-hint">${t("recovery.deep_scan_hint")}</p>
      </div>
      <div id="deep-scan-progress" class="recovery-progress hidden"></div>
    </div>`;

  container.querySelector("#btn-deep-scan")?.addEventListener("click", () => _startDeepScan(container));
}

async function _startDeepScan(container) {
  const btn = container.querySelector("#btn-deep-scan");
  const progressEl = container.querySelector("#deep-scan-progress");
  if (btn) { btn.disabled = true; btn.textContent = t("recovery.scanning"); }
  if (progressEl) progressEl.classList.remove("hidden");

  try {
    const { task_id } = await apiPost("/recovery/deep-scan", {});
    await _pollTask(task_id, progressEl, (result) => {
      _deepScanResult = result;
      _renderDeepScanResults(container);
    });
  } catch (e) {
    showToast(t("general.error", { message: e.message }), "error");
    if (btn) { btn.disabled = false; btn.textContent = `\u{1F50D} ${t("recovery.start_deep_scan")}`; }
  }
}

function _renderDeepScanResults(container) {
  const r = _deepScanResult;
  if (!r) return;

  const locationCards = (r.locations || []).map(loc => `
    <div class="recovery-location-card">
      <div class="recovery-location-icon">\u{1F4C1}</div>
      <div class="recovery-location-info">
        <span class="recovery-location-name">${_escHtml(loc.name)}</span>
        <span class="recovery-location-meta">${loc.files_count} ${t("recovery.files_label")} \u2022 ${_formatSize(loc.total_size)}</span>
      </div>
    </div>
  `).join("");

  const fileRows = (r.files || []).slice(0, 200).map((f, i) => `
    <div class="recovery-file-item" data-idx="${i}">
      <label class="recovery-file-check">
        <input type="checkbox" data-path="${_escHtml(f.path)}" class="ds-check">
      </label>
      <div class="recovery-file-icon">${_categoryIcon(f.category)}</div>
      <div class="recovery-file-info">
        <span class="recovery-file-name">${_escHtml(f.name)}</span>
        <span class="recovery-file-meta">${_escHtml(f.location)} \u2022 ${_formatSize(f.size)}</span>
      </div>
    </div>
  `).join("");

  container.innerHTML = `
    <div class="recovery-section">
      <div class="recovery-stats-row">
        <div class="recovery-stat-card accent">
          <span class="recovery-stat-value">${r.files_found}</span>
          <span class="recovery-stat-label">${t("recovery.files_found")}</span>
        </div>
        <div class="recovery-stat-card">
          <span class="recovery-stat-value">${_formatSize(r.total_size)}</span>
          <span class="recovery-stat-label">${t("recovery.total_size")}</span>
        </div>
        <div class="recovery-stat-card">
          <span class="recovery-stat-value">${r.locations_scanned}</span>
          <span class="recovery-stat-label">${t("recovery.locations_scanned")}</span>
        </div>
      </div>

      ${locationCards ? `<h4 class="recovery-section-title">${t("recovery.found_locations")}</h4><div class="recovery-locations-grid">${locationCards}</div>` : ""}

      ${r.files_found > 0 ? `
        <h4 class="recovery-section-title">${t("recovery.found_files")}</h4>
        <div class="recovery-actions-bar">
          <button class="btn-secondary" id="ds-select-all">${t("action.select_all")}</button>
          <button class="btn-primary" id="ds-recover-selected" disabled>\u{1F4E5} ${t("recovery.recover_selected")}</button>
        </div>
        <div class="recovery-file-list">${fileRows}</div>
        ${r.files_found > 200 ? `<p class="recovery-hint">${t("recovery.showing_first", { count: 200, total: r.files_found })}</p>` : ""}
      ` : ""}

      <div class="recovery-action-center" style="margin-top:24px">
        <button class="btn-secondary" id="btn-rescan">\u{1F504} ${t("recovery.rescan")}</button>
      </div>
    </div>`;

  // Bind events
  const selectAll = container.querySelector("#ds-select-all");
  const recoverBtn = container.querySelector("#ds-recover-selected");

  function updateButtons() {
    const checked = container.querySelectorAll(".ds-check:checked");
    _selectedDeepScan = new Set([...checked].map(c => c.dataset.path));
    if (recoverBtn) recoverBtn.disabled = _selectedDeepScan.size === 0;
  }

  container.querySelectorAll(".ds-check").forEach(cb => cb.addEventListener("change", updateButtons));

  if (selectAll) {
    selectAll.addEventListener("click", () => {
      const checks = container.querySelectorAll(".ds-check");
      const allChecked = [...checks].every(c => c.checked);
      checks.forEach(c => { c.checked = !allChecked; });
      updateButtons();
    });
  }

  if (recoverBtn) {
    recoverBtn.addEventListener("click", async () => {
      const dest = prompt(t("recovery.recover_destination"), `${_homeDir()}/Desktop/GML_Recovery`);
      if (!dest) return;
      try {
        const result = await apiPost("/recovery/recover-files", {
          paths: [..._selectedDeepScan],
          destination: dest,
        });
        showToast(t("recovery.recovered_count", { count: result.recovered }), "success");
      } catch (e) {
        showToast(t("general.error", { message: e.message }), "error");
      }
    });
  }

  container.querySelector("#btn-rescan")?.addEventListener("click", () => {
    _deepScanResult = null;
    _renderDeepScan(container);
  });
}

// ── Integrity Check ──────────────────────────────────

function _renderIntegrity(container) {
  if (_integrityResult) {
    _renderIntegrityResults(container);
    return;
  }

  container.innerHTML = `
    <div class="recovery-section">
      <div class="recovery-card-grid">
        <div class="recovery-feature-card">
          <div class="recovery-feature-icon">\u{1F5BC}\uFE0F</div>
          <h4>${t("recovery.check_jpeg")}</h4>
          <p>${t("recovery.check_jpeg_desc")}</p>
        </div>
        <div class="recovery-feature-card">
          <div class="recovery-feature-icon">\u{1F3AC}</div>
          <h4>${t("recovery.check_video")}</h4>
          <p>${t("recovery.check_video_desc")}</p>
        </div>
        <div class="recovery-feature-card">
          <div class="recovery-feature-icon">\u{1F527}</div>
          <h4>${t("recovery.auto_repair")}</h4>
          <p>${t("recovery.auto_repair_desc")}</p>
        </div>
      </div>
      <div class="recovery-action-center">
        <button class="btn-primary btn-lg" id="btn-integrity">
          \u{1F6E1}\uFE0F ${t("recovery.start_integrity")}
        </button>
        <p class="recovery-hint">${t("recovery.integrity_hint")}</p>
      </div>
      <div id="integrity-progress" class="recovery-progress hidden"></div>
    </div>`;

  container.querySelector("#btn-integrity")?.addEventListener("click", () => _startIntegrity(container));
}

async function _startIntegrity(container) {
  const btn = container.querySelector("#btn-integrity");
  const progressEl = container.querySelector("#integrity-progress");
  if (btn) { btn.disabled = true; btn.textContent = t("recovery.checking"); }
  if (progressEl) progressEl.classList.remove("hidden");

  try {
    const { task_id } = await apiPost("/recovery/integrity-check", {});
    await _pollTask(task_id, progressEl, (result) => {
      _integrityResult = result;
      _renderIntegrityResults(container);
    });
  } catch (e) {
    showToast(t("general.error", { message: e.message }), "error");
    if (btn) { btn.disabled = false; btn.textContent = `\u{1F6E1}\uFE0F ${t("recovery.start_integrity")}`; }
  }
}

function _renderIntegrityResults(container) {
  const r = _integrityResult;
  if (!r) return;

  const healthPct = r.total_checked > 0 ? Math.round((r.healthy / r.total_checked) * 100) : 100;

  const errorRows = (r.errors || []).map(e => `
    <div class="recovery-file-item ${e.repairable ? "repairable" : ""}">
      <div class="recovery-file-icon">${e.repairable ? "\u{1F527}" : "\u274C"}</div>
      <div class="recovery-file-info">
        <span class="recovery-file-name">${_escHtml((e.path || "").split("/").pop())}</span>
        <span class="recovery-file-meta">${_escHtml(e.description)}</span>
        <span class="recovery-file-path">${_escHtml(e.path)}</span>
      </div>
      <div class="recovery-file-actions">
        ${e.repairable ? `<button class="btn-sm btn-repair" data-path="${_escHtml(e.path)}">\u{1F527} ${t("recovery.repair")}</button>` : `<span class="recovery-badge-danger">${t("recovery.unrepairable")}</span>`}
      </div>
    </div>
  `).join("");

  container.innerHTML = `
    <div class="recovery-section">
      <div class="recovery-stats-row">
        <div class="recovery-stat-card ${healthPct === 100 ? "success" : healthPct > 90 ? "" : "danger"}">
          <span class="recovery-stat-value">${healthPct}%</span>
          <span class="recovery-stat-label">${t("recovery.health_score")}</span>
        </div>
        <div class="recovery-stat-card">
          <span class="recovery-stat-value">${r.total_checked}</span>
          <span class="recovery-stat-label">${t("recovery.checked")}</span>
        </div>
        <div class="recovery-stat-card success">
          <span class="recovery-stat-value">${r.healthy}</span>
          <span class="recovery-stat-label">${t("recovery.healthy")}</span>
        </div>
        <div class="recovery-stat-card ${r.corrupted > 0 ? "danger" : ""}">
          <span class="recovery-stat-value">${r.corrupted}</span>
          <span class="recovery-stat-label">${t("recovery.corrupted")}</span>
        </div>
      </div>

      ${r.corrupted === 0 ? `
        <div class="recovery-empty-state">
          <div class="recovery-empty-icon">\u2705</div>
          <h3>${t("recovery.all_healthy")}</h3>
          <p>${t("recovery.all_healthy_hint")}</p>
        </div>
      ` : `
        <h4 class="recovery-section-title">${t("recovery.corrupted_files")} (${r.corrupted})</h4>
        <div class="recovery-actions-bar">
          <button class="btn-primary" id="btn-repair-all">\u{1F527} ${t("recovery.repair_all")}</button>
        </div>
        <div class="recovery-file-list">${errorRows}</div>
      `}

      <div class="recovery-action-center" style="margin-top:24px">
        <button class="btn-secondary" id="btn-recheck">\u{1F504} ${t("recovery.recheck")}</button>
      </div>
    </div>`;

  // Bind repair buttons
  container.querySelectorAll(".btn-repair").forEach(btn => {
    btn.addEventListener("click", async () => {
      btn.disabled = true;
      btn.textContent = "...";
      try {
        const result = await apiPost("/recovery/repair", { path: btn.dataset.path });
        if (result.success) {
          showToast(t("recovery.repair_ok"), "success");
          btn.closest(".recovery-file-item")?.classList.add("repaired");
          btn.textContent = "\u2705";
        } else {
          showToast(t("general.error", { message: result.error }), "error");
          btn.textContent = "\u274C";
        }
      } catch (e) {
        showToast(t("general.error", { message: e.message }), "error");
        btn.disabled = false;
        btn.textContent = `\u{1F527} ${t("recovery.repair")}`;
      }
    });
  });

  // Repair all
  container.querySelector("#btn-repair-all")?.addEventListener("click", async () => {
    const repairBtns = container.querySelectorAll(".btn-repair:not(:disabled)");
    for (const btn of repairBtns) {
      btn.click();
      await new Promise(r => setTimeout(r, 200));
    }
  });

  container.querySelector("#btn-recheck")?.addEventListener("click", () => {
    _integrityResult = null;
    _renderIntegrity(container);
  });
}

// ── PhotoRec ─────────────────────────────────────────

async function _renderPhotoRec(container) {
  try {
    _photorec = await api("/recovery/photorec/status");
  } catch { _photorec = { available: false }; }

  if (!_photorec.available) {
    container.innerHTML = `
      <div class="recovery-section">
        <div class="recovery-empty-state">
          <div class="recovery-empty-icon">\u{1F4BE}</div>
          <h3>${t("recovery.photorec_not_installed")}</h3>
          <p>${t("recovery.photorec_install_hint")}</p>
          <code class="recovery-install-cmd">brew install testdisk</code>
        </div>
      </div>`;
    return;
  }

  let disksHtml = "";
  try {
    const { disks } = await api("/recovery/disks");
    disksHtml = (disks || []).map(d => `
      <div class="recovery-disk-card" data-device="${_escHtml(d.device)}">
        <div class="recovery-disk-icon">\u{1F4BD}</div>
        <div class="recovery-disk-info">
          <span class="recovery-disk-name">${_escHtml(d.description || d.device)}</span>
          <span class="recovery-disk-meta">${d.total_size ? _formatSize(d.total_size) : ""} ${d.device}</span>
        </div>
      </div>
    `).join("");
  } catch { /* ignore */ }

  container.innerHTML = `
    <div class="recovery-section">
      <div class="recovery-feature-card" style="border-left:4px solid var(--color-warning)">
        <p>\u26A0\uFE0F ${t("recovery.photorec_warning")}</p>
      </div>
      <h4 class="recovery-section-title">${t("recovery.select_disk")}</h4>
      <div class="recovery-disk-grid">${disksHtml || `<p>${t("recovery.no_disks")}</p>`}</div>
      <div class="recovery-photorec-config" id="photorec-config" style="margin-top:20px">
        <div class="recovery-field">
          <label>${t("recovery.output_dir")}</label>
          <input type="text" id="pr-output" class="recovery-input" value="${_homeDir()}/Desktop/GML_Recovery" />
        </div>
        <button class="btn-primary btn-lg" id="btn-photorec" disabled>
          \u{1F4BE} ${t("recovery.start_photorec")}
        </button>
      </div>
      <div id="photorec-progress" class="recovery-progress hidden"></div>
    </div>`;

  let selectedDisk = null;
  container.querySelectorAll(".recovery-disk-card").forEach(card => {
    card.addEventListener("click", () => {
      container.querySelectorAll(".recovery-disk-card").forEach(c => c.classList.remove("selected"));
      card.classList.add("selected");
      selectedDisk = card.dataset.device;
      const btn = container.querySelector("#btn-photorec");
      if (btn) btn.disabled = false;
    });
  });

  container.querySelector("#btn-photorec")?.addEventListener("click", async () => {
    if (!selectedDisk) return;
    const outputDir = container.querySelector("#pr-output")?.value || "";
    const btn = container.querySelector("#btn-photorec");
    const progressEl = container.querySelector("#photorec-progress");
    if (btn) { btn.disabled = true; btn.textContent = t("recovery.running_photorec"); }
    if (progressEl) progressEl.classList.remove("hidden");

    try {
      const { task_id } = await apiPost("/recovery/photorec/run", {
        source: selectedDisk,
        output_dir: outputDir,
      });
      await _pollTask(task_id, progressEl, (result) => {
        showToast(t("recovery.photorec_complete", { count: result.files_recovered }), "success");
      });
    } catch (e) {
      showToast(t("general.error", { message: e.message }), "error");
      if (btn) { btn.disabled = false; btn.textContent = `\u{1F4BE} ${t("recovery.start_photorec")}`; }
    }
  });
}

// ── Helpers ──────────────────────────────────────────

async function _pollTask(taskId, progressEl, onComplete) {
  const maxAttempts = 600;
  for (let i = 0; i < maxAttempts; i++) {
    await new Promise(r => setTimeout(r, 1000));
    try {
      const task = await api(`/tasks/${taskId}`);
      if (task.progress && progressEl) {
        const pct = task.progress.progress_pct || 0;
        progressEl.innerHTML = `
          <div class="recovery-progress-bar">
            <div class="recovery-progress-fill" style="width:${pct}%"></div>
          </div>
          <span class="recovery-progress-text">${pct}% ${task.progress.phase || ""}</span>`;
      }
      if (task.status === "completed") {
        if (progressEl) progressEl.classList.add("hidden");
        if (onComplete) onComplete(task.result);
        return;
      }
      if (task.status === "failed") {
        if (progressEl) progressEl.classList.add("hidden");
        showToast(t("task.failed_toast", { error: task.error }), "error");
        return;
      }
    } catch { /* retry */ }
  }
}

function _formatSize(bytes) {
  if (!bytes || bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0) + " " + units[i];
}

function _categoryIcon(cat) {
  switch (cat) {
    case "image": return "\u{1F5BC}\uFE0F";
    case "video": return "\u{1F3AC}";
    case "audio": return "\u{1F3B5}";
    default: return "\u{1F4C4}";
  }
}

function _escHtml(str) {
  if (!str) return "";
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function _homeDir() {
  // Best-effort home dir for prompts
  return "/Users/" + (location.hostname === "localhost" ? "user" : "user");
}
