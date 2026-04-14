/* GOD MODE Media Library — iPhone Import */

import { t } from "../i18n.js";
import { $, showToast, formatBytes } from "../utils.js";
import { api, apiPost } from "../api.js";

let _container = null;
let _pollTimer = null;
let _ws = null;

export function render(container) {
  _container = container;
  container.innerHTML = `
    <div class="page-header">
      <h1>&#128241; iPhone Import</h1>
      <p class="text-muted">${t("iphone.subtitle") || "Přeneste média z iPhone přímo na Google Drive"}</p>
    </div>

    <div id="iphone-connection" class="card" style="margin-bottom: 1rem;">
      <div class="card-body">
        <h3>${t("iphone.connection") || "Připojení"}</h3>
        <div id="iphone-status-indicator">
          <span class="status-dot status-checking"></span>
          ${t("iphone.checking") || "Kontroluji připojení..."}
        </div>
        <div id="iphone-device-info" style="margin-top: 0.5rem;"></div>
      </div>
    </div>

    <div id="iphone-files-card" class="card" style="margin-bottom: 1rem; display: none;">
      <div class="card-body">
        <h3>${t("iphone.files_on_device") || "Soubory na zařízení"}</h3>
        <div id="iphone-files-summary"></div>
        <button id="btn-iphone-scan" class="btn btn-secondary" style="margin-top: 0.5rem;">
          ${t("iphone.scan_device") || "Naskenovat zařízení"}
        </button>
      </div>
    </div>

    <div id="iphone-transfer-card" class="card" style="margin-bottom: 1rem; display: none;">
      <div class="card-body">
        <h3>${t("iphone.transfer") || "Přenos"}</h3>
        <div class="iphone-config" style="margin-bottom: 1rem;">
          <label>Cíl: <strong>gws-backup:GML-Consolidated</strong></label>
        </div>
        <div class="iphone-actions" style="display: flex; gap: 0.5rem; margin-bottom: 1rem;">
          <button id="btn-iphone-start" class="btn btn-primary">
            ${t("iphone.start_transfer") || "Spustit přenos"}
          </button>
          <button id="btn-iphone-pause" class="btn btn-secondary" style="display: none;">
            ${t("iphone.pause") || "Pozastavit"}
          </button>
          <button id="btn-iphone-resume" class="btn btn-primary" style="display: none;">
            ${t("iphone.resume") || "Pokračovat"}
          </button>
        </div>
        <div id="iphone-progress" style="display: none;">
          <div class="progress-bar-container" style="height: 24px; background: var(--bg-tertiary); border-radius: 6px; overflow: hidden; margin-bottom: 0.75rem;">
            <div id="iphone-progress-bar" style="height: 100%; background: var(--accent); transition: width 0.3s; width: 0%;"></div>
          </div>
          <div id="iphone-progress-stats" class="grid-2col" style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem;">
          </div>
          <div id="iphone-current-file" class="text-muted" style="margin-top: 0.5rem; font-size: 0.85rem;"></div>
        </div>
      </div>
    </div>

    <div id="iphone-log" class="card" style="display: none;">
      <div class="card-body">
        <h3>${t("iphone.log") || "Log"}</h3>
        <div id="iphone-log-content" style="max-height: 200px; overflow-y: auto; font-family: monospace; font-size: 0.8rem;"></div>
      </div>
    </div>
  `;

  // Wire up buttons
  $("#btn-iphone-scan")?.addEventListener("click", _scanDevice);
  $("#btn-iphone-start")?.addEventListener("click", _startTransfer);
  $("#btn-iphone-pause")?.addEventListener("click", _pauseTransfer);
  $("#btn-iphone-resume")?.addEventListener("click", _resumeTransfer);

  // Initial status check
  _checkStatus();
  _pollTimer = setInterval(_checkStatus, 5000);
}

export function cleanup() {
  if (_pollTimer) { clearInterval(_pollTimer); _pollTimer = null; }
  if (_ws) { _ws.close(); _ws = null; }
  _container = null;
}

async function _checkStatus() {
  try {
    const status = await api("/iphone/status");
    _updateConnectionUI(status);
    if (status.progress && status.progress.phase !== "idle") {
      _updateProgressUI(status.progress);
    }
  } catch (e) {
    _updateConnectionUI({ connected: false });
  }
}

function _updateConnectionUI(status) {
  const el = $("#iphone-status-indicator");
  if (!el) return;

  const filesCard = $("#iphone-files-card");
  const transferCard = $("#iphone-transfer-card");

  if (status.connected) {
    const name = status.device_name || "iPhone";
    el.innerHTML = `<span class="status-dot status-ok"></span> <strong>${name}</strong> — připojen přes USB`;
    if (filesCard) filesCard.style.display = "";
    if (transferCard) transferCard.style.display = "";
  } else {
    el.innerHTML = `<span class="status-dot status-error"></span> iPhone nepřipojen`;
    if (filesCard) filesCard.style.display = "none";
    // Keep transfer card visible if transfer is in progress
    const progress = status.progress;
    if (transferCard && (!progress || progress.phase === "idle")) {
      transferCard.style.display = "none";
    }
  }

  // Show device info
  const info = $("#iphone-device-info");
  if (info && status.device_name) {
    info.textContent = status.device_name;
  }
}

async function _scanDevice() {
  const btn = $("#btn-iphone-scan");
  if (btn) { btn.disabled = true; btn.textContent = "Skenuji..."; }

  try {
    const result = await api("/iphone/list");
    const summary = $("#iphone-files-summary");
    if (summary) {
      summary.innerHTML = `
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 0.5rem;">
          <div><strong>${result.media_files}</strong> médií</div>
          <div><strong>${formatBytes(result.media_size)}</strong></div>
          <div>${result.total_files} souborů celkem</div>
          <div>${result.folders?.length || 0} složek</div>
        </div>
      `;
    }
    const transferCard = $("#iphone-transfer-card");
    if (transferCard) transferCard.style.display = "";
  } catch (e) {
    showToast("Chyba: " + (e.message || e), "error");
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "Naskenovat zařízení"; }
  }
}

async function _startTransfer() {
  const btn = $("#btn-iphone-start");
  if (btn) { btn.disabled = true; btn.textContent = "Spouštím..."; }

  try {
    const result = await apiPost("/iphone/start", {
      dest_remote: "gws-backup",
      dest_path: "GML-Consolidated",
      structure_pattern: "year_month",
      media_only: true,
    });
    showToast("Import spuštěn", "success");
    _showTransferUI(true);
  } catch (e) {
    showToast("Chyba: " + (e.detail || e.message || e), "error");
    if (btn) { btn.disabled = false; btn.textContent = "Spustit přenos"; }
  }
}

async function _pauseTransfer() {
  try {
    await apiPost("/iphone/pause", {});
    showToast("Pozastaveno", "info");
    _showTransferUI(false, true);
  } catch (e) {
    showToast("Chyba: " + (e.message || e), "error");
  }
}

async function _resumeTransfer() {
  try {
    await apiPost("/iphone/resume", {});
    showToast("Pokračuji...", "success");
    _showTransferUI(true);
  } catch (e) {
    showToast("Chyba: " + (e.message || e), "error");
  }
}

function _showTransferUI(running, paused = false) {
  const start = $("#btn-iphone-start");
  const pause = $("#btn-iphone-pause");
  const resume = $("#btn-iphone-resume");
  const progress = $("#iphone-progress");

  if (start) start.style.display = running ? "none" : "";
  if (pause) pause.style.display = running ? "" : "none";
  if (resume) resume.style.display = paused ? "" : "none";
  if (progress) progress.style.display = "";
}

function _updateProgressUI(p) {
  if (!_container) return;

  const isRunning = p.phase === "transferring" || p.phase === "listing";
  const isPaused = p.phase === "paused";
  const isDone = p.phase === "completed";
  const isError = p.phase === "error";

  _showTransferUI(isRunning, isPaused);

  // Progress bar
  const bar = $("#iphone-progress-bar");
  if (bar && p.total_files > 0) {
    const pct = Math.min(100, (p.completed_files / p.total_files) * 100);
    bar.style.width = pct.toFixed(1) + "%";
    if (isDone) bar.style.background = "var(--success, #22c55e)";
    else if (isError) bar.style.background = "var(--danger, #ef4444)";
    else bar.style.background = "var(--accent)";
  }

  // Stats
  const stats = $("#iphone-progress-stats");
  if (stats) {
    const speed = p.speed_bps > 0 ? formatBytes(p.speed_bps) + "/s" : "—";
    const eta = p.speed_bps > 0 && p.bytes_total > p.bytes_transferred
      ? _formatEta((p.bytes_total - p.bytes_transferred) / p.speed_bps)
      : "—";

    stats.innerHTML = `
      <div><strong>${p.completed_files}</strong> / ${p.total_files} souborů</div>
      <div><strong>${formatBytes(p.bytes_transferred)}</strong> / ${formatBytes(p.bytes_total)}</div>
      <div>Rychlost: <strong>${speed}</strong></div>
      <div>ETA: <strong>${eta}</strong></div>
      <div>Přeskočeno: ${p.skipped_files}</div>
      <div>Selhalo: ${p.failed_files}</div>
    `;
  }

  // Current file
  const curr = $("#iphone-current-file");
  if (curr) {
    if (isError && p.error) {
      curr.innerHTML = `<span style="color: var(--danger);">&#9888; ${p.error}</span>`;
    } else if (isDone) {
      curr.textContent = "Import dokončen!";
    } else if (isPaused) {
      curr.textContent = p.error || "Pozastaveno";
    } else {
      curr.textContent = p.current_file || "";
    }
  }

  // Show progress card
  const progressEl = $("#iphone-progress");
  if (progressEl) progressEl.style.display = "";
  const transferCard = $("#iphone-transfer-card");
  if (transferCard) transferCard.style.display = "";
}

function _formatEta(seconds) {
  if (!seconds || seconds <= 0 || !isFinite(seconds)) return "—";
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m`;
  return `< 1m`;
}
