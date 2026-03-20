/* GOD MODE Media Library — Pipeline (settings panel content) */

import { api, apiPost, apiDelete } from "../api.js";
import { $, escapeHtml, showToast } from "../utils.js";
import { t } from "../i18n.js";
import { pollTask } from "../tasks.js";
import { openFolderPicker } from "../folder-picker.js";

let _roots = [];

export async function render(container) {
  // Load saved roots
  try {
    const data = await api("/roots");
    _roots = data.roots || [];
  } catch {
    _roots = [];
  }

  _renderContent(container);
}

function _renderContent(container) {
  let chipsHtml = "";
  if (_roots.length > 0) {
    chipsHtml = `<div class="folder-chips">`;
    for (const root of _roots) {
      const name = root.split("/").pop() || root;
      chipsHtml += `<span class="folder-chip"><span class="folder-chip-icon">\u{1F4C1}</span> ${escapeHtml(name)}<span class="folder-chip-path">${escapeHtml(root)}</span><button class="folder-chip-remove" data-path="${escapeHtml(root)}" aria-label="${t("folder.remove")}">&times;</button></span>`;
    }
    chipsHtml += "</div>";
  } else {
    chipsHtml = `<p class="pipeline-no-roots">${t("pipeline.no_roots")}</p>`;
  }

  container.innerHTML = `
    <p style="color:var(--text-muted);margin-bottom:12px;font-size:13px">${t("pipeline.description")}</p>
    <div class="pipeline-roots-section">
      <label class="form-label">${t("pipeline.configured_roots")}</label>
      ${chipsHtml}
      <button class="pipeline-add-folder-btn" id="btn-pipeline-add-folder">
        \u{1F4C1} ${t("folder.add_folder")}
      </button>
    </div>
    <div style="display:flex;gap:8px;margin-top:12px">
      <button class="primary" id="btn-start-pipeline" aria-label="${t("pipeline.start_pipeline")}" ${_roots.length === 0 ? "disabled" : ""}>${t("pipeline.start_pipeline")}</button>
      <button id="btn-start-scan" aria-label="${t("pipeline.scan_only")}" ${_roots.length === 0 ? "disabled" : ""}>${t("pipeline.scan_only")}</button>
    </div>
    <div id="task-output" aria-live="polite" style="margin-top:12px"></div>`;

  // Bind add folder button
  container.querySelector("#btn-pipeline-add-folder").addEventListener("click", () => {
    openFolderPicker(async (paths) => {
      const merged = [...new Set([..._roots, ...paths])];
      _roots = merged;
      try {
        await apiPost("/roots", { roots: _roots });
      } catch { /* silent */ }
      _renderContent(container);
    }, _roots);
  });

  // Bind chip remove buttons
  container.querySelectorAll(".folder-chip-remove").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const path = btn.dataset.path;
      _roots = _roots.filter(r => r !== path);
      try {
        await apiDelete("/roots", { path });
      } catch { /* silent */ }
      _renderContent(container);
    });
  });

  // Bind buttons
  container.querySelector("#btn-start-pipeline").addEventListener("click", startPipeline);
  container.querySelector("#btn-start-scan").addEventListener("click", startScan);
}

function getScanConfig() {
  return { roots: _roots, workers: 1, extract_exiftool: true };
}

async function startPipeline() {
  try {
    const data = await apiPost("/pipeline", getScanConfig());
    showToast(t("pipeline.started"), "info");
    pollTask(data.task_id);
  } catch (e) {
    showToast(t("pipeline.start_failed", { message: e.message }), "error");
    const el = $("#task-output");
    if (el) el.innerHTML = `<div class="task-status failed">${t("general.error", { message: e.message })}</div>`;
  }
}

async function startScan() {
  try {
    const data = await apiPost("/scan", getScanConfig());
    showToast(t("pipeline.scan_started"), "info");
    pollTask(data.task_id);
  } catch (e) {
    showToast(t("pipeline.scan_failed", { message: e.message }), "error");
    const el = $("#task-output");
    if (el) el.innerHTML = `<div class="task-status failed">${t("general.error", { message: e.message })}</div>`;
  }
}
