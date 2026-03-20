/* GOD MODE Media Library — Pipeline page */

import { apiPost } from "../api.js";
import { $, escapeHtml, showToast } from "../utils.js";
import { t } from "../i18n.js";
import { pollTask } from "../tasks.js";

export async function render(container) {
  container.innerHTML = `<h2>${t("pipeline.title")}</h2>
    <p style="color:var(--text-muted);margin-bottom:16px">${t("pipeline.description")}</p>
    <div class="config-form">
      <div class="form-group">
        <label class="form-label" for="cfg-roots">${t("pipeline.roots")}</label>
        <textarea id="cfg-roots" rows="3" placeholder="${t("pipeline.roots_placeholder")}" aria-label="${t("pipeline.roots")}"></textarea>
      </div>
      <div class="form-row">
        <div class="form-group">
          <label class="form-label" for="cfg-workers">${t("pipeline.workers")}</label>
          <input type="number" id="cfg-workers" value="1" min="1" max="16" style="width:70px">
        </div>
        <label class="filter-checkbox"><input type="checkbox" id="cfg-exiftool" checked> ${t("pipeline.exiftool")}</label>
      </div>
    </div>
    <div style="display:flex;gap:8px;margin-bottom:20px">
      <button class="primary" id="btn-start-pipeline" aria-label="${t("pipeline.start_pipeline")}">${t("pipeline.start_pipeline")}</button>
      <button id="btn-start-scan" aria-label="${t("pipeline.scan_only")}">${t("pipeline.scan_only")}</button>
    </div>
    <div id="task-output" aria-live="polite"></div>`;

  // Bind buttons
  container.querySelector("#btn-start-pipeline").addEventListener("click", startPipeline);
  container.querySelector("#btn-start-scan").addEventListener("click", startScan);
}

function getScanConfig() {
  const rootsText = $("#cfg-roots")?.value || "";
  const roots = rootsText.split("\n").map(s => s.trim()).filter(Boolean);
  const workers = parseInt($("#cfg-workers")?.value || "1", 10);
  const extract_exiftool = $("#cfg-exiftool")?.checked ?? true;
  const body = { workers, extract_exiftool };
  if (roots.length) body.roots = roots;
  return body;
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
