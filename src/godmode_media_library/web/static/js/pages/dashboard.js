/* GOD MODE Media Library — Dashboard page */

import { api } from "../api.js";
import { content, formatBytes, escapeHtml } from "../utils.js";
import { t } from "../i18n.js";

export async function render(container) {
  try {
    const stats = await api("/stats");
    const cards = [
      [t("dashboard.total_files"), stats.total_files?.toLocaleString() ?? 0],
      [t("dashboard.total_size"), formatBytes(stats.total_size_bytes)],
      [t("dashboard.hashed"), stats.hashed_files?.toLocaleString() ?? 0],
      [t("dashboard.duplicate_groups"), stats.duplicate_groups ?? 0],
      [t("dashboard.duplicate_files"), stats.duplicate_files ?? 0],
      [t("dashboard.gps_files"), stats.gps_files ?? 0],
      [t("dashboard.media_probed"), stats.media_probed ?? 0],
      [t("dashboard.labeled"), stats.labeled_files ?? 0],
    ];
    let html = `<h2>${t("dashboard.title")}</h2><div class='stats-grid'>`;
    for (const [label, value] of cards) {
      html += `<div class="stat-card"><div class="label">${label}</div><div class="value">${value}</div></div>`;
    }
    html += "</div>";

    const exts = stats.top_extensions;
    if (exts && exts.length) {
      html += `<h2>${t("dashboard.top_extensions")}</h2><table><tr><th>${t("dashboard.extension")}</th><th>${t("dashboard.count")}</th></tr>`;
      for (const [ext, count] of exts) {
        html += `<tr><td>.${escapeHtml(ext)}</td><td>${count.toLocaleString()}</td></tr>`;
      }
      html += "</table>";
    }

    const cams = stats.top_cameras;
    if (cams && cams.length) {
      html += `<h2>${t("dashboard.top_cameras")}</h2><table><tr><th>${t("dashboard.camera")}</th><th>${t("dashboard.count")}</th></tr>`;
      for (const [cam, count] of cams) {
        html += `<tr><td>${escapeHtml(cam)}</td><td>${count.toLocaleString()}</td></tr>`;
      }
      html += "</table>";
    }

    container.innerHTML = html;
  } catch (e) {
    container.innerHTML = `<div class="empty"><div class="empty-icon">&#128202;</div><div class="empty-text">${t("dashboard.empty_title")}</div><div class="empty-hint">${t("dashboard.empty_hint")}</div></div>`;
  }
}
