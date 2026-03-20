/* GOD MODE Media Library — Doctor page (settings panel content) */

import { api } from "../api.js";
import { escapeHtml } from "../utils.js";
import { t } from "../i18n.js";

export async function render(container) {
  try {
    const data = await api("/deps");
    let html = '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden">';
    for (const d of data.dependencies) {
      const status = d.available ? "ok" : "missing";
      const statusLabel = d.available ? t("doctor.available") : t("doctor.missing");
      const ver = d.version ? ` (${escapeHtml(d.version)})` : "";
      const hint = d.install_hint ? `<span class="dep-hint">${escapeHtml(d.install_hint)}</span>` : "";
      html += `<div class="dep-item">
        <div class="dep-status ${status}" aria-label="${statusLabel}"></div>
        <strong style="font-size:13px">${escapeHtml(d.name)}</strong>${ver}
        ${hint}
      </div>`;
    }
    html += "</div>";
    container.innerHTML = html;
  } catch (e) {
    container.innerHTML = `<div class="empty" style="padding:16px">${t("general.error", { message: e.message })}</div>`;
  }
}
