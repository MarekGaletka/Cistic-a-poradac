/* GOD MODE Media Library — Similar page */

import { api } from "../api.js";
import { escapeHtml, fileName } from "../utils.js";
import { t } from "../i18n.js";
import { showVisualDiff } from "../modal.js";

export async function render(container) {
  try {
    const data = await api("/similar?threshold=10&limit=50");
    if (!data.pairs.length) {
      container.innerHTML = `<h2>${t("similar.title")}</h2><div class="empty"><div class="empty-icon">&#127912;</div><div class="empty-text">${t("similar.empty_title")}</div><div class="empty-hint">${t("similar.empty_hint")}</div></div>`;
      return;
    }
    let html = `<h2>${t("similar.title")} <span style="color:var(--text-muted);font-size:14px">(${t("similar.pairs", { count: data.total_pairs })})</span></h2>`;
    html += '<div class="similar-grid">';
    for (const p of data.pairs) {
      const srcA = `/api/thumbnail${encodeURI(p.path_a)}?size=200`;
      const srcB = `/api/thumbnail${encodeURI(p.path_b)}?size=200`;
      html += `<div class="similar-pair">
        <div class="distance">${t("similar.distance", { value: p.distance })}</div>
        <div class="thumbs">
          <img src="${srcA}" alt="${escapeHtml(fileName(p.path_a))}" onerror="this.style.display='none'">
          <img src="${srcB}" alt="${escapeHtml(fileName(p.path_b))}" onerror="this.style.display='none'">
        </div>
        <div style="margin-top:6px;font-size:12px;color:var(--text-muted)">
          ${escapeHtml(fileName(p.path_a))}<br>${escapeHtml(fileName(p.path_b))}
        </div>
        <button style="margin-top:6px;width:100%" class="btn-compare" data-pa="${escapeHtml(p.path_a)}" data-pb="${escapeHtml(p.path_b)}">${t("similar.compare")}</button>
      </div>`;
    }
    html += "</div>";
    container.innerHTML = html;

    // Bind compare buttons
    container.querySelectorAll(".btn-compare").forEach(btn => {
      btn.addEventListener("click", () => {
        showVisualDiff(btn.dataset.pa, btn.dataset.pb, null, null);
      });
    });
  } catch (e) {
    container.innerHTML = `<h2>${t("similar.title")}</h2><div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}
