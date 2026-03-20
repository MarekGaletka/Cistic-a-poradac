/* GOD MODE Media Library — Similar page (improved) */

import { api } from "../api.js";
import { escapeHtml, fileName } from "../utils.js";
import { t } from "../i18n.js";
import { showVisualDiff } from "../modal.js";

export async function render(container) {
  try {
    const data = await api("/similar?threshold=10&limit=50");
    if (!data.pairs.length) {
      container.innerHTML = `
        <div class="page-header"><h2>${t("similar.title")}</h2></div>
        <div class="empty-state-hero" style="padding:40px 0">
          <div class="empty-state-icon" style="font-size:48px">&#127912;</div>
          <h3 class="empty-state-title">${t("similar.empty_title")}</h3>
          <p class="empty-state-subtitle">${t("similar.empty_hint")}</p>
        </div>`;
      return;
    }
    let html = `
      <div class="page-header">
        <h2>${t("similar.title")} <span class="header-count">${t("similar.pairs", { count: data.total_pairs })}</span></h2>
      </div>`;
    html += '<div class="similar-grid">';
    for (const p of data.pairs) {
      const srcA = `/api/thumbnail${encodeURI(p.path_a)}?size=200`;
      const srcB = `/api/thumbnail${encodeURI(p.path_b)}?size=200`;
      // Color-code distance
      const distColor = p.distance <= 3 ? "var(--red)" : p.distance <= 6 ? "var(--yellow)" : "var(--text-muted)";
      html += `<div class="similar-pair">
        <div class="distance" style="color:${distColor}">${t("similar.distance", { value: p.distance })}</div>
        <div class="thumbs">
          <img src="${srcA}" alt="${escapeHtml(fileName(p.path_a))}" onerror="this.style.display='none'" loading="lazy">
          <img src="${srcB}" alt="${escapeHtml(fileName(p.path_b))}" onerror="this.style.display='none'" loading="lazy">
        </div>
        <div class="similar-names">
          <span>${escapeHtml(fileName(p.path_a))}</span>
          <span>${escapeHtml(fileName(p.path_b))}</span>
        </div>
        <button class="btn-compare primary" data-pa="${escapeHtml(p.path_a)}" data-pb="${escapeHtml(p.path_b)}">${t("similar.compare")}</button>
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
    container.innerHTML = `<div class="page-header"><h2>${t("similar.title")}</h2></div><div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}
