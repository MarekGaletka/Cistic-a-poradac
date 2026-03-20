/* GOD MODE Media Library — Timeline page */

import { api } from "../api.js";
import { escapeHtml, fileName, IMAGE_EXTS } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";

export async function render(container) {
  try {
    const data = await api("/files?limit=500");
    const files = data.files.filter(f => f.date_original);

    if (!files.length) {
      container.innerHTML = `
        <div class="page-header"><h2>${t("timeline.title")}</h2></div>
        <div class="empty-state-hero" style="padding:40px 0">
          <div class="empty-state-icon" style="font-size:48px">&#128197;</div>
          <h3 class="empty-state-title">${t("timeline.empty_title")}</h3>
          <p class="empty-state-subtitle">${t("timeline.empty_hint")}</p>
        </div>`;
      return;
    }

    // Group files by month
    const groups = {};
    for (const f of files) {
      const date = f.date_original;
      const match = date.match(/^(\d{4})[:\-/](\d{2})/);
      const key = match ? `${match[1]}-${match[2]}` : "Unknown";
      if (!groups[key]) groups[key] = [];
      groups[key].push(f);
    }

    const sortedMonths = Object.keys(groups).sort().reverse();

    let html = `
      <div class="page-header">
        <h2>${t("timeline.title")} <span class="header-count">${t("timeline.dated_files", { count: files.length })}</span></h2>
      </div>`;
    html += '<div class="timeline">';

    for (const month of sortedMonths) {
      const monthFiles = groups[month];
      const [y, m] = month.split("-");
      const monthName = m ? new Date(parseInt(y), parseInt(m) - 1).toLocaleDateString("cs", { year: "numeric", month: "long" }) : month;

      html += `<div class="timeline-month">
        <div class="timeline-header">${escapeHtml(monthName)} <span class="timeline-count">(${monthFiles.length})</span></div>
        <div class="timeline-grid">`;

      for (const f of monthFiles.slice(0, 20)) {
        const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());
        const thumb = isImage
          ? `<img src="/api/thumbnail${encodeURI(f.path)}?size=150" onerror="this.style.display='none'" alt="${escapeHtml(fileName(f.path))}" loading="lazy">`
          : `<div class="timeline-icon">${escapeHtml(f.ext)}</div>`;
        html += `<div class="timeline-item" tabindex="0" role="button" data-file-path="${escapeHtml(f.path)}" title="${escapeHtml(f.path)}">
          ${thumb}
          <div class="timeline-name">${escapeHtml(fileName(f.path))}</div>
        </div>`;
      }
      if (monthFiles.length > 20) {
        html += `<div class="timeline-more">${t("timeline.more", { count: monthFiles.length - 20 })}</div>`;
      }
      html += '</div></div>';
    }

    html += '</div>';
    container.innerHTML = html;

    // Bind click events
    container.querySelectorAll("[data-file-path]").forEach(item => {
      item.addEventListener("click", () => showFileDetail(item.dataset.filePath));
      item.addEventListener("keydown", e => { if (e.key === "Enter") showFileDetail(item.dataset.filePath); });
    });
  } catch (e) {
    container.innerHTML = `<div class="page-header"><h2>${t("timeline.title")}</h2></div><div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}
